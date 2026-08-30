import sqlite3
from typing import List, Dict, Any, Tuple, Optional, Set
from datetime import datetime
import pandas as pd
from src.config import DATABASE_PATH, CONSOLIDATED_CSV_PATH


def get_connection(db_path=DATABASE_PATH) -> sqlite3.Connection:
    """Cria e retorna uma conexão com o banco SQLite."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str) -> None:
    """Garante que uma coluna exista na tabela, adicionando-a caso não exista."""
    cursor.execute(f"PRAGMA table_info({table_name});")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if column_name not in existing_cols:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};")


def init_db(db_path=DATABASE_PATH) -> None:
    """Cria as tabelas, índices e views no banco de dados SQLite com migração automática."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # 1. Tabela de laboratórios
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS laboratorios (
                id_asset INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                initials TEXT,
                institution_name TEXT,
                institution_initials TEXT,
                city TEXT,
                state TEXT,
                has_sharing INTEGER,
                about TEXT,
                path_image TEXT,
                first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Migração segura para colunas de enriquecimento
        lab_columns = [
            ("social_reason", "TEXT"),
            ("zip_code", "TEXT"),
            ("street", "TEXT"),
            ("complement", "TEXT"),
            ("district", "TEXT"),
            ("address_number", "TEXT"),
            ("contact_name", "TEXT"),
            ("contact_phone", "TEXT"),
            ("email_lab", "TEXT"),
            ("website", "TEXT"),
            ("latitude", "TEXT"),
            ("longitude", "TEXT"),
            ("sharing_type", "TEXT"),
            ("own_share_link", "TEXT"),
            ("area_expertise", "TEXT"),
            ("techniques", "TEXT"),
            ("created_date", "TEXT"),
            ("details_synced_at", "TIMESTAMP"),
        ]
        for col_name, col_type in lab_columns:
            _ensure_column(cursor, "laboratorios", col_name, col_type)

        # 2. Tabela de equipamentos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS equipamentos (
                id INTEGER PRIMARY KEY,
                laboratorio_id INTEGER NOT NULL,
                code TEXT,
                name TEXT,
                brand TEXT,
                model TEXT,
                manufacturer TEXT,
                situation TEXT,
                equipment_situation TEXT,
                replicated INTEGER,
                is_editor INTEGER,
                first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (laboratorio_id) REFERENCES laboratorios(id_asset) ON DELETE CASCADE
            );
        """)
        
        # 3. Tabela de auditoria / execuções de scraping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                job_type TEXT NOT NULL DEFAULT 'FULL_SYNC',
                total_extracted INTEGER NOT NULL,
                new_records INTEGER NOT NULL,
                updated_records INTEGER NOT NULL,
                status TEXT NOT NULL,
                duration_seconds REAL NOT NULL,
                error_message TEXT
            );
        """)
        _ensure_column(cursor, "scraping_runs", "job_type", "TEXT NOT NULL DEFAULT 'FULL_SYNC'")

        # 4. Índices para performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_laboratorios_state ON laboratorios(state);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_laboratorios_institution ON laboratorios(institution_initials);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_equipamentos_lab_id ON equipamentos(laboratorio_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_equipamentos_name ON equipamentos(name);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_equipamentos_brand ON equipamentos(brand);")

        # 5. View unificada para consultas com joins (laboratórios + equipamentos)
        cursor.execute("DROP VIEW IF EXISTS vw_laboratorios_equipamentos;")
        cursor.execute("""
            CREATE VIEW vw_laboratorios_equipamentos AS
            SELECT 
                l.id_asset AS lab_id_asset,
                l.name AS lab_name,
                l.initials AS lab_initials,
                l.institution_name AS lab_institution_name,
                l.institution_initials AS lab_institution_initials,
                l.social_reason AS lab_social_reason,
                l.city AS lab_city,
                l.state AS lab_state,
                l.zip_code AS lab_zip_code,
                l.street AS lab_street,
                l.district AS lab_district,
                l.address_number AS lab_address_number,
                l.contact_name AS lab_contact_name,
                l.contact_phone AS lab_contact_phone,
                l.email_lab AS lab_email_lab,
                l.website AS lab_website,
                l.latitude AS lab_latitude,
                l.longitude AS lab_longitude,
                l.has_sharing AS lab_has_sharing,
                l.sharing_type AS lab_sharing_type,
                l.area_expertise AS lab_area_expertise,
                l.techniques AS lab_techniques,
                l.created_date AS lab_created_date,
                l.about AS lab_about,
                e.id AS eq_id,
                e.code AS eq_code,
                e.name AS eq_name,
                e.brand AS eq_brand,
                e.model AS eq_model,
                e.manufacturer AS eq_manufacturer,
                e.situation AS eq_situation,
                e.equipment_situation AS eq_equipment_situation,
                e.replicated AS eq_replicated
            FROM laboratorios l
            LEFT JOIN equipamentos e ON l.id_asset = e.laboratorio_id;
        """)
        
        conn.commit()


def get_existing_lab_ids(db_path=DATABASE_PATH) -> Set[int]:
    """Retorna o conjunto de id_asset já existentes na tabela laboratorios."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id_asset FROM laboratorios;")
        return {row[0] for row in cursor.fetchall()}


def get_all_lab_ids(db_path=DATABASE_PATH) -> List[int]:
    """Retorna todos os IDs de laboratórios cadastrados."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id_asset FROM laboratorios ORDER BY id_asset;")
        return [row[0] for row in cursor.fetchall()]


def get_pending_enrichment_lab_ids(db_path=DATABASE_PATH) -> List[int]:
    """Retorna a lista de IDs de laboratórios que ainda não tiveram detalhes e equipamentos sincronizados."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id_asset FROM laboratorios WHERE details_synced_at IS NULL ORDER BY id_asset;")
        return [row[0] for row in cursor.fetchall()]


def upsert_laboratorios_base(records: List[Dict[str, Any]], db_path=DATABASE_PATH) -> Tuple[List[int], int]:
    """
    Insere novos laboratórios e atualiza os existentes.
    Retorna uma tupla (lista_de_novos_id_assets, total_atualizados).
    """
    if not records:
        return [], 0

    existing_ids = get_existing_lab_ids(db_path)
    new_ids = []
    updated_count = 0
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    upsert_sql = """
        INSERT INTO laboratorios (
            id_asset,
            name,
            initials,
            institution_name,
            institution_initials,
            city,
            state,
            has_sharing,
            about,
            path_image,
            first_seen_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id_asset) DO UPDATE SET
            name = excluded.name,
            initials = excluded.initials,
            institution_name = excluded.institution_name,
            institution_initials = excluded.institution_initials,
            city = excluded.city,
            state = excluded.state,
            has_sharing = excluded.has_sharing,
            about = excluded.about,
            path_image = excluded.path_image,
            updated_at = excluded.updated_at;
    """

    data_to_insert = []
    for item in records:
        id_asset = item.get("idAsset")
        if id_asset is None:
            continue
            
        if id_asset in existing_ids:
            updated_count += 1
        else:
            new_ids.append(id_asset)

        has_sharing = item.get("hasSharing")
        has_sharing_int = 1 if has_sharing is True else (0 if has_sharing is False else None)

        data_to_insert.append((
            id_asset,
            item.get("name"),
            item.get("initials"),
            item.get("institutionName"),
            item.get("institutionInitials"),
            item.get("city"),
            item.get("state"),
            has_sharing_int,
            item.get("about"),
            item.get("path"),
            now_str,
            now_str
        ))

    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.executemany(upsert_sql, data_to_insert)
        conn.commit()

    return new_ids, updated_count


def upsert_lab_enrichment_and_equipments(
    lab_id: int,
    lab_detail: Optional[Dict[str, Any]],
    equipments: Optional[List[Dict[str, Any]]],
    db_path=DATABASE_PATH
) -> Tuple[int, int]:
    """
    Atualiza os dados detalhados do laboratório e sincroniza seus equipamentos.
    Retorna (equipamentos_novos, equipamentos_atualizados).
    """
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    new_eq = 0
    updated_eq = 0

    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # 1. Atualiza detalhes do laboratório se fornecidos
        if lab_detail:
            addr = lab_detail.get("address") or {}
            contact = lab_detail.get("contact") or {}
            
            areas = ", ".join([a.get("name", "") for a in lab_detail.get("areaExpertiseList", []) if isinstance(a, dict)])
            techniques = ", ".join([t.get("name", "") for t in lab_detail.get("techniqueList", []) if isinstance(t, dict)])
            
            cursor.execute("""
                UPDATE laboratorios SET
                    social_reason = ?,
                    zip_code = ?,
                    street = ?,
                    complement = ?,
                    district = ?,
                    address_number = ?,
                    contact_name = ?,
                    contact_phone = ?,
                    email_lab = ?,
                    website = ?,
                    latitude = ?,
                    longitude = ?,
                    sharing_type = ?,
                    own_share_link = ?,
                    area_expertise = ?,
                    techniques = ?,
                    created_date = ?,
                    details_synced_at = ?,
                    updated_at = ?
                WHERE id_asset = ?;
            """, (
                lab_detail.get("socialReason"),
                addr.get("zipCode"),
                addr.get("street"),
                addr.get("complement"),
                addr.get("district") or addr.get("city"),
                addr.get("number"),
                contact.get("name"),
                contact.get("phone"),
                lab_detail.get("emailLab"),
                lab_detail.get("website"),
                str(lab_detail.get("latitude")) if lab_detail.get("latitude") is not None else None,
                str(lab_detail.get("longitude")) if lab_detail.get("longitude") is not None else None,
                lab_detail.get("sharingType"),
                lab_detail.get("ownShareLink"),
                areas,
                techniques,
                lab_detail.get("createdDate"),
                now_str,
                now_str,
                lab_id
            ))
        else:
            # Marca como sincronizado mesmo sem detalhes extras para não reprocessar indefinidamente se não houver resposta
            cursor.execute("UPDATE laboratorios SET details_synced_at = ? WHERE id_asset = ?;", (now_str, lab_id))

        # 2. Sincroniza Equipamentos do Laboratório
        if equipments is not None:
            cursor.execute("SELECT id FROM equipamentos WHERE laboratorio_id = ?;", (lab_id,))
            existing_eq_ids = {row[0] for row in cursor.fetchall()}

            upsert_eq_sql = """
                INSERT INTO equipamentos (
                    id,
                    laboratorio_id,
                    code,
                    name,
                    brand,
                    model,
                    manufacturer,
                    situation,
                    equipment_situation,
                    replicated,
                    is_editor,
                    first_seen_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    laboratorio_id = excluded.laboratorio_id,
                    code = excluded.code,
                    name = excluded.name,
                    brand = excluded.brand,
                    model = excluded.model,
                    manufacturer = excluded.manufacturer,
                    situation = excluded.situation,
                    equipment_situation = excluded.equipment_situation,
                    replicated = excluded.replicated,
                    is_editor = excluded.is_editor,
                    updated_at = excluded.updated_at;
            """
            
            eq_params = []
            for eq in equipments:
                eq_id = eq.get("id")
                if eq_id is None:
                    continue
                
                if eq_id in existing_eq_ids:
                    updated_eq += 1
                else:
                    new_eq += 1
                
                eq_params.append((
                    eq_id,
                    lab_id,
                    eq.get("code"),
                    eq.get("name"),
                    eq.get("brand"),
                    eq.get("model"),
                    eq.get("manufacturer"),
                    eq.get("situation"),
                    eq.get("equipmentSituation"),
                    1 if eq.get("replicated") else 0,
                    1 if eq.get("isEditor") else 0,
                    now_str,
                    now_str
                ))

            if eq_params:
                cursor.executemany(upsert_eq_sql, eq_params)

        conn.commit()

    return new_eq, updated_eq


def log_scraping_run(
    job_type: str,
    total_extracted: int,
    new_records: int,
    updated_records: int,
    status: str,
    duration_seconds: float,
    error_message: str = None,
    db_path=DATABASE_PATH
) -> None:
    """Registra o histórico de execução na tabela scraping_runs."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scraping_runs (
                job_type,
                total_extracted,
                new_records,
                updated_records,
                status,
                duration_seconds,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (
            job_type,
            total_extracted,
            new_records,
            updated_records,
            status,
            round(duration_seconds, 2),
            error_message
        ))
        conn.commit()


def export_consolidated_to_csv(csv_path=CONSOLIDATED_CSV_PATH, db_path=DATABASE_PATH) -> int:
    """Exporta a View consolidada (laboratórios + equipamentos) para CSV."""
    with get_connection(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_laboratorios_equipamentos;", conn)
        df.to_csv(csv_path, index=False, encoding="utf-8")
        return len(df)


def get_database_stats(db_path=DATABASE_PATH) -> Dict[str, Any]:
    """Retorna estatísticas gerais do banco de dados SQLite."""
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM laboratorios;")
        total_labs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM laboratorios WHERE details_synced_at IS NOT NULL;")
        enriched_labs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM laboratorios WHERE details_synced_at IS NULL;")
        pending_labs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM equipamentos;")
        total_equipments = cursor.fetchone()[0]
        
        return {
            "total_laboratorios": total_labs,
            "laboratorios_enriquecidos": enriched_labs,
            "laboratorios_pendentes": pending_labs,
            "total_equipamentos": total_equipments
        }
