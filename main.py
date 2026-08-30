import argparse
import logging
import time
from typing import List
import pandas as pd

from src.config import (
    DATABASE_PATH,
    LABS_CSV_PATH,
    CONSOLIDATED_CSV_PATH,
    PAGE_SIZE,
    DEFAULT_WORKERS
)
from src.database import (
    init_db,
    upsert_laboratorios_base,
    get_all_lab_ids,
    get_pending_enrichment_lab_ids,
    export_consolidated_to_csv,
    log_scraping_run,
    get_database_stats,
)
from src.scraper import (
    fetch_pnipe_laboratorios,
    enrich_laboratories,
)

logger = logging.getLogger("pnipe_pipeline")


def run_pipeline(
    max_pages=None,
    max_enrich=None,
    workers=DEFAULT_WORKERS,
    force_all=False,
    skip_enrichment=False,
    export_csv=True
) -> None:
    """
    Executa o pipeline inteligente:
    1. Varre a listagem de laboratórios na API.
    2. Identifica automaticamente qualquer NOVO idAsset ou laboratórios com enriquecimento pendente.
    3. Executa a raspagem dos detalhes e equipamentos apenas dos novos/pendentes (ou de todos com --all).
    4. Salva e incrementa os dados no banco SQLite local e atualiza CSVs.
    """
    logger.info("==================================================")
    logger.info("   PIPELINE INCREMENTAL PNIPE/MCTI (SQL LOCAL)   ")
    logger.info("==================================================")
    
    start_time = time.time()
    init_db(DATABASE_PATH)

    # ----------------------------------------------------
    # ETAPA 1: Raspagem da Listagem de Laboratórios
    # ----------------------------------------------------
    logger.info("\n--- [ETAPA 1/2] Sincronização da Lista de Laboratórios ---")
    new_lab_ids: List[int] = []
    upd_labs_count = 0
    try:
        lab_records = fetch_pnipe_laboratorios(max_pages=max_pages, page_size=PAGE_SIZE)
        new_lab_ids, upd_labs_count = upsert_laboratorios_base(lab_records, DATABASE_PATH)
        
        logger.info(f"Resultado da listagem: {len(new_lab_ids)} novos laboratórios encontrados | {upd_labs_count} existentes atualizados.")
        
        # Atualiza CSV simples de laboratórios
        if export_csv and lab_records:
            df_labs = pd.DataFrame(lab_records)
            cols_desejadas = [
                'idAsset', 'name', 'initials', 'institutionName', 
                'institutionInitials', 'city', 'state', 'hasSharing', 'about'
            ]
            cols = [c for c in cols_desejadas if c in df_labs.columns]
            df_labs[cols].to_csv(LABS_CSV_PATH, index=False, encoding="utf-8")
            logger.info(f"CSV de laboratórios atualizado em: {LABS_CSV_PATH}")

    except Exception as e:
        logger.error(f"Erro durante a Etapa 1: {e}", exc_info=True)
        log_scraping_run("LIST_SYNC", 0, 0, 0, "FAILED", time.time() - start_time, str(e))
        raise

    # ----------------------------------------------------
    # ETAPA 2: Detalhes e Equipamentos (Incremental Automático)
    # ----------------------------------------------------
    total_enriched = 0
    new_eq, upd_eq = 0, 0
    
    if not skip_enrichment:
        logger.info("\n--- [ETAPA 2/2] Raspagem de Detalhes e Equipamentos ---")
        
        if force_all:
            # Força o enriquecimento de todos os laboratórios do banco
            target_ids = get_all_lab_ids(DATABASE_PATH)
            logger.info(f"Modo --all ativado: {len(target_ids)} laboratórios serão enriquecidos.")
        else:
            # Modo padrão: novos IDs encontrados + quaisquer pendentes de execuções anteriores
            pending_ids = get_pending_enrichment_lab_ids(DATABASE_PATH)
            target_ids = list(dict.fromkeys(new_lab_ids + pending_ids))
            
            if target_ids:
                logger.info(
                    f"Alvos para enriquecimento: {len(target_ids)} laboratórios "
                    f"({len(new_lab_ids)} novos idAssets + {len(pending_ids) - len(new_lab_ids)} pendentes)."
                )
            else:
                logger.info("Nenhum novo laboratório detectado e nenhum pendente de enriquecimento.")

        if target_ids:
            if max_enrich is not None:
                target_ids = target_ids[:max_enrich]
                logger.info(f"Limitando enriquecimento aos primeiros {len(target_ids)} laboratórios (--max-enrich).")

            try:
                total_enriched, new_eq, upd_eq = enrich_laboratories(
                    lab_ids=target_ids,
                    max_workers=workers
                )
            except Exception as e:
                logger.error(f"Erro durante a Etapa 2: {e}", exc_info=True)
                log_scraping_run("ENRICH_SYNC", 0, 0, 0, "FAILED", time.time() - start_time, str(e))
                raise

        # Atualiza o CSV consolidado (View SQLite: Laboratórios + Equipamentos)
        if export_csv:
            total_linhas = export_consolidated_to_csv(CONSOLIDATED_CSV_PATH, DATABASE_PATH)
            logger.info(f"CSV consolidado (Laboratórios + Equipamentos) atualizado: {CONSOLIDATED_CSV_PATH} ({total_linhas} linhas)")

    else:
        logger.info("Etapa 2 ignorada (--skip-enrichment solicitado).")

    duration = time.time() - start_time
    stats = get_database_stats(DATABASE_PATH)

    # Registrar execução no banco
    log_scraping_run(
        job_type="INCREMENTAL_SYNC",
        total_extracted=stats["total_laboratorios"],
        new_records=len(new_lab_ids) + new_eq,
        updated_records=upd_labs_count + upd_eq,
        status="SUCCESS",
        duration_seconds=duration,
        error_message=None
    )

    logger.info("\n==================================================")
    logger.info("            RESUMO FINAL DO BANCO SQL             ")
    logger.info("==================================================")
    logger.info(f"Total de Laboratórios no Banco: {stats['total_laboratorios']}")
    logger.info(f"Laboratórios com Detalhes/Equipamentos Sincronizados: {stats['laboratorios_enriquecidos']}")
    logger.info(f"Laboratórios Pendentes: {stats['laboratorios_pendentes']}")
    logger.info(f"Total de Equipamentos Registrados: {stats['total_equipamentos']}")
    logger.info(f"Novos laboratórios adicionados nesta execução: {len(new_lab_ids)}")
    logger.info(f"Novos equipamentos adicionados nesta execução: {new_eq}")
    logger.info(f"Duração total: {duration:.2f} segundos ({duration/60:.2f} min)")
    logger.info(f"Banco SQLite salvo em: {DATABASE_PATH}")
    logger.info("==================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automação de Scraping Incremental PNIPE/MCTI para SQLite")
    parser.add_argument("--test", action="store_true", help="Modo teste: extrai 2 páginas e enriquece 10 laboratórios")
    parser.add_argument("--max-pages", type=int, default=None, help="Limite de páginas na etapa 1")
    parser.add_argument("--max-enrich", type=int, default=None, help="Limite de laboratórios para enriquecer na etapa 2")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Número de threads simultâneas para enriquecimento")
    parser.add_argument("--all", action="store_true", help="Força a atualização de detalhes e equipamentos de TODOS os laboratórios do banco")
    parser.add_argument("--skip-enrichment", action="store_true", help="Pula a etapa de detalhes e equipamentos")
    parser.add_argument("--no-csv", action="store_true", help="Não gera arquivos CSV auxiliares")

    args = parser.parse_args()

    if args.test:
        run_pipeline(
            max_pages=2,
            max_enrich=10,
            workers=4,
            export_csv=not args.no_csv
        )
    else:
        run_pipeline(
            max_pages=args.max_pages,
            max_enrich=args.max_enrich,
            workers=args.workers,
            force_all=args.all,
            skip_enrichment=args.skip_enrichment,
            export_csv=not args.no_csv
        )
