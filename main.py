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
    Executa o pipeline:
    1. Primeira execução: extrai tudo (listagem + detalhes + equipamentos de todos os laboratórios).
    2. Execuções seguintes: consulta a listagem, identifica os idAssets que NÃO estão na base,
       e para estes novos, executa a raspagem completa (detalhes + equipamentos) e incrementa a base.
    """
    logger.info("==================================================")
    logger.info("   PIPELINE PNIPE/MCTI (LABORATÓRIOS E EQUIPAMENTOS)   ")
    logger.info("==================================================")
    
    start_time = time.time()
    init_db(DATABASE_PATH)

    # Verifica o estado atual da base antes da raspagem
    initial_stats = get_database_stats(DATABASE_PATH)
    is_first_run = (initial_stats["laboratorios_enriquecidos"] == 0)

    if is_first_run:
        logger.info("ℹ️ PRIMEIRA EXECUÇÃO DETECTADA: O pipeline irá extrair todos os dados completos (laboratórios e equipamentos).")
    else:
        logger.info(f"ℹ️ BASE EXISTENTE DETECTADA ({initial_stats['total_laboratorios']} laboratórios, {initial_stats['total_equipamentos']} equipamentos).")
        logger.info("   O pipeline fará a consulta rápida e apenas raspará detalhes/equipamentos de idAssets NOVOS.")

    # ----------------------------------------------------
    # ETAPA 1: Consulta da Listagem de Laboratórios
    # ----------------------------------------------------
    logger.info("\n--- [ETAPA 1/2] Sincronização da Lista de Laboratórios ---")
    new_lab_ids: List[int] = []
    upd_labs_count = 0
    try:
        lab_records = fetch_pnipe_laboratorios(max_pages=max_pages, page_size=PAGE_SIZE)
        new_lab_ids, upd_labs_count = upsert_laboratorios_base(lab_records, DATABASE_PATH)
        
        logger.info(f"Resultado da listagem: {len(new_lab_ids)} novos idAssets encontrados | {upd_labs_count} existentes atualizados.")
        
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
    # ETAPA 2: Detalhes e Equipamentos
    # ----------------------------------------------------
    total_enriched = 0
    new_eq, upd_eq = 0, 0
    
    if not skip_enrichment:
        logger.info("\n--- [ETAPA 2/2] Raspagem de Detalhes e Equipamentos ---")
        
        if force_all or is_first_run:
            # Na primeira execução (ou com --all), processa todos os laboratórios pendentes
            target_ids = get_pending_enrichment_lab_ids(DATABASE_PATH)
            if not target_ids and force_all:
                target_ids = get_all_lab_ids(DATABASE_PATH)
            logger.info(f"Modo carga completa/inicial: {len(target_ids)} laboratórios selecionados para raspagem completa de equipamentos.")
        else:
            # Nas execuções seguintes, processa APENAS os novos idAssets que acabaram de ser descobertos
            target_ids = new_lab_ids
            if target_ids:
                logger.info(f"⚡ {len(target_ids)} NOVO(S) idAsset(s) identificado(s)! Iniciando raspagem de detalhes e equipamentos...")
            else:
                logger.info("✅ Nenhum novo idAsset encontrado na API. Todos os laboratórios já estão salvos e enriquecidos na base.")

        if target_ids:
            if max_enrich is not None:
                target_ids = target_ids[:max_enrich]
                logger.info(f"Limitando aos primeiros {len(target_ids)} laboratórios (--max-enrich).")

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
    final_stats = get_database_stats(DATABASE_PATH)

    # Registrar execução no banco
    log_scraping_run(
        job_type="INITIAL_FULL_SYNC" if is_first_run else "INCREMENTAL_SYNC",
        total_extracted=final_stats["total_laboratorios"],
        new_records=len(new_lab_ids) + new_eq,
        updated_records=upd_labs_count + upd_eq,
        status="SUCCESS",
        duration_seconds=duration,
        error_message=None
    )

    logger.info("\n==================================================")
    logger.info("            RESUMO FINAL DO BANCO SQL             ")
    logger.info("==================================================")
    logger.info(f"Total de Laboratórios no Banco: {final_stats['total_laboratorios']}")
    logger.info(f"Laboratórios com Detalhes/Equipamentos Salvos: {final_stats['laboratorios_enriquecidos']}")
    logger.info(f"Total de Equipamentos no Banco: {final_stats['total_equipamentos']}")
    logger.info(f"Novos laboratórios inseridos nesta execução: {len(new_lab_ids)}")
    logger.info(f"Novos equipamentos inseridos nesta execução: {new_eq}")
    logger.info(f"Duração total: {duration:.2f} segundos ({duration/60:.2f} min)")
    logger.info(f"Banco SQLite: {DATABASE_PATH}")
    logger.info("==================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automação PNIPE/MCTI: Carga Inicial Completa e Incremento Mensal por idAsset")
    parser.add_argument("--test", action="store_true", help="Modo teste rápido: 2 páginas e 10 laboratórios")
    parser.add_argument("--max-pages", type=int, default=None, help="Limite de páginas na listagem")
    parser.add_argument("--max-enrich", type=int, default=None, help="Limite de laboratórios para enriquecer")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Número de threads simultâneas")
    parser.add_argument("--all", action="store_true", help="Força a re-raspagem de todos os laboratórios do banco")
    parser.add_argument("--skip-enrichment", action="store_true", help="Pula a etapa de detalhes e equipamentos")
    parser.add_argument("--no-csv", action="store_true", help="Não gera arquivos CSV")

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
