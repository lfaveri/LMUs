import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.config import (
    API_BASE_URL,
    API_LAB_DETAIL_URL,
    PAGE_SIZE,
    EQUIPMENTS_PAGE_SIZE,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    DELAY_BETWEEN_PAGES,
    DEFAULT_WORKERS,
    HEADERS,
)
from src.database import upsert_lab_enrichment_and_equipments

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def create_resilient_session() -> requests.Session:
    """Cria uma sessão requests com retries automáticos e headers padronizados."""
    session = requests.Session()
    session.headers.update(HEADERS)

    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# =====================================================================
# ETAPA 1: Raspagem da Listagem de Laboratórios
# =====================================================================

def fetch_pnipe_laboratorios(
    max_pages: Optional[int] = None,
    page_size: int = PAGE_SIZE,
    delay: float = DELAY_BETWEEN_PAGES
) -> List[Dict[str, Any]]:
    """
    Executa a extração completa da lista de laboratórios da API do PNIPE/MCTI.
    Percorre a paginação dinamicamente até a última página.
    """
    session = create_resilient_session()
    all_labs = []
    page = 0
    total_pages = None

    logger.info("Iniciando extração da listagem de laboratórios do PNIPE/MCTI...")

    while True:
        if max_pages is not None and page >= max_pages:
            logger.info(f"Limite de {max_pages} páginas atingido (modo teste/parcial).")
            break

        url = f"{API_BASE_URL}?page={page}&size={page_size}"
        try:
            response = session.post(url, data="{}", timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao requisitar página {page}: {e}")
            raise

        if not isinstance(data, dict):
            logger.warning(f"Resposta inesperada na página {page}. Encerrando.")
            break

        content = data.get("content", [])
        if not content:
            logger.info(f"Nenhum conteúdo retornado na página {page}. Fim da extração.")
            break

        all_labs.extend(content)

        if total_pages is None and "totalPages" in data:
            total_pages = data["totalPages"]
            total_elements = data.get("totalElements", "desconhecido")
            logger.info(f"Total informado pela API: {total_elements} registros em {total_pages} páginas.")

        is_last = data.get("last", False)
        page_info = f"{page + 1}/{total_pages}" if total_pages else f"{page + 1}"
        logger.info(f"Página {page_info} processada (+{len(content)} registros | Total acumulado: {len(all_labs)})")

        if is_last:
            logger.info("Última página alcançada segundo a API.")
            break

        page += 1
        time.sleep(delay)

    logger.info(f"Extração da listagem concluída! Total de laboratórios obtidos: {len(all_labs)}")
    return all_labs


# =====================================================================
# ETAPA 2: Raspagem de Detalhes e Equipamentos de Cada Laboratório
# =====================================================================

def fetch_single_lab_detail_and_equipments(
    lab_id: int,
    session: Optional[requests.Session] = None
) -> Tuple[int, Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Coleta os detalhes completos e a lista de equipamentos de um laboratório específico.
    """
    sess = session or create_resilient_session()
    
    # 1. Requisição de Detalhes do Laboratório
    lab_detail = None
    url_lab = f"{API_LAB_DETAIL_URL}/{lab_id}"
    try:
        res_lab = sess.get(url_lab, timeout=REQUEST_TIMEOUT)
        if res_lab.status_code == 200:
            lab_detail = res_lab.json()
        else:
            logger.warning(f"Laboratório ID {lab_id}: Status {res_lab.status_code} ao buscar detalhes.")
    except Exception as e:
        logger.error(f"Erro ao buscar detalhes do laboratório {lab_id}: {e}")

    # 2. Requisição dos Equipamentos do Laboratório
    equipments = []
    url_eq = f"{API_LAB_DETAIL_URL}/{lab_id}/equipments?page=0&size={EQUIPMENTS_PAGE_SIZE}"
    try:
        res_eq = sess.get(url_eq, timeout=REQUEST_TIMEOUT)
        if res_eq.status_code == 200:
            eq_json = res_eq.json()
            if isinstance(eq_json, dict):
                equipments = eq_json.get("content", [])
            elif isinstance(eq_json, list):
                equipments = eq_json
    except Exception as e:
        logger.error(f"Erro ao buscar equipamentos do laboratório {lab_id}: {e}")

    return lab_id, lab_detail, equipments


def enrich_laboratories(
    lab_ids: List[int],
    max_workers: int = DEFAULT_WORKERS,
    batch_log_interval: int = 50
) -> Tuple[int, int, int]:
    """
    Executa o enriquecimento de detalhes e equipamentos de múltiplos laboratórios
    utilizando pool concorrente de threads e salvando diretamente no SQLite.
    Retorna (total_processados, total_equipamentos_novos, total_equipamentos_atualizados).
    """
    if not lab_ids:
        logger.info("Nenhum laboratório para enriquecer.")
        return 0, 0, 0

    total = len(lab_ids)
    logger.info(f"Iniciando enriquecimento de {total} laboratórios com {max_workers} threads...")
    
    session = create_resilient_session()
    processed_count = 0
    total_new_eq = 0
    total_upd_eq = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(fetch_single_lab_detail_and_equipments, lab_id, session): lab_id
            for lab_id in lab_ids
        }

        for future in as_completed(future_to_id):
            lab_id = future_to_id[future]
            try:
                lid, lab_detail, equipments = future.result()
                new_eq, upd_eq = upsert_lab_enrichment_and_equipments(lid, lab_detail, equipments)
                total_new_eq += new_eq
                total_upd_eq += upd_eq
            except Exception as e:
                logger.error(f"Erro ao processar e salvar dados do laboratório {lab_id}: {e}")

            processed_count += 1
            if processed_count % batch_log_interval == 0 or processed_count == total:
                logger.info(
                    f"Progresso: {processed_count}/{total} laboratórios enriquecidos "
                    f"({(processed_count / total) * 100:.1f}%) | Equipamentos acumulados: {total_new_eq + total_upd_eq}"
                )

    logger.info(f"Enriquecimento concluído! Processados: {processed_count} | Novos equipamentos: {total_new_eq} | Atualizados: {total_upd_eq}")
    return processed_count, total_new_eq, total_upd_eq
