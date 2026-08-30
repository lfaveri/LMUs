from pathlib import Path

# Caminhos do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATABASE_PATH = DATA_DIR / "laboratorios_pnipe.db"
LABS_CSV_PATH = DATA_DIR / "laboratorios_pnipe.csv"
CONSOLIDATED_CSV_PATH = DATA_DIR / "laboratorios_e_equipamentos_pnipe.csv"

# Garantir existência do diretório de dados
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configurações da API PNIPE
API_BASE_URL = "https://api.pnipe.mcti.gov.br/assets/lab"
API_LAB_DETAIL_URL = "https://api.pnipe.mcti.gov.br/assets/laboratory"

PAGE_SIZE = 96
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_FACTOR = 0.5
DELAY_BETWEEN_PAGES = 0.4

# Configurações para scraping de detalhes e equipamentos
DEFAULT_WORKERS = 6
EQUIPMENTS_PAGE_SIZE = 1000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:154.0) Gecko/20100101 Firefox/154.0",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://pnipe.mcti.gov.br",
    "Referer": "https://pnipe.mcti.gov.br/",
}
