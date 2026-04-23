"""Central paths + tunable constants for the toxicity pipeline."""
from pathlib import Path

TOX_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = TOX_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LLM_CACHE_DIR = DATA_DIR / "llm_cache"
REFERENCE_DIR = DATA_DIR / "reference"
OUTPUTS_DIR = DATA_DIR / "outputs"

NCI_NSCLC_URL = "https://www.cancer.gov/about-cancer/treatment/drugs/lung"
NCI_SCLC_URL = "https://www.cancer.gov/about-cancer/treatment/drugs/small-cell-lung"

CTGOV_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
CTGOV_PAGE_SIZE = 200
CTGOV_MAX_URL_BYTES = 2500  # safe budget for Essie OR expression (URL-encoding inflation ~3-4x)

DIVERSITY_THRESHOLD = 0.95
FUZZY_ARM_THRESHOLD = 90

DEMOG_TIERS = ("A1", "A1-trial", "A2", "B1", "B2", "C1", "C2", "C3", "D1", "D2", "NONE")

MONOETHNIC_CSV = REFERENCE_DIR / "monoethnic_countries.csv"
DRUG_CLASS_CSV = REFERENCE_DIR / "drug_class_lookup.csv"
LLM_CACHE_FILE = LLM_CACHE_DIR / "tier_b_results.jsonl"

def ensure_dirs() -> None:
    for d in (RAW_DIR, LLM_CACHE_DIR, REFERENCE_DIR, OUTPUTS_DIR):
        d.mkdir(parents=True, exist_ok=True)
