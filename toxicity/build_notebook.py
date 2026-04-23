"""Generate lung_cancer_ppl_toxicity.ipynb from this script.

Run: `python build_notebook.py`. Idempotent — overwrites the .ipynb each time.
"""
from __future__ import annotations
import nbformat as nbf
from pathlib import Path

CELLS = []


def md(src: str):
    CELLS.append(nbf.v4.new_markdown_cell(src))


def code(src: str):
    CELLS.append(nbf.v4.new_code_cell(src))


# ---------------------------------------------------------------------------
# Cell 1: Title markdown
# ---------------------------------------------------------------------------
md("""\
# Lung Cancer Population-Level Toxicity Pipeline

Notebook orchestrator. Runs on Colab (L4 GPU) with runtime-cloned code.
Set `REPO_URL` below to your GitHub fork before running cell 1.
All deterministic logic lives in `lib/`; cells below are thin orchestration.""")

# ---------------------------------------------------------------------------
# Cell 2: Colab setup (code)
# ---------------------------------------------------------------------------
code("""\
import os, sys, subprocess
IN_COLAB = "google.colab" in sys.modules
REPO_URL = "https://github.com/Postlytllp/ct-fda-data-pipeline.git"
REPO_DIR = "/content/ct-fda-data-pipeline" if IN_COLAB else os.getcwd()
TOX_DIR = f"{REPO_DIR}/toxicity"
if IN_COLAB and not os.path.exists(REPO_DIR):
    subprocess.run(["git", "clone", REPO_URL, REPO_DIR], check=True)  # git clone
    subprocess.run(["pip", "install", "-q",
                    "rapidfuzz", "beautifulsoup4", "lxml",
                    "pandas", "tqdm", "sqlalchemy", "nbformat"], check=True)
os.chdir(TOX_DIR)
if TOX_DIR not in sys.path:
    sys.path.insert(0, TOX_DIR)
print(f"Working from: {TOX_DIR}")
print(f"Colab runtime: {IN_COLAB}")\
""")

# ---------------------------------------------------------------------------
# Cell 3: Setup ensure-dirs (code)
# ---------------------------------------------------------------------------
code("""\
import time, json
import pandas as pd
from lib import config
from lib.config import ensure_dirs
ensure_dirs()
t0 = time.time()\
""")

# ---------------------------------------------------------------------------
# Cell 4: Phase 1 markdown
# ---------------------------------------------------------------------------
md("## Phase 1 — Lung cancer drug list")

# ---------------------------------------------------------------------------
# Cell 5: Phase 1 code (scrape + harmonize + merge)
# ---------------------------------------------------------------------------
code("""\
import requests
from datetime import date
from lib.nci_scraper import parse_nci_drug_page
from lib.drug_list import merge_drugs, attach_subtypes, to_dataframe

ua = {"User-Agent": "toxicity-pipeline/1.0"}
# NCI merged NSCLC + SCLC into one URL; we read it once and split by subtype
html = requests.get(config.NCI_NSCLC_URL, timeout=30, headers=ua).text
nsclc = parse_nci_drug_page(html, subtype="NSCLC")
sclc  = parse_nci_drug_page(html, subtype="SCLC")
merged = attach_subtypes(merge_drugs(nsclc, sclc), nsclc, sclc)
print(f"merged drugs: {len(merged)}  (NSCLC: {len(nsclc)}  SCLC: {len(sclc)})")

# Harmonize via the existing project module
sys.path.insert(0, str(config.TOX_ROOT.parent))  # data_pipeline is the package root
from medical_libraries.drug_harmonizer import DrugHarmonizer
h = DrugHarmonizer(use_rxnorm=True, use_openfda=True, use_unii=True)
generics = [e["name"] for e in merged if e["kind"] == "generic"]
print(f"harmonizing {len(generics)} generics via RxNorm+OpenFDA+UNII...")
harmonized_objs = h.harmonize_drug_list(generics)
harmonized = {
    name: {
        "rxcui": getattr(info, "rxcui", None),
        "rxnorm_generic_name": getattr(info, "rxnorm_generic_name", None),
        "all_brand_names": sorted(list(getattr(info, "all_brand_names", set()) or set())),
        "all_synonyms": sorted(list(getattr(info, "all_synonyms", set()) or set())),
    }
    for name, info in harmonized_objs.items()
}
drugs_df = to_dataframe(merged, harmonized, snapshot_date=str(date.today()))
drugs_df.to_csv(config.OUTPUTS_DIR / "lung_cancer_drugs.csv", index=False)
drugs_df.head()\
""")

# ---------------------------------------------------------------------------
# Cell 6: Phase 2 markdown
# ---------------------------------------------------------------------------
md("## Phase 2 — CT.gov bulk fetch")

# ---------------------------------------------------------------------------
# Cell 7: Phase 2 code
# ---------------------------------------------------------------------------
code("""\
from lib.ctgov_query import build_essie_or, split_aliases_by_url_budget
from lib.ctgov_client import fetch_all_pages
from lib.config import CTGOV_MAX_URL_BYTES, CTGOV_PAGE_SIZE, RAW_DIR

all_aliases = []
for _, row in drugs_df.iterrows():
    all_aliases.extend(json.loads(row["aliases"]))
all_aliases = sorted(set(all_aliases))
batches = split_aliases_by_url_budget(all_aliases, max_bytes=CTGOV_MAX_URL_BYTES)
print(f"{len(all_aliases)} aliases in {len(batches)} Essie OR batches")

fetch_stats = []
for i, batch in enumerate(batches, start=1):
    batch_dir = RAW_DIR / f"batch_{i:02d}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    essie = build_essie_or(batch)
    stats = fetch_all_pages(essie, cache_dir=batch_dir, page_size=CTGOV_PAGE_SIZE, sleep_s=0.5)
    fetch_stats.append(stats)
print("fetch_stats:", fetch_stats)\
""")

# ---------------------------------------------------------------------------
# Cell 8: Phase 3 markdown
# ---------------------------------------------------------------------------
md("## Phase 3 — Parse to base tables")

# ---------------------------------------------------------------------------
# Cell 9: Phase 3 code
# ---------------------------------------------------------------------------
code("""\
from lib.parsers import parse_trials, parse_arms, parse_arm_interventions
from lib.baseline_ae_parsers import parse_baseline_raw, parse_ae_raw, annotate_regimen_on_arms
from lib.arm_resolver import resolve_arm_labels

drug_class_df = pd.read_csv(config.DRUG_CLASS_CSV)

def iter_studies():
    for page_file in sorted(RAW_DIR.rglob("page_*.json")):
        data = json.loads(page_file.read_text(encoding="utf-8"))
        for s in data.get("studies", []):
            yield s

studies = list(iter_studies())
# dedupe by nctId
seen = set(); unique = []
for s in studies:
    nct = s.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
    if nct and nct not in seen:
        seen.add(nct); unique.append(s)
studies = unique
print(f"Unique studies after dedupe: {len(studies)}")

trials_df = parse_trials(studies)
arms_df = parse_arms(studies)
ai_df = parse_arm_interventions(studies, drugs_df, drug_class_df)
baseline_df = parse_baseline_raw(studies)
ae_df = parse_ae_raw(studies)
arms_df = annotate_regimen_on_arms(arms_df, ai_df)

# Resolve arm labels across modules
import pandas as pd
from tqdm.auto import tqdm
match_rows = []
for nct, arm_sub in tqdm(arms_df.groupby("nct_id"), desc="arm-resolve", leave=False):
    ae_titles = ae_df.loc[ae_df.nct_id == nct, "arm_label"].dropna().unique().tolist()
    resolutions = resolve_arm_labels(arm_sub["arm_label"].tolist(), ae_titles)
    for arm_label, info in resolutions.items():
        match_rows.append({"nct_id": nct, "arm_label": arm_label, **info})
match_df = pd.DataFrame(match_rows)
arms_df = arms_df.drop(columns=[c for c in ("match_method", "fuzzy_score", "arm_match_status")
                                if c in arms_df.columns])
arms_df = arms_df.merge(match_df, on=["nct_id", "arm_label"], how="left")
arms_df.head()\
""")

# ---------------------------------------------------------------------------
# Cell 10: Phase 4 markdown
# ---------------------------------------------------------------------------
md("## Phase 4 — Demographic cascade (Tier A → D + optional LLM for Tier B2)")

# ---------------------------------------------------------------------------
# Cell 11: Phase 4 LLM setup (code, Colab L4 specific)
# ---------------------------------------------------------------------------
code("""\
# LLM backend for Tier B2 (optional). On Colab L4 this loads Gemma 4 26B
# (Q5_K_M GGUF) via llama-cpp-python. Set LLM_ENABLED=False to skip.
LLM_ENABLED = IN_COLAB  # auto-enable on Colab, skip locally

llm_callable = None
if LLM_ENABLED:
    try:
        from llama_cpp import Llama  # type: ignore
        print("Loading Gemma 4 26B Q5_K_M (this takes 2-3 min on L4)...")
        # Model URL placeholder; user should replace with actual HF or local path
        GEMMA_MODEL_PATH = "/content/models/gemma-4-26b-q5_k_m.gguf"
        if not os.path.exists(GEMMA_MODEL_PATH):
            print("Model not found at", GEMMA_MODEL_PATH, "- skipping LLM backend")
        else:
            gemma = Llama(
                model_path=GEMMA_MODEL_PATH,
                n_gpu_layers=-1,
                n_ctx=4096,
                verbose=False,  # silence per-call llama_perf_context_print spam
            )
            from lib.llm_client import PopulationHit, B2_SYSTEM_PROMPT
            def llm_callable(text: str) -> PopulationHit:
                prompt = f"{B2_SYSTEM_PROMPT}\\n\\nCRITERIA:\\n{text}\\n\\nJSON output (one line):\\n"
                # Tighter stop set: model must emit one JSON object and stop.
                # "}" alone stops generation when the JSON closes, preventing 300-token ceiling hits.
                resp = gemma(
                    prompt,
                    max_tokens=200,
                    stop=["\\n\\n", "```", "</s>", "<end_of_turn>"],
                    temperature=0.0,
                )
                raw = resp["choices"][0]["text"].strip()
                try:
                    data = json.loads(raw)
                except Exception:
                    return PopulationHit(False, None, False, "", f"parse error: {raw[:100]}")
                return PopulationHit(
                    has_population_restriction=bool(data.get("has_population_restriction")),
                    population=data.get("population"),
                    is_inclusion_criterion=bool(data.get("is_inclusion_criterion")),
                    evidence_span=str(data.get("evidence_span") or ""),
                    reasoning=str(data.get("reasoning") or ""),
                )
    except ImportError:
        print("llama-cpp-python not installed; install via `!pip install llama-cpp-python[cuda]` if wanted. Proceeding without Tier B2 LLM.")\
""")

# ---------------------------------------------------------------------------
# Cell 12: Phase 4 cascade (code)
# ---------------------------------------------------------------------------
code("""\
from lib.demog_tier_a import load_monoethnic_countries
from lib.demog_cascade import run_cascade
from lib.biomarker_signal import detect_biomarker_signal
from lib.llm_client import LLMCache

monoeth_df = load_monoethnic_countries(config.MONOETHNIC_CSV)
llm_cache = LLMCache(config.LLM_CACHE_FILE)

trial_dicts = []
for _, t in trials_df.iterrows():
    trial_dicts.append({
        "nct_id": t["nct_id"],
        "brief_title": t["brief_title"],
        "official_title": t["official_title"],
        "detailed_description": t["detailed_description"],
        "eligibility_criteria_text": t["eligibility_criteria_text"],
        "site_countries": json.loads(t["countries"] or "[]"),
        "lead_sponsor_country": t.get("lead_sponsor_country"),
        "secondary_ids": json.loads(t["secondary_ids"] or "[]"),
    })

demog_df = run_cascade(
    trials=trial_dicts, baseline_df=baseline_df, monoeth_df=monoeth_df,
    llm_client=llm_callable, llm_cache=llm_cache,
)
demog_df["biomarker_indirect_signal"] = demog_df["nct_id"].map({
    t["nct_id"]: detect_biomarker_signal(t["eligibility_criteria_text"] or "")
    for t in trial_dicts
})
print("demog tier counts:", demog_df["demog_tier"].value_counts().to_dict())
demog_df.head()\
""")

# ---------------------------------------------------------------------------
# Cell 13: Phase 5 markdown
# ---------------------------------------------------------------------------
md("## Phase 5 — Filters")

# ---------------------------------------------------------------------------
# Cell 14: Phase 5 code
# ---------------------------------------------------------------------------
code("""\
from lib.filters import (add_has_any_ae_flag, add_passes_diversity_flag,
                         add_has_lung_cancer_drug_match_flag)
from lib.ae_summary import build_ae_arm_summary, build_ae_long

ae_summary = build_ae_arm_summary(ae_df)
arms_df = add_has_any_ae_flag(arms_df, ae_summary)
arms_df = add_has_lung_cancer_drug_match_flag(arms_df, ai_df)
demog_df = add_passes_diversity_flag(demog_df)
print("arms passing has_any_ae:", arms_df.has_any_ae.sum())
print("arms passing drug_match:", arms_df.has_lung_cancer_drug_match.sum())
print("demog passing diversity:", demog_df.passes_diversity.sum())\
""")

# ---------------------------------------------------------------------------
# Cell 15: Phase 6 markdown
# ---------------------------------------------------------------------------
md("## Phase 6 — Join + outputs")

# ---------------------------------------------------------------------------
# Cell 16: Phase 6 code
# ---------------------------------------------------------------------------
code("""\
from lib.storage import write_csvs, write_sqlite, build_cohort_view
from lib.run_summary import build_run_summary

ae_long = build_ae_long(ae_df, arms_df[["nct_id", "arm_label"]])

# aggregate primary oncology + backbone drug lists per (nct, arm)
def _join_names(idx_mask, names):
    return "|".join(sorted(set(n for n, m in zip(names, idx_mask) if m)))

ai_agg_rows = []
for (nct, arm), grp in ai_df.groupby(["nct_id", "arm_label"]):
    ai_agg_rows.append({
        "nct_id": nct, "arm_label": arm,
        "primary_oncology_drugs": "|".join(sorted(set(
            grp.loc[grp["is_primary_oncology"] == True, "intervention_name"].dropna().astype(str)
        ))),
        "backbone_drugs": "|".join(sorted(set(
            grp.loc[grp["drug_class"] == "chemo_backbone", "intervention_name"].dropna().astype(str)
        ))),
    })
ai_agg = pd.DataFrame(ai_agg_rows)

cohort_df = build_cohort_view(trials_df, arms_df, demog_df, ai_agg, ae_long)
print(f"cohort rows: {len(cohort_df)}")

out_dfs = {
    "trials": trials_df, "arms": arms_df, "arm_interventions": ai_df,
    "demographics": demog_df, "ae_long": ae_long,
    "ae_arm_summary": ae_summary,
    "lung_cancer_toxicity_cohort": cohort_df,
}
write_csvs(out_dfs, config.OUTPUTS_DIR)
write_sqlite(out_dfs, config.OUTPUTS_DIR / "lung_cancer_toxicity.db")

from datetime import date
summary = build_run_summary(
    drugs_queried=len(drugs_df), trials=trials_df, arms=arms_df,
    demographics=demog_df, baseline_df=baseline_df, ae_df=ae_df,
    llm_calls_made=0, llm_cache_hits=len(llm_cache._by_nct),
    total_runtime_seconds=time.time() - t0,
)
(config.OUTPUTS_DIR / f"run_summary_{date.today().strftime('%Y%m%d')}.json").write_text(
    json.dumps(summary, indent=2), encoding="utf-8"
)
print(json.dumps(summary, indent=2))\
""")

# ---------------------------------------------------------------------------
# Build and write notebook
# ---------------------------------------------------------------------------
nb = nbf.v4.new_notebook()
nb.cells = CELLS
out = Path(__file__).parent / "lung_cancer_ppl_toxicity.ipynb"
nbf.write(nb, out)

md_count = sum(1 for c in CELLS if c["cell_type"] == "markdown")
code_count = sum(1 for c in CELLS if c["cell_type"] == "code")
print(f"wrote {out}")
print(f"total cells: {len(CELLS)}  ({md_count} markdown + {code_count} code)")
