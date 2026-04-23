import json
import pandas as pd
from pathlib import Path
from tests.conftest import FIXTURES
from lib.parsers import parse_trials, parse_arms, parse_arm_interventions
from lib.baseline_ae_parsers import parse_baseline_raw, parse_ae_raw, annotate_regimen_on_arms
from lib.demog_tier_a import load_monoethnic_countries
from lib.demog_cascade import run_cascade
from lib.biomarker_signal import detect_biomarker_signal
from lib.filters import (
    add_has_any_ae_flag, add_passes_diversity_flag, add_has_lung_cancer_drug_match_flag,
)
from lib.ae_summary import build_ae_arm_summary, build_ae_long
from lib.storage import build_cohort_view
from lib.run_summary import build_run_summary


def _studies():
    return json.loads((FIXTURES / "ctgov_page_001.json").read_text(encoding="utf-8"))["studies"]


def test_end_to_end_produces_nonempty_frames_and_runsummary(tmp_path):
    studies = _studies()
    drugs = pd.DataFrame([
        {"canonical_name": "tepotinib", "rxcui": "2049110",
         "aliases": '["tepotinib","Tepmetko"]'},
        {"canonical_name": "capmatinib", "rxcui": "2049111",
         "aliases": '["capmatinib","Tabrecta"]'},
    ])
    drug_class_df = pd.DataFrame([
        {"rxcui": "2049110", "generic_name": "tepotinib", "drug_class": "targeted_oncology"},
        {"rxcui": "2049111", "generic_name": "capmatinib", "drug_class": "targeted_oncology"},
    ])
    monoeth = load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")

    trials = parse_trials(studies)
    arms = parse_arms(studies)
    ai = parse_arm_interventions(studies, drugs, drug_class_df)
    baseline = parse_baseline_raw(studies)
    ae = parse_ae_raw(studies)
    arms = annotate_regimen_on_arms(arms, ai)

    trial_dicts = []
    for _, t in trials.iterrows():
        trial_dicts.append({
            "nct_id": t["nct_id"],
            "brief_title": t["brief_title"],
            "official_title": t["official_title"],
            "detailed_description": t["detailed_description"],
            "eligibility_criteria_text": t["eligibility_criteria_text"],
            "site_countries": json.loads(t["countries"] or "[]"),
            "lead_sponsor_country": None,
            "secondary_ids": json.loads(t["secondary_ids"] or "[]"),
        })
    demog = run_cascade(trial_dicts, baseline, monoeth, llm_client=None, llm_cache=None)
    demog["biomarker_indirect_signal"] = None

    ae_summary = build_ae_arm_summary(ae)
    arms = add_has_any_ae_flag(arms, ae_summary)
    arms = add_has_lung_cancer_drug_match_flag(arms, ai)
    demog = add_passes_diversity_flag(demog)
    ae_long = build_ae_long(ae, arms[["nct_id", "arm_label"]])
    # minimal ai_agg for the cohort view
    ai_agg = (ai.groupby(["nct_id", "arm_label"])["intervention_name"]
                .apply(lambda s: "|".join(sorted(set(s.dropna().astype(str)))))
                .reset_index(name="primary_oncology_drugs"))
    ai_agg["backbone_drugs"] = ""
    cohort = build_cohort_view(trials, arms, demog, ai_agg, ae_long)
    summary = build_run_summary(
        drugs_queried=2, trials=trials, arms=arms, demographics=demog,
        baseline_df=baseline, ae_df=ae, llm_calls_made=0, llm_cache_hits=0,
        total_runtime_seconds=0.0,
    )
    # Assertions: nothing crashed; frames have expected semantic shape
    assert len(trials) > 0
    assert len(arms) > 0
    assert {"has_any_ae", "has_lung_cancer_drug_match"}.issubset(arms.columns)
    assert "passes_diversity" in demog.columns
    assert summary["arms_total"] == len(arms)
    # cohort may be empty for this fixture (diverse-country trials) — that's OK,
    # but the view builder must at least run without error:
    assert hasattr(cohort, "columns")

    # PK uniqueness: arm_label must uniquely identify an arm within an NCT
    dupes = arms.groupby(["nct_id", "arm_label"]).size()
    assert (dupes == 1).all(), (
        f"arm_label PK collision in arms_df: {dupes[dupes > 1].to_dict()}"
    )
