import json
import pandas as pd
from lib.run_summary import build_run_summary

def test_build_run_summary_has_expected_keys(tmp_path):
    trials = pd.DataFrame([{"nct_id": "N1"}, {"nct_id": "N2"}])
    arms = pd.DataFrame([
        {"nct_id": "N1", "arm_label": "A", "has_any_ae": True,
         "has_lung_cancer_drug_match": True, "arm_match_status": "ok"},
        {"nct_id": "N2", "arm_label": "A", "has_any_ae": False,
         "has_lung_cancer_drug_match": False, "arm_match_status": "count_mismatch"},
    ])
    demog = pd.DataFrame([
        {"nct_id": "N1", "demog_tier": "A1", "passes_diversity": True},
        {"nct_id": "N2", "demog_tier": "NONE", "passes_diversity": False},
    ])
    baseline_df = pd.DataFrame([{"nct_id": "N1"}])  # just N1 has baseline
    ae_df = pd.DataFrame([{"nct_id": "N1"}])
    out = build_run_summary(
        drugs_queried=42, trials=trials, arms=arms, demographics=demog,
        baseline_df=baseline_df, ae_df=ae_df,
        llm_calls_made=3, llm_cache_hits=7,
        total_runtime_seconds=120.5,
    )
    assert out["drugs_queried"] == 42
    assert out["trials_returned"] == 2
    assert out["trials_with_baseline"] == 1
    assert out["trials_with_ae"] == 1
    assert out["arms_total"] == 2
    assert out["arms_passing_each_filter"]["has_any_ae"] == 1
    assert out["arms_passing_each_filter"]["has_lung_cancer_drug_match"] == 1
    assert out["demog_tier_distribution"]["A1"] == 1
    assert out["arm_match_status_distribution"]["count_mismatch"] == 1
    assert out["llm_calls_made"] == 3
    # round-trips through json
    path = tmp_path / "run.json"
    path.write_text(json.dumps(out))
    assert json.loads(path.read_text())["drugs_queried"] == 42
