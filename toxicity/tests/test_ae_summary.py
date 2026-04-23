import pandas as pd
from lib.ae_summary import build_ae_arm_summary, build_ae_long

def test_ae_arm_summary_computes_totals_and_rates():
    ae_raw = pd.DataFrame([
        {"nct_id": "N1", "arm_label": "A", "ae_category": "SERIOUS",
         "meddra_term": "T1", "affected_count": 2, "at_risk_count": 100},
        {"nct_id": "N1", "arm_label": "A", "ae_category": "SERIOUS",
         "meddra_term": "T2", "affected_count": 1, "at_risk_count": 100},
        {"nct_id": "N1", "arm_label": "A", "ae_category": "OTHER",
         "meddra_term": "T3", "affected_count": 7, "at_risk_count": 100},
    ])
    out = build_ae_arm_summary(ae_raw)
    r = out.iloc[0]
    assert r.total_serious_affected == 3
    assert r.total_other_affected == 7
    assert r.total_at_risk == 100  # max, not sum
    assert r.distinct_serious_terms == 2
    assert r.distinct_other_terms == 1
    assert round(r.serious_ae_rate, 2) == 0.03
    assert round(r.ae_events_per_participant, 2) == 0.10

def test_ae_long_drops_rows_without_resolved_arm():
    ae_raw = pd.DataFrame([
        {"nct_id": "N1", "arm_label": "A", "ae_category": "SERIOUS",
         "meddra_term": "T", "affected_count": 1, "at_risk_count": 10},
        {"nct_id": "N1", "arm_label": None, "ae_category": "OTHER",
         "meddra_term": "T", "affected_count": 1, "at_risk_count": 10},
    ])
    known_arms = pd.DataFrame([{"nct_id": "N1", "arm_label": "A"}])
    out = build_ae_long(ae_raw, known_arms)
    assert len(out) == 1
    assert out.iloc[0].arm_label == "A"
