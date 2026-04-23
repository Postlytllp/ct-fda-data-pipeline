import pandas as pd
from lib.filters import add_has_any_ae_flag, add_passes_diversity_flag, add_has_lung_cancer_drug_match_flag

def test_has_any_ae_true_when_total_affected_positive():
    arms = pd.DataFrame([{"nct_id": "N1", "arm_label": "A"},
                        {"nct_id": "N1", "arm_label": "B"}])
    ae_summary = pd.DataFrame([
        {"nct_id": "N1", "arm_label": "A",
         "total_serious_affected": 3, "total_other_affected": 0,
         "total_at_risk": 50},
        {"nct_id": "N1", "arm_label": "B",
         "total_serious_affected": 0, "total_other_affected": 0,
         "total_at_risk": 40},
    ])
    out = add_has_any_ae_flag(arms, ae_summary)
    assert out.loc[out.arm_label == "A", "has_any_ae"].iloc[0] == True
    assert out.loc[out.arm_label == "B", "has_any_ae"].iloc[0] == False

def test_passes_diversity_tier_a_requires_threshold():
    demog = pd.DataFrame([
        {"nct_id": "N1", "arm_label": None, "demog_tier": "A1",
         "inferred_diversity_pct": 0.96, "inferred_population": "Asian"},
        {"nct_id": "N2", "arm_label": None, "demog_tier": "A1",
         "inferred_diversity_pct": 0.80, "inferred_population": "Asian"},
    ])
    out = add_passes_diversity_flag(demog)
    assert out.loc[out.nct_id == "N1", "passes_diversity"].iloc[0] == True
    assert out.loc[out.nct_id == "N2", "passes_diversity"].iloc[0] == False

def test_passes_diversity_tier_b_c_d_accept_categorical():
    demog = pd.DataFrame([
        {"nct_id": "N3", "arm_label": None, "demog_tier": "B1",
         "inferred_diversity_pct": None, "inferred_population": "Japanese"},
        {"nct_id": "N4", "arm_label": None, "demog_tier": "NONE",
         "inferred_diversity_pct": None, "inferred_population": None},
    ])
    out = add_passes_diversity_flag(demog)
    assert out.loc[out.nct_id == "N3", "passes_diversity"].iloc[0] == True
    assert out.loc[out.nct_id == "N4", "passes_diversity"].iloc[0] == False

def test_has_lung_cancer_drug_match_any_intervention_primary():
    arms = pd.DataFrame([{"nct_id": "N1", "arm_label": "A"},
                        {"nct_id": "N1", "arm_label": "B"}])
    ai = pd.DataFrame([
        {"nct_id": "N1", "arm_label": "A", "is_primary_oncology": True},
        {"nct_id": "N1", "arm_label": "A", "is_primary_oncology": False},
        {"nct_id": "N1", "arm_label": "B", "is_primary_oncology": False},
    ])
    out = add_has_lung_cancer_drug_match_flag(arms, ai)
    assert out.loc[out.arm_label == "A", "has_lung_cancer_drug_match"].iloc[0] == True
    assert out.loc[out.arm_label == "B", "has_lung_cancer_drug_match"].iloc[0] == False
