import pandas as pd
from sqlalchemy import create_engine, text
from lib.storage import write_csvs, write_sqlite, build_cohort_view

def test_write_csvs_round_trip(tmp_path):
    dfs = {"trials": pd.DataFrame([{"nct_id": "N1"}])}
    paths = write_csvs(dfs, tmp_path)
    assert (tmp_path / "trials.csv").exists()
    back = pd.read_csv(paths["trials"])
    assert back["nct_id"].iloc[0] == "N1"

def test_write_sqlite_creates_tables(tmp_path):
    dfs = {
        "trials": pd.DataFrame([{"nct_id": "N1"}]),
        "arms": pd.DataFrame([{"nct_id": "N1", "arm_label": "A",
                              "has_any_ae": True, "has_lung_cancer_drug_match": True}]),
        "demographics": pd.DataFrame([{"nct_id": "N1", "arm_label": None,
                                      "demog_tier": "B1", "passes_diversity": True,
                                      "inferred_population": "Japanese"}]),
        "ae_long": pd.DataFrame([{"nct_id": "N1", "arm_label": "A",
                                 "ae_category": "SERIOUS", "meddra_term": "T"}]),
    }
    db_path = tmp_path / "tox.db"
    write_sqlite(dfs, db_path)
    eng = create_engine(f"sqlite:///{db_path}")
    with eng.begin() as conn:
        tables = set(r[0] for r in conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table'")).all())
    assert {"trials", "arms", "demographics", "ae_long"}.issubset(tables)

def test_build_cohort_view_filters_on_three_flags():
    trials = pd.DataFrame([{"nct_id": "N1", "phase": "PHASE3",
                           "sponsor_name": "X", "lead_sponsor_country": "Japan"}])
    arms = pd.DataFrame([{"nct_id": "N1", "arm_label": "A", "regimen_display": "pembrolizumab",
                         "has_any_ae": True, "has_lung_cancer_drug_match": True}])
    demog = pd.DataFrame([{"nct_id": "N1", "arm_label": None, "demog_tier": "B1",
                          "demog_confidence": "high", "inferred_population": "Japanese",
                          "passes_diversity": True}])
    ae_long = pd.DataFrame([{"nct_id": "N1", "arm_label": "A", "ae_category": "SERIOUS",
                            "meddra_term": "Neutropenia", "organ_system": "Blood",
                            "affected_count": 2, "at_risk_count": 50}])
    ai_agg = pd.DataFrame([{"nct_id": "N1", "arm_label": "A",
                           "primary_oncology_drugs": "pembrolizumab",
                           "backbone_drugs": ""}])
    cohort = build_cohort_view(trials, arms, demog, ai_agg, ae_long)
    assert len(cohort) == 1
    r = cohort.iloc[0]
    assert r.nct_id == "N1"
    assert r.meddra_term == "Neutropenia"
    assert r.inferred_population == "Japanese"
    assert r.primary_oncology_drugs == "pembrolizumab"
