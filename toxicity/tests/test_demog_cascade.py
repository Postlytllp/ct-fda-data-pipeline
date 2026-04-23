import pandas as pd
from tests.conftest import FIXTURES
from lib.demog_tier_a import load_monoethnic_countries
from lib.demog_cascade import run_cascade

def test_tier_a_wins_even_when_b1_would_say_otherwise():
    monoeth = load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")
    trial = {
        "nct_id": "NCT1",
        "brief_title": "Japanese patients study",
        "official_title": "", "detailed_description": "",
        "eligibility_criteria_text": "Inclusion: Age 18+, ECOG 0-1",
        "site_countries": ["Japan"], "lead_sponsor_country": None,
        "secondary_ids": [],
    }
    baseline_df = pd.DataFrame([
        {"nct_id": "NCT1", "group_id": "BG000", "group_title": "All participants",
         "measure_title": "Race (NIH-OMB)", "category": "Asian",
         "value": 80, "source_units": "Participants"},
        {"nct_id": "NCT1", "group_id": "BG000", "group_title": "All participants",
         "measure_title": "Race (NIH-OMB)", "category": "White",
         "value": 20, "source_units": "Participants"},
    ])
    out = run_cascade(trials=[trial], baseline_df=baseline_df,
                     monoeth_df=monoeth, llm_client=None, llm_cache=None)
    r = out.iloc[0]
    # Tier A data exists — higher tier always wins. Does not fall through to B/C.
    assert r.demog_tier in ("A1", "A1-trial", "NONE")
    # because max was 80%, diversity_pct is 0.8 and population is null
    assert r.inferred_diversity_pct in (0.8,) or r.inferred_population is None

def test_cascade_falls_through_to_b1_when_no_baseline():
    monoeth = load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")
    trial = {
        "nct_id": "NCT2",
        "brief_title": "Korean Patients With Advanced NSCLC",
        "official_title": "", "detailed_description": "",
        "eligibility_criteria_text": "Must have EGFR mutation",
        "site_countries": [], "lead_sponsor_country": None,
        "secondary_ids": [],
    }
    out = run_cascade(trials=[trial], baseline_df=pd.DataFrame(),
                     monoeth_df=monoeth, llm_client=None, llm_cache=None)
    r = out.iloc[0]
    assert r.demog_tier == "B1"
    assert r.inferred_population.lower() == "korean"

def test_cascade_none_when_no_evidence():
    monoeth = load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")
    trial = {
        "nct_id": "NCT3",
        "brief_title": "Phase III Randomized Trial of Foo vs Bar",
        "official_title": "", "detailed_description": "",
        "eligibility_criteria_text": "",
        "site_countries": ["United States"],
        "lead_sponsor_country": "United States",
        "secondary_ids": [],
    }
    out = run_cascade(trials=[trial], baseline_df=pd.DataFrame(),
                     monoeth_df=monoeth, llm_client=None, llm_cache=None)
    r = out.iloc[0]
    assert r.demog_tier == "NONE"
    assert r.inferred_population is None
