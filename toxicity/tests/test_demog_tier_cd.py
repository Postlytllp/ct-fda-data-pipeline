import pandas as pd
from tests.conftest import FIXTURES
from lib.demog_tier_a import load_monoethnic_countries
from lib.demog_tier_cd import tier_c_location, tier_d_registry

def _monoeth():
    return load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")

def test_c1_single_site_in_monoethnic_whitelist():
    trial = {"nct_id": "NCT1", "site_countries": ["Japan"], "lead_sponsor_country": None,
             "secondary_ids": []}
    out = tier_c_location(trial, _monoeth())
    assert out["demog_tier"] == "C1"
    assert "Japanese" in out["inferred_population"]
    assert out["demog_confidence"] == "medium"

def test_c2_multi_country_same_continent():
    trial = {"nct_id": "NCT2", "site_countries": ["Japan", "South Korea", "China"],
             "lead_sponsor_country": None, "secondary_ids": []}
    out = tier_c_location(trial, _monoeth())
    assert out["demog_tier"] == "C2"
    assert "East Asia" in out["inferred_population"]
    assert out["demog_confidence"] == "medium"

def test_c3_sponsor_fallback_when_no_sites():
    trial = {"nct_id": "NCT3", "site_countries": [], "lead_sponsor_country": "South Korea",
             "secondary_ids": []}
    out = tier_c_location(trial, _monoeth())
    assert out["demog_tier"] == "C3"
    assert out["demog_confidence"] == "low"

def test_c_returns_none_when_sites_in_diverse_exclusion_list():
    trial = {"nct_id": "NCT4", "site_countries": ["United States"],
             "lead_sponsor_country": None, "secondary_ids": []}
    out = tier_c_location(trial, _monoeth())
    assert out is None

def test_d1_cTRI_secondary_id_points_to_india():
    trial = {"nct_id": "NCT5", "site_countries": [], "lead_sponsor_country": None,
             "secondary_ids": [{"type": "REGISTRY", "domain": "CTRI", "id": "CTRI/2020/01/001"}]}
    out = tier_d_registry(trial, _monoeth())
    assert out["demog_tier"] == "D1"
    assert "South Asian" in out["inferred_population"]
    assert out["demog_confidence"] == "low"
