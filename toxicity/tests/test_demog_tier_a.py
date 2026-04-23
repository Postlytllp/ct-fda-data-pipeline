import pandas as pd
from tests.conftest import FIXTURES
from lib.demog_tier_a import (
    load_monoethnic_countries, tier_a1_per_arm, tier_a1_trial_level, tier_a2_country,
)

def _baseline_race(nct, per_group):
    rows = []
    for gid, counts in per_group.items():
        for category, v in counts.items():
            rows.append({
                "nct_id": nct, "group_id": gid, "group_title": f"Arm {gid}",
                "measure_title": "Race (NIH-OMB)", "category": category,
                "value": v, "source_units": "Participants",
            })
    return pd.DataFrame(rows)

def test_tier_a1_passes_when_one_race_is_at_least_95pct():
    df = _baseline_race("NCT1", {
        "BG000": {"Asian": 95, "Black or African American": 3, "White": 2},
    })
    out = tier_a1_per_arm(df)
    assert len(out) == 1
    r = out.iloc[0]
    assert r.demog_tier == "A1"
    assert r.inferred_population.lower().startswith("asian")
    assert r.inferred_diversity_pct >= 0.95
    assert r.demog_confidence == "high"

def test_tier_a1_fails_when_max_below_threshold_and_records_pct():
    df = _baseline_race("NCT2", {
        "BG000": {"Asian": 60, "White": 40},
    })
    out = tier_a1_per_arm(df)
    assert len(out) == 1
    r = out.iloc[0]
    assert r.demog_tier != "A1" or r.inferred_diversity_pct < 0.95

def test_tier_a1_trial_level_pools_arms():
    # BG000: 47/50 = 94% Asian (below threshold alone)
    # BG001: 48/50 = 96% Asian (above threshold alone)
    # Pooled: 95/100 = 95% Asian — at threshold, so trial-level fires
    df = _baseline_race("NCT3", {
        "BG000": {"Asian": 47, "White": 3},
        "BG001": {"Asian": 48, "White": 2},
    })
    out = tier_a1_trial_level(df)
    assert (out.demog_tier == "A1-trial").any()
    assert (out.fallback_trial_level == True).any()

def test_tier_a2_excludes_diverse_countries():
    monoeth = load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")
    country_df = pd.DataFrame([
        {"nct_id": "NCT4", "group_id": "BG000",
         "group_title": "Arm 1",
         "measure_title": "Region of Enrollment",
         "category": "United States", "value": 100, "source_units": "Participants"},
    ])
    out = tier_a2_country(country_df, monoeth)
    # USA is in_diverse_exclusion_list=True → no A2 hit
    assert out.empty or (out.demog_tier != "A2").all()

def test_tier_a2_accepts_japan_when_at_threshold():
    monoeth = load_monoethnic_countries(FIXTURES / "monoethnic_countries_min.csv")
    country_df = pd.DataFrame([
        {"nct_id": "NCT5", "group_id": "BG000",
         "group_title": "Arm 1",
         "measure_title": "Country of Enrollment",
         "category": "Japan", "value": 98, "source_units": "Participants"},
    ])
    out = tier_a2_country(country_df, monoeth)
    assert (out.demog_tier == "A2").any()
    r = out[out.demog_tier == "A2"].iloc[0]
    assert "Japanese" in r.inferred_population
