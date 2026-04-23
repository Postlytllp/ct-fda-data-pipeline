import pandas as pd
from pathlib import Path
from tests.conftest import FIXTURES
from lib import config

def _load_monoethnic(path: Path) -> pd.DataFrame:
    from lib.demog_tier_a import load_monoethnic_countries
    return load_monoethnic_countries(path)

def test_monoethnic_fixture_has_required_columns():
    df = _load_monoethnic(FIXTURES / "monoethnic_countries_min.csv")
    required = {"country_iso3", "country_name", "dominant_ancestry",
               "homogeneity_score", "continent", "region", "in_diverse_exclusion_list"}
    assert required.issubset(df.columns)
    assert df.loc[df.country_iso3 == "JPN", "dominant_ancestry"].iloc[0] == "East Asian (Japanese)"
    assert df.loc[df.country_iso3 == "USA", "in_diverse_exclusion_list"].iloc[0] == True
    assert df.loc[df.country_iso3 == "JPN", "in_diverse_exclusion_list"].iloc[0] == False

def test_committed_monoethnic_file_exists_and_is_parseable():
    assert config.MONOETHNIC_CSV.exists()
    df = _load_monoethnic(config.MONOETHNIC_CSV)
    assert len(df) >= 30
    assert "JPN" in set(df.country_iso3)
    # classification invariants (regressions here silently break Tier A2)
    assert df.loc[df.country_iso3 == "USA", "in_diverse_exclusion_list"].iloc[0] == True
    assert df.loc[df.country_iso3 == "JPN", "in_diverse_exclusion_list"].iloc[0] == False
    # schema invariants
    assert df["homogeneity_score"].between(0, 1).all()
    assert df["country_iso3"].str.match(r"^[A-Z]{3}$").all()
    assert df["country_iso3"].is_unique

def test_drug_class_lookup_has_required_columns_and_entries():
    df = pd.read_csv(config.DRUG_CLASS_CSV)
    required = {"rxcui", "generic_name", "drug_class", "biomarker_note"}
    assert required.issubset(df.columns)
    # at least one of each spec-mandated class present
    classes = set(df.drug_class)
    for c in ("targeted_oncology", "immunotherapy", "chemo_backbone",
              "antiangiogenic", "supportive_care", "placebo"):
        assert c in classes, f"missing class: {c}"
    # known drug-class mapping smoke-check
    assert df.loc[df.generic_name == "pembrolizumab", "drug_class"].iloc[0] == "immunotherapy"
    assert df.loc[df.generic_name == "osimertinib", "drug_class"].iloc[0] == "targeted_oncology"
