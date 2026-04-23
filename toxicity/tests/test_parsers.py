import json
import pandas as pd
from tests.conftest import FIXTURES
from lib.parsers import parse_trials, parse_arms, parse_arm_interventions


def _load_studies():
    page = json.loads((FIXTURES / "ctgov_page_001.json").read_text(encoding="utf-8"))
    return page["studies"]


def test_parse_trials_returns_dataframe_with_required_columns():
    studies = _load_studies()
    df = parse_trials(studies)
    required = {
        "nct_id", "brief_title", "official_title", "detailed_description", "phase",
        "overall_status", "study_type", "start_date", "completion_date",
        "enrollment_actual", "sponsor_name", "sponsor_class", "lead_sponsor_country",
        "conditions", "keywords", "countries", "secondary_ids", "lung_cancer_subtypes",
        "eligibility_criteria_text", "gender", "minimum_age", "maximum_age",
        "healthy_volunteers",
    }
    assert required.issubset(df.columns)
    assert len(df) == len(studies)
    # JSON-encoded columns are strings
    assert isinstance(df["conditions"].iloc[0], str)
    assert df["nct_id"].iloc[0].startswith("NCT")


def test_parse_arms_returns_one_row_per_arm_group():
    studies = _load_studies()
    trials_df = parse_trials(studies)
    arms_df = parse_arms(studies)
    assert set(["nct_id", "arm_label", "raw_arm_label", "arm_type",
               "arm_description"]).issubset(arms_df.columns)
    # every arm row corresponds to a known trial
    assert set(arms_df["nct_id"]).issubset(set(trials_df["nct_id"]))


def test_parse_arm_interventions_is_mn_with_required_columns():
    studies = _load_studies()
    drug_df = pd.DataFrame([
        {"canonical_name": "tepotinib", "rxcui": "2049110",
         "aliases": '["tepotinib","Tepmetko"]'},
        {"canonical_name": "capmatinib", "rxcui": "2049111",
         "aliases": '["capmatinib","Tabrecta"]'},
    ])
    drug_class_df = pd.DataFrame([
        {"rxcui": "2049110", "generic_name": "tepotinib", "drug_class": "targeted_oncology"},
        {"rxcui": "2049111", "generic_name": "capmatinib", "drug_class": "targeted_oncology"},
    ])
    ai_df = parse_arm_interventions(studies, drug_df, drug_class_df)
    required = {"nct_id", "arm_label", "intervention_name", "intervention_type",
                "rxnorm_rxcui", "matched_lung_cancer_drug", "is_primary_oncology",
                "drug_class"}
    assert required.issubset(ai_df.columns)
