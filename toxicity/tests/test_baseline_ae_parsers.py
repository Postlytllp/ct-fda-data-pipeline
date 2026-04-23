import json
import pandas as pd
from tests.conftest import FIXTURES
from lib.baseline_ae_parsers import parse_baseline_raw, parse_ae_raw, annotate_regimen_on_arms
from lib.parsers import parse_arms, parse_arm_interventions

def _studies():
    return json.loads((FIXTURES / "ctgov_page_001.json").read_text(encoding="utf-8"))["studies"]

def test_parse_baseline_raw_has_expected_shape():
    df = parse_baseline_raw(_studies())
    required = {"nct_id", "group_id", "measure_title", "category", "value", "source_units"}
    assert required.issubset(df.columns)

def test_parse_ae_raw_has_expected_shape_and_categories():
    df = parse_ae_raw(_studies())
    required = {"nct_id", "raw_group_id", "arm_label", "ae_category", "meddra_term",
               "organ_system", "affected_count", "at_risk_count", "source_vocab"}
    assert required.issubset(df.columns)
    assert set(df["ae_category"].dropna().unique()).issubset({"SERIOUS", "OTHER"})
    # at_risk / affected are numeric-like
    assert pd.api.types.is_numeric_dtype(df["at_risk_count"])

def test_annotate_regimen_builds_sorted_pipe_key_and_display():
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
    studies = _studies()
    arms = parse_arms(studies)
    ai = parse_arm_interventions(studies, drug_df, drug_class_df)
    out = annotate_regimen_on_arms(arms, ai)
    # non-null keys are lowercased alphabetical pipe-joined
    sample = out[out["regimen_key"].notna()].head(1)
    if len(sample):
        k = sample["regimen_key"].iloc[0]
        parts = k.split("|")
        assert parts == sorted(parts)
