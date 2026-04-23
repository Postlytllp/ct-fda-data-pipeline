import json
import pandas as pd
from lib.drug_list import merge_drugs, attach_subtypes, to_dataframe

def test_merge_drugs_unions_and_dedupes():
    nsclc = [{"name": "pembrolizumab", "kind": "generic"},
             {"name": "keytruda", "kind": "brand"}]
    sclc = [{"name": "etoposide", "kind": "generic"},
            {"name": "pembrolizumab", "kind": "generic"}]  # both subtypes
    merged = merge_drugs(nsclc, sclc)
    names = {r["name"] for r in merged}
    assert names == {"pembrolizumab", "keytruda", "etoposide"}

def test_attach_subtypes_sets_intersection():
    nsclc = [{"name": "pembrolizumab", "kind": "generic"}]
    sclc = [{"name": "etoposide", "kind": "generic"},
            {"name": "pembrolizumab", "kind": "generic"}]
    merged = merge_drugs(nsclc, sclc)
    merged = attach_subtypes(merged, nsclc, sclc)
    by_name = {r["name"]: r for r in merged}
    assert set(by_name["pembrolizumab"]["subtypes"]) == {"NSCLC", "SCLC"}
    assert by_name["etoposide"]["subtypes"] == ["SCLC"]

def test_to_dataframe_canonicalizes_and_collapses_brands_to_aliases():
    merged = [
        {"name": "pembrolizumab", "kind": "generic", "subtypes": ["NSCLC"]},
        {"name": "keytruda", "kind": "brand", "subtypes": ["NSCLC"]},
    ]
    harmonized = {
        "pembrolizumab": {
            "rxcui": "1547545",
            "rxnorm_generic_name": "pembrolizumab",
            "all_brand_names": ["Keytruda"],
            "all_synonyms": ["MK-3475"],
        },
    }
    df = to_dataframe(merged, harmonized, snapshot_date="2026-04-23")
    assert set(df.columns) == {
        "canonical_name", "rxcui", "aliases", "subtypes",
        "source", "snapshot_date"
    }
    row = df.iloc[0]
    assert row.canonical_name == "pembrolizumab"
    assert row.rxcui == "1547545"
    assert row.source == "nci"
    assert row.snapshot_date == "2026-04-23"
    aliases = json.loads(row.aliases)
    assert "Keytruda" in aliases
    assert "MK-3475" in aliases
    assert "pembrolizumab" in aliases
