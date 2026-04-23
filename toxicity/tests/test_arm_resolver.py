from lib.arm_resolver import normalize_arm_label, resolve_arm_labels

def test_normalize_strips_prefix_and_dose():
    assert normalize_arm_label("Experimental: Pembrolizumab 200 mg Q3W") == "pembrolizumab"
    assert normalize_arm_label("Arm A — Osimertinib 80mg daily") == "osimertinib"
    assert normalize_arm_label("Placebo Control") == "placebo control"
    assert normalize_arm_label("  Cohort 2 : Docetaxel 75 mg/m2  ") == "docetaxel"

def test_resolve_arm_labels_exact_match_first():
    arms = ["Pembrolizumab 200mg Q3W", "Chemotherapy Doublet"]
    ae_groups = ["Pembrolizumab 200mg Q3W", "Chemotherapy Doublet"]
    out = resolve_arm_labels(arms, ae_groups)
    assert out["Pembrolizumab 200mg Q3W"]["match_method"] == "exact_normalized"
    assert out["Pembrolizumab 200mg Q3W"]["matched_to"] == "Pembrolizumab 200mg Q3W"

def test_resolve_arm_labels_fuzzy_fallback():
    arms = ["Pembrolizumab + Chemo"]
    ae_groups = ["Pembrolizumab plus Chemotherapy"]
    out = resolve_arm_labels(arms, ae_groups)
    m = out["Pembrolizumab + Chemo"]
    assert m["match_method"] == "fuzzy"
    assert m["fuzzy_score"] >= 90
    assert m["matched_to"] == "Pembrolizumab plus Chemotherapy"

def test_resolve_arm_labels_positional_when_counts_match_and_no_fuzzy():
    arms = ["Foo", "Bar"]
    ae_groups = ["Widget One", "Widget Two"]
    out = resolve_arm_labels(arms, ae_groups)
    assert out["Foo"]["match_method"] == "positional"
    assert out["Foo"]["matched_to"] == "Widget One"
    assert out["Bar"]["matched_to"] == "Widget Two"

def test_resolve_arm_labels_count_mismatch_flags_remaining():
    arms = ["Foo", "Bar", "Baz"]
    ae_groups = ["Widget One", "Widget Two"]
    out = resolve_arm_labels(arms, ae_groups)
    unresolved = [k for k, v in out.items() if v["match_method"] == "unmatched"]
    assert len(unresolved) >= 1
    for k, v in out.items():
        if v["match_method"] == "unmatched":
            assert v["arm_match_status"] == "count_mismatch"
