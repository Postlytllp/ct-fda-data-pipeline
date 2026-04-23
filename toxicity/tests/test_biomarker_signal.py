from lib.biomarker_signal import detect_biomarker_signal

def test_egfr_mutation_sets_asian_skewed_signal():
    s = detect_biomarker_signal("Patients must have EGFR mutation positive disease")
    assert "EGFR" in s and "Asian" in s

def test_alk_rearrangement_sets_signal():
    s = detect_biomarker_signal("ALK positive rearrangement confirmed by FISH")
    assert "ALK" in s

def test_kras_g12c_sets_signal():
    s = detect_biomarker_signal("KRAS G12C mutated advanced NSCLC")
    assert "KRAS" in s

def test_no_match_returns_none():
    assert detect_biomarker_signal("Standard chemotherapy doublet in Stage IIIB NSCLC") is None
