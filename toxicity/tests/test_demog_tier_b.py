from lib.demog_tier_b import tier_b1_text_regex, needs_b2_llm

def test_b1_hits_on_title_with_population_word():
    hits = tier_b1_text_regex(
        brief_title="A Study in Japanese Patients with NSCLC",
        official_title="", detailed_description="",
    )
    assert hits[0]["population"].lower() == "japanese"
    assert hits[0]["tier"] == "B1"
    assert hits[0]["demog_confidence"] in ("medium", "high")

def test_b1_respects_negation():
    hits = tier_b1_text_regex(
        brief_title="Excluding Japanese patients, enroll worldwide",
        official_title="", detailed_description="",
    )
    assert hits == []

def test_b1_handles_non_prefix_but_full_pattern():
    hits = tier_b1_text_regex(
        brief_title="",
        official_title="Korean Population Study of Osimertinib",
        detailed_description="",
    )
    assert hits and hits[0]["population"].lower() == "korean"

def test_needs_b2_triggers_when_no_regex_hit_and_long_inclusion():
    assert needs_b2_llm(b1_hit=None, inclusion_text="x" * 300) is True

def test_needs_b2_triggers_when_regex_hit_is_in_exclusion_section():
    # A regex hit but under a bullet beginning with 'Exclusion'
    assert needs_b2_llm(b1_hit={"population": "Japanese", "context_label": "exclusion"},
                       inclusion_text="Exclusion: Japanese patients excluded") is True

def test_needs_b2_skips_when_clean_inclusion_hit():
    assert needs_b2_llm(b1_hit={"population": "Japanese", "context_label": "inclusion"},
                       inclusion_text="Patients must be of Japanese origin") is False
