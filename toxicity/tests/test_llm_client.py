import json
import pytest
from pathlib import Path
from lib.llm_client import PopulationHit, LLMCache, extract_population_from_eligibility

def test_populationhit_schema_defaults():
    p = PopulationHit(
        has_population_restriction=True, population="Japanese",
        is_inclusion_criterion=True, evidence_span="...Japanese patients...",
        reasoning="literal restriction",
    )
    assert p.population == "Japanese"
    assert p.is_inclusion_criterion is True

def test_llm_cache_round_trips(tmp_path):
    c = LLMCache(tmp_path / "cache.jsonl")
    assert c.get("NCT1") is None
    hit = PopulationHit(True, "Korean", True, "Korean patients", "match")
    c.put("NCT1", hit)
    # New instance, same file
    c2 = LLMCache(tmp_path / "cache.jsonl")
    got = c2.get("NCT1")
    assert got is not None and got.population == "Korean"

def test_extract_uses_cache_and_does_not_call_llm_on_hit(tmp_path):
    cache = LLMCache(tmp_path / "cache.jsonl")
    cache.put("NCT1", PopulationHit(True, "Thai", True, "Thai patients", "cached"))
    called = {"n": 0}
    def fake_llm(text):
        called["n"] += 1
        return PopulationHit(False, None, False, "", "not called")
    got = extract_population_from_eligibility("irrelevant",
                                              nct_id="NCT1", cache=cache,
                                              llm_callable=fake_llm)
    assert got.population == "Thai"
    assert called["n"] == 0

def test_extract_calls_llm_on_miss_and_persists(tmp_path):
    cache = LLMCache(tmp_path / "cache.jsonl")
    def fake_llm(text):
        return PopulationHit(True, "Chinese", True, "Chinese patients", "from llm")
    got = extract_population_from_eligibility("Chinese adults, ECOG 0-1",
                                              nct_id="NCTX", cache=cache,
                                              llm_callable=fake_llm)
    assert got.population == "Chinese"
    assert cache.get("NCTX").population == "Chinese"
