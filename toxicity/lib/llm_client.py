"""Tier B2 LLM client with JSONL cache; LLM backend is injected.

Real backend (Gemma 4 26B MoE via llama-cpp or transformers) is created by
notebook cells and passed in as `llm_callable`. Tests use a fake callable.
"""
from __future__ import annotations
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Callable, Dict, Optional


@dataclass
class PopulationHit:
    has_population_restriction: bool
    population: Optional[str]
    is_inclusion_criterion: bool
    evidence_span: str
    reasoning: str


class LLMCache:
    """Append-only JSONL cache keyed by nct_id."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._by_nct: Dict[str, PopulationHit] = {}
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                nct = rec.pop("nct_id")
                self._by_nct[nct] = PopulationHit(**rec)

    def get(self, nct_id: str) -> Optional[PopulationHit]:
        return self._by_nct.get(nct_id)

    def put(self, nct_id: str, hit: PopulationHit) -> None:
        self._by_nct[nct_id] = hit
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"nct_id": nct_id, **asdict(hit)}, ensure_ascii=False) + "\n")


B2_SYSTEM_PROMPT = (
    "You extract population restrictions from clinical trial eligibility criteria.\n"
    "Return strict JSON: {has_population_restriction: bool, population: str|null, "
    "is_inclusion_criterion: bool, evidence_span: str, reasoning: str}.\n"
    "Only set has_population_restriction=true when the criterion is a hard "
    "inclusion filter on ethnicity, nationality, or ancestry. Biomarker or "
    "disease-stage filters do not count."
)


def extract_population_from_eligibility(
    text: str,
    nct_id: str,
    cache: LLMCache,
    llm_callable: Callable[[str], PopulationHit],
) -> PopulationHit:
    cached = cache.get(nct_id)
    if cached is not None:
        return cached
    hit = llm_callable(text)
    cache.put(nct_id, hit)
    return hit
