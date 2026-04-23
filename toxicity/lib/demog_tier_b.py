"""Tier B: explicit text evidence for population restriction.

B1 — regex over brief/official title + detailed description.
B2 — regex prescreen + LLM over eligibilityCriteria inclusion block.
"""
from __future__ import annotations
import re
from typing import Dict, List, Optional

POPULATIONS = (
    r"(?P<population>japanese|korean|chinese|taiwanese|vietnamese|thai|filipino|indian|"
    r"pakistani|bangladeshi|malay|indonesian|arab|persian|turkish|israeli|"
    r"black|african[\s-]american|african|hispanic|latino|caucasian|white|"
    r"asian|south\s+asian|east\s+asian|southeast\s+asian|mena|"
    r"european|polish|german|french|italian|spanish|dutch|swedish|finnish|"
    r"russian|american|brazilian|mexican|nigerian|ethiopian|"
    r"native\s+american|hawaiian|pacific\s+islander)"
)
_POP_CONTEXT = r"(?P<context>patient|subject|population|participant|adult|volunteer|cohort)"
_NEG = r"(?<!excluding )(?<!without )(?<!non-)(?<!no )"

_B1_RE = re.compile(
    rf"{_NEG}\b{POPULATIONS}\s+{_POP_CONTEXT}s?\b",
    flags=re.IGNORECASE,
)


def _search(text: str) -> Optional[Dict]:
    if not text:
        return None
    m = _B1_RE.search(text)
    if not m:
        return None
    return {
        "population": m.group("population"),
        "context_word": m.group("context"),
        "evidence_span": text[max(0, m.start() - 30): m.end() + 30],
    }


def tier_b1_text_regex(brief_title: str, official_title: str,
                       detailed_description: str) -> List[Dict]:
    hits: List[Dict] = []
    for field, text in (("brief_title", brief_title),
                        ("official_title", official_title),
                        ("detailed_description", detailed_description)):
        h = _search(text or "")
        if h:
            hits.append({
                "tier": "B1",
                "demog_confidence": "high" if field != "detailed_description" else "medium",
                "inferred_population": h["population"],
                "population": h["population"],
                "demog_source_evidence": h["evidence_span"],
                "source_field": field,
                "fallback_trial_level": True,
                "context_label": "inclusion",
            })
    return hits


_EXCLUSION_PREFIXES = ("exclusion", "excluding", "stratification",
                       "sub-analysis", "subanalysis", "biomarker")


def _looks_like_exclusion_context(inclusion_text: str) -> bool:
    head = (inclusion_text or "").strip().lower()[:60]
    return any(head.startswith(p) for p in _EXCLUSION_PREFIXES)


def needs_b2_llm(b1_hit: Optional[Dict], inclusion_text: str) -> bool:
    if not b1_hit:
        return len(inclusion_text or "") > 200
    ctx = b1_hit.get("context_label") if isinstance(b1_hit, dict) else None
    if ctx == "exclusion":
        return True
    if _looks_like_exclusion_context(inclusion_text):
        return True
    return False
