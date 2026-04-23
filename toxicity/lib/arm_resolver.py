"""Arm label resolution across CT.gov modules.

Order: normalize → exact → fuzzy (rapidfuzz token_set_ratio ≥ threshold) →
positional (when group counts match) → unmatched+count_mismatch flag.
"""
from __future__ import annotations
import re
from typing import Dict, List
from rapidfuzz import fuzz

from lib.config import FUZZY_ARM_THRESHOLD

_PREFIX_RE = re.compile(
    r"^(experimental|active\s+comparator|placebo(\s+comparator)?|arm\s+[a-z0-9]+|"
    r"group\s+\d+|cohort\s+[a-z\d]+)\s*[:\-–]\s*",
    flags=re.IGNORECASE,
)
_DOSE_RE = re.compile(
    r"\b\d+(\.\d+)?\s*(mg/m2|mg/kg|mg|mcg|g|auc\d+|iu|units)\b",
    flags=re.IGNORECASE,
)
_FREQ_RE = re.compile(
    r"\bq[dwm]\d*|q\d*[dwm]|qod|bid|tid|daily|weekly|monthly|once\b",
    flags=re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")
_DASH_RE = re.compile(r"[–—−]")


def normalize_arm_label(label: str) -> str:
    s = _DASH_RE.sub("-", label or "")
    s = s.strip()
    s = s.casefold()
    s = _PREFIX_RE.sub("", s)
    s = _DOSE_RE.sub("", s)
    s = _FREQ_RE.sub("", s)
    s = s.replace("+", " + ").replace("/", " / ")
    s = _WS_RE.sub(" ", s).strip(" -:–—")
    return s


def resolve_arm_labels(arm_labels: List[str], ae_group_titles: List[str]) -> Dict[str, dict]:
    """Map each arm label to the best AE group title match.

    Returns dict keyed by original arm label → {matched_to, match_method,
    fuzzy_score, arm_match_status}.
    """
    arm_norm = [normalize_arm_label(a) for a in arm_labels]
    ae_norm = [normalize_arm_label(g) for g in ae_group_titles]
    ae_used = set()
    result: Dict[str, dict] = {}

    # Pass 1: exact normalized match
    for a_label, a_n in zip(arm_labels, arm_norm):
        for i, g_n in enumerate(ae_norm):
            if i in ae_used:
                continue
            if a_n == g_n:
                result[a_label] = {
                    "matched_to": ae_group_titles[i],
                    "match_method": "exact_normalized",
                    "fuzzy_score": None,
                    "arm_match_status": "ok",
                }
                ae_used.add(i)
                break

    # Pass 2: fuzzy
    for a_label, a_n in zip(arm_labels, arm_norm):
        if a_label in result:
            continue
        best_i, best_score = -1, -1
        for i, g_n in enumerate(ae_norm):
            if i in ae_used:
                continue
            score = fuzz.partial_token_set_ratio(a_n, g_n)
            if score > best_score:
                best_score, best_i = score, i
        if best_i >= 0 and best_score >= FUZZY_ARM_THRESHOLD:
            result[a_label] = {
                "matched_to": ae_group_titles[best_i],
                "match_method": "fuzzy",
                "fuzzy_score": best_score,
                "arm_match_status": "ok",
            }
            ae_used.add(best_i)

    # Pass 3: positional — only when counts equal and some arms still unmatched
    counts_match = len(arm_labels) == len(ae_group_titles)
    for idx, a_label in enumerate(arm_labels):
        if a_label in result:
            continue
        if counts_match and idx not in ae_used:
            result[a_label] = {
                "matched_to": ae_group_titles[idx],
                "match_method": "positional",
                "fuzzy_score": None,
                "arm_match_status": "ok",
            }
            ae_used.add(idx)

    # Pass 4: flag remaining as unmatched with count_mismatch
    for a_label in arm_labels:
        if a_label in result:
            continue
        result[a_label] = {
            "matched_to": None,
            "match_method": "unmatched",
            "fuzzy_score": None,
            "arm_match_status": "count_mismatch" if not counts_match else "ambiguous",
        }

    return result
