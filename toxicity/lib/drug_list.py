"""Phase 1 part B — merge NSCLC+SCLC drug lists, harmonize, emit final CSV."""
from __future__ import annotations
import json
import re
from typing import Dict, Iterable, List, Any
import pandas as pd

_ALIAS_NOISE_RE = re.compile(
    r"\b\d+(\.\d+)?\s*(ML|MG|MCG|UNT|IU|KG|G|%)\b"
    r"|\b(Auto-Injector|Prefilled Syringe|Injection|Tablet|Capsule|"
    r"Extended Release|Oral Solution|Oral Suspension|Inhalation|"
    r"Patch|Gel|Cream|Ointment|Syrup|Drops|Lozenge|Film|Strip|Powder)\b",
    re.IGNORECASE,
)


def _alias_is_clean(alias: str) -> bool:
    """True when alias is a plausible intervention-query token.

    Rejects RxNorm SBD dose-form strings like
    "10 ML ramucirumab 10 MG/ML Injection [Cyramza]".
    """
    if not alias:
        return False
    s = alias.strip()
    if len(s) > 40:
        return False
    if _ALIAS_NOISE_RE.search(s):
        return False
    return True


def merge_drugs(nsclc: Iterable[dict], sclc: Iterable[dict]) -> List[dict]:
    seen = {}
    for src in (list(nsclc), list(sclc)):
        for d in src:
            seen.setdefault(d["name"], {"name": d["name"], "kind": d["kind"]})
    return list(seen.values())


def attach_subtypes(merged: List[dict],
                    nsclc: Iterable[dict], sclc: Iterable[dict]) -> List[dict]:
    nsclc_set = {d["name"] for d in nsclc}
    sclc_set = {d["name"] for d in sclc}
    out = []
    for d in merged:
        subtypes = []
        if d["name"] in nsclc_set:
            subtypes.append("NSCLC")
        if d["name"] in sclc_set:
            subtypes.append("SCLC")
        out.append({**d, "subtypes": subtypes})
    return out


def to_dataframe(merged_with_subtypes: List[dict],
                 harmonized: Dict[str, Dict[str, Any]],
                 snapshot_date: str) -> pd.DataFrame:
    """Collapse generics+brands to one row per canonical drug.

    A generic's row carries its brand-name aliases + RxNorm synonyms.
    Brand-only entries that did not get harmonized are dropped
    (the generic row's alias list covers them).
    """
    rows = []
    for entry in merged_with_subtypes:
        if entry["kind"] != "generic":
            continue
        name = entry["name"]
        h = harmonized.get(name) or {}
        aliases = set()
        aliases.add(name)
        for a in (h.get("all_brand_names") or []) + (h.get("all_synonyms") or []):
            if _alias_is_clean(a):
                aliases.add(a)
        rows.append({
            "canonical_name": name,
            "rxcui": h.get("rxcui"),
            "aliases": json.dumps(sorted(aliases), ensure_ascii=False),
            "subtypes": json.dumps(entry["subtypes"]),
            "source": "nci",
            "snapshot_date": snapshot_date,
        })
    return pd.DataFrame(rows, columns=[
        "canonical_name", "rxcui", "aliases", "subtypes", "source", "snapshot_date"
    ])
