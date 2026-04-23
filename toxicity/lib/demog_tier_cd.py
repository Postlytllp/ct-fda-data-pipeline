"""Tier C (location) + Tier D (registry) of demographic cascade."""
from __future__ import annotations
from typing import Dict, List, Optional
import pandas as pd


def _lookup_country(name: str, monoeth_df: pd.DataFrame) -> Optional[pd.Series]:
    if not name:
        return None
    hit = monoeth_df[monoeth_df["country_name"].str.lower() == name.lower()]
    if hit.empty:
        return None
    return hit.iloc[0]


def _same_region(countries: List[str], monoeth_df: pd.DataFrame) -> Optional[str]:
    regions = set()
    for c in countries:
        row = _lookup_country(c, monoeth_df)
        if row is None:
            return None
        regions.add(row["region"])
    if len(regions) == 1:
        return regions.pop()
    return None


def tier_c_location(trial: Dict, monoeth_df: pd.DataFrame) -> Optional[Dict]:
    sites = trial.get("site_countries") or []
    # C1
    if len(sites) == 1:
        row = _lookup_country(sites[0], monoeth_df)
        if row is not None and not bool(row["in_diverse_exclusion_list"]):
            return {
                "nct_id": trial["nct_id"],
                "arm_label": None,
                "demog_tier": "C1",
                "demog_confidence": "medium",
                "inferred_population": row["dominant_ancestry"],
                "inferred_diversity_pct": None,
                "demog_source_evidence": f"site country: {row['country_name']}",
                "fallback_trial_level": True,
            }
        return None
    # C2
    if len(sites) > 1:
        # if any site is in the diverse list, bail
        for c in sites:
            row = _lookup_country(c, monoeth_df)
            if row is None or bool(row["in_diverse_exclusion_list"]):
                return None
        region = _same_region(sites, monoeth_df)
        if region:
            return {
                "nct_id": trial["nct_id"],
                "arm_label": None,
                "demog_tier": "C2",
                "demog_confidence": "medium",
                "inferred_population": region,
                "inferred_diversity_pct": None,
                "demog_source_evidence": f"sites: {', '.join(sites)}",
                "fallback_trial_level": True,
            }
        return None
    # C3 — no sites; sponsor-country fallback
    sc = trial.get("lead_sponsor_country")
    if sc:
        row = _lookup_country(sc, monoeth_df)
        if row is not None and not bool(row["in_diverse_exclusion_list"]):
            return {
                "nct_id": trial["nct_id"],
                "arm_label": None,
                "demog_tier": "C3",
                "demog_confidence": "low",
                "inferred_population": row["dominant_ancestry"],
                "inferred_diversity_pct": None,
                "demog_source_evidence": f"sponsor country: {sc}",
                "fallback_trial_level": True,
            }
    return None


_REGISTRY_TO_COUNTRY = {
    "CTRI": "India",
    "JRCT": "Japan",
    "CFDA": "China",
    "NMPA": "China",
    "EudraCT": None,       # multinational — skip
    "ANZCTR": "Australia", # mixed, but included per spec
}


def tier_d_registry(trial: Dict, monoeth_df: pd.DataFrame) -> Optional[Dict]:
    for sid in (trial.get("secondary_ids") or []):
        domain = str(sid.get("domain") or sid.get("type") or "").upper()
        for key, country in _REGISTRY_TO_COUNTRY.items():
            if key.upper() in domain and country:
                row = _lookup_country(country, monoeth_df)
                if row is None or bool(row["in_diverse_exclusion_list"]):
                    continue
                return {
                    "nct_id": trial["nct_id"],
                    "arm_label": None,
                    "demog_tier": "D1",
                    "demog_confidence": "low",
                    "inferred_population": row["dominant_ancestry"],
                    "inferred_diversity_pct": None,
                    "demog_source_evidence": f"secondary_id {key}: {sid.get('id')}",
                    "fallback_trial_level": True,
                }
    return None
