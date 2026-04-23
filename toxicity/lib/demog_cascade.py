"""Run the four-tier demographic cascade per trial. Higher tier always wins."""
from __future__ import annotations
from typing import Callable, List, Optional
import pandas as pd

from lib.demog_tier_a import tier_a1_per_arm, tier_a1_trial_level, tier_a2_country
from lib.demog_tier_b import tier_b1_text_regex, needs_b2_llm
from lib.demog_tier_cd import tier_c_location, tier_d_registry
from lib.llm_client import extract_population_from_eligibility, LLMCache


_COLUMNS = ["nct_id", "arm_label", "demog_tier", "demog_confidence",
           "inferred_population", "inferred_diversity_pct",
           "demog_source_evidence", "fallback_trial_level",
           "biomarker_indirect_signal"]


def _blank_row(nct_id: str) -> dict:
    return {
        "nct_id": nct_id, "arm_label": None,
        "demog_tier": "NONE", "demog_confidence": None,
        "inferred_population": None, "inferred_diversity_pct": None,
        "demog_source_evidence": None, "fallback_trial_level": False,
        "biomarker_indirect_signal": None,
    }


def run_cascade(trials: List[dict], baseline_df: pd.DataFrame,
                monoeth_df: pd.DataFrame, *,
                llm_client: Optional[Callable] = None,
                llm_cache: Optional[LLMCache] = None) -> pd.DataFrame:
    a1_rows = tier_a1_per_arm(baseline_df) if not baseline_df.empty else pd.DataFrame()
    a1t_rows = tier_a1_trial_level(baseline_df) if not baseline_df.empty else pd.DataFrame()
    a2_rows = tier_a2_country(baseline_df, monoeth_df) if not baseline_df.empty else pd.DataFrame()

    a_by_nct = set()
    for df in (a1_rows, a1t_rows, a2_rows):
        if not df.empty:
            a_by_nct.update(df["nct_id"].tolist())

    results: List[dict] = []

    # For Tier A-resolved trials, emit their A rows directly (higher tier wins; no fallthrough).
    for df, tag in ((a1_rows, "A1"), (a1t_rows, "A1-trial"), (a2_rows, "A2")):
        if df.empty:
            continue
        for _, r in df.iterrows():
            row = _blank_row(r["nct_id"])
            row.update({
                "arm_label": r.get("arm_label"),
                "demog_tier": r["demog_tier"],
                "demog_confidence": r["demog_confidence"],
                "inferred_population": r["inferred_population"],
                "inferred_diversity_pct": r["inferred_diversity_pct"],
                "demog_source_evidence": r["demog_source_evidence"],
                "fallback_trial_level": bool(r.get("fallback_trial_level", False)),
            })
            results.append(row)

    # B -> C -> D cascade for trials with no Tier A data
    for trial in trials:
        if trial["nct_id"] in a_by_nct:
            continue
        # B1
        b1_hits = tier_b1_text_regex(
            trial.get("brief_title"), trial.get("official_title"),
            trial.get("detailed_description"),
        )
        if b1_hits:
            h = b1_hits[0]
            row = _blank_row(trial["nct_id"])
            row.update({
                "demog_tier": "B1", "demog_confidence": h["demog_confidence"],
                "inferred_population": h["inferred_population"],
                "demog_source_evidence": h["demog_source_evidence"],
                "fallback_trial_level": True,
            })
            results.append(row)
            continue

        # B2 via LLM if triggered
        inclusion_text = trial.get("eligibility_criteria_text") or ""
        if llm_client and llm_cache and needs_b2_llm(None, inclusion_text):
            hit = extract_population_from_eligibility(
                inclusion_text, nct_id=trial["nct_id"],
                cache=llm_cache, llm_callable=llm_client,
            )
            if hit.has_population_restriction and hit.is_inclusion_criterion and hit.population:
                row = _blank_row(trial["nct_id"])
                row.update({
                    "demog_tier": "B2", "demog_confidence": "medium",
                    "inferred_population": hit.population,
                    "demog_source_evidence": hit.evidence_span,
                    "fallback_trial_level": True,
                })
                results.append(row)
                continue

        # C
        c_hit = tier_c_location(trial, monoeth_df)
        if c_hit:
            results.append({**_blank_row(trial["nct_id"]), **c_hit,
                           "biomarker_indirect_signal": None})
            continue

        # D
        d_hit = tier_d_registry(trial, monoeth_df)
        if d_hit:
            results.append({**_blank_row(trial["nct_id"]), **d_hit,
                           "biomarker_indirect_signal": None})
            continue

        # nothing
        results.append(_blank_row(trial["nct_id"]))

    return pd.DataFrame(results, columns=_COLUMNS)
