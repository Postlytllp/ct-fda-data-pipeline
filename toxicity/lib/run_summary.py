"""Phase 6 part C — build run_summary_YYYYMMDD.json."""
from __future__ import annotations
from typing import Dict
import pandas as pd


def build_run_summary(*, drugs_queried: int, trials: pd.DataFrame,
                      arms: pd.DataFrame, demographics: pd.DataFrame,
                      baseline_df: pd.DataFrame, ae_df: pd.DataFrame,
                      llm_calls_made: int, llm_cache_hits: int,
                      total_runtime_seconds: float) -> Dict:
    arm_passing = {}
    for flag in ("has_any_ae", "has_lung_cancer_drug_match", "arm_match_status"):
        if flag in arms.columns:
            if flag == "arm_match_status":
                arm_passing[flag] = int((arms[flag] == "ok").sum())
            else:
                arm_passing[flag] = int(arms[flag].fillna(False).astype(bool).sum())

    return {
        "drugs_queried": drugs_queried,
        "trials_returned": int(len(trials)),
        "trials_with_baseline": int(baseline_df["nct_id"].nunique())
                                 if "nct_id" in baseline_df.columns else 0,
        "trials_with_ae": int(ae_df["nct_id"].nunique())
                           if "nct_id" in ae_df.columns else 0,
        "arms_total": int(len(arms)),
        "arms_passing_each_filter": arm_passing,
        "demog_tier_distribution": ({str(k): int(v) for k, v in
                                     demographics["demog_tier"].value_counts().to_dict().items()}
                                   if "demog_tier" in demographics.columns else {}),
        "arm_match_status_distribution": ({str(k): int(v) for k, v in
                                           arms["arm_match_status"].value_counts().to_dict().items()}
                                         if "arm_match_status" in arms.columns else {}),
        "llm_calls_made": llm_calls_made,
        "llm_cache_hits": llm_cache_hits,
        "total_runtime_seconds": float(total_runtime_seconds),
    }
