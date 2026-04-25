"""Phase 5 filter flag columns. Base tables stay intact; the filtered cohort
view is produced in Phase 6 by AND-ing these flags."""
from __future__ import annotations
import pandas as pd
from lib.config import DIVERSITY_THRESHOLD


def add_has_any_ae_flag(arms_df: pd.DataFrame, ae_summary_df: pd.DataFrame) -> pd.DataFrame:
    lhs = arms_df.copy()
    if ae_summary_df.empty:
        lhs["has_any_ae"] = False
        return lhs
    agg = ae_summary_df[["nct_id", "arm_label", "total_serious_affected",
                        "total_other_affected"]].copy()
    agg["_total"] = agg[["total_serious_affected", "total_other_affected"]].sum(axis=1)
    # Use matched_to (AE group title set by arm resolver) as the join key when
    # present; fall back to arm_label otherwise. This bridges the raw arm-group
    # label (on arms_df) and the raw AE group title (on ae_df) — which may differ.
    if "matched_to" in lhs.columns:
        lhs["_ae_key"] = lhs["matched_to"].fillna(lhs["arm_label"])
    else:
        lhs["_ae_key"] = lhs["arm_label"]
    agg = agg.rename(columns={"arm_label": "_ae_key"})
    lhs = lhs.merge(agg[["nct_id", "_ae_key", "_total"]],
                    on=["nct_id", "_ae_key"], how="left")
    lhs["has_any_ae"] = lhs["_total"].fillna(0) > 0
    lhs = lhs.drop(columns=["_total", "_ae_key"])
    return lhs


_TIER_A = {"A1", "A1-trial", "A2"}
_TIER_BCD = {"B1", "B2", "C1", "C2", "C3", "D1", "D2"}


def add_passes_diversity_flag(demog_df: pd.DataFrame) -> pd.DataFrame:
    out = demog_df.copy()

    def _passes(row) -> bool:
        tier = row.get("demog_tier")
        if tier in _TIER_A:
            pct = row.get("inferred_diversity_pct")
            return pct is not None and float(pct) >= DIVERSITY_THRESHOLD
        if tier in _TIER_BCD:
            return bool(row.get("inferred_population"))
        return False

    out["passes_diversity"] = out.apply(_passes, axis=1)
    return out


def add_has_lung_cancer_drug_match_flag(arms_df: pd.DataFrame,
                                       ai_df: pd.DataFrame) -> pd.DataFrame:
    out = arms_df.copy()
    if ai_df.empty or "is_primary_oncology" not in ai_df.columns:
        out["has_lung_cancer_drug_match"] = False
        return out
    # Rename on the DataFrame after reset_index so the column always lands,
    # regardless of pandas' Series-name behavior on empty groupby results.
    agg = (ai_df.groupby(["nct_id", "arm_label"])["is_primary_oncology"]
              .any().reset_index()
              .rename(columns={"is_primary_oncology": "has_lung_cancer_drug_match"}))
    out = out.merge(agg, on=["nct_id", "arm_label"], how="left")
    out["has_lung_cancer_drug_match"] = out["has_lung_cancer_drug_match"].fillna(False)
    return out
