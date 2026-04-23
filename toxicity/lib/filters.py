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
    lhs = lhs.merge(agg[["nct_id", "arm_label", "_total"]],
                    on=["nct_id", "arm_label"], how="left")
    lhs["has_any_ae"] = lhs["_total"].fillna(0) > 0
    lhs = lhs.drop(columns=["_total"])
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
    agg = (ai_df.groupby(["nct_id", "arm_label"])["is_primary_oncology"]
              .any().rename("has_lung_cancer_drug_match").reset_index())
    out = arms_df.merge(agg, on=["nct_id", "arm_label"], how="left")
    out["has_lung_cancer_drug_match"] = out["has_lung_cancer_drug_match"].fillna(False)
    return out
