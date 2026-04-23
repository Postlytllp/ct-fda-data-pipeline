"""Phase 6 part A — AE per-arm summary and cleaned long view."""
from __future__ import annotations
import pandas as pd


def build_ae_arm_summary(ae_raw: pd.DataFrame) -> pd.DataFrame:
    if ae_raw.empty:
        return pd.DataFrame(columns=[
            "nct_id", "arm_label", "total_serious_affected", "total_other_affected",
            "total_at_risk", "distinct_serious_terms", "distinct_other_terms",
            "serious_ae_rate", "ae_events_per_participant",
        ])
    df = ae_raw.copy()
    df["affected_count"] = pd.to_numeric(df["affected_count"], errors="coerce").fillna(0)
    df["at_risk_count"] = pd.to_numeric(df["at_risk_count"], errors="coerce").fillna(0)
    grouped = df.groupby(["nct_id", "arm_label"])
    out = pd.DataFrame({
        "total_serious_affected": grouped.apply(
            lambda g: g.loc[g["ae_category"] == "SERIOUS", "affected_count"].sum(),
            include_groups=False),
        "total_other_affected": grouped.apply(
            lambda g: g.loc[g["ae_category"] == "OTHER", "affected_count"].sum(),
            include_groups=False),
        "total_at_risk": grouped["at_risk_count"].max(),
        "distinct_serious_terms": grouped.apply(
            lambda g: g.loc[g["ae_category"] == "SERIOUS", "meddra_term"].nunique(),
            include_groups=False),
        "distinct_other_terms": grouped.apply(
            lambda g: g.loc[g["ae_category"] == "OTHER", "meddra_term"].nunique(),
            include_groups=False),
    }).reset_index()
    out["serious_ae_rate"] = out.apply(
        lambda r: (r["total_serious_affected"] / r["total_at_risk"])
                  if r["total_at_risk"] else 0.0, axis=1)
    out["ae_events_per_participant"] = out.apply(
        lambda r: ((r["total_serious_affected"] + r["total_other_affected"]) / r["total_at_risk"])
                  if r["total_at_risk"] else 0.0, axis=1)
    return out


def build_ae_long(ae_raw: pd.DataFrame, arms_df: pd.DataFrame) -> pd.DataFrame:
    if ae_raw.empty or arms_df.empty:
        return ae_raw.iloc[0:0].copy()
    return ae_raw.merge(arms_df[["nct_id", "arm_label"]], on=["nct_id", "arm_label"], how="inner")
