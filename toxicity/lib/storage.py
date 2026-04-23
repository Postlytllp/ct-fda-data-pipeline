"""Phase 6 part B — write CSVs + SQLite mirror, build cohort view."""
from __future__ import annotations
from pathlib import Path
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine


def write_csvs(dfs: Dict[str, pd.DataFrame], out_dir: Path) -> Dict[str, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, df in dfs.items():
        p = out_dir / f"{name}.csv"
        df.to_csv(p, index=False)
        paths[name] = p
    return paths


def write_sqlite(dfs: Dict[str, pd.DataFrame], db_path: Path) -> None:
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        for name, df in dfs.items():
            df.to_sql(name, conn, if_exists="replace", index=False)


def build_cohort_view(trials: pd.DataFrame, arms: pd.DataFrame,
                      demog: pd.DataFrame, ai_agg: pd.DataFrame,
                      ae_long: pd.DataFrame) -> pd.DataFrame:
    keep_arms = arms[(arms["has_any_ae"] == True) & (arms["has_lung_cancer_drug_match"] == True)]
    keep_demog = demog[demog["passes_diversity"] == True]

    # Demographics at trial level if arm_label is null, else at arm level
    demog_trial = keep_demog[keep_demog["arm_label"].isna()].drop(columns=["arm_label"])
    demog_arm = keep_demog[keep_demog["arm_label"].notna()]

    arms_trial = keep_arms.merge(demog_trial, on="nct_id", how="inner")
    arms_both = keep_arms.merge(demog_arm, on=["nct_id", "arm_label"], how="inner")
    arms_with_demog = pd.concat([arms_trial, arms_both], ignore_index=True)

    joined = (arms_with_demog
              .merge(trials, on="nct_id", how="left", suffixes=("", "_trial"))
              .merge(ai_agg, on=["nct_id", "arm_label"], how="left")
              .merge(ae_long, on=["nct_id", "arm_label"], how="inner"))
    keep_cols = [c for c in [
        "nct_id", "phase", "arm_label", "regimen_display",
        "primary_oncology_drugs", "backbone_drugs",
        "inferred_population", "demog_tier", "demog_confidence",
        "ae_category", "meddra_term", "organ_system",
        "affected_count", "at_risk_count",
        "sponsor_name", "lead_sponsor_country",
    ] if c in joined.columns]
    return joined[keep_cols]
