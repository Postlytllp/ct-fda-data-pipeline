"""Tier A of demographic cascade: reported baseline data + A2 country pass.
(Full tier logic added in Task 12; this stub hosts the reference loader.)"""
from pathlib import Path
import pandas as pd

def load_monoethnic_countries(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["in_diverse_exclusion_list"] = df["in_diverse_exclusion_list"].map(
        lambda x: True if str(x).lower() in ("true", "1", "yes") else False
    )
    return df

# ---------------------------------------------------------------------------
# Tier A cascade logic (added in Task 12; stub above is Task 2 loader)
# ---------------------------------------------------------------------------

from lib.config import DIVERSITY_THRESHOLD

_RACE_TITLES = ("Race", "Race (NIH-OMB)", "Race/Ethnicity",
                "Race and Ethnicity", "Race and Ethnicity, Customized")

_COUNTRY_TITLES = ("Country of Enrollment", "Region of Enrollment", "Country")


def _tier_a_for_group(sub: pd.DataFrame) -> dict | None:
    total = sub["value"].sum()
    if total <= 0:
        return None
    max_row = sub.loc[sub["value"].idxmax()]
    pct = max_row["value"] / total
    if pct >= DIVERSITY_THRESHOLD:
        return {
            "inferred_population": max_row["category"],
            "inferred_diversity_pct": float(pct),
            "demog_confidence": "high",
            "demog_source_evidence": max_row["measure_title"],
        }
    return {
        "inferred_population": None,
        "inferred_diversity_pct": float(pct),
        "demog_confidence": None,
        "demog_source_evidence": max_row["measure_title"],
    }


def tier_a1_per_arm(baseline_df: pd.DataFrame) -> pd.DataFrame:
    out_rows = []
    race_rows = baseline_df[baseline_df["measure_title"].isin(_RACE_TITLES)]
    for (nct, gid), sub in race_rows.groupby(["nct_id", "group_id"], dropna=False):
        hit = _tier_a_for_group(sub)
        if hit is None:
            continue
        tier = "A1" if hit["inferred_population"] else "NONE"
        out_rows.append({
            "nct_id": nct,
            "arm_label": sub["group_title"].iloc[0],
            "demog_tier": tier,
            "demog_confidence": hit["demog_confidence"],
            "inferred_population": hit["inferred_population"],
            "inferred_diversity_pct": hit["inferred_diversity_pct"],
            "demog_source_evidence": hit["demog_source_evidence"],
            "fallback_trial_level": False,
        })
    return pd.DataFrame(out_rows)


def tier_a1_trial_level(baseline_df: pd.DataFrame) -> pd.DataFrame:
    out_rows = []
    race_rows = baseline_df[baseline_df["measure_title"].isin(_RACE_TITLES)]
    for nct, sub in race_rows.groupby("nct_id"):
        pooled = sub.groupby("category", as_index=False)["value"].sum()
        pooled["nct_id"] = nct
        pooled["measure_title"] = sub["measure_title"].iloc[0]
        hit = _tier_a_for_group(pooled)
        if hit is None or not hit["inferred_population"]:
            continue
        out_rows.append({
            "nct_id": nct,
            "arm_label": None,
            "demog_tier": "A1-trial",
            "demog_confidence": "high",
            "inferred_population": hit["inferred_population"],
            "inferred_diversity_pct": hit["inferred_diversity_pct"],
            "demog_source_evidence": hit["demog_source_evidence"],
            "fallback_trial_level": True,
        })
    return pd.DataFrame(out_rows)


def tier_a2_country(baseline_df: pd.DataFrame, monoethnic_df: pd.DataFrame) -> pd.DataFrame:
    country_rows = baseline_df[baseline_df["measure_title"].isin(_COUNTRY_TITLES)]
    out_rows = []
    name_to_dominant = {
        str(r["country_name"]).lower(): r for _, r in monoethnic_df.iterrows()
    }
    for (nct, gid), sub in country_rows.groupby(["nct_id", "group_id"], dropna=False):
        total = sub["value"].sum()
        if total <= 0:
            continue
        top = sub.loc[sub["value"].idxmax()]
        pct = top["value"] / total
        if pct < DIVERSITY_THRESHOLD:
            continue
        country = str(top["category"]).lower()
        ref = name_to_dominant.get(country)
        if ref is None:
            continue
        if bool(ref["in_diverse_exclusion_list"]):
            continue
        out_rows.append({
            "nct_id": nct,
            "arm_label": sub["group_title"].iloc[0] if "group_title" in sub.columns else None,
            "demog_tier": "A2",
            "demog_confidence": "high",
            "inferred_population": ref["dominant_ancestry"],
            "inferred_diversity_pct": float(pct),
            "demog_source_evidence": f"{top['measure_title']}: {top['category']}",
            "fallback_trial_level": False,
        })
    return pd.DataFrame(out_rows)
