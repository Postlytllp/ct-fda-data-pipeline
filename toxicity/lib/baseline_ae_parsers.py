"""Baseline + AE module parsers and regimen_key assembly for arms."""
from __future__ import annotations
import json
from typing import Any, Dict, List
import pandas as pd
from tqdm.auto import tqdm



def _safe_list(x) -> List:
    return list(x) if isinstance(x, list) else []


def parse_baseline_raw(studies: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for s in tqdm(studies, desc="baseline", leave=False):
        nct = s.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
        rs = s.get("resultsSection", {}) or {}
        bc = rs.get("baselineCharacteristicsModule", {}) or {}
        groups = {g.get("id"): g.get("title") for g in _safe_list(bc.get("groups"))}
        for meas in _safe_list(bc.get("measures")):
            m_title = meas.get("title")
            units = meas.get("unitOfMeasure")
            for cls in _safe_list(meas.get("classes")):
                for cat in _safe_list(cls.get("categories")):
                    cat_title = cat.get("title")
                    for measurement in _safe_list(cat.get("measurements")):
                        rows.append({
                            "nct_id": nct,
                            "group_id": measurement.get("groupId"),
                            "group_title": groups.get(measurement.get("groupId")),
                            "measure_title": m_title,
                            "category": cat_title,
                            "value": measurement.get("value"),
                            "source_units": units,
                        })
    return pd.DataFrame(rows)


def parse_ae_raw(studies: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for s in tqdm(studies, desc="ae", leave=False):
        nct = s.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
        rs = s.get("resultsSection", {}) or {}
        ae = rs.get("adverseEventsModule", {}) or {}
        # group id -> title
        groups = {g.get("id"): g.get("title") for g in _safe_list(ae.get("eventGroups"))}
        for section_key, category in (("seriousEvents", "SERIOUS"), ("otherEvents", "OTHER")):
            for ev in _safe_list(ae.get(section_key)):
                organ = ev.get("organSystem")
                term = ev.get("term")
                for st in _safe_list(ev.get("stats")):
                    gid = st.get("groupId")
                    rows.append({
                        "nct_id": nct,
                        "raw_group_id": gid,
                        "arm_label": groups.get(gid),
                        "ae_category": category,
                        "meddra_term": term,
                        "organ_system": organ,
                        "affected_count": st.get("numAffected"),
                        "at_risk_count": st.get("numAtRisk"),
                        "source_vocab": f"MedDRA {ae.get('frequencyCriteria') or ''}".strip(),
                    })
    df = pd.DataFrame(rows)
    for col in ("affected_count", "at_risk_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def annotate_regimen_on_arms(arms_df: pd.DataFrame, ai_df: pd.DataFrame) -> pd.DataFrame:
    """Add regimen_key and regimen_display columns to arms_df.

    regimen_key: sorted pipe-joined RxCUIs (or unknown:<raw_name> for unharmonized).
    regimen_display: sorted pipe-joined generic names (or raw name for unharmonized).
    """
    out = arms_df.copy()
    if ai_df.empty:
        out["regimen_key"] = None
        out["regimen_display"] = None
        return out

    # Pre-group ai_df once; constant-time lookup per arm.
    grouped = {k: sub for k, sub in ai_df.groupby(["nct_id", "arm_label"], sort=False)}

    keys = []
    displays = []
    rows_iter = tqdm(out.iterrows(), total=len(out), desc="regimen", leave=False)
    for _, row in rows_iter:
        sel = grouped.get((row["nct_id"], row["arm_label"]))
        if sel is None or sel.empty:
            keys.append(None)
            displays.append(None)
            continue
        k_parts: List[str] = []
        d_parts: List[str] = []
        for _, iv in sel.iterrows():
            rxcui = iv.get("rxnorm_rxcui")
            name = iv.get("intervention_name") or ""
            if rxcui:
                k_parts.append(str(rxcui))
                d_parts.append(name.lower())
            else:
                k_parts.append(f"unknown:{name.lower()}")
                d_parts.append(name.lower())
        keys.append("|".join(sorted(k_parts)))
        displays.append("|".join(sorted(set(d_parts))))
    out["regimen_key"] = keys
    out["regimen_display"] = displays
    return out
