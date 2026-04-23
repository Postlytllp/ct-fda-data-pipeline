"""Parse CT.gov v2 study JSON objects into base dataframes."""
from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional
import pandas as pd
from tqdm.auto import tqdm

from lib.arm_resolver import normalize_arm_label

_INTERVENTION_PREFIX_RE = re.compile(
    r"^(Drug|Biological|Other|Procedure|Device|Radiation|"
    r"Diagnostic\s+Test|Dietary\s+Supplement|Behavioral|"
    r"Combination\s+Product|Genetic)\s*:\s*",
    flags=re.IGNORECASE,
)


def _strip_intervention_prefix(name: str) -> str:
    """Remove CT.gov v2 intervention-type prefix (e.g. 'Drug: ', 'Biological: ')."""
    if not name:
        return name
    return _INTERVENTION_PREFIX_RE.sub("", name).strip()


def _get(path, d, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _safe_list(x) -> List:
    return list(x) if isinstance(x, list) else []


def parse_trials(studies: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for s in tqdm(studies, desc="trials", leave=False):
        ps = s.get("protocolSection", {})
        ident = ps.get("identificationModule", {})
        status = ps.get("statusModule", {})
        design = ps.get("designModule", {})
        desc = ps.get("descriptionModule", {})
        elig = ps.get("eligibilityModule", {})
        spon = ps.get("sponsorCollaboratorsModule", {})
        conds = ps.get("conditionsModule", {})
        cloc = ps.get("contactsLocationsModule", {})
        lead = spon.get("leadSponsor", {}) or {}
        locations = _safe_list(cloc.get("locations"))
        countries = sorted({loc.get("country") for loc in locations if loc.get("country")})
        rows.append({
            "nct_id": ident.get("nctId"),
            "brief_title": ident.get("briefTitle"),
            "official_title": ident.get("officialTitle"),
            "detailed_description": desc.get("detailedDescription"),
            "phase": ",".join(_safe_list(design.get("phases"))) or None,
            "overall_status": status.get("overallStatus"),
            "study_type": design.get("studyType"),
            "start_date": _get(("startDateStruct", "date"), status),
            "completion_date": _get(("completionDateStruct", "date"), status),
            "enrollment_actual": _get(("enrollmentInfo", "count"), design),
            "sponsor_name": lead.get("name"),
            "sponsor_class": lead.get("class"),
            "lead_sponsor_country": None,  # not exposed at lead level; set by caller if needed
            "conditions": json.dumps(_safe_list(conds.get("conditions"))),
            "keywords": json.dumps(_safe_list(conds.get("keywords"))),
            "countries": json.dumps(countries),
            "secondary_ids": json.dumps(_safe_list(ident.get("secondaryIdInfos"))),
            "lung_cancer_subtypes": json.dumps([]),  # filled downstream (Task 13)
            "eligibility_criteria_text": elig.get("eligibilityCriteria"),
            "gender": elig.get("sex"),
            "minimum_age": elig.get("minimumAge"),
            "maximum_age": elig.get("maximumAge"),
            "healthy_volunteers": elig.get("healthyVolunteers"),
        })
    return pd.DataFrame(rows)


def parse_arms(studies: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for s in tqdm(studies, desc="arms", leave=False):
        ps = s.get("protocolSection", {})
        ident = ps.get("identificationModule", {})
        nct = ident.get("nctId")
        ai = ps.get("armsInterventionsModule", {})
        for g in _safe_list(ai.get("armGroups")):
            raw = g.get("label")
            rows.append({
                "nct_id": nct,
                "arm_label": raw,
                "raw_arm_label": raw,
                "arm_type": g.get("type"),
                "arm_description": g.get("description"),
                "n_at_risk": None,  # filled by baseline_ae_parsers
                "regimen_key": None,
                "regimen_display": None,
                "match_method": None,
                "fuzzy_score": None,
                "arm_match_status": None,
            })
    return pd.DataFrame(rows)


def _alias_to_rxcui(drug_df: pd.DataFrame) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for _, r in drug_df.iterrows():
        rxcui = r.get("rxcui")
        if not rxcui:
            continue
        aliases = json.loads(r["aliases"]) if isinstance(r["aliases"], str) else (r["aliases"] or [])
        for a in aliases:
            mapping[str(a).lower()] = str(rxcui)
    return mapping


def parse_arm_interventions(studies: List[Dict[str, Any]],
                            drug_df: pd.DataFrame,
                            drug_class_df: pd.DataFrame) -> pd.DataFrame:
    alias_map = _alias_to_rxcui(drug_df)
    class_map = {
        str(r["rxcui"]): r["drug_class"]
        for _, r in drug_class_df.iterrows() if pd.notna(r.get("rxcui"))
    }
    rows = []
    for s in tqdm(studies, desc="interventions", leave=False):
        ps = s.get("protocolSection", {})
        nct = _get(("identificationModule", "nctId"), ps)
        ai = ps.get("armsInterventionsModule", {}) or {}
        interventions = _safe_list(ai.get("interventions"))
        iv_index: Dict[str, Dict] = {iv.get("name"): iv for iv in interventions if iv.get("name")}
        for g in _safe_list(ai.get("armGroups")):
            arm_label = g.get("label")
            for iv_name in _safe_list(g.get("interventionNames")):
                iv = iv_index.get(iv_name, {})
                lookup_name = _strip_intervention_prefix(iv_name)
                rxcui = alias_map.get(lookup_name.lower())
                rows.append({
                    "nct_id": nct,
                    "arm_label": arm_label,
                    "intervention_name": iv_name,
                    "intervention_type": iv.get("type"),
                    "rxnorm_rxcui": rxcui,
                    "matched_lung_cancer_drug": bool(rxcui),
                    "is_primary_oncology": bool(rxcui),
                    "drug_class": class_map.get(rxcui) if rxcui else None,
                })
    return pd.DataFrame(rows)
