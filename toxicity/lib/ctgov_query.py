"""Essie OR expression builder + URL-length-safe batching for CT.gov v2."""
from __future__ import annotations
from typing import Dict, List, Optional


def _quote(alias: str) -> str:
    return f'"{alias}"' if " " in alias else alias


def build_essie_or(aliases: List[str]) -> str:
    return "(" + " OR ".join(_quote(a) for a in aliases) + ")"


def split_aliases_by_url_budget(aliases: List[str], max_bytes: int) -> List[List[str]]:
    batches: List[List[str]] = []
    cur: List[str] = []
    for a in aliases:
        trial = cur + [a]
        if len(build_essie_or(trial).encode("utf-8")) > max_bytes and cur:
            batches.append(cur)
            cur = [a]
        else:
            cur = trial
    if cur:
        batches.append(cur)
    return batches


def build_query_params(essie_expr: str, page_size: int,
                       page_token: Optional[str]) -> Dict[str, str]:
    params: Dict[str, str] = {
        "query.intr": essie_expr,
        "aggFilters": "results:with",
        "filter.overallStatus": "COMPLETED,TERMINATED",
        "pageSize": page_size,
        "format": "json",
    }
    if page_token:
        params["pageToken"] = page_token
    else:
        params["countTotal"] = "true"
    return params
