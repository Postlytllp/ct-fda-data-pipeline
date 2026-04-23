"""Paginated CT.gov v2 fetcher with durable disk cache."""
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Optional

import requests

from lib.config import CTGOV_BASE_URL
from lib.ctgov_query import build_query_params


def _load_latest_cached(cache_dir: Path) -> tuple[int, Optional[str], Optional[int]]:
    """Return (num_pages_present, next_token_from_last_page, total_count_known)."""
    pages = sorted(cache_dir.glob("page_*.json"))
    if not pages:
        return 0, None, None
    last = json.loads(pages[-1].read_text(encoding="utf-8"))
    first = json.loads(pages[0].read_text(encoding="utf-8"))
    total = first.get("totalCount")
    return len(pages), last.get("nextPageToken"), total


def fetch_all_pages(essie_expr: str, cache_dir: Path, page_size: int = 200,
                    sleep_s: float = 0.5) -> Dict[str, object]:
    """Fetch all CT.gov pages for the given Essie OR expression, caching each
    page as `page_NNN.json` before requesting the next. Idempotent on re-run.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    n_pages, next_token, total_count = _load_latest_cached(cache_dir)

    if n_pages > 0 and not next_token:
        return {"pages_written": n_pages, "total_count": total_count}

    page_idx = n_pages + 1
    while True:
        params = build_query_params(
            essie_expr=essie_expr, page_size=page_size,
            page_token=next_token,
        )
        resp = requests.get(CTGOV_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        (cache_dir / f"page_{page_idx:03d}.json").write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8"
        )
        if total_count is None:
            total_count = data.get("totalCount")
        next_token = data.get("nextPageToken")
        page_idx += 1
        if not next_token:
            break
        if sleep_s:
            time.sleep(sleep_s)

    return {"pages_written": page_idx - 1, "total_count": total_count}
