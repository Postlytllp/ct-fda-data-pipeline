import json
from lib.ctgov_query import build_essie_or, split_aliases_by_url_budget, build_query_params

def test_build_essie_or_wraps_in_parens_and_joins_OR():
    aliases = ["pembrolizumab", "Keytruda", "osimertinib"]
    expr = build_essie_or(aliases)
    assert expr.startswith("(") and expr.endswith(")")
    assert expr == '(pembrolizumab OR Keytruda OR osimertinib)'

def test_build_essie_or_quotes_multiword_aliases():
    aliases = ["pembrolizumab", "paclitaxel injection"]
    expr = build_essie_or(aliases)
    assert '"paclitaxel injection"' in expr
    assert 'pembrolizumab' in expr

def test_split_aliases_respects_url_budget():
    aliases = [f"drug{i:03d}" for i in range(1000)]
    batches = split_aliases_by_url_budget(aliases, max_bytes=500)
    assert all(len(build_essie_or(b).encode()) <= 500 for b in batches)
    flat = [a for b in batches for a in b]
    assert flat == aliases  # preserves order, no dupes dropped

def test_build_query_params_includes_required_fields():
    params = build_query_params(essie_expr='(pembrolizumab)', page_size=200, page_token=None)
    assert params["query.intr"] == '(pembrolizumab)'
    assert params["aggFilters"] == "results:with"
    assert params["filter.overallStatus"] == "COMPLETED,TERMINATED"
    assert params["pageSize"] == 200
    assert params["countTotal"] == "true"
    assert params["format"] == "json"
    assert "pageToken" not in params

def test_build_query_params_adds_page_token_when_given():
    params = build_query_params(essie_expr='(x)', page_size=100, page_token="tok-123")
    assert params["pageToken"] == "tok-123"
    assert "countTotal" not in params  # only on first page


def test_split_aliases_defaults_to_config_budget():
    from lib.config import CTGOV_MAX_URL_BYTES
    # Not a coverage test per se — just ensures the constant hasn't silently
    # drifted back to an unsafe value that lets 414 errors through.
    assert CTGOV_MAX_URL_BYTES <= 4000, (
        "Server returned 414 when budget was 8000; keep budget ≤ 4000 to "
        "account for URL-encoding inflation."
    )
