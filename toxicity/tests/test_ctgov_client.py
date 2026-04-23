import json
from pathlib import Path
import pytest
from lib.ctgov_client import fetch_all_pages

class FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
    def json(self):
        return self._payload

def test_fetch_all_pages_writes_each_page_and_stops_on_no_token(tmp_path, monkeypatch):
    pages = [
        {"totalCount": 3, "nextPageToken": "t1", "studies": [{"nct": "A"}]},
        {"nextPageToken": "t2", "studies": [{"nct": "B"}]},
        {"studies": [{"nct": "C"}]},
    ]
    calls = []
    def fake_get(url, params, timeout):
        calls.append(dict(params))
        return FakeResp(pages[len(calls) - 1])
    monkeypatch.setattr("lib.ctgov_client.requests.get", fake_get)

    essie = "(x)"
    out = fetch_all_pages(essie, cache_dir=tmp_path, page_size=50, sleep_s=0)
    assert out["total_count"] == 3
    assert out["pages_written"] == 3
    for i in (1, 2, 3):
        assert (tmp_path / f"page_{i:03d}.json").exists()

    # first call requests countTotal, subsequent calls include pageToken
    assert calls[0]["countTotal"] == "true"
    assert "pageToken" not in calls[0]
    assert calls[1]["pageToken"] == "t1"
    assert calls[2]["pageToken"] == "t2"

def test_fetch_all_pages_resumes_from_cache(tmp_path, monkeypatch):
    # Pre-existing page_001.json with a nextPageToken
    (tmp_path / "page_001.json").write_text(json.dumps(
        {"totalCount": 2, "nextPageToken": "t1", "studies": [{"nct": "A"}]}
    ))
    remaining = [{"studies": [{"nct": "B"}]}]
    calls = []
    def fake_get(url, params, timeout):
        calls.append(dict(params))
        return FakeResp(remaining[len(calls) - 1])
    monkeypatch.setattr("lib.ctgov_client.requests.get", fake_get)

    out = fetch_all_pages("(x)", cache_dir=tmp_path, page_size=50, sleep_s=0)
    # Exactly one new network call, continuing from t1
    assert len(calls) == 1
    assert calls[0]["pageToken"] == "t1"
    assert out["pages_written"] == 2
    assert (tmp_path / "page_002.json").exists()

def test_fetch_all_pages_is_noop_when_cache_complete(tmp_path, monkeypatch):
    (tmp_path / "page_001.json").write_text(json.dumps(
        {"totalCount": 1, "studies": [{"nct": "A"}]}
    ))
    def fake_get(url, params, timeout):  # should never be called
        raise AssertionError("network should not be hit")
    monkeypatch.setattr("lib.ctgov_client.requests.get", fake_get)

    out = fetch_all_pages("(x)", cache_dir=tmp_path, page_size=50, sleep_s=0)
    assert out["pages_written"] == 1
    assert out["total_count"] == 1
