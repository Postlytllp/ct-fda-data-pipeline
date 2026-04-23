from pathlib import Path
from tests.conftest import FIXTURES
from lib.nci_scraper import parse_nci_drug_page


def test_parse_nsclc_fixture_extracts_known_drugs():
    html = (FIXTURES / "nci_nsclc.html").read_text(encoding="utf-8")
    drugs = parse_nci_drug_page(html)
    names = {d["name"].lower() for d in drugs}
    # pembrolizumab and osimertinib are NSCLC-approved and listed on the NCI page
    assert "pembrolizumab" in names
    assert "osimertinib" in names
    # brand names should appear as separate entries when listed
    assert all("name" in d and "kind" in d for d in drugs)
    assert {d["kind"] for d in drugs}.issubset({"generic", "brand"})


def test_parse_sclc_fixture_extracts_known_drugs():
    html = (FIXTURES / "nci_sclc.html").read_text(encoding="utf-8")
    drugs = parse_nci_drug_page(html)
    names = {d["name"].lower() for d in drugs}
    assert "etoposide" in names
    assert "topotecan" in names
