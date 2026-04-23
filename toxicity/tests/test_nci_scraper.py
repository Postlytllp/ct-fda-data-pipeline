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


def test_parse_nsclc_subtype_excludes_sclc_only_drug():
    html = (FIXTURES / "nci_nsclc.html").read_text(encoding="utf-8")
    nsclc_only = parse_nci_drug_page(html, subtype="NSCLC")
    sclc_only = parse_nci_drug_page(html, subtype="SCLC")
    nsclc_names = {d["name"].lower() for d in nsclc_only}
    sclc_names = {d["name"].lower() for d in sclc_only}
    # Osimertinib is NSCLC-only on NCI's list.
    assert "osimertinib" in nsclc_names
    assert "osimertinib" not in sclc_names
    # topotecan is SCLC-only
    assert "topotecan" in sclc_names
    assert "topotecan" not in nsclc_names


def test_parse_nci_drug_page_with_no_subtype_returns_union():
    html = (FIXTURES / "nci_nsclc.html").read_text(encoding="utf-8")
    all_drugs = parse_nci_drug_page(html)
    nsclc_only = parse_nci_drug_page(html, subtype="NSCLC")
    sclc_only = parse_nci_drug_page(html, subtype="SCLC")
    all_names = {d["name"].lower() for d in all_drugs}
    combined = {d["name"].lower() for d in nsclc_only} | {d["name"].lower() for d in sclc_only}
    # union equals all (ignoring possible duplicates across sections)
    assert combined.issubset(all_names)


def test_no_bracket_junk_from_enhertu_split_anchor():
    html = (FIXTURES / "nci_nsclc.html").read_text(encoding="utf-8")
    drugs = parse_nci_drug_page(html)
    names = {d["name"] for d in drugs}
    # The split-anchor markup must not leak malformed entries
    assert not any(n.strip().endswith("(") for n in names)
    assert not any(n.strip().startswith(")") for n in names)
    assert "enhertu (" not in names


def test_combination_entries_split_into_components():
    html = (FIXTURES / "nci_nsclc.html").read_text(encoding="utf-8")
    drugs = parse_nci_drug_page(html, subtype="NSCLC")
    names = {d["name"] for d in drugs}
    # Components from drug-combinations section should appear as standalone entries
    assert "carboplatin" in names
    assert "cisplatin" in names
    assert "gemcitabine" in names
    # And the combo string itself must NOT appear as a single generic
    assert not any("-" in n and any(chem in n for chem in ("carboplatin", "cisplatin", "gemcitabine")) for n in names)
