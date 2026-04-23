"""Scrape NCI drug-list pages for lung cancer."""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List

from bs4 import BeautifulSoup

# Section IDs used on the NCI lung-cancer drug page (as of 2026-04).
_SECTION_IDS = frozenset(
    {
        "drugs-approved-for-non-small-cell-lung-cancer",
        "drug-combinations-used-to-treat-non-small-cell-lung-cancer",
        "drugs-approved-for-small-cell-lung-cancer",
    }
)

# Trailing salt / dosage-form words to strip from generic names.
_SALT_RE = re.compile(
    r"\s+(?:mesylate|dimaleate|hydrochloride|phosphate|disodium|sodium|"
    r"tartrate|hydrate|dimethyl\s+sulfoxide|sulfoxide|acetate|citrate|"
    r"fumarate|maleate|tosylate|besylate|succinate|sulfate|nitrate|"
    r"bromide|chloride|gluconate|"
    r"albumin-stabilized\s+nanoparticle\s+formulation)\s*$",
    re.I,
)

# Recognise "BrandWord (Generic Part)" entries.
_BRAND_RE = re.compile(r"^(.+?)\s*\((.+)\)\s*$")


def _normalise(raw: str) -> str:
    """Unicode-normalise, lowercase, collapse whitespace."""
    s = unicodedata.normalize("NFKC", raw).strip().lower()
    return re.sub(r"\s+", " ", s)


def _strip_salts(name: str) -> str:
    """Iteratively remove trailing pharmaceutical salt / form words."""
    prev = None
    while prev != name:
        prev = name
        name = _SALT_RE.sub("", name).strip()
    return name


def parse_nci_drug_page(html: str) -> List[Dict[str, str]]:
    """Parse an NCI 'Drugs Approved for ...' page into drug entries.

    Each entry: {"name": str, "kind": "generic" | "brand"}.

    The NCI lung page (cancer.gov/about-cancer/treatment/drugs/lung) lists
    all approved drugs as ``<a href="/about-cancer/treatment/drugs/...">``
    anchors inside ``<section aria-labelledby="...">`` containers.
    Entries with the pattern ``Brand (Generic)`` produce a brand entry for
    the trade name and a generic entry for the INN (salt suffixes stripped).
    Plain entries (no parenthetical brand prefix) are treated as generics.
    """
    soup = BeautifulSoup(html, "lxml")
    entries: List[Dict[str, str]] = []
    seen: set = set()

    for section_id in _SECTION_IDS:
        h2 = soup.find("h2", id=section_id)
        if h2 is None:
            continue
        section = h2.parent
        for a in section.find_all("a", href=True):
            if "/about-cancer/treatment/drugs/" not in a.get("href", ""):
                continue
            raw = unicodedata.normalize("NFKC", a.get_text(strip=True)).strip()
            if not raw or raw in (")", "(", ""):
                continue

            m = _BRAND_RE.match(raw)
            if m:
                # Brand entry
                brand_norm = _normalise(m.group(1))
                _add(entries, seen, brand_norm, "brand")
                # Generic entry with salts stripped
                generic_norm = _strip_salts(_normalise(m.group(2)))
                _add(entries, seen, generic_norm, "generic")
            else:
                # Plain generic (strip salts for normalisation)
                norm = _strip_salts(_normalise(raw))
                if norm:
                    _add(entries, seen, norm, "generic")

    return entries


def _add(
    entries: List[Dict[str, str]],
    seen: set,
    name: str,
    kind: str,
) -> None:
    key = (name, kind)
    if key not in seen:
        seen.add(key)
        entries.append({"name": name, "kind": kind})
