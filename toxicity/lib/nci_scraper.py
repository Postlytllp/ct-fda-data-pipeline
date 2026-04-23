"""Scrape NCI drug-list pages for lung cancer."""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

# Section IDs used on the NCI lung-cancer drug page (as of 2026-04).
_SECTION_IDS = frozenset(
    {
        "drugs-approved-for-non-small-cell-lung-cancer",
        "drug-combinations-used-to-treat-non-small-cell-lung-cancer",
        "drugs-approved-for-small-cell-lung-cancer",
    }
)

# Mapping from subtype name to the aria-labelledby IDs that belong to it.
_SUBTYPE_SECTION_IDS: Dict[str, frozenset] = {
    "NSCLC": frozenset(
        {
            "drugs-approved-for-non-small-cell-lung-cancer",
            # NCI groups the combination-regimen list under NSCLC (combos are
            # not listed for SCLC on this page).
            "drug-combinations-used-to-treat-non-small-cell-lung-cancer",
        }
    ),
    "SCLC": frozenset(
        {
            "drugs-approved-for-small-cell-lung-cancer",
        }
    ),
}

# Section ID that contains drug-combination regimens (e.g. CARBOPLATIN-TAXOL).
_COMBO_SECTION_ID = "drug-combinations-used-to-treat-non-small-cell-lung-cancer"

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

# Regex to split combination entries like CARBOPLATIN-TAXOL-BEVACIZUMAB.
_COMBO_SPLIT_RE = re.compile(r"\s*(?:-|\+|\sand\s|\swith\s)\s*")


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


def parse_nci_drug_page(
    html: str, subtype: Optional[str] = None
) -> List[Dict[str, str]]:
    """Parse an NCI 'Drugs Approved for ...' page into drug entries.

    Each entry: {"name": str, "kind": "generic" | "brand"}.

    The NCI lung page (cancer.gov/about-cancer/treatment/drugs/lung) lists
    all approved drugs as ``<a href="/about-cancer/treatment/drugs/...">``
    anchors inside ``<section aria-labelledby="...">`` containers.
    Entries with the pattern ``Brand (Generic)`` produce a brand entry for
    the trade name and a generic entry for the INN (salt suffixes stripped).
    Plain entries (no parenthetical brand prefix) are treated as generics.

    Args:
        html: Raw HTML of the NCI lung cancer drug page.
        subtype: If "NSCLC", return only drugs from the NSCLC section(s).
                 If "SCLC", return only drugs from the SCLC section(s).
                 If None (default), return all drugs from both sections.
    """
    if subtype is not None and subtype not in _SUBTYPE_SECTION_IDS:
        raise ValueError(
            f"subtype must be 'NSCLC', 'SCLC', or None; got {subtype!r}"
        )

    active_ids = _SUBTYPE_SECTION_IDS[subtype] if subtype else _SECTION_IDS

    soup = BeautifulSoup(html, "lxml")
    entries: List[Dict[str, str]] = []
    seen: set = set()

    for section_id in active_ids:
        h2 = soup.find("h2", id=section_id)
        if h2 is None:
            continue
        section = h2.parent
        is_combo_section = section_id == _COMBO_SECTION_ID

        # Collect all drug anchors for this section so we can look ahead/behind
        # when handling split-anchor markup (e.g. the Enhertu triple-anchor).
        anchors = [
            a for a in section.find_all("a", href=True)
            if "/about-cancer/treatment/drugs/" in a.get("href", "")
        ]

        skip_indices: set = set()
        for idx, a in enumerate(anchors):
            if idx in skip_indices:
                continue

            raw = unicodedata.normalize("NFKC", a.get_text(strip=True)).strip()

            # --- C1: handle split-anchor markup for entries like:
            #   <a>Enhertu\xa0(</a><a>Fam-Trastuzumab Deruxtecan-nxki</a><a>)</a>
            # Detect: current anchor ends with '(' (and has no ')'),
            # and the anchor two positions later is just ')'.
            if raw.endswith("(") and ")" not in raw:
                if (
                    idx + 2 < len(anchors)
                    and unicodedata.normalize(
                        "NFKC", anchors[idx + 2].get_text(strip=True)
                    ).strip() == ")"
                ):
                    # Merge all three into one brand-form string.
                    brand_part = raw[:-1].strip()   # drop trailing '('
                    generic_part = unicodedata.normalize(
                        "NFKC", anchors[idx + 1].get_text(strip=True)
                    ).strip()
                    merged = f"{brand_part} ({generic_part})"
                    skip_indices.add(idx + 1)
                    skip_indices.add(idx + 2)
                    raw = merged
                else:
                    # Stray '(' with no matching closing anchor — drop it.
                    raw = raw[:-1].strip()

            # Strip a leading ')' with no matching '('.
            if raw.startswith(")") and "(" not in raw:
                raw = raw[1:].strip()

            # Skip fragments that contain no letter at all.
            if not re.search(r"[A-Za-z]", raw):
                continue

            # --- C2: combination-section entries are split into components.
            if is_combo_section:
                components = _COMBO_SPLIT_RE.split(raw)
                for comp in components:
                    norm = _strip_salts(_normalise(comp))
                    if norm:
                        _add(entries, seen, norm, "generic")
                continue

            # Normal (non-combination) entry processing.
            m = _BRAND_RE.match(raw)
            if m:
                brand_norm = _normalise(m.group(1))
                _add(entries, seen, brand_norm, "brand")
                generic_norm = _strip_salts(_normalise(m.group(2)))
                _add(entries, seen, generic_norm, "generic")
            else:
                norm = _strip_salts(_normalise(raw))
                if norm:
                    _add(entries, seen, norm, "generic")

    return entries
