"""Supplementary biomarker-selection signal regex.

Never used as a filter; written into `biomarker_indirect_signal` for
downstream interpretation (EGFR-selective trials skew Asian, etc.).
"""
from __future__ import annotations
import re
from typing import Optional

_PATTERNS = [
    (re.compile(r"\bEGFR\b[^.\n]{0,60}?(mutation|positive|sensitizing)", re.I),
     "EGFR-selective → Asian-skewed"),
    (re.compile(r"\bALK\b[^.\n]{0,40}?(positive|rearrange|fusion)", re.I),
     "ALK-selective"),
    (re.compile(r"\bKRAS\s*G12C\b", re.I),
     "KRAS G12C-selective"),
    (re.compile(r"\bROS1\b[^.\n]{0,40}?(positive|rearrange|fusion)", re.I),
     "ROS1-selective"),
    (re.compile(r"\bBRAF\s*V600[EK]?\b", re.I),
     "BRAF-V600-selective"),
    (re.compile(r"\bMET\s*(exon\s*14|amplification)\b", re.I),
     "MET-selective"),
    (re.compile(r"\bRET\s*(fusion|rearrange)", re.I),
     "RET-selective"),
]


def detect_biomarker_signal(eligibility_text: str) -> Optional[str]:
    if not eligibility_text:
        return None
    hits = []
    for rx, label in _PATTERNS:
        if rx.search(eligibility_text):
            hits.append(label)
    return "; ".join(hits) if hits else None
