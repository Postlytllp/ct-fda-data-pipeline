"""Tier A of demographic cascade: reported baseline data + A2 country pass.
(Full tier logic added in Task 12; this stub hosts the reference loader.)"""
from pathlib import Path
import pandas as pd

def load_monoethnic_countries(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["in_diverse_exclusion_list"] = df["in_diverse_exclusion_list"].map(
        lambda x: True if str(x).lower() in ("true", "1", "yes") else False
    )
    return df
