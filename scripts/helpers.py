# scripts/helpers.py
import pandas as pd
from typing import Tuple

def read_csv_safe(path: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False, **kwargs)

def parse_dates(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce')
    return df

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    # Lowercase column names, strip spaces
    df.columns = [c.strip() for c in df.columns]
    # Drop fully empty rows
    df = df.dropna(how='all')
    # Remove duplicates
    df = df.drop_duplicates()
    return df
