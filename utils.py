"""
Tahoe Phase 1 — Shared Utilities
"""
import pandas as pd
import re
import json
import os
from config import (
    ORGAN_SYSTEM_KEYWORDS, ROUTE_PATTERNS, FREQUENCY_PATTERNS,
    RAW_DIR, INTERMEDIATE_DIR
)


def load_raw(table_name: str) -> pd.DataFrame:
    """Load a raw parquet table by name."""
    path = os.path.join(RAW_DIR, f"{table_name}.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Raw table not found: {path}. Run 01_download_and_profile.py first.")
    return pd.read_parquet(path)


def load_intermediate(filename: str) -> pd.DataFrame:
    """Load an intermediate parquet file."""
    path = os.path.join(INTERMEDIATE_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Intermediate file not found: {path}. Run previous scripts first.")
    return pd.read_parquet(path)


def save_intermediate(df: pd.DataFrame, filename: str):
    """Save a dataframe to intermediate dir."""
    path = os.path.join(INTERMEDIATE_DIR, filename)
    df.to_parquet(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows, {len(df.columns)} cols)")


def profile_table(df: pd.DataFrame, name: str) -> dict:
    """Profile a dataframe and return stats dict."""
    stats = {
        "name": name,
        "rows": len(df),
        "columns": list(df.columns),
        "column_stats": {}
    }
    for col in df.columns:
        null_pct = round(df[col].isnull().mean() * 100, 1)
        nunique = df[col].nunique()
        dtype = str(df[col].dtype)
        sample = None
        non_null = df[col].dropna()
        if len(non_null) > 0:
            sample = str(non_null.iloc[0])[:100]
        stats["column_stats"][col] = {
            "null_pct": null_pct,
            "nunique": nunique,
            "dtype": dtype,
            "sample": sample,
        }
    return stats


def print_profile(stats: dict):
    """Pretty-print profiling stats."""
    print(f"\n{'='*60}")
    print(f"TABLE: {stats['name']}")
    print(f"{'='*60}")
    print(f"  Rows: {stats['rows']:,}")
    print(f"  Columns ({len(stats['columns'])}): {stats['columns']}")
    print(f"  Column details:")
    for col, info in stats["column_stats"].items():
        fill = 100 - info["null_pct"]
        bar = "█" * int(fill / 5) + "░" * (20 - int(fill / 5))
        print(f"    {col}")
        print(f"      [{bar}] {fill:.1f}% filled | {info['nunique']:,} unique | {info['dtype']}")
        if info["sample"]:
            print(f"      Sample: {info['sample']}")


def classify_study_source(study_id: str) -> str:
    """Classify the registry source from study_id prefix."""
    if pd.isna(study_id):
        return "unknown"
    sid = str(study_id)
    if sid.startswith("NCT"):
        return "clinicaltrials.gov"
    elif sid.startswith("EUCTR"):
        return "eu_clinical_trials"
    elif sid.startswith("JPRN"):
        return "japan_registry"
    elif sid.startswith("ACTRN"):
        return "australia_nz"
    elif sid.startswith("ISRCTN"):
        return "isrctn"
    elif sid.startswith("ChiCTR"):
        return "china_registry"
    elif sid.startswith("KCT"):
        return "korea_registry"
    elif sid.startswith("CTRI"):
        return "india_registry"
    elif sid.startswith("DRKS"):
        return "german_registry"
    elif sid.startswith("IRCT"):
        return "iran_registry"
    else:
        return "other"


def parse_dose_from_text(text: str) -> dict:
    """
    Extract dose amount, unit, route, and frequency from free-text
    intervention descriptions like "Pembrolizumab 200mg IV every 3 weeks".
    """
    if pd.isna(text) or not text:
        return {"dose_amount": None, "dose_unit": None, "route": None, "frequency": None}

    text_lower = str(text).lower().strip()
    result = {}

    # ── Dose amount + unit ──
    dose_match = re.search(
        r"(\d+\.?\d*)\s*(mg/kg|mg/m2|mg|mcg|µg|ug|g|ml|iu|units?|mmol)\b",
        text_lower
    )
    if dose_match:
        result["dose_amount"] = float(dose_match.group(1))
        result["dose_unit"] = dose_match.group(2)
    else:
        result["dose_amount"] = None
        result["dose_unit"] = None

    # ── Route ──
    result["route"] = None
    for route_name, pattern in ROUTE_PATTERNS.items():
        if re.search(pattern, text_lower):
            result["route"] = route_name
            break

    # ── Frequency ──
    result["frequency"] = None
    for freq_name, pattern in FREQUENCY_PATTERNS.items():
        if re.search(pattern, text_lower):
            result["frequency"] = freq_name
            break

    return result


def map_ae_to_organ_system(ae_name: str) -> str:
    """Map an adverse event name to its organ system using keyword matching."""
    if pd.isna(ae_name):
        return None
    ae_lower = str(ae_name).lower().strip()
    for organ_system, keywords in ORGAN_SYSTEM_KEYWORDS.items():
        for keyword in keywords:
            if keyword in ae_lower:
                return organ_system
    return None


def safe_join_unique(series, sep="; ", max_items=20):
    """Join unique non-null values from a series into a string."""
    unique_vals = series.dropna().unique()
    if len(unique_vals) == 0:
        return None
    items = [str(v) for v in unique_vals[:max_items]]
    return sep.join(items)


def safe_first(series):
    """Return first non-null value from a series."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return None
    return non_null.iloc[0]
