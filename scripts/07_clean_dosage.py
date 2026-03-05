"""
Script 07: Clean Dosage columns.

Handles: dose amount, dose unit, route, frequency, combination drugs
Mostly free-text parsing from intervention descriptions.

Usage:
    python scripts/07_clean_dosage.py
"""
import sys
import os
import re
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import ROUTE_PATTERNS, FREQUENCY_PATTERNS
from utils import load_intermediate, save_intermediate, parse_dose_from_text

print("="*60)
print("STEP 07: Clean Dosage")
print("="*60)

df = load_intermediate("06_clean_cohort.parquet")
print(f"Input: {len(df):,} rows")

# ──────────────────────────────────────────────
# 1. PARSE DOSAGE FROM INTERVENTION DESCRIPTIONS
# ──────────────────────────────────────────────
print("\n1. Parsing dosage from intervention descriptions...")

# The intervention_descriptions column was created in Script 02 from dispositions
# It contains strings like "Pembrolizumab 200mg IV every 3 weeks"

desc_col = None
for candidate in ["intervention_descriptions", "intervention_name", "dose_description_raw"]:
    if candidate in df.columns:
        desc_col = candidate
        break

if desc_col:
    # Parse each description
    parsed = df[desc_col].apply(parse_dose_from_text).apply(pd.Series)
    df["dose_amount"] = parsed["dose_amount"]
    df["dose_unit"] = parsed["dose_unit"]
    df["route"] = parsed["route"]
    df["frequency"] = parsed["frequency"]

    # Stats
    for col in ["dose_amount", "dose_unit", "route", "frequency"]:
        filled = df[col].notna().sum()
        print(f"   {col}: {filled:,} parsed ({filled/len(df)*100:.1f}%)")

    # Show sample of successful parses
    has_dose = df[df["dose_amount"].notna()].head(5)
    if len(has_dose) > 0:
        print(f"\n   Sample parses:")
        for _, row in has_dose.iterrows():
            desc = str(row.get(desc_col, ""))[:60]
            print(f"     '{desc}' → {row['dose_amount']}{row['dose_unit']}, {row['route']}, {row['frequency']}")
else:
    df["dose_amount"] = None
    df["dose_unit"] = None
    df["route"] = None
    df["frequency"] = None
    print(f"   No intervention description column found")
    print(f"   Available columns: {[c for c in df.columns if 'inter' in c.lower() or 'dose' in c.lower() or 'drug' in c.lower()]}")

# ──────────────────────────────────────────────
# 2. COMBINATION DRUG FLAG
# ──────────────────────────────────────────────
print("\n2. Combination drug flag...")

if "num_drug_arms" in df.columns:
    df["is_combination"] = df["num_drug_arms"].fillna(1) > 1
    n_combo = df["is_combination"].sum()
    print(f"   Combination trials: {n_combo:,} ({n_combo/len(df)*100:.1f}%)")
else:
    df["is_combination"] = False
    print(f"   num_drug_arms not available — defaulting to False")

# ──────────────────────────────────────────────
# 3. BUILD DOSE DESCRIPTION STRING
# ──────────────────────────────────────────────
print("\n3. Building dose description string...")

def build_dose_string(row):
    parts = []
    if pd.notna(row.get("dose_amount")):
        dose_str = f"{row['dose_amount']}"
        if pd.notna(row.get("dose_unit")):
            dose_str += f" {row['dose_unit']}"
        parts.append(dose_str)
    if pd.notna(row.get("route")):
        parts.append(row["route"])
    if pd.notna(row.get("frequency")):
        parts.append(row["frequency"])
    return ", ".join(parts) if parts else None

df["dose_summary"] = df.apply(build_dose_string, axis=1)
filled = df["dose_summary"].notna().sum()
print(f"   Dose summaries built: {filled:,} ({filled/len(df)*100:.1f}%)")

save_intermediate(df, "07_clean_dosage.parquet")
