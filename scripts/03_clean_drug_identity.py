"""
Script 03: Clean Drug Identity columns.
Owner: Shreya

Handles: drug_name, drug_moa_id, drug_description, rx_normalized_name,
         fda_approved, ema_approved, pmda_approved

Usage:
    python scripts/03_clean_drug_identity.py
"""
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_intermediate, save_intermediate

print("="*60)
print("STEP 03: Clean Drug Identity")
print("="*60)

df = load_intermediate("02_joined.parquet")
print(f"Input: {len(df):,} rows")

# ──────────────────────────────────────────────
# DRUG NAME — Standardize
# ──────────────────────────────────────────────
print("\n1. Cleaning drug names...")

# >>> ADJUST: The drug name column might be 'drug_name', 'name', 'intervention_name', etc.
drug_name_col = None
for candidate in ["drug_name", "name", "drug"]:
    if candidate in df.columns:
        drug_name_col = candidate
        break

if drug_name_col:
    df["drug_name_clean"] = (
        df[drug_name_col]
        .astype(str)
        .str.strip()
        .str.title()  # "pembrolizumab" → "Pembrolizumab"
        .replace("Nan", pd.NA)
    )
    # Remove common noise strings
    noise = ["Placebo", "Not Specified", "Unknown", "Other", "Na", "None", "N/A"]
    df.loc[df["drug_name_clean"].isin(noise), "drug_name_clean"] = pd.NA

    n_unique = df["drug_name_clean"].nunique()
    n_null = df["drug_name_clean"].isna().sum()
    print(f"   {n_unique:,} unique drug names, {n_null:,} nulls removed (placebo/noise)")
else:
    print(f"   WARNING: No drug name column found. Available: {list(df.columns)}")
    df["drug_name_clean"] = pd.NA

# ──────────────────────────────────────────────
# RX NORMALIZED NAME
# ──────────────────────────────────────────────
print("\n2. RxNorm normalized name...")

rx_col = None
for candidate in ["rx_norm_name", "rxnorm_name", "rx_normalized_name", "rxcui"]:
    if candidate in df.columns:
        rx_col = candidate
        break

if rx_col:
    df["rx_normalized_name"] = df[rx_col].str.strip()
    filled = df["rx_normalized_name"].notna().sum()
    print(f"   Found column '{rx_col}': {filled:,} filled ({filled/len(df)*100:.1f}%)")
else:
    df["rx_normalized_name"] = None
    print(f"   No RxNorm column found. Available: {list(df.columns)}")

# ──────────────────────────────────────────────
# REGULATORY APPROVAL FLAGS
# ──────────────────────────────────────────────
print("\n3. Regulatory approval flags...")

for flag in ["fda_approved", "ema_approved", "pmda_approved"]:
    if flag in df.columns:
        # Convert to boolean, handle various formats
        col = df[flag]
        if col.dtype == "bool":
            pass  # already boolean
        elif col.dtype in ["int64", "float64"]:
            df[flag] = col.fillna(0).astype(bool)
        else:
            df[flag] = col.astype(str).str.lower().isin(["true", "1", "yes", "approved"])
        filled = df[flag].sum()
        print(f"   {flag}: {filled:,} approved ({filled/len(df)*100:.1f}%)")
    else:
        # Try variations
        found = False
        for variant in [flag.replace("_", ""), flag.upper(), flag.replace("approved", "approval")]:
            if variant in df.columns:
                df[flag] = df[variant].fillna(False).astype(bool)
                found = True
                print(f"   {flag}: found as '{variant}'")
                break
        if not found:
            df[flag] = False
            print(f"   {flag}: NOT FOUND — defaulting to False")

# ──────────────────────────────────────────────
# DRUG MOA ID & DESCRIPTION
# ──────────────────────────────────────────────
print("\n4. Drug MoA and description...")

# These are unlikely to be in TrialPanorama directly
# Check for DrugBank ID which could be used to look up MoA later
drugbank_col = None
for candidate in ["drugbank_id", "drugbank", "db_id"]:
    if candidate in df.columns:
        drugbank_col = candidate
        break

if drugbank_col:
    df["drugbank_id_clean"] = df[drugbank_col].str.strip()
    filled = df["drugbank_id_clean"].notna().sum()
    print(f"   DrugBank ID ('{drugbank_col}'): {filled:,} filled — can be used for MoA lookup later")
else:
    df["drugbank_id_clean"] = None
    print(f"   No DrugBank column found")

# MoA and description — flag as Phase 2 / enrichment task
moa_col = None
for candidate in ["drug_moa_id", "moa", "mechanism_of_action", "drug_moa"]:
    if candidate in df.columns:
        moa_col = candidate
        break

if moa_col:
    df["drug_moa_id"] = df[moa_col]
    filled = df["drug_moa_id"].notna().sum()
    print(f"   MoA ID found ('{moa_col}'): {filled:,} filled")
else:
    df["drug_moa_id"] = None
    print(f"   MoA ID not found — will need DrugBank lookup or Phase 2")

# Drug description
desc_col = None
for candidate in ["drug_description", "description"]:
    if candidate in df.columns:
        desc_col = candidate
        break

if desc_col:
    df["drug_description"] = df[desc_col]
    filled = df["drug_description"].notna().sum()
    print(f"   Description found ('{desc_col}'): {filled:,} filled")
else:
    df["drug_description"] = None
    print(f"   Description not found — Phase 2 enrichment")

# ──────────────────────────────────────────────
# DROP ROWS WITH NO DRUG NAME
# ──────────────────────────────────────────────
before = len(df)
df = df.dropna(subset=["drug_name_clean"])
after = len(df)
print(f"\nDropped {before - after:,} rows with no drug name")
print(f"Remaining: {after:,} rows, {df['drug_name_clean'].nunique():,} unique drugs")

save_intermediate(df, "03_clean_drugs.parquet")
