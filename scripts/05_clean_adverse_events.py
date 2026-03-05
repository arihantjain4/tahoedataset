"""
Script 05: Clean Adverse Events columns.
Owner: Sumedh Kotrannavar

Handles: adverse event names, severity grades, frequency percentages

This is the hardest extraction — AE data quality varies a lot.

Usage:
    python scripts/05_clean_adverse_events.py
"""
import sys
import os
import json
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_raw, load_intermediate, save_intermediate

print("="*60)
print("STEP 05: Clean Adverse Events")
print("="*60)

df = load_intermediate("04_clean_trial_info.parquet")
print(f"Main table: {len(df):,} rows")

# Load raw AE table
ae_raw = load_raw("adverse_events")
print(f"Raw AE table: {len(ae_raw):,} rows, columns: {list(ae_raw.columns)}")

# ──────────────────────────────────────────────
# 1. IDENTIFY KEY COLUMNS
# ──────────────────────────────────────────────
print("\n1. Identifying AE columns...")

# AE name
ae_name_col = None
for candidate in ["adverse_event_name", "ae_name", "event_name", "name", "term",
                   "adverse_event_term", "ae_term"]:
    if candidate in ae_raw.columns:
        ae_name_col = candidate
        break
print(f"   AE name column: {ae_name_col}")

# MedDRA ID
meddra_col = None
for candidate in ["meddra_id", "meddra_code", "ae_meddra_id"]:
    if candidate in ae_raw.columns:
        meddra_col = candidate
        break
print(f"   MedDRA column: {meddra_col}")

# Severity / Grade
severity_col = None
for candidate in ["severity", "grade", "ctcae_grade", "severity_grade", "ae_grade"]:
    if candidate in ae_raw.columns:
        severity_col = candidate
        break
print(f"   Severity column: {severity_col}")

# Frequency / Count
num_affected_col = None
for candidate in ["num_affected", "num_events", "count", "n_affected",
                   "subjects_affected", "event_count"]:
    if candidate in ae_raw.columns:
        num_affected_col = candidate
        break
print(f"   Num affected column: {num_affected_col}")

num_at_risk_col = None
for candidate in ["num_at_risk", "total_subjects", "n_at_risk", "subjects_at_risk",
                   "total", "group_total"]:
    if candidate in ae_raw.columns:
        num_at_risk_col = candidate
        break
print(f"   Num at risk column: {num_at_risk_col}")

# Serious flag
serious_col = None
for candidate in ["serious", "is_serious", "sae", "serious_event"]:
    if candidate in ae_raw.columns:
        serious_col = candidate
        break
print(f"   Serious flag column: {serious_col}")

# ──────────────────────────────────────────────
# 2. CLEAN AE NAMES
# ──────────────────────────────────────────────
print("\n2. Cleaning AE names...")

if ae_name_col:
    ae_raw["ae_name_clean"] = (
        ae_raw[ae_name_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .str.title()
        .replace("Nan", pd.NA)
    )
    n_unique = ae_raw["ae_name_clean"].nunique()
    print(f"   {n_unique:,} unique adverse event names")
    print(f"   Top 10:")
    for ae, count in ae_raw["ae_name_clean"].value_counts().head(10).items():
        print(f"     {ae}: {count:,}")
else:
    ae_raw["ae_name_clean"] = pd.NA
    print("   WARNING: No AE name column found!")

# ──────────────────────────────────────────────
# 3. CALCULATE FREQUENCY PERCENTAGES
# ──────────────────────────────────────────────
print("\n3. Calculating frequency percentages...")

if num_affected_col and num_at_risk_col:
    ae_raw["frequency_pct"] = (
        pd.to_numeric(ae_raw[num_affected_col], errors="coerce") /
        pd.to_numeric(ae_raw[num_at_risk_col], errors="coerce") * 100
    ).round(2)
    # Cap at 100% (data errors)
    ae_raw["frequency_pct"] = ae_raw["frequency_pct"].clip(upper=100)
    filled = ae_raw["frequency_pct"].notna().sum()
    print(f"   Frequency calculated: {filled:,} rows ({filled/len(ae_raw)*100:.1f}%)")
elif num_affected_col:
    ae_raw["frequency_pct"] = None
    print(f"   Have num_affected but no denominator — can't calculate %")
else:
    ae_raw["frequency_pct"] = None
    print(f"   No count columns found — frequency will be null")

# ──────────────────────────────────────────────
# 4. EXTRACT SEVERITY GRADES
# ──────────────────────────────────────────────
print("\n4. Extracting severity grades...")

if severity_col:
    ae_raw["severity_grade"] = pd.to_numeric(ae_raw[severity_col], errors="coerce")
    filled = ae_raw["severity_grade"].notna().sum()
    print(f"   Severity grades found: {filled:,} ({filled/len(ae_raw)*100:.1f}%)")
    print(f"   Distribution: {ae_raw['severity_grade'].value_counts().sort_index().to_dict()}")
else:
    ae_raw["severity_grade"] = None
    print(f"   No severity column — grades will need Phase 2 (PaperQA)")

# ──────────────────────────────────────────────
# 5. SERIOUS EVENT FLAG
# ──────────────────────────────────────────────
print("\n5. Serious event flag...")

if serious_col:
    ae_raw["is_serious"] = ae_raw[serious_col].astype(str).str.lower().isin(
        ["true", "1", "yes", "y", "serious"]
    )
    n_serious = ae_raw["is_serious"].sum()
    print(f"   Serious AEs: {n_serious:,} ({n_serious/len(ae_raw)*100:.1f}%)")
else:
    ae_raw["is_serious"] = None
    print(f"   No serious flag column found")

# ──────────────────────────────────────────────
# 6. AGGREGATE PER STUDY
# ──────────────────────────────────────────────
print("\n6. Aggregating AEs per study_id...")

def aggregate_aes(group):
    """Build a structured AE profile for one study."""
    ae_names = group["ae_name_clean"].dropna().unique().tolist()

    # Top AEs by frequency
    ae_with_freq = []
    if group["frequency_pct"].notna().any():
        top = (
            group[group["frequency_pct"].notna()]
            .nlargest(20, "frequency_pct")
        )
        for _, row in top.iterrows():
            entry = {"name": row["ae_name_clean"], "frequency_pct": row["frequency_pct"]}
            if pd.notna(row.get("severity_grade")):
                entry["grade"] = int(row["severity_grade"])
            ae_with_freq.append(entry)

    # Severity distribution
    severity_dist = {}
    if group["severity_grade"].notna().any():
        severity_dist = group["severity_grade"].value_counts().sort_index().to_dict()
        severity_dist = {f"grade_{int(k)}": int(v) for k, v in severity_dist.items()}

    serious_count = None
    if group["is_serious"].notna().any():
        serious_count = int(group["is_serious"].sum())

    return pd.Series({
        "adverse_event_names": "; ".join(ae_names[:30]),
        "ae_count": len(ae_names),
        "ae_frequency_data": json.dumps(ae_with_freq) if ae_with_freq else None,
        "severity_grades": json.dumps(severity_dist) if severity_dist else None,
        "serious_ae_count": serious_count,
        "has_high_grade": bool((group["severity_grade"] >= 3).any()) if group["severity_grade"].notna().any() else None,
    })

ae_summary = ae_raw.groupby("study_id").apply(aggregate_aes).reset_index()
print(f"   AE summaries for {len(ae_summary):,} studies")

# ──────────────────────────────────────────────
# 7. MERGE BACK TO MAIN TABLE
# ──────────────────────────────────────────────
print("\n7. Merging AE data back to main table...")

df = df.merge(ae_summary, on="study_id", how="left")

has_ae = df["adverse_event_names"].notna().sum()
print(f"   Rows with AE data: {has_ae:,} ({has_ae/len(df)*100:.1f}%)")

save_intermediate(df, "05_clean_ae.parquet")
