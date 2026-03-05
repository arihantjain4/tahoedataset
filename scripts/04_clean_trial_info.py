"""
Script 04: Clean Trial Info columns.
Owner: Shubhan

Handles: NCT ID, trial phase, current status, study source

Usage:
    python scripts/04_clean_trial_info.py
"""
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import PHASE_MAP, STATUS_MAP
from utils import load_intermediate, save_intermediate, classify_study_source

print("="*60)
print("STEP 04: Clean Trial Info")
print("="*60)

df = load_intermediate("03_clean_drugs.parquet")
print(f"Input: {len(df):,} rows")

# ──────────────────────────────────────────────
# NCT ID & STUDY SOURCE
# ──────────────────────────────────────────────
print("\n1. NCT ID and study source...")

# study_id is the primary key — extract NCT ID where applicable
df["nct_id"] = df["study_id"].where(
    df["study_id"].astype(str).str.startswith("NCT"),
    other=None
)
df["study_source"] = df["study_id"].apply(classify_study_source)

source_counts = df["study_source"].value_counts()
print(f"   Study sources:")
for source, count in source_counts.head(10).items():
    print(f"     {source}: {count:,}")

nct_count = df["nct_id"].notna().sum()
print(f"   Rows with NCT ID: {nct_count:,} ({nct_count/len(df)*100:.1f}%)")

# ──────────────────────────────────────────────
# TRIAL PHASE
# ──────────────────────────────────────────────
print("\n2. Normalizing trial phase...")

# >>> ADJUST: The phase column might be 'phase', 'study_phase', etc.
phase_col = None
for candidate in ["phase", "study_phase", "trial_phase"]:
    if candidate in df.columns:
        phase_col = candidate
        break

if phase_col:
    # Show raw values first
    raw_phases = df[phase_col].value_counts()
    print(f"   Raw phase values:")
    for val, count in raw_phases.head(15).items():
        print(f"     '{val}': {count:,}")

    # Apply mapping
    df["trial_phase"] = df[phase_col].map(PHASE_MAP)

    # For values not in the map, try to keep them as-is
    unmapped = df[phase_col].notna() & df["trial_phase"].isna()
    if unmapped.sum() > 0:
        # Try to extract phase numbers directly
        df.loc[unmapped, "trial_phase"] = df.loc[unmapped, phase_col].astype(str)
        print(f"   {unmapped.sum():,} values not in PHASE_MAP — kept as-is")

    filled = df["trial_phase"].notna().sum()
    print(f"   Phase filled: {filled:,} ({filled/len(df)*100:.1f}%)")
    print(f"   Normalized values: {df['trial_phase'].value_counts().to_dict()}")
else:
    df["trial_phase"] = None
    print(f"   WARNING: No phase column found. Available: {list(df.columns)}")

# ──────────────────────────────────────────────
# CURRENT STATUS
# ──────────────────────────────────────────────
print("\n3. Normalizing status...")

# >>> ADJUST: Status column name
status_col = None
for candidate in ["status", "overall_status", "recruitment_status", "study_status"]:
    if candidate in df.columns:
        status_col = candidate
        break

if status_col:
    raw_statuses = df[status_col].value_counts()
    print(f"   Raw status values:")
    for val, count in raw_statuses.head(15).items():
        print(f"     '{val}': {count:,}")

    df["current_status"] = df[status_col].map(STATUS_MAP).fillna(df[status_col])

    filled = df["current_status"].notna().sum()
    print(f"   Status filled: {filled:,} ({filled/len(df)*100:.1f}%)")
else:
    df["current_status"] = None
    print(f"   WARNING: No status column found. Available: {list(df.columns)}")

# ──────────────────────────────────────────────
# SAVE
# ──────────────────────────────────────────────
save_intermediate(df, "04_clean_trial_info.parquet")
