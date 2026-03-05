"""
Script 06: Clean Patient Cohort columns.

Handles: disease indication, age range, sex distribution, sample size

Usage:
    python scripts/06_clean_patient_cohort.py
"""
import sys
import os
import re
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_raw, load_intermediate, save_intermediate

print("="*60)
print("STEP 06: Clean Patient Cohort")
print("="*60)

df = load_intermediate("05_clean_ae.parquet")
studies = load_raw("studies")
print(f"Main table: {len(df):,} rows")

# ──────────────────────────────────────────────
# 1. DISEASE INDICATION — already joined in Script 02
# ──────────────────────────────────────────────
print("\n1. Disease indication...")

if "disease_indications" in df.columns:
    filled = df["disease_indications"].notna().sum()
    print(f"   Already joined: {filled:,} filled ({filled/len(df)*100:.1f}%)")
else:
    df["disease_indications"] = None
    print(f"   Not available — check Script 02")

# ──────────────────────────────────────────────
# 2. AGE RANGE
# ──────────────────────────────────────────────
print("\n2. Age range...")

# Check for structured age columns in studies
age_min_col = None
for candidate in ["minimum_age", "min_age", "eligibility_min_age"]:
    if candidate in studies.columns:
        age_min_col = candidate
        break

age_max_col = None
for candidate in ["maximum_age", "max_age", "eligibility_max_age"]:
    if candidate in studies.columns:
        age_max_col = candidate
        break

if age_min_col or age_max_col:
    age_df = studies[["study_id"]].copy()

    if age_min_col:
        age_df["min_age"] = studies[age_min_col].astype(str).str.strip()
    else:
        age_df["min_age"] = None

    if age_max_col:
        age_df["max_age"] = studies[age_max_col].astype(str).str.strip()
    else:
        age_df["max_age"] = None

    # Build age range string
    def format_age_range(row):
        min_a = row["min_age"]
        max_a = row["max_age"]
        if pd.isna(min_a) and pd.isna(max_a):
            return None
        if min_a in ["nan", "None", "N/A", ""]:
            min_a = None
        if max_a in ["nan", "None", "N/A", ""]:
            max_a = None
        parts = []
        if min_a:
            parts.append(str(min_a))
        if max_a:
            parts.append(str(max_a))
        return " - ".join(parts) if parts else None

    age_df["age_range"] = age_df.apply(format_age_range, axis=1)
    df = df.merge(age_df[["study_id", "age_range"]], on="study_id", how="left")
    filled = df["age_range"].notna().sum()
    print(f"   Age range: {filled:,} filled ({filled/len(df)*100:.1f}%)")
else:
    df["age_range"] = None
    print(f"   No age columns found in studies table")
    print(f"   Studies columns: {list(studies.columns)}")

# ──────────────────────────────────────────────
# 3. SEX DISTRIBUTION
# ──────────────────────────────────────────────
print("\n3. Sex distribution...")

sex_col = None
for candidate in ["gender", "sex", "eligible_gender", "eligible_sex"]:
    if candidate in studies.columns:
        sex_col = candidate
        break

if sex_col:
    sex_df = studies[["study_id", sex_col]].rename(columns={sex_col: "sex"})
    # Normalize
    sex_map = {
        "ALL": "All",
        "MALE": "Male",
        "FEMALE": "Female",
        "All": "All",
        "Male": "Male",
        "Female": "Female",
    }
    sex_df["sex"] = sex_df["sex"].map(sex_map).fillna(sex_df["sex"])
    df = df.merge(sex_df, on="study_id", how="left")
    filled = df["sex"].notna().sum()
    print(f"   Sex: {filled:,} filled ({filled/len(df)*100:.1f}%)")
    print(f"   Distribution: {df['sex'].value_counts().to_dict()}")
else:
    df["sex"] = None
    print(f"   No sex/gender column found in studies table")

# ──────────────────────────────────────────────
# 4. SAMPLE SIZE / ENROLLMENT
# ──────────────────────────────────────────────
print("\n4. Sample size...")

enroll_col = None
for candidate in ["enrollment", "enrollment_count", "sample_size", "number_of_participants",
                   "total_enrollment", "target_size"]:
    if candidate in studies.columns:
        enroll_col = candidate
        break

if enroll_col:
    enroll_df = studies[["study_id", enroll_col]].copy()
    enroll_df["sample_size"] = pd.to_numeric(enroll_df[enroll_col], errors="coerce")
    df = df.merge(enroll_df[["study_id", "sample_size"]], on="study_id", how="left")
    filled = df["sample_size"].notna().sum()
    median_size = df["sample_size"].median()
    print(f"   Sample size: {filled:,} filled ({filled/len(df)*100:.1f}%)")
    print(f"   Median enrollment: {median_size:.0f}")
else:
    df["sample_size"] = None
    print(f"   No enrollment column found in studies table")
    print(f"   Studies columns: {list(studies.columns)}")

save_intermediate(df, "06_clean_cohort.parquet")
