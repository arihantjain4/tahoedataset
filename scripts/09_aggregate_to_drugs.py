"""
Script 09: Aggregate from drug-trial level → drug level.

Goes from ~1M+ drug-trial rows to ~5,000-7,000 unique drugs.
Each drug gets a summary across all its trials.

Usage:
    python scripts/09_aggregate_to_drugs.py
"""
import sys
import os
import json
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import TARGET_DRUG_COUNT
from utils import load_intermediate, save_intermediate, safe_join_unique, safe_first

print("="*60)
print("STEP 09: Aggregate to Drug Level")
print("="*60)

df = load_intermediate("08_clean_toxicity.parquet")
print(f"Input: {len(df):,} drug-trial rows")
print(f"Unique drugs: {df['drug_name_clean'].nunique():,}")

# ──────────────────────────────────────────────
# BUILD AGGREGATION SPEC
# ──────────────────────────────────────────────
print("\n1. Aggregating across trials per drug...")

# Define aggregation for each column
agg_dict = {}

# Drug Identity — take first non-null (drug-level attributes, same across trials)
for col in ["drug_moa_id", "drug_description", "rx_normalized_name",
            "drugbank_id_clean"]:
    if col in df.columns:
        agg_dict[col] = safe_first

# Regulatory — True if ANY trial says approved
for col in ["fda_approved", "ema_approved", "pmda_approved"]:
    if col in df.columns:
        agg_dict[col] = "max"

# Trial Info — aggregate across trials
if "study_id" in df.columns:
    agg_dict["study_id"] = "nunique"

if "nct_id" in df.columns:
    agg_dict["nct_id"] = lambda x: safe_join_unique(x, max_items=15)

if "trial_phase" in df.columns:
    agg_dict["trial_phase"] = lambda x: safe_join_unique(x, max_items=10)

if "current_status" in df.columns:
    agg_dict["current_status"] = lambda x: safe_join_unique(x, max_items=10)

# Patient Cohort — aggregate
if "disease_indications" in df.columns:
    agg_dict["disease_indications"] = lambda x: safe_join_unique(x, max_items=15)

if "sample_size" in df.columns:
    agg_dict["sample_size"] = "sum"

if "age_range" in df.columns:
    agg_dict["age_range"] = safe_first

if "sex" in df.columns:
    agg_dict["sex"] = lambda x: safe_join_unique(x, max_items=5)

# Dosage — collect ranges
if "dose_amount" in df.columns:
    agg_dict["dose_amount"] = lambda x: (
        f"{x.dropna().min():.1f} - {x.dropna().max():.1f}"
        if x.dropna().any() and x.dropna().min() != x.dropna().max()
        else f"{x.dropna().iloc[0]:.1f}" if x.dropna().any()
        else None
    )

if "dose_unit" in df.columns:
    agg_dict["dose_unit"] = safe_first

if "route" in df.columns:
    agg_dict["route"] = lambda x: safe_join_unique(x, max_items=5)

if "frequency" in df.columns:
    agg_dict["frequency"] = lambda x: safe_join_unique(x, max_items=5)

if "is_combination" in df.columns:
    agg_dict["is_combination"] = "max"

# Adverse Events — merge across trials
if "adverse_event_names" in df.columns:
    agg_dict["adverse_event_names"] = lambda x: safe_join_unique(x, max_items=30)

if "ae_frequency_data" in df.columns:
    agg_dict["ae_frequency_data"] = safe_first  # Take most detailed

if "severity_grades" in df.columns:
    agg_dict["severity_grades"] = safe_first

# Toxicity Profile
if "primary_toxic_organ_system" in df.columns:
    agg_dict["primary_toxic_organ_system"] = lambda x: x.mode().iloc[0] if not x.mode().empty else None

if "high_grade_toxicity_present" in df.columns:
    agg_dict["high_grade_toxicity_present"] = "max"

for col in ["has_dlt", "dlt_description", "mtd_value", "rp2d_value", "dose_toxicity_pattern"]:
    if col in df.columns:
        agg_dict[col] = safe_first

# Sources
if "source_type" in df.columns:
    agg_dict["source_type"] = lambda x: safe_join_unique(x, max_items=3)

if "pmid_or_doi" in df.columns:
    agg_dict["pmid_or_doi"] = lambda x: safe_join_unique(x, max_items=5)

if "study_source" in df.columns:
    agg_dict["study_source"] = lambda x: safe_join_unique(x, max_items=5)

# ── Run aggregation ──
drug_table = df.groupby("drug_name_clean").agg(**{
    col: pd.NamedAgg(column=col, aggfunc=func)
    for col, func in agg_dict.items()
}).reset_index()

# Rename columns to final schema names
rename_map = {
    "drug_name_clean": "drug_name",
    "study_id": "num_trials",
    "nct_id": "nct_ids",
    "trial_phase": "trial_phases",
    "current_status": "current_statuses",
    "sample_size": "total_sample_size",
    "dose_amount": "dose_range",
    "route": "routes",
    "frequency": "frequencies",
    "is_combination": "is_used_in_combination",
}
drug_table = drug_table.rename(columns={k: v for k, v in rename_map.items() if k in drug_table.columns})

print(f"\nAggregated to {len(drug_table):,} unique drugs")

# ──────────────────────────────────────────────
# 2. RANK BY DATA COMPLETENESS
# ──────────────────────────────────────────────
print("\n2. Ranking by data completeness...")

# Score each drug by how many key fields are populated
key_fields = [c for c in [
    "adverse_event_names", "dose_range", "disease_indications",
    "primary_toxic_organ_system", "nct_ids", "trial_phases",
    "routes", "total_sample_size"
] if c in drug_table.columns]

drug_table["completeness_score"] = drug_table[key_fields].notna().sum(axis=1)
drug_table = drug_table.sort_values(
    ["completeness_score", "num_trials"],
    ascending=[False, False]
)

print(f"   Completeness distribution:")
for score in sorted(drug_table["completeness_score"].unique(), reverse=True):
    count = (drug_table["completeness_score"] == score).sum()
    print(f"     Score {score}/{len(key_fields)}: {count:,} drugs")

# ──────────────────────────────────────────────
# 3. SELECT TOP N DRUGS
# ──────────────────────────────────────────────
print(f"\n3. Selecting top {TARGET_DRUG_COUNT:,} drugs...")

if len(drug_table) > TARGET_DRUG_COUNT:
    drug_table_final = drug_table.head(TARGET_DRUG_COUNT).copy()
    print(f"   Filtered from {len(drug_table):,} → {len(drug_table_final):,}")
    min_score = drug_table_final["completeness_score"].min()
    print(f"   Minimum completeness score in final set: {min_score}/{len(key_fields)}")
else:
    drug_table_final = drug_table.copy()
    print(f"   All {len(drug_table_final):,} drugs included (below target)")

# Drop the scoring column
drug_table_final = drug_table_final.drop(columns=["completeness_score"], errors="ignore")

# Also save the FULL table (all drugs) for reference
save_intermediate(drug_table, "09_all_drugs.parquet")
save_intermediate(drug_table_final, "09_top_drugs.parquet")

print(f"\nSaved:")
print(f"  All drugs: {len(drug_table):,}")
print(f"  Top drugs: {len(drug_table_final):,}")
