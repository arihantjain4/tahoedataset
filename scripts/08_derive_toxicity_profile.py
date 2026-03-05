"""
Script 08: Derive Toxicity Profile columns.
Owner: Jishnuu

Handles: organ system, DLT, MTD, RP2D, dose-toxicity pattern, high-grade flag
Most toxicity fields will be NULL — that's expected. Phase 2 fills them via PaperQA.

Usage:
    python scripts/08_derive_toxicity_profile.py
"""
import sys
import os
import json
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import ORGAN_SYSTEM_KEYWORDS
from utils import load_intermediate, save_intermediate, map_ae_to_organ_system

print("="*60)
print("STEP 08: Derive Toxicity Profile")
print("="*60)

df = load_intermediate("07_clean_dosage.parquet")
print(f"Input: {len(df):,} rows")

# ──────────────────────────────────────────────
# 1. PRIMARY TOXIC ORGAN SYSTEM
# ──────────────────────────────────────────────
print("\n1. Deriving primary toxic organ system from AE names...")

def get_primary_organ_system(ae_names_str):
    """From semicolon-separated AE names, find the most common organ system."""
    if pd.isna(ae_names_str):
        return None

    ae_names = [name.strip() for name in str(ae_names_str).split(";")]
    organ_counts = {}

    for ae_name in ae_names:
        organ = map_ae_to_organ_system(ae_name)
        if organ:
            organ_counts[organ] = organ_counts.get(organ, 0) + 1

    if organ_counts:
        return max(organ_counts, key=organ_counts.get)
    return None

df["primary_toxic_organ_system"] = df["adverse_event_names"].apply(get_primary_organ_system)

filled = df["primary_toxic_organ_system"].notna().sum()
print(f"   Organ system derived: {filled:,} ({filled/len(df)*100:.1f}%)")
if filled > 0:
    print(f"   Distribution:")
    for organ, count in df["primary_toxic_organ_system"].value_counts().head(10).items():
        print(f"     {organ}: {count:,}")

# ──────────────────────────────────────────────
# 2. HIGH-GRADE TOXICITY PRESENT
# ──────────────────────────────────────────────
print("\n2. High-grade toxicity flag...")

# Use the has_high_grade flag from AE script if available
if "has_high_grade" in df.columns:
    df["high_grade_toxicity_present"] = df["has_high_grade"]
    n_high = df["high_grade_toxicity_present"].sum() if df["high_grade_toxicity_present"].notna().any() else 0
    print(f"   From severity data: {n_high:,} with high-grade toxicity")
elif "serious_ae_count" in df.columns:
    # Fallback: use serious AE count as proxy
    df["high_grade_toxicity_present"] = df["serious_ae_count"].fillna(0) > 0
    n_high = df["high_grade_toxicity_present"].sum()
    print(f"   From serious AE proxy: {n_high:,} with high-grade toxicity")
else:
    df["high_grade_toxicity_present"] = None
    print(f"   No severity or serious data available")

# ──────────────────────────────────────────────
# 3. FIELDS THAT REQUIRE LITERATURE (Phase 2)
# ──────────────────────────────────────────────
print("\n3. Phase 2 fields (setting to null)...")

phase2_fields = {
    "has_dlt": "Dose-Limiting Toxicity flag — requires paper text",
    "dlt_description": "DLT description — requires paper text",
    "mtd_value": "Maximum Tolerated Dose — requires paper text (usually Phase I results)",
    "rp2d_value": "Recommended Phase 2 Dose — requires paper text",
    "dose_toxicity_pattern": "Dose-toxicity relationship — requires paper analysis",
}

for field, reason in phase2_fields.items():
    df[field] = None
    print(f"   {field}: NULL — {reason}")

# ──────────────────────────────────────────────
# 4. SOURCE TYPE
# ──────────────────────────────────────────────
print("\n4. Source type...")

if "pmid_or_doi" in df.columns:
    df["source_type"] = df["pmid_or_doi"].apply(
        lambda x: "literature_linked" if pd.notna(x) else "registry_only"
    )
    lit_count = (df["source_type"] == "literature_linked").sum()
    reg_count = (df["source_type"] == "registry_only").sum()
    print(f"   Literature-linked: {lit_count:,} ({lit_count/len(df)*100:.1f}%)")
    print(f"   Registry-only:    {reg_count:,} ({reg_count/len(df)*100:.1f}%)")
else:
    df["source_type"] = "registry_only"
    print(f"   No PMID column — all registry_only")

# ──────────────────────────────────────────────
# SUMMARY
# ──────────────────────────────────────────────
print(f"\n{'='*60}")
print("TOXICITY PROFILE COVERAGE SUMMARY")
print(f"{'='*60}")
tox_cols = [
    "primary_toxic_organ_system", "high_grade_toxicity_present",
    "has_dlt", "dlt_description", "mtd_value", "rp2d_value",
    "dose_toxicity_pattern", "source_type", "pmid_or_doi",
]
for col in tox_cols:
    if col in df.columns:
        filled = df[col].notna().sum()
        pct = filled / len(df) * 100
        status = "✓ PHASE 1" if pct > 5 else "→ PHASE 2"
        print(f"  {status} | {col}: {pct:.1f}% ({filled:,})")

save_intermediate(df, "08_clean_toxicity.parquet")
