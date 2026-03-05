"""
Script 02: Join tables on study_id to create one wide drug-trial table.

Each row in the output = one (study_id, drug_name) pair.
Conditions, AEs, dispositions get merged in as aggregated columns.

IMPORTANT: After running 01, you may need to adjust column names below.
Look for lines marked with # >>> ADJUST to find places you might need to change.

Usage:
    python scripts/02_join_and_flatten.py
"""
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import RAW_DIR, INTERMEDIATE_DIR
from utils import load_raw, save_intermediate, safe_join_unique

print("="*60)
print("STEP 02: Join & Flatten")
print("="*60)

# ── Load raw tables ──
print("\nLoading raw tables...")
studies = load_raw("studies")
drugs = load_raw("drugs")
conditions = load_raw("conditions")
dispositions = load_raw("disposition")
relations = load_raw("relations")

print(f"  studies:      {len(studies):>10,} rows | cols: {list(studies.columns)}")
print(f"  drugs:        {len(drugs):>10,} rows | cols: {list(drugs.columns)}")
print(f"  conditions:   {len(conditions):>10,} rows | cols: {list(conditions.columns)}")
print(f"  dispositions: {len(dispositions):>10,} rows | cols: {list(dispositions.columns)}")
print(f"  relations:    {len(relations):>10,} rows | cols: {list(relations.columns)}")

# ──────────────────────────────────────────────
# 1. START WITH DRUGS AS THE SPINE
# ──────────────────────────────────────────────
print("\n1. Building base from drugs table...")

# >>> ADJUST: Change column names to match what you saw in profiling
# Common drug table columns: study_id, drug_name, drugbank_id, rx_norm_name,
# fda_approved, ema_approved, pmda_approved, etc.
base = drugs.copy()
print(f"   Base table: {len(base):,} rows (drug-trial pairs)")

# ──────────────────────────────────────────────
# 2. JOIN STUDIES METADATA
# ──────────────────────────────────────────────
print("\n2. Joining studies metadata...")

# >>> ADJUST: Pick the columns that actually exist in your studies table
study_cols = ["study_id"]
for col in ["phase", "status", "start_year", "enrollment", "title",
            "brief_summary", "study_type", "sponsor_type",
            "minimum_age", "maximum_age", "gender",
            # Try variations:
            "overall_status", "recruitment_status"]:
    if col in studies.columns:
        study_cols.append(col)

print(f"   Using study columns: {study_cols}")
base = base.merge(studies[study_cols], on="study_id", how="left")
print(f"   After join: {len(base):,} rows")

# ──────────────────────────────────────────────
# 3. AGGREGATE CONDITIONS PER STUDY
# ──────────────────────────────────────────────
print("\n3. Aggregating conditions per study...")

# >>> ADJUST: The condition name column might be 'condition_name', 'condition', etc.
cond_name_col = None
for candidate in ["condition_name", "condition", "name", "mesh_term"]:
    if candidate in conditions.columns:
        cond_name_col = candidate
        break

if cond_name_col:
    cond_agg = (
        conditions
        .groupby("study_id")[cond_name_col]
        .apply(lambda x: "; ".join(x.dropna().unique()[:10]))
        .reset_index()
        .rename(columns={cond_name_col: "disease_indications"})
    )
    base = base.merge(cond_agg, on="study_id", how="left")
    print(f"   Merged conditions using column '{cond_name_col}'")
else:
    base["disease_indications"] = None
    print(f"   WARNING: Could not find condition name column. Available: {list(conditions.columns)}")

# Also get MeSH IDs if available
mesh_col = None
for candidate in ["condition_mesh_id", "mesh_id", "condition_id"]:
    if candidate in conditions.columns:
        mesh_col = candidate
        break

if mesh_col:
    mesh_agg = (
        conditions
        .groupby("study_id")[mesh_col]
        .apply(lambda x: "; ".join(x.dropna().astype(str).unique()[:10]))
        .reset_index()
        .rename(columns={mesh_col: "condition_mesh_ids"})
    )
    base = base.merge(mesh_agg, on="study_id", how="left")

# ──────────────────────────────────────────────
# 4. GET DISPOSITION / INTERVENTION INFO (for dosage later)
# ──────────────────────────────────────────────
print("\n4. Joining disposition/intervention info...")

# >>> ADJUST: Column names for dispositions
disp_cols = ["study_id"]
for col in ["intervention_name", "intervention_type", "group_type",
            "arm_group_label", "description"]:
    if col in dispositions.columns:
        disp_cols.append(col)

print(f"   Using disposition columns: {disp_cols}")

# Filter to drug interventions only
type_col = None
for candidate in ["intervention_type", "type"]:
    if candidate in dispositions.columns:
        type_col = candidate
        break

if type_col:
    drug_dispositions = dispositions[
        dispositions[type_col].str.upper().str.contains("DRUG", na=False)
    ][disp_cols].copy()
else:
    drug_dispositions = dispositions[disp_cols].copy()

# Aggregate intervention names per study
int_name_col = None
for candidate in ["intervention_name", "name", "arm_group_label"]:
    if candidate in drug_dispositions.columns:
        int_name_col = candidate
        break

if int_name_col:
    disp_agg = (
        drug_dispositions
        .groupby("study_id")
        .agg({
            int_name_col: lambda x: " | ".join(x.dropna().unique()[:5]),
        })
        .reset_index()
        .rename(columns={int_name_col: "intervention_descriptions"})
    )
    base = base.merge(disp_agg, on="study_id", how="left")
    print(f"   Merged dispositions using column '{int_name_col}'")

    # Count drug arms per study (for combination flag)
    arm_counts = (
        drug_dispositions
        .groupby("study_id")[int_name_col]
        .nunique()
        .reset_index()
        .rename(columns={int_name_col: "num_drug_arms"})
    )
    base = base.merge(arm_counts, on="study_id", how="left")
else:
    base["intervention_descriptions"] = None
    base["num_drug_arms"] = None
    print(f"   WARNING: Could not find intervention name column. Available: {list(dispositions.columns)}")

# ──────────────────────────────────────────────
# 5. LINK PUBMED PAPERS FROM RELATIONS
# ──────────────────────────────────────────────
print("\n5. Linking PubMed papers from relations...")

print(f"   Relations columns: {list(relations.columns)}")

# >>> ADJUST: Figure out how relations links studies to papers
# Look for columns like: related_id, pmid, doi, relation_type, type
rel_type_col = None
for candidate in ["relation_type", "type", "link_type"]:
    if candidate in relations.columns:
        rel_type_col = candidate
        break

rel_id_col = None
for candidate in ["related_id", "pmid", "doi", "target_id", "reference_id"]:
    if candidate in relations.columns:
        rel_id_col = candidate
        break

if rel_type_col and rel_id_col:
    # Try to find publication/pubmed links
    pubmed_mask = relations[rel_type_col].astype(str).str.lower().str.contains(
        "pubmed|publication|paper|reference|pmid", na=False
    )
    if pubmed_mask.sum() > 0:
        pubmed_links = (
            relations[pubmed_mask]
            .groupby("study_id")[rel_id_col]
            .first()
            .reset_index()
            .rename(columns={rel_id_col: "pmid_or_doi"})
        )
        base = base.merge(pubmed_links, on="study_id", how="left")
        print(f"   Found {pubmed_mask.sum():,} pubmed links using '{rel_type_col}' / '{rel_id_col}'")
    else:
        # Maybe ALL relations are publication links — just take first per study
        pubmed_links = (
            relations
            .groupby("study_id")[rel_id_col]
            .first()
            .reset_index()
            .rename(columns={rel_id_col: "pmid_or_doi"})
        )
        base = base.merge(pubmed_links, on="study_id", how="left")
        print(f"   No clear pubmed filter found; took first related_id per study")
elif rel_id_col:
    pubmed_links = (
        relations
        .groupby("study_id")[rel_id_col]
        .first()
        .reset_index()
        .rename(columns={rel_id_col: "pmid_or_doi"})
    )
    base = base.merge(pubmed_links, on="study_id", how="left")
    print(f"   Used '{rel_id_col}' as PMID/DOI source")
else:
    base["pmid_or_doi"] = None
    print(f"   WARNING: Could not determine relation columns. Available: {list(relations.columns)}")

# ──────────────────────────────────────────────
# SAVE
# ──────────────────────────────────────────────
print(f"\nFinal joined table: {len(base):,} rows, {len(base.columns)} columns")
print(f"Columns: {list(base.columns)}")
save_intermediate(base, "02_joined.parquet")

# Quick stats
if "drug_name" in base.columns:
    print(f"\nUnique drugs: {base['drug_name'].nunique():,}")
print(f"Unique studies: {base['study_id'].nunique():,}")
has_pmid = base["pmid_or_doi"].notna().sum() if "pmid_or_doi" in base.columns else 0
print(f"Rows with PubMed link: {has_pmid:,} ({has_pmid/len(base)*100:.1f}%)")
