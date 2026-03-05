"""
Script 10: Validate, generate coverage report, and export final outputs.

Produces:
    - output/gold_standard_table.parquet
    - output/gold_standard_table.csv
    - output/coverage_report.csv
    - output/coverage_report.html (visual)
    - output/sample_rows.csv (first 50 rows for quick review)

Usage:
    python scripts/10_validate_and_export.py
"""
import sys
import os
import pandas as pd
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import FINAL_SCHEMA, OUTPUT_DIR
from utils import load_intermediate

print("="*60)
print("STEP 10: Validate & Export")
print("="*60)

df = load_intermediate("09_top_drugs.parquet")
print(f"Input: {len(df):,} drugs")

# ──────────────────────────────────────────────
# 1. REORDER COLUMNS TO MATCH FINAL SCHEMA
# ──────────────────────────────────────────────
print("\n1. Reordering columns to final schema...")

# Include columns that exist, in schema order
final_cols = [c for c in FINAL_SCHEMA if c in df.columns]
# Add any extra columns not in schema (keep them at the end)
extra_cols = [c for c in df.columns if c not in FINAL_SCHEMA]

df_final = df[final_cols + extra_cols].copy()
print(f"   Schema columns present: {len(final_cols)}/{len(FINAL_SCHEMA)}")
missing_from_schema = [c for c in FINAL_SCHEMA if c not in df.columns]
if missing_from_schema:
    print(f"   Missing from schema: {missing_from_schema}")
if extra_cols:
    print(f"   Extra columns (appended): {extra_cols}")

# ──────────────────────────────────────────────
# 2. VALIDATION CHECKS
# ──────────────────────────────────────────────
print("\n2. Running validation checks...")

issues = []

# Check: no duplicate drug names
dupes = df_final["drug_name"].duplicated().sum()
if dupes > 0:
    issues.append(f"WARN: {dupes} duplicate drug names")
    # Deduplicate — keep first (highest completeness from sorting)
    df_final = df_final.drop_duplicates(subset=["drug_name"], keep="first")
    print(f"   Deduplicated: removed {dupes} duplicates")
else:
    print(f"   ✓ No duplicate drug names")

# Check: drug names are clean
null_names = df_final["drug_name"].isna().sum()
if null_names > 0:
    issues.append(f"WARN: {null_names} null drug names")
    df_final = df_final.dropna(subset=["drug_name"])
print(f"   ✓ All rows have drug names")

# Check: reasonable data
if "num_trials" in df_final.columns:
    median_trials = df_final["num_trials"].median()
    max_trials = df_final["num_trials"].max()
    print(f"   Trials per drug: median={median_trials:.0f}, max={max_trials:,}")

if "total_sample_size" in df_final.columns:
    huge = (df_final["total_sample_size"] > 1_000_000).sum()
    if huge > 0:
        issues.append(f"NOTE: {huge} drugs with >1M total participants (aggregated across trials)")

print(f"   Issues found: {len(issues)}")
for issue in issues:
    print(f"     {issue}")

# ──────────────────────────────────────────────
# 3. COVERAGE REPORT
# ──────────────────────────────────────────────
print(f"\n3. Coverage Report")
print(f"{'='*70}")
print(f"{'Column':<35} {'Filled':>8} {'Total':>8} {'Coverage':>10} {'Source':>20}")
print(f"{'-'*70}")

coverage_data = []
for col in df_final.columns:
    filled = df_final[col].notna().sum()
    # For boolean columns, count True
    if df_final[col].dtype == "bool":
        filled = df_final[col].sum()
    total = len(df_final)
    pct = filled / total * 100

    # Determine source
    if col in ["drug_name", "drug_moa_id", "drug_description", "rx_normalized_name",
               "fda_approved", "ema_approved", "pmda_approved", "drugbank_id_clean"]:
        source = "drugs table"
    elif col in ["nct_ids", "trial_phases", "current_statuses", "num_trials", "study_source"]:
        source = "studies table"
    elif col in ["disease_indications", "condition_mesh_ids"]:
        source = "conditions table"
    elif col in ["age_range", "sex", "total_sample_size"]:
        source = "studies table"
    elif col in ["dose_range", "dose_unit", "routes", "frequencies", "is_used_in_combination"]:
        source = "dispositions (parsed)"
    elif col in ["adverse_event_names", "ae_frequency_data", "severity_grades",
                 "ae_count", "serious_ae_count", "has_high_grade"]:
        source = "adverse_events table"
    elif col in ["primary_toxic_organ_system", "high_grade_toxicity_present"]:
        source = "derived (MedDRA)"
    elif col in ["has_dlt", "dlt_description", "mtd_value", "rp2d_value", "dose_toxicity_pattern"]:
        source = "→ PHASE 2 (PaperQA)"
    elif col in ["source_type", "pmid_or_doi"]:
        source = "relations table"
    else:
        source = "other"

    status = "✓" if pct > 50 else "△" if pct > 10 else "✗"
    print(f"  {status} {col:<33} {filled:>8,} {total:>8,} {pct:>9.1f}% {source:>20}")

    coverage_data.append({
        "column": col,
        "filled": filled,
        "total": total,
        "coverage_pct": round(pct, 1),
        "source": source,
        "phase": "Phase 2" if "PHASE 2" in source else "Phase 1",
    })

# ──────────────────────────────────────────────
# 4. EXPORT FILES
# ──────────────────────────────────────────────
print(f"\n4. Exporting...")

# Main table — Parquet (best for downstream use)
parquet_path = os.path.join(OUTPUT_DIR, "gold_standard_table.parquet")
df_final.to_parquet(parquet_path, index=False)
parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)
print(f"   ✓ {parquet_path} ({parquet_size:.1f} MB)")

# Main table — CSV (for spreadsheet viewing)
csv_path = os.path.join(OUTPUT_DIR, "gold_standard_table.csv")
df_final.to_csv(csv_path, index=False)
csv_size = os.path.getsize(csv_path) / (1024 * 1024)
print(f"   ✓ {csv_path} ({csv_size:.1f} MB)")

# Coverage report — CSV
coverage_df = pd.DataFrame(coverage_data)
coverage_csv_path = os.path.join(OUTPUT_DIR, "coverage_report.csv")
coverage_df.to_csv(coverage_csv_path, index=False)
print(f"   ✓ {coverage_csv_path}")

# Coverage report — HTML (for presentation)
html_path = os.path.join(OUTPUT_DIR, "coverage_report.html")
html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Phase 1 Coverage Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 40px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .meta {{ color: #666; margin-bottom: 20px; }}
        table {{ border-collapse: collapse; width: 100%; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        th {{ background: #2d2d2d; color: white; padding: 12px 16px; text-align: left; }}
        td {{ padding: 10px 16px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        .bar {{ height: 20px; border-radius: 3px; display: inline-block; }}
        .bar-fill {{ background: #4CAF50; }}
        .bar-empty {{ background: #e0e0e0; }}
        .phase2 {{ color: #f44336; font-weight: bold; }}
        .good {{ color: #4CAF50; }}
        .mid {{ color: #FF9800; }}
        .low {{ color: #f44336; }}
        .summary {{ background: white; padding: 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
    </style>
</head>
<body>
    <h1>Tahoe Phase 1 — Gold Standard Table Coverage Report</h1>
    <div class="meta">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Total drugs: {len(df_final):,}</div>

    <div class="summary">
        <h3>Summary</h3>
        <p>Phase 1 columns with >50% coverage: <strong>{sum(1 for r in coverage_data if r['coverage_pct'] > 50 and r['phase'] == 'Phase 1')}</strong></p>
        <p>Phase 2 columns (null, needs PaperQA): <strong>{sum(1 for r in coverage_data if r['phase'] == 'Phase 2')}</strong></p>
    </div>

    <table>
        <tr>
            <th>Column</th>
            <th>Coverage</th>
            <th>Filled / Total</th>
            <th>Source</th>
        </tr>
"""

for row in coverage_data:
    pct = row["coverage_pct"]
    color_class = "good" if pct > 50 else "mid" if pct > 10 else "low"
    if row["phase"] == "Phase 2":
        color_class = "phase2"
    bar_width = min(pct, 100)

    html_content += f"""        <tr>
            <td><strong>{row['column']}</strong></td>
            <td>
                <div style="width: 200px; background: #e0e0e0; border-radius: 3px; overflow: hidden;">
                    <div style="width: {bar_width}%; height: 20px; background: {'#4CAF50' if pct > 50 else '#FF9800' if pct > 10 else '#f44336'};"></div>
                </div>
                <span class="{color_class}">{pct:.1f}%</span>
            </td>
            <td>{row['filled']:,} / {row['total']:,}</td>
            <td>{row['source']}</td>
        </tr>
"""

html_content += """    </table>
</body>
</html>"""

with open(html_path, "w") as f:
    f.write(html_content)
print(f"   ✓ {html_path}")

# Sample rows for quick review
sample_path = os.path.join(OUTPUT_DIR, "sample_rows.csv")
df_final.head(50).to_csv(sample_path, index=False)
print(f"   ✓ {sample_path} (first 50 rows)")

# ──────────────────────────────────────────────
# FINAL SUMMARY
# ──────────────────────────────────────────────
print(f"\n{'='*60}")
print("PHASE 1 COMPLETE")
print(f"{'='*60}")
print(f"  Drugs in final table: {len(df_final):,}")
print(f"  Columns: {len(df_final.columns)}")
print(f"  Files exported to: {OUTPUT_DIR}/")
print(f"""
  DELIVERABLES FOR AIDEN:
    1. gold_standard_table.csv  — the table itself
    2. coverage_report.html     — visual coverage analysis
    3. sample_rows.csv          — quick preview

  WHAT PHASE 2 NEEDS TO FILL (via PaperQA):
    - has_dlt, dlt_description
    - mtd_value, rp2d_value
    - dose_toxicity_pattern
    - drug_moa_id (via DrugBank API or literature)
    - drug_description
    - severity_grades (CTCAE grades 1-5 from papers)
""")
