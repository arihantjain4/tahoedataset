"""
Script 01: Download TrialPanorama Parquet files & profile every table.

RUN THIS FIRST. Read the output carefully before running anything else.
The column names printed here determine how every subsequent script works.

Usage:
    python scripts/01_download_and_profile.py
    python scripts/01_download_and_profile.py --tables studies drugs   # download specific tables only
    python scripts/01_download_and_profile.py --profile-only           # skip download, just profile
"""
import sys
import os
import json
import argparse
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import HF_DATASET, TABLES, PRIORITY_TABLES, RAW_DIR, OUTPUT_DIR
from utils import profile_table, print_profile


def download_tables(tables_to_download):
    """Download tables by fetching parquet files directly from HuggingFace Hub."""
    from huggingface_hub import hf_hub_download, list_repo_tree
    import pandas as pd

    # The dataset repo on HF
    repo_id = "TrialPanorama/TrialPanorama-database"

    for i, table in enumerate(tables_to_download, 1):
        out_path = os.path.join(RAW_DIR, f"{table}.parquet")

        # Skip if already downloaded
        if os.path.exists(out_path):
            print(f"[{i}/{len(tables_to_download)}] {table} — already exists, skipping download")
            continue

        print(f"[{i}/{len(tables_to_download)}] Downloading '{table}'...")
        start = time.time()

        try:
            # Find all parquet files for this table config
            # They're stored as: {table}/{split}/*.parquet or data/{table}-*.parquet
            # Try multiple path patterns
            parquet_files = []

            # Pattern 1: {table}/train-*.parquet or {table}/all-*.parquet
            # Pattern 2: data/{table}-*.parquet
            # We'll list the repo and filter
            try:
                tree = list(list_repo_tree(repo_id, path_in_repo=table, repo_type="dataset"))
                parquet_files = [
                    f.rfilename for f in tree
                    if f.rfilename.endswith(".parquet")
                ]
            except Exception:
                pass

            if not parquet_files:
                # Try top-level data/ folder
                try:
                    tree = list(list_repo_tree(repo_id, path_in_repo="data", repo_type="dataset"))
                    parquet_files = [
                        f.rfilename for f in tree
                        if f.rfilename.endswith(".parquet") and table in f.rfilename
                    ]
                except Exception:
                    pass

            if not parquet_files:
                # Try root level
                try:
                    tree = list(list_repo_tree(repo_id, repo_type="dataset"))
                    parquet_files = [
                        f.rfilename for f in tree
                        if f.rfilename.endswith(".parquet") and table in f.rfilename
                    ]
                except Exception:
                    pass

            if not parquet_files:
                # Last resort: try direct known patterns
                known_patterns = [
                    f"{table}/{table}.parquet",
                    f"{table}/train-00000-of-00001.parquet",
                    f"data/{table}.parquet",
                    f"{table}.parquet",
                ]
                for pattern in known_patterns:
                    try:
                        path = hf_hub_download(
                            repo_id=repo_id,
                            filename=pattern,
                            repo_type="dataset",
                        )
                        parquet_files = [pattern]
                        break
                    except Exception:
                        continue

            if not parquet_files:
                print(f"  ✗ Could not find parquet files for '{table}'")
                continue

            print(f"  Found {len(parquet_files)} parquet file(s): {parquet_files[:3]}...")

            # Download and concatenate all parquet parts
            dfs = []
            for pf in parquet_files:
                local_path = hf_hub_download(
                    repo_id=repo_id,
                    filename=pf,
                    repo_type="dataset",
                )
                dfs.append(pd.read_parquet(local_path))

            df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
            df.to_parquet(out_path, index=False)
            elapsed = time.time() - start
            print(f"  ✓ {len(df):,} rows, {len(df.columns)} cols — saved in {elapsed:.1f}s")

        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            continue

    print(f"\nAll downloads saved to: {RAW_DIR}")


def profile_all_tables():
    """Profile every downloaded table and save a summary."""
    import pandas as pd

    all_stats = {}
    parquet_files = sorted([f for f in os.listdir(RAW_DIR) if f.endswith(".parquet")])

    if not parquet_files:
        print("No parquet files found. Run download first.")
        return

    print(f"\n{'#'*60}")
    print(f"# PROFILING {len(parquet_files)} TABLES")
    print(f"{'#'*60}")

    for fname in parquet_files:
        table_name = fname.replace(".parquet", "")
        df = pd.read_parquet(os.path.join(RAW_DIR, fname))
        stats = profile_table(df, table_name)
        print_profile(stats)
        all_stats[table_name] = stats

    # Save profiling summary as JSON for reference
    summary_path = os.path.join(OUTPUT_DIR, "profiling_summary.json")
    # Convert to serializable format
    serializable = {}
    for table, stats in all_stats.items():
        serializable[table] = {
            "rows": stats["rows"],
            "columns": stats["columns"],
            "column_stats": stats["column_stats"]
        }
    with open(summary_path, "w") as f:
        json.dump(serializable, f, indent=2, default=str)
    print(f"\nProfiling summary saved to: {summary_path}")

    # Print the KEY MAPPING GUIDANCE
    print(f"\n{'#'*60}")
    print("# NEXT STEP: Column Mapping")
    print(f"{'#'*60}")
    print("""
Review the columns above and update the scripts if needed.
Key things to look for:

1. DRUGS TABLE — what are the exact column names for:
   - drug name?
   - RxNorm ID/name?
   - DrugBank ID?
   - FDA/EMA/PMDA approval status?

2. STUDIES TABLE — what are the exact column names for:
   - study_id / NCT ID?
   - phase?
   - status?
   - enrollment / sample size?
   - age eligibility?

3. ADVERSE_EVENTS TABLE — what columns exist for:
   - event name?
   - MedDRA ID?
   - severity / grade?
   - frequency / count / num_affected / num_at_risk?
   - serious event flag?

4. CONDITIONS TABLE — column names for:
   - condition name?
   - MeSH ID?

5. DISPOSITION TABLE — column names for:
   - intervention name (often contains dose info)?
   - intervention type?
   - group type?

6. RELATIONS TABLE — how are PubMed links stored?
   - What's the column for PMID or DOI?
   - What's the relation type column?

>>> WRITE DOWN the actual column names before proceeding to Script 02.
""")


def main():
    parser = argparse.ArgumentParser(description="Download and profile TrialPanorama tables")
    parser.add_argument("--tables", nargs="+", help="Specific tables to download")
    parser.add_argument("--profile-only", action="store_true", help="Skip download, just profile")
    parser.add_argument("--priority-only", action="store_true", help="Download only priority tables")
    args = parser.parse_args()

    if not args.profile_only:
        if args.tables:
            tables = args.tables
        elif args.priority_only:
            tables = PRIORITY_TABLES
        else:
            tables = TABLES

        download_tables(tables)

    profile_all_tables()


if __name__ == "__main__":
    main()
