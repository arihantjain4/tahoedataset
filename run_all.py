"""
Master runner — executes all pipeline scripts in sequence.
Stops on any error so you can fix before continuing.

Usage:
    python run_all.py              # run everything
    python run_all.py --from 5     # resume from script 05
    python run_all.py --only 1     # run only script 01
"""
import subprocess
import sys
import os
import time
import argparse

SCRIPTS = [
    ("01_download_and_profile.py",    "Download & Profile (this takes the longest)"),
    ("02_join_and_flatten.py",        "Join tables on study_id"),
    ("03_clean_drug_identity.py",     "Clean Drug Identity"),
    ("04_clean_trial_info.py",        "Clean Trial Info"),
    ("05_clean_adverse_events.py",    "Clean Adverse Events"),
    ("06_clean_patient_cohort.py",    "Clean Patient Cohort"),
    ("07_clean_dosage.py",            "Clean Dosage"),
    ("08_derive_toxicity_profile.py", "Derive Toxicity Profile"),
    ("09_aggregate_to_drugs.py",      "Aggregate to Drug Level"),
    ("10_validate_and_export.py",     "Validate & Export"),
    ("11_fetch_publication_links.py", "Fetch Publication Links"),
    ("12_fill_with_paperqa.py",       "Fill Phase 2 Columns with PaperQA"),
    ("13_final_cleanups.py",          "Final Cleanups"),
]

NUM_SCRIPTS = len(SCRIPTS)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="start_from", type=int, default=1,
                       help="Start from script number (e.g., --from 5)")
    parser.add_argument("--only", type=int, default=None,
                       help="Run only this script number (e.g., --only 3)")
    args = parser.parse_args()

    project_root = os.path.dirname(os.path.abspath(__file__))
    scripts_dir = os.path.join(project_root, "scripts")

    total_start = time.time()

    print("="*60)
    print("TAHOE — FULL PIPELINE")
    print("="*60)

    for i, (script, description) in enumerate(SCRIPTS, 1):
        if args.only and i != args.only:
            continue
        if i < args.start_from:
            print(f"\n[{i:02d}/{NUM_SCRIPTS}] SKIP — {description}")
            continue

        script_path = os.path.join(scripts_dir, script)

        print(f"\n{'#'*60}")
        print(f"# [{i:02d}/{NUM_SCRIPTS}] {description}")
        print(f"# Script: {script}")
        print(f"{'#'*60}\n")

        start = time.time()
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=project_root,
        )
        elapsed = time.time() - start

        if result.returncode != 0:
            print(f"\n{'!'*60}")
            print(f"! FAILED: {script} (exit code {result.returncode})")
            print(f"! Fix the issue and re-run with: python run_all.py --from {i}")
            print(f"{'!'*60}")
            sys.exit(1)

        print(f"\n  ✓ Completed in {elapsed:.1f}s")

        if i == 1 and not args.only:
            print(f"\n{'*'*60}")
            print("* IMPORTANT: Review the profiling output above!")
            print("* Check that column names match what scripts 02+ expect.")
            print("* If column names differ, update the scripts before continuing.")
            print("* Press Enter to continue, or Ctrl+C to stop and adjust...")
            print(f"{'*'*60}")
            try:
                input()
            except KeyboardInterrupt:
                print("\nStopped. Adjust column names, then run: python run_all.py --from 2")
                sys.exit(0)

    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"ALL DONE — Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    print(f"{'='*60}")
    print(f"\nOutputs:")
    print(f"  - gold_standard_table.csv")
    print(f"  - gold_standard_table_cleaned.csv")
    print(f"  - gold_standard_table_cleaned.parquet")
    print(f"  - publication_links.csv")
    print(f"  - coverage_report.html")
    print(f"  - coverage_report.csv")
    print(f"  - sample_rows.csv")


if __name__ == "__main__":
    main()
