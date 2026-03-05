"""
Tahoe Phase 1 — Configuration
All paths and constants in one place.
"""
import os

# ── Paths ──
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
INTERMEDIATE_DIR = os.path.join(PROJECT_ROOT, "data", "intermediate")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# Create dirs if they don't exist
for d in [RAW_DIR, INTERMEDIATE_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ── HuggingFace Dataset ──
HF_DATASET = "TrialPanorama/TrialPanorama-database"

# Tables we need (actual names from HuggingFace repo)
# Available configs: studies, conditions, drugs, disposition, outcomes,
#   adverse_events, results, biomarkers, endpoints, relations, drug_moa
TABLES = [
    "studies",
    "drugs",
    "drug_moa",
    "conditions",
    "adverse_events",
    "disposition",       # NOTE: singular, not "dispositions"
    "endpoints",
    "outcomes",
    "results",
    "biomarkers",
    "relations",
]

# Tables that are essential for Phase 1 (download these first)
PRIORITY_TABLES = [
    "studies",
    "drugs",
    "drug_moa",
    "conditions",
    "adverse_events",
    "disposition",       # NOTE: singular
    "outcomes",
    "relations",
]

# ── Schema Target ──
# The final column order for the gold-standard table
FINAL_SCHEMA = [
    # Identifiers
    "drug_name",
    # Drug Identity
    "drug_moa_id",
    "drug_description",
    "rx_normalized_name",
    "fda_approved",
    "ema_approved",
    "pmda_approved",
    # Trial Info
    "nct_ids",
    "trial_phases",
    "num_trials",
    "current_statuses",
    # Patient Cohort
    "disease_indications",
    "age_range",
    "sex",
    "total_sample_size",
    # Dosage
    "dose_range",
    "dose_unit",
    "routes",
    "frequencies",
    "is_used_in_combination",
    # Adverse Events
    "adverse_event_names",
    "ae_frequency_data",
    "severity_grades",
    # Toxicity Profile
    "primary_toxic_organ_system",
    "high_grade_toxicity_present",
    "has_dlt",
    "dlt_description",
    "mtd_value",
    "rp2d_value",
    "dose_toxicity_pattern",
    # Sources
    "source_type",
    "pmid_or_doi",
]

# ── Phase Normalization ──
PHASE_MAP = {
    "PHASE1": "I",
    "PHASE2": "II",
    "PHASE3": "III",
    "PHASE4": "IV",
    "PHASE1/PHASE2": "I/II",
    "PHASE2/PHASE3": "II/III",
    "EARLY_PHASE1": "Early I",
    "NOT_APPLICABLE": None,
    "Phase 1": "I",
    "Phase 2": "II",
    "Phase 3": "III",
    "Phase 4": "IV",
    "Phase 1/Phase 2": "I/II",
    "Phase 2/Phase 3": "II/III",
    "Early Phase 1": "Early I",
    "NA": None,
    "N/A": None,
}

# ── Status Normalization ──
STATUS_MAP = {
    "COMPLETED": "Completed",
    "RECRUITING": "Recruiting",
    "ACTIVE_NOT_RECRUITING": "Active, not recruiting",
    "TERMINATED": "Terminated",
    "WITHDRAWN": "Withdrawn",
    "SUSPENDED": "Suspended",
    "NOT_YET_RECRUITING": "Not yet recruiting",
    "UNKNOWN_STATUS": "Unknown",
    "ENROLLING_BY_INVITATION": "Enrolling by invitation",
    "NO_LONGER_AVAILABLE": "No longer available",
    "TEMPORARILY_NOT_AVAILABLE": "Temporarily not available",
    "APPROVED_FOR_MARKETING": "Approved for marketing",
    "AVAILABLE": "Available",
    "WITHHELD": "Withheld",
}

# ── MedDRA SOC (System Organ Class) keyword mapping ──
# Maps common adverse event name keywords → organ system
# This is a simplified heuristic; production would use full MedDRA hierarchy
ORGAN_SYSTEM_KEYWORDS = {
    "Gastrointestinal": [
        "nausea", "vomiting", "diarrhea", "diarrhoea", "constipation",
        "abdominal pain", "stomatitis", "mucositis", "colitis",
        "gastritis", "dyspepsia", "anorexia",
    ],
    "Blood/Lymphatic": [
        "neutropenia", "thrombocytopenia", "anemia", "anaemia",
        "leukopenia", "lymphopenia", "pancytopenia", "febrile neutropenia",
        "bone marrow", "myelosuppression",
    ],
    "Hepatobiliary": [
        "hepatotoxicity", "alt increased", "ast increased",
        "bilirubin", "hepatitis", "liver", "jaundice",
        "transaminase", "hepatic",
    ],
    "Skin/Subcutaneous": [
        "rash", "pruritus", "dermatitis", "alopecia", "dry skin",
        "palmar-plantar", "hand-foot", "erythema", "urticaria",
        "skin toxicity", "photosensitivity",
    ],
    "Nervous System": [
        "headache", "neuropathy", "peripheral neuropathy", "dizziness",
        "seizure", "tremor", "insomnia", "somnolence", "paresthesia",
        "dysgeusia", "encephalopathy",
    ],
    "Cardiac": [
        "cardiotoxicity", "qt prolongation", "arrhythmia", "tachycardia",
        "bradycardia", "heart failure", "myocardial", "ejection fraction",
        "cardiac", "hypertension", "hypotension",
    ],
    "Respiratory": [
        "pneumonitis", "dyspnea", "cough", "pneumonia",
        "interstitial lung", "pulmonary", "respiratory",
    ],
    "Renal/Urinary": [
        "nephrotoxicity", "creatinine increased", "renal",
        "proteinuria", "acute kidney", "hematuria",
    ],
    "Musculoskeletal": [
        "arthralgia", "myalgia", "back pain", "bone pain",
        "muscle spasm", "musculoskeletal",
    ],
    "General/Constitutional": [
        "fatigue", "asthenia", "pyrexia", "fever", "weight loss",
        "weight gain", "edema", "oedema", "pain", "malaise",
    ],
    "Immune System": [
        "hypersensitivity", "anaphylaxis", "cytokine release",
        "infusion reaction", "immune", "autoimmune",
    ],
    "Endocrine": [
        "hypothyroidism", "hyperthyroidism", "adrenal insufficiency",
        "hyperglycemia", "hypoglycemia", "diabetes",
    ],
    "Eye": [
        "blurred vision", "visual", "ocular", "conjunctivitis",
        "retinal", "uveitis",
    ],
}

# ── Dosage Parsing Patterns ──
ROUTE_PATTERNS = {
    "IV": r"\b(iv|intravenous|infusion)\b",
    "Oral": r"\b(oral|po|tablet|capsule|orally)\b",
    "Subcutaneous": r"\b(sc|subcutaneous|subq|sq)\b",
    "Intramuscular": r"\b(im|intramuscular)\b",
    "Topical": r"\b(topical|cream|ointment|transdermal)\b",
    "Intrathecal": r"\b(intrathecal|it)\b",
    "Inhalation": r"\b(inhalation|inhaled|nebulized)\b",
}

FREQUENCY_PATTERNS = {
    "QD (daily)": r"\b(daily|qd|once daily|once a day|q24h)\b",
    "BID": r"\b(bid|twice daily|2x daily|b\.i\.d)\b",
    "TID": r"\b(tid|three times daily|3x daily)\b",
    "Weekly": r"\b(weekly|qw|once weekly|q7d)\b",
    "Q2W": r"\b(q2w|every 2 weeks|biweekly|every two weeks|every 14 days)\b",
    "Q3W": r"\b(q3w|every 3 weeks|every three weeks|every 21 days)\b",
    "Q4W": r"\b(q4w|monthly|every 4 weeks|every 28 days)\b",
    "Continuous": r"\b(continuous|continuously)\b",
    "Single dose": r"\b(single dose|one-time|once)\b",
}

# Target drug count for final table
TARGET_DRUG_COUNT = 5000
