"""
config.py
=========
Central path configuration for the APL document pipeline.
All scripts import from here — never hardcode paths elsewhere.

Edit ONEDRIVE_ROOT and LOCAL_ROOT if your machine paths differ.
"""

from pathlib import Path

# ── Root paths ────────────────────────────────────────────────────────────────

LOCAL_ROOT = Path(r"C:\Development\Document-Pipeline")

ONEDRIVE_ROOT = Path(
    r"C:\Users\NathanKazakevich\OneDrive - Count"
    r"\McGing - Team - Documents\Clients\ETSL"
    r"\2026 Inv Govnce\4. Analysis\2. Docs"
)

# ── Local paths (code, reference data, intermediate working files) ────────────

REFERENCE_DIR     = LOCAL_ROOT / "reference_data"
APIR_CSV          = REFERENCE_DIR / "Updated IO and APIR table.csv"
NOT_AVAILABLE_CSV = REFERENCE_DIR / "not_available.csv"
OUTPUT_DIR        = LOCAL_ROOT / "output"          # processor zip files land here
PIPELINE_DIR      = LOCAL_ROOT / "pipeline"
LOG_DIR           = LOCAL_ROOT / "logs"

# Existing run_batch.py input folder (disclosure docs processed locally first)
LOCAL_DISCLOSURE_INPUT = LOCAL_ROOT / "input_pdfs"

# ── Database — lives on OneDrive so manager can run export from their machine ─

DB_PATH = ONEDRIVE_ROOT / "4. Database" / "apl.duckdb"

# ── OneDrive paths (shared with manager) ─────────────────────────────────────

# 1. Input
INBOX_ROOT      = ONEDRIVE_ROOT / "1. Input" / "1. inbox"
INBOX_DISCLOSURE = INBOX_ROOT / "Disclosure_docs"
INBOX_LONSEC    = INBOX_ROOT / "LSCR"
INBOX_ZENITH    = INBOX_ROOT / "ZNTR"
INBOX_MORNINGSTAR = INBOX_ROOT / "MSTR"
INBOX_SQM       = INBOX_ROOT / "SQMR"

PROCESSED_DIR   = ONEDRIVE_ROOT / "1. Input" / "2. Processed"
FAILED_DIR      = ONEDRIVE_ROOT / "1. Input" / "3. Failed"

# 3. APIR Documents
APIR_DOCS_ROOT  = ONEDRIVE_ROOT / "3. APIR_documents"

# 4. Exports (index, masterlist, tracker written here)
EXPORTS_DIR     = ONEDRIVE_ROOT / "2. File processing (Input - Output)" / "exports"

# ── Source folder registry ────────────────────────────────────────────────────
# Maps source folder name → inbox path.
# Used by ingest.py to discover which folders to process.

SOURCE_FOLDERS = {
    "Disclosure_docs": INBOX_DISCLOSURE,
    "LSCR":            INBOX_LONSEC,
    "ZNTR":            INBOX_ZENITH,
    "MSTR":            INBOX_MORNINGSTAR,
    "SQMR":            INBOX_SQM,
}

# ── Processor script paths ────────────────────────────────────────────────────

SCRIPTS_DIR               = LOCAL_ROOT / "scripts"
RUN_BATCH_SCRIPT          = LOCAL_ROOT / "run_batch.py"
LONSEC_SCRIPT             = SCRIPTS_DIR / "lonsec_classifier.py"
ZENITH_SCRIPT             = SCRIPTS_DIR / "zenith_report_processor.py"
MORNINGSTAR_SCRIPT        = SCRIPTS_DIR / "morningstar_renamer.py"
SQM_SCRIPT                = SCRIPTS_DIR / "sqm_report_processor.py"

# ── Document type sets ────────────────────────────────────────────────────────

MANDATORY_DOC_TYPES     = {"PDSX", "TMDX", "ARPT"}
SUPPLEMENTARY_DOC_TYPES = {"PERF", "ZNTR", "LSCR", "MSTR", "SQMR"}
ALL_DOC_TYPES           = MANDATORY_DOC_TYPES | SUPPLEMENTARY_DOC_TYPES
