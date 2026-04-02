"""
ingest.py
=========
Main pipeline orchestrator. For each source folder in 1. Input/Inbox:
  1. Copies PDFs to local input folder so the processor script can read them.
  2. Runs the appropriate processor script, captures its zip output.
  3. Reads rename_rows and exception_rows from the zip.
  4. For each rename_row:
       a. Compute SHA-256 of the original PDF.
       b. Check hash_registry — if known, mark as duplicate, skip copy.
       c. If new: copy renamed file to 3. APIR_documents/{apir_code}/
       d. Move original PDF to 1. Input/Processed/
       e. Write to files + hash_registry + runs tables in DuckDB.
  5. For each exception_row:
       a. Move original PDF to 1. Input/Failed/
       b. Write to exceptions + files tables in DuckDB.
  6. After all sources processed: call export_index.py to regenerate outputs.

USAGE
-----
  # Process all sources
  python pipeline\ingest.py

  # Process one specific source
  python pipeline\ingest.py --source LSCR

  # Process without regenerating index at the end
  python pipeline\ingest.py --no-export

  # Dry run — show what would happen without moving/copying any files
  python pipeline\ingest.py --dry-run
"""

import argparse
import csv
import io
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DB_PATH, OUTPUT_DIR, LOG_DIR,
    LOCAL_DISCLOSURE_INPUT,
    APIR_DOCS_ROOT, PROCESSED_DIR, FAILED_DIR,
    SOURCE_FOLDERS,
    RUN_BATCH_SCRIPT, LONSEC_SCRIPT, ZENITH_SCRIPT,
    MORNINGSTAR_SCRIPT, SQM_SCRIPT,
    APIR_CSV,
)
import hash_check as hc

try:
    import duckdb
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"])
    import duckdb


# ── Processor dispatch ────────────────────────────────────────────────────────

def run_disclosure_processor(source_dir: Path, temp_output: Path) -> Path | None:
    """
    Copy PDFs from source_dir into a temporary input_pdfs subfolder,
    run run_batch.py, return path to the output zip.
    run_batch.py writes zips to ./output/ relative to its working directory.
    """
    subfolder_name = "_ingest_temp"
    local_input = LOCAL_DISCLOSURE_INPUT / subfolder_name
    local_input.mkdir(parents=True, exist_ok=True)

    # Copy all PDFs recursively from OneDrive inbox to local input folder
    pdfs = list(source_dir.rglob("*.pdf"))
    if not pdfs:
        return None
    for pdf in pdfs:
        dest = local_input / pdf.name
        if not dest.exists():
            shutil.copy2(pdf, dest)

    result = subprocess.run(
        [sys.executable, str(RUN_BATCH_SCRIPT), subfolder_name],
        capture_output=True, text=True,
        cwd=str(RUN_BATCH_SCRIPT.parent),
    )
    if result.returncode != 0:
        print(f"  run_batch.py stderr:\n{result.stderr[-2000:]}")
        return None

    # Find the zip written to output/
    zips = sorted((RUN_BATCH_SCRIPT.parent / "output").glob(f"{subfolder_name}_*.zip"))
    return zips[-1] if zips else None


def run_research_processor(script: Path, source_dir: Path, temp_output: Path) -> Path | None:
    """
    Run a research house processor script with patched INPUT/OUTPUT folder
    by passing environment overrides. Each script reads INPUT_FOLDER and
    OUTPUT_FOLDER from its CONFIG block — we override those via a small
    wrapper call that sets them before importing.
    """
    wrapper = f"""
import sys, importlib.util, types
spec = importlib.util.spec_from_file_location("proc", r"{script}")
mod  = importlib.util.module_from_spec(spec)
mod.INPUT_FOLDER  = r"{source_dir}"
mod.OUTPUT_FOLDER = r"{temp_output}"
spec.loader.exec_module(mod)
mod.main()
"""
    result = subprocess.run(
        [sys.executable, "-c", wrapper],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  Processor stderr:\n{result.stderr[-2000:]}")
        return None

    zips = sorted(Path(temp_output).glob("*.zip"))
    return zips[-1] if zips else None


# ── Zip parsing ───────────────────────────────────────────────────────────────

def parse_zip_outputs(zip_path: Path) -> tuple[list[dict], list[dict]]:
    """
    Extract rename_rows and exception_rows from a processor output zip.
    Returns (rename_rows, exception_rows) as lists of dicts.
    """
    rename_rows    = []
    exception_rows = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            base = Path(name).name.lower()
            if base.startswith("rename_list"):
                with zf.open(name) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                    for row in reader:
                        rename_rows.append(dict(row))
            elif base.startswith("exceptions_list"):
                with zf.open(name) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                    for row in reader:
                        exception_rows.append(dict(row))

    return rename_rows, exception_rows


def _get(row: dict, *keys) -> str:
    """Try multiple possible column name variants, return stripped value or ''."""
    for k in keys:
        v = row.get(k, "")
        if v:
            return str(v).strip()
    return ""


# ── File movement helpers ─────────────────────────────────────────────────────

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def safe_move(src: Path, dest_dir: Path, dry_run: bool) -> Path:
    """Move src into dest_dir, appending _1, _2 etc. if dest already exists."""
    ensure_dir(dest_dir)
    dest = dest_dir / src.name
    if dest.exists():
        stem, suffix = src.stem, src.suffix
        i = 1
        while dest.exists():
            dest = dest_dir / f"{stem}_{i}{suffix}"
            i += 1
    if not dry_run:
        shutil.move(str(src), str(dest))
    return dest


def safe_copy(src: Path, dest: Path, dry_run: bool):
    """Copy src to dest, creating parent dirs as needed."""
    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)


# ── DB write helpers ──────────────────────────────────────────────────────────

def insert_file(con, file_id, sha256, original_name, renamed_name,
                apir_code, doc_type, doc_date, confidence,
                source_folder, status, raw_path, renamed_path, run_id):
    con.execute("""
        INSERT OR IGNORE INTO files
            (file_id, sha256, original_name, renamed_name, apir_code, doc_type,
             doc_date, confidence, source_folder, status, raw_path, renamed_path,
             ingested_at, run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [file_id, sha256, original_name, renamed_name, apir_code, doc_type,
          doc_date, confidence, source_folder, status, raw_path, renamed_path,
          datetime.now(timezone.utc), run_id])


def insert_exception(con, run_id, file_id, original_name, reason_code, detail):
    con.execute("""
        INSERT INTO exceptions
            (exception_id, run_id, file_id, original_name, reason_code, detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [str(uuid.uuid4()), run_id, file_id, original_name,
          reason_code, detail, datetime.now(timezone.utc)])


# ── Per-source processor ──────────────────────────────────────────────────────

PROCESSOR_MAP = {
    "Disclosure_docs": ("disclosure", None),
    "LSCR":            ("research",   LONSEC_SCRIPT),
    "ZNTR":            ("research",   ZENITH_SCRIPT),
    "MSTR":            ("research",   MORNINGSTAR_SCRIPT),
    "SQMR":            ("research",   SQM_SCRIPT),
}

# Column name variants across the different processor scripts
RENAME_COL_ORIGINAL  = ("Original file name", "original_filename", "Original Filename")
RENAME_COL_RENAMED   = ("Renamed file name",  "renamed_filename",  "Renamed Filename")
RENAME_COL_APIR      = ("APIR / Ticker Code", "apir_ticker",       "APIR",  "APIR / Ticker")
RENAME_COL_DOCTYPE   = ("Document Type Code", "doc_type",          "Doctype")
RENAME_COL_DATE      = ("Date",               "date")
RENAME_COL_CONF      = ("Confidence score",   "confidence")

EXCEPT_COL_ORIGINAL  = ("Original file name", "original_filename", "Original Filename")
EXCEPT_COL_REASON    = ("Field(s) which could not be completed", "field_issue",
                        "Field(s) which could not be completed", "Reason Code")


def process_source(source_name: str, con, dry_run: bool, run_id: str) -> dict:
    """Process one inbox source folder. Returns stats dict."""
    source_dir = SOURCE_FOLDERS.get(source_name)
    if source_dir is None:
        print(f"  Unknown source '{source_name}' — skipping.")
        return {}

    pdfs = list(source_dir.rglob("*.pdf"))
    if not pdfs:
        print(f"  No PDFs found in {source_dir} — skipping.")
        return {"files_processed": 0, "renamed": 0, "failed": 0, "duplicates": 0}

    print(f"  Found {len(pdfs)} PDFs in {source_dir}")

    # Build a name → path index for quick lookup after processor runs
    pdf_index: dict[str, Path] = {}
    for p in pdfs:
        pdf_index[p.name] = p
        # Also index by stem in case processor strips extension
        pdf_index[p.stem] = p

    proc_type, script_path = PROCESSOR_MAP[source_name]

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        print(f"  Running processor...")

        if proc_type == "disclosure":
            zip_path = run_disclosure_processor(source_dir, tmp_path)
        else:
            zip_path = run_research_processor(script_path, source_dir, tmp_path)

        if zip_path is None:
            print(f"  ERROR: Processor produced no zip output for {source_name}.")
            return {"files_processed": len(pdfs), "renamed": 0, "failed": len(pdfs), "duplicates": 0}

        rename_rows, exception_rows = parse_zip_outputs(zip_path)
        print(f"  Processor output: {len(rename_rows)} rename rows, "
              f"{len(exception_rows)} exception rows.")

        # Save zip to local output dir for audit trail
        if not dry_run:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(zip_path, OUTPUT_DIR / zip_path.name)

    stats = {"files_processed": len(pdfs), "renamed": 0, "failed": 0, "duplicates": 0}

    # Track which original files were handled (to catch any not in rename or exception lists)
    handled: set[str] = set()

    # ── Process rename rows ────────────────────────────────────────────────
    for row in rename_rows:
        orig_name    = _get(row, *RENAME_COL_ORIGINAL)
        renamed_name = _get(row, *RENAME_COL_RENAMED)
        apir_code    = _get(row, *RENAME_COL_APIR)
        doc_type     = _get(row, *RENAME_COL_DOCTYPE)
        doc_date     = _get(row, *RENAME_COL_DATE)
        confidence_s = _get(row, *RENAME_COL_CONF)
        confidence   = float(confidence_s) if confidence_s else None

        if not orig_name:
            continue

        orig_path = pdf_index.get(orig_name) or pdf_index.get(Path(orig_name).stem)
        if orig_path is None:
            print(f"    WARNING: original file '{orig_name}' not found in inbox — skipping row.")
            continue

        handled.add(orig_name)
        digest = hc.compute_sha256(orig_path)

        if hc.is_known(con, digest):
            # Duplicate — file already in APIR_documents
            existing = hc.lookup(con, digest)
            print(f"    DUPLICATE: {orig_name} already ingested as "
                  f"{existing['renamed_name'] if existing else '?'} — skipping copy.")
            stats["duplicates"] += 1
            file_id = str(uuid.uuid4())
            if not dry_run:
                insert_file(con, file_id, digest, orig_name, renamed_name,
                            apir_code, doc_type, doc_date, confidence,
                            source_name, "skipped_duplicate",
                            str(orig_path), None, run_id)
                safe_move(orig_path, PROCESSED_DIR, dry_run=False)
            else:
                print(f"    [DRY RUN] Would move {orig_name} → Processed/")
            continue

        # New file — copy renamed version to APIR_documents
        if apir_code:
            dest_dir  = APIR_DOCS_ROOT / apir_code
            dest_path = dest_dir / renamed_name
        else:
            dest_dir  = APIR_DOCS_ROOT / "_unknown"
            dest_path = dest_dir / renamed_name

        file_id = str(uuid.uuid4())

        if dry_run:
            print(f"    [DRY RUN] {orig_name}")
            print(f"              → copy to {dest_path}")
            print(f"              → move original to Processed/")
            stats["renamed"] += 1
            continue

        safe_copy(orig_path, dest_path, dry_run=False)
        processed_path = safe_move(orig_path, PROCESSED_DIR, dry_run=False)

        insert_file(con, file_id, digest, orig_name, renamed_name,
                    apir_code, doc_type, doc_date, confidence,
                    source_name, "renamed",
                    str(processed_path), str(dest_path), run_id)
        hc.register(con, digest, file_id, renamed_name)
        stats["renamed"] += 1
        print(f"    OK: {orig_name} → {apir_code}/{renamed_name}")

    # ── Process exception rows ─────────────────────────────────────────────
    for row in exception_rows:
        orig_name   = _get(row, *EXCEPT_COL_ORIGINAL)
        reason_code = _get(row, *EXCEPT_COL_REASON)

        if not orig_name:
            continue

        # Don't double-handle files that had a successful rename row too
        if orig_name in handled:
            continue
        handled.add(orig_name)

        orig_path = pdf_index.get(orig_name) or pdf_index.get(Path(orig_name).stem)
        if orig_path is None:
            print(f"    WARNING: exception file '{orig_name}' not found in inbox.")
            continue

        digest  = hc.compute_sha256(orig_path)
        file_id = str(uuid.uuid4())

        if dry_run:
            print(f"    [DRY RUN] EXCEPTION {orig_name} ({reason_code}) → Failed/")
            stats["failed"] += 1
            continue

        failed_path = safe_move(orig_path, FAILED_DIR, dry_run=False)
        insert_file(con, file_id, digest, orig_name, None,
                    None, None, None, None,
                    source_name, "failed",
                    str(failed_path), None, run_id)
        insert_exception(con, run_id, file_id, orig_name, reason_code,
                         reason_code)
        stats["failed"] += 1
        print(f"    FAILED: {orig_name} ({reason_code})")

    # ── Any PDFs not mentioned in rename or exception lists ────────────────
    for pdf in pdfs:
        if pdf.name not in handled:
            print(f"    WARNING: {pdf.name} was not in rename or exception output — "
                  f"leaving in Inbox for manual review.")

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="APL document ingest pipeline.")
    parser.add_argument("--source",    default=None,
                        help="Process only this source folder (e.g. LSCR). Default: all.")
    parser.add_argument("--no-export", action="store_true",
                        help="Skip regenerating index/masterlist after ingest.")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Show what would happen without moving or copying files.")
    args = parser.parse_args()

    if args.dry_run:
        print("*** DRY RUN — no files will be moved or copied ***\n")

    # Ensure output dirs exist
    for d in (OUTPUT_DIR, LOG_DIR, PROCESSED_DIR, FAILED_DIR):
        if not args.dry_run:
            ensure_dir(d)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    sources = [args.source] if args.source else list(SOURCE_FOLDERS.keys())
    run_id  = str(uuid.uuid4())
    started = datetime.now(timezone.utc)

    if not args.dry_run:
        con.execute("""
            INSERT INTO runs (run_id, source_folder, started_at)
            VALUES (?, ?, ?)
        """, [run_id, args.source or "all", started])

    total = {"files_processed": 0, "renamed": 0, "failed": 0, "duplicates": 0}

    for source in sources:
        print(f"\n── {source} ──────────────────────────────────────────")
        stats = process_source(source, con, args.dry_run, run_id)
        for k in total:
            total[k] += stats.get(k, 0)

    if not args.dry_run:
        con.execute("""
            UPDATE runs SET completed_at=?, files_processed=?, renamed=?, failed=?, duplicates=?
            WHERE run_id=?
        """, [datetime.now(timezone.utc),
              total["files_processed"], total["renamed"],
              total["failed"], total["duplicates"], run_id])

    con.close()

    print(f"\n{'='*60}")
    print(f"INGEST COMPLETE")
    print(f"  Files processed : {total['files_processed']}")
    print(f"  Renamed         : {total['renamed']}")
    print(f"  Failed          : {total['failed']}")
    print(f"  Duplicates      : {total['duplicates']}")
    print(f"{'='*60}")

    if not args.no_export and not args.dry_run:
        print("\nRegenerating index and masterlist...")
        export_script = Path(__file__).parent / "export_index.py"
        subprocess.run([sys.executable, str(export_script)], check=False)


if __name__ == "__main__":
    main()
