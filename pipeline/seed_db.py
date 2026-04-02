"""
seed_db.py
==========
One-time initialisation script. Run this ONCE before using the pipeline.

What it does:
  1. Creates the DuckDB database and all tables (safe to re-run — uses IF NOT EXISTS).
  2. Loads apir_reference from the APIR CSV.
  3. Loads not_available from not_available.csv (if it exists).
  4. Scans 3. APIR_documents/, computes SHA-256 for every existing renamed PDF,
     and seeds the files and hash_registry tables so the dedup check works
     immediately on first ingest run.

Safe to re-run: existing rows are skipped via INSERT OR IGNORE / ON CONFLICT DO NOTHING.

USAGE
-----
  cd C:\Development\ETSL-Renaming
  python pipeline\seed_db.py

Optional flags:
  --skip-scan    Skip scanning APIR_documents (only create schema + load reference data)
  --dry-run      Print what would be inserted without writing to DB
"""

import argparse
import csv
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running from repo root without installing as package
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DB_PATH, APIR_CSV, NOT_AVAILABLE_CSV, APIR_DOCS_ROOT
)

try:
    import duckdb
except ImportError:
    print("duckdb not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"])
    import duckdb


# ── Schema DDL ────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        VARCHAR PRIMARY KEY,
    source_folder VARCHAR,
    started_at    TIMESTAMP,
    completed_at  TIMESTAMP,
    files_processed INTEGER DEFAULT 0,
    renamed         INTEGER DEFAULT 0,
    failed          INTEGER DEFAULT 0,
    duplicates      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    file_id       VARCHAR PRIMARY KEY,
    sha256        VARCHAR NOT NULL,
    original_name VARCHAR NOT NULL,
    renamed_name  VARCHAR,
    apir_code     VARCHAR,
    doc_type      VARCHAR,
    doc_date      VARCHAR,
    confidence    DOUBLE,
    source_folder VARCHAR,
    status        VARCHAR,
    raw_path      VARCHAR,
    renamed_path  VARCHAR,
    ingested_at   TIMESTAMP,
    run_id        VARCHAR
);

CREATE TABLE IF NOT EXISTS exceptions (
    exception_id  VARCHAR PRIMARY KEY,
    run_id        VARCHAR,
    file_id       VARCHAR,
    original_name VARCHAR,
    reason_code   VARCHAR,
    detail        VARCHAR,
    created_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hash_registry (
    sha256        VARCHAR PRIMARY KEY,
    first_seen_at TIMESTAMP,
    file_id       VARCHAR,
    renamed_name  VARCHAR
);

CREATE TABLE IF NOT EXISTS not_available (
    apir_code  VARCHAR,
    doc_type   VARCHAR,
    reason     VARCHAR,
    updated_at TIMESTAMP,
    updated_by VARCHAR,
    PRIMARY KEY (apir_code, doc_type)
);

CREATE TABLE IF NOT EXISTS apir_reference (
    apir_code        VARCHAR PRIMARY KEY,
    official_name    VARCHAR,
    mstar_io_name    VARCHAR,
    centric_io_name  VARCHAR,
    amg_io_name      VARCHAR,
    dash_io_name     VARCHAR,
    platform_io_name VARCHAR,
    loaded_at        TIMESTAMP
);
"""


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def create_schema(con):
    con.executescript(SCHEMA_SQL)
    print("Schema created / verified.")


def load_apir_reference(con, dry_run: bool):
    if not APIR_CSV.exists():
        print(f"WARNING: APIR CSV not found at {APIR_CSV} — skipping apir_reference load.")
        return

    now = datetime.now(timezone.utc)
    rows = []
    with open(APIR_CSV, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("APIR / Ticker", "").strip()
            if not code:
                continue
            rows.append((
                code,
                row.get("Official Name", "").strip() or None,
                row.get("Mstar IO name", "").strip() or None,
                row.get("Centric IO name", "").strip() or None,
                row.get("AMG IO name", "").strip() or None,
                row.get("DASH - Super Simplifier IO Name", "").strip() or None,
                row.get("PlatformPlus IO Name", "").strip() or None,
                now,
            ))

    if dry_run:
        print(f"[DRY RUN] Would insert {len(rows)} rows into apir_reference.")
        return

    con.executemany("""
        INSERT OR REPLACE INTO apir_reference
            (apir_code, official_name, mstar_io_name, centric_io_name,
             amg_io_name, dash_io_name, platform_io_name, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    print(f"apir_reference: {len(rows)} rows loaded.")


def parse_not_available_csv(csv_path: Path) -> list[tuple]:
    """
    Parse the wide-format Not_Available CSV.

    Column layout:
      Code, Morningstar Investment option name, Priority,
      Morningstar, Comment,   ← doc type MSTR
      Lonsec,      Comment,   ← doc type LSCR
      SQM,         Comment,   ← doc type SQMR
      Zenith,      Comment,   ← doc type ZNTR
      ARPT,        Comment,
      TMDX,        Comment,
      PDSX,        Comment,
      PERF,        Comment

    Only rows where the status value is exactly 'Not Available'
    (case-insensitive) are inserted into the not_available table.
    All other values (Yes, Downloaded, TBC, blank) are ignored.

    Returns a list of (apir_code, doc_type, reason, fund_name) tuples.
    """
    # Maps the column header name in the CSV → internal doc_type code
    COL_TO_DOCTYPE = {
        "Morningstar": "MSTR",
        "Lonsec":      "LSCR",
        "SQM":         "SQMR",
        "Zenith":      "ZNTR",
        "ARPT":        "ARPT",
        "TMDX":        "TMDX",
        "PDSX":        "PDSX",
        "PERF":        "PERF",
    }
    NOT_AVAILABLE_VALUES = {"not available"}

    rows = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for record in reader:
            apir = record.get("Code", "").strip()
            fund_name = record.get("Morningstar Investment option name", "").strip()
            if not apir:
                continue
            for col_header, doc_type in COL_TO_DOCTYPE.items():
                status = record.get(col_header, "").strip().lower()
                if status in NOT_AVAILABLE_VALUES:
                    rows.append((apir, doc_type, "Not Available", fund_name))
    return rows


def load_not_available(con, dry_run: bool):
    if not NOT_AVAILABLE_CSV.exists():
        print(f"INFO: not_available.csv not found at {NOT_AVAILABLE_CSV} — skipping.")
        print("      Place the Not_Available CSV (wide format) at:")
        print(f"      {NOT_AVAILABLE_CSV}")
        return

    now  = datetime.now(timezone.utc)
    parsed = parse_not_available_csv(NOT_AVAILABLE_CSV)
    rows = [
        (apir, doc_type, reason, now, None)
        for apir, doc_type, reason, _ in parsed
    ]

    if dry_run:
        print(f"[DRY RUN] Would insert {len(rows)} rows into not_available.")
        # Show a short sample so the user can verify the parse
        for r in rows[:5]:
            print(f"          {r[0]:<14}  {r[1]:<6}  {r[2]}")
        if len(rows) > 5:
            print(f"          ... and {len(rows) - 5} more.")
        return

    con.executemany("""
        INSERT OR REPLACE INTO not_available
            (apir_code, doc_type, reason, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?)
    """, rows)
    print(f"not_available: {len(rows)} rows loaded.")


def seed_from_apir_docs(con, dry_run: bool):
    """
    Scan 3. APIR_documents/, hash every PDF, insert into files + hash_registry.
    Skips files whose sha256 already exists in hash_registry.
    """
    if not APIR_DOCS_ROOT.exists():
        print(f"WARNING: APIR_documents root not found at {APIR_DOCS_ROOT} — skipping scan.")
        return

    existing_hashes = {
        row[0] for row in con.execute("SELECT sha256 FROM hash_registry").fetchall()
    }

    now = datetime.now(timezone.utc)
    pdf_files = list(APIR_DOCS_ROOT.rglob("*.pdf"))
    print(f"Scanning {len(pdf_files)} PDFs in {APIR_DOCS_ROOT} ...")

    import uuid
    import re

    # Pattern: APIRCODE_DOCTYPE_DATE.pdf
    # e.g. AAP0001AU_PDSX_2024_06_30.pdf  or  CD1_ARPT_2023_06_30.pdf
    fname_re = re.compile(
        r"^([A-Z0-9]+)_(PDSX|TMDX|ARPT|PERF|ZNTR|LSCR|MSTR|SQMR)_(\d{4}_\d{2}_\d{2})\.pdf$",
        re.IGNORECASE,
    )

    files_rows   = []
    hash_rows    = []
    skipped      = 0
    unmatched    = []

    for pdf in pdf_files:
        digest = sha256_of_file(pdf)

        if digest in existing_hashes:
            skipped += 1
            continue

        m = fname_re.match(pdf.name)
        apir_code = m.group(1).upper() if m else None
        doc_type  = m.group(2).upper() if m else None
        doc_date  = m.group(3)         if m else None

        if not m:
            unmatched.append(pdf.name)

        file_id = str(uuid.uuid4())
        files_rows.append((
            file_id,
            digest,
            pdf.name,          # original_name — same as renamed for seeded files
            pdf.name,          # renamed_name
            apir_code,
            doc_type,
            doc_date,
            None,              # confidence unknown for seeded files
            "seeded",          # source_folder
            "renamed",         # status
            None,              # raw_path — original source file unknown
            str(pdf),          # renamed_path
            now,
            None,              # run_id
        ))
        hash_rows.append((digest, now, file_id, pdf.name))
        existing_hashes.add(digest)

    if dry_run:
        print(f"[DRY RUN] Would insert {len(files_rows)} files, skip {skipped} duplicates.")
        if unmatched:
            print(f"[DRY RUN] {len(unmatched)} files did not match rename pattern:")
            for u in unmatched[:10]:
                print(f"          {u}")
        return

    if files_rows:
        con.executemany("""
            INSERT OR IGNORE INTO files
                (file_id, sha256, original_name, renamed_name, apir_code, doc_type,
                 doc_date, confidence, source_folder, status, raw_path, renamed_path,
                 ingested_at, run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, files_rows)

        con.executemany("""
            INSERT OR IGNORE INTO hash_registry
                (sha256, first_seen_at, file_id, renamed_name)
            VALUES (?, ?, ?, ?)
        """, hash_rows)

    print(f"Seeded {len(files_rows)} files, skipped {skipped} already-known hashes.")
    if unmatched:
        print(f"WARNING: {len(unmatched)} files did not match the rename pattern "
              f"(APIR_DOCTYPE_DATE.pdf) — inserted with null metadata:")
        for u in unmatched[:20]:
            print(f"  {u}")
        if len(unmatched) > 20:
            print(f"  ... and {len(unmatched) - 20} more.")


def main():
    parser = argparse.ArgumentParser(description="Initialise APL DuckDB and seed from existing files.")
    parser.add_argument("--skip-scan", action="store_true",
                        help="Skip scanning APIR_documents — only create schema and load reference data.")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Print what would be inserted without writing to DB.")
    args = parser.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))

    create_schema(con)
    load_apir_reference(con, args.dry_run)
    load_not_available(con, args.dry_run)

    if not args.skip_scan:
        seed_from_apir_docs(con, args.dry_run)
    else:
        print("Skipping APIR_documents scan (--skip-scan).")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
