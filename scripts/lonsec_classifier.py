#!/usr/bin/env python3
"""
Lonsec Research Report Classifier & Renamer
Processes PDF documents in an input folder, extracts metadata from page 1,
validates APIR/Ticker codes against a reference CSV, and produces a single zip archive
containing three output CSVs: Rename_list, Exceptions_list, and Rename_log.

All Lonsec reports share a fixed page-1 structure:
  Line 0: <Fund / IO Name>
  Line 1: <APIR_CODE> Author: <n> Published: <DD Mon YYYY>

Some reports have a fund name that wraps onto a second line (e.g.
"ATLAS Infrastructure Australian Feeder Fund AUD Hedged\nClass"),
pushing the APIR/Published line to position 2. The script checks
line 1 first and falls back to line 2 if no match is found.

Only lines 0–2 are parsed, and the APIR reference CSV is loaded as a
plain set for O(1) lookups. Both keep the script fast for large batches.
"""

import io
import os
import sys
import csv
import re
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo
import subprocess

def ensure_pdfplumber():
    try:
        import pdfplumber
    except ImportError:
        print("Installing pdfplumber...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pdfplumber"])
        import pdfplumber
        print("pdfplumber installed successfully.")

ensure_pdfplumber()
import pdfplumber


# =============================================================================
#  USER CONFIGURATION — edit these three paths before running
# =============================================================================

INPUT_FOLDER  = r"C:\Users\NathanKazakevich\ETSL Local\1b. LSCR individual raw"
OUTPUT_FOLDER = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2c. Research Report outputs"
APIR_CSV      = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2a. Claude renaming inputs and process\Updated IO and APIR table.csv"

# =============================================================================


# ── Constants ─────────────────────────────────────────────────────────────────

DOC_TYPE_CODE = 'LSCR'

# Single compiled regex parses the APIR/Published line in one pass:
#   group 1 → APIR / ticker code
#   group 2 → day   (DD)
#   group 3 → month (Mon / full)
#   group 4 → year  (YYYY)
# Matched against line 1 first, then line 2 as a fallback.
LINE1_RE = re.compile(
    r'^(\S+)'                                               # APIR / ticker
    r'\s+Author:\s+.+?\s+'                                  # Author: <n>  (non-greedy)
    r'Published:\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',     # Published: DD Mon YYYY
    re.IGNORECASE
)

MONTH_MAP = {
    'jan':1,  'feb':2,  'mar':3,  'apr':4,  'may':5,  'jun':6,
    'jul':7,  'aug':8,  'sep':9,  'oct':10, 'nov':11, 'dec':12,
    'january':1,   'february':2, 'march':3,     'april':4,
    'june':6,      'july':7,     'august':8,    'september':9,
    'october':10,  'november':11,'december':12,
}


# ── Timestamp helpers ──────────────────────────────────────────────────────────

def get_aedt_timestamp() -> str:
    return datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y_%m_%d_%H_%M_%S")

def get_aedt_display() -> str:
    return datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%d %H:%M:%S AEDT")


# ── Reference CSV loader ───────────────────────────────────────────────────────

def load_valid_codes(csv_path: str) -> set:
    """
    Load Column A of the APIR/Ticker reference CSV into a set.
    A set gives O(1) membership tests and uses less memory than a dict.
    """
    codes = set()
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('APIR / Ticker', '').strip()
            if code:
                codes.add(code)
    return codes


# ── Page-1 metadata extraction ─────────────────────────────────────────────────

def extract_metadata(pdf_path: str) -> dict:
    """
    Open the PDF and read only page 1. Extract:
      io_name   — line 0 (the fund/IO heading)
      apir_code — first token of line 1 (or line 2 if the fund name wraps)
      date_str  — YYYY_MM_DD from 'Published:' on line 1 or 2
      date_raw  — the raw date string as it appears in the document

    Line 1 is tried first; line 2 is the fallback for reports whose fund
    name spans two lines (e.g. "... AUD Hedged\nClass").

    Uses layout=False which skips expensive word-position computation,
    making text extraction ~3-5x faster than the pdfplumber default.
    """
    result = {'io_name': None, 'apir_code': None, 'date_str': None, 'date_raw': None}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return result
            text = pdf.pages[0].extract_text(layout=False) or ''
    except Exception as exc:
        result['error'] = str(exc)
        return result

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return result

    # Line 0 — IO / fund name
    result['io_name'] = lines[0]

    # Lines 1 & 2 — APIR code + published date
    # Line 1 is checked first; line 2 is the fallback for reports whose fund
    # name wraps onto a second line (e.g. "... AUD Hedged\nClass").
    m = None
    for line in lines[1:3]:
        m = LINE1_RE.match(line)
        if m:
            break

    if m:
        code    = m.group(1)
        day     = int(m.group(2))
        mon_str = m.group(3).lower()
        year    = m.group(4)
        mon     = MONTH_MAP.get(mon_str)

        result['apir_code'] = code
        result['date_raw']  = m.group(0).split('Published:')[1].strip()
        if mon:
            result['date_str'] = f"{year}_{mon:02d}_{day:02d}"

    return result


# ── Per-file processor ─────────────────────────────────────────────────────────

def process_file(pdf_path: str, valid_codes: set) -> dict:
    filename = os.path.basename(pdf_path)
    ext      = os.path.splitext(filename)[1].lower()

    result = {
        'original_filename': filename,
        'io_name':           None,
        'apir_raw':          None,
        'apir_validated':    False,
        'date_str':          None,
        'date_raw':          None,
        'renamed_rows':      [],
        'exception_rows':    [],
        'confidence':        0.0,
        'reasoning':         '',
        'success':           False,
    }

    meta = extract_metadata(pdf_path)

    if 'error' in meta:
        msg = f"PDF read error: {meta['error']}"
        result['reasoning'] = msg
        result['exception_rows'].append({'original_filename': filename, 'issue': msg})
        return result

    result['io_name']  = meta['io_name']
    result['apir_raw'] = meta['apir_code']
    result['date_str'] = meta['date_str']
    result['date_raw'] = meta['date_raw']

    code = meta['apir_code']

    # Validate APIR / ticker code
    if not code:
        result['exception_rows'].append({
            'original_filename': filename,
            'issue': "APIR_NOT_FOUND | No APIR or Ticker code found on page 1 line 1",
        })
    elif code not in valid_codes:
        result['exception_rows'].append({
            'original_filename': filename,
            'issue': (f"APIR_NOT_IN_SPREADSHEET | Code '{code}' found in document "
                      f"but not present in CSV Column A"),
        })
    else:
        result['apir_validated'] = True

    # Build rename row
    if result['apir_validated']:
        if result['date_str']:
            renamed = f"{code}_{DOC_TYPE_CODE}_{result['date_str']}{ext}"
            result['renamed_rows'].append({
                'original_filename': filename,
                'renamed_filename':  renamed,
                'apir':              code,
                'doc_type':          DOC_TYPE_CODE,
                'date':              result['date_str'],
                'confidence':        9.5,
            })
            result['confidence'] = 9.5
            result['success']    = True
        else:
            result['exception_rows'].append({
                'original_filename': filename,
                'issue': (f"DATE_NOT_FOUND | APIR validated ({code}) but published "
                          f"date could not be parsed from page 1 line 1"),
            })
            result['confidence'] = 5.0
    else:
        result['confidence'] = 2.0

    # Reasoning narrative
    date_note = (
        f"Published date found: '{result['date_raw']}' -> standardised: {result['date_str']}"
        if result['date_str'] else "Published date: NOT FOUND"
    )
    result['reasoning'] = ' | '.join([
        f"File: {filename}",
        f"IO Name (page 1, line 0): {result['io_name']}",
        "Extraction method: Fixed Lonsec structure — APIR from page 1 line 1 (NOT from filename)",
        f"Raw code on line 1: {code if code else 'None'}",
        f"Validated against CSV Column A: {'Yes' if result['apir_validated'] else 'No'}",
        date_note,
        f"Confidence score: {result['confidence']}",
        f"Outcome: {'Successfully renamed' if result['success'] else 'Exception — see Exceptions file'}",
    ])
    return result


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # Validate configured paths
    for label, path in [("INPUT_FOLDER", INPUT_FOLDER),
                         ("APIR_CSV",     APIR_CSV)]:
        if not os.path.exists(path):
            print(f"ERROR: {label} path not found: {path}")
            sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Load reference codes once — O(1) lookups for every file thereafter

    # Discover PDFs
    pdf_files = sorted([
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if f.lower().endswith('.pdf')
    ])

    if not pdf_files:
        print("No PDFs found. Exiting.")
        sys.exit(0)

    # Process all files
    all_results = []
    for path in pdf_files:
        all_results.append(process_file(path, valid_codes))

    # Single timestamp for all three output filenames
    ts               = get_aedt_timestamp()
    run_time_display = get_aedt_display()

    rename_name    = f"Rename_list_{ts}.csv"
    exception_name = f"Exceptions_list_{ts}.csv"
    log_name       = f"Rename_log_{ts}.csv"
    zip_name       = f"LSCR_outputs_{ts}.zip"
    zip_path       = os.path.join(OUTPUT_FOLDER, zip_name)

    files_generated = f"{rename_name}, {exception_name}, {log_name} (inside {zip_name})"

    def make_csv_bytes(headers: list, rows: list) -> bytes:
        """Serialise a CSV table to UTF-8 bytes ready for writing directly into the zip archive."""
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(headers)
        w.writerows(rows)
        return buf.getvalue().encode('utf-8')

    # Rename List — serialised to bytes for zip entry
    rename_rows = [
        [r['original_filename'], r['renamed_filename'],
         r['apir'], r['doc_type'], r['date'], r['confidence']]
        for res in all_results for r in res['renamed_rows']
    ]
    rename_bytes = make_csv_bytes(
        ['Original file name', 'Renamed file name',
         'APIR / Ticker Code', 'Document Type Code', 'Date', 'Confidence score'],
        rename_rows,
    )

    # Exceptions List — serialised to bytes for zip entry
    exc_rows = [
        [r['original_filename'], r['issue']]
        for res in all_results for r in res['exception_rows']
    ]
    exc_bytes = make_csv_bytes(
        ['Original file name', "Field(s) which could not be completed"],
        exc_rows,
    )

    # Audit / Defensibility Log — serialised to bytes for zip entry
    log_rows = [
        [res['original_filename'],
         'Yes' if res['success'] else 'No',
         res['confidence'],
         run_time_display,
         files_generated,
         res['reasoning']]
        for res in all_results
    ]
    log_bytes = make_csv_bytes(
        ['Input file name', 'Successfully renamed', 'Confidence score',
         'Date & time prompt run', 'Files generated', 'Document-level reasoning'],
        log_rows,
    )

    # Bundle all three CSVs into a single timestamped zip archive in OUTPUT_FOLDER
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(rename_name,    rename_bytes)
        zf.writestr(exception_name, exc_bytes)
        zf.writestr(log_name,       log_bytes)

    print(f"\nOutput zip  -> {zip_path}")
    print(f"  Contains  : {rename_name}")
    print(f"            : {exception_name}")
    print(f"            : {log_name}")

    # Summary
    succeeded = sum(1 for r in all_results if r['success'])
    print(f"\n{'-'*50}")
    print(f"Processed : {len(all_results)} file(s)")
    print(f"Renamed   : {succeeded}")
    print(f"Exceptions: {len(all_results) - succeeded}")
    print(f"Timestamp : {run_time_display}")


if __name__ == '__main__':
    main()
