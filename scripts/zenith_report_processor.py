
"""
Zenith Research Report PDF Processor
=====================================
Processes Zenith "Product Assessment" PDF reports from an input folder.

For each PDF it:
  1. Extracts: Fund Name, APIR/Ticker code, and Report Date
  2. Validates the APIR/Ticker code against the reference CSV
  3. Writes three output CSV files bundled into a single zip archive:
       - ZNTR_output_<timestamp>.zip containing:
           - Rename_list_<timestamp>.csv
           - Exceptions_list_<timestamp>.csv
           - Audit_log_<timestamp>.csv

Document type code is always ZNTR (Zenith Research Report).

Rename pattern:  <APIR>_ZNTR_<YYYY_MM_DD>.pdf
"""

import os
import re
import csv
import calendar
import subprocess
import sys
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

# Auto-install pypdf if not present
try:
    import pypdf
except ImportError:
    print("pypdf not found — installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pypdf"])
    import pypdf

# ---------------------------------------------------------------------------
# CONFIGURATION — change these paths as needed
# ---------------------------------------------------------------------------
INPUT_FOLDER  = r"C:\Users\NathanKazakevich\ETSL Local\1b. ZNTR individual raw"
OUTPUT_FOLDER = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2c. Research Report outputs"
APIR_CSV_PATH = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2a. Claude renaming inputs and process\Updated IO and APIR table.csv"
# ---------------------------------------------------------------------------

DOC_TYPE_CODE = "ZNTR"

# APIR pattern: three uppercase letters + four digits + "AU"
# No \b word-boundary — pypdf may strip spaces, making codes run together with surrounding text
APIR_PATTERN = re.compile(r'([A-Z]{3}[0-9]{4}AU)')

# Valid short ticker codes (from skill reference data)
VALID_TICKERS = {
    "CD1", "MOT", "CD2", "PE1", "CD3", "MXT",
    "MA1", "REV", "LEND", "GPEQ", "QRI", "PCX"
}

# ── Date extraction helpers ─────────────────────────────────────────────────

# Patterns for "Report as at DD Mon YYYY" or "Report as at Mon YYYY"
# \s* handles pypdf stripping spaces (e.g. "Reportasat22Mar2026")
DATE_FULL_RE   = re.compile(
    r'[Rr]eport\s*as\s*at\s*(\d{1,2})\s*([A-Za-z]+)\s*(\d{4})')
DATE_MY_RE     = re.compile(
    r'[Rr]eport\s*as\s*at\s*([A-Za-z]+)\s*(\d{4})')

# Fallback: "Rating issued on DD Mon YYYY"
RATING_DATE_RE = re.compile(
    r'[Rr]ating\s*issued\s*on\s*(\d{1,2})\s*([A-Za-z]+)\s*(\d{4})')

# Generic "DD Mon YYYY" anywhere in text
GENERIC_DMY_RE = re.compile(
    r'\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})\b')

MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}


def month_num(name: str) -> int | None:
    return MONTH_MAP.get(name.lower())


def last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def parse_date_string(text: str) -> tuple[str | None, str]:
    """Try to extract a date from text. Returns (YYYY_MM_DD or None, reasoning)."""

    # 1. "Report as at DD Mon YYYY"
    m = DATE_FULL_RE.search(text)
    if m:
        d, mon, y = int(m.group(1)), month_num(m.group(2)), int(m.group(3))
        if mon:
            return f"{y}_{mon:02d}_{d:02d}", "Extracted from 'Report as at DD Mon YYYY' header"

    # 2. "Report as at Mon YYYY"
    m = DATE_MY_RE.search(text)
    if m:
        mon, y = month_num(m.group(1)), int(m.group(2))
        if mon:
            d = last_day(y, mon)
            return f"{y}_{mon:02d}_{d:02d}", "Extracted from 'Report as at Mon YYYY' (last day of month used)"

    # 3. "Rating issued on DD Mon YYYY"
    m = RATING_DATE_RE.search(text)
    if m:
        d, mon, y = int(m.group(1)), month_num(m.group(2)), int(m.group(3))
        if mon:
            return f"{y}_{mon:02d}_{d:02d}", "Extracted from 'Rating issued on DD Mon YYYY'"

    # 4. First generic DD Mon YYYY found in text
    for m in GENERIC_DMY_RE.finditer(text):
        d_val, mon_name, y_val = int(m.group(1)), month_num(m.group(2)), int(m.group(3))
        if mon_name and 1 <= d_val <= 31:
            return f"{y_val}_{mon_name:02d}_{d_val:02d}", f"Extracted from generic date pattern '{m.group(0)}'"

    return None, "No date could be reliably extracted from the document"


# ── PDF text extraction ──────────────────────────────────────────────────────

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text from a PDF using pypdf."""
    text_parts = []
    with open(pdf_path, "rb") as f:
        reader = pypdf.PdfReader(f)
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
    return "\n".join(text_parts)


# ── Fund name extraction ─────────────────────────────────────────────────────

# In space-stripped pypdf output the layout is:
#   Line 1: "ProductAssessment"
#   Line 2: "Reportasat22Mar2026<disclaimer text>"  — all on one line
#   Line 3: "<FundName>"
#   Line 4: "Rating issued on ... | APIR: ..."
# The fund name is the line that precedes "Rating issued on" near the top of the doc.
FUND_NAME_BEFORE_RATING_RE = re.compile(
    r'([^\n]+)\n[^\n]*[Rr]ating\s*issued\s*on', re.IGNORECASE)

def extract_fund_name(text: str) -> str | None:
    # Try to find the line immediately before "Rating issued on"
    m = FUND_NAME_BEFORE_RATING_RE.search(text)
    if m:
        name = m.group(1).strip()
        # Reject if it looks like a URL or disclaimer line
        if len(name) < 80 and "http" not in name and "zenith" not in name.lower():
            return name
    # Fallback: line after Product Assessment header block
    m2 = FUND_NAME_RE.search(text)
    if m2:
        name = m2.group(1).strip()
        name = re.split(r'\s*Rating\s*issued', name, flags=re.IGNORECASE)[0].strip()
        if len(name) < 80 and "http" not in name:
            return name if name else None
    return None


# ── APIR / Ticker extraction ─────────────────────────────────────────────────

def extract_apir_codes(text: str) -> list[str]:
    """Return all unique APIR codes + tickers found in document text."""
    codes = set(APIR_PATTERN.findall(text))
    # Also look for known short tickers as whole words
    for ticker in VALID_TICKERS:
        if re.search(rf'\b{re.escape(ticker)}\b', text):
            codes.add(ticker)
    return list(codes)


# ── CSV reference loader ─────────────────────────────────────────────────────

def load_valid_apir_codes(csv_path: str) -> set[str]:
    valid = set()
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("APIR / Ticker", "").strip()
            if code:
                valid.add(code)
    return valid


# ── Timestamp ────────────────────────────────────────────────────────────────

def get_aedt_timestamp() -> str:
    aedt = datetime.now(ZoneInfo("Australia/Sydney"))
    return aedt.strftime("%Y_%m_%d_%H_%M_%S")


def get_aedt_display() -> str:
    aedt = datetime.now(ZoneInfo("Australia/Sydney"))
    return aedt.strftime("%Y-%m-%d %H:%M:%S AEDT")


# ── Main processing ──────────────────────────────────────────────────────────

def process_pdfs(input_folder: str, output_folder: str, apir_csv: str) -> None:
    os.makedirs(output_folder, exist_ok=True)

    valid_codes = load_valid_apir_codes(apir_csv)
    timestamp   = get_aedt_timestamp()
    run_time    = get_aedt_display()

    rename_rows     = []   # rows for Rename_list
    exception_rows  = []   # rows for Exceptions_list
    audit_rows      = []   # rows for Audit_log

    pdf_files = [f for f in os.listdir(input_folder)
                 if f.lower().endswith(".pdf")]

    if not pdf_files:
        print(f"No PDF files found in '{input_folder}'.")
        return

    for filename in sorted(pdf_files):
        pdf_path = os.path.join(input_folder, filename)
        reasoning_lines = []
        successfully_renamed = False
        confidence = 0.0
        renamed_filename = ""
        apir_used = ""
        date_used = ""

        reasoning_lines.append(f"=== Processing: {filename} ===")

        # --- Extract text ---
        try:
            text = extract_text_from_pdf(pdf_path)
        except Exception as e:
            reason = f"Could not extract text from PDF: {e}"
            exception_rows.append({
                "Original file name": filename,
                "Field(s) which could not be completed": reason,
            })
            reasoning_lines.append(f"FAILED: {reason}")
            audit_rows.append(_build_audit_row(
                filename, False, 0.0, run_time, reasoning_lines))
            continue

        # --- Document type ---
        # Always ZNTR for Zenith research reports; confirm by checking heading
        doc_type = DOC_TYPE_CODE
        if "Product Assessment" in text:
            reasoning_lines.append("Document type: ZNTR confirmed via 'Product Assessment' heading.")
        else:
            reasoning_lines.append("Document type: ZNTR assumed (heading not clearly found).")

        # --- Fund name ---
        fund_name = extract_fund_name(text)
        if fund_name:
            reasoning_lines.append(f"Fund name extracted: '{fund_name}'")
        else:
            reasoning_lines.append("Fund name: could not extract cleanly.")

        # --- APIR extraction (Method 1 — from document content only) ---
        reasoning_lines.append("APIR extraction method: APIR/Ticker-First (Method 1) — scanning document content only.")
        all_codes = extract_apir_codes(text)
        reasoning_lines.append(f"APIR/Ticker codes found in document content: {all_codes if all_codes else 'None'}")

        validated = [c for c in all_codes if c in valid_codes]
        not_validated = [c for c in all_codes if c not in valid_codes]

        reasoning_lines.append(f"Codes validated against CSV: {validated}")
        if not_validated:
            reasoning_lines.append(f"Codes NOT in CSV (will be exceptions): {not_validated}")

        # --- Date extraction ---
        date_str, date_reasoning = parse_date_string(text)
        reasoning_lines.append(f"Date determination: {date_reasoning}")
        if date_str:
            reasoning_lines.append(f"Date value: {date_str}")

        # --- Build output rows ---
        exceptions_for_this_file = []

        if not date_str:
            exceptions_for_this_file.append("DATE_NOT_FOUND — No date could be reliably extracted from the document")

        if not validated:
            if all_codes:
                exceptions_for_this_file.append(
                    f"APIR_NOT_IN_SPREADSHEET — Code(s) found in document ({', '.join(all_codes)}) "
                    f"but not present in Column A of CSV file")
            else:
                exceptions_for_this_file.append(
                    "APIR_NOT_FOUND — No APIR or Ticker code found anywhere in document content")

        if exceptions_for_this_file:
            exception_rows.append({
                "Original file name": filename,
                "Field(s) which could not be completed": "; ".join(exceptions_for_this_file),
            })
            reasoning_lines.append(f"Result: EXCEPTION — {'; '.join(exceptions_for_this_file)}")

        else:
            # One rename row per validated code
            for code in validated:
                renamed = f"{code}_{doc_type}_{date_str}.pdf"
                confidence = 9.0  # APIR from document + full date confirmed
                if "last day of month" in date_reasoning.lower():
                    confidence = 7.5

                rename_rows.append({
                    "Original Filename":  filename,
                    "Renamed Filename":   renamed,
                    "APIR":               code,
                    "Doctype":            doc_type,
                    "Date":               date_str,
                    "Confidence score":   confidence,
                })
                reasoning_lines.append(
                    f"Result: RENAMED → '{renamed}' (code={code}, confidence={confidence})")

            successfully_renamed = True
            apir_used  = ", ".join(validated)
            date_used  = date_str
            confidence = rename_rows[-1]["Confidence score"]

        audit_rows.append(_build_audit_row(
            filename, successfully_renamed, confidence, run_time, reasoning_lines))

    # --- Write output files ---
    rename_path    = os.path.join(output_folder, f"Rename_list_{timestamp}.csv")
    exception_path = os.path.join(output_folder, f"Exceptions_list_{timestamp}.csv")
    audit_path     = os.path.join(output_folder, f"Audit_log_{timestamp}.csv")

    _write_rename_list(rename_path, rename_rows)
    _write_exceptions_list(exception_path, exception_rows)
    _write_audit_log(audit_path, audit_rows, rename_path, exception_path)

    # --- Bundle CSVs into a single zip archive ---
    zip_name = f"ZNTR_output_{timestamp}.zip"
    zip_path = os.path.join(output_folder, zip_name)
    csv_paths = [rename_path, exception_path, audit_path]
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in csv_paths:
            zf.write(p, arcname=os.path.basename(p))

    # Remove the individual CSV files now that they're in the zip
    for p in csv_paths:
        os.remove(p)

    print(f"\nProcessing complete.")
    print(f"  PDFs processed : {len(pdf_files)}")
    print(f"  Renamed        : {len(rename_rows)}")
    print(f"  Exceptions     : {len(exception_rows)}")
    print(f"\nOutput zip written to '{output_folder}':")
    print(f"  {zip_name}")
    print(f"\nZip contains:")
    print(f"  {os.path.basename(rename_path)}")
    print(f"  {os.path.basename(exception_path)}")
    print(f"  {os.path.basename(audit_path)}")


# ── Output helpers ───────────────────────────────────────────────────────────

def _build_audit_row(filename, success, confidence, run_time, reasoning_lines):
    return {
        "Input file name":         filename,
        "Successfully renamed":    "Yes" if success else "No",
        "Confidence score":        confidence,
        "Date & time prompt run":  run_time,
        "Document-level reasoning": "\n".join(reasoning_lines),
    }


def _write_rename_list(path: str, rows: list[dict]) -> None:
    fieldnames = [
        "Original Filename",
        "Renamed Filename",
        "APIR",
        "Doctype",
        "Date",
        "Confidence score",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_exceptions_list(path: str, rows: list[dict]) -> None:
    fieldnames = ["Original file name", "Field(s) which could not be completed"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_audit_log(path: str, rows: list[dict],
                     rename_path: str, exception_path: str) -> None:
    # Record the zip archive name rather than the individual CSVs,
    # as the loose CSV files are deleted after bundling
    zip_name = os.path.basename(rename_path).replace(
        "Rename_list_", "ZNTR_output_"
    ).replace(".csv", ".zip")
    files_generated = zip_name
    fieldnames = [
        "Input file name",
        "Successfully renamed",
        "Confidence score",
        "Date & time prompt run",
        "Files generated",
        "Document-level reasoning",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row["Files generated"] = files_generated
            writer.writerow(row)


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    process_pdfs(INPUT_FOLDER, OUTPUT_FOLDER, APIR_CSV_PATH)
