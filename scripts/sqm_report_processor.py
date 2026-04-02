#!/usr/bin/env python3
"""
SQM Research Report PDF Processor
===================================
Processes SQM Research Report PDFs from an input folder, extracts metadata,
validates APIR/Ticker codes against a reference CSV, and produces a single
zip file (SQM_Output_<timestamp>.zip) containing three CSV files:
  1. Rename list  – successfully matched files with proposed new filenames
  2. Exceptions   – files where the APIR/Ticker code could not be validated
  3. Audit log    – full reasoning trail for every processed file

The three CSVs are written to the output folder, bundled into the zip, and
then deleted so only the zip remains in the output folder.

USAGE
-----
  python sqm_report_processor.py

Configure the paths in the CONFIG section below before running.
"""

import os
import re
import csv
import calendar
import sys
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import pdfplumber
import pandas as pd

# ============================================================
# CONFIG  – change these paths before running
# ============================================================
INPUT_FOLDER  = r"C:\Users\NathanKazakevich\OneDrive - Count\2. SQMR files"
OUTPUT_FOLDER = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2c. Research Report outputs"
APIR_CSV_PATH = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2a. Claude renaming inputs and process\Updated IO and APIR table.csv"
# ============================================================


# ---- helpers -------------------------------------------------------

def get_aedt_timestamp() -> str:
    """Return current AEDT time as YYYY_MM_DD_HH_MM_SS."""
    tz = ZoneInfo("Australia/Sydney")
    return datetime.now(tz).strftime("%Y_%m_%d_%H_%M_%S")


def load_apir_csv(path: str) -> tuple[set, dict]:
    """
    Load the APIR reference CSV.
    Returns:
        valid_codes : set of all valid APIR/Ticker codes (Column A)
        name_to_code: dict mapping normalised IO names → APIR/Ticker code
    """
    df = pd.read_csv(path, encoding="utf-8-sig")
    # Column A is the first column regardless of its exact header text
    code_col = df.columns[0]
    valid_codes = set(df[code_col].dropna().astype(str).str.strip())

    name_to_code: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row[code_col]).strip()
        for col in df.columns[1:]:           # Columns B-F are IO name columns
            raw_name = str(row[col]).strip()
            if raw_name and raw_name.lower() not in ("nan", "#n/a", ""):
                norm = normalise_name(raw_name)
                if norm:
                    name_to_code[norm] = code
    return valid_codes, name_to_code


def normalise_name(name: str) -> str:
    """Lower-case, strip punctuation and common fund-class labels."""
    name = name.lower()
    # Remove common class/unit labels
    for token in ("class a", "class b", "wholesale", "retail",
                  "hedged", "unhedged", "units", "unit trust",
                  "fund", "- daily", "daily"):
        name = name.replace(token, "")
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


# ---- APIR / Ticker extraction patterns ----------------------------

APIR_PATTERN   = re.compile(r"\b([A-Z]{3}[0-9]{4}AU)\b")
TICKER_CODES   = {"CD1", "MOT", "CD2", "PE1", "CD3", "MXT", "MA1",
                  "REV", "LEND", "GPEQ", "QRI", "PCX"}
TICKER_PATTERN = re.compile(r"\b(" + "|".join(re.escape(t) for t in TICKER_CODES) + r")\b")


# ---- SQM-specific field extraction --------------------------------

# These patterns are tuned to the standard SQM Research Report layout.

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text from every page of a PDF."""
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_parts.append(t)
    return "\n".join(text_parts)


def extract_apir_codes(text: str) -> list[str]:
    """Return deduplicated list of APIR codes found in text."""
    codes = list(dict.fromkeys(APIR_PATTERN.findall(text)))
    return codes


def extract_ticker_codes(text: str) -> list[str]:
    """Return deduplicated list of known Ticker codes found in text."""
    codes = list(dict.fromkeys(TICKER_PATTERN.findall(text)))
    return codes


# SQM Summary table patterns
# The Fund Description table in SQM reports always has:
#   "APIR code   <CODE>"  (possibly with different whitespace)
#   "Report Date:  DD Month YYYY"  (on page 2, the intro page)
#   "Fund Name   <Name>"

APIR_IN_TABLE  = re.compile(
    r"APIR\s+code\s+([A-Z]{3}[0-9]{4}AU)",
    re.IGNORECASE
)

FUND_NAME_RE   = re.compile(
    r"Fund\s+Name\s+(.+)",
    re.IGNORECASE
)

# "Report Date: 19 December 2025"  or  "Report Date:  23 May 2025"
REPORT_DATE_RE = re.compile(
    r"Report\s+Date[:\s]+(\d{1,2}\s+\w+\s+\d{4})",
    re.IGNORECASE
)

# Fallback: cover page often just shows "Month YYYY" or "May 2025"
MONTH_YEAR_RE  = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{4})\b",
    re.IGNORECASE
)


def extract_fund_name(text: str) -> str:
    """Extract the fund name from the SQM Fund Description table."""
    m = FUND_NAME_RE.search(text)
    if m:
        # The value can run onto the next line; take only the first logical line
        name = m.group(1).strip().split("\n")[0].strip()
        return name
    return ""


def extract_apir_from_table(text: str) -> str | None:
    """Extract APIR code explicitly labelled in the Fund Description table."""
    m = APIR_IN_TABLE.search(text)
    if m:
        return m.group(1).strip()
    return None


def parse_date(date_str: str) -> str | None:
    """
    Parse date strings such as:
      "19 December 2025"  →  "2025_12_19"
      "December 2025"     →  "2025_12_31"
    Returns None if unparseable.
    """
    date_str = date_str.strip()
    # Try full date first
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y_%m_%d")
        except ValueError:
            pass
    # Month + Year only → use last day of that month
    for fmt in ("%B %Y", "%b %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            last_day = calendar.monthrange(dt.year, dt.month)[1]
            return f"{dt.year}_{dt.month:02d}_{last_day:02d}"
        except ValueError:
            pass
    return None


def extract_document_date(text: str) -> str | None:
    """
    Extract the Report Date from the standard SQM report layout.
    Tries:
      1. "Report Date: DD Month YYYY" label (most reliable)
      2. First Month YYYY pattern found (cover page or intro)
    Returns date in YYYY_MM_DD format or None.
    """
    # Method 1 – explicit Report Date label
    m = REPORT_DATE_RE.search(text)
    if m:
        parsed = parse_date(m.group(1))
        if parsed:
            return parsed

    # Method 2 – first Month YYYY found (likely cover-page date)
    m = MONTH_YEAR_RE.search(text)
    if m:
        date_str = f"{m.group(1)} {m.group(2)}"
        parsed = parse_date(date_str)
        if parsed:
            return parsed

    return None


# ---- per-file processing ------------------------------------------

def process_pdf(
    pdf_path: str,
    valid_codes: set,
    name_to_code: dict,
    run_timestamp: str,
) -> dict:
    """
    Process a single PDF file following the classification rules.
    Returns a result dict used to populate the three output CSVs that are
    later bundled into the output zip file.
    """
    filename    = os.path.basename(pdf_path)
    doc_type    = "SQMR"          # All files in this batch are SQM Research Reports
    result = {
        "original_filename": filename,
        "doc_type":          doc_type,
        "apir_found_in_doc": [],
        "validated_codes":   [],
        "unvalidated_codes": [],
        "fund_name":         "",
        "date_str":          None,
        "method_used":       "",
        "reasoning":         [],
        "rename_rows":       [],   # list of dicts for rename CSV
        "exception_rows":    [],   # list of dicts for exceptions CSV
        "success":           False,
    }

    log = result["reasoning"]
    log.append(f"Processing file: {filename}")
    log.append(f"Document type assigned: {doc_type} (SQM Research Report – all files in batch)")

    # --- Extract text ---
    try:
        full_text = extract_text_from_pdf(pdf_path)
    except Exception as exc:
        log.append(f"ERROR: Could not extract text from PDF – {exc}")
        result["exception_rows"].append({
            "original_filename": filename,
            "field_issue":       f"PDF extraction error: {exc}",
        })
        return result

    # --- Fund name ---
    fund_name = extract_fund_name(full_text)
    result["fund_name"] = fund_name
    log.append(f"Fund name extracted: '{fund_name}'")

    # --- Step 1: APIR extraction – labelled table value first ---
    table_apir = extract_apir_from_table(full_text)
    all_apir   = extract_apir_codes(full_text)
    tickers    = extract_ticker_codes(full_text)
    all_codes  = list(dict.fromkeys(
        ([table_apir] if table_apir else []) + all_apir + tickers
    ))

    result["apir_found_in_doc"] = all_codes
    log.append(f"APIR/Ticker-First method used (Method 1).")
    log.append(f"Codes extracted from document content (NOT filename): {all_codes}")

    # --- Validate ---
    validated   = [c for c in all_codes if c in valid_codes]
    unvalidated = [c for c in all_codes if c not in valid_codes]
    result["validated_codes"]   = validated
    result["unvalidated_codes"] = unvalidated
    log.append(f"Validated codes (found in CSV Column A): {validated}")
    if unvalidated:
        log.append(f"Unvalidated codes (not in CSV): {unvalidated}")

    # --- If no validated codes, try name-fallback ---
    if not validated:
        log.append("No validated APIR/Ticker codes found. Attempting Name Fallback (Method 2).")
        result["method_used"] = "Name Fallback (Method 2)"
        norm_fund = normalise_name(fund_name) if fund_name else ""
        matched_code = name_to_code.get(norm_fund)
        if matched_code:
            validated = [matched_code]
            result["validated_codes"] = validated
            log.append(f"Name fallback matched '{fund_name}' → {matched_code}")
        else:
            log.append(f"Name fallback failed: '{fund_name}' did not match any IO name in CSV.")
    else:
        result["method_used"] = "APIR/Ticker-First (Method 1)"

    # --- Date extraction ---
    date_val = extract_document_date(full_text)
    result["date_str"] = date_val
    if date_val:
        log.append(f"Document date extracted: {date_val}")
    else:
        log.append("WARNING: Could not determine document date.")

    # --- Build output rows ---
    ext = os.path.splitext(filename)[1].lower()

    # Exceptions for unvalidated codes
    for code in unvalidated:
        result["exception_rows"].append({
            "original_filename": filename,
            "field_issue":       f"APIR_NOT_IN_SPREADSHEET: code '{code}' found in document but not in CSV",
        })

    if not validated:
        # No codes at all
        result["exception_rows"].append({
            "original_filename": filename,
            "field_issue":       "APIR_NOT_FOUND: no APIR/Ticker code could be validated",
        })
        log.append("File added to Exceptions list (no validated code).")
        return result

    if not date_val:
        result["exception_rows"].append({
            "original_filename": filename,
            "field_issue":       "DATE_NOT_FOUND: document date could not be determined",
        })
        log.append("File added to Exceptions list (no date).")
        return result

    # Success – one rename row per validated code
    for code in validated:
        renamed = f"{code}_{doc_type}_{date_val}{ext}"
        result["rename_rows"].append({
            "original_filename": filename,
            "renamed_filename":  renamed,
            "apir_ticker":       code,
            "doc_type":          doc_type,
            "date":              date_val,
        })
        log.append(f"Rename row: '{filename}' → '{renamed}' (code={code}, date={date_val})")

    result["success"] = True
    log.append(f"Total validated codes used: {len(validated)}")
    return result


# ---- main ----------------------------------------------------------

def main():
    # Validate paths
    input_folder  = Path(INPUT_FOLDER)
    output_folder = Path(OUTPUT_FOLDER)
    apir_csv_path = Path(APIR_CSV_PATH)

    if not input_folder.is_dir():
        print(f"ERROR: Input folder not found: {input_folder}")
        sys.exit(1)
    if not apir_csv_path.is_file():
        print(f"ERROR: APIR CSV not found: {apir_csv_path}")
        sys.exit(1)

    output_folder.mkdir(parents=True, exist_ok=True)

    # Generate run timestamp (AEDT)
    ts = get_aedt_timestamp()
    print(f"Run timestamp (AEDT): {ts}")

    # Load reference data
    print(f"Loading APIR reference CSV: {apir_csv_path}")
    valid_codes, name_to_code = load_apir_csv(str(apir_csv_path))
    print(f"  Loaded {len(valid_codes)} valid APIR/Ticker codes.")

    # Collect PDFs – use a seen-set to avoid duplicates on case-insensitive filesystems
    seen: set[str] = set()
    pdf_files: list[Path] = []
    for p in sorted(input_folder.iterdir()):
        if p.suffix.lower() == ".pdf" and p.name.lower() not in seen:
            seen.add(p.name.lower())
            pdf_files.append(p)
    if not pdf_files:
        print(f"No PDF files found in {input_folder}")
        sys.exit(0)
    print(f"Found {len(pdf_files)} PDF file(s) to process.")

    # Process each file
    all_rename_rows    = []
    all_exception_rows = []
    all_log_rows       = []

    full_prompt = (
        "SQM Research Report batch processor. "
        "For each PDF: extract fund name, APIR code (from labelled table field), "
        "and Report Date. Validate APIR against Updated_IO_and_APIR_table.csv. "
        "Output SQMR type code. Produce rename list, exceptions list, audit log."
    )

    output_filenames = {
        "rename":    f"Rename_list_{ts}.csv",
        "exception": f"Exceptions_list_{ts}.csv",
        "log":       f"Rename_log_{ts}.csv",
    }

    for pdf_path in pdf_files:
        result = process_pdf(str(pdf_path), valid_codes, name_to_code, ts)

        all_rename_rows.extend(result["rename_rows"])
        all_exception_rows.extend(result["exception_rows"])

        reasoning_text = " | ".join(result["reasoning"])
        all_log_rows.append({
            "input_file_name":          result["original_filename"],
            "successfully_renamed":     "Yes" if result["success"] else "No",
            "date_time_prompt_run":     ts,
            "full_prompt_used":         full_prompt,
            "files_generated":          ", ".join(output_filenames.values()),  # CSVs bundled into zip
            "document_level_reasoning": reasoning_text,
        })

    # Write Rename list
    rename_path = output_folder / output_filenames["rename"]
    with open(rename_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "original_filename", "renamed_filename",
            "apir_ticker", "doc_type", "date"
        ])
        writer.writeheader()
        writer.writerows(all_rename_rows)
    # Write Exceptions list
    exc_path = output_folder / output_filenames["exception"]
    with open(exc_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["original_filename", "field_issue"])
        writer.writeheader()
        writer.writerows(all_exception_rows)

    # Write Audit log – QUOTE_ALL + pipe-separated reasoning = one row per document
    log_path = output_folder / output_filenames["log"]
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "input_file_name", "successfully_renamed",
                "date_time_prompt_run", "full_prompt_used",
                "files_generated", "document_level_reasoning",
            ],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(all_log_rows)

    # Bundle all three CSVs into a single zip file, then remove the loose CSVs
    zip_filename = f"SQM_Output_{ts}.zip"
    zip_path = output_folder / zip_filename
    csv_paths = [rename_path, exc_path, log_path]
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in csv_paths:
            zf.write(p, arcname=p.name)
    for p in csv_paths:
        p.unlink()
    print(f"Output zip written:      {zip_path}")
    print(f"  Contains:")
    print(f"    - {output_filenames['rename']}")
    print(f"    - {output_filenames['exception']}")
    print(f"    - {output_filenames['log']}")

    # Summary
    print(f"\nDone. {len(all_rename_rows)} rename rows, "
          f"{len(all_exception_rows)} exception rows, "
          f"{len(all_log_rows)} audit rows.")


if __name__ == "__main__":
    main()
