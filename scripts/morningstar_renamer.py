"""
Morningstar Report Filename Processor
======================================
Reads PDF filenames from a folder, extracts APIR/Ticker codes from the filename,
validates against the IO/APIR reference CSV, and produces a single zip file
containing three output CSVs:
  1. Rename_list_[timestamp].csv      - Successfully matched files with new names
  2. Exceptions_list_[timestamp].csv  - Files where no valid APIR/Ticker was found
  3. Rename_log_[timestamp].csv       - Full audit trail

The zip file is saved to the output folder as:
  Morningstar_Rename_Output_[timestamp].zip

HOW TO RUN:
  1. Edit the three paths in the CONFIG section directly below.
  2. Open Terminal (Mac/Linux) or Command Prompt / PowerShell (Windows).
  3. Navigate to the folder containing this script, e.g.:
       cd /Users/yourname/Documents/scripts
  4. Run:
       python morningstar_renamer.py

  Alternatively, pass paths as command-line arguments (these override the CONFIG values):
       python morningstar_renamer.py --folder /path/to/pdfs --csv /path/to/table.csv --output /path/to/output
"""

import re
import csv
import argparse
import zipfile
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    HAS_ZONEINFO = True
except ImportError:
    # Python < 3.9 fallback - timestamps will be local time, clearly labelled
    HAS_ZONEINFO = False


# ===========================================================================
# CONFIG - Edit these three paths before running
# ===========================================================================

# Folder containing the Morningstar PDF reports to be processed.
# Examples:
#   Windows : r"C:\Users\yourname\Documents\Morningstar Reports"
#   Mac/Linux: "/Users/yourname/Documents/Morningstar Reports"
INPUT_FOLDER = r"C:\Users\NathanKazakevich\OneDrive - Count\2. MSTR files"

# Full path to the Updated_IO_and_APIR_table.csv reference file.
# Examples:
#   Windows : r"C:\Users\yourname\Documents\Updated_IO_and_APIR_table.csv"
#   Mac/Linux: "/Users/yourname/Documents/Updated_IO_and_APIR_table.csv"
CSV_FILE = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2a. Claude renaming inputs and process\Updated IO and APIR table.csv"

# Folder where the output zip file will be saved.
# The zip contains all three output CSVs.
# Will be created automatically if it does not exist.
# Examples:
#   Windows : r"C:\Users\yourname\Documents\Output"
#   Mac/Linux: "/Users/yourname/Documents/Output"
OUTPUT_FOLDER = r"C:\Users\NathanKazakevich\OneDrive - Count\McGing - Team - Documents\Clients\ETSL\2026 Inv Govnce\4. Analysis\2. Docs\2c. Research Report outputs"

# ===========================================================================


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DOC_TYPE = "MSTR"  # All files processed by this script are Morningstar reports

# APIR pattern: three uppercase letters, four digits, then AU.
# Uses lookaround instead of \b so underscore delimiters (e.g. __HOW0019AU__) work correctly.
APIR_PATTERN = re.compile(r'(?<![A-Z0-9])([A-Z]{3}[0-9]{4}AU)(?![A-Z0-9])')

# Known ticker codes (non-APIR short codes from the reference CSV)
TICKER_CODES = {"CD1", "MOT", "CD2", "PE1", "CD3", "MXT", "MA1",
                "REV", "LEND", "GPEQ", "QRI", "PCX"}

# Ticker pattern - sorted longest-first to avoid partial matches on shorter codes.
TICKER_PATTERN = re.compile(
    r'(?<![A-Za-z0-9])(' +
    '|'.join(re.escape(t) for t in sorted(TICKER_CODES, key=len, reverse=True)) +
    r')(?![A-Za-z0-9])'
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_timestamp():
    """Return (filename_ts, display_ts) strings in AEDT, or local time as fallback."""
    if HAS_ZONEINFO:
        now = datetime.now(ZoneInfo("Australia/Sydney"))
        label = "AEDT"
    else:
        now = datetime.now()
        label = "local"
    return (
        now.strftime("%Y_%m_%d_%H_%M_%S"),
        now.strftime("%Y-%m-%d %H:%M:%S") + f" ({label})"
    )


def get_file_modified_date(filepath: Path) -> str:
    """Return the file's last-modified date as YYYY_MM_DD."""
    dt = datetime.fromtimestamp(filepath.stat().st_mtime)
    return dt.strftime("%Y_%m_%d")


def load_apir_csv(csv_path: str) -> set:
    """Load the reference CSV and return a set of all valid APIR/Ticker codes (Column A)."""
    valid_codes = set()
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("APIR / Ticker", "").strip()
            if code:
                valid_codes.add(code)
    return valid_codes


def extract_codes_from_filename(filename: str) -> list:
    """
    Extract all APIR and Ticker codes from the filename stem.
    Returns a deduplicated list of (code_type, code) tuples in order of appearance.
    """
    stem = Path(filename).stem
    found = []
    seen = set()

    for m in APIR_PATTERN.finditer(stem):
        code = m.group(1)
        if code not in seen:
            found.append(("APIR", code))
            seen.add(code)

    for m in TICKER_PATTERN.finditer(stem):
        code = m.group(1)
        if code not in seen:
            found.append(("Ticker", code))
            seen.add(code)

    return found


def build_renamed_filename(code: str, date_str: str, extension: str) -> str:
    """Build the renamed filename: [Code]_[TypeCode]_[Date].[ext]"""
    ext = extension.lstrip('.').lower()
    return f"{code}_{DOC_TYPE}_{date_str}.{ext}"


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_folder(folder: str, csv_path: str, output_folder: str, dry_run: bool = False):
    """
    Main processing function. Iterates over all PDFs in `folder`, extracts
    APIR/Ticker codes from filenames, validates against the reference CSV,
    and writes three output CSVs bundled into a single zip file.
    """

    folder_path = Path(folder).resolve()
    output_path = Path(output_folder).resolve()

    # Validate inputs
    if not folder_path.exists():
        print(f"\nERROR: Input folder not found:\n  {folder_path}")
        print("Please update INPUT_FOLDER in the CONFIG section at the top of this script.")
        return

    if not Path(csv_path).exists():
        print(f"\nERROR: CSV reference file not found:\n  {csv_path}")
        print("Please update CSV_FILE in the CONFIG section at the top of this script.")
        return

    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Input folder : {folder_path}")
    print(f"Reference CSV: {csv_path}")
    print(f"Output folder: {output_path}\n")

    print(f"Loading reference CSV...")
    valid_codes = load_apir_csv(csv_path)
    print(f"  Loaded {len(valid_codes)} valid APIR/Ticker codes.\n")

    # Collect PDF files (case-insensitive extension match)
    pdf_files = sorted(
        p for p in folder_path.iterdir()
        if p.is_file() and p.suffix.lower() == ".pdf"
    )

    if not pdf_files:
        print(f"No PDF files found in: {folder_path}")
        return

    print(f"Found {len(pdf_files)} PDF file(s) to process.\n")
    print("-" * 60)

    ts_filename, ts_display = get_timestamp()

    rename_rows    = []
    exception_rows = []
    log_rows       = []

    output_rename    = output_path / f"Rename_list_{ts_filename}.csv"
    output_exception = output_path / f"Exceptions_list_{ts_filename}.csv"
    output_log       = output_path / f"Rename_log_{ts_filename}.csv"
    output_zip       = output_path / f"Morningstar_Rename_Output_{ts_filename}.zip"

    prompt_text = (
        "Script: morningstar_renamer.py | "
        "Mode: Extract APIR/Ticker from filename, validate against IO/APIR CSV, "
        f"rename as [Code]_MSTR_[ModifiedDate].[ext] | "
        f"Reference CSV: {csv_path} | "
        f"Input folder: {folder_path} | "
        f"Run timestamp: {ts_display}"
    )

    # -----------------------------------------------------------------------
    # Process each file
    # -----------------------------------------------------------------------
    for pdf in pdf_files:
        filename  = pdf.name
        extension = pdf.suffix
        date_str  = get_file_modified_date(pdf)

        extracted   = extract_codes_from_filename(filename)
        validated   = [(ct, c) for ct, c in extracted if c in valid_codes]
        unvalidated = [(ct, c) for ct, c in extracted if c not in valid_codes]

        successfully_renamed = len(validated) > 0

        # Build audit reasoning
        reasoning_parts = [
            f"File: '{filename}'",
            "Extraction method: APIR/Ticker codes extracted from filename only "
            "(this script does not read document content).",
            f"Codes found in filename: {[c for _, c in extracted] if extracted else 'None'}",
            f"Codes validated against CSV: {[c for _, c in validated] if validated else 'None'}",
            f"Codes NOT in CSV: {[c for _, c in unvalidated] if unvalidated else 'None'}",
            f"Document date: derived from file last-modified date = {date_str}",
            f"Document type: {DOC_TYPE} (fixed for all Morningstar reports processed by this script)",
        ]

        if not extracted:
            reasoning_parts.append(
                "EXCEPTION: No APIR or Ticker code pattern found in filename. "
                "File sent to Exceptions list."
            )
        elif not validated:
            reasoning_parts.append(
                f"EXCEPTION: Code(s) found in filename ({[c for _, c in unvalidated]}) "
                "did not match any entry in Column A of the reference CSV. "
                "File sent to Exceptions list."
            )
        else:
            renamed_files = [build_renamed_filename(c, date_str, extension) for _, c in validated]
            reasoning_parts.append(
                f"SUCCESS: {len(validated)} validated code(s) -> renamed file(s): {renamed_files}"
            )
            if unvalidated:
                reasoning_parts.append(
                    f"NOTE: Additional code(s) found in filename but not in CSV (ignored): "
                    f"{[c for _, c in unvalidated]}"
                )

        reasoning = " | ".join(reasoning_parts)

        # Populate rows
        if successfully_renamed:
            for _, code in validated:
                renamed = build_renamed_filename(code, date_str, extension)
                rename_rows.append({
                    "Original Filename": filename,
                    "Renamed Filename":  renamed,
                    "APIR / Ticker":     code,
                    "Document Type":     DOC_TYPE,
                    "Date":              date_str,
                })
            log_rows.append({
                "Input file name":          filename,
                "Successfully renamed":     "Yes",
                "Confidence score":         "9.0",
                "Date & time prompt run":   ts_display,
                "Full prompt used":         prompt_text,
                "Files generated":          f"{output_rename.name} | {output_exception.name} | {output_log.name}",
                "Document-level reasoning": reasoning,
            })
        else:
            if not extracted:
                reason_code   = "APIR_NOT_FOUND"
                reason_detail = "No APIR or Ticker code pattern detected in filename."
            else:
                codes_str     = ", ".join(c for _, c in unvalidated)
                reason_code   = "APIR_NOT_IN_SPREADSHEET"
                reason_detail = (
                    f"Code(s) found in filename ({codes_str}) "
                    "not present in Column A of reference CSV."
                )

            exception_rows.append({
                "Original Filename": filename,
                "Reason Code":       reason_code,
                "Reason Detail":     reason_detail,
            })
            log_rows.append({
                "Input file name":          filename,
                "Successfully renamed":     "No",
                "Confidence score":         "0.0",
                "Date & time prompt run":   ts_display,
                "Full prompt used":         prompt_text,
                "Files generated":          f"{output_rename.name} | {output_exception.name} | {output_log.name}",
                "Document-level reasoning": reasoning,
            })

        # Console output per file
        status = "RENAMED  " if successfully_renamed else "EXCEPTION"
        print(f"  [{status}]  {filename}")
        if validated:
            for _, code in validated:
                print(f"              -> {build_renamed_filename(code, date_str, extension)}")
        else:
            print(f"              {exception_rows[-1]['Reason Code']}: {exception_rows[-1]['Reason Detail']}")

    # -----------------------------------------------------------------------
    # Write output CSVs and bundle into zip
    # -----------------------------------------------------------------------
    print("-" * 60)
    print(f"Summary: {len(rename_rows)} rename row(s) | {len(exception_rows)} exception(s)\n")

    if dry_run:
        print("[DRY RUN] No output files written.")
        return

    with open(output_rename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Original Filename", "Renamed Filename", "APIR / Ticker", "Document Type", "Date"
        ])
        writer.writeheader()
        writer.writerows(rename_rows)

    with open(output_exception, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Original Filename", "Reason Code", "Reason Detail"
        ])
        writer.writeheader()
        writer.writerows(exception_rows)

    with open(output_log, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Input file name", "Successfully renamed", "Confidence score",
            "Date & time prompt run", "Full prompt used", "Files generated",
            "Document-level reasoning",
        ])
        writer.writeheader()
        writer.writerows(log_rows)

    # Bundle all three CSVs into a single zip file, then remove the loose CSVs
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(output_rename,    output_rename.name)
        zf.write(output_exception, output_exception.name)
        zf.write(output_log,       output_log.name)

    output_rename.unlink()
    output_exception.unlink()
    output_log.unlink()

    print(f"  Output zip     -> {output_zip}")
    print(f"    Contains: {output_rename.name}")
    print(f"             {output_exception.name}")
    print(f"             {output_log.name}")
    print(f"\nDone. Run timestamp: {ts_display}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Process Morningstar PDF filenames: extract APIR/Ticker, validate, rename.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "If no arguments are provided, the CONFIG values at the top of the\n"
            "script are used. Command-line arguments override CONFIG values."
        )
    )
    parser.add_argument("--folder", default=None, help="Folder containing Morningstar PDF reports.")
    parser.add_argument("--csv",    default=None, help="Path to Updated_IO_and_APIR_table.csv.")
    parser.add_argument("--output", default=None, help="Folder where the output zip file will be saved.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview results in the terminal only; do not write output files.")

    args = parser.parse_args()

    # Command-line args override CONFIG values; fall back to CONFIG if not provided
    folder = args.folder or INPUT_FOLDER
    csv_   = args.csv    or CSV_FILE
    output = args.output or OUTPUT_FOLDER

    process_folder(folder=folder, csv_path=csv_, output_folder=output, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
