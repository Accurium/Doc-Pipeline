---
name: skills
description: Runs the Investment Document Classification & Renaming pipeline for one or more subfolders.
---

# run-batch

Run the Investment Document Classification & Renaming pipeline (`run_batch.py`) for one or more subfolders.

## Usage

```
/run-batch {SUBFOLDER} [{SUBFOLDER2} ...]
```

**Example:**
```
/run-batch Pinnacle
/run-batch Pinnacle Magellan Vanguard
```

Each subfolder must exist under `input_pdfs/{SUBFOLDER}/` and contain one or more `.pdf` files.

---

## Pipeline Instructions

When this skill is invoked, execute the following steps in order. The subfolder name(s) are provided as arguments: $ARGUMENTS

Record the current wall-clock time as **RUN_START_TIME** (format: HH:MM:SS).

OUTPUT DISCIPLINE — applies to every step:
— Do not write prose narration, explanations, or commentary between steps.
— Permitted output between steps: log lines and mandatory checkpoint blocks only.
— If a continuation prompt would occur mid-output: resume with raw output only — no preamble, no re-introduction.

---

### STEP 0 — PRE-FLIGHT CHECKS

Before running any batch, confirm the following. Any failure: report to user and STOP.

**0a — Dependencies installed**

**Python packages** — check that all required packages are installed by running:
```
python -c "import pdfplumber, pytesseract, pdf2image"
```
If any import fails, install the missing packages with:
```
pip install pdfplumber pytesseract pdf2image
```
Then re-run the import check to confirm all packages are available.

**System binaries** — check that Tesseract and poppler are installed:
```
python -c "import pytesseract; pytesseract.get_tesseract_version()"
python -c "from pdf2image import convert_from_path"
```
The script auto-adds `%LOCALAPPDATA%\Programs\Tesseract-OCR` and `%LOCALAPPDATA%\Programs\poppler\poppler-24.08.0\Library\bin` to PATH at startup. If Tesseract is not found, install it:
```
winget install UB-Mannheim.TesseractOCR --accept-package-agreements --accept-source-agreements
```
If poppler is not found, download and extract the Windows binaries:
```
curl -L -o /tmp/poppler.zip "https://github.com/oschwartz10612/poppler-windows/releases/download/v24.08.0-0/Release-24.08.0-0.zip"
unzip -o /tmp/poppler.zip -d "%LOCALAPPDATA%/Programs/poppler"
```
Re-run the checks after installation.

FAIL (after install attempt): log `DEPS_INSTALL_FAILED — could not install required packages or system binaries`. STOP.

On success, log one line:
```
All required dependencies available
```

**0b — Script present**
Confirm `run_batch.py` exists in the working directory (`C:/Development/ETSL-Renaming`).
FAIL: log `SCRIPT_NOT_FOUND — run_batch.py missing from working directory`. STOP.

**0c — Reference data present**
Confirm `reference_data/Updated IO and APIR table.csv` exists and is readable.
Load the file. Confirm:
- It contains a header row with at minimum column A (`APIR / Ticker`) and columns B–F (IO name columns).
- It contains at least one data row.
FAIL: log `REF_DATA_NOT_FOUND — reference CSV missing or unreadable`. STOP.

**0d — Input folders present**
For each SUBFOLDER in $ARGUMENTS:
- Confirm `input_pdfs/{SUBFOLDER}/` exists and contains at least one `.pdf` file.
FAIL: log `INPUT_FOLDER_NOT_FOUND — {SUBFOLDER}` or `NO_PDFS_FOUND — {SUBFOLDER}`. STOP for that subfolder; continue with remaining subfolders if any.

**0e — OCR runtime check**
The script requires `pytesseract` and `pdf2image` to be installed, and Tesseract must be available on PATH. These are hard dependencies — the script will exit immediately at startup if they are missing.

Confirm OCR availability by checking that the script starts without a `FATAL: OCR dependencies missing` error.

FAIL: log `OCR_UNAVAILABLE — pytesseract or pdf2image not installed, or Tesseract not on PATH`. STOP.

On success, log one line:
```
OCR available — Tesseract fallback enabled
```

**0f — Output folder**
Confirm `output/` exists or can be created.

Log one line on all checks passing:
```
PRE-FLIGHT PASSED | subfolders: {N} | reference codes loaded: {N}
```

---

### STEP 1 — RUN BATCH (one per subfolder, sequential)

For each SUBFOLDER in $ARGUMENTS (run sequentially — do not parallelise across subfolders):

**1a — Execute the script**

Run:
```
python run_batch.py {SUBFOLDER}
```

from the working directory `C:/Development/ETSL-Renaming`.

Capture both stdout and stderr. Do not suppress output — stream it to the user as it runs.

**1b — Monitor for errors**

During execution, watch for the following in stdout/stderr:

| Signal | Action |
|--------|--------|
| `ERROR: subfolder not found` | Log `SUBFOLDER_NOT_FOUND — {SUBFOLDER}`. Mark as FAILED. Stop this subfolder. |
| `WARNING: No PDFs found` | Log `NO_PDFS — {SUBFOLDER}`. Mark as SKIPPED. Continue. |
| Unhandled Python exception / traceback | Log `SCRIPT_ERROR — {SUBFOLDER}: {first line of traceback}`. Mark as FAILED. Stop this subfolder. |
| Non-zero exit code | Log `EXIT_NONZERO — {SUBFOLDER}: exit code {N}`. Mark as FAILED. |

**1c — Confirm outputs**

On clean exit (exit code 0), confirm the following files were created under `output/`:
- `{SUBFOLDER}_Rename_list_{timestamp}.csv`
- `{SUBFOLDER}_Exceptions_list_{timestamp}.csv`
- `{SUBFOLDER}_Rename_log_{timestamp}.csv`
- `{SUBFOLDER}_{timestamp}.zip` (containing all three CSVs)

If any file is missing: log `OUTPUT_MISSING — {SUBFOLDER}: {filename}`. Mark as INCOMPLETE.

Log one line on success:
```
BATCH COMPLETE | {SUBFOLDER} | renamed: {N} | exceptions: {N} | zip: {SUBFOLDER}_{timestamp}.zip
```

Extract these counts from the script's final summary output block (lines beginning `Files processed`, `Successfully renamed`, `With exceptions`).

---

### STEP 2 — POST-RUN REVIEW

After all subfolders have been processed, compile and report the following to the user:

**2a — Run summary table**

For each subfolder, report:

| Subfolder | Status | PDFs processed | Renamed | Exceptions | Zip file |
|-----------|--------|----------------|---------|------------|----------|

Status values: `COMPLETE`, `FAILED`, `SKIPPED`, `INCOMPLETE`

**2b — Exception breakdown**

For each subfolder that produced exceptions, extract the exception reason codes from the script's summary output and list:

- `APIR_NOT_FOUND` count
- `APIR_NOT_IN_SPREADSHEET` count
- `DOCTYPE_UNKNOWN` count
- `DATE_NOT_FOUND` count
- `SCANNED_UNREADABLE` count — ⚠️ flag these for manual review
- `NAME_AMBIGUOUS` count
- `OCR_FALLBACK` count — extract from summary line `OCR fallback applied`; ⚠️ flag these for manual review (confidence penalised −0.5)
- `APIR_O_M` count (APIR in filename differed from content-derived code; file renamed using content code)
- `DOCTYPE_O_M` count (type code in filename differed from content-derived type; file renamed using filename type)
- `DATE_M` count (date in filename differed from content-derived date)

**2c — Flags requiring human review**

Explicitly call out any of the following:

- **SCANNED_UNREADABLE files:** These are image-only PDFs where OCR was either unavailable or also yielded no text. The script cannot rename them. List each affected filename. Manual review required.
- **OCR fallback files:** These were image-only PDFs that were successfully processed via Tesseract OCR. They carry a −0.5 confidence penalty. Extract affected filenames from `OCR_FALLBACK` entries in `Rename_log_*.csv` reasoning. Recommend human verification of classification and rename accuracy.
- **APIR_O_M files:** The APIR code in the original filename is valid but was not found in document content. An additional rename row has been generated for the filename APIR alongside the content-derived code(s). Verify which APIR is correct and remove the incorrect rename row.
- **DOCTYPE_O_M files:** The type code in the original filename differs from content-derived classification. The file has been renamed using the filename type — verify whether the content-derived type is actually correct.
- **DATE_M files:** The date in the original filename differs from the date found in document content. Renamed using content-derived date — verify.
- **Low confidence scores (< 5.0):** These rows used filename fallback or name fallback with uncertain results. Recommend manual spot-check.

**2d — Mismatch flags summary**

Report counts for filename vs content mismatches across all subfolders:

```
Filename fallback stats:
  APIR from filename fallback  : {N}
  DOCTYPE from filename fallback: {N}
  Date from filename fallback   : {N}

Mismatch flags (content vs filename):
  APIR_O_M   : {N}
  DOCTYPE_O_M: {N}
  DATE_M     : {N}
```

---

### STEP 3 — COMPLETION

Record the current wall-clock time as **RUN_END_TIME** (format: HH:MM:SS).
Calculate elapsed time.

Report:

```
==============================
RUN COMPLETE
==============================
Subfolders processed : {N}
Total PDFs processed : {N}
Total renamed        : {N}
Total exceptions     : {N}
Output directory     : C:/Development/ETSL-Renaming/output/

Total run time: Xm Ys ({HH:MM:SS} → {HH:MM:SS})
==============================
```

If any subfolder was marked FAILED or INCOMPLETE, append:

```
⚠️  The following subfolders require attention:
  - {SUBFOLDER}: {status} — {reason}
```

---

## Classification Rules (enforced by run_batch.py)

These rules are implemented in the script. This section documents them for awareness and human review:

| Rule | Detail |
|------|--------|
| **OCR fallback (mandatory)** | Image-only PDFs with no native text layer are processed via Tesseract OCR. OCR is a hard dependency — the pipeline will not start without it. Files successfully processed via OCR carry a −0.5 confidence penalty. Only if OCR also yields no text is the file flagged `SCANNED_UNREADABLE`. |
| **Filename doctype is authoritative** | The document type code from the original filename is always used for the renamed file. The script independently derives a doctype from document content; if it differs from the filename type, a `Doc_Mismatch` exception is raised but the filename type is kept. |
| **Content doctype classification** | Document type is independently derived from the cover page heading hierarchy, then section headings, then body — used for cross-check only. |
| **Never use filename for APIR** | APIR/Ticker codes are extracted from document content only (filename is a last-resort fallback). |
| **Validate all codes** | Every APIR/Ticker code extracted from document content is validated against Column A of the reference CSV before use. |
| **One row per validated code** | Multi-code documents (e.g. PDS with multiple fund options) generate one renamed output row per validated code. |
| **ARPT multi-match** | Annual report documents use a multi-match name fallback — all matched investment option names generate separate output rows. |
| **Batch re-validation** | At end of batch, every APIR/Ticker in the rename list is re-checked against the reference CSV. Failures move to exceptions as `APIR_NOT_IN_SPREADSHEET`. |
| **Filename fallbacks** | If APIR, type code, or date cannot be extracted from content, the script falls back to parsing the original filename as a last resort. Files processed via filename fallback carry reduced confidence scores and are flagged in the exceptions list. |
| **Filename APIR rename row (APIR_O_M)** | If the APIR/Ticker code in the original filename is valid (in reference CSV) but not found among content-extracted codes, an additional rename row is generated for the filename APIR and an `APIR_O_M` exception is raised. This ensures the filename APIR is not lost even when the document content yields different codes. Both exceptions (`APIR_Mismatch` and `APIR_O_M`) are removed if post-batch OCR retry confirms the filename APIR in the document. |

---

## Exception Reason Codes

| Code | Meaning |
|------|---------|
| `APIR_NOT_FOUND` | No APIR or Ticker code found in document content |
| `APIR_NOT_IN_SPREADSHEET` | Code found in document but not in reference CSV Column A |
| `NAME_NO_MATCH` | IO name fallback found no match in CSV |
| `NAME_AMBIGUOUS` | IO name matched multiple CSV entries — cannot determine unique code |
| `DATE_NOT_FOUND` | No date could be extracted from document content or filename |
| `DOCTYPE_UNKNOWN` | Document type could not be determined from headings or content |
| `SCANNED_UNREADABLE` | Image-only PDF — OCR was attempted via Tesseract but yielded no text; manual review required |
| `APIR_O_M` | APIR in original filename is valid but not found in content-extracted codes. An additional rename row is added for the filename APIR; flagged for manual review to verify correct code |
| `DOCTYPE_O_M` | Content-derived type differs from type code in original filename (file renamed using filename type) |
| `DATE_M` | Content-derived date differs from date in original filename, OR date sourced from filename because no content date found |

> ℹ️ `OCR_FALLBACK` is not an exception code — it appears in the `Rename_log_*.csv` reasoning column for files that were successfully processed via Tesseract OCR. These files are renamed normally but carry a −0.5 confidence penalty. OCR is always attempted for image-only PDFs; `SCANNED_UNREADABLE` only occurs when OCR also yields no text.

---

## Confidence Score Reference

| Score | Meaning |
|-------|---------|
| 9.0–10.0 | APIR/Ticker from document content, validated; exact date confirmed |
| 7.0–8.9 | APIR/Ticker from document content, validated; date inferred (month/year only) |
| 5.0–6.9 | Name-fallback used; date confirmed |
| 3.0–4.9 | Name-fallback used; date inferred or partially uncertain |
| < 3.0 | Filename fallback used for APIR and/or date; manual review recommended |

---

*Skill version: run-batch-v3 | Script: run_batch.py (SKILL v5, OCR mandatory) | Reference: Updated IO and APIR table.csv | Last updated: 2026-04-02*
