#!/usr/bin/env python3
"""run_batch.py - Investment Document Classification & Renaming - SKILL v5"""

import subprocess, sys

_REQUIRED_PACKAGES = {
    "fitz": "PyMuPDF",
    "pdfplumber": "pdfplumber",
    "aiohttp": "aiohttp",
    "pytesseract": "pytesseract",
    "pdf2image": "pdf2image",
}

_missing = []
for _mod, _pkg in _REQUIRED_PACKAGES.items():
    try:
        __import__(_mod)
    except ImportError:
        _missing.append(_pkg)

if _missing:
    print(f"Installing missing packages: {', '.join(_missing)}")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet"] + _missing
    )

import fitz  # PyMuPDF — 10-20x faster than pdfplumber for text extraction
import pdfplumber, os, csv, re, time, calendar, zipfile, atexit, base64
import asyncio
import aiohttp
import warnings, logging
try:
    import orjson as json  # faster JSON parsing
except ImportError:
    import json
warnings.filterwarnings("ignore", message=".*FontBBox.*")
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
from datetime import datetime, date as _date_cls
from zoneinfo import ZoneInfo
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# ── Claude API (enabled by default) ──
# Set your Anthropic API key here.
# Claude API is on by default; pass --no-claude as second argument to disable.
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Exception codes that trigger Claude API retry when Claude API is enabled.
CLAUDE_TRIGGER_CODES = {
    "SCANNED_UNREADABLE",
    "APIR_NOT_FOUND",
    "NAME_AMBIGUOUS",
    "DOCTYPE_UNKNOWN",
    "DATE_NOT_FOUND",
    "APIR_Mismatch",
    "DATE_Mismatch",
}

_current_file = None

def _on_exit():
    if _current_file is not None:
        print(f"\n*** Last file being processed: {_current_file} ***", flush=True)

atexit.register(_on_exit)

# ── OCR tool paths ──
# Ensure Tesseract and poppler binaries are on PATH.
_ocr_paths = [
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\Tesseract-OCR"),
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\poppler\poppler-24.08.0\Library\bin"),
]
for _p in _ocr_paths:
    if os.path.isdir(_p) and _p not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _p + os.pathsep + os.environ.get("PATH", "")

# ── OCR dependencies — mandatory ──
# pytesseract and pdf2image are required. The pipeline will not start without them.
try:
    import pytesseract
    from pdf2image import convert_from_path
except ImportError as _ocr_err:
    print(
        f"FATAL: OCR dependencies missing — {_ocr_err}\n"
        "Install required packages: pip install pytesseract pdf2image\n"
        "Tesseract must also be installed and available on PATH."
    )
    sys.exit(1)

run_start = time.time()

# Parse arguments: one or more subfolders, plus optional --no-claude flag
_args = sys.argv[1:]
_no_claude_flag = "--no-claude"
_claude_disabled = _no_claude_flag in _args
subfolders = [a for a in _args if a != _no_claude_flag]
if not subfolders:
    print("Usage: python run_batch.py <subfolder> [<subfolder2> ...] [--no-claude]")
    sys.exit(1)
if _claude_disabled:
    CLAUDE_ENABLED = False
    print("Claude API mode: DISABLED (--no-claude flag passed)")
elif ANTHROPIC_API_KEY.startswith("sk-ant-YOUR"):
    CLAUDE_ENABLED = False
    print("WARNING: Claude API enabled by default but ANTHROPIC_API_KEY is not set. Claude API disabled.")
else:
    CLAUDE_ENABLED = True
    print("Claude API mode: ENABLED (pass --no-claude to disable)")

BASE = Path("C:/Development/ETSL-Renaming")
os.chdir(BASE)

# Expand and validate subfolders: if a subfolder contains subdirectories (not PDFs
# directly), expand it one level into its child subfolders.
_expanded = []
for _sf in subfolders:
    _sf_dir = BASE / "input_pdfs" / _sf
    if not _sf_dir.is_dir():
        print(f"ERROR: subfolder not found: {_sf_dir}")
        sys.exit(1)
    _has_pdfs = any(f.lower().endswith(".pdf") for f in os.listdir(_sf_dir))
    _child_dirs = sorted([d for d in os.listdir(_sf_dir) if (_sf_dir / d).is_dir()])
    if _has_pdfs:
        _expanded.append(_sf)
    elif _child_dirs:
        print(f"Expanding '{_sf}' into {len(_child_dirs)} subfolders: {', '.join(_child_dirs)}")
        _expanded.extend(f"{_sf}/{d}" for d in _child_dirs)
    else:
        print(f"WARNING: '{_sf}' has no PDFs and no subfolders — skipping.")
subfolders = _expanded
if not subfolders:
    print("ERROR: No valid subfolders to process.")
    sys.exit(1)

print(f"Subfolders to process: {', '.join(subfolders)}")

# ── Step 1: Load Reference Data ──
with open("reference_data/Updated IO and APIR table.csv", encoding="utf-8-sig") as f:
    reader = csv.reader(f)
    header = next(reader)
    rows = list(reader)

apir_set = set()
name_index = []
for r in rows:
    code = r[0].strip() if r[0] else ""
    if not code:
        continue
    apir_set.add(code)
    names = [c.strip() for c in r[1:7] if c.strip() and c.strip() != "#N/A"]
    name_index.append({"apir_ticker": code, "names": names})

apir_set = frozenset(apir_set)  # freeze for faster lookups

# Lookup: APIR code → list of IO names (for targeted name verification)
apir_to_names = {}
for entry in name_index:
    apir_to_names[entry["apir_ticker"]] = entry["names"]

print(f"Reference loaded: {len(apir_set)} codes")

apir_pat = re.compile(r"[A-Z]{3}[0-9]{4}AU")
ticker_codes = {"CD1","MOT","CD2","PE1","CD3","MXT","MA1","REV","LEND","GPEQ","QRI","PCX"}

MONTH_MAP = {m: i for i, m in enumerate(
    ["january","february","march","april","may","june",
     "july","august","september","october","november","december"], 1)}

def last_day(y, m):
    return calendar.monthrange(y, m)[1]

# ── Pre-compiled date patterns (Change 3) ──
_DATE_DD_MONTH_YYYY = re.compile(r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})", re.I)
_DATE_DD_MONTH_YYYY_NOSPACE = re.compile(r"(\d{1,2})(January|February|March|April|May|June|July|August|September|October|November|December)(\d{4})", re.I)
_DATE_DD_MM_YYYY    = re.compile(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})")
_DATE_DD_DOT_MM     = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_DATE_ISO           = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_DATE_MONTH_YYYY    = re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})", re.I)
_DATE_YYYY_ONLY     = re.compile(r"\b(20\d{2})\b")

from functools import lru_cache

@lru_cache(maxsize=1024)
def parse_date(text):
    # DD Month YYYY (with spaces)
    m = _DATE_DD_MONTH_YYYY.search(text)
    if m:
        d, mn, y = int(m.group(1)), MONTH_MAP[m.group(2).lower()], int(m.group(3))
        return f"{y}_{mn:02d}_{d:02d}", "exact"
    # DDMonthYYYY (no spaces — common in Equity Trustees PDS text extraction)
    m = _DATE_DD_MONTH_YYYY_NOSPACE.search(text)
    if m:
        d, mn, y = int(m.group(1)), MONTH_MAP[m.group(2).lower()], int(m.group(3))
        return f"{y}_{mn:02d}_{d:02d}", "exact"
    # DD/MM/YYYY or DD-MM-YYYY
    m = _DATE_DD_MM_YYYY.search(text)
    if m:
        d, mn, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mn <= 12 and 1 <= d <= 31:
            return f"{y}_{mn:02d}_{d:02d}", "exact"
    # DD.MM.YYYY
    m = _DATE_DD_DOT_MM.search(text)
    if m:
        d, mn, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mn <= 12 and 1 <= d <= 31:
            return f"{y}_{mn:02d}_{d:02d}", "exact"
    # YYYY-MM-DD
    m = _DATE_ISO.search(text)
    if m:
        y, mn, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mn <= 12 and 1 <= d <= 31:
            return f"{y}_{mn:02d}_{d:02d}", "exact"
    # Month YYYY
    m = _DATE_MONTH_YYYY.search(text)
    if m:
        mn, y = MONTH_MAP[m.group(1).lower()], int(m.group(2))
        return f"{y}_{mn:02d}_{last_day(y, mn):02d}", "month_year_inferred"
    # YYYY only — use 30 June per SKILL v5
    m = _DATE_YYYY_ONLY.search(text)
    if m:
        y = int(m.group(1))
        return f"{y}_06_30", "year_only_inferred"
    return None, None

# ── Document Type Keywords (SKILL v5 — expanded) ──
# Ordered longest/most-specific first within each type to minimise false partial matches.
# Searching is done against the cover page zone first (COVER_CHARS), then full text.
# Keywords that ONLY match in the cover zone (first COVER_CHARS characters).
# Real PDS/TMD documents always carry these as prominent cover headings;
# body-only mentions are boilerplate references ("refer to the PDS").
COVER_ONLY_KEYWORDS = [
    ("product disclosure statement",        "PDSX"),
    ("target market determination",         "TMDX"),
]

# Keywords that match in cover zone first, then full text.
DOCTYPE_KEYWORDS = [
    # Annual / financial reports — check before shorter "report" variants
    ("annual financial report",             "ARPT"),
    ("interim financial report",            "ARPT"),
    ("directors' report",                   "ARPT"),
    ("directors report",                    "ARPT"),
    ("financial statements",                "ARPT"),
    ("annual report",                       "ARPT"),
    ("interim report",                      "ARPT"),
    # Performance documents — most specific first
    ("annual information performance",      "PERF"),
    ("performance report",                  "PERF"),
    ("investment option profile",           "PERF"),
    ("monthly performance",                 "PERF"),
    ("monthly report",                      "PERF"),
    ("monthly update",                      "PERF"),
    ("quarterly report",                    "PERF"),
    ("quarterly update",                    "PERF"),
    ("quarterly review",                    "PERF"),
    ("performance as at",                   "PERF"),
    ("fund description",                    "PERF"),
    ("fund profile",                        "PERF"),
    ("fund report",                         "PERF"),
    ("fund update",                         "PERF"),
    ("fund facts",                          "PERF"),
    ("key facts",                           "PERF"),
    ("portfolio update",                    "PERF"),
    ("investment update",                   "PERF"),
    ("manager commentary",                  "PERF"),
    ("market update",                       "PERF"),
    ("factsheet",                           "PERF"),
    ("fact sheet",                          "PERF"),
    ("snapshot",                            "PERF"),
]

# Number of characters from the start of extracted text treated as the "cover page zone".
# Classification signal priority: cover zone → full text.
COVER_CHARS = 2000

KNOWN_TYPECODES = {"PDSX", "TMDX", "PERF", "ARPT"}
_ALL_COVER_KEYWORDS = COVER_ONLY_KEYWORDS + DOCTYPE_KEYWORDS  # pre-computed


# ── Pre-compiled normalise_name patterns (Change 1) ──
_NORM_LABELS  = re.compile(r"\b(class [a-z]|wholesale|retail|hedged|unhedged|units?|fund)\b")
_NORM_NONALNUM = re.compile(r"[^a-z0-9 ]")
_NORM_SPACES   = re.compile(r"\s+")

def normalise_name(n, strip_labels=False):
    """Lowercase and strip punctuation for exact substring matching.
    strip_labels=True also removes class/fund/unit labels (used for ARPT relaxed matching)."""
    n = n.lower()
    if strip_labels:
        n = _NORM_LABELS.sub("", n)
    n = _NORM_NONALNUM.sub(" ", n)
    return _NORM_SPACES.sub(" ", n).strip()


def tokenise_name(n):
    """Extract meaningful tokens from an IO name for token-based matching."""
    norm = normalise_name(n)
    # Filter out very short tokens (1-2 chars) and common noise words
    noise = {"the", "and", "for", "ltd", "pty", "inc", "llc", "abn", "afsl",
             "arsn", "acn", "no", "aus", "aust", "idx", "hdg", "crdt", "secs",
             "intl", "inv", "mgmt", "mgt", "cap", "eq", "fxd", "inc"}
    tokens = [t for t in norm.split() if len(t) >= 3 and t not in noise]
    return tokens


# Precompute token sets and normalised forms for each reference name (Change 2)
_name_token_cache = {}
_name_norm_cache = {}
for entry in name_index:
    for name in entry["names"]:
        if name not in _name_token_cache:
            _name_token_cache[name] = set(tokenise_name(name))
        if name not in _name_norm_cache:
            _name_norm_cache[name] = normalise_name(name)


def token_match_score(ref_tokens, text_tokens):
    """
    Fraction of reference name tokens found in the document text tokens.
    Returns a float 0.0–1.0.
    """
    if not ref_tokens:
        return 0.0
    hits = sum(1 for t in ref_tokens if t in text_tokens)
    return hits / len(ref_tokens)


# Minimum fraction of reference tokens that must appear in the document
TOKEN_MATCH_THRESHOLD = 0.6


def verify_apir_by_name(apir_code, text, norm_text=None):
    """
    Targeted name verification — given a specific APIR code, look up its IO
    names in the reference CSV and check if any appear in the document text.

    Uses two strategies:
    1. Exact substring match (original method)
    2. Token-based match — checks if enough key tokens from the reference name
       appear anywhere in the document text. Handles abbreviations, reordering,
       and platform prefixes.

    Returns (True, matched_name) if found, (False, None) otherwise.
    """
    names = apir_to_names.get(apir_code, [])
    if not names:
        return False, None

    if norm_text is None:
        norm_text = normalise_name(text)

    # Strategy 1: exact substring match
    for name in names:
        norm_name = _name_norm_cache.get(name) or normalise_name(name)
        if len(norm_name) < 5:
            continue
        if norm_name in norm_text:
            return True, name

    # Strategy 2: token-based match
    text_tokens = set(norm_text.split())
    best_score = 0.0
    best_name = None
    for name in names:
        ref_tokens = _name_token_cache.get(name, set())
        if len(ref_tokens) < 2:
            continue
        score = token_match_score(ref_tokens, text_tokens)
        if score > best_score:
            best_score = score
            best_name = name

    if best_score >= TOKEN_MATCH_THRESHOLD and best_name:
        return True, best_name

    return False, None



def extract_primary_codes(full_text):
    """
    Method 1 — APIR/Ticker extraction from full document text.

    Finds all APIR codes matching [A-Z]{3}[0-9]{4}AU and all known ticker codes,
    then validates each against the reference CSV.

    Returns (validated_codes_list, detail_str).
    """
    found = set(apir_pat.findall(full_text))
    for tc in ticker_codes:
        if re.search(r"\b" + re.escape(tc) + r"\b", full_text):
            found.add(tc)

    validated = sorted([c for c in found if c in apir_set])
    not_validated = sorted([c for c in found if c not in apir_set])

    detail_parts = []
    if not_validated:
        detail_parts.append(f"Codes found but not in reference CSV: {not_validated}")
    detail = " | ".join(detail_parts) if detail_parts else ""
    return validated, detail


def name_fallback(text, name_index, norm_text=None):
    """
    Method 2 — Investment Option Name Matching (fallback).

    Scan document text for investment option names and match against
    all IO name columns (B–F) of the reference CSV.

    Returns (matched_codes, detail_string, exception_reason_or_None).
    Multiple distinct codes matched → NAME_AMBIGUOUS (not just NAME_NO_MATCH).
    Single code matched → success.
    """
    if norm_text is None:
        norm_text = normalise_name(text)
    matches = {}  # apir_ticker -> first name matched
    for entry in name_index:
        for name in entry["names"]:
            if not name:
                continue
            norm_name = _name_norm_cache.get(name) or normalise_name(name)
            if len(norm_name) < 5:
                continue  # skip very short names — too ambiguous
            if norm_name in norm_text:
                if entry["apir_ticker"] not in matches:
                    matches[entry["apir_ticker"]] = name

    if len(matches) == 0:
        return [], "Name fallback: no IO name matched in document.", "APIR_NOT_FOUND"

    if len(matches) > 1:
        # Multiple distinct codes — ambiguous
        return (
            [],
            f"Name fallback: matched multiple codes {sorted(matches.keys())} — ambiguous.",
            "NAME_AMBIGUOUS",
        )

    code, name = next(iter(matches.items()))
    return (
        [code],
        f"Name fallback: matched IO name '{name}' -> code {code}.",
        None,
    )


def name_fallback_multi(text, name_index, norm_text=None, relaxed=False):
    """
    Multi-match name fallback.

    Returns ALL matched codes rather than treating multiple matches as ambiguous.
    Uses exact substring matching only — tries all name variants per entry,
    preferring the longest (most complete) name first so that shortened CSV
    entries still match when the document contains the full name.

    relaxed=True strips labels (fund, class, wholesale, etc.) for ARPT documents
    where IO names in the document may not exactly match the reference CSV format.

    Returns (matched_codes_list, detail_string, exception_reason_or_None).
    No match → APIR_NOT_FOUND.
    One or more matches → success (caller creates one output row per code).
    """
    if norm_text is None:
        norm_text = normalise_name(text, strip_labels=relaxed)
    matches = {}  # apir_ticker -> first name matched

    for entry in name_index:
        if entry["apir_ticker"] in matches:
            continue
        # Try longest name first — most specific, least likely to false-positive
        sorted_names = sorted(
            [n for n in entry["names"] if n],
            key=len, reverse=True,
        )
        for name in sorted_names:
            if relaxed:
                norm_name = normalise_name(name, strip_labels=True)
            else:
                norm_name = _name_norm_cache.get(name) or normalise_name(name)
            if len(norm_name) < 5:
                continue
            if norm_name in norm_text:
                matches[entry["apir_ticker"]] = name
                break

    if len(matches) == 0:
        return [], "Name fallback: no IO name matched in document.", "APIR_NOT_FOUND"

    codes = sorted(matches.keys())
    names_matched = [matches[c] for c in codes]
    return (
        codes,
        f"ARPT name fallback: matched IO name(s) {names_matched} -> code(s) {codes}.",
        None,
    )


def classify_doctype_from_cover(text, text_lower=None):
    """
    Classification Step 1 — cover page zone first, then full text.

    Two-tier keyword system:
    - COVER_ONLY_KEYWORDS (PDSX, TMDX): only matched in the cover zone.
      Body mentions are boilerplate references, not document-type signals.
    - DOCTYPE_KEYWORDS: matched in cover zone first, then full text.

    Returns (typecode_or_None, signal_zone) where signal_zone is 'cover' or 'body'.
    """
    if text_lower is None:
        text_lower = text.lower()
    cover = text_lower[:COVER_CHARS]

    # Pass 1: cover zone — check both cover-only and general keywords
    best_pos  = len(cover) + 1
    best_code = None
    for kw, code in _ALL_COVER_KEYWORDS:
        pos = cover.find(kw)
        if 0 <= pos < best_pos:
            best_pos  = pos
            best_code = code
    if best_code:
        return best_code, "cover"

    # Pass 2: full text — general keywords only (NOT cover-only keywords)
    full = text_lower
    best_pos  = len(full) + 1
    best_code = None
    for kw, code in DOCTYPE_KEYWORDS:
        pos = full.find(kw)
        if 0 <= pos < best_pos:
            best_pos  = pos
            best_code = code
    if best_code:
        return best_code, "body"

    return None, None


def classify_doctype_content_fallback(text):
    """
    SKILL v5 content-based classification fallback — used only when no keyword match
    was found in cover or body.

    Stage 1: performance table signal (fund-specific data + period returns) → PERF
    Stage 2: audited financial statements signal → ARPT
    Otherwise: None (caller will route to DOCTYPE_UNKNOWN exception)

    Returns (typecode_or_None, detail_string).
    """
    norm = text.lower()

    # Stage 1 — performance table: period return headers + fund-specific data
    period_signals   = ["1 month", "3 month", "1 year", "since inception",
                        "3 year", "5 year", "ytd", "p.a."]
    fund_data_signals = ["apir", "fund size", "portfolio", "nav", "unit price",
                         "isin", "mer", "management fee"]

    period_hits    = [s for s in period_signals    if s in norm]
    fund_data_hits = [s for s in fund_data_signals if s in norm]

    if period_hits and fund_data_hits:
        return "PERF", (
            f"Content fallback Stage 1: performance table signals {period_hits} "
            f"+ fund data signals {fund_data_hits} -> PERF."
        )

    # Stage 2 — audited financial statements
    audit_signals = ["balance sheet", "income statement",
                     "notes to the financial statements",
                     "notes to financial statements",
                     "statement of financial position",
                     "auditor's report", "auditors report"]
    audit_hits = [s for s in audit_signals if s in norm]
    if audit_hits:
        return "ARPT", (
            f"Content fallback Stage 2: audited financial statement signals {audit_hits} -> ARPT."
        )

    return None, "Content fallback: no performance table or financial statement signals found."


# ── Filename parsing helpers ──
# The expected naming pattern is: {APIR}_{TYPECODE}_{YYYY}_{MM}_{DD}.{ext}
# These helpers extract each field from the original filename for use as
# last-resort fallbacks when content extraction fails.

def parse_apir_from_filename(fname):
    """
    Extract the first APIR/Ticker code present in the filename.
    Returns the code string or None.
    APIR pattern: [A-Z]{3}[0-9]{4}AU
    Ticker: the part before the first underscore, checked against reference data.
    """
    m = apir_pat.search(fname)
    if m:
        code = m.group(0)
        return code if code in apir_set else None
    # Ticker fallback: extract the part before the first underscore.
    # Return it even if not in apir_set so downstream can classify as APIR_NOT_IN_SPREADSHEET.
    stem = fname.rsplit(".", 1)[0] if "." in fname else fname
    prefix = stem.split("_")[0].strip()
    if prefix and prefix.isalnum() and len(prefix) <= 6 and not apir_pat.match(prefix):
        return prefix
    return None


def parse_typecode_from_filename(fname):
    """
    Extract the first known typecode present in the filename (case-insensitive).
    Returns the typecode string or None.
    """
    for tc in KNOWN_TYPECODES:
        if tc in fname.upper():
            return tc
    return None


# ── Pre-compiled PDS application form heading pattern (Change 4) ──
_APPLICATION_FORM_RE = re.compile(r"This\s+Application\s+Form\s+accompanies", re.I)
# Match "Statement(s) of Comprehensive Income" only as a page heading —
# not in table of contents (has dots/page numbers) or inline body references.
_ARPT_STOP_RE = re.compile(r"^Statements?\s+of\s+Comprehensive\s+Income\s*$", re.I | re.M)

# ── Pre-compiled filename date patterns (Change 5) ──
_FNAME_DATE_UNDERSCORE = re.compile(r"(20\d{2})_(\d{2})_(\d{1,2})")
_FNAME_DATE_COMPACT    = re.compile(r"(20\d{2})(\d{2})(\d{2})")

def parse_date_from_filename(fname):
    """
    Extract a date from the filename using the pattern YYYY_MM_DD or YYYYMMDD.
    Returns (date_string, precision) or (None, None).
    """
    # YYYY_MM_DD or YYYY_MM_D
    m = _FNAME_DATE_UNDERSCORE.search(fname)
    if m:
        y, mn, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mn <= 12 and 1 <= d <= 31:
            return f"{y}_{mn:02d}_{d:02d}", "exact"
    # YYYYMMDD
    m = _FNAME_DATE_COMPACT.search(fname)
    if m:
        y, mn, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mn <= 12 and 1 <= d <= 31:
            return f"{y}_{mn:02d}_{d:02d}", "exact"
    return None, None


async def _call_claude_api(session, prompt, model, pdf_b64=None, doc_text=None):
    """Send a single Claude API request via aiohttp. Returns parsed JSON or None.
    Provide either pdf_b64 (full PDF) or doc_text (extracted text)."""
    if pdf_b64:
        content = [
            {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": pdf_b64,
                },
            },
            {"type": "text", "text": prompt},
        ]
    elif doc_text:
        content = [
            {"type": "text", "text": f"Document text:\n\n{doc_text}\n\n---\n\n{prompt}"},
        ]
    else:
        return None

    payload = {
        "model": model,
        "max_tokens": 100,
        "temperature": 0,
        "system": [
            {
                "type": "text",
                "text": (
                    "You extract metadata from Australian investment documents. "
                    "Return ONLY a JSON object with keys: apir, doctype, date. No explanation.\n"
                    "apir: APIR code in XXX0000AU format (3 letters + 4 digits + AU). null if not visible.\n"
                    "doctype: exactly one of these codes:\n"
                    "  PDSX = Product Disclosure Statement\n"
                    "  TMDX = Target Market Determination\n"
                    "  PERF = Performance report, factsheet, fund profile, monthly/quarterly update\n"
                    "  ARPT = Annual/interim financial report, directors report, financial statements\n"
                    "  Use null if the document does not clearly match any of the above.\n"
                    "date: primary document date in YYYY_MM_DD format. "
                    "Use the report date, issue date, or 'as at' date — not a printing date. null if not visible."
                ),
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "messages": [{"role": "user", "content": content}],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "prompt-caching-2024-07-31",
    }

    sem = _HAIKU_SEMAPHORE if model == _CLAUDE_MODEL_FAST else _SONNET_SEMAPHORE
    MAX_RETRIES = 5
    _RETRY_STATUSES = {429, 500, 502, 503, 529}
    for attempt in range(MAX_RETRIES):
        try:
            async with sem:
                async with session.post(
                    "https://api.anthropic.com/v1/messages",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    if resp.status == 400:
                        # Bad request — payload issue (e.g. invalid PDF), don't retry
                        _body = await resp.text()
                        _err_msg = _body[:200] if _body else "no body"
                        if pdf_b64 and "PDF" in _err_msg:
                            # Signal invalid PDF so caller can retry with text
                            return {"_invalid_pdf": True}
                        print(f"    Claude API error ({model}): HTTP 400 — {_err_msg}")
                        return None
                    if resp.status in _RETRY_STATUSES:
                        if attempt < MAX_RETRIES - 1:
                            delay = float(resp.headers.get("retry-after", 0))
                            if not delay:
                                delay = min(2 ** (attempt + 1), 30)  # 2s, 4s, 8s, 16s
                            await asyncio.sleep(delay)
                            continue
                        print(f"    Claude API error ({model}): HTTP {resp.status} after {MAX_RETRIES} attempts")
                        return None
                    resp.raise_for_status()
                    response_body = await resp.json()
            break  # success
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            _status = getattr(e, 'status', None)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(min(2 ** (attempt + 1), 30))
                continue
            print(f"    Claude API error ({model}): {type(e).__name__} (status={_status}) after {MAX_RETRIES} attempts")
            return None
        except Exception as e:
            print(f"    Claude API error ({model}): {type(e).__name__}: {e}")
            return None

    try:
        raw_text = ""
        for block in response_body.get("content", []):
            if block.get("type") == "text":
                raw_text += block["text"]
        clean = re.sub(r"```[a-z]*", "", raw_text).replace("```", "").strip()
        return json.loads(clean)
    except Exception as e:
        print(f"    Claude API response parse error ({model}): {e} | raw: {raw_text[:200]}")
        return None


_CLAUDE_MODEL_FAST = "claude-haiku-4-5-20251001"
_CLAUDE_MODEL_FULL = "claude-sonnet-4-20250514"


async def classify_with_claude(session, pdf_path, known_apir=None, known_type=None, known_date=None,
                               full_text=None, fname=None, pages=None):
    """
    Claude API fallback — tiered approach: try Haiku first (fast),
    escalate to Sonnet only if Haiku returns incomplete results.

    Hybrid input: sends extracted text for speed when available,
    but always sends the full PDF for PERF documents (image-heavy)
    or when text is sparse (<200 chars).

    Parameters
    ----------
    pdf_path    : str  — full path to the PDF file
    known_apir  : str or None  — already-confirmed APIR (won't be overwritten)
    known_type  : str or None  — already-confirmed doctype (won't be overwritten)
    known_date  : str or None  — already-confirmed date (won't be overwritten)
    full_text   : str or None  — pre-extracted document text
    fname       : str or None  — original filename (used to detect PERF)

    Returns
    -------
    dict with keys 'apir', 'doctype', 'date' — any field that could not be
    determined is set to None.
    On total failure, returns None.
    """
    # ── Decide input mode: PDF binary vs extracted text ──
    is_perf = fname and "PERF" in fname.upper()
    has_text = full_text and len(full_text.strip()) >= 200
    use_pdf = is_perf or not has_text

    pdf_b64 = None
    if use_pdf:
        try:
            with open(pdf_path, "rb") as f:
                pdf_b64 = base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            print(f"    PDF read error: {e}")
            return None
        if not pdf_b64:
            return None

    # ── Build a compact prompt — only ask for missing fields ──
    fields = []
    if not known_apir:
        fields.append('"apir": APIR code (XXX0000AU) or ticker. null if not visible')
    if not known_type:
        fields.append(f'"doctype": one of {sorted(KNOWN_TYPECODES)}. null if unclear')
    if not known_date:
        fields.append('"date": YYYY_MM_DD (report/issue date, not print date). null if not visible')

    if not fields:
        return {"apir": known_apir, "doctype": known_type, "date": known_date}

    prompt = "Extract: " + ", ".join(fields) + ". JSON only."

    # Add hint with expected APIR and official name from filename
    if not known_apir and fname:
        fn_apir_hint = parse_apir_from_filename(fname)
        if fn_apir_hint and fn_apir_hint in apir_set:
            official_names = apir_to_names.get(fn_apir_hint, [])
            official = official_names[0] if official_names else None
            if official:
                prompt += (f'\nHint: expected APIR {fn_apir_hint}, '
                          f'official name "{official}". '
                          f'If you find this name in the document, return this APIR.')

    # ── Tier 1: Haiku with chunked text (fast) ──
    # Send 3000-char chunks until all fields resolved or text exhausted.
    # For PDF mode (PERF), send the full PDF in one call.
    PAGES_PER_CHUNK = 2
    validated = None

    if use_pdf:
        result = await _call_claude_api(session, prompt, _CLAUDE_MODEL_FAST, pdf_b64=pdf_b64)
        # If PDF was rejected by the API, fall back to text mode
        if result and result.get("_invalid_pdf"):
            print(f"    PDF rejected by API — falling back to text for {fname or 'unknown'}")
            use_pdf = False
            pdf_b64 = None
            result = None
        else:
            validated = _validate_claude_result(result, known_apir, known_type, known_date)
    if not use_pdf and pages and len(pages) > 0:
        # Send pages in chunks of PAGES_PER_CHUNK — preserves page boundaries
        for i in range(0, len(pages), PAGES_PER_CHUNK):
            chunk = "\n\n".join(pages[i:i + PAGES_PER_CHUNK])
            if not chunk.strip():
                continue
            result = await _call_claude_api(session, prompt, _CLAUDE_MODEL_FAST, doc_text=chunk)
            chunk_validated = _validate_claude_result(result, known_apir, known_type, known_date)
            if chunk_validated:
                if validated:
                    for k in ("apir", "doctype", "date"):
                        if not validated.get(k) and chunk_validated.get(k):
                            validated[k] = chunk_validated[k]
                else:
                    validated = chunk_validated
            # Stop if all needed fields are resolved
            all_resolved = validated and (
                (known_apir or validated.get("apir")) and
                (known_type or validated.get("doctype")) and
                (known_date or validated.get("date"))
            )
            if all_resolved:
                break
    if not use_pdf and not (pages and len(pages) > 0) and full_text:
        # Fallback: no pages available, send full text
        result = await _call_claude_api(session, prompt, _CLAUDE_MODEL_FAST, doc_text=full_text[:3000])
        validated = _validate_claude_result(result, known_apir, known_type, known_date)

    # ── Tier 2: Sonnet escalation — only if Haiku chunks couldn't resolve ──
    needs_escalation = validated is None or (
        (not known_apir and not validated.get("apir")) or
        (not known_type and not validated.get("doctype")) or
        (not known_date and not validated.get("date"))
    )

    if needs_escalation:
        if use_pdf and not pdf_b64:
            try:
                with open(pdf_path, "rb") as f:
                    pdf_b64 = base64.b64encode(f.read()).decode("utf-8")
            except Exception:
                pdf_b64 = None
        escalation_kwargs = {"pdf_b64": pdf_b64} if (use_pdf and pdf_b64) else {"doc_text": full_text}
        result2 = await _call_claude_api(session, prompt, _CLAUDE_MODEL_FULL, **escalation_kwargs)
        # If PDF was rejected, retry Sonnet with text
        if result2 and result2.get("_invalid_pdf") and full_text:
            use_pdf = False
            pdf_b64 = None
            result2 = await _call_claude_api(session, prompt, _CLAUDE_MODEL_FULL, doc_text=full_text)
        validated2 = _validate_claude_result(result2, known_apir, known_type, known_date)
        if validated2:
            if validated:
                for k in ("apir", "doctype", "date"):
                    if not validated.get(k) and validated2.get(k):
                        validated[k] = validated2[k]
            else:
                validated = validated2

    return validated


def _validate_claude_result(result, known_apir, known_type, known_date):
    """Validate and clean raw Claude API JSON result. Returns dict or None."""
    if not result:
        return None

    apir_raw = result.get("apir")
    doctype_raw = result.get("doctype")
    date_raw = result.get("date")

    # Validate APIR from Claude against reference set
    final_apir = known_apir
    if not known_apir and apir_raw:
        apir_clean = apir_raw.strip()
        if apir_clean in apir_set:
            final_apir = apir_clean
        else:
            if apir_clean in ticker_codes and apir_clean in apir_set:
                final_apir = apir_clean
            else:
                print(f"    Claude API returned APIR '{apir_clean}' — not in reference CSV, discarding.")
                final_apir = None

    # Validate doctype from Claude
    final_type = known_type
    if not known_type and doctype_raw:
        dt_clean = doctype_raw.strip().upper()
        if dt_clean in KNOWN_TYPECODES:
            final_type = dt_clean
        else:
            print(f"    Claude API returned doctype '{doctype_raw}' — not a known typecode, discarding.")
            final_type = None

    # Validate date from Claude — must match YYYY_MM_DD pattern
    final_date = known_date
    if not known_date and date_raw:
        date_clean = date_raw.strip()
        if re.fullmatch(r"20\d{2}_\d{2}_\d{2}", date_clean):
            final_date = date_clean
        else:
            # Try to parse it through the existing date parser as a fallback
            parsed, _ = parse_date(date_clean)
            if parsed:
                final_date = parsed
            else:
                print(f"    Claude API returned date '{date_raw}' — unrecognised format, discarding.")
                final_date = None

    return {"apir": final_apir, "doctype": final_type, "date": final_date}


# Concurrency settings
EXECUTOR_WORKERS = 200            # CPU-bound pool for pdfplumber/OCR
EXTRACT_CONCURRENCY = 50          # max PDFs open simultaneously (RAM limiter)
PIPELINE_CONCURRENCY = 2000       # max files in-flight simultaneously
CLAUDE_HAIKU_CONCURRENCY = 260    # max concurrent Haiku API calls
CLAUDE_SONNET_CONCURRENCY = 60    # max concurrent Sonnet API calls
_HAIKU_SEMAPHORE = asyncio.Semaphore(CLAUDE_HAIKU_CONCURRENCY)
_SONNET_SEMAPHORE = asyncio.Semaphore(CLAUDE_SONNET_CONCURRENCY)

PROMPT_TEXT = (
    "Investment Document Classification & Renaming Engine — SKILL v5. "
    "Classify document type from cover page heading hierarchy first, then body. "
    "Extract APIR/Ticker codes from document primary content only (never filename, "
    "never disclaimer/ratings boilerplate sections). "
    "Validate all codes against Updated_IO_and_APIR_table.csv Column A. "
    "If no codes found via Method 1, apply name fallback (Method 2) against columns B–F. "
    "ARPT documents use multi-match name fallback. "
    "Standardise date to YYYY_MM_DD; year-only falls back to YYYY_06_30. "
    "Rename pattern: [Code]_[TypeCode]_[Date].[ext]."
)

def print_progress(current, total):
    bar_len = 40
    filled = int(bar_len * current / total) if total else 0
    bar = "#" * filled + "-" * (bar_len - filled)
    pct = (current / total * 100) if total else 0
    elapsed = time.time() - subfolder_start
    avg = elapsed / current if current else 0
    eta = avg * (total - current)
    eta_str = f"{int(eta // 60)}m{int(eta % 60):02d}s" if eta >= 60 else f"{int(eta)}s"
    sys.stdout.write(f"\r  Progress: |{bar}| {current}/{total} ({pct:.1f}%) ETA {eta_str}   ")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write("\n")


async def process_single_file(session, fname):
    """Process one PDF and return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)."""
    file_rename_rows = []
    file_exception_rows = []
    file_log_rows = []
    file_stats = {k: 0 for k in stats}
    output_lines = []

    file_stats["processed"] = 1
    fn_apir_cached = parse_apir_from_filename(fname)
    path = os.path.join(pdf_dir, fname)

    # ── Early bail: filename has an APIR/ticker not in reference data ──
    if fn_apir_cached and fn_apir_cached not in apir_set:
        file_stats["exceptions"] += 1
        now_str = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%d %H:%M:%S AEDT")
        file_exception_rows.append({"original": fname, "fields": "APIR_NOT_IN_SPREADSHEET"})
        file_log_rows.append({
            "input": fname, "renamed": "No", "confidence": 0.0,
            "datetime": now_str, "prompt": PROMPT_TEXT,
            "files": "", "reasoning": f"Filename APIR/ticker '{fn_apir_cached}' not in reference CSV. Skipped.",
        })
        output_lines.append(f"  {fname} -> EXCEPTION (APIR_NOT_IN_SPREADSHEET) — not in reference CSV")
        return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

    # ── 2a: Use pre-extracted text from cache ──
    full_text, ocr_used_preextract, apir_bailed = _text_cache.get(fname, ("", False, False))

    if ocr_used_preextract and full_text.strip():
        file_stats["ocr_fallback"] += 1

    if not full_text.strip():
            # If bailed during extraction (APIR not in reference, no valid APIRs found)
            # or filename APIR is not in reference — skip entirely
            if apir_bailed or (fn_apir_cached and fn_apir_cached not in apir_set):
                file_stats["exceptions"] += 1
                now_str = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%d %H:%M:%S AEDT")
                _fn_apir_raw = apir_pat.search(fname)
                _fn_apir_label = _fn_apir_raw.group() if _fn_apir_raw else fn_apir_cached
                file_exception_rows.append({"original": fname, "fields": "APIR_NOT_IN_SPREADSHEET"})
                file_log_rows.append({
                    "input": fname, "renamed": "No", "confidence": 0.0,
                    "datetime": now_str, "prompt": PROMPT_TEXT,
                    "files": "", "reasoning": f"Filename APIR '{_fn_apir_label}' not in reference CSV. Skipped.",
                })
                output_lines.append(f"  {fname} -> EXCEPTION (APIR_NOT_IN_SPREADSHEET) — not in reference CSV")
                return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

            file_stats["scanned_unreadable"] += 1
            file_stats["exceptions"] += 1
            now_str = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%d %H:%M:%S AEDT")

            if CLAUDE_ENABLED:
                output_lines.append(f"  {fname} -> SCANNED_UNREADABLE — attempting Claude API fallback...")
                claude_result = await classify_with_claude(session, path, fname=fname,
                    pages=_pages_cache.get(fname, []))
                if claude_result and any(claude_result.values()):
                    v_apir   = claude_result.get("apir")
                    v_type   = claude_result.get("doctype")
                    v_date   = claude_result.get("date")
                    # Fill missing APIR from filename if available and in reference
                    if not v_apir and fn_apir_cached and fn_apir_cached in apir_set:
                        v_apir = fn_apir_cached
                    # Fill missing doctype from filename if available
                    if not v_type:
                        fn_type_scan = parse_typecode_from_filename(fname)
                        if fn_type_scan:
                            v_type = fn_type_scan
                    missing  = [k for k, v in {"apir": v_apir, "doctype": v_type, "date": v_date}.items() if not v]
                    if missing:
                        file_stats["claude_api_failed"] += 1
                        file_exception_rows.append({"original": fname, "fields": "CLAUDE_API_FAILED"})
                        file_log_rows.append({
                            "input": fname, "renamed": "No", "confidence": 0.0,
                            "datetime": now_str, "prompt": PROMPT_TEXT, "files": "",
                            "reasoning": (
                                f"SCANNED_UNREADABLE — OCR yielded no text. "
                                f"Claude API attempted but could not determine: {missing}. "
                                f"Partial result: apir={v_apir}, doctype={v_type}, date={v_date}."
                            ),
                        })
                        output_lines.append(f"  {fname} -> EXCEPTION (CLAUDE_API_FAILED — missing: {missing})")
                        return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)
                    if v_apir not in apir_set:
                        file_stats["claude_api_failed"] += 1
                        file_exception_rows.append({"original": fname, "fields": "CLAUDE_API_FAILED"})
                        file_log_rows.append({
                            "input": fname, "renamed": "No", "confidence": 0.0,
                            "datetime": now_str, "prompt": PROMPT_TEXT, "files": "",
                            "reasoning": (
                                f"SCANNED_UNREADABLE — OCR yielded no text. "
                                f"Claude API returned APIR '{v_apir}' which is not in reference CSV."
                            ),
                        })
                        output_lines.append(f"  {fname} -> EXCEPTION (CLAUDE_API_FAILED — APIR '{v_apir}' not in CSV)")
                        return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)
                    file_stats["claude_api"] += 1
                    ext = os.path.splitext(fname)[1].lower()
                    renamed = f"{v_apir}_{v_type}_{v_date}{ext}"
                    file_rename_rows.append({
                        "original": fname, "renamed": renamed, "code": v_apir,
                        "type": v_type, "date": v_date, "confidence": 5.0,
                    })
                    file_stats["renamed"] += 1
                    file_log_rows.append({
                        "input": fname, "renamed": "Yes", "confidence": 5.0,
                        "datetime": now_str, "prompt": PROMPT_TEXT, "files": "",
                        "reasoning": (
                            f"SCANNED_UNREADABLE — OCR yielded no text. "
                            f"Claude API fallback succeeded: apir={v_apir}, "
                            f"doctype={v_type}, date={v_date}."
                        ),
                    })
                    output_lines.append(f"  {fname} -> {renamed} (5.0) [CLAUDE_API]")
                    return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)
                else:
                    file_stats["claude_api_failed"] += 1
                    file_exception_rows.append({"original": fname, "fields": "CLAUDE_API_FAILED"})
                    file_log_rows.append({
                        "input": fname, "renamed": "No", "confidence": 0.0,
                        "datetime": now_str, "prompt": PROMPT_TEXT, "files": "",
                        "reasoning": (
                            "SCANNED_UNREADABLE — OCR yielded no text. "
                            "Claude API fallback returned no usable result."
                        ),
                    })
                    output_lines.append(f"  {fname} -> EXCEPTION (CLAUDE_API_FAILED)")
                    return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

            file_exception_rows.append({"original": fname, "fields": "SCANNED_UNREADABLE"})
            file_log_rows.append({
                "input": fname, "renamed": "No", "confidence": 0.0,
                "datetime": now_str, "prompt": PROMPT_TEXT, "files": "",
                "reasoning": (
                    "Document is image-only (no extractable text across all pages). "
                    "OCR fallback applied but yielded no text — manual review required."
                ),
            })
            output_lines.append(f"  {fname} -> EXCEPTION (SCANNED_UNREADABLE — OCR yielded no text)")
            return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

    ocr_used = ocr_used_preextract

    first_3000 = full_text[:3000]
    full_text_lower = full_text.lower()       # pre-lowercase once for all lookups
    norm_text = normalise_name(full_text)      # pre-normalise once for name matching

    # ── 2a-ii: ARPT fast-path — if ARPT in filename and APIR in reference,
    # check if the official name appears in the text. If so, confirm immediately. ──
    is_arpt_filename = "ARPT" in fname.upper()
    if is_arpt_filename and fn_apir_cached and fn_apir_cached in apir_set:
        found, matched_name = verify_apir_by_name(fn_apir_cached, full_text, norm_text=norm_text)
        if found:
            # APIR confirmed — extract date and return
            verified_date, date_conf = parse_date(first_3000)
            if not verified_date:
                verified_date, date_conf = parse_date(full_text)
            if verified_date:
                now_str = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%d %H:%M:%S AEDT")
                ext = os.path.splitext(fname)[1].lower()
                renamed = f"{fn_apir_cached}_ARPT_{verified_date}{ext}"
                file_rename_rows.append({
                    "original": fname, "renamed": renamed, "code": fn_apir_cached,
                    "type": "ARPT", "date": verified_date, "confidence": 9.5,
                })
                file_stats["renamed"] += 1
                file_stats["fast_verified"] += 1
                file_log_rows.append({
                    "input": fname, "renamed": "Yes", "confidence": 9.5,
                    "datetime": now_str, "prompt": PROMPT_TEXT, "files": "",
                    "reasoning": (
                        f"ARPT fast-path: filename APIR '{fn_apir_cached}' confirmed via "
                        f"official name '{matched_name}' in document. date={verified_date}."
                    ),
                })
                output_lines.append(f"  {fname} -> {renamed} (9.5)")
                return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

    # ── 2b: Document type classification — cover page first ──
    verified_type, type_signal_zone = classify_doctype_from_cover(full_text, full_text_lower)
    doctype_fallback_method = None

    # ── 2c: APIR / Ticker extraction (Method 1) ──
    validated_codes, disclaimer_detail = extract_primary_codes(full_text)
    content_extracted_codes = set(validated_codes)  # snapshot for APIR_Mismatch check later

    # ── 2c-ii: Multiple-APIR dedup for PDS application forms ──
    # PDS documents often have an application section listing many APIRs.
    # When multiple codes are found, prefer the cover-page APIR or filename APIR.
    if len(validated_codes) > 1:
        cover_text = full_text[:COVER_CHARS]
        cover_codes = sorted([c for c in validated_codes if c in cover_text])
        fn_apir_dedup = fn_apir_cached
        if fn_apir_dedup and fn_apir_dedup in validated_codes:
            validated_codes = [fn_apir_dedup]
        elif len(cover_codes) == 1:
            validated_codes = cover_codes

    # ── 2d: Extract filename typecode for cross-check ──
    fn_type = None
    for tc in KNOWN_TYPECODES:
        if tc in fname.upper():
            fn_type = tc
            break

    # ── 2e: Date extraction ──
    verified_date, date_conf = None, None
    date_context = re.search(
        r"(?:TMD\s+issue\s+date|Date\s+TMD\s+approved|Issued:?)\s*[:\s]*(.{5,60})",
        full_text, re.I)
    if date_context:
        verified_date, date_conf = parse_date(date_context.group(1))
    if not verified_date:
        verified_date, date_conf = parse_date(first_3000)
    if not verified_date:
        verified_date, date_conf = parse_date(full_text)

    # ── 2e-ii: PERF "Asset Class Breakdown" date override ──
    # Investment Centre PERF documents often pick up a stale fund-inception date.
    # When the initial date is >1 year old, prefer the date next to "Asset Class Breakdown".
    if verified_date:
        try:
            _y, _m, _d = (int(x) for x in verified_date.split("_"))
            _age_days = (_date_cls.today() - _date_cls(_y, _m, _d)).days
        except (ValueError, OverflowError):
            _age_days = 0  # skip override if date is invalid
        if _age_days > 365:
            _acb = re.search(
                r"ASSET\s+CLASS\s+BREAKDOWN\s*\(?([^)\n]{5,60})\)?",
                full_text, re.I,
            )
            if _acb:
                _acb_date, _acb_conf = parse_date(_acb.group(1))
                if _acb_date:
                    verified_date, date_conf = _acb_date, _acb_conf

    reasoning = []
    reasoning.append(
        f"Classification: type={verified_type} (zone={type_signal_zone}), "
        f"codes={validated_codes}, date={verified_date} ({date_conf})"
    )

    now_str = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%d %H:%M:%S AEDT")

    method          = "FAST_PATH_VERIFIED"
    name_fallback_used = False
    claude_resolved_apir = False

    # ── 2f: Handle missing APIR codes ──
    # Priority order: 1) direct extraction (Method 1, already done above)
    #                 2) filename APIR fallback
    #                 3) Claude API (handled in 2i-claude below)
    #                 4) name fallback (Method 2) — last resort
    apir_filename_fallback_used = False

    _is_arpt = verified_type == "ARPT" or "ARPT" in fname.upper()
    _is_pds_tmd = verified_type in ("PDSX", "TMDX") or any(
        tc in fname.upper() for tc in ("PDSX", "PDS", "TMDX", "TMD"))

    if not validated_codes:
        fn_apir = fn_apir_cached
        # fn_apir_cached is None when filename APIR not in CSV — get raw APIR for PDS/TMD
        _fn_apir_raw_m = apir_pat.search(fname)
        _fn_apir_any = fn_apir or (_fn_apir_raw_m.group() if _fn_apir_raw_m else None)

        if fn_apir and fn_apir not in apir_set and not _is_arpt and not _is_pds_tmd:
            # Filename APIR is not in the reference CSV — no point continuing.
            # Return exception immediately. (ARPTs skip to try name fallback;
            # PDS/TMDs skip to use filename APIR directly.)
            file_stats["exceptions"] += 1
            file_exception_rows.append({"original": fname, "fields": "APIR_NOT_IN_SPREADSHEET"})
            reasoning.append(
                f"Filename APIR '{fn_apir}' is not in reference CSV. "
                f"No valid APIR codes found in document content either."
            )
            file_log_rows.append({
                "input": fname, "renamed": "No", "confidence": 0.0,
                "datetime": now_str, "prompt": PROMPT_TEXT,
                "files": "", "reasoning": " | ".join(reasoning),
            })
            output_lines.append(f"  {fname} -> EXCEPTION (APIR_NOT_IN_SPREADSHEET) — not in reference CSV")
            return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

        # PDS/TMD: use filename APIR if in CSV, otherwise discard — no name fallback
        if _is_pds_tmd and not fn_apir and _fn_apir_any:
            if _fn_apir_any in apir_set:
                validated_codes = [_fn_apir_any]
                apir_filename_fallback_used = True
                method = "APIR_FILENAME_FALLBACK"
                file_stats["apir_filename_fallback"] += 1
                reasoning.append(
                    f"PDS/TMD filename APIR fallback: using APIR '{_fn_apir_any}' from filename."
                )
            else:
                file_stats["exceptions"] += 1
                file_exception_rows.append({"original": fname, "fields": "APIR_NOT_IN_SPREADSHEET"})
                reasoning.append(
                    f"PDS/TMD: APIR '{_fn_apir_any}' from filename not in reference CSV. Discarded."
                )
                file_log_rows.append({
                    "input": fname, "renamed": "No", "confidence": 0.0,
                    "datetime": now_str, "prompt": PROMPT_TEXT,
                    "files": "", "reasoning": " | ".join(reasoning),
                })
                output_lines.append(f"  {fname} -> EXCEPTION (APIR_NOT_IN_SPREADSHEET) — not in reference CSV")
                return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)
        # Try targeted name verification: look up the filename APIR's IO names
        # in the reference CSV and search for them in the document text.
        elif fn_apir:
            found, matched_name = verify_apir_by_name(fn_apir, full_text, norm_text=norm_text)
            if found:
                validated_codes = [fn_apir]
                method = "APIR_NAME_VERIFIED"
                reasoning.append(
                    f"APIR name verification: filename APIR '{fn_apir}' confirmed — "
                    f"IO name '{matched_name}' found in document content."
                )
            else:
                # Name not found — fall back to using filename APIR directly
                validated_codes          = [fn_apir]
                apir_filename_fallback_used = True
                method                   = "APIR_FILENAME_FALLBACK"
                file_stats["apir_filename_fallback"] += 1
                reasoning.append(
                    f"APIR filename fallback: no APIR or IO name found in content; "
                    f"using APIR '{fn_apir}' from original filename."
                )

    # ── 2g: Handle missing document type (content-based only — filename fallback is at the end) ──
    doctype_filename_fallback_used = False

    if not verified_type:
        inferred_type, infer_detail = classify_doctype_content_fallback(full_text)
        reasoning.append(infer_detail)
        if inferred_type:
            verified_type           = inferred_type
            doctype_fallback_method = "content_fallback"
            file_stats["doctype_content_fallback"] += 1
            reasoning.append(f"DOCTYPE content fallback resolved type as {inferred_type}.")
        else:
            file_exception_rows.append({"original": fname, "fields": "DOCTYPE_UNKNOWN"})
            file_stats["exceptions"] += 1
            reasoning.append("DOCTYPE: no content-based classification found; deferring to later fallbacks.")

    # ── 2h: Filename typecode cross-check ──
    # Only check mismatch if we have both a content-derived type and a filename type.
    if verified_type and not doctype_filename_fallback_used:
        if fn_type and fn_type != verified_type:
            # Keyword disagrees with filename — use Claude API as tiebreaker if available
            claude_tiebreak = None
            if CLAUDE_ENABLED:
                reasoning.append(
                    f"Doc type conflict: filename='{fn_type}' vs content='{verified_type}' "
                    f"— invoking Claude API as tiebreaker."
                )
                claude_result = await classify_with_claude(
                    session, path, known_apir=None, known_type=None, known_date=verified_date,
                    full_text=full_text, fname=fname,
                    pages=_pages_cache.get(fname, []),
                )
                if claude_result and claude_result.get("doctype"):
                    vt = claude_result["doctype"].strip().upper()
                    if vt in KNOWN_TYPECODES:
                        claude_tiebreak = vt
                        reasoning.append(f"Claude API doctype tiebreaker: '{vt}'.")

            if claude_tiebreak:
                # Claude API resolved the conflict — use its answer, no Doc_Mismatch
                verified_type = claude_tiebreak
                file_stats["claude_api"] += 1
                reasoning.append(
                    f"Doc type resolved by Claude API: '{claude_tiebreak}' "
                    f"(filename='{fn_type}', content='{verified_type}')."
                )
            else:
                # No Claude API or Claude API couldn't help — fall back to filename, flag mismatch
                file_stats["typecode_mismatch"] += 1
                file_stats["doc_mismatch"] += 1
                file_exception_rows.append({"original": fname, "fields": "Doc_Mismatch"})
                file_stats["exceptions"] += 1
                reasoning.append(
                    f"Doc_Mismatch: filename contains '{fn_type}' but content-derived type "
                    f"is '{verified_type}'. Using filename type '{fn_type}' for rename."
                )
                verified_type = fn_type
        elif fn_type:
            reasoning.append(f"Filename typecode {fn_type} matches content-derived {verified_type}.")
        else:
            reasoning.append("No recognisable typecode in filename.")

    # ── 2i: Date — defer filename fallback to end of process ──
    date_filename_fallback_used = False
    fn_date, fn_date_conf = parse_date_from_filename(fname)

    if not verified_date:
        file_exception_rows.append({"original": fname, "fields": "DATE_NOT_FOUND"})
        file_stats["exceptions"] += 1
        reasoning.append("DATE: no date found in content; deferring to later fallbacks.")

    # ── 2i-claude: Claude API retry for selected exception types ──
    # Skip Claude if filename APIR isn't in reference CSV and no codes found
    # in content — Claude will return the same unvalidated APIR, wasting a call.
    _skip_claude = (
        not validated_codes
        and fn_apir_cached
        and fn_apir_cached not in apir_set
    )

    if CLAUDE_ENABLED and not _skip_claude:
        file_exc_codes = {
            r["fields"] for r in file_exception_rows if r["original"] == fname
        }
        trigger_codes_hit = file_exc_codes & (CLAUDE_TRIGGER_CODES - {"SCANNED_UNREADABLE"})

        if trigger_codes_hit:
            output_lines.append(f"  {fname} -> Claude API retry triggered by: {sorted(trigger_codes_hit)}")

            apir_for_claude  = validated_codes[0] if len(validated_codes) == 1 else None
            type_for_claude  = verified_type  if verified_type  and "DOCTYPE_UNKNOWN"  not in trigger_codes_hit and "Doc_Mismatch"  not in trigger_codes_hit else None
            date_for_claude  = verified_date  if verified_date  and "DATE_Mismatch" not in trigger_codes_hit and "DATE_NOT_FOUND" not in trigger_codes_hit else None
            if trigger_codes_hit & {"APIR_NOT_FOUND", "NAME_AMBIGUOUS", "APIR_Mismatch"}:
                apir_for_claude = None

            claude_result = await classify_with_claude(
                session, path,
                known_apir=apir_for_claude,
                known_type=type_for_claude,
                known_date=date_for_claude,
                full_text=full_text,
                fname=fname,
                pages=_pages_cache.get(fname, []),
            )

            if claude_result and any(claude_result.values()):
                v_apir  = claude_result.get("apir")
                v_type  = claude_result.get("doctype")
                v_date  = claude_result.get("date")

                if v_apir and v_apir in apir_set:
                    if not validated_codes or "APIR_NOT_FOUND" in trigger_codes_hit \
                            or "NAME_AMBIGUOUS" in trigger_codes_hit \
                            or "APIR_Mismatch" in trigger_codes_hit:
                        validated_codes = [v_apir]
                        claude_resolved_apir = True
                        reasoning.append(
                            f"Claude API resolved APIR as '{v_apir}' "
                            f"(trigger: {sorted(trigger_codes_hit)})."
                        )
                if v_type and v_type in KNOWN_TYPECODES:
                    if not verified_type or "DOCTYPE_UNKNOWN" in trigger_codes_hit \
                            or "Doc_Mismatch" in trigger_codes_hit:
                        verified_type = v_type
                        reasoning.append(
                            f"Claude API resolved doctype as '{v_type}' "
                            f"(trigger: {sorted(trigger_codes_hit)})."
                        )
                if v_date and re.fullmatch(r"20\d{2}_\d{2}_\d{2}", v_date):
                    if not verified_date or "DATE_Mismatch" in trigger_codes_hit:
                        verified_date = v_date
                        date_conf = "exact"
                        reasoning.append(
                            f"Claude API resolved date as '{v_date}' "
                            f"(trigger: {sorted(trigger_codes_hit)})."
                        )

                still_missing = []
                if not validated_codes:
                    still_missing.append("apir")
                if not verified_type:
                    still_missing.append("doctype")
                if not verified_date:
                    still_missing.append("date")

                if still_missing:
                    file_stats["claude_api_failed"] += 1
                    reasoning.append(
                        f"Claude API attempted but could not resolve: {still_missing}. "
                        f"Deferring to filename fallbacks."
                    )
                else:
                    file_stats["claude_api"] += 1
                    method = "CLAUDE_API"
                    reasoning.append("Claude API fallback succeeded.")
            else:
                file_stats["claude_api_failed"] += 1
                reasoning.append("Claude API fallback returned no usable result. Deferring to filename fallbacks.")

    # ── 2j: Name fallback (Method 2) — last resort for missing APIR ──
    # Only runs if all prior methods (direct extraction, filename, Claude API) failed.
    # PDS/TMD: skip name fallback — only APIR codes in content should determine the code.
    if not validated_codes and not _is_pds_tmd:
        fb_codes, fb_detail, fb_exc = name_fallback_multi(
            full_text, name_index, norm_text=norm_text, relaxed=_is_arpt)
        reasoning.append(fb_detail)
        if fb_exc:
            # For ARPTs: if strict matching also fails, try relaxed as a second pass
            # (only if we didn't already use relaxed)
            if not _is_arpt:
                pass  # non-ARPT, no second attempt
            # If name fallback failed and filename has an APIR/ticker, flag as
            # APIR_NOT_IN_SPREADSHEET rather than APIR_NOT_FOUND
            _fn_apir_raw = apir_pat.search(fname)
            _fn_ticker_label = _fn_apir_raw.group() if _fn_apir_raw else fn_apir_cached
            if fb_exc == "APIR_NOT_FOUND" and _fn_ticker_label:
                fb_exc = "APIR_NOT_IN_SPREADSHEET"
                reasoning.append(
                    f"Filename APIR/ticker '{_fn_ticker_label}' not in reference CSV "
                    f"and no IO name matched in document."
                )
            file_exception_rows.append({"original": fname, "fields": fb_exc})
            file_stats["exceptions"] += 1
            file_log_rows.append({"input": fname, "renamed": "No", "confidence": 0.0,
                "datetime": now_str, "prompt": PROMPT_TEXT,
                "files": "", "reasoning": " | ".join(reasoning)})
            output_lines.append(f"  {fname} -> EXCEPTION ({fb_exc}) — all APIR methods exhausted")
            return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)
        else:
            validated_codes    = fb_codes
            name_fallback_used = True
            if verified_type == "ARPT":
                method = "NAME_FALLBACK_ARPT"
                file_stats["name_fallback_arpt"] += 1
            else:
                method = "NAME_FALLBACK"
                file_stats["name_fallback"] += 1
            file_exception_rows.append({"original": fname, "fields": "NAME_FALLBACK"})
            file_stats["exceptions"] += 1

    # PDS/TMD: name fallback was skipped — flag exception if still no codes
    if not validated_codes and _is_pds_tmd:
        _fn_apir_raw = apir_pat.search(fname)
        _fn_ticker_label2 = _fn_apir_raw.group() if _fn_apir_raw else fn_apir_cached
        if _fn_ticker_label2:
            fb_exc = "APIR_NOT_IN_SPREADSHEET"
            reasoning.append(
                f"PDS/TMD: IO name fallback skipped (content APIR only). "
                f"Filename APIR/ticker '{_fn_ticker_label2}' not in reference CSV."
            )
        else:
            fb_exc = "APIR_NOT_FOUND"
            reasoning.append("PDS/TMD: IO name fallback skipped. No APIR found in content or filename.")
        file_exception_rows.append({"original": fname, "fields": fb_exc})
        file_stats["exceptions"] += 1
        file_log_rows.append({"input": fname, "renamed": "No", "confidence": 0.0,
            "datetime": now_str, "prompt": PROMPT_TEXT,
            "files": "", "reasoning": " | ".join(reasoning)})
        output_lines.append(f"  {fname} -> EXCEPTION ({fb_exc}) — all APIR methods exhausted")
        return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

    # ── 2k: Filename fallback for doctype (last resort) ──
    if not verified_type:
        fn_tc = parse_typecode_from_filename(fname)
        if fn_tc:
            verified_type = fn_tc
            doctype_filename_fallback_used = True
            file_stats["doctype_filename_fallback"] += 1
            reasoning.append(
                f"DOCTYPE filename fallback: all classification methods exhausted; "
                f"using typecode '{fn_tc}' from original filename."
            )
        else:
            file_log_rows.append({"input": fname, "renamed": "No", "confidence": 0.0,
                "datetime": now_str, "prompt": PROMPT_TEXT,
                "files": "", "reasoning": " | ".join(reasoning)})
            output_lines.append(f"  {fname} -> EXCEPTION (DOCTYPE_UNKNOWN) — all methods exhausted")
            return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

    # ── 2l: Filename fallback for date (last resort) ──
    if not verified_date:
        if fn_date:
            verified_date = fn_date
            date_conf = fn_date_conf
            date_filename_fallback_used = True
            file_stats["date_filename_fallback"] += 1
            file_stats["date_m"] += 1
            reasoning.append(
                f"DATE filename fallback: no date found in content; "
                f"using date '{fn_date}' from original filename."
            )
        else:
            file_log_rows.append({"input": fname, "renamed": "No", "confidence": 0.0,
                "datetime": now_str, "prompt": PROMPT_TEXT,
                "files": "", "reasoning": " | ".join(reasoning)})
            output_lines.append(f"  {fname} -> EXCEPTION (DATE_NOT_FOUND) — all methods exhausted")
            return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)
    elif fn_date and verified_date != fn_date:
        # Content date exists but differs from filename — filename wins, flag DATE_Mismatch
        file_stats["date_m"] += 1
        file_exception_rows.append({"original": fname, "fields": "DATE_Mismatch"})
        file_stats["exceptions"] += 1
        reasoning.append(
            f"DATE_Mismatch: content date '{verified_date}' differs from filename date '{fn_date}'. "
            f"Using filename date '{fn_date}' for rename."
        )
        verified_date = fn_date
        date_conf = fn_date_conf
        date_filename_fallback_used = True

    # ── 2m: APIR_Mismatch — filename APIR not found by Method 1, name verification, or Claude API ──
    apir_name_verified = (method == "APIR_NAME_VERIFIED")
    if fn_apir_cached and fn_apir_cached not in content_extracted_codes \
            and not claude_resolved_apir and not apir_name_verified:
        file_stats["apir_mismatch"] += 1
        file_exception_rows.append({"original": fname, "fields": "APIR_Mismatch"})
        file_stats["exceptions"] += 1
        reasoning.append(
            f"APIR_Mismatch: filename APIR '{fn_apir_cached}' was not found in PDF content "
            f"by direct extraction, name verification, or Claude API."
        )
        # ── 2m-ii: APIR_O_M — add rename row for filename APIR + exception ──
        # The filename APIR is valid (in reference CSV) but absent from content-derived codes.
        # Add it to validated_codes so it gets its own rename row, and flag APIR_O_M.
        if fn_apir_cached not in validated_codes:
            validated_codes.append(fn_apir_cached)
            file_stats["apir_o_m"] = file_stats.get("apir_o_m", 0) + 1
            file_exception_rows.append({"original": fname, "fields": "APIR_O_M"})
            file_stats["exceptions"] += 1
            reasoning.append(
                f"APIR_O_M: filename APIR '{fn_apir_cached}' not extracted from content. "
                f"Added rename row for filename APIR; flagged for manual review."
            )

    # ── 2n: Final check — all required fields present ──
    if not validated_codes:
        _fn_apir_raw = apir_pat.search(fname)
        _fn_final_label = _fn_apir_raw.group() if _fn_apir_raw else fn_apir_cached
        _final_exc = "APIR_NOT_IN_SPREADSHEET" if _fn_final_label else "APIR_NOT_FOUND"
        file_log_rows.append({"input": fname, "renamed": "No", "confidence": 0.0,
            "datetime": now_str, "prompt": PROMPT_TEXT,
            "files": "", "reasoning": " | ".join(reasoning)})
        output_lines.append(f"  {fname} -> EXCEPTION ({_final_exc}) — all methods exhausted")
        return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)

    # ── 2n: Confidence scoring — SKILL v5 matrix ──
    if apir_filename_fallback_used:
        conf = 2.5
    elif not name_fallback_used:
        conf = {"exact": 9.5, "month_year_inferred": 8.0, "year_only_inferred": 7.0}.get(date_conf, 7.0)
        if doctype_fallback_method == "content_fallback" or doctype_filename_fallback_used:
            conf = max(conf - 1.0, 3.0)
    else:
        conf = {"exact": 6.0, "month_year_inferred": 4.5, "year_only_inferred": 3.5}.get(date_conf, 3.5)
        if doctype_fallback_method == "content_fallback" or doctype_filename_fallback_used:
            conf = max(conf - 0.5, 3.0)

    if date_filename_fallback_used:
        conf = max(conf - 1.0, 2.0)

    if ocr_used:
        conf = max(conf - 0.5, 2.0)

    ext = os.path.splitext(fname)[1].lower()

    if method == "FAST_PATH_VERIFIED":
        file_stats["fast_verified"] += 1
    elif method in ("NAME_FALLBACK", "NAME_FALLBACK_ARPT"):
        pass
    else:
        file_stats["fast_corrected"] += 1

    if ocr_used:
        reasoning.append("OCR_FALLBACK: text extracted via Tesseract OCR (confidence penalised −0.5).")

    for code in validated_codes:
        renamed = f"{code}_{verified_type}_{verified_date}{ext}"
        file_rename_rows.append({
            "original": fname, "renamed": renamed, "code": code,
            "type": verified_type, "date": verified_date, "confidence": conf,
        })
        file_stats["renamed"] += 1
        output_lines.append(f"  {fname} -> {renamed} ({conf})")

    reasoning.append(
        f"Output: codes={validated_codes}, type={verified_type}, "
        f"date={verified_date} ({date_conf}), conf={conf}, method={method}"
    )
    file_log_rows.append({
        "input": fname, "renamed": "Yes", "confidence": conf,
        "datetime": now_str, "prompt": PROMPT_TEXT,
        "files": "", "reasoning": " | ".join(reasoning),
    })

    return (file_rename_rows, file_exception_rows, file_log_rows, file_stats, output_lines)


import threading as _threading
_extract_sem = _threading.Semaphore(EXTRACT_CONCURRENCY)  # limits concurrent PDF opens (RAM)

for subfolder in subfolders:
    print(f"\n{'=' * 60}")
    print(f"STARTING SUBFOLDER: {subfolder}")
    print(f"{'=' * 60}")

    subfolder_start = time.time()

    pdf_dir = BASE / "input_pdfs" / subfolder
    files = sorted([f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")])
    if not files:
        print(f"WARNING: No PDFs found in {pdf_dir} — skipping.")
        continue
    print(f"PDFs found: {len(files)}")

    rename_rows    = []
    exception_rows = []
    log_rows       = []
    stats = {
        "processed": 0, "renamed": 0, "exceptions": 0,
        "fast_verified": 0, "fast_corrected": 0,
        "name_fallback": 0, "name_fallback_arpt": 0,
        "doctype_content_fallback": 0, "typecode_mismatch": 0,
        "scanned_unreadable": 0,
        "apir_filename_fallback": 0, "apir_mismatch": 0, "apir_o_m": 0,
        "doctype_filename_fallback": 0, "doc_mismatch": 0,
        "date_filename_fallback": 0, "date_m": 0,
        "ocr_fallback": 0,
        "claude_api": 0, "claude_api_failed": 0,
    }

    # ── Step 1b: Pre-extract text from all PDFs ──
    # Extracts text once in parallel, caches results so processing never re-reads PDFs.
    _text_cache = {}   # fname -> (full_text, ocr_used, bailed)
    _pages_cache = {}  # fname -> list of per-page text strings (for Claude chunking)

    def _extract_text(fname):
        """Pre-extract text from a single PDF. Returns (fname, full_text, ocr_used, pages_list).
        For files where the filename APIR is not in the reference CSV, extracts only the
        first 2 pages to check for other valid APIRs. If none found, returns empty."""
        fn_m = apir_pat.search(fname)
        fn_apir_unknown = fn_m and fn_m.group() not in apir_set
        path = os.path.join(pdf_dir, fname)
        ocr_used = False
        fname_upper = fname.upper()
        is_pds = "PDSX" in fname_upper or "PDS" in fname_upper
        is_arpt = "ARPT" in fname_upper
        pages_list = []
        chars_extracted = 0
        bail_checked = False
        try:
            with _extract_sem:
                doc = fitz.open(path)
                for i in range(len(doc)):
                    t = doc[i].get_text()
                    if not t or not t.strip():
                        continue
                    # PDS: stop extracting at "This Application Form accompanies"
                    if is_pds:
                        m = _APPLICATION_FORM_RE.search(t)
                        if m:
                            before = t[:m.start()].strip()
                            if before:
                                pages_list.append(before)
                            break
                    # ARPT: stop extracting at "Statement of Comprehensive Income" heading
                    if is_arpt:
                        m = _ARPT_STOP_RE.search(t)
                        if m:
                            before = t[:m.start()].strip()
                            if before:
                                pages_list.append(before)
                            break
                    pages_list.append(t)
                    chars_extracted += len(t)
                    # For unknown filename APIRs: once we have 3000+ chars, check if
                    # any valid APIRs exist. If not, stop early to save time.
                    if fn_apir_unknown and not bail_checked and chars_extracted >= 3000:
                        bail_checked = True
                        found_valid = any(c in apir_set for c in apir_pat.findall("\n".join(pages_list)))
                        if not found_valid:
                            pages_list = []
                            break
                doc.close()
            full_text = "\n\n".join(pages_list)
        except Exception:
            full_text = ""

        # OCR fallback for empty text — skip if we bailed due to unrecognised APIR
        if not full_text.strip() and not (fn_apir_unknown and bail_checked):
            try:
                images = convert_from_path(path)
                ocr_pages = []
                for img in images:
                    t = pytesseract.image_to_string(img)
                    if t:
                        ocr_pages.append(t)
                full_text = "\n\n".join(ocr_pages)
                if full_text.strip():
                    ocr_used = True
                    pages_list = ocr_pages
            except Exception:
                pass

        bailed = fn_apir_unknown and bail_checked and not full_text.strip()
        return fname, full_text, ocr_used, pages_list, bailed

    # ── Step 2: Async pipeline — extract (CPU pool) → process (async + Claude API) ──

    async def _async_main():
        global _current_file
        executor = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)
        loop = asyncio.get_running_loop()
        pipeline_sem = asyncio.Semaphore(PIPELINE_CONCURRENCY)
        completed = 0
        extracted = 0

        async def handle_file(fname):
            nonlocal extracted
            async with pipeline_sem:
                # Phase 1: extract text (CPU-bound, in thread pool)
                fname_r, text_r, ocr_r, pages_r, bailed_r = await loop.run_in_executor(
                    executor, _extract_text, fname
                )
                _text_cache[fname_r] = (text_r, ocr_r, bailed_r)
                _pages_cache[fname_r] = pages_r
                extracted += 1
                sys.stdout.write(f"\r  Extracted: {extracted}/{len(files)}   ")
                sys.stdout.flush()

                # Phase 2: process (async — Claude calls are non-blocking)
                return await process_single_file(session, fname_r)

        print(f"Extracting & processing {len(files)} PDFs "
              f"(executor={EXECUTOR_WORKERS}, pipeline={PIPELINE_CONCURRENCY}, "
              f"haiku={CLAUDE_HAIKU_CONCURRENCY}, sonnet={CLAUDE_SONNET_CONCURRENCY})...",
              flush=True)

        connector = aiohttp.TCPConnector(limit=250, limit_per_host=250)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [asyncio.create_task(handle_file(f)) for f in files]

            # Gather results in original file order for deterministic output
            results = await asyncio.gather(*tasks, return_exceptions=True)
            sys.stdout.write("\n")  # newline after extraction progress
            for result in results:
                if isinstance(result, Exception):
                    print(f"  UNEXPECTED ERROR: {result}")
                    completed += 1
                    continue

                f_rename, f_exc, f_log, f_stats, f_output = result
                rename_rows.extend(f_rename)
                exception_rows.extend(f_exc)
                log_rows.extend(f_log)
                for k in stats:
                    stats[k] += f_stats[k]
                completed += 1
                print_progress(completed, len(files))
                for line in f_output:
                    print(line)

        executor.shutdown(wait=True)
        _current_file = None

    # Suppress Windows ProactorEventLoop connection cleanup noise
    import logging as _logging
    _logging.getLogger("asyncio").setLevel(_logging.CRITICAL)

    asyncio.run(_async_main())

    # ── Batch validation ──
    # Re-validate every APIR/Ticker code in the completed rename list against Column A.
    # Any code that fails this check is moved to exceptions with APIR_NOT_IN_SPREADSHEET
    # (SKILL v5 exception code — replaces BATCH_VALIDATION_FAIL used in v4).
    final_rename = []
    for r in rename_rows:
        if r["code"] not in apir_set:
            exception_rows.append({"original": r["original"], "fields": "APIR_NOT_IN_SPREADSHEET"})
            stats["renamed"]    -= 1
            stats["exceptions"] += 1
        else:
            final_rename.append(r)
    rename_rows = final_rename

    # ── Post-batch OCR retry for APIR_Mismatch / Doc_Mismatch files ──
    # Runs Tesseract OCR only on files that had a mismatch between filename and content,
    # then re-classifies locally. Only OCRs the first 3 pages (APIR/doctype are on the
    # cover) and processes files in parallel for speed.

    _mismatch_fnames = set()
    for r in exception_rows:
        if r["fields"] in ("APIR_Mismatch", "Doc_Mismatch"):
            _mismatch_fnames.add(r["original"])

    # Remove files where pre-extract already used OCR
    _mismatch_fnames = {
        f for f in _mismatch_fnames
        if not _text_cache.get(f, ("", False, False))[1]
    }

    _OCR_MAX_PAGES = 3  # Only OCR first N pages — APIR/doctype are on the cover

    def _ocr_mismatch_file(fname):
        """OCR first pages of a mismatch file. Returns (fname, ocr_text) or (fname, '')."""
        path = os.path.join(pdf_dir, fname)
        try:
            images = convert_from_path(path, first_page=1, last_page=_OCR_MAX_PAGES)
            pages = []
            for img in images:
                t = pytesseract.image_to_string(img)
                if t and t.strip():
                    pages.append(t)
            return fname, "\n\n".join(pages)
        except Exception:
            return fname, ""

    if _mismatch_fnames:
        print(f"\n  OCR retry pass: {len(_mismatch_fnames)} files with APIR/Doc mismatch "
              f"(first {_OCR_MAX_PAGES} pages, parallel)...", flush=True)
        _ocr_resolved = 0

        # Parallel OCR using thread pool
        _ocr_results = {}
        with ThreadPoolExecutor(max_workers=min(len(_mismatch_fnames), 8)) as _ocr_pool:
            for fname, ocr_text in _ocr_pool.map(_ocr_mismatch_file, sorted(_mismatch_fnames)):
                if ocr_text.strip():
                    _ocr_results[fname] = ocr_text

        # Process OCR results
        for _ocr_fname, _ocr_text in _ocr_results.items():
            _file_exc_types = set(
                r["fields"] for r in exception_rows if r["original"] == _ocr_fname
            )
            _changed = False
            _ocr_reasoning_parts = ["OCR post-batch retry: Tesseract applied for mismatch resolution."]

            # --- APIR_Mismatch: try to find APIR in OCR text ---
            if "APIR_Mismatch" in _file_exc_types:
                _ocr_codes, _ = extract_primary_codes(_ocr_text)
                _fn_apir = parse_apir_from_filename(_ocr_fname)
                if _ocr_codes and _fn_apir in _ocr_codes:
                    exception_rows = [
                        r for r in exception_rows
                        if not (r["original"] == _ocr_fname and r["fields"] in ("APIR_Mismatch", "APIR_O_M"))
                    ]
                    stats["apir_mismatch"] -= 1
                    if stats.get("apir_o_m", 0) > 0:
                        stats["apir_o_m"] -= 1
                        stats["exceptions"] -= 1  # one for APIR_O_M
                    stats["exceptions"] -= 1  # one for APIR_Mismatch
                    stats["ocr_fallback"] += 1
                    _changed = True
                    _ocr_reasoning_parts.append(
                        f"OCR confirmed APIR '{_fn_apir}' in document content — APIR_Mismatch and APIR_O_M removed."
                    )
                elif _ocr_codes:
                    _ocr_reasoning_parts.append(
                        f"OCR found APIR codes {_ocr_codes} but filename APIR '{_fn_apir}' "
                        f"not among them — APIR_Mismatch retained."
                    )

            # --- Doc_Mismatch: try to classify doctype from OCR text ---
            if "Doc_Mismatch" in _file_exc_types:
                _ocr_text_lower = _ocr_text.lower()
                _ocr_type, _ = classify_doctype_from_cover(_ocr_text, _ocr_text_lower)
                if not _ocr_type:
                    _ocr_type, _ = classify_doctype_content_fallback(_ocr_text)
                _fn_type = parse_typecode_from_filename(_ocr_fname)
                if _ocr_type and _ocr_type == _fn_type:
                    exception_rows = [
                        r for r in exception_rows
                        if not (r["original"] == _ocr_fname and r["fields"] == "Doc_Mismatch")
                    ]
                    stats["doc_mismatch"] -= 1
                    stats["exceptions"] -= 1
                    stats["ocr_fallback"] += 1
                    _changed = True
                    _ocr_reasoning_parts.append(
                        f"OCR confirmed doctype '{_fn_type}' matches filename — Doc_Mismatch removed."
                    )
                elif _ocr_type:
                    _ocr_reasoning_parts.append(
                        f"OCR derived doctype '{_ocr_type}' vs filename '{_fn_type}' "
                        f"— Doc_Mismatch retained."
                    )

            if _changed:
                _ocr_resolved += 1
                for lr in log_rows:
                    if lr["input"] == _ocr_fname:
                        lr["reasoning"] += " | " + " | ".join(_ocr_reasoning_parts)
                        break
                print(f"    OCR resolved: {_ocr_fname}", flush=True)

        print(f"  OCR retry pass complete: {_ocr_resolved}/{len(_mismatch_fnames)} mismatches resolved.",
              flush=True)

    # ── Write CSVs to ./output/ ──
    ts = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y_%m_%d_%H_%M_%S")
    os.makedirs("output", exist_ok=True)

    # Sanitize subfolder name for output filenames (replace / and \ with _)
    safe_subfolder = subfolder.replace("/", "_").replace("\\", "_")

    # Use subfolder-prefixed names for the temp files on disk, but strip the prefix
    # for the arcnames inside the zip so the CSVs are named e.g. Rename_list_2024_...csv
    rename_path = f"output/{safe_subfolder}_Rename_list_{ts}.csv"
    exc_path    = f"output/{safe_subfolder}_Exceptions_list_{ts}.csv"
    log_path    = f"output/{safe_subfolder}_Rename_log_{ts}.csv"

    csv_names = (
        f"Rename_list_{ts}.csv | "
        f"Exceptions_list_{ts}.csv | "
        f"Rename_log_{ts}.csv"
    )
    for lr in log_rows:
        lr["files"] = csv_names

    with open(rename_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Original file name", "Renamed file name", "APIR / Ticker Code",
                    "Document Type Code", "Date", "Confidence score"])
        for r in rename_rows:
            w.writerow([r["original"], r["renamed"], r["code"],
                        r["type"], r["date"], r["confidence"]])

    with open(exc_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Original file name", "Field(s) which could not be completed"])
        for r in exception_rows:
            w.writerow([r["original"], r["fields"]])

    with open(log_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Input file name", "Successfully renamed", "Confidence score",
                    "Date & time prompt run", "Full prompt used",
                    "Files generated", "Document-level reasoning"])
        for r in log_rows:
            w.writerow([r["input"], r["renamed"], r["confidence"],
                        r["datetime"], r["prompt"], r["files"], r["reasoning"]])

    # ── Zip the three CSVs (arcnames stripped of subfolder prefix) ──
    zip_name = f"{safe_subfolder}_{ts}.zip"
    zip_path = f"output/{zip_name}"
    csv_files = [
        (Path(rename_path), f"Rename_list_{ts}.csv"),
        (Path(exc_path),    f"Exceptions_list_{ts}.csv"),
        (Path(log_path),    f"Rename_log_{ts}.csv"),
    ]
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for cp, arcname in csv_files:
            zf.write(cp, arcname=arcname)
    for cp, _ in csv_files:
        cp.unlink()
    print(f"  Zipped -> output/{zip_name}")

    # ── Final Summary ──
    elapsed = time.time() - subfolder_start
    mins, secs = divmod(int(elapsed), 60)
    elapsed_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    print("=" * 60)
    print(f"SUBFOLDER: {subfolder}")
    print("=" * 60)
    print(f"  Files processed            : {stats['processed']}")
    print(f"  Successfully renamed       : {stats['renamed']}")
    print(f"  With exceptions            : {stats['exceptions']}")
    print(f"  Total rename rows          : {len(rename_rows)}")
    print()
    print(f"  Fast-path verified         : {stats['fast_verified']}")
    print(f"  Fast-path corrected        : {stats['fast_corrected']}")
    print(f"  Name fallback (non-ARPT)   : {stats['name_fallback']}")
    print(f"  Name fallback (ARPT multi) : {stats['name_fallback_arpt']}")
    print(f"  DOCTYPE content fallback   : {stats['doctype_content_fallback']}")
    print(f"  Scanned/unreadable         : {stats['scanned_unreadable']}")
    print(f"  OCR fallback applied       : {stats['ocr_fallback']}")
    print(f"  OCR mismatch resolved      : {_ocr_resolved if _mismatch_fnames else 0}")
    print(f"  Claude API fallback     : {stats['claude_api']}")
    print(f"  Claude API failed       : {stats['claude_api_failed']}")
    print()
    print(f"  Filename fallbacks used:")
    print(f"    APIR from filename       : {stats['apir_filename_fallback']}")
    print(f"    DOCTYPE from filename    : {stats['doctype_filename_fallback']}")
    print(f"    Date from filename       : {stats['date_filename_fallback']}")
    print()
    print(f"  Mismatch flags (in exceptions list):")
    print(f"    APIR_Mismatch            : {stats['apir_mismatch']}")
    print(f"    APIR_O_M                 : {stats['apir_o_m']}")
    print(f"    Doc_Mismatch             : {stats['doc_mismatch']}")
    print(f"    DATE_Mismatch            : {stats['date_m']}")
    print()
    print(f"Output written to ./output/:")
    print(f"  {safe_subfolder}_Rename_list_{ts}.csv")
    print(f"  {safe_subfolder}_Exceptions_list_{ts}.csv")
    print(f"  {safe_subfolder}_Rename_log_{ts}.csv")
    print(f"  {zip_name}")
    print()
    print(f"  Elapsed time               : {elapsed_str}")
    print("=" * 60)

# Total elapsed time across all subfolders
if len(subfolders) > 1:
    total_elapsed = time.time() - run_start
    t_mins, t_secs = divmod(int(total_elapsed), 60)
    total_str = f"{t_mins}m {t_secs}s" if t_mins else f"{t_secs}s"
    print(f"\nAll {len(subfolders)} subfolders completed in {total_str}")
