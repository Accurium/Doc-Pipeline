"""
db_extractor.py
===============
Ad-hoc query tool for the APL DuckDB database.
Useful for investigating exceptions, checking pipeline status,
and exporting raw tables to CSV or Excel for your manager.

USAGE
-----
  # Show pipeline run summary
  python pipeline\db_extractor.py --summary

  # Show all exceptions from the last run
  python pipeline\db_extractor.py --exceptions

  # Show all files with status 'failed'
  python pipeline\db_extractor.py --failed

  # Export a full raw CSV of any table
  python pipeline\db_extractor.py --export files --out C:\temp\files.csv

  # Run any ad-hoc SQL and print results
  python pipeline\db_extractor.py --sql "SELECT apir_code, COUNT(*) FROM files GROUP BY 1 ORDER BY 2 DESC"

  # Refresh not_available from CSV (re-run after updating reference_data/not_available.csv)
  python pipeline\db_extractor.py --refresh-na

  # Show tracker completeness summary
  python pipeline\db_extractor.py --completeness
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH, NOT_AVAILABLE_CSV, MANDATORY_DOC_TYPES, ALL_DOC_TYPES

try:
    import duckdb
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"])
    import duckdb


# ── Not Available CSV parser ──────────────────────────────────────────────────

# Maps the column header in the wide-format CSV → internal doc_type code.
# Only 'Not Available' is treated as not available.
# All other values (Yes, Downloaded, TBC, blank) mean the document is
# available or pending and are ignored.

_COL_TO_DOCTYPE = {
    "Morningstar": "MSTR",
    "Lonsec":      "LSCR",
    "SQM":         "SQMR",
    "Zenith":      "ZNTR",
    "ARPT":        "ARPT",
    "TMDX":        "TMDX",
    "PDSX":        "PDSX",
    "PERF":        "PERF",
}


def parse_not_available_csv(csv_path: Path) -> list[tuple]:
    """
    Parse the wide-format Not_Available CSV.

    Column layout:
      Code, Morningstar Investment option name, Priority,
      Morningstar, Comment,
      Lonsec,      Comment,
      SQM,         Comment,
      Zenith,      Comment,
      ARPT,        Comment,
      TMDX,        Comment,
      PDSX,        Comment,
      PERF,        Comment

    Only rows where the status value is exactly 'Not Available'
    (case-insensitive) are inserted into the not_available table.
    All other values (Yes, Downloaded, TBC, blank) are ignored.

    Returns list of (apir_code, doc_type, reason, fund_name) tuples.
    """
    rows = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for record in reader:
            apir = record.get("Code", "").strip()
            if not apir:
                continue
            fund_name = record.get("Morningstar Investment option name", "").strip()
            for col_header, doc_type in _COL_TO_DOCTYPE.items():
                status = record.get(col_header, "").strip().lower()
                if status == "not available":
                    rows.append((apir, doc_type, "Not Available", fund_name))
    return rows


# ── Display helper ────────────────────────────────────────────────────────────

def print_table(rows, headers):
    if not rows:
        print("  (no rows)")
        return
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, v in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(v) if v is not None else ""))
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in col_widths))
    for row in rows:
        print(fmt.format(*[str(v) if v is not None else "" for v in row]))


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_summary(con):
    rows = con.execute("""
        SELECT run_id[:8] AS run, source_folder, started_at::VARCHAR[:19] AS started,
               files_processed, renamed, failed, duplicates
        FROM runs ORDER BY started_at DESC LIMIT 20
    """).fetchall()
    print("\nRecent pipeline runs:")
    print_table(rows, ["run_id", "source", "started", "processed", "renamed", "failed", "dupes"])

    totals = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status='renamed')           AS renamed,
            COUNT(*) FILTER (WHERE status='failed')            AS failed,
            COUNT(*) FILTER (WHERE status='skipped_duplicate') AS dupes,
            COUNT(*) FILTER (WHERE source_folder='seeded')     AS seeded
        FROM files
    """).fetchone()
    print(f"\nDatabase totals — renamed: {totals[0]}, failed: {totals[1]}, "
          f"duplicates: {totals[2]}, seeded: {totals[3]}")


def cmd_exceptions(con):
    rows = con.execute("""
        SELECT e.created_at::VARCHAR[:19], e.original_name, e.reason_code, e.detail[:80]
        FROM exceptions e ORDER BY e.created_at DESC LIMIT 50
    """).fetchall()
    print(f"\nLast {len(rows)} exceptions:")
    print_table(rows, ["created_at", "original_name", "reason_code", "detail"])


def cmd_failed(con):
    rows = con.execute("""
        SELECT original_name, source_folder, ingested_at::VARCHAR[:19], raw_path
        FROM files WHERE status='failed'
        ORDER BY ingested_at DESC
    """).fetchall()
    print(f"\nFailed files ({len(rows)} total):")
    print_table(rows, ["original_name", "source", "ingested_at", "raw_path"])


def cmd_export(con, table: str, out_path: str):
    try:
        rows = con.execute(f"SELECT * FROM {table}").fetchall()
        cols = [d[0] for d in con.description]
    except Exception as e:
        print(f"ERROR: {e}")
        return
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"Exported {len(rows)} rows from '{table}' to {out}")


def cmd_sql(con, sql: str):
    try:
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description] if con.description else []
        print_table(rows, cols)
    except Exception as e:
        print(f"ERROR: {e}")


def cmd_refresh_na(con):
    """
    Re-load the not_available table from the wide-format CSV.
    Run this whenever you update reference_data/not_available.csv.
    Completely replaces the existing not_available table contents.
    """
    if not NOT_AVAILABLE_CSV.exists():
        print(f"not_available.csv not found at {NOT_AVAILABLE_CSV}")
        print("Place the wide-format Not_Available CSV there and re-run.")
        return

    parsed = parse_not_available_csv(NOT_AVAILABLE_CSV)
    now    = datetime.now(timezone.utc)
    rows   = [(apir, doc_type, reason, now, None) for apir, doc_type, reason, _ in parsed]

    con.execute("DELETE FROM not_available")
    con.executemany("""
        INSERT INTO not_available (apir_code, doc_type, reason, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?)
    """, rows)
    print(f"not_available refreshed: {len(rows)} rows loaded from {NOT_AVAILABLE_CSV.name}")

    # Show a short sample so the user can verify the parse
    print("\nSample (first 10 rows):")
    print_table(rows[:10], ["apir_code", "doc_type", "reason", "updated_at", "updated_by"])
    if len(rows) > 10:
        print(f"  ... and {len(rows) - 10} more.")


def cmd_completeness(con):
    mandatory = sorted(MANDATORY_DOC_TYPES)

    apir_rows = con.execute(
        "SELECT apir_code, mstar_io_name FROM apir_reference ORDER BY apir_code"
    ).fetchall()

    renamed_set = {
        (r[0], r[1])
        for r in con.execute(
            "SELECT apir_code, doc_type FROM files WHERE status='renamed'"
        ).fetchall()
    }
    na_set = {
        (r[0], r[1])
        for r in con.execute("SELECT apir_code, doc_type FROM not_available").fetchall()
    }

    complete   = 0
    incomplete = 0
    missing_by_type: dict[str, int] = {dt: 0 for dt in mandatory}

    for apir, _ in apir_rows:
        statuses = []
        for dt in mandatory:
            key = (apir, dt)
            if key in renamed_set:
                statuses.append("C")
            elif key in na_set:
                statuses.append("NA")
            else:
                statuses.append("X")
                missing_by_type[dt] += 1
        if all(s in ("C", "NA") for s in statuses):
            complete += 1
        else:
            incomplete += 1

    total = len(apir_rows)
    print(f"\nTracker completeness ({total} APIR codes):")
    print(f"  Complete   : {complete}  ({complete/total*100:.1f}%)" if total else "  No APIR codes loaded.")
    print(f"  Incomplete : {incomplete}  ({incomplete/total*100:.1f}%)" if total else "")
    print(f"\nMissing by mandatory doc type:")
    for dt, count in missing_by_type.items():
        pct = count / total * 100 if total else 0
        print(f"  {dt:<6} : {count} missing  ({pct:.1f}%)")

    print(f"\nSupplementary docs present (status='renamed'):")
    for dt in sorted(ALL_DOC_TYPES - MANDATORY_DOC_TYPES):
        count = sum(1 for (_, d) in renamed_set if d == dt)
        print(f"  {dt:<6} : {count}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="APL DuckDB query and export tool.")
    parser.add_argument("--summary",      action="store_true", help="Show run summary.")
    parser.add_argument("--exceptions",   action="store_true", help="Show recent exceptions.")
    parser.add_argument("--failed",       action="store_true", help="Show failed files.")
    parser.add_argument("--completeness", action="store_true", help="Show tracker completeness summary.")
    parser.add_argument("--refresh-na",   action="store_true",
                        help="Reload not_available from wide-format CSV.")
    parser.add_argument("--export",       metavar="TABLE",     help="Export a table to CSV.")
    parser.add_argument("--out",          metavar="PATH",      help="Output path for --export.")
    parser.add_argument("--sql",          metavar="SQL",       help="Run ad-hoc SQL query.")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    con = duckdb.connect(str(DB_PATH))

    if args.summary:
        cmd_summary(con)
    if args.exceptions:
        cmd_exceptions(con)
    if args.failed:
        cmd_failed(con)
    if args.completeness:
        cmd_completeness(con)
    if args.refresh_na:
        cmd_refresh_na(con)
    if args.export:
        out = args.out or f"{args.export}_export.csv"
        cmd_export(con, args.export, out)
    if args.sql:
        cmd_sql(con, args.sql)

    con.close()


if __name__ == "__main__":
    main()
