"""
hash_check.py
=============
SHA-256 hashing and deduplication utilities.
Imported by ingest.py — not run directly.
"""

import hashlib
from pathlib import Path


def compute_sha256(path: Path) -> str:
    """Return the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def is_known(con, sha256: str) -> bool:
    """Return True if this hash already exists in hash_registry."""
    row = con.execute(
        "SELECT 1 FROM hash_registry WHERE sha256 = ?", [sha256]
    ).fetchone()
    return row is not None


def register(con, sha256: str, file_id: str, renamed_name: str):
    """
    Insert a new entry into hash_registry.
    Safe to call only after is_known() returns False.
    """
    from datetime import datetime, timezone
    con.execute("""
        INSERT OR IGNORE INTO hash_registry (sha256, first_seen_at, file_id, renamed_name)
        VALUES (?, ?, ?, ?)
    """, [sha256, datetime.now(timezone.utc), file_id, renamed_name])


def lookup(con, sha256: str) -> dict | None:
    """
    Return the hash_registry row for a known hash, or None.
    Returned dict has keys: sha256, first_seen_at, file_id, renamed_name.
    """
    row = con.execute("""
        SELECT sha256, first_seen_at, file_id, renamed_name
        FROM hash_registry WHERE sha256 = ?
    """, [sha256]).fetchone()
    if row is None:
        return None
    return {
        "sha256":        row[0],
        "first_seen_at": row[1],
        "file_id":       row[2],
        "renamed_name":  row[3],
    }
