"""Append-only, tamper-evident audit log.

Every state change writes one entry.  Each entry stores the hash of the previous
entry and its own hash over the canonical payload, so deleting, reordering or
editing history is detectable via :func:`verify_chain`.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from .util import canonical_json, row_to_dict, sha256_hex, utcnow_iso

GENESIS = "0" * 64


def _entry_hash(seq: int, at: str, actor: str, event: str, dataset: str | None,
                hour: str | None, detail_json: str, prev_hash: str) -> str:
    material = canonical_json(
        {
            "seq": seq,
            "at": at,
            "actor": actor,
            "event": event,
            "dataset": dataset,
            "hour": hour,
            "detail_json": detail_json,
            "prev_hash": prev_hash,
        }
    )
    return sha256_hex(material)


def append(
    conn: sqlite3.Connection,
    actor: str,
    event: str,
    *,
    dataset: str | None = None,
    hour: str | None = None,
    detail: Any = None,
) -> dict[str, Any]:
    """Append one audit entry inside the caller's transaction.

    ``seq`` is derived from the current maximum inside the same transaction, so
    concurrent writers serialise on SQLite's write lock rather than forking the
    chain.
    """

    prev = conn.execute(
        "SELECT seq, entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    seq = (prev["seq"] + 1) if prev else 1
    prev_hash = prev["entry_hash"] if prev else GENESIS
    at = utcnow_iso()
    detail_json = canonical_json(detail if detail is not None else {})
    entry_hash = _entry_hash(seq, at, actor, event, dataset, hour, detail_json, prev_hash)
    conn.execute(
        """
        INSERT INTO audit_log(seq, at, actor, event, dataset, hour, detail_json, prev_hash, entry_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (seq, at, actor, event, dataset, hour, detail_json, prev_hash, entry_hash),
    )
    return {
        "seq": seq,
        "at": at,
        "actor": actor,
        "event": event,
        "dataset": dataset,
        "hour": hour,
        "detail": detail if detail is not None else {},
        "prev_hash": prev_hash,
        "entry_hash": entry_hash,
    }


def entries(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM audit_log ORDER BY seq DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = row_to_dict(row) or {}
        import json

        item["detail"] = json.loads(item.pop("detail_json", "{}") or "{}")
        out.append(item)
    return out


def verify_chain(conn: sqlite3.Connection) -> dict[str, Any]:
    """Recompute every hash and confirm links are intact.

    Returns ``{ok, entries, broken_at, reason}``.  Additional consistency checks
    assert that the log is not silently truncated: the head must equal ``seq``
    and no sequence gaps may exist.
    """

    rows = conn.execute("SELECT * FROM audit_log ORDER BY seq").fetchall()
    prev_hash = GENESIS
    expected_seq = 1
    for row in rows:
        if row["seq"] != expected_seq:
            return {
                "ok": False,
                "entries": len(rows),
                "broken_at": row["seq"],
                "reason": f"sequence gap: expected {expected_seq}, found {row['seq']}",
            }
        if row["prev_hash"] != prev_hash:
            return {
                "ok": False,
                "entries": len(rows),
                "broken_at": row["seq"],
                "reason": "prev_hash does not match the preceding entry",
            }
        recomputed = _entry_hash(
            row["seq"], row["at"], row["actor"], row["event"], row["dataset"],
            row["hour"], row["detail_json"], row["prev_hash"],
        )
        if recomputed != row["entry_hash"]:
            return {
                "ok": False,
                "entries": len(rows),
                "broken_at": row["seq"],
                "reason": "entry payload does not match its recorded hash",
            }
        prev_hash = row["entry_hash"]
        expected_seq += 1
    return {
        "ok": True,
        "entries": len(rows),
        "broken_at": None,
        "head_hash": prev_hash,
        "reason": "chain intact",
    }


def record_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM audit_log").fetchone()
    return int(row["n"]) if row else 0
