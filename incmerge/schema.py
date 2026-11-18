"""SQLite schema for the incremental merge engine.

The database is the single source of truth for: delivered batches, their raw
rows, the materialised current table, per-hour snapshots, derived conflict
observations, merge attempts and the tamper-evident audit chain.
"""

from __future__ import annotations

import sqlite3

from .util import connect

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS datasets (
    name            TEXT PRIMARY KEY,
    expected_start  TEXT NOT NULL,
    head_hour       TEXT,
    watermark_hour  TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset       TEXT NOT NULL,
    hour          TEXT NOT NULL,
    filename      TEXT,
    content_hash  TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    outcome       TEXT NOT NULL,
    batch_id      INTEGER,
    note          TEXT,
    rows_json     TEXT,
    received_at   TEXT NOT NULL,
    resolved_at   TEXT,
    resolution    TEXT
);
CREATE INDEX IF NOT EXISTS idx_deliveries_hour ON deliveries(dataset, hour);

CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset       TEXT NOT NULL,
    hour          TEXT NOT NULL,
    hour_us       INTEGER NOT NULL,
    content_hash  TEXT NOT NULL,
    status        TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    delete_count  INTEGER NOT NULL,
    source        TEXT NOT NULL,
    delivery_id   INTEGER,
    first_seen_at TEXT NOT NULL,
    applied_at    TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    needs_replay  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(dataset, hour)
);
CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status, dataset, hour);

CREATE TABLE IF NOT EXISTS batch_rows (
    batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    line_no     INTEGER NOT NULL,
    op          TEXT NOT NULL,
    key         TEXT NOT NULL,
    event_us    INTEGER NOT NULL,
    event_ts    TEXT NOT NULL,
    payload_json TEXT,
    PRIMARY KEY (batch_id, seq)
);

CREATE TABLE IF NOT EXISTS records (
    dataset      TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload_json TEXT,
    deleted      INTEGER NOT NULL DEFAULT 0,
    event_us     INTEGER NOT NULL,
    event_ts     TEXT NOT NULL,
    hour_us      INTEGER NOT NULL,
    batch_id     INTEGER NOT NULL,
    seq          INTEGER NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (dataset, key)
);
CREATE INDEX IF NOT EXISTS idx_records_hour ON records(dataset, hour_us);

CREATE TABLE IF NOT EXISTS snapshots (
    dataset       TEXT NOT NULL,
    hour          TEXT NOT NULL,
    hour_us       INTEGER NOT NULL,
    version       INTEGER NOT NULL DEFAULT 1,
    key_count     INTEGER NOT NULL,
    active_count  INTEGER NOT NULL,
    deleted_count INTEGER NOT NULL,
    content_hash  TEXT NOT NULL,
    max_event_us  INTEGER,
    state         TEXT NOT NULL,
    blocked_by    TEXT,
    computed_at   TEXT NOT NULL,
    recomputed_at TEXT,
    published_at  TEXT,
    last_batch_id INTEGER,
    PRIMARY KEY (dataset, hour)
);

CREATE TABLE IF NOT EXISTS conflicts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset      TEXT NOT NULL,
    hour         TEXT NOT NULL,
    batch_id     INTEGER,
    key          TEXT,
    kind         TEXT NOT NULL,
    detail_json  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    resolution   TEXT,
    resolved_at  TEXT,
    UNIQUE (dataset, hour, batch_id, key, kind, detail_json)
);
CREATE INDEX IF NOT EXISTS idx_conflicts_dataset ON conflicts(dataset, hour);

CREATE TABLE IF NOT EXISTS merge_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL,
    error       TEXT,
    detail_json TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    at          TEXT NOT NULL,
    actor       TEXT NOT NULL,
    event       TEXT NOT NULL,
    dataset     TEXT,
    hour        TEXT,
    detail_json TEXT NOT NULL,
    prev_hash   TEXT NOT NULL,
    entry_hash  TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_audit_at ON audit_log(at);
"""


def init_db(path: str) -> sqlite3.Connection:
    conn = connect(path)
    conn.executescript(DDL)
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES ('created_at', datetime('now'))"
    )
    return conn


def table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]
