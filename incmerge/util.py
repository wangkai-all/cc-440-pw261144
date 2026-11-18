"""Small deterministic helpers shared by the merge engine, CLI and web UI."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator


EPOCH = datetime(1970, 1, 1)
HOUR_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[T _](\d{2})(?::(\d{2}))?$")


class IncmergeError(Exception):
    """Base class for user-facing errors."""


class ValidationError(IncmergeError):
    """The input file or request is not valid."""


class DuplicateMismatch(IncmergeError):
    """The same hourly batch was delivered again with different content."""


class TransientMergeError(IncmergeError):
    """A merge attempt failed in a way that is safe to retry."""


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def utcnow_iso() -> str:
    return format_timestamp(utcnow())


def format_timestamp(dt: datetime) -> str:
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        base += f".{dt.microsecond:06d}"
    return base + "Z"


def parse_hour(value: str) -> datetime:
    """Parse an hour label such as ``2026-11-18T04`` into a naive UTC hour.

    Offsets and ``Z`` are accepted and normalised to UTC.  Sub-hour precision is
    rejected rather than silently truncated - a mislabelled batch is a data
    integrity problem, not something to paper over.
    """

    if not isinstance(value, str) or not value.strip():
        raise ValidationError("hour must be a non-empty string")
    raw = value.strip()
    match = HOUR_RE.match(raw)
    if match:
        year, month, day, hour, minute = match.groups()
        if minute not in (None, "00"):
            raise ValidationError(f"hour must be on the hour boundary: {raw!r}")
        try:
            return datetime(int(year), int(month), int(day), int(hour))
        except ValueError as exc:
            raise ValidationError(f"invalid hour {raw!r}: {exc}") from exc
    normalised = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValidationError(
            f"invalid hour {raw!r}; expected YYYY-MM-DDTHH"
        ) from exc
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    if dt.minute or dt.second or dt.microsecond:
        raise ValidationError(f"hour must be on the hour boundary: {raw!r}")
    return dt.replace(minute=0, second=0, microsecond=0)


def format_hour(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H")


def add_hours(dt: datetime, hours: int) -> datetime:
    return dt + timedelta(hours=hours)


def hour_range(start: datetime, end: datetime) -> list[datetime]:
    if end < start:
        return []
    count = int((end - start).total_seconds() // 3600) + 1
    return [start + timedelta(hours=i) for i in range(count)]


def hours_between(start: str, end: str) -> list[str]:
    return [format_hour(h) for h in hour_range(parse_hour(start), parse_hour(end))]


def parse_event_ts(value: str) -> datetime:
    """Parse a business timestamp.  Naive values are treated as UTC."""

    if value is None or not str(value).strip():
        raise ValidationError("event_ts is required")
    raw = str(value).strip()
    normalised = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValidationError(
            f"invalid event_ts {raw!r}; expected ISO-8601 timestamp"
        ) from exc
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def datetime_to_us(dt: datetime) -> int:
    delta = dt - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def us_to_datetime(value: int) -> datetime:
    return EPOCH + timedelta(microseconds=value)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def short_hash(value: str | None, length: int = 12) -> str:
    return (value or "")[:length]


@contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = True) -> Iterator[sqlite3.Connection]:
    """Explicit transaction helper.

    The connection is opened with ``isolation_level=None`` (autocommit) so that
    we control exactly where a merge commits.  Any exception rolls the whole
    batch back, which is what makes an interrupted merge safe to retry.
    """

    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None
