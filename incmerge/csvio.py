"""CSV parsing and normalisation.

A batch is an hourly change file.  Required columns are ``key`` and
``event_ts``; ``op`` is optional and accepts ``upsert`` (default), ``insert``,
``update`` and ``delete``.  Every other column becomes part of the row payload.

Parsing is strict and all-or-nothing: one malformed row rejects the whole file,
because a partially applied batch would break the "one hour = one atomic
transaction" invariant the merge engine relies on.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import Any

from .util import (
    ValidationError,
    canonical_json,
    datetime_to_us,
    parse_event_ts,
    sha256_hex,
)

KEY_COLUMN = "key"
OP_COLUMN = "op"
TS_COLUMN = "event_ts"
RESERVED = {KEY_COLUMN, OP_COLUMN, TS_COLUMN}
DELETE_OPS = {"delete", "del", "d", "remove", "tombstone"}
UPSERT_OPS = {"upsert", "insert", "update", "i", "u", "set"}


@dataclass
class Row:
    seq: int
    line_no: int
    op: str
    key: str
    event_us: int
    event_ts: str
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def deleted(self) -> bool:
        return self.op == "delete"

    def to_json(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "line_no": self.line_no,
            "op": self.op,
            "key": self.key,
            "event_ts": self.event_ts,
            "event_us": self.event_us,
            "payload": self.payload,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Row":
        return cls(
            seq=int(data["seq"]),
            line_no=int(data["line_no"]),
            op=str(data["op"]),
            key=str(data["key"]),
            event_us=int(data["event_us"]),
            event_ts=str(data["event_ts"]),
            payload=dict(data.get("payload") or {}),
        )


@dataclass
class ParsedBatch:
    rows: list[Row]
    content_hash: str
    delete_count: int
    empty_file: bool = False

    @property
    def row_count(self) -> int:
        return len(self.rows)


def _normalise_op(raw: str, line_no: int) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return "upsert"
    if value in DELETE_OPS:
        return "delete"
    if value in UPSERT_OPS:
        return "upsert"
    raise ValidationError(
        f"line {line_no}: unsupported op {raw!r}; expected upsert or delete"
    )


def batch_content_hash(rows: list[Row]) -> str:
    """Hash the normalised rows in file order.

    File order is part of the identity because equal ``event_ts`` values are
    broken by position; two files with rows in a different order are therefore
    different batches and must be reviewed as a conflict rather than silently
    accepted as a duplicate.
    """

    material = [
        [row.op, row.key, row.event_us, row.payload]
        for row in rows
    ]
    return sha256_hex(canonical_json(material))


def parse_csv_bytes(data: bytes, filename: str | None = None) -> ParsedBatch:
    if not data.strip():
        empty_hash = batch_content_hash([])
        return ParsedBatch(rows=[], content_hash=empty_hash, delete_count=0, empty_file=True)
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"{filename or 'file'} is not valid UTF-8: {exc}") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        raw_header = next(reader)
    except StopIteration:
        raise ValidationError(f"{filename or 'file'} is empty")
    header = [h.strip().lower() for h in raw_header]
    if not any(h for h in header):
        raise ValidationError(f"{filename or 'file'} has a blank header")

    duplicates = {h for h in header if h and header.count(h) > 1}
    if duplicates:
        raise ValidationError(
            f"duplicate column(s) in header: {', '.join(sorted(duplicates))}"
        )
    for required in (KEY_COLUMN, TS_COLUMN):
        if required not in header:
            raise ValidationError(f"missing required column {required!r}")

    key_idx = header.index(KEY_COLUMN)
    ts_idx = header.index(TS_COLUMN)
    op_idx = header.index(OP_COLUMN) if OP_COLUMN in header else None
    payload_idx = [
        (idx, name)
        for idx, name in enumerate(header)
        if name and name not in RESERVED
    ]

    rows: list[Row] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(not cell.strip() for cell in raw):
            continue  # blank line, not a data row
        if len(raw) != len(header):
            raise ValidationError(
                f"line {line_no}: expected {len(header)} columns, found {len(raw)}"
            )
        key = raw[key_idx].strip()
        if not key:
            raise ValidationError(f"line {line_no}: key is empty")
        ts_raw = raw[ts_idx].strip()
        if not ts_raw:
            raise ValidationError(f"line {line_no}: event_ts is empty")
        try:
            event_ts = parse_event_ts(ts_raw)
        except ValidationError as exc:
            raise ValidationError(f"line {line_no}: {exc}") from exc
        op = _normalise_op(raw[op_idx] if op_idx is not None else "", line_no)
        payload = {
            name: raw[idx].strip()
            for idx, name in payload_idx
        }
        if op == "delete":
            payload = {}
        rows.append(
            Row(
                seq=len(rows),
                line_no=line_no,
                op=op,
                key=key,
                event_us=datetime_to_us(event_ts),
                event_ts=ts_raw,
                payload=payload,
            )
        )

    return ParsedBatch(
        rows=rows,
        content_hash=batch_content_hash(rows),
        delete_count=sum(1 for row in rows if row.deleted),
        empty_file=not rows,
    )
