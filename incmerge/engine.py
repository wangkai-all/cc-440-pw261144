"""The merge engine.

Design invariants
-----------------
1. **One hour = one atomic transaction.**  Registering and applying a batch,
   writing snapshots, conflict observations and audit entries all commit
   together or not at all.  A crash therefore leaves the previous consistent
   state and the merge can simply be retried.
2. **Materialisation is a pure function of the applied batches.**  Records are
   folded in ``(hour, file position)`` order with an event-time last-writer-wins
   comparison ``(event_ts, hour, seq)``.  Replaying the same batches in any
   arrival order produces the same table, which is what makes duplicate
   deliveries and out-of-order hours safe.
3. **A gap blocks only its own partition.**  The published watermark is the
   contiguous run from the dataset's expected start.  Hours after a gap are
   materialised but withheld; other datasets publish independently.
4. **Every mutation is audited in the same transaction** via
   :mod:`incmerge.audit`, giving a hash-chained trail of recovery actions.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Callable

from . import audit
from .csvio import ParsedBatch, Row, parse_csv_bytes
from .schema import init_db
from .util import (
    TransientMergeError,
    ValidationError,
    add_hours,
    canonical_json,
    datetime_to_us,
    format_hour,
    hour_range,
    parse_hour,
    row_to_dict,
    sha256_hex,
    transaction,
    us_to_datetime,
    utcnow_iso,
)

APPLIED = "applied"
PENDING = "pending"

# Conflict kinds.  ``content_mismatch`` is the only blocking kind; everything
# else is an observation that the deterministic merge already resolved.
CONFLICT_RULES = {
    "content_mismatch": "same hour re-delivered with different content; publish withheld until resolved",
    "intra_batch_multi_update": "same key touched several times in one batch; latest event_ts (then file order) wins",
    "event_ts_outside_hour": "event_ts falls outside the batch's hour; kept for event-time ordering",
    "late_event_ignored": "row is older than the value already materialised; ignored",
    "out_of_order_arrival": "hour arrived after later hours were already applied",
    "delete_missing_key": "delete marker for a key with no materialised row",
    "redundant_delete": "delete marker for a key that is already tombstoned",
    "tombstone_applied": "delete marker won and the key is now tombstoned",
    "resurrected_key": "upsert won over an existing tombstone",
    "empty_batch": "batch contained a header but no data rows; hour is still complete",
}


@dataclass(frozen=True)
class RecordState:
    payload: dict[str, Any] | None
    deleted: bool
    event_us: int
    hour_us: int
    seq: int
    batch_id: int
    event_ts: str

    @property
    def order(self) -> tuple[int, int, int]:
        return (self.event_us, self.hour_us, self.seq)


def _wins(candidate: Row, hour_us: int, existing: RecordState | None) -> bool:
    if existing is None:
        return True
    return (candidate.event_us, hour_us, candidate.seq) > existing.order


def _snapshot_fingerprint(state: dict[str, RecordState]) -> tuple[str, int, int, int, int | None]:
    material = []
    active = deleted = 0
    max_event = None
    for key in sorted(state):
        rec = state[key]
        if rec.deleted:
            deleted += 1
        else:
            active += 1
        if max_event is None or rec.event_us > max_event:
            max_event = rec.event_us
        material.append([key, 1 if rec.deleted else 0, rec.payload])
    digest = sha256_hex(canonical_json(material))
    return digest, len(state), active, deleted, max_event


class Engine:
    def __init__(
        self,
        db_path: str,
        *,
        actor: str = "cli",
        clock: Callable[[], str] = utcnow_iso,
        fault: str | None = None,
    ) -> None:
        self.db_path = db_path
        self.actor = actor
        self._clock = clock
        self.fault = fault
        self.conn = init_db(db_path)

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ setup
    def ensure_dataset(self, name: str, expected_start: str) -> dict[str, Any]:
        start = format_hour(parse_hour(expected_start))
        now = self._clock()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM datasets WHERE name=?", (name,)
            ).fetchone()
            if row is None:
                self.conn.execute(
                    """
                    INSERT INTO datasets(name, expected_start, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (name, start, now, now),
                )
                audit.append(
                    self.conn,
                    self.actor,
                    "dataset.created",
                    dataset=name,
                    detail={"expected_start": start},
                )
            elif row["expected_start"] != start:
                raise ValidationError(
                    f"dataset {name!r} already starts at {row['expected_start']}, not {start}"
                )
        return self.dataset_info(name)

    def dataset_info(self, name: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM datasets WHERE name=?", (name,)).fetchone()
        if row is None:
            raise ValidationError(f"unknown dataset {name!r}")
        return row_to_dict(row) or {}

    def datasets(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM datasets ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- ingestion
    def deliver_bytes(
        self,
        dataset: str,
        hour: str,
        data: bytes,
        *,
        filename: str | None = None,
        actor: str | None = None,
    ) -> dict[str, Any]:
        """Register an hourly file without applying it.

        Returns a delivery record whose ``outcome`` is ``accepted``,
        ``duplicate`` or ``conflict``.  Nothing about the materialised table
        changes until :meth:`merge` runs.
        """

        who = actor or self.actor
        info = self.dataset_info(dataset)
        hour_dt = parse_hour(hour)
        hour_label = format_hour(hour_dt)
        start = parse_hour(info["expected_start"])
        if hour_dt < start:
            raise ValidationError(
                f"batch hour {hour_label} precedes dataset start {info['expected_start']}"
            )
        parsed = parse_csv_bytes(data, filename)
        hour_us = datetime_to_us(hour_dt)
        now = self._clock()

        with transaction(self.conn):
            existing = self.conn.execute(
                "SELECT * FROM batches WHERE dataset=? AND hour=?",
                (dataset, hour_label),
            ).fetchone()

            if existing and existing["content_hash"] == parsed.content_hash:
                delivery_id = self._insert_delivery(
                    dataset, hour_label, filename, parsed, "duplicate",
                    batch_id=existing["id"], rows_payload=None, now=now,
                )
                audit.append(
                    self.conn, who, "delivery.duplicate",
                    dataset=dataset, hour=hour_label,
                    detail={
                        "delivery_id": delivery_id,
                        "batch_id": existing["id"],
                        "content_hash": parsed.content_hash,
                        "row_count": parsed.row_count,
                    },
                )
                return {
                    "delivery_id": delivery_id,
                    "outcome": "duplicate",
                    "batch_id": existing["id"],
                    "hour": hour_label,
                    "content_hash": parsed.content_hash,
                    "row_count": parsed.row_count,
                    "duplicate_of": existing["id"],
                }

            if existing:
                delivery_id = self._insert_delivery(
                    dataset, hour_label, filename, parsed, "conflict",
                    batch_id=existing["id"],
                    rows_payload=[row.to_json() for row in parsed.rows],
                    now=now,
                )
                detail = {
                    "delivery_id": delivery_id,
                    "existing_batch_id": existing["id"],
                    "existing_hash": existing["content_hash"],
                    "incoming_hash": parsed.content_hash,
                    "existing_rows": existing["row_count"],
                    "incoming_rows": parsed.row_count,
                    "filename": filename,
                }
                self._add_conflict(
                    dataset, hour_label, existing["id"], None, "content_mismatch", detail, now
                )
                audit.append(
                    self.conn, who, "delivery.conflict",
                    dataset=dataset, hour=hour_label, detail=detail,
                )
                # A conflict on an hour whose snapshot is not yet published must
                # withhold publication immediately; one on an already-published
                # hour is held for review without retracting the watermark.
                self._refresh_dataset_state(dataset)
                return {
                    "delivery_id": delivery_id,
                    "outcome": "conflict",
                    "batch_id": existing["id"],
                    "hour": hour_label,
                    "content_hash": parsed.content_hash,
                    "row_count": parsed.row_count,
                    "existing_hash": existing["content_hash"],
                }

            cur = self.conn.execute(
                """
                INSERT INTO batches(dataset, hour, hour_us, content_hash, status,
                                    row_count, delete_count, source, delivery_id,
                                    first_seen_at, attempts)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'file', NULL, ?, 0)
                """,
                (
                    dataset, hour_label, hour_us, parsed.content_hash, PENDING,
                    parsed.row_count, parsed.delete_count, now,
                ),
            )
            batch_id = int(cur.lastrowid)
            for row in parsed.rows:
                self.conn.execute(
                    """
                    INSERT INTO batch_rows(batch_id, seq, line_no, op, key, event_us, event_ts, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id, row.seq, row.line_no, row.op, row.key,
                        row.event_us, row.event_ts, canonical_json(row.payload),
                    ),
                )
            delivery_id = self._insert_delivery(
                dataset, hour_label, filename, parsed, "accepted",
                batch_id=batch_id, rows_payload=None, now=now,
            )
            self.conn.execute(
                "UPDATE batches SET delivery_id=? WHERE id=?", (delivery_id, batch_id)
            )
            audit.append(
                self.conn, who, "delivery.received",
                dataset=dataset, hour=hour_label,
                detail={
                    "delivery_id": delivery_id,
                    "batch_id": batch_id,
                    "filename": filename,
                    "content_hash": parsed.content_hash,
                    "row_count": parsed.row_count,
                    "delete_count": parsed.delete_count,
                    "empty_file": parsed.empty_file,
                },
            )
            if parsed.empty_file:
                self._add_conflict(
                    dataset, hour_label, batch_id, None, "empty_batch",
                    {"filename": filename}, now,
                )
            return {
                "delivery_id": delivery_id,
                "outcome": "accepted",
                "batch_id": batch_id,
                "hour": hour_label,
                "content_hash": parsed.content_hash,
                "row_count": parsed.row_count,
                "delete_count": parsed.delete_count,
                "empty_file": parsed.empty_file,
            }

    def deliver_file(self, dataset: str, hour: str, path: str, **kwargs: Any) -> dict[str, Any]:
        with open(path, "rb") as fh:
            data = fh.read()
        return self.deliver_bytes(dataset, hour, data, filename=path, **kwargs)

    def _insert_delivery(
        self,
        dataset: str,
        hour: str,
        filename: str | None,
        parsed: ParsedBatch,
        outcome: str,
        *,
        batch_id: int | None,
        rows_payload: list[dict[str, Any]] | None,
        now: str,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO deliveries(dataset, hour, filename, content_hash, row_count,
                                   outcome, batch_id, rows_json, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dataset, hour, filename, parsed.content_hash, parsed.row_count,
                outcome, batch_id,
                canonical_json(rows_payload) if rows_payload is not None else None,
                now,
            ),
        )
        return int(cur.lastrowid)

    # ---------------------------------------------------------------- merging
    def merge(self, batch_id: int, *, actor: str | None = None) -> dict[str, Any]:
        """Apply one pending batch.  Safe to call again after any failure.

        Returns a summary; ``outcome`` is ``merged``, ``noop`` or ``retried``
        (a previous attempt failed and this one succeeded).
        """

        who = actor or self.actor
        run_id = self._begin_run(batch_id)
        try:
            with transaction(self.conn):
                result = self._merge_locked(batch_id, who)
                if self.fault == "before_commit":
                    raise TransientMergeError(
                        "injected fault before commit (transaction rolled back)"
                    )
                self.conn.execute(
                    """
                    UPDATE merge_runs SET status='succeeded', finished_at=?, detail_json=?
                    WHERE id=?
                    """,
                    (self._clock(), canonical_json(result), run_id),
                )
            return result
        except BaseException as exc:
            self._fail_run(run_id, batch_id, who, exc)
            raise

    def _fail_run(self, run_id: int, batch_id: int, who: str, exc: BaseException) -> None:
        """Record a failed attempt outside the rolled-back transaction.

        ``attempts`` counts every attempt, including ones that rolled back, so
        the retry that follows is visibly attempt 2 in the run history.
        """

        try:
            with transaction(self.conn):
                self.conn.execute(
                    "UPDATE batches SET attempts=attempts+1, last_error=? "
                    "WHERE id=? AND status=?",
                    (str(exc), batch_id, PENDING),
                )
                audit.append(
                    self.conn, who, "merge.failed",
                    dataset=self._dataset_of_batch(batch_id),
                    hour=self._hour_of_batch(batch_id),
                    detail={"batch_id": batch_id, "run_id": run_id, "error": str(exc)},
                )
        except sqlite3.Error:
            pass
        try:
            with transaction(self.conn):
                self.conn.execute(
                    "UPDATE merge_runs SET status='failed', finished_at=?, error=? WHERE id=?",
                    (self._clock(), str(exc), run_id),
                )
        except sqlite3.Error:
            pass

    def merge_delivery(self, delivery_id: int, *, actor: str | None = None) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM deliveries WHERE id=?", (delivery_id,)
        ).fetchone()
        if row is None:
            raise ValidationError(f"unknown delivery {delivery_id}")
        if not row["batch_id"]:
            raise ValidationError("delivery has no batch to merge")
        return self.merge(int(row["batch_id"]), actor=actor)

    def merge_hour(self, dataset: str, hour: str, *, actor: str | None = None) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT id FROM batches WHERE dataset=? AND hour=?",
            (dataset, format_hour(parse_hour(hour))),
        ).fetchone()
        if row is None:
            raise ValidationError(f"no batch for {dataset} {hour}")
        return self.merge(int(row["id"]), actor=actor)

    def backfill(
        self,
        dataset: str,
        hour: str,
        data: bytes,
        *,
        filename: str | None = None,
        actor: str | None = None,
    ) -> dict[str, Any]:
        """Deliver and merge a late/missing hour, recomputing snapshots.

        This is the operation behind the dashboard's "fill gap" button.
        """

        who = actor or self.actor
        delivery = self.deliver_bytes(
            dataset, hour, data, filename=filename, actor=who
        )
        if delivery["outcome"] == "conflict":
            return {
                "outcome": "blocked",
                "reason": "delivery conflicts with the existing batch; resolve it first",
                "delivery": delivery,
            }
        merge_result = self.merge(int(delivery["batch_id"]), actor=who)
        return {"outcome": "backfilled", "delivery": delivery, "merge": merge_result}

    def backfill_file(
        self,
        dataset: str,
        hour: str,
        path: str,
        *,
        actor: str | None = None,
    ) -> dict[str, Any]:
        with open(path, "rb") as fh:
            data = fh.read()
        return self.backfill(dataset, hour, data, filename=path, actor=actor)

    def _merge_locked(self, batch_id: int, who: str) -> dict[str, Any]:
        batch = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if batch is None:
            raise ValidationError(f"unknown batch {batch_id}")
        dataset, hour = batch["dataset"], batch["hour"]
        if batch["status"] == APPLIED:
            result = {
                "outcome": "noop",
                "batch_id": batch_id,
                "dataset": dataset,
                "hour": hour,
                "already_applied_at": batch["applied_at"],
            }
            audit.append(
                self.conn, who, "merge.noop", dataset=dataset, hour=hour,
                detail={
                    "batch_id": batch_id,
                    "attempt": batch["attempts"],
                    "content_hash": batch["content_hash"],
                },
            )
            return result

        attempts = int(batch["attempts"]) + 1
        self.conn.execute(
            "UPDATE batches SET attempts=? WHERE id=?", (attempts, batch_id)
        )
        info = self.dataset_info(dataset)
        prev_head = info["head_hour"]
        prev_head_us = datetime_to_us(parse_hour(prev_head)) if prev_head else None

        rows = self._load_batch_rows(batch_id)
        state_before = self._load_current_state(dataset)

        conflicts = self._observe_batch(batch, rows, state_before)

        now = self._clock()
        self.conn.execute(
            "UPDATE batches SET status=?, applied_at=?, last_error=NULL WHERE id=?",
            (APPLIED, now, batch_id),
        )

        recompute = self._recompute(
            dataset,
            from_hour=hour,
            previous_head_us=prev_head_us,
            seed_from_records=(
                prev_head_us is not None
                and datetime_to_us(parse_hour(hour)) > prev_head_us
            ),
        )
        publish = self._refresh_dataset_state(dataset)
        result = {
            "outcome": "retried" if attempts > 1 else "merged",
            "batch_id": batch_id,
            "dataset": dataset,
            "hour": hour,
            "attempt": attempts,
            "rows": len(rows),
            "deletes": int(batch["delete_count"]),
            "conflict_observations": conflicts,
            "recomputed_snapshots": recompute["changed"],
            "watermark": publish["watermark"],
            "published": publish["newly_published"],
        }
        audit.append(
            self.conn, who, "merge.applied", dataset=dataset, hour=hour,
            detail={
                "batch_id": batch_id,
                "attempt": attempts,
                "content_hash": batch["content_hash"],
                "rows": len(rows),
                "deletes": int(batch["delete_count"]),
                "conflict_observations": conflicts,
                "recompute_from": recompute["from_hour"],
                "changed_snapshots": recompute["changed"],
            },
        )
        if conflicts:
            audit.append(
                self.conn, who, "conflicts.observed", dataset=dataset, hour=hour,
                detail={"batch_id": batch_id, "kinds": conflicts},
            )
        return result

    def _load_batch_rows(self, batch_id: int) -> list[Row]:
        rows = self.conn.execute(
            "SELECT * FROM batch_rows WHERE batch_id=? ORDER BY seq", (batch_id,)
        ).fetchall()
        return [
            Row(
                seq=int(r["seq"]),
                line_no=int(r["line_no"]),
                op=r["op"],
                key=r["key"],
                event_us=int(r["event_us"]),
                event_ts=r["event_ts"],
                payload=json.loads(r["payload_json"] or "{}"),
            )
            for r in rows
        ]

    def _load_current_state(self, dataset: str) -> dict[str, RecordState]:
        rows = self.conn.execute("SELECT * FROM records WHERE dataset=?", (dataset,)).fetchall()
        state: dict[str, RecordState] = {}
        for r in rows:
            state[r["key"]] = RecordState(
                payload=json.loads(r["payload_json"]) if r["payload_json"] else None,
                deleted=bool(r["deleted"]),
                event_us=int(r["event_us"]),
                hour_us=int(r["hour_us"]),
                seq=int(r["seq"]),
                batch_id=int(r["batch_id"]),
                event_ts=r["event_ts"],
            )
        return state

    def _observe_batch(
        self,
        batch: sqlite3.Row,
        rows: list[Row],
        state_before: dict[str, RecordState],
    ) -> dict[str, int]:
        """Derive non-blocking conflict observations for a batch.

        These never change what is applied - the merge rules are deterministic.
        They exist so operators can see *why* a value changed (or did not).
        """

        dataset, hour = batch["dataset"], batch["hour"]
        now = self._clock()
        counts: dict[str, int] = {}
        hour_us = int(batch["hour_us"])
        hour_end = hour_us + 3600 * 1_000_000

        def note(kind: str, key: str | None, detail: dict[str, Any]) -> None:
            counts[kind] = counts.get(kind, 0) + 1
            self._add_conflict(dataset, hour, int(batch["id"]), key, kind, detail, now)

        by_key: dict[str, list[Row]] = {}
        for row in rows:
            by_key.setdefault(row.key, []).append(row)

        for key, occurrences in by_key.items():
            if len(occurrences) > 1:
                winner = max(occurrences, key=lambda r: (r.event_us, r.seq))
                note(
                    "intra_batch_multi_update",
                    key,
                    {
                        "occurrences": [
                            {"seq": r.seq, "op": r.op, "event_ts": r.event_ts}
                            for r in occurrences
                        ],
                        "winner_seq": winner.seq,
                        "winner_op": winner.op,
                    },
                )

        for row in rows:
            if row.event_us < hour_us or row.event_us >= hour_end:
                note(
                    "event_ts_outside_hour",
                    row.key,
                    {
                        "hour": hour,
                        "event_ts": row.event_ts,
                        "seq": row.seq,
                        "op": row.op,
                    },
                )
            existing = state_before.get(row.key)
            if existing is not None and not _wins(row, hour_us, existing):
                note(
                    "late_event_ignored",
                    row.key,
                    {
                        "seq": row.seq,
                        "op": row.op,
                        "event_ts": row.event_ts,
                        "winner_batch_id": existing.batch_id,
                        "winner_event_ts": existing.event_ts,
                        "winner_deleted": existing.deleted,
                    },
                )
                continue
            if row.deleted:
                if existing is None:
                    note(
                        "delete_missing_key",
                        row.key,
                        {"seq": row.seq, "event_ts": row.event_ts},
                    )
                elif existing.deleted:
                    note(
                        "redundant_delete",
                        row.key,
                        {
                            "seq": row.seq,
                            "event_ts": row.event_ts,
                            "tombstone_event_ts": existing.event_ts,
                        },
                    )
                else:
                    note(
                        "tombstone_applied",
                        row.key,
                        {
                            "seq": row.seq,
                            "event_ts": row.event_ts,
                            "prior_event_ts": existing.event_ts,
                        },
                    )
            elif existing is not None and existing.deleted and _wins(row, hour_us, existing):
                note(
                    "resurrected_key",
                    row.key,
                    {
                        "seq": row.seq,
                        "event_ts": row.event_ts,
                        "tombstone_event_ts": existing.event_ts,
                    },
                )

        previous = self.conn.execute(
            "SELECT MAX(hour_us) AS h FROM batches WHERE dataset=? AND status=?",
            (dataset, APPLIED),
        ).fetchone()
        if previous and previous["h"] is not None and hour_us < int(previous["h"]):
            note(
                "out_of_order_arrival",
                None,
                {
                    "hour": hour,
                    "latest_applied_hour": format_hour(us_to_datetime(int(previous["h"]))),
                    "rows": len(rows),
                },
            )
        return counts

    def _add_conflict(
        self,
        dataset: str,
        hour: str,
        batch_id: int | None,
        key: str | None,
        kind: str,
        detail: dict[str, Any],
        now: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO conflicts(dataset, hour, batch_id, key, kind, detail_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (dataset, hour, batch_id, key, kind, canonical_json(detail), now),
        )

    # ------------------------------------------------------------- recompute
    def _recompute(
        self,
        dataset: str,
        *,
        from_hour: str,
        previous_head_us: int | None,
        seed_from_records: bool,
    ) -> dict[str, Any]:
        """Rebuild records and snapshots from ``from_hour`` through the head.

        When the incoming hour is appended after the current head we can seed
        from the materialised records; otherwise (a late or backfilled hour) we
        replay all applied batches so every later snapshot is corrected.  Both
        paths are deterministic and idempotent.
        """

        cutoff_us = datetime_to_us(parse_hour(from_hour))
        batches = self.conn.execute(
            """
            SELECT id, hour, hour_us FROM batches
            WHERE dataset=? AND status=? ORDER BY hour_us, id
            """,
            (dataset, APPLIED),
        ).fetchall()

        state: dict[str, RecordState] = {}
        if seed_from_records:
            state = self._load_current_state(dataset)

        snapshots: list[tuple[str, int, tuple[str, int, int, int, int | None], int]] = []
        for batch in batches:
            hour_us = int(batch["hour_us"])
            if hour_us < cutoff_us:
                if seed_from_records:
                    continue
                self._fold_batch(state, int(batch["id"]), hour_us)
                continue
            self._fold_batch(state, int(batch["id"]), hour_us)
            snapshots.append(
                (
                    batch["hour"],
                    hour_us,
                    _snapshot_fingerprint(state),
                    int(batch["id"]),
                )
            )

        self._replace_records(dataset, state)
        changed: list[str] = []
        now = self._clock()
        for hour_label, hour_us, fingerprint, last_batch_id in snapshots:
            digest, key_count, active, deleted, max_event = fingerprint
            previous = self.conn.execute(
                "SELECT * FROM snapshots WHERE dataset=? AND hour=?",
                (dataset, hour_label),
            ).fetchone()
            if previous is None:
                self.conn.execute(
                    """
                    INSERT INTO snapshots(dataset, hour, hour_us, version, key_count,
                                          active_count, deleted_count, content_hash,
                                          max_event_us, state, computed_at, last_batch_id)
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        dataset, hour_label, hour_us, key_count, active, deleted,
                        digest, max_event, now, last_batch_id,
                    ),
                )
                changed.append(hour_label)
                continue
            if previous["content_hash"] == digest:
                continue
            self.conn.execute(
                """
                UPDATE snapshots
                SET version=version+1, key_count=?, active_count=?, deleted_count=?,
                    content_hash=?, max_event_us=?, recomputed_at=?, last_batch_id=?
                WHERE dataset=? AND hour=?
                """,
                (key_count, active, deleted, digest, max_event, now, last_batch_id,
                 dataset, hour_label),
            )
            changed.append(hour_label)

        if changed:
            audit.append(
                self.conn, self.actor, "snapshot.recomputed",
                dataset=dataset,
                detail={
                    "from_hour": from_hour,
                    "changed": changed,
                    "snapshots": len(snapshots),
                    "seeded_from_records": seed_from_records,
                },
            )
        return {"from_hour": from_hour, "changed": changed, "scanned": len(snapshots)}

    def _fold_batch(self, state: dict[str, RecordState], batch_id: int, hour_us: int) -> None:
        rows = self.conn.execute(
            "SELECT * FROM batch_rows WHERE batch_id=? ORDER BY seq", (batch_id,)
        ).fetchall()
        for r in rows:
            deleted = r["op"] == "delete"
            candidate = RecordState(
                payload=None if deleted else (
                    json.loads(r["payload_json"]) if r["payload_json"] else {}
                ),
                deleted=deleted,
                event_us=int(r["event_us"]),
                hour_us=hour_us,
                seq=int(r["seq"]),
                batch_id=batch_id,
                event_ts=r["event_ts"],
            )
            existing = state.get(r["key"])
            if existing is None or candidate.order > existing.order:
                state[r["key"]] = candidate

    def _replace_records(self, dataset: str, state: dict[str, RecordState]) -> None:
        now = self._clock()
        self.conn.execute("DELETE FROM records WHERE dataset=?", (dataset,))
        payload = [
            (
                dataset, key, canonical_json(rec.payload) if rec.payload is not None else None,
                1 if rec.deleted else 0, rec.event_us, rec.event_ts, rec.hour_us,
                rec.batch_id, rec.seq, now,
            )
            for key, rec in state.items()
        ]
        self.conn.executemany(
            """
            INSERT INTO records(dataset, key, payload_json, deleted, event_us, event_ts,
                                hour_us, batch_id, seq, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    # --------------------------------------------------------- state & publish
    def _refresh_dataset_state(self, dataset: str) -> dict[str, Any]:
        info = self.dataset_info(dataset)
        expected = parse_hour(info["expected_start"])
        applied_rows = self.conn.execute(
            "SELECT * FROM batches WHERE dataset=? AND status=? ORDER BY hour_us",
            (dataset, APPLIED),
        ).fetchall()
        applied = {r["hour"]: r for r in applied_rows}
        conflicted = self._blocking_conflict_hours(dataset)
        pending = self._pending_hours(dataset)

        watermark: str | None = None
        gap: str | None = None
        cursor = expected
        while True:
            label = format_hour(cursor)
            if label not in applied:
                gap = label
                break
            if label in conflicted:
                gap = label
                break
            watermark = label
            cursor = add_hours(cursor, 1)
        head = applied_rows[-1]["hour"] if applied_rows else None
        head_us = int(applied_rows[-1]["hour_us"]) if applied_rows else None

        now = self._clock()
        newly_published: list[str] = []
        block_reason = (
            f"conflict:{gap}" if gap in conflicted else f"missing:{gap}"
        )
        for row in applied_rows:
            label = row["hour"]
            in_run = watermark is not None and parse_hour(label) <= parse_hour(watermark)
            if in_run:
                blocked_by = None
                state = "published"
            else:
                blocked_by = block_reason
                state = "blocked"
            current = self.conn.execute(
                "SELECT state, published_at FROM snapshots WHERE dataset=? AND hour=?",
                (dataset, label),
            ).fetchone()
            if current is None:
                continue
            published_at = current["published_at"]
            if state == "published" and not published_at:
                published_at = now
                newly_published.append(label)
            self.conn.execute(
                "UPDATE snapshots SET state=?, blocked_by=?, published_at=? WHERE dataset=? AND hour=?",
                (state, blocked_by, published_at, dataset, label),
            )

        previous_watermark = info["watermark_hour"]
        self.conn.execute(
            "UPDATE datasets SET head_hour=?, watermark_hour=?, updated_at=? WHERE name=?",
            (head, watermark, now, dataset),
        )
        if watermark != previous_watermark:
            audit.append(
                self.conn, self.actor, "watermark.advanced",
                dataset=dataset,
                detail={
                    "from": previous_watermark,
                    "to": watermark,
                    "head": head,
                    "newly_published": newly_published,
                },
            )
        if newly_published:
            audit.append(
                self.conn, self.actor, "snapshot.published",
                dataset=dataset,
                detail={
                    "hours": newly_published,
                    "watermark": watermark,
                    "count": len(newly_published),
                },
            )
        return {
            "watermark": watermark,
            "head": head,
            "head_us": head_us,
            "pending": pending,
            "conflicted": sorted(conflicted),
            "newly_published": newly_published,
        }

    def _blocking_conflict_hours(self, dataset: str) -> set[str]:
        """Unresolved content mismatches that still withhold publication.

        A mismatch on an hour whose snapshot is already published is retained
        for review but does not retract the downstream watermark.
        """

        rows = self.conn.execute(
            """
            SELECT d.hour AS hour, s.state AS snap_state
            FROM deliveries d
            LEFT JOIN snapshots s ON s.dataset = d.dataset AND s.hour = d.hour
            WHERE d.dataset=? AND d.outcome='conflict' AND d.resolution IS NULL
            """,
            (dataset,),
        ).fetchall()
        return {
            r["hour"]
            for r in rows
            if r["snap_state"] is None or r["snap_state"] != "published"
        }

    def _pending_hours(self, dataset: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT hour FROM batches WHERE dataset=? AND status=? ORDER BY hour",
            (dataset, PENDING),
        ).fetchall()
        return [r["hour"] for r in rows]

    # ------------------------------------------------------------ resolution
    def unresolved_conflicts(self, dataset: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT d.*, b.content_hash AS applied_hash
            FROM deliveries d LEFT JOIN batches b ON b.id = d.batch_id
            WHERE d.outcome='conflict' AND d.resolution IS NULL
        """
        params: list[Any] = []
        if dataset:
            sql += " AND d.dataset=?"
            params.append(dataset)
        sql += " ORDER BY d.hour, d.id"
        rows = self.conn.execute(sql, params).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item.pop("rows_json", None)
            out.append(item)
        return out

    def resolve_conflict(
        self, delivery_id: int, action: str, *, actor: str | None = None
    ) -> dict[str, Any]:
        """Resolve a same-hour content mismatch.

        ``keep_existing`` discards the incoming file; ``accept_incoming``
        replaces the stored batch with it and recomputes from that hour.
        """

        if action not in {"keep_existing", "accept_incoming"}:
            raise ValidationError("action must be keep_existing or accept_incoming")
        who = actor or self.actor
        now = self._clock()
        with transaction(self.conn):
            delivery = self.conn.execute(
                "SELECT * FROM deliveries WHERE id=?", (delivery_id,)
            ).fetchone()
            if delivery is None:
                raise ValidationError(f"unknown delivery {delivery_id}")
            if delivery["outcome"] != "conflict" or delivery["resolution"]:
                raise ValidationError(
                    f"delivery {delivery_id} is not an unresolved conflict"
                )
            dataset, hour = delivery["dataset"], delivery["hour"]
            batch = self.conn.execute(
                "SELECT * FROM batches WHERE dataset=? AND hour=?", (dataset, hour)
            ).fetchone()
            if batch is None:
                raise ValidationError("conflict has no applied batch to resolve against")

            if action == "accept_incoming":
                rows = json.loads(delivery["rows_json"] or "[]")
                parsed_rows = [Row.from_json(item) for item in rows]
                content_hash = sha256_hex(
                    canonical_json(
                        [[r.op, r.key, r.event_us, r.payload] for r in parsed_rows]
                    )
                )
                self.conn.execute("DELETE FROM batch_rows WHERE batch_id=?", (batch["id"],))
                for row in parsed_rows:
                    self.conn.execute(
                        """
                        INSERT INTO batch_rows(batch_id, seq, line_no, op, key, event_us, event_ts, payload_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            batch["id"], row.seq, row.line_no, row.op, row.key,
                            row.event_us, row.event_ts, canonical_json(row.payload),
                        ),
                    )
                self.conn.execute(
                    """
                    UPDATE batches SET content_hash=?, row_count=?, delete_count=?,
                                       status=?, applied_at=?, attempts=attempts+1,
                                       last_error=NULL
                    WHERE id=?
                    """,
                    (
                        content_hash,
                        len(parsed_rows),
                        sum(1 for r in parsed_rows if r.op == "delete"),
                        APPLIED,
                        now,
                        batch["id"],
                    ),
                )
                info = self.dataset_info(dataset)
                previous_head_us = (
                    datetime_to_us(parse_hour(info["head_hour"])) if info["head_hour"] else None
                )
                recompute = self._recompute(
                    dataset,
                    from_hour=hour,
                    previous_head_us=previous_head_us,
                    seed_from_records=False,
                )
            else:
                recompute = {"changed": [], "from_hour": hour, "scanned": 0}

            self.conn.execute(
                "UPDATE deliveries SET resolution=?, resolved_at=? WHERE id=?",
                (action, now, delivery_id),
            )
            self.conn.execute(
                """
                UPDATE conflicts SET resolution=?, resolved_at=?
                WHERE dataset=? AND hour=? AND kind='content_mismatch' AND resolution IS NULL
                """,
                (action, now, dataset, hour),
            )
            publish = self._refresh_dataset_state(dataset)
            audit.append(
                self.conn, who, "conflict.resolved",
                dataset=dataset, hour=hour,
                detail={
                    "delivery_id": delivery_id,
                    "batch_id": batch["id"],
                    "action": action,
                    "changed_snapshots": recompute["changed"],
                    "watermark": publish["watermark"],
                },
            )
            result = {
                "outcome": "resolved",
                "action": action,
                "dataset": dataset,
                "hour": hour,
                "batch_id": batch["id"],
                "recomputed_snapshots": recompute["changed"],
                "watermark": publish["watermark"],
                "published": publish["newly_published"],
            }
        return result

    # --------------------------------------------------------------- reporting
    def status(self, dataset: str | None = None) -> dict[str, Any]:
        datasets = [dataset] if dataset else [d["name"] for d in self.datasets()]
        out = []
        for name in datasets:
            info = self.dataset_info(name)
            conflict_hours = self._blocking_conflict_hours(name)
            applied = self.conn.execute(
                "SELECT hour, row_count FROM batches WHERE dataset=? AND status=?",
                (name, APPLIED),
            ).fetchall()
            total_rows = sum(int(r["row_count"]) for r in applied)
            missing = self.missing_hours(name)
            out.append(
                {
                    "dataset": name,
                    "expected_start": info["expected_start"],
                    "head": info["head_hour"],
                    "watermark": info["watermark_hour"],
                    "applied_hours": len(applied),
                    "rows_delivered": total_rows,
                    "missing_hours": missing,
                    "blocking_conflicts": sorted(conflict_hours),
                    "unresolved_conflicts": len(self.unresolved_conflicts(name)),
                    "published_snapshots": self._count_snapshots(name, "published"),
                    "blocked_snapshots": self._count_snapshots(name, "blocked"),
                    "pending_batches": self._count_batches(name, PENDING),
                    "records": self._count_records(name),
                }
            )
        return {"datasets": out, "audit": audit.verify_chain(self.conn)}

    def _count_snapshots(self, dataset: str, state: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM snapshots WHERE dataset=? AND state=?",
            (dataset, state),
        ).fetchone()
        return int(row["n"]) if row else 0

    def _count_batches(self, dataset: str, status: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM batches WHERE dataset=? AND status=?",
            (dataset, status),
        ).fetchone()
        return int(row["n"]) if row else 0

    def _count_records(self, dataset: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM records WHERE dataset=? AND deleted=0", (dataset,)
        ).fetchone()
        return int(row["n"]) if row else 0

    def missing_hours(self, dataset: str) -> list[str]:
        info = self.dataset_info(dataset)
        if not info["head_hour"]:
            return []
        applied = {
            r["hour"]
            for r in self.conn.execute(
                "SELECT hour FROM batches WHERE dataset=? AND status=?", (dataset, APPLIED)
            ).fetchall()
        }
        return [
            fmt
            for fmt in (
                format_hour(h)
                for h in hour_range(
                    parse_hour(info["expected_start"]), parse_hour(info["head_hour"])
                )
            )
            if fmt not in applied
        ]

    def timeline(self, dataset: str) -> dict[str, Any]:
        """Per-hour batch chain used by the dashboard's dependency view."""

        info = self.dataset_info(dataset)
        head = info["head_hour"]
        start = parse_hour(info["expected_start"])
        # The chain covers everything through the newest delivered hour, even if
        # that hour is still pending and therefore has no applied head yet.
        latest = self.conn.execute(
            "SELECT MAX(hour) AS h FROM batches WHERE dataset=?", (dataset,)
        ).fetchone()
        end_candidates = [start]
        if head:
            end_candidates.append(parse_hour(head))
        if latest and latest["h"]:
            end_candidates.append(parse_hour(latest["h"]))
        end = max(end_candidates)
        hours = hour_range(start, end)
        batches = {
            r["hour"]: dict(r)
            for r in self.conn.execute(
                "SELECT * FROM batches WHERE dataset=?", (dataset,)
            ).fetchall()
        }
        snapshots = {
            r["hour"]: dict(r)
            for r in self.conn.execute(
                "SELECT * FROM snapshots WHERE dataset=?", (dataset,)
            ).fetchall()
        }
        conflict_hours = self._blocking_conflict_hours(dataset)
        entries = []
        for h in hours:
            label = format_hour(h)
            batch = batches.get(label)
            snap = snapshots.get(label, {})
            if batch is None:
                state = "missing"
            elif batch["status"] == PENDING:
                state = "pending"
            elif label in conflict_hours:
                state = "conflict"
            elif snap.get("state") == "blocked":
                state = "blocked"
            else:
                state = "published"
            entries.append(
                {
                    "hour": label,
                    "state": state,
                    "batch_id": batch["id"] if batch else None,
                    "content_hash": batch["content_hash"] if batch else None,
                    "row_count": batch["row_count"] if batch else 0,
                    "delete_count": batch["delete_count"] if batch else 0,
                    "attempts": batch["attempts"] if batch else 0,
                    "filename": self._delivery_filename(batch["id"]) if batch else None,
                    "applied_at": batch["applied_at"] if batch else None,
                    "snapshot_version": snap.get("version"),
                    "active_count": snap.get("active_count"),
                    "deleted_count": snap.get("deleted_count"),
                    "blocked_by": snap.get("blocked_by"),
                }
            )
        return {
            "dataset": dataset,
            "expected_start": info["expected_start"],
            "head": head,
            "watermark": info["watermark_hour"],
            "hours": entries,
        }

    def _delivery_filename(self, batch_id: int | None) -> str | None:
        if batch_id is None:
            return None
        row = self.conn.execute(
            "SELECT filename FROM deliveries WHERE batch_id=? ORDER BY id LIMIT 1",
            (batch_id,),
        ).fetchone()
        return row["filename"] if row else None

    def conflicts(self, dataset: str, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM conflicts WHERE dataset=?
            ORDER BY hour DESC, id DESC LIMIT ?
            """,
            (dataset, limit),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json") or "{}")
            item["rule"] = CONFLICT_RULES.get(item["kind"], "")
            item["blocking"] = item["kind"] == "content_mismatch" and not item["resolution"]
            out.append(item)
        return out

    def audit_trail(self, limit: int = 200) -> list[dict[str, Any]]:
        return audit.entries(self.conn, limit)

    def verify_audit(self) -> dict[str, Any]:
        return audit.verify_chain(self.conn)

    def snapshot(self, dataset: str, hour: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM snapshots WHERE dataset=? AND hour=?",
            (dataset, format_hour(parse_hour(hour))),
        ).fetchone()
        return row_to_dict(row)

    def records(self, dataset: str, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM records WHERE dataset=? ORDER BY key LIMIT ?",
            (dataset, limit),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "null")
            item["deleted"] = bool(item["deleted"])
            out.append(item)
        return out

    def merge_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM merge_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------------------------------------------------------------- run log
    def _begin_run(self, batch_id: int) -> int:
        with transaction(self.conn):
            cur = self.conn.execute(
                "INSERT INTO merge_runs(batch_id, started_at, status) VALUES (?, ?, 'running')",
                (batch_id, self._clock()),
            )
            return int(cur.lastrowid)

    def _complete_run(self, run_id: int, status: str, error: str | None) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE merge_runs SET status=?, finished_at=?, error=? WHERE id=?",
                (status, self._clock(), error, run_id),
            )

    def _dataset_of_batch(self, batch_id: int) -> str | None:
        row = self.conn.execute("SELECT dataset FROM batches WHERE id=?", (batch_id,)).fetchone()
        return row["dataset"] if row else None

    def _hour_of_batch(self, batch_id: int) -> str | None:
        row = self.conn.execute("SELECT hour FROM batches WHERE id=?", (batch_id,)).fetchone()
        return row["hour"] if row else None
