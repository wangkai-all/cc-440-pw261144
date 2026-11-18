# Architecture

## Why SQLite, and what it buys

The whole system is one SQLite file. That is a deliberate choice for this
problem shape: an hourly batch loader has a single writer, modest volume, and a
hard requirement that "apply one hour" be atomic and idempotent. SQLite gives
us those properties without an external service, and the same code path runs in
tests, in the CLI and behind the dashboard.

The one thing SQLite does not give us is multi-statement atomicity across
*separate* connections, so the engine opens the connection in autocommit mode
and wraps every mutation in an explicit `BEGIN IMMEDIATE … COMMIT`.

## Tables

| Table | Purpose |
|---|---|
| `datasets` | Expected start hour, current head, current watermark. |
| `deliveries` | Immutable record of every file received, including conflicts, with the parsed rows kept for a conflicting delivery so it can be reviewed or accepted later. |
| `batches` | One row per `(dataset, hour)`; the applied file. Status is `pending` or `applied`. |
| `batch_rows` | Normalised rows of each batch. This is the replay log. |
| `records` | Current materialised state, one row per key. Rebuilt inside the merge transaction. |
| `snapshots` | Per-hour full state: counts, content hash, version, publish state, blocker. |
| `conflicts` | Observations and blocking mismatches, with the merge rule that explains each. |
| `merge_runs` | Per-attempt history including failures. |
| `audit_log` | Hash-chained append-only log. |

## The fold

```
state = {}
for batch in applied_batches ordered by (hour, id):
    for row in batch.rows ordered by seq:
        candidate = (row.event_us, batch.hour_us, row.seq)
        if candidate > state[row.key].order:
            state[row.key] = row
```

This is a pure function of the applied batches, which is the property everything
else leans on:

- replaying the same batches produces identical records and snapshots;
- arrival order is irrelevant, so out-of-order and duplicate delivery are
  safe by construction;
- a late hour is handled by re-running the fold from that hour forward, not by
  patching the current table.

Two recompute modes keep this cheap:

- **Append** (`seed_from_records=True`) — the incoming hour is past the current
  head, so the existing materialised records are a valid prefix and the fold
  only processes the new batch.
- **Replay** (`seed_from_records=False`) — the incoming hour is late, so all
  applied batches from the start are folded again and every snapshot from that
  hour onward is rewritten (bumping `version`).

Correctness never depends on choosing the fast path; both produce the same
state. If a snapshot's content hash is unchanged after a recompute, its version
is not bumped, so a retry or a no-op backfill does not create churn.

## Conflict kinds

`content_mismatch` is the only **blocking** kind. Everything else is an
observation recorded at merge time — the rules already resolved it, and the
dashboard shows the observation so nobody has to guess why a row is what it is:

| Kind | Meaning |
|---|---|
| `intra_batch_multi_update` | Same key several times in one file; latest event time (then file order) won. |
| `event_ts_outside_hour` | The row's event time is outside the batch's hour. Allowed — the fold is event-time ordered — but suspicious. |
| `late_event_ignored` | Older than the stored value, so it lost. |
| `out_of_order_arrival` | This hour arrived after a later hour was applied. |
| `delete_missing_key` | Delete for a key never seen. |
| `redundant_delete` | Delete for an already-tombstoned key. |
| `tombstone_applied` | Delete won and the key is tombstoned. |
| `resurrected_key` | Upsert won over a tombstone. |
| `empty_batch` | File had a header but no data rows; the hour is complete. |

## Publication gate

`_refresh_dataset_state` walks from `expected_start`, stopping at the first hour
that is not applied, or is applied but blocked by an unresolved conflict on a
non-published hour. That hour is the new watermark. Every applied snapshot is
then labelled `published` (inside the run) or `blocked` with the reason for the
stop. Publication is idempotent: `published_at` is stamped once and only newly
published hours are emitted in `snapshot.published` audit entries.

The gate is per dataset. The watermark loop, the conflict query and the missing
hour scan are all scoped by `dataset`, so `orders` and `payments` advance
independently.

## Audit chain

```
entry_hash = sha256(canonical_json({
    seq, at, actor, event, dataset, hour, detail_json, prev_hash
}))
prev_hash  = previous entry's entry_hash, or 64 zeros for the genesis entry
```

`verify_chain` iterates in `seq` order and fails on: a sequence gap, a
`prev_hash` mismatch, or a hash mismatch. Uses:

- `incmerge audit` exits 1 when verification fails, so it can run as a health
  check.
- The dashboard badge re-verifies on every load.
- Tests tamper with the table directly and assert the failure is located.

The chain is tamper-*evident*, not tamper-*proof*: an attacker with write access
to the whole database can rebuild it from any point. That is what an external
anchoring scheme (write the head hash somewhere append-only) is for, and it is
out of scope for a local tool.

## Known limits

- Single writer per database. Concurrent merges serialise; the HTTP layer also
  takes a process lock around mutating endpoints.
- Snapshots store a hash and counts, not the full key set, so "diff two hours"
  is not supported — the state is rebuildable from `batch_rows` if you need it.
- The chain covers mutations to *this* database, not the source files.
- Published hours are never retracted; accepting an incoming file for a
  published hour recomputes and republishes, but consumers that already read
  the old version are not notified.
