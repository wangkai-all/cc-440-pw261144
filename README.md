# incmerge

Builds an incremental table from **hourly CSV change files**: duplicates,
out-of-order arrival, delete markers, repeated keys within a batch and empty
files are all handled by one deterministic fold. Merges are transactional and
retryable, a missing hour blocks downstream publication **only for its own
dataset**, and filling the gap automatically recomputes every affected hourly
snapshot. A local dashboard shows the batch dependency chain, conflicts, the
current watermark, and lets you backfill / resolve.

Pure Python 3.11 standard library — no dependencies, no build step.

```bash
python3 -I scripts/demo.py          # end-to-end scenario with an injected failure
python3 -I scripts/verify.py        # asserts the invariants, exits non-zero on violation
python3 -m unittest discover -s tests -t tests
python3 -m incmerge --db local.db init orders --start 2026-11-18T00
python3 -m incmerge --db local.db serve --port 8765

# a dense day of sample data for looking at the dashboard
python3 -I scripts/seed_dashboard.py --db dashboard.db   # 24h, a gap at 06, an open mismatch
python3 -m incmerge --db dashboard.db serve --port 8800
```

## Model

| Concept | Definition |
|---|---|
| **Batch** | Exactly one hour of one dataset. Identified by `(dataset, hour)`, content-hashed over its normalised rows. |
| **Delivery** | A file handed to the system. Recorded with its hash; may be `accepted`, `duplicate`, or `conflict`. |
| **Record** | The current materialised row per key, folded from every applied batch. |
| **Snapshot** | The full key state at the end of an hour — the publishable unit. Append-versioned: later backfills bump `version`, never rewrite history. |
| **Watermark** | The last hour in the contiguous, conflict-free run from the dataset's expected start. |

### Merge rules (deterministic, replayable)

Rows are folded in `(hour, file position)` order, and each row wins only if
`(event_ts, hour, seq)` is greater than the current value's. Consequences:

- **Duplicates** — a re-delivered file whose hash matches is recorded as
  `duplicate` and merges as a `noop`; snapshots are untouched.
- **Out-of-order** — a late hour is folded against the already-materialised
  state; a row whose event time is older than the stored value is ignored and
  surfaced as `late_event_ignored`.
- **Same key, many rows in one file** — the latest `event_ts` (ties broken by
  file position) wins; the batch is still applied atomically.
- **Deletes** — `op=delete` writes a tombstone that participates in the same
  event-time ordering, so an older delete cannot remove a newer upsert, and a
  later upsert resurrects a tombstone (both are reported).
- **Empty file** (0 bytes or header only) — completes its hour. An hour with no
  changes is not the same as a missing hour, so the watermark advances.
- **Same hour, different content** — a `content_mismatch` conflict. The applied
  batch is not touched; if the hour is not yet published, publication is
  withheld from there until an operator picks a version.

### Failure and retry semantics

One batch = one SQLite transaction containing the batch status change, the
record rebuild, the snapshot writes, the conflict observations and the audit
entries. If anything fails, everything rolls back; retrying the same batch id
is always safe and produces `outcome: "retried"`. Nothing is partially applied,
so there is no repair script to write.

`attempts` counts every attempt including the rolled-back ones, and
`merge_runs` keeps the per-attempt history with its error.

### Publication gate

For each dataset the watermark walks forward hour by hour from
`expected_start` and stops at the first hour that is missing, still pending, or
blocked by an unresolved conflict *on a not-yet-published hour*. Snapshots past
the stop are materialised but marked `blocked` with a `blocked_by` reason
(`missing:…`, `conflict:…`); they publish automatically once the gate clears.
Other datasets are untouched — a gap in `orders` never blocks `payments`.

A conflict against an **already published** hour is kept for review but does
not retract the watermark: downstream consumers have seen that version, and
retracting published data is a different (and much more dangerous) operation.

## Dashboard

`python3 -m incmerge --db local.db serve` serves one page:

- **Watermark hero + KPI tiles** — head, applied hours, published/blocked
  snapshots, missing hours, open conflicts, active records.
- **Batch dependency chain** — one column per hour, height proportional to the
  active key count, coloured by state (published / applied-but-withheld /
  missing / conflict / pending). State is also spelled out in the legend and in
  each column's accessible label, so colour is never the only channel.
- **Watchlist** — blocking mismatches first, each with *Keep applied batch*
  (discard the re-delivery) and *Review incoming* (a dialog showing both hashes
  before applying it), then the non-blocking observations that explain why a
  value changed.
- **Hour detail** — batch id, source file, row/delete counts, snapshot version,
  active/tombstoned counts, blocker, the recovery action for that state
  (apply a pending batch, backfill a missing hour, re-deliver, review a
  mismatch), plus the observations recorded for that hour.
- **Audit chain** — the hash-linked log with a filter box, plus a badge showing
  live verification status.
- **Materialised records** — the current table.

Everything the page shows is also available over JSON:
`GET /api/state?dataset=…`, `GET /api/audit`, and `POST /api/merge`,
`/api/deliver`, `/api/backfill`, `/api/resolve`.

## Audit chain

Every mutation appends an entry containing the previous entry's hash and its own
SHA-256 over `(seq, at, actor, event, dataset, hour, detail, prev_hash)`.
`verify_chain` recomputes the whole chain and checks for sequence gaps, so
editing, deleting or reordering history is detectable. The actor comes from
`--actor` on the CLI or the *Actor* field in the dashboard, so recovery actions
are attributable.

## Layout

```
incmerge/
  csvio.py    strict CSV parsing + content hashing
  engine.py   deliver, merge, recompute, publish, resolve
  audit.py    hash-chained append-only log
  schema.py   SQLite DDL
  web.py      stdlib HTTP API + static server
  cli.py      incmerge <command>
web/          dashboard (vanilla HTML/CSS/JS)
samples/      hourly inbox, backlog (the late hour) and conflicting re-delivery
scripts/      demo.py, verify.py, seed_dashboard.py
tests/        unittest suite (68 tests)
docs/         architecture and runbook
```

See [docs/architecture.md](docs/architecture.md) for the reasoning and
[docs/runbook.md](docs/runbook.md) for the operator procedure.
