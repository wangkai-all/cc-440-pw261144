# Runbook

## Hourly ingest (normal path)

```bash
python3 -m incmerge --db prod.db ingest orders 2026-11-18T04 /inbox/orders-2026-11-18T04.csv
```

Exit codes: `0` merged (or duplicate), `2` blocked/conflict or invalid input.
`ingest` delivers and merges in one command; the two steps are separate
endpoints (`deliver`, `merge`) if your scheduler wants to stage files first.

A duplicate file is not an error — it exits `0` and reports
`"outcome": "duplicate"`.

## When a batch is missing

```bash
python3 -m incmerge --db prod.db status
python3 -m incmerge --db prod.db timeline orders
```

`missing_hours` lists gaps between the expected start and the head. Later hours
are already applied but hold `state: blocked` with `blocked_by: missing:<hour>`.
Downstream consumers should read only published snapshots — the watermark tells
them what is safe.

Fill the gap when the file turns up:

```bash
python3 -m incmerge --db prod.db backfill orders 2026-11-18T02 /backlog/orders-2026-11-18T02.csv
```

The response shows `published` (newly published hours) and
`recomputed_snapshots` (hours whose content changed, i.e. version bumped).
A backfill of a *late* hour rewrites every snapshot after it, which is expected.

Only the affected dataset is blocked. If `payments` is complete it keeps
publishing while `orders` waits.

## When the same hour is delivered twice with different content

```bash
python3 -m incmerge --db prod.db conflicts payments
```

You will see an `unresolved` entry with `existing_hash` and `content_hash`, plus
observation entries. Pick a version:

```bash
# The already-applied file is correct; discard the re-delivery.
python3 -m incmerge --db prod.db resolve <delivery_id> keep_existing

# The re-delivery is correct; replace the batch and recompute from that hour.
python3 -m incmerge --db prod.db resolve <delivery_id> accept_incoming
```

If the hour was not yet published, publication is withheld until you resolve —
that is the blocking case. If it was already published, the watermark does not
retract; resolve it before the next snapshot for that hour is consumed.

## When a merge fails

```bash
python3 -m incmerge --db prod.db runs --limit 10
python3 -m incmerge --db prod.db merge <batch_id>
```

A failed merge left no partial state: the transaction rolled back. Re-run the
same `merge`; the outcome is `retried` on success. `attempts` on the batch and
the `merge_runs` rows show the whole history, and each failure is in the audit
chain as `merge.failed`.

## Audit verification

```bash
python3 -m incmerge --db prod.db audit --limit 20
echo $?    # 1 if the chain does not verify
```

A `broken_at` value points at the first entry whose hash or link does not match.
Treat that as a security incident: the database was modified outside the tool.
Preserve it and restore from a known-good copy — do not "fix" the chain.

Run `incmerge audit` from cron/health checks; it is cheap for local-scale data.

Recovery actions are attributed: the CLI's `--actor` and the dashboard's
*Actor* field are written into every entry, so `conflict.resolved` and
`merge.applied` record who did it.

## Dashboard

```bash
python3 -m incmerge --db prod.db serve --port 8765
```

Read-only by default in spirit — the only buttons are the documented recovery
actions (apply/retry a pending batch, backfill an hour, resolve a mismatch).
Every one of them writes to the audit chain with the actor from the header
field. Bind to `127.0.0.1` unless you have a reason not to; there is no auth.

## Verifying a deployment

```bash
python3 -I scripts/verify.py          # samples + failure injection + audit chain
python3 -m unittest discover -s tests -t tests
```
