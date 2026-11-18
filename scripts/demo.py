"""End-to-end local scenario for incmerge.

Runs the sample files through the engine in the same order a real ingestion
pipeline would see them, injecting a failure to prove recovery, and prints a
report you can compare against `docs/runbook.md`.

Usage:
    python3 -I scripts/demo.py [--db path] [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from incmerge.engine import Engine  # noqa: E402
from incmerge.util import TransientMergeError  # noqa: E402

INBOX = os.path.join(ROOT, "samples", "inbox")
BACKLOG = os.path.join(ROOT, "samples", "backlog")
CONFLICTS = os.path.join(ROOT, "samples", "conflicts")


def read(*parts: str) -> bytes:
    with open(os.path.join(*parts), "rb") as fh:
        return fh.read()


def line(label: str) -> None:
    print(f"\n=== {label} " + "=" * max(0, 64 - len(label)))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.path.join(tempfile.mkdtemp(), "demo.db"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if os.path.exists(args.db):
        os.remove(args.db)
    engine = Engine(args.db, actor="demo")
    engine.ensure_dataset("orders", "2026-11-18T00")
    engine.ensure_dataset("payments", "2026-11-18T00")
    report: dict = {}

    line("1. hour 00 for both datasets")
    for ds in ("orders", "payments"):
        d = engine.deliver_file(ds, "2026-11-18T00", os.path.join(INBOX, ds, f"{ds}-2026-11-18T00.csv"))
        r = engine.merge(d["batch_id"])
        print(f"{ds}: {d['outcome']} -> {r['outcome']}, watermark {r['watermark']}")

    line("2. duplicate delivery of orders hour 00")
    dup = engine.deliver_file(
        "orders", "2026-11-18T00", os.path.join(INBOX, "orders", "orders-2026-11-18T00.csv")
    )
    print(f"outcome={dup['outcome']} duplicate_of=batch #{dup['batch_id']}")
    again = engine.merge(dup["batch_id"])
    print(f"re-merge -> {again['outcome']} (no snapshot churn)")

    line("3. orders hour 03 arrives while hour 02 is missing")
    d3 = engine.deliver_file("orders", "2026-11-18T03", os.path.join(INBOX, "orders", "orders-2026-11-18T03.csv"))
    r3 = engine.merge(d3["batch_id"])
    st = engine.status("orders")["datasets"][0]
    print(f"merge -> {r3['outcome']} watermark={r3['watermark']} (hour 03 withheld)")
    print(f"missing={st['missing_hours']} blocked_snapshots={st['blocked_snapshots']}")
    print(f"payments watermark unaffected: {engine.status('payments')['datasets'][0]['watermark']}")

    line("4. injected failure on orders hour 01, then retry")
    d1 = engine.deliver_file("orders", "2026-11-18T01", os.path.join(INBOX, "orders", "orders-2026-11-18T01.csv"))
    broken = Engine(args.db, actor="demo", fault="before_commit")
    try:
        broken.merge(d1["batch_id"])
    except TransientMergeError as exc:
        print(f"attempt 1 failed as injected: {exc}")
    finally:
        broken.close()
    after_fail = engine.status("orders")["datasets"][0]
    print(f"after failure: watermark={after_fail['watermark']} pending={after_fail['pending_batches']}")
    retried = engine.merge(d1["batch_id"])
    print(f"retry -> {retried['outcome']} watermark={retried['watermark']} recomputed={retried['recomputed_snapshots']}")
    print(f"observations: {retried['conflict_observations']}")

    line("5. backfill orders hour 02 from the backlog")
    bf = engine.backfill(
        "orders", "2026-11-18T02", read(BACKLOG, "orders", "orders-2026-11-18T02.csv"),
        filename="backlog/orders-2026-11-18T02.csv",
    )
    print(f"backfill -> {bf['outcome']} watermark={bf['merge']['watermark']}")
    print(f"recomputed snapshots: {bf['merge']['recomputed_snapshots']}")
    print(f"published by the backfill: {bf['merge']['published']}")

    line("6. payments hour 01 has repeated keys, a delete and an empty hour 02")
    d = engine.deliver_file("payments", "2026-11-18T01", os.path.join(INBOX, "payments", "payments-2026-11-18T01.csv"))
    r = engine.merge(d["batch_id"])
    print(f"hour 01 -> {r['outcome']} observations={r['conflict_observations']}")
    e = engine.deliver_file("payments", "2026-11-18T02", os.path.join(INBOX, "payments", "payments-2026-11-18T02.csv"))
    er = engine.merge(e["batch_id"])
    print(f"empty hour 02 -> {er['outcome']} watermark={er['watermark']} (empty file still completes the hour)")

    line("7. conflicting re-delivery of payments hour 01 (already published)")
    c = engine.deliver_file(
        "payments", "2026-11-18T01",
        os.path.join(CONFLICTS, "payments", "payments-2026-11-18T01.replayed.csv"),
    )
    print(f"outcome={c['outcome']} existing_hash={c['existing_hash'][:12]} incoming_hash={c['content_hash'][:12]}")
    pst = engine.status("payments")["datasets"][0]
    print(f"payments watermark stays {pst['watermark']} (published hours are never retracted); "
          f"blocking={pst['blocking_conflicts']} unresolved={pst['unresolved_conflicts']}")
    print(f"orders watermark still {engine.status('orders')['datasets'][0]['watermark']}")

    line("7b. conflicting delivery for an hour that is not published yet")
    pending = engine.deliver_file(
        "payments", "2026-11-18T03",
        os.path.join(INBOX, "payments", "payments-2026-11-18T03.csv"),
    )
    clash = engine.deliver_file(
        "payments", "2026-11-18T03",
        os.path.join(CONFLICTS, "payments", "payments-2026-11-18T03.alternate.csv"),
    )
    print(f"first={pending['outcome']} second={clash['outcome']}")
    engine.merge(pending["batch_id"])
    pst = engine.status("payments")["datasets"][0]
    snap3 = engine.snapshot("payments", "2026-11-18T03")
    print(f"watermark held at {pst['watermark']} while hour 03 is {snap3['state']} "
          f"({snap3['blocked_by']}); blocking={pst['blocking_conflicts']}")
    held = engine.resolve_conflict(clash["delivery_id"], "keep_existing")
    print(f"keep_existing -> watermark {held['watermark']}, hour 03 {engine.snapshot('payments', '2026-11-18T03')['state']}")

    line("8. accept the incoming file and watch snapshots recompute")
    res = engine.resolve_conflict(c["delivery_id"], "accept_incoming")
    print(f"resolve -> {res['outcome']}/{res['action']} watermark={res['watermark']}")
    print(f"recomputed={res['recomputed_snapshots']} published={res['published']}")

    line("9. audit chain")
    chain = engine.verify_audit()
    print(f"verified={chain['ok']} entries={chain['entries']} head={chain['head_hash'][:16]}...")
    events: dict[str, int] = {}
    for entry in engine.audit_trail(1000):
        events[entry["event"]] = events.get(entry["event"], 0) + 1
    for event, count in sorted(events.items()):
        print(f"  {event:<24} {count}")

    line("10. final state")
    status = engine.status()
    for ds in status["datasets"]:
        print(json.dumps(ds, ensure_ascii=False))
    print(f"\ndb: {args.db}")
    report = {"status": status, "chain": chain, "db": args.db}
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    engine.close()
    return 0 if chain["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
