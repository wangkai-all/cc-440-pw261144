"""Runnable verification of the failure-recovery and audit invariants.

Unlike scripts/demo.py (a narrative walkthrough), this script asserts the
properties an operator cares about and exits non-zero on the first violation.

    python3 -I scripts/verify.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from incmerge import audit  # noqa: E402
from incmerge.engine import Engine  # noqa: E402
from incmerge.util import TransientMergeError  # noqa: E402

CHECKS = 0


def check(condition: bool, label: str) -> None:
    global CHECKS
    CHECKS += 1
    if condition:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label}")
        raise SystemExit(1)


def header(*rows: str) -> bytes:
    return ("key,event_ts,op,status\n" + "".join(r + "\n" for r in rows)).encode()


def main() -> int:
    db = os.path.join(tempfile.mkdtemp(prefix="incmerge-verify-"), "verify.db")
    engine = Engine(db, actor="verify")
    engine.ensure_dataset("orders", "2026-11-18T00")
    engine.ensure_dataset("payments", "2026-11-18T00")

    print("\nmerge identity and retry safety")
    d = engine.deliver_bytes("orders", "2026-11-18T00", header("A,2026-11-18T00:10:00,upsert,new"))
    merged = engine.merge(d["batch_id"])
    check(merged["watermark"] == "2026-11-18T00", "hour 00 publishes")
    check(engine.merge(d["batch_id"])["outcome"] == "noop", "re-merge is a no-op")

    dup = engine.deliver_bytes("orders", "2026-11-18T00", header("A,2026-11-18T00:10:00,upsert,new"))
    check(dup["outcome"] == "duplicate", "identical redelivery detected as duplicate")

    print("\nfailure injection and recovery")
    d1 = engine.deliver_bytes("orders", "2026-11-18T01", header("A,2026-11-18T01:10:00,upsert,paid"))
    broken = Engine(db, actor="verify", fault="before_commit")
    try:
        broken.merge(d1["batch_id"])
        check(False, "injected failure raised")
    except TransientMergeError:
        check(True, "injected failure raised and rolled back")
    finally:
        broken.close()
    state = engine.status("orders")["datasets"][0]
    check(state["pending_batches"] == 1, "failed batch stays pending")
    check(state["records"] == 1, "no partial records were written")
    check(engine.verify_audit()["ok"], "audit chain intact after failure")
    retried = engine.merge(d1["batch_id"])
    check(retried["outcome"] == "retried", "retry succeeds")
    check(retried["attempt"] == 2, "attempt counter includes the failed attempt")
    check(engine.status("orders")["datasets"][0]["watermark"] == "2026-11-18T01",
          "watermark advances after retry")

    print("\nisolation of a gap")
    d3 = engine.deliver_bytes("orders", "2026-11-18T03", header("A,2026-11-18T03:10:00,upsert,shipped"))
    engine.merge(d3["batch_id"])
    check(engine.status("orders")["datasets"][0]["watermark"] == "2026-11-18T01",
          "hour 03 withheld while 02 is missing")
    check(engine.snapshot("orders", "2026-11-18T03")["state"] == "blocked",
          "withheld snapshot is marked blocked")
    check(engine.snapshot("orders", "2026-11-18T03")["blocked_by"] == "missing:2026-11-18T02",
          "blocker names the missing hour")

    p = engine.deliver_bytes("payments", "2026-11-18T00", header("P,2026-11-18T00:10:00,upsert,new"))
    engine.merge(p["batch_id"])
    check(engine.status("payments")["datasets"][0]["watermark"] == "2026-11-18T00",
          "other dataset publishes independently")

    print("\nbackfill recomputation")
    before = engine.snapshot("orders", "2026-11-18T03")
    result = engine.backfill(
        "orders", "2026-11-18T02", header("B,2026-11-18T02:10:00,upsert,new")
    )
    check(result["outcome"] == "backfilled", "backfill accepted")
    check(result["merge"]["watermark"] == "2026-11-18T03", "gap fill releases the gate")
    check("2026-11-18T03" in result["merge"]["recomputed_snapshots"],
          "later snapshot recomputed")
    after = engine.snapshot("orders", "2026-11-18T03")
    check(after["version"] == before["version"] + 1, "snapshot version bumped")
    check(after["active_count"] == before["active_count"] + 1, "snapshot reflects the new key")

    print("\nconflict handling")
    clash = engine.deliver_bytes(
        "payments", "2026-11-18T00", header("P,2026-11-18T00:10:00,upsert,refunded")
    )
    check(clash["outcome"] == "conflict", "mismatched redelivery is a conflict")
    check(engine.status("payments")["datasets"][0]["unresolved_conflicts"] == 1,
          "conflict is surfaced")
    resolved = engine.resolve_conflict(clash["delivery_id"], "accept_incoming")
    check(resolved["outcome"] == "resolved", "conflict resolved")
    payloads = {r["key"]: r["payload"] for r in engine.records("payments")}
    check(payloads["P"]["status"] == "refunded", "accepted version is materialised")

    print("\naudit chain")
    chain = engine.verify_audit()
    check(chain["ok"], f"chain verifies ({chain['entries']} entries)")
    check(chain["entries"] >= 20, "chain captured the whole session")
    events = {e["event"] for e in engine.audit_trail(500)}
    for expected in ("merge.failed", "conflict.resolved", "snapshot.recomputed",
                     "watermark.advanced", "delivery.duplicate"):
        check(expected in events, f"audit contains {expected}")

    conn = sqlite3.connect(db)
    with conn:
        conn.execute("UPDATE audit_log SET detail_json='{\"forged\":true}' WHERE seq=5")
    conn.close()
    tampered = engine.verify_audit()
    check(not tampered["ok"] and tampered["broken_at"] == 5,
          "tampering is detected and located")

    print(f"\n{CHECKS} checks passed. db: {db}")
    engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
