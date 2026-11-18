"""Seed a realistic day of sample data for the dashboard.

The hand-written files under ``samples/`` are small and narrative, which makes
them great for the demo but poor for judging the dashboard at real density.
This script generates a full 24-hour day for two datasets, with the failure
modes that matter deliberately placed:

  orders/00–23   gap at 06 (delivered late), withheld stretch 07–09,
                 repeated keys, deletes, resurrect, an empty hour at 04
  payments/00–23 clean, plus an unresolved mismatch on an already-published hour

    python3 -I scripts/seed_dashboard.py --db local.db
"""

from __future__ import annotations

import argparse
import os
import random
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from incmerge.engine import Engine  # noqa: E402

HEADER = "key,event_ts,op,status,amount,region\n"
REGIONS = ["eu", "us", "apac"]
STATUSES = ["new", "packed", "shipped", "paid", "refunded"]
DAY = "2026-11-18"


def hour(h: int) -> str:
    return f"{DAY}T{h:02d}"


def ts(h: int, minute: int, second: int = 0) -> str:
    return f"{DAY}T{h:02d}:{minute:02d}:{second:02d}"


def build(rows: list[tuple[str, str, str, str, str, str]]) -> bytes:
    return (HEADER + "".join(",".join(r) + "\n" for r in rows)).encode()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="dashboard.db")
    parser.add_argument(
        "--fill-gap", action="store_true",
        help="also backfill hour 06 (default leaves it open so the dashboard shows a withheld stretch)",
    )
    args = parser.parse_args()

    if os.path.exists(args.db):
        os.remove(args.db)
    engine = Engine(args.db, actor="seed")
    engine.ensure_dataset("orders", hour(0))
    engine.ensure_dataset("payments", hour(0))
    rng = random.Random(20261118)

    delivered: list[int] = []
    for h in range(24):
        if h == 4:
            # Header-only file: a genuinely quiet hour, not a missing one.
            empty = engine.deliver_bytes(
                "orders", hour(h), b"key,event_ts,op,status,amount,region\n",
                filename=f"orders-{hour(h)}.csv",
            )
            engine.merge(empty["batch_id"])
            delivered.append(h)
            continue
        if h == 6:
            continue  # the gap; backfilled below

        rows: list[tuple[str, str, str, str, str, str]] = []
        for n in range(rng.randint(3, 6)):
            key = f"ORD-{1000 + (h * 7 + n * 3) % 42}"
            rows.append((key, ts(h, rng.randint(1, 55), rng.randint(0, 59)),
                         "upsert", rng.choice(STATUSES),
                         f"{rng.uniform(5, 400):.2f}", rng.choice(REGIONS)))
        if h in (3, 11, 19):
            # A key deleted, later recreated.
            rows.append((f"ORD-{1000 + h}", ts(h, 58), "delete", "", "", ""))
        if h == 8:
            # Same key twice in one file with different event times.
            rows.append(("ORD-1001", ts(h, 10), "upsert", "packed", "42.50", "eu"))
            rows.append(("ORD-1001", ts(h, 45), "upsert", "shipped", "42.50", "eu"))
        if h == 13:
            # A row carrying an event time from a previous hour.
            rows.append(("ORD-1002", ts(h - 1, 30), "upsert", "paid", "18.00", "us"))

        # Deliver hours 7–9 before 6 is backfilled, so they are withheld.
        delivery = engine.deliver_bytes(
            "orders", hour(h), build(rows), filename=f"orders-{hour(h)}.csv"
        )
        engine.merge(delivery["batch_id"])
        delivered.append(h)

    # Backfill the gap: hours 7–9 publish retroactively.
    gap_rows = [
        (f"ORD-{1000 + n}", ts(6, 8 + n * 11, 30), "upsert", "new",
         f"{rng.uniform(5, 400):.2f}", rng.choice(REGIONS))
        for n in range(4)
    ]
    if args.fill_gap:
        engine.backfill("orders", hour(6), build(gap_rows), filename="backlog/orders-2026-11-18T06.csv")

    for h in range(24):
        rows = [
            (f"PAY-{700 + (h + n) % 25}", ts(h, rng.randint(1, 58), rng.randint(0, 59)),
             "upsert", rng.choice(["authorized", "captured", "refunded"]),
             f"{rng.uniform(5, 400):.2f}", "")
            for n in range(rng.randint(1, 3))
        ]
        delivery = engine.deliver_bytes(
            "payments", hour(h), build(rows), filename=f"payments-{hour(h)}.csv"
        )
        engine.merge(delivery["batch_id"])

    # A re-delivery of an already-published hour with different content: held
    # for review, but it does not retract the watermark.
    engine.deliver_bytes(
        "payments", hour(14),
        build([("PAY-714", ts(14, 20), "upsert", "refunded", "999.00", "")]),
        filename="conflicts/payments-2026-11-18T14.replayed.csv",
    )

    status = engine.status()
    for ds in status["datasets"]:
        print(
            f"{ds['dataset']:<9} head={ds['head']} watermark={ds['watermark']} "
            f"published={ds['published_snapshots']} blocked={ds['blocked_snapshots']} "
            f"conflicts={ds['unresolved_conflicts']} records={ds['records']}"
        )
    print(f"audit chain ok={status['audit']['ok']} entries={status['audit']['entries']}")
    print(f"db: {args.db}")
    engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
