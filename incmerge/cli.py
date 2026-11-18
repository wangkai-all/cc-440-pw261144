"""Command-line interface for incmerge."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from typing import Any, Sequence

from .engine import Engine
from .util import IncmergeError


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False))


def _engine(args: argparse.Namespace) -> Engine:
    return Engine(args.db, actor=args.actor)


def cmd_init(args: argparse.Namespace) -> int:
    engine = _engine(args)
    with_engine = engine.ensure_dataset(args.dataset, args.start)
    _print({"outcome": "initialised", "dataset": with_engine})
    engine.close()
    return 0


def cmd_deliver(args: argparse.Namespace) -> int:
    engine = _engine(args)
    if args.start:
        engine.ensure_dataset(args.dataset, args.start)
    result = engine.deliver_file(args.dataset, args.hour, args.path)
    _print(result)
    engine.close()
    return 0 if result["outcome"] in {"accepted", "duplicate"} else 2


def cmd_merge(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.merge(args.batch_id)
    _print(result)
    engine.close()
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    """Deliver + merge in one step (what an hourly job would run)."""

    engine = _engine(args)
    if args.start:
        engine.ensure_dataset(args.dataset, args.start)
    delivery = engine.deliver_file(args.dataset, args.hour, args.path)
    if delivery["outcome"] == "conflict":
        _print({"delivery": delivery, "outcome": "blocked"})
        engine.close()
        return 2
    result = engine.merge(int(delivery["batch_id"]))
    _print({"delivery": delivery, "merge": result})
    engine.close()
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.backfill_file(args.dataset, args.hour, args.path)
    _print(result)
    engine.close()
    return 0 if result["outcome"] == "backfilled" else 2


def cmd_status(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _print(engine.status(args.dataset))
    engine.close()
    return 0


def cmd_timeline(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _print(engine.timeline(args.dataset))
    engine.close()
    return 0


def cmd_conflicts(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _print(
        {
            "unresolved": engine.unresolved_conflicts(args.dataset),
            "observations": engine.conflicts(args.dataset, args.limit),
        }
    )
    engine.close()
    return 0


def cmd_resolve(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.resolve_conflict(args.delivery_id, args.action)
    _print(result)
    engine.close()
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    engine = _engine(args)
    verification = engine.verify_audit()
    _print({"verification": verification, "entries": engine.audit_trail(args.limit)})
    engine.close()
    return 0 if verification["ok"] else 1


def cmd_runs(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _print(engine.merge_runs(args.limit))
    engine.close()
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .web import serve

    engine = _engine(args)
    if args.start and args.dataset:
        engine.ensure_dataset(args.dataset, args.start)
    engine.close()
    serve(args.db, host=args.host, port=args.port, open_browser=args.open)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="incmerge",
        description="Build incremental tables from hourly CSV change files.",
    )
    parser.add_argument("--db", default=os.environ.get("INCMERGE_DB", "incmerge.db"))
    parser.add_argument("--actor", default="cli")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="create the database and a dataset")
    p.add_argument("dataset")
    p.add_argument("--start", required=True, help="expected first hour, e.g. 2026-11-18T00")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("deliver", help="register an hourly file without merging")
    p.add_argument("dataset")
    p.add_argument("hour")
    p.add_argument("path")
    p.add_argument("--start")
    p.set_defaults(func=cmd_deliver)

    p = sub.add_parser("merge", help="apply a pending batch by id (idempotent)")
    p.add_argument("batch_id", type=int)
    p.set_defaults(func=cmd_merge)

    p = sub.add_parser("ingest", help="deliver then merge one hourly file")
    p.add_argument("dataset")
    p.add_argument("hour")
    p.add_argument("path")
    p.add_argument("--start")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("backfill", help="fill a missing hour and recompute snapshots")
    p.add_argument("dataset")
    p.add_argument("hour")
    p.add_argument("path")
    p.set_defaults(func=cmd_backfill)

    p = sub.add_parser("status", help="dataset watermarks and audit health")
    p.add_argument("dataset", nargs="?")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("timeline", help="per-hour batch dependency chain")
    p.add_argument("dataset")
    p.set_defaults(func=cmd_timeline)

    p = sub.add_parser("conflicts", help="show blocking conflicts and observations")
    p.add_argument("dataset")
    p.add_argument("--limit", type=int, default=200)
    p.set_defaults(func=cmd_conflicts)

    p = sub.add_parser("resolve", help="resolve a same-hour content mismatch")
    p.add_argument("delivery_id", type=int)
    p.add_argument("action", choices=["keep_existing", "accept_incoming"])
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("audit", help="verify and print the audit chain")
    p.add_argument("--limit", type=int, default=100)
    p.set_defaults(func=cmd_audit)

    p = sub.add_parser("runs", help="merge attempt history")
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_runs)

    p = sub.add_parser("serve", help="start the local dashboard")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--dataset")
    p.add_argument("--start")
    p.add_argument("--open", action="store_true")
    p.set_defaults(func=cmd_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        return int(args.func(args) or 0)
    except IncmergeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
