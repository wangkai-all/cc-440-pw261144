"""End-to-end CLI tests (subprocess, real files, real database)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES = os.path.join(ROOT, "samples")


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="incmerge-cli-")
        self.db = os.path.join(self.tmp, "cli.db")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *args: str, expect: int = 0) -> dict:
        proc = subprocess.run(
            [sys.executable, "-m", "incmerge", "--db", self.db, *args],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.assertEqual(
            proc.returncode, expect,
            f"exit {proc.returncode}\nstdout={proc.stdout}\nstderr={proc.stderr}",
        )
        return json.loads(proc.stdout) if proc.stdout.strip() else {}

    def write(self, name: str, content: str) -> str:
        path = os.path.join(self.tmp, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    def test_full_recovery_workflow(self) -> None:
        self.run_cli("init", "orders", "--start", "2026-11-18T00")
        self.run_cli("init", "payments", "--start", "2026-11-18T00")

        ingest = self.run_cli(
            "ingest", "orders", "2026-11-18T00",
            os.path.join(SAMPLES, "inbox", "orders", "orders-2026-11-18T00.csv"),
        )
        self.assertEqual(ingest["merge"]["watermark"], "2026-11-18T00")

        dup = self.run_cli(
            "ingest", "orders", "2026-11-18T00",
            os.path.join(SAMPLES, "inbox", "orders", "orders-2026-11-18T00.csv"),
        )
        self.assertEqual(dup["delivery"]["outcome"], "duplicate")
        self.assertEqual(dup["merge"]["outcome"], "noop")

        # hour 03 without hour 01/02 -> withheld
        late = self.run_cli(
            "ingest", "orders", "2026-11-18T03",
            os.path.join(SAMPLES, "inbox", "orders", "orders-2026-11-18T03.csv"),
        )
        self.assertEqual(late["merge"]["watermark"], "2026-11-18T00")

        status = self.run_cli("status", "orders")["datasets"][0]
        self.assertEqual(status["missing_hours"], ["2026-11-18T01", "2026-11-18T02"])
        self.assertEqual(status["blocked_snapshots"], 1)

        # backfill 02 before 01: still blocked
        bf = self.run_cli(
            "backfill", "orders", "2026-11-18T02",
            os.path.join(SAMPLES, "backlog", "orders", "orders-2026-11-18T02.csv"),
        )
        self.assertEqual(bf["outcome"], "backfilled")
        self.assertEqual(bf["merge"]["watermark"], "2026-11-18T00")
        self.assertEqual(bf["merge"]["published"], [])

        # backfill 01: everything publishes and later snapshots recompute
        bf1 = self.run_cli(
            "backfill", "orders", "2026-11-18T01",
            os.path.join(SAMPLES, "inbox", "orders", "orders-2026-11-18T01.csv"),
        )
        self.assertEqual(bf1["merge"]["watermark"], "2026-11-18T03")
        self.assertEqual(bf1["merge"]["published"], ["2026-11-18T01", "2026-11-18T02", "2026-11-18T03"])

        timeline = self.run_cli("timeline", "orders")["hours"]
        self.assertEqual({h["state"] for h in timeline}, {"published"})

        chain = self.run_cli("audit")
        self.assertTrue(chain["verification"]["ok"])
        events = {e["event"] for e in chain["entries"]}
        self.assertTrue({"delivery.received", "merge.applied", "merge.noop",
                         "watermark.advanced", "snapshot.recomputed"} <= events)

    def test_conflict_and_resolution(self) -> None:
        self.run_cli("init", "payments", "--start", "2026-11-18T00")
        self.run_cli(
            "ingest", "payments", "2026-11-18T01",
            os.path.join(SAMPLES, "inbox", "payments", "payments-2026-11-18T01.csv"),
        )
        clash = self.run_cli(
            "deliver", "payments", "2026-11-18T01",
            os.path.join(SAMPLES, "conflicts", "payments", "payments-2026-11-18T01.replayed.csv"),
            expect=2,
        )
        self.assertEqual(clash["outcome"], "conflict")
        conflicts = self.run_cli("conflicts", "payments")
        self.assertEqual(len(conflicts["unresolved"]), 1)

        resolved = self.run_cli("resolve", str(clash["delivery_id"]), "accept_incoming")
        self.assertEqual(resolved["outcome"], "resolved")
        self.assertIn("2026-11-18T01", resolved["recomputed_snapshots"])

        again = self.run_cli("resolve", str(clash["delivery_id"]), "accept_incoming", expect=2)
        self.assertEqual(again, {})

        self.assertEqual(self.run_cli("conflicts", "payments")["unresolved"], [])

    def test_bad_input_exits_nonzero_with_message(self) -> None:
        self.run_cli("init", "orders", "--start", "2026-11-18T00")
        bad = self.write("bad.csv", "key,event_ts,amount\nA,not-a-date,1\n")
        proc = subprocess.run(
            [sys.executable, "-m", "incmerge", "--db", self.db, "ingest", "orders", "2026-11-18T00", bad],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("invalid event_ts", proc.stderr)

    def test_audit_exits_nonzero_when_chain_is_broken(self) -> None:
        self.run_cli("init", "orders", "--start", "2026-11-18T00")
        self.run_cli(
            "ingest", "orders", "2026-11-18T00",
            os.path.join(SAMPLES, "inbox", "orders", "orders-2026-11-18T00.csv"),
        )
        import sqlite3

        conn = sqlite3.connect(self.db)
        with conn:
            conn.execute("UPDATE audit_log SET actor='intruder' WHERE seq=2")
        conn.close()
        proc = subprocess.run(
            [sys.executable, "-m", "incmerge", "--db", self.db, "audit"],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 1)
        self.assertFalse(json.loads(proc.stdout)["verification"]["ok"])


if __name__ == "__main__":
    unittest.main()
