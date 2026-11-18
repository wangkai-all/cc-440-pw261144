"""Audit chain integrity and the HTTP dashboard API."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from support import EngineTestCase, csv_bytes, row

from incmerge import audit
from incmerge.web import Handler
from http.server import ThreadingHTTPServer


class AuditChainTests(EngineTestCase):
    def test_every_mutation_is_chained(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        chain = self.engine.verify_audit()
        self.assertTrue(chain["ok"])
        self.assertGreater(chain["entries"], 5)
        seqs = [e["seq"] for e in self.engine.audit_trail(100)]
        self.assertEqual(seqs, list(range(len(seqs), 0, -1)))

    def test_tampered_payload_is_detected(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        conn = sqlite3.connect(self.db)
        with conn:
            conn.execute(
                "UPDATE audit_log SET detail_json='{\"forged\":true}' WHERE seq=2"
            )
        conn.close()
        chain = self.engine.verify_audit()
        self.assertFalse(chain["ok"])
        self.assertEqual(chain["broken_at"], 2)
        self.assertIn("does not match", chain["reason"])

    def test_deleted_entry_breaks_the_chain(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        conn = sqlite3.connect(self.db)
        with conn:
            conn.execute("DELETE FROM audit_log WHERE seq=3")
        conn.close()
        chain = self.engine.verify_audit()
        self.assertFalse(chain["ok"])
        self.assertIn("sequence gap", chain["reason"])

    def test_reordering_is_detected(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        conn = sqlite3.connect(self.db)
        with conn:
            conn.execute("UPDATE audit_log SET prev_hash=? WHERE seq=4", ("f" * 64,))
        conn.close()
        chain = self.engine.verify_audit()
        self.assertFalse(chain["ok"])
        self.assertEqual(chain["broken_at"], 4)

    def test_audit_records_recovery_actions_with_actor(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="9"))
        )
        delivery = self.engine.unresolved_conflicts(self.dataset)[0]
        self.engine.resolve_conflict(delivery["id"], "keep_existing", actor="ops/alice")
        entries = self.engine.audit_trail(100)
        resolved = [e for e in entries if e["event"] == "conflict.resolved"]
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0]["actor"], "ops/alice")
        self.assertTrue(self.engine.verify_audit()["ok"])


class WebApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.mkdtemp(prefix="incmerge-web-")
        cls.db = os.path.join(cls.tmp, "web.db")
        import incmerge.web as web

        web._db_path = cls.db
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

        from incmerge.engine import Engine

        engine = Engine(cls.db, actor="seed")
        engine.ensure_dataset("orders", "2026-11-18T00")
        engine.ensure_dataset("payments", "2026-11-18T00")
        d = engine.deliver_bytes("orders", "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        engine.merge(d["batch_id"])
        d = engine.deliver_bytes("orders", "2026-11-18T02", csv_bytes(row("C", "2026-11-18T02:10:00")))
        engine.merge(d["batch_id"])
        pending = engine.deliver_bytes("orders", "2026-11-18T01", csv_bytes(row("B", "2026-11-18T01:10:00")))
        cls.pending_batch = pending["batch_id"]
        d = engine.deliver_bytes("payments", "2026-11-18T00", csv_bytes(row("P", "2026-11-18T00:10:00", amount="1")))
        engine.merge(d["batch_id"])
        clash = engine.deliver_bytes(
            "payments", "2026-11-18T00", csv_bytes(row("P", "2026-11-18T00:10:00", amount="2"))
        )
        cls.delivery_id = clash["delivery_id"]
        # A dataset that is never mutated by mutating-endpoint tests, so the
        # gap/publish assertions stay order-independent.
        engine.ensure_dataset("ledger", "2026-11-18T00")
        d = engine.deliver_bytes("ledger", "2026-11-18T00", csv_bytes(row("L", "2026-11-18T00:10:00")))
        engine.merge(d["batch_id"])
        d = engine.deliver_bytes("ledger", "2026-11-18T02", csv_bytes(row("M", "2026-11-18T02:10:00")))
        engine.merge(d["batch_id"])
        engine.deliver_bytes("ledger", "2026-11-18T03", csv_bytes(row("N", "2026-11-18T03:10:00")))
        engine.close()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def request(self, path: str, method: str = "GET", body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as res:
                return res.status, json.loads(res.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    def test_state_lists_datasets_and_defaults_to_first(self) -> None:
        code, body = self.request("/api/state")
        self.assertEqual(code, 200)
        self.assertIn("ledger", body["datasets"])
        self.assertIn("orders", body["datasets"])
        self.assertIn("payments", body["datasets"])
        self.assertEqual(body["selected"], body["datasets"][0])
        self.assertIn("timeline", body)
        self.assertTrue(body["audit"]["ok"])

    def test_timeline_reports_gap_and_withheld_state(self) -> None:
        _, body = self.request("/api/state?dataset=ledger")
        states = {h["hour"]: h["state"] for h in body["timeline"]["hours"]}
        self.assertEqual(states["2026-11-18T00"], "published")
        self.assertEqual(states["2026-11-18T01"], "missing")
        self.assertEqual(states["2026-11-18T02"], "blocked")
        self.assertEqual(states["2026-11-18T03"], "pending")
        self.assertEqual(body["status"]["missing_hours"], ["2026-11-18T01"])
        self.assertEqual(body["status"]["watermark"], "2026-11-18T00")

    def test_conflicts_endpoint_surfaces_blocking_mismatch(self) -> None:
        _, body = self.request("/api/state?dataset=payments")
        kinds = [c["kind"] for c in body["conflicts"]]
        self.assertIn("content_mismatch", kinds)
        self.assertEqual(len(body["unresolved"]), 1)
        self.assertIn("publish withheld", body["conflict_rules"]["content_mismatch"])

    def test_merge_endpoint_is_retry_safe(self) -> None:
        code, body = self.request("/api/merge", "POST", {"batch_id": self.pending_batch, "actor": "ops/bob"})
        self.assertEqual(code, 200)
        _, state = self.request("/api/state?dataset=orders")
        self.assertEqual(state["status"]["missing_hours"], [])
        self.assertEqual(state["status"]["watermark"], "2026-11-18T02")
        code, again = self.request("/api/merge", "POST", {"batch_id": self.pending_batch})
        self.assertEqual(code, 200)
        self.assertEqual(again["outcome"], "noop")

    def test_resolve_endpoint_requires_valid_action(self) -> None:
        code, body = self.request("/api/resolve", "POST", {"delivery_id": self.delivery_id, "action": "nope"})
        self.assertEqual(code, 400)
        self.assertIn("action must be", body["error"])

    def test_backfill_endpoint_accepts_csv(self) -> None:
        code, body = self.request(
            "/api/backfill", "POST",
            {"dataset": "orphan", "hour": "2026-11-18T00",
             "csv": "key,event_ts,op,status,amount\nZ,2026-11-18T00:10:00,upsert,new,5\n"},
        )
        self.assertEqual(code, 400)
        self.assertIn("unknown dataset", body["error"])

        from incmerge.engine import Engine

        seed = Engine(self.db, actor="seed")
        seed.ensure_dataset("backfill-ds", "2026-11-18T00")
        seed.close()
        code, body = self.request(
            "/api/backfill", "POST",
            {"dataset": "backfill-ds", "hour": "2026-11-18T00",
             "csv": "key,event_ts,op,status,amount\nZ,2026-11-18T00:10:00,upsert,new,5\n"},
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["outcome"], "backfilled")
        self.assertEqual(body["merge"]["watermark"], "2026-11-18T00")

    def test_unknown_route_and_bad_json(self) -> None:
        code, _ = self.request("/api/nope")
        self.assertEqual(code, 404)
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/merge", data=b"not json", method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            self.fail("expected 400")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 400)

    def test_path_traversal_is_rejected(self) -> None:
        code, _ = self.request("/static/../../incmerge/engine.py")
        self.assertEqual(code, 404)

    def test_static_index_is_served(self) -> None:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=10) as res:
            html = res.read().decode()
        self.assertIn("incmerge", html)
        self.assertIn("Batch dependency chain", html)


if __name__ == "__main__":
    unittest.main()
