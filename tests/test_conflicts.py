"""Same-hour content conflicts, resolution, and the publish gate."""

from __future__ import annotations

import unittest

from support import EngineTestCase, csv_bytes, row


class ConflictTests(EngineTestCase):
    def test_different_content_for_same_hour_blocks(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="10")))
        clash = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="999"))
        )
        self.assertEqual(clash["outcome"], "conflict")
        self.assertNotEqual(clash["existing_hash"], clash["content_hash"])
        st = self.status()
        self.assertEqual(st["unresolved_conflicts"], 1)
        # Already published: retained for review, never retracted.
        self.assertEqual(st["watermark"], "2026-11-18T00")
        self.assertEqual(st["blocking_conflicts"], [])
        kinds = {(c["kind"], c["blocking"]) for c in self.engine.conflicts(self.dataset)}
        self.assertIn(("content_mismatch", True), kinds)

    def test_conflict_on_unpublished_hour_withholds_publication(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        first = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T01", csv_bytes(row("B", "2026-11-18T01:10:00", amount="1"))
        )
        self.engine.merge(first["batch_id"])
        self.assertEqual(self.status()["watermark"], "2026-11-18T01")

        # A conflict on the *next* hour, which is not yet delivered at all,
        # cannot exist (it needs an existing batch). Build the scenario where
        # hour 02 is delivered but hour 01 is missing, then conflict hour 02.
        # Here we instead conflict hour 01 before it is merged.
        self.engine.ensure_dataset("late", "2026-11-18T00")
        v1 = self.engine.deliver_bytes("late", "2026-11-18T00", csv_bytes(row("X", "2026-11-18T00:10:00", amount="1")))
        self.engine.merge(v1["batch_id"])
        v2 = self.engine.deliver_bytes("late", "2026-11-18T01", csv_bytes(row("Y", "2026-11-18T01:10:00", amount="1")))
        clash = self.engine.deliver_bytes(
            "late", "2026-11-18T01", csv_bytes(row("Y", "2026-11-18T01:10:00", amount="2"))
        )
        self.assertEqual(clash["outcome"], "conflict")
        # Batch for hour 01 is still pending; the conflict alone must block.
        late = self.engine.status("late")["datasets"][0]
        self.assertEqual(late["blocking_conflicts"], ["2026-11-18T01"])
        self.assertEqual(late["watermark"], "2026-11-18T00")

    def test_keep_existing_discards_incoming_and_unblocks(self) -> None:
        self.engine.ensure_dataset("late", "2026-11-18T00")
        v1 = self.engine.deliver_bytes("late", "2026-11-18T00", csv_bytes(row("X", "2026-11-18T00:10:00", amount="1")))
        self.engine.merge(v1["batch_id"])
        v2 = self.engine.deliver_bytes("late", "2026-11-18T01", csv_bytes(row("Y", "2026-11-18T01:10:00", amount="1")))
        clash = self.engine.deliver_bytes(
            "late", "2026-11-18T01", csv_bytes(row("Y", "2026-11-18T01:10:00", amount="2"))
        )
        result = self.engine.resolve_conflict(clash["delivery_id"], "keep_existing")
        self.assertEqual(result["watermark"], "2026-11-18T00")
        self.engine.merge(v2["batch_id"])
        resolved = self.engine.status("late")["datasets"][0]
        self.assertEqual(resolved["watermark"], "2026-11-18T01")
        self.assertEqual(resolved["unresolved_conflicts"], 0)
        payloads = {r["key"]: r["payload"] for r in self.engine.records("late")}
        self.assertEqual(payloads["Y"]["amount"], "1")
        self.assertIn("conflict.resolved", [e["event"] for e in self.engine.audit_trail(100)])

    def test_accept_incoming_replaces_batch_and_recomputes(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="10")))
        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T01:10:00", amount="20")))
        before = self.engine.snapshot(self.dataset, "2026-11-18T01")
        clash = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="11"))
        )
        result = self.engine.resolve_conflict(clash["delivery_id"], "accept_incoming")
        self.assertEqual(result["outcome"], "resolved")
        self.assertIn("2026-11-18T00", result["recomputed_snapshots"])
        self.assertTrue(self.record("A")["payload"]["amount"] in {"11", "20"})
        after = self.engine.snapshot(self.dataset, "2026-11-18T00")
        self.assertEqual(after["version"], 2)

    def test_cannot_resolve_twice(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="10")))
        clash = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="999"))
        )
        self.engine.resolve_conflict(clash["delivery_id"], "keep_existing")
        with self.assertRaises(Exception) as ctx:
            self.engine.resolve_conflict(clash["delivery_id"], "accept_incoming")
        self.assertIn("not an unresolved conflict", str(ctx.exception))

    def test_invalid_resolution_action(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="10")))
        clash = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="999"))
        )
        with self.assertRaises(Exception):
            self.engine.resolve_conflict(clash["delivery_id"], "merge_both")

    def test_duplicate_after_conflict_is_still_a_conflict(self) -> None:
        """The applied batch never changed, so the same file keeps conflicting
        until an operator resolves it."""

        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="10")))
        content = csv_bytes(row("A", "2026-11-18T00:10:00", amount="999"))
        first = self.engine.deliver_bytes(self.dataset, "2026-11-18T00", content)
        second = self.engine.deliver_bytes(self.dataset, "2026-11-18T00", content)
        self.assertEqual(first["outcome"], "conflict")
        self.assertEqual(second["outcome"], "conflict")
        self.assertEqual(self.status()["unresolved_conflicts"], 2)

    def test_backfill_of_conflicting_hour_is_blocked_not_applied(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="10")))
        result = self.engine.backfill(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="999"))
        )
        self.assertEqual(result["outcome"], "blocked")
        self.assertEqual(self.record("A")["payload"]["amount"], "10")


if __name__ == "__main__":
    unittest.main()
