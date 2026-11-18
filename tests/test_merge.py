"""Merge semantics: duplicates, ordering, deletes, multi-updates, empty files."""

from __future__ import annotations

import unittest

from support import EngineTestCase, csv_bytes, row


class DuplicateAndRetryTests(EngineTestCase):
    def test_same_content_is_a_duplicate_and_merge_is_a_noop(self) -> None:
        first = self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        second = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00"))
        )
        self.assertEqual(first["outcome"], "accepted")
        self.assertEqual(second["outcome"], "duplicate")
        self.assertEqual(second["batch_id"], first["batch_id"])
        # Re-merging an applied batch must not touch snapshots.
        version_before = self.engine.snapshot(self.dataset, "2026-11-18T00")["version"]
        again = self.engine.merge(second["batch_id"])
        self.assertEqual(again["outcome"], "noop")
        self.assertEqual(self.engine.snapshot(self.dataset, "2026-11-18T00")["version"], version_before)

    def test_merge_is_idempotent_across_attempts(self) -> None:
        delivery = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00"))
        )
        first = self.engine.merge(delivery["batch_id"])
        second = self.engine.merge(delivery["batch_id"])
        self.assertEqual(first["outcome"], "merged")
        self.assertEqual(second["outcome"], "noop")
        self.assertEqual(self.status()["records"], 1)

    def test_failed_attempt_rolls_back_and_retry_succeeds(self) -> None:
        delivery = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00"))
        )
        from incmerge.engine import Engine
        from incmerge.util import TransientMergeError

        broken = Engine(self.db, actor="test", fault="before_commit")
        with self.assertRaises(TransientMergeError):
            broken.merge(delivery["batch_id"])
        broken.close()

        after = self.status()
        self.assertIsNone(after["watermark"])
        self.assertEqual(after["pending_batches"], 1)
        self.assertEqual(after["records"], 0)
        self.assertFalse(
            [s for s in self.engine.timeline(self.dataset)["hours"] if s["state"] == "published"]
        )

        retried = self.engine.merge(delivery["batch_id"])
        self.assertEqual(retried["outcome"], "retried")
        self.assertEqual(retried["attempt"], 2, "the rolled-back attempt still counts")
        self.assertEqual(self.record("A")["payload"]["status"], "new")

    def test_failed_attempt_is_audited_and_chain_stays_valid(self) -> None:
        delivery = self.engine.deliver_bytes(
            self.dataset, "2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00"))
        )
        from incmerge.engine import Engine
        from incmerge.util import TransientMergeError

        broken = Engine(self.db, actor="test", fault="before_commit")
        with self.assertRaises(TransientMergeError):
            broken.merge(delivery["batch_id"])
        broken.close()
        self.engine.merge(delivery["batch_id"])

        events = [e["event"] for e in self.engine.audit_trail(100)]
        self.assertIn("merge.failed", events)
        self.assertIn("merge.applied", events)
        self.assertTrue(self.engine.verify_audit()["ok"])
        runs = self.engine.merge_runs(10)
        self.assertEqual([r["status"] for r in runs], ["succeeded", "failed"])


class OrderingTests(EngineTestCase):
    def test_out_of_order_arrival_folds_by_event_time(self) -> None:
        self.ingest("2026-11-18T02", csv_bytes(row("A", "2026-11-18T02:10:00", status="shipped")))
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", status="new")))
        # hour 01 is still missing: later hours are withheld but materialised
        self.assertEqual(self.status()["watermark"], "2026-11-18T00")
        self.assertEqual(self.record("A")["payload"]["status"], "shipped")

        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T01:10:00", status="packed")))
        st = self.status()
        self.assertEqual(st["watermark"], "2026-11-18T02")
        self.assertEqual(st["missing_hours"], [])
        self.assertEqual(self.record("A")["payload"]["status"], "shipped")

        kinds = {c["kind"] for c in self.engine.conflicts(self.dataset)}
        self.assertIn("out_of_order_arrival", kinds)
        self.assertIn("late_event_ignored", kinds)

    def test_event_time_beats_arrival_order_within_late_batch(self) -> None:
        """A later hour can carry an older event; the older event must lose."""

        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:50:00", status="late")))
        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T00:20:00", status="middle")))
        self.assertEqual(self.record("A")["payload"]["status"], "late")
        self.assertEqual(self.record("A")["event_ts"], "2026-11-18T00:50:00")
        kinds = {c["kind"] for c in self.engine.conflicts(self.dataset)}
        self.assertIn("late_event_ignored", kinds)
        self.assertIn("event_ts_outside_hour", kinds)

    def test_newer_event_ts_wins_even_from_a_later_hour(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", status="new")))
        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T00:59:59", status="paid")))
        self.assertEqual(self.record("A")["payload"]["status"], "paid")

    def test_backfill_recomputes_later_snapshots(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", amount="1")))
        self.ingest("2026-11-18T02", csv_bytes(row("B", "2026-11-18T02:10:00")))
        before = self.engine.snapshot(self.dataset, "2026-11-18T02")
        backfill = self.engine.backfill(
            self.dataset, "2026-11-18T01", csv_bytes(row("A", "2026-11-18T01:10:00", amount="2"))
        )
        self.assertEqual(backfill["merge"]["watermark"], "2026-11-18T02")
        self.assertIn("2026-11-18T02", backfill["merge"]["recomputed_snapshots"])
        after = self.engine.snapshot(self.dataset, "2026-11-18T02")
        self.assertEqual(after["version"], before["version"] + 1)
        self.assertEqual(after["active_count"], 2)

    def test_empty_batch_completes_its_hour(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        empty = self.engine.deliver_bytes(self.dataset, "2026-11-18T01", b"key,event_ts,op,status,amount\n")
        self.assertTrue(empty["empty_file"])
        result = self.engine.merge(empty["batch_id"])
        self.assertEqual(result["watermark"], "2026-11-18T01")
        self.assertEqual(self.status()["missing_hours"], [])
        kinds = {c["kind"] for c in self.engine.conflicts(self.dataset)}
        self.assertIn("empty_batch", kinds)
        self.assertEqual(self.status()["records"], 1)

    def test_zero_byte_file_is_treated_as_empty_batch(self) -> None:
        delivery = self.engine.deliver_bytes(self.dataset, "2026-11-18T00", b"")
        self.assertTrue(delivery["empty_file"])
        merged = self.engine.merge(delivery["batch_id"])
        self.assertEqual(merged["watermark"], "2026-11-18T00")
        self.assertEqual(self.status()["published_snapshots"], 1)


class MutationRuleTests(EngineTestCase):
    def test_same_key_multiple_times_in_batch_latest_event_wins(self) -> None:
        self.ingest(
            "2026-11-18T00",
            csv_bytes(
                row("A", "2026-11-18T00:40:00", status="paid"),
                row("A", "2026-11-18T00:10:00", status="new"),
                row("A", "2026-11-18T00:20:00", status="packed"),
            ),
        )
        self.assertEqual(self.record("A")["payload"]["status"], "paid")
        conflicts = [c for c in self.engine.conflicts(self.dataset) if c["kind"] == "intra_batch_multi_update"]
        self.assertEqual(len(conflicts), 1)
        self.assertFalse(conflicts[0]["blocking"])
        self.assertEqual(conflicts[0]["detail"]["winner_seq"], 0)

    def test_equal_event_ts_breaks_ties_by_file_order(self) -> None:
        self.ingest(
            "2026-11-18T00",
            csv_bytes(
                row("A", "2026-11-18T00:10:00", status="first"),
                row("A", "2026-11-18T00:10:00", status="second"),
            ),
        )
        self.assertEqual(self.record("A")["payload"]["status"], "second")

    def test_delete_tombstones_and_upsert_resurrects(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", status="new")))
        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T01:10:00", op="delete")))
        rec = self.record("A")
        self.assertTrue(rec["deleted"])
        self.assertIsNone(rec["payload"])
        snap = self.engine.snapshot(self.dataset, "2026-11-18T01")
        self.assertEqual((snap["active_count"], snap["deleted_count"]), (0, 1))
        self.assertEqual(self.status()["records"], 0)

        self.ingest("2026-11-18T02", csv_bytes(row("A", "2026-11-18T02:10:00", status="back")))
        self.assertFalse(self.record("A")["deleted"])
        kinds = {c["kind"] for c in self.engine.conflicts(self.dataset)}
        self.assertIn("resurrected_key", kinds)

    def test_delete_for_unknown_key_is_observed_not_fatal(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("ZZ", "2026-11-18T00:10:00", op="delete")))
        kinds = {c["kind"] for c in self.engine.conflicts(self.dataset)}
        self.assertIn("delete_missing_key", kinds)
        self.assertEqual(self.record("ZZ")["deleted"], True)

    def test_repeated_delete_is_labelled_as_redundant(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00", op="delete")))
        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T01:10:00", op="delete")))
        kinds = [c["kind"] for c in self.engine.conflicts(self.dataset)]
        self.assertIn("delete_missing_key", kinds)
        self.assertIn("redundant_delete", kinds)
        self.assertTrue(self.record("A")["deleted"])

    def test_older_delete_does_not_remove_newer_record(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:50:00", status="new")))
        self.ingest("2026-11-18T01", csv_bytes(row("A", "2026-11-18T00:20:00", op="delete")))
        self.assertFalse(self.record("A")["deleted"])
        kinds = {c["kind"] for c in self.engine.conflicts(self.dataset)}
        self.assertIn("late_event_ignored", kinds)


class SnapshotTests(EngineTestCase):
    def test_snapshot_history_is_append_versioned(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        self.ingest("2026-11-18T01", csv_bytes(row("B", "2026-11-18T01:10:00")))
        snap0 = self.engine.snapshot(self.dataset, "2026-11-18T00")
        snap1 = self.engine.snapshot(self.dataset, "2026-11-18T01")
        self.assertEqual(snap0["active_count"], 1)
        self.assertEqual(snap1["active_count"], 2)
        self.assertEqual(snap0["state"], "published")
        self.assertEqual(snap1["state"], "published")
        # Hour 0 is *not* rewritten when hour 1 arrives.
        self.assertEqual(snap0["version"], 1)

    def test_withheld_hour_becomes_published_on_gap_fill(self) -> None:
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        self.ingest("2026-11-18T02", csv_bytes(row("C", "2026-11-18T02:10:00")))
        blocked = self.engine.snapshot(self.dataset, "2026-11-18T02")
        self.assertEqual(blocked["state"], "blocked")
        self.assertEqual(blocked["blocked_by"], "missing:2026-11-18T01")
        self.assertIsNone(blocked["published_at"])

        self.engine.backfill(self.dataset, "2026-11-18T01", csv_bytes(row("B", "2026-11-18T01:10:00")))
        published = self.engine.snapshot(self.dataset, "2026-11-18T02")
        self.assertEqual(published["state"], "published")
        self.assertIsNone(published["blocked_by"])
        self.assertIsNotNone(published["published_at"])
        self.assertEqual(self.status()["watermark"], "2026-11-18T02")

    def test_gap_in_one_dataset_does_not_block_another(self) -> None:
        self.engine.ensure_dataset("payments", "2026-11-18T00")
        self.ingest("2026-11-18T00", csv_bytes(row("A", "2026-11-18T00:10:00")))
        self.ingest("2026-11-18T02", csv_bytes(row("C", "2026-11-18T02:10:00")))
        self.ingest("2026-11-18T00", csv_bytes(row("P", "2026-11-18T00:10:00")), dataset="payments")
        self.ingest("2026-11-18T01", csv_bytes(row("Q", "2026-11-18T01:10:00")), dataset="payments")
        self.assertEqual(self.status("orders")["watermark"], "2026-11-18T00")
        self.assertEqual(self.status("payments")["watermark"], "2026-11-18T01")
        self.assertEqual(self.status("payments")["blocked_snapshots"], 0)


if __name__ == "__main__":
    unittest.main()
