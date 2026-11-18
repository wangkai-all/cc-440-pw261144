"""CSV parsing and validation rules."""

from __future__ import annotations

import unittest

from support import EngineTestCase

from incmerge.csvio import parse_csv_bytes
from incmerge.util import ValidationError


class ParserTests(unittest.TestCase):
    def test_minimal_file_defaults_to_upsert(self) -> None:
        parsed = parse_csv_bytes(b"key,event_ts\nA,2026-11-18T00:10:00\n")
        self.assertEqual(parsed.row_count, 1)
        self.assertEqual(parsed.rows[0].op, "upsert")
        self.assertEqual(parsed.rows[0].payload, {})
        self.assertFalse(parsed.empty_file)

    def test_header_only_file_is_empty_but_valid(self) -> None:
        parsed = parse_csv_bytes(b"key,event_ts\n")
        self.assertTrue(parsed.empty_file)
        self.assertEqual(parsed.row_count, 0)

    def test_whitespace_only_file_is_empty(self) -> None:
        parsed = parse_csv_bytes(b"\n\n  \n")
        self.assertTrue(parsed.empty_file)

    def test_bom_and_casing_are_normalised(self) -> None:
        parsed = parse_csv_bytes("﻿KEY,Event_TS,Op,Amount\nA,2026-11-18T00:10:00,UPSERT,5\n".encode())
        self.assertEqual(parsed.rows[0].op, "upsert")
        self.assertEqual(parsed.rows[0].payload, {"amount": "5"})

    def test_delete_variants_are_accepted(self) -> None:
        for op in ("delete", "DELETE", "del", "remove", "tombstone"):
            parsed = parse_csv_bytes(f"key,event_ts,op\nA,2026-11-18T00:10:00,{op}\n".encode())
            self.assertEqual(parsed.rows[0].op, "delete", op)
            self.assertEqual(parsed.rows[0].payload, {})

    def test_blank_lines_are_skipped_but_counted_by_source_line(self) -> None:
        parsed = parse_csv_bytes(b"key,event_ts\n\nA,2026-11-18T00:10:00\n\nB,2026-11-18T00:20:00\n")
        self.assertEqual([r.line_no for r in parsed.rows], [3, 5])
        self.assertEqual([r.seq for r in parsed.rows], [0, 1])

    def test_timezone_offsets_normalise_to_utc(self) -> None:
        parsed = parse_csv_bytes(b"key,event_ts\nA,2026-11-18T02:10:00+02:00\n")
        self.assertEqual(parsed.rows[0].event_us // 1_000_000, 1794960600)
        # 02:10+02:00 == 00:10Z
        self.assertEqual(parsed.rows[0].event_ts, "2026-11-18T02:10:00+02:00")

    def test_content_hash_is_order_sensitive(self) -> None:
        a = parse_csv_bytes(b"key,event_ts\nA,2026-11-18T00:10:00\nB,2026-11-18T00:20:00\n")
        b = parse_csv_bytes(b"key,event_ts\nB,2026-11-18T00:20:00\nA,2026-11-18T00:10:00\n")
        c = parse_csv_bytes(b"key,event_ts\nA,2026-11-18T00:10:00\nB,2026-11-18T00:20:00\n")
        self.assertNotEqual(a.content_hash, b.content_hash)
        self.assertEqual(a.content_hash, c.content_hash)

    def test_content_hash_ignores_irrelevant_whitespace(self) -> None:
        a = parse_csv_bytes(b"key,event_ts,amount\nA,2026-11-18T00:10:00,5\n")
        b = parse_csv_bytes(b"key,event_ts,amount\n A , 2026-11-18T00:10:00 , 5 \n")
        self.assertEqual(a.content_hash, b.content_hash)


class ParserRejectionTests(unittest.TestCase):
    def assertRejects(self, data: bytes, needle: str) -> None:
        with self.assertRaises(ValidationError) as ctx:
            parse_csv_bytes(data)
        self.assertIn(needle, str(ctx.exception))

    def test_missing_key_column(self) -> None:
        self.assertRejects(b"event_ts\n2026-11-18T00:10:00\n", "missing required column")

    def test_missing_event_ts_column(self) -> None:
        self.assertRejects(b"key\nA\n", "missing required column")

    def test_duplicate_header(self) -> None:
        self.assertRejects(b"key,event_ts,amount,amount\nA,2026-11-18T00:10:00,1,2\n", "duplicate column")

    def test_ragged_row(self) -> None:
        self.assertRejects(b"key,event_ts,amount\nA,2026-11-18T00:10:00\n", "expected 3 columns")

    def test_empty_key(self) -> None:
        self.assertRejects(b"key,event_ts\n,2026-11-18T00:10:00\n", "key is empty")

    def test_bad_timestamp(self) -> None:
        self.assertRejects(b"key,event_ts\nA,not-a-time\n", "invalid event_ts")

    def test_bad_op(self) -> None:
        self.assertRejects(b"key,event_ts,op\nA,2026-11-18T00:10:00,explode\n", "unsupported op")

    def test_non_utf8(self) -> None:
        self.assertRejects(b"key,event_ts\n\xff\xfe,2026-11-18T00:10:00\n", "not valid UTF-8")

    def test_blank_header(self) -> None:
        self.assertRejects(b",,\nA,2026-11-18T00:10:00,\n", "blank header")

    def test_one_bad_row_rejects_whole_file(self) -> None:
        with self.assertRaises(ValidationError):
            parse_csv_bytes(b"key,event_ts\nA,2026-11-18T00:10:00\nB,oops\n")


class DeliveryValidationTests(EngineTestCase):
    def test_rejected_file_leaves_no_batch_or_audit_gap(self) -> None:
        before = self.engine.verify_audit()["entries"]
        with self.assertRaises(ValidationError):
            self.engine.deliver_bytes(self.dataset, "2026-11-18T00", b"key\nA\n")
        self.assertEqual(self.status()["pending_batches"], 0)
        self.assertEqual(self.engine.verify_audit()["entries"], before)
        self.assertTrue(self.engine.verify_audit()["ok"])

    def test_hour_before_dataset_start_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self.engine.deliver_bytes(self.dataset, "2026-11-17T23", b"key,event_ts\nA,2026-11-17T23:10:00\n")
        self.assertIn("precedes dataset start", str(ctx.exception))

    def test_hour_must_be_on_the_hour(self) -> None:
        with self.assertRaises(ValidationError):
            self.engine.deliver_bytes(self.dataset, "2026-11-18T00:30", b"key,event_ts\nA,2026-11-18T00:10:00\n")

    def test_unknown_dataset_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.engine.deliver_bytes("nope", "2026-11-18T00", b"key,event_ts\nA,2026-11-18T00:10:00\n")


if __name__ == "__main__":
    unittest.main()
