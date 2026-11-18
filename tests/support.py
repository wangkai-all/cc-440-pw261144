"""Shared fixtures for the incmerge test-suite (stdlib unittest only)."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from incmerge.engine import Engine  # noqa: E402

HEADER = "key,event_ts,op,status,amount\n"


def csv_bytes(*rows: str) -> bytes:
    return (HEADER + "".join(row + "\n" for row in rows)).encode("utf-8")


def row(key: str, ts: str, op: str = "upsert", status: str = "new", amount: str = "1.00") -> str:
    return f"{key},{ts},{op},{status},{amount}"


class EngineTestCase(unittest.TestCase):
    dataset = "orders"
    start = "2026-11-18T00"

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="incmerge-test-")
        self.db = os.path.join(self.tmp, "test.db")
        self.engine = Engine(self.db, actor="test")
        self.engine.ensure_dataset(self.dataset, self.start)

    def tearDown(self) -> None:
        self.engine.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # helpers -----------------------------------------------------------
    def ingest(self, hour: str, content: bytes, dataset: str | None = None) -> dict:
        ds = dataset or self.dataset
        delivery = self.engine.deliver_bytes(ds, hour, content, filename=f"{hour}.csv")
        if delivery["outcome"] == "conflict":
            return delivery
        merged = self.engine.merge(delivery["batch_id"])
        delivery["merge"] = merged
        return delivery

    def status(self, dataset: str | None = None) -> dict:
        return self.engine.status(dataset or self.dataset)["datasets"][0]

    def record(self, key: str, dataset: str | None = None) -> dict | None:
        for item in self.engine.records(dataset or self.dataset, limit=10_000):
            if item["key"] == key:
                return item
        return None
