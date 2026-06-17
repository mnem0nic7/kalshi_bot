"""Append-only tick store for the data spine.

Per-partition JSONL appends (crash-safe, dep-free) on the mounted volume; an
optional Parquet compaction step rolls a partition up when pyarrow is available.
"""
from __future__ import annotations

from kalshi_bot.mm.storage import TickStore


def test_append_and_read_partition(tmp_path):
    store = TickStore(base_dir=str(tmp_path))
    store.append([{"market_ticker": "A", "spot_price": "100"}], partition="2026-06-17")
    store.append([{"market_ticker": "B", "spot_price": "101"}], partition="2026-06-17")
    rows = store.read_partition("2026-06-17")
    assert [r["market_ticker"] for r in rows] == ["A", "B"]
    assert rows[1]["spot_price"] == "101"


def test_append_empty_is_noop(tmp_path):
    store = TickStore(base_dir=str(tmp_path))
    store.append([], partition="2026-06-17")
    assert store.read_partition("2026-06-17") == []


def test_partitions_are_isolated(tmp_path):
    store = TickStore(base_dir=str(tmp_path))
    store.append([{"market_ticker": "A"}], partition="2026-06-17")
    store.append([{"market_ticker": "B"}], partition="2026-06-18")
    assert len(store.read_partition("2026-06-17")) == 1
    assert len(store.read_partition("2026-06-18")) == 1
    assert set(store.partitions()) == {"2026-06-17", "2026-06-18"}
