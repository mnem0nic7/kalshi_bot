"""Append-only tick store for the data spine (plan §4.1 storage).

Per-partition (per-UTC-day) JSONL appends on a mounted volume: crash-safe,
dependency-free, and the prerequisite dataset for fair-value calibration and
backtesting. ``compact_to_parquet`` rolls a partition into a columnar Parquet
file when pyarrow is available (the format chosen for the offline dataset); the
hot path stays JSONL so a partial write never corrupts a columnar file.
"""
from __future__ import annotations

import json
import os
from typing import Any


class TickStore:
    def __init__(self, base_dir: str) -> None:
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _path(self, partition: str) -> str:
        return os.path.join(self.base_dir, f"{partition}.jsonl")

    def append(self, rows: list[dict[str, Any]], *, partition: str) -> int:
        if not rows:
            return 0
        with open(self._path(partition), "a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=str) + "\n")
        return len(rows)

    def read_partition(self, partition: str) -> list[dict[str, Any]]:
        path = self._path(partition)
        if not os.path.exists(path):
            return []
        rows: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def partitions(self) -> list[str]:
        return sorted(
            name[: -len(".jsonl")]
            for name in os.listdir(self.base_dir)
            if name.endswith(".jsonl")
        )

    def compact_to_parquet(self, partition: str) -> str | None:
        """Roll a JSONL partition into a Parquet file. No-op if pyarrow is absent."""
        rows = self.read_partition(partition)
        if not rows:
            return None
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            return None
        table = pa.Table.from_pylist(rows)
        out_path = os.path.join(self.base_dir, f"{partition}.parquet")
        pq.write_table(table, out_path)
        return out_path
