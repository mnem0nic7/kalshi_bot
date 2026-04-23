#!/usr/bin/env python3
"""Backfill cli_reconciliation from historical corpus (Session 5, §4.1.5).

For each station × observation_date with a settled label carrying crosscheck_high_f,
finds the highest current_temp_f across all weather snapshots for that station/date
and writes a cli_reconciliation row (cli_value - asos_observed_max = delta_degf).

Rows use ON CONFLICT DO UPDATE so reruns are safe.

Usage:
    python scripts/backfill_cli_reconciliation.py [--dry-run] [--station STATION]

Exit code 0 on success, 1 on failure.
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
from collections import defaultdict
from datetime import UTC, date, datetime

from sqlalchemy import and_, select, text

from kalshi_bot.config import get_settings
from kalshi_bot.db.models import (
    CliReconciliationRecord,
    CliStationVariance,
    HistoricalSettlementLabelRecord,
    HistoricalWeatherSnapshotRecord,
)
from kalshi_bot.db.session import create_engine, create_session_factory

# Source kinds we trust for the peak observation (same priority as refit script).
_TRUSTED_SOURCE_KINDS = {
    "checkpoint_archived_weather_bundle",
    "coverage_repair_checkpoint_promotion",
    "external_forecast_archive_weather_bundle",
    "archived_weather_bundle",
    "daemon_checkpoint_capture",
}


async def run(dry_run: bool, station_filter: str | None) -> int:
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    # Step 1: load settled labels with crosscheck_high_f
    async with session_factory() as session:
        where_clauses = [
            HistoricalSettlementLabelRecord.crosscheck_high_f.is_not(None),
            HistoricalSettlementLabelRecord.series_ticker.is_not(None),
        ]
        labels_stmt = select(HistoricalSettlementLabelRecord).where(*where_clauses)
        labels = (await session.execute(labels_stmt)).scalars().all()
    print(f"[backfill] {len(labels)} settled labels with crosscheck_high_f")

    # Step 2: for each label, find the peak ASOS reading for that station/date.
    # Keyed by (series_ticker, local_market_day) → best snapshot (highest current_temp_f).
    async with session_factory() as session:
        # Load all trusted weather snapshots into memory grouped by (series_ticker, date).
        snaps_stmt = select(HistoricalWeatherSnapshotRecord).where(
            HistoricalWeatherSnapshotRecord.current_temp_f.is_not(None),
            HistoricalWeatherSnapshotRecord.station_id.is_not(None),
        )
        if station_filter:
            snaps_stmt = snaps_stmt.where(
                HistoricalWeatherSnapshotRecord.station_id == station_filter
            )
        all_snaps = (await session.execute(snaps_stmt)).scalars().all()

    print(f"[backfill] {len(all_snaps)} weather snapshots loaded")

    # Index snapshots by (series_ticker, local_market_day) → list of snapshots.
    snap_index: dict[tuple[str, str], list[HistoricalWeatherSnapshotRecord]] = defaultdict(list)
    for snap in all_snaps:
        if snap.series_ticker and snap.source_kind in _TRUSTED_SOURCE_KINDS:
            snap_index[(snap.series_ticker, snap.local_market_day)].append(snap)

    # Step 3: build cli_reconciliation rows.
    rows_to_write: list[dict] = []
    skipped = 0

    for label in labels:
        series_ticker = label.series_ticker
        local_day = label.local_market_day
        cli_value = float(label.crosscheck_high_f)

        snaps = snap_index.get((series_ticker, local_day), [])
        if not snaps:
            skipped += 1
            continue

        # Find snapshot with the highest current_temp_f (ASOS peak for the day).
        peak_snap = max(snaps, key=lambda s: float(s.current_temp_f))
        asos_max = float(peak_snap.current_temp_f)

        if station_filter and peak_snap.station_id != station_filter:
            continue

        try:
            obs_date = date.fromisoformat(local_day)
        except (ValueError, TypeError):
            skipped += 1
            continue

        rows_to_write.append({
            "station": peak_snap.station_id,
            "observation_date": obs_date,
            "asos_observed_max": asos_max,
            "asos_observed_at": peak_snap.observation_ts,
            "cli_value": cli_value,
            "cli_published_at": label.settlement_ts,
            "delta_degf": round(cli_value - asos_max, 4),
            "note": f"backfill from series={series_ticker}",
        })

    print(f"[backfill] {len(rows_to_write)} rows to write, {skipped} skipped (no snapshot)")

    if dry_run:
        for r in rows_to_write[:5]:
            print(f"  DRY RUN: {r['station']} {r['observation_date']} "
                  f"asos={r['asos_observed_max']:.1f} cli={r['cli_value']:.1f} "
                  f"delta={r['delta_degf']:+.2f}")
        if len(rows_to_write) > 5:
            print(f"  ... and {len(rows_to_write) - 5} more")
        print("[backfill] dry-run — no DB writes")
        return 0

    # Step 4: upsert rows via SQLAlchemy (merge pattern for idempotency).
    async with session_factory() as session:
        written = 0
        for r in rows_to_write:
            existing = await session.get(
                CliReconciliationRecord,
                (r["station"], r["observation_date"]),
            )
            if existing is not None:
                existing.asos_observed_max = r["asos_observed_max"]
                existing.asos_observed_at = r["asos_observed_at"]
                existing.cli_value = r["cli_value"]
                existing.cli_published_at = r["cli_published_at"]
                existing.delta_degf = r["delta_degf"]
                existing.note = r["note"]
            else:
                session.add(CliReconciliationRecord(**r))
            written += 1

        await session.commit()
    print(f"[backfill] wrote/updated {written} cli_reconciliation rows")

    # Step 5: refresh cli_station_variance rollup.
    await _refresh_station_variance(session_factory)
    return 0


async def _refresh_station_variance(session_factory) -> None:
    """Recompute cli_station_variance from all cli_reconciliation rows."""
    async with session_factory() as session:
        rows = (await session.execute(select(CliReconciliationRecord))).scalars().all()

    by_station: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        by_station[r.station].append(r.delta_degf)

    async with session_factory() as session:
        now = datetime.now(UTC)
        for station, deltas in sorted(by_station.items()):
            if len(deltas) < 2:
                continue
            n = len(deltas)
            signed_mean = statistics.mean(deltas)
            signed_std = statistics.stdev(deltas)
            abs_deltas = [abs(d) for d in deltas]
            mean_abs = statistics.mean(abs_deltas)
            sorted_abs = sorted(abs_deltas)
            p95_idx = max(0, int(0.95 * n) - 1)
            p95_abs = sorted_abs[p95_idx]

            existing = await session.get(CliStationVariance, station)
            if existing is not None:
                existing.sample_count = n
                existing.signed_mean_delta_degf = signed_mean
                existing.signed_stddev_delta_degf = signed_std
                existing.mean_abs_delta_degf = mean_abs
                existing.p95_abs_delta_degf = p95_abs
                existing.last_refreshed_at = now
            else:
                session.add(CliStationVariance(
                    station=station,
                    sample_count=n,
                    signed_mean_delta_degf=signed_mean,
                    signed_stddev_delta_degf=signed_std,
                    mean_abs_delta_degf=mean_abs,
                    p95_abs_delta_degf=p95_abs,
                    last_refreshed_at=now,
                ))

        await session.commit()
    print(f"[backfill] refreshed cli_station_variance for {len(by_station)} stations")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill cli_reconciliation from historical corpus")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    parser.add_argument("--station", default=None, help="Limit to a single station ID (e.g. KBOS)")
    args = parser.parse_args()

    exit_code = asyncio.run(run(dry_run=args.dry_run, station_filter=args.station))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
