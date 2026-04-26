#!/usr/bin/env python3
"""Refit per-station sigma calibration (Addition 2, §4.2).

Stage 1: fit σ_base(station, season) across all lead times.
Stage 2: fit global lead_factor(lead_bucket) across all stations.

Writes results to station_sigma_params and global_lead_factor tables.
All prior active rows are deactivated; new rows become active.

Usage:
    python scripts/refit_station_sigma.py [--dry-run] [--version VERSION]

Exit code 0 on success, 1 on failure.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import UTC, datetime

from sqlalchemy import and_, select

from kalshi_bot.config import get_settings
from kalshi_bot.db.models import HistoricalSettlementLabelRecord, HistoricalWeatherSnapshotRecord
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.weather.sigma_calibration import (
    fit_lead_factors,
    fit_sigma_base,
    lead_bucket_for_hours,
    persist_lead_factors,
    persist_sigma_params,
    season_for_month,
)

_PREFERRED_SOURCE_KINDS = (
    "checkpoint_archived_weather_bundle",
    "coverage_repair_checkpoint_promotion",
    "external_forecast_archive_weather_bundle",
    "archived_weather_bundle",
    "daemon_checkpoint_capture",
)

# Global fallback σ used for CRPS baseline comparison.
_GLOBAL_SIGMA = 3.5
_GLOBAL_BIAS = 0.0


async def run(dry_run: bool, version: str) -> int:
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    # residuals keyed by (station, season_bucket) for Stage 1
    # and by lead_bucket for Stage 2
    by_station_season: dict[tuple[str, str], list[float]] = defaultdict(list)
    by_lead: dict[str, list[float]] = defaultdict(list)
    lead_counts: dict[str, int] = defaultdict(int)

    async with session_factory() as session:
        labels_stmt = select(HistoricalSettlementLabelRecord).where(
            HistoricalSettlementLabelRecord.crosscheck_high_f.is_not(None),
            HistoricalSettlementLabelRecord.series_ticker.is_not(None),
        )
        labels = (await session.execute(labels_stmt)).scalars().all()
        print(f"[refit] {len(labels)} settled labels with crosscheck_high_f")

        matched = 0
        for label in labels:
            series_ticker = label.series_ticker
            actual_high = float(label.crosscheck_high_f)

            snap = None
            for source_kind in _PREFERRED_SOURCE_KINDS:
                snap_stmt = (
                    select(HistoricalWeatherSnapshotRecord)
                    .where(
                        and_(
                            HistoricalWeatherSnapshotRecord.series_ticker == series_ticker,
                            HistoricalWeatherSnapshotRecord.local_market_day == label.local_market_day,
                            HistoricalWeatherSnapshotRecord.source_kind == source_kind,
                        )
                    )
                    .limit(1)
                )
                snap = (await session.execute(snap_stmt)).scalar_one_or_none()
                if snap is not None:
                    break

            if snap is None or snap.forecast_high_f is None:
                continue

            forecast_high = float(snap.forecast_high_f)
            residual = actual_high - forecast_high

            # Derive season from settlement month
            settlement_date = label.local_market_day  # YYYY-MM-DD string
            try:
                month = int(settlement_date[5:7])
            except (ValueError, TypeError, IndexError):
                continue
            season = season_for_month(month)

            # Derive station from series template (use series_ticker prefix → station mapping)
            station = snap.station_id or ""
            if not station:
                continue

            by_station_season[(station, season)].append(residual)

            # Lead time from asof_ts vs settlement_ts
            if snap.asof_ts and label.settlement_ts:
                try:
                    asof = snap.asof_ts if snap.asof_ts.tzinfo else snap.asof_ts.replace(tzinfo=UTC)
                    settle = label.settlement_ts if label.settlement_ts.tzinfo else label.settlement_ts.replace(tzinfo=UTC)
                    lead_hours = (settle - asof).total_seconds() / 3600
                    bucket = lead_bucket_for_hours(lead_hours)
                    by_lead[bucket].append(residual)
                    lead_counts[bucket] += 1
                except Exception:
                    pass

            matched += 1

        print(f"[refit] {matched} matched (forecast, actual) pairs")

    # Stage 1: fit σ_base per (station, season)
    print("[refit] Stage 1: fitting σ_base per (station, season)")
    stage1_results: dict[tuple[str, str], dict] = {}
    for (station, season), residuals in sorted(by_station_season.items()):
        params = fit_sigma_base(residuals, global_sigma=_GLOBAL_SIGMA, global_bias=_GLOBAL_BIAS)
        if not params:
            print(f"  skip  {station}/{season}: {len(residuals)} samples (insufficient)")
            continue
        crps = params.get("crps_improvement_vs_global")
        print(
            f"  {station}/{season}: n={params['sample_count']:4d}  "
            f"σ={params['sigma_base_f']:.2f}  bias={params['mean_bias_f']:+.2f}  "
            f"SE={params['sigma_se_f']:.2f}  CRPS_Δ={crps:+.4f}" if crps is not None else
            f"  {station}/{season}: n={params['sample_count']:4d}  σ={params['sigma_base_f']:.2f}"
        )
        stage1_results[(station, season)] = params

    # Stage 2: fit lead factors
    print("[refit] Stage 2: fitting global lead_factor")
    lead_factors = fit_lead_factors(by_lead)
    for bucket, factor in sorted(lead_factors.items()):
        n = lead_counts.get(bucket, 0)
        print(f"  {bucket}: factor={factor:.3f}  n={n}")

    if dry_run:
        print("[refit] dry-run mode — no DB writes")
        return 0

    async with session_factory() as session:
        for (station, season), params in stage1_results.items():
            await persist_sigma_params(session, station, season, params, version)

        await persist_lead_factors(session, lead_factors, dict(lead_counts), version)
        await session.commit()

    total = len(stage1_results)
    print(f"[refit] wrote {total} station_sigma_params rows + {len(lead_factors)} global_lead_factor rows (version={version})")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Refit per-station sigma calibration")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    parser.add_argument(
        "--version",
        default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        help="Version tag for this fit (default: UTC timestamp)",
    )
    args = parser.parse_args()

    exit_code = asyncio.run(run(dry_run=args.dry_run, version=args.version))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
