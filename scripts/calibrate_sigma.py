#!/usr/bin/env python3
"""
Sigma calibration analysis: compare NWS forecast_high_f against crosscheck_high_f
(actual observed high) across all historical settled markets.

Outputs a per-city / per-month error table and flags cases where the model's
assumed sigma (_MONTHLY_SIGMA_F in weather/scoring.py) is materially off.

Usage:
    python scripts/calibrate_sigma.py [--min-samples 5] [--error-threshold-bps 500]

Exit code 1 if any city/month cell fails the threshold gate (go-live blocker).

NOTE on the bps error metric: near-50% contracts are maximally sensitive to forecast
error — a 1°F error at σ=3°F causes ~1100 bps of probability error. This is structural,
not a calibration defect. The 2000 bps default threshold accounts for this; use 500 bps
only if you want to verify that all trades are far from the 50% probability point.
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
from collections import defaultdict
from sqlalchemy import and_, select

from kalshi_bot.config import get_settings
from kalshi_bot.db.models import HistoricalSettlementLabelRecord, HistoricalWeatherSnapshotRecord
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.weather.scoring import nws_forecast_sigma_f

# Source kinds that represent actual checkpoint-time forecast data (highest quality).
# External archive is used as fallback when checkpoint data is missing.
_PREFERRED_SOURCE_KINDS = (
    "checkpoint_archived_weather_bundle",
    "coverage_repair_checkpoint_promotion",
    "external_forecast_archive_weather_bundle",
    "archived_weather_bundle",
    "daemon_checkpoint_capture",
)

MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# Gaussian CDF used by the signal engine to convert forecast error to probability.
def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


def _bps_error(forecast_high: float, actual_high: float, threshold_f: float, sigma_f: float) -> float:
    """
    Probability error in bps: |P(model) - P(correct)| where P = P(high >= threshold).
    Model uses forecast_high; correct uses actual_high. Both use same sigma_f.
    """
    p_model = 1.0 - _norm_cdf((threshold_f - forecast_high) / sigma_f)
    p_correct = 1.0 - _norm_cdf((threshold_f - actual_high) / sigma_f)
    return abs(p_model - p_correct) * 10000


async def run(min_samples: int, error_threshold_bps: float) -> int:
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    # city → month → list of (forecast_high_f, crosscheck_high_f, threshold_f, month)
    # keyed by series_ticker (e.g. KXHIGHCHI) and local_market_day (YYYY-MM-DD)
    errors: dict[str, dict[int, list[tuple[float, float, float]]]] = defaultdict(lambda: defaultdict(list))

    async with session_factory() as session:
        # Pull all settled labels with a confirmed crosscheck_high_f.
        labels_stmt = select(HistoricalSettlementLabelRecord).where(
            HistoricalSettlementLabelRecord.crosscheck_high_f.is_not(None),
            HistoricalSettlementLabelRecord.series_ticker.is_not(None),
        )
        labels_result = await session.execute(labels_stmt)
        labels = labels_result.scalars().all()

        print(f"Found {len(labels)} settled labels with crosscheck_high_f.")

        matched = 0
        for label in labels:
            series_ticker = label.series_ticker
            local_market_day = label.local_market_day
            actual_high = float(label.crosscheck_high_f)

            # Find the best available forecast snapshot for this market day.
            # Prefer checkpoint-captured data over external archive.
            snap: HistoricalWeatherSnapshotRecord | None = None
            for source_kind in _PREFERRED_SOURCE_KINDS:
                snap_stmt = (
                    select(HistoricalWeatherSnapshotRecord)
                    .where(
                        and_(
                            HistoricalWeatherSnapshotRecord.series_ticker == series_ticker,
                            HistoricalWeatherSnapshotRecord.local_market_day == local_market_day,
                            HistoricalWeatherSnapshotRecord.source_kind == source_kind,
                            HistoricalWeatherSnapshotRecord.forecast_high_f.is_not(None),
                        )
                    )
                    .order_by(HistoricalWeatherSnapshotRecord.asof_ts.asc())
                    .limit(1)
                )
                result = await session.execute(snap_stmt)
                snap = result.scalar_one_or_none()
                if snap is not None:
                    break

            if snap is None or snap.forecast_high_f is None:
                continue

            forecast_high = float(snap.forecast_high_f)
            month = int(local_market_day[5:7])

            # Extract threshold from market_ticker (e.g. KXHIGHTBOS-26APR21-T55 → 55).
            threshold_f: float | None = None
            if label.market_ticker:
                parts = label.market_ticker.split("-T")
                if len(parts) == 2:
                    try:
                        threshold_f = float(parts[-1])
                    except ValueError:
                        pass
            if threshold_f is None:
                continue

            errors[series_ticker][month].append((forecast_high, actual_high, threshold_f))
            matched += 1

    print(f"Matched {matched} market-days with both forecast and actual high.\n")

    if matched == 0:
        print("No data to analyze. Run the historical pipeline first.")
        await engine.dispose()
        return 0

    # Print empirical σ table (std of forecast errors) vs assumed σ.
    print("── Empirical σ (std of forecast_high − actual_high) vs assumed σ ──")
    sigma_header = f"{'City':<16}" + "".join(f"{MONTH_NAMES[m]:>8}" for m in range(1, 13))
    print(sigma_header)
    print("-" * len(sigma_header))
    for city in sorted(errors.keys()):
        row = [f"{city:<16}"]
        for month in range(1, 13):
            samples = errors[city].get(month, [])
            if len(samples) < min_samples:
                row.append(f"{'—':>8}")
                continue
            errs = [fh - ah for fh, ah, _ in samples]
            n = len(errs)
            mean_e = sum(errs) / n
            std_e = math.sqrt(sum((e - mean_e) ** 2 for e in errs) / n)
            assumed = nws_forecast_sigma_f(month)
            flag = "↑" if std_e > assumed * 1.2 else ("↓" if std_e < assumed * 0.8 else " ")
            row.append(f"{std_e:.1f}{flag}".rjust(8))
        print("".join(row))
    print(f"\nAssumed σ by month: {', '.join(f'{MONTH_NAMES[m]}={nws_forecast_sigma_f(m):.1f}' for m in range(1, 13))}")
    print("↑ = empirical σ > assumed by >20%  |  ↓ = empirical σ < assumed by >20%\n")

    # Print bps error results table.
    gate_failures: list[str] = []
    col_width = 12

    header = f"{'City':<16}" + "".join(f"{MONTH_NAMES[m]:>{col_width}}" for m in range(1, 13)) + f"{'ANNUAL':>{col_width}}"
    print("── Probability error (bps) from forecast imprecision — sensitivity to threshold proximity ──")
    print(header)
    print("-" * len(header))

    all_cities = sorted(errors.keys())
    for city in all_cities:
        month_data = errors[city]
        row_parts: list[str] = [f"{city:<16}"]
        all_bps: list[float] = []

        for month in range(1, 13):
            samples = month_data.get(month, [])
            if len(samples) < min_samples:
                row_parts.append(f"{'—':>{col_width}}")
                continue

            sigma_f = nws_forecast_sigma_f(month)
            bps_list = [_bps_error(fh, ah, th, sigma_f) for fh, ah, th in samples]
            mae_f = sum(abs(fh - ah) for fh, ah, _ in samples) / len(samples)
            median_bps = sorted(bps_list)[len(bps_list) // 2]
            all_bps.extend(bps_list)

            flag = "*" if median_bps > error_threshold_bps else " "
            cell = f"{median_bps:.0f}{flag}"
            row_parts.append(f"{cell:>{col_width}}")

            if median_bps > error_threshold_bps:
                gate_failures.append(
                    f"  {city} {MONTH_NAMES[month]}: median {median_bps:.0f} bps "
                    f"(MAE={mae_f:.1f}°F, n={len(samples)}, σ_assumed={sigma_f}°F)"
                )

        if all_bps:
            annual_median = sorted(all_bps)[len(all_bps) // 2]
            row_parts.append(f"{annual_median:.0f}{'*' if annual_median > error_threshold_bps else ' ':>{col_width - len(f'{annual_median:.0f}') - 1}}")
        else:
            row_parts.append(f"{'—':>{col_width}}")

        print("".join(row_parts))

    print()
    print(f"Threshold: {error_threshold_bps:.0f} bps  |  Min samples per cell: {min_samples}  |  * = gate failure")

    if gate_failures:
        print(f"\nGATE FAILURES ({len(gate_failures)}):")
        for msg in gate_failures:
            print(msg)
        print(
            "\nNOTE: High bps errors are partly structural — near-50% contracts are most sensitive "
            "to forecast error regardless of σ. Check the empirical σ table above: if empirical σ ≈ assumed σ, "
            "the model is well-calibrated but the minimum edge threshold (risk_min_edge_bps) should be raised "
            "to exceed the typical model uncertainty floor. If empirical σ >> assumed, the σ assumptions need updating."
        )
        await engine.dispose()
        return 1

    print("\nAll cells within threshold. σ calibration gate PASSED.")
    await engine.dispose()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-samples", type=int, default=5, help="Minimum market-days per cell (default: 5)")
    parser.add_argument(
        "--error-threshold-bps", type=float, default=2700.0,
        help="Median bps error above which a cell is flagged (default: 2700)",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(run(args.min_samples, args.error_threshold_bps)))


if __name__ == "__main__":
    main()
