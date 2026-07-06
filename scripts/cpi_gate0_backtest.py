#!/usr/bin/env python3
"""Gate-0 backtest CLI: does the Cleveland Fed CPI nowcast beat the Kalshi market?

Sub-project A (`docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`): a
NON-TRADING, offline/read-only research script. It:

1. Fetches the Cleveland Fed nowcast archive (MoM + YoY, ~13y of point-in-time daily vintages)
   and evaluates **signal quality** — nowcast Brier vs a naive carry-forward baseline, against
   the actual BLS-derived CPI print, pooled across every point-in-time vintage.
2. Fetches every settled `KXCPI` / `KXCPIYOY` event + daily candlestick price history from
   Kalshi's public (unauthenticated) API and evaluates **market edge** — nowcast-implied ladder
   vs the Kalshi mid, net of `rate * p * (1-p)` fees, on the tradeable (near-money) subset. This
   is the **decisive** check: gate-0 fails outright if this fails, regardless of (1) and (3).
3. Reports **fillability** — near-money strike volume/OI, per event.

Only GET requests are made; nothing here places an order or touches AppContainer. Network
access is required to run this for real (`--offline` skips network and only prints the CLI
shape, for smoke-testing in an environment without egress).

Usage:
    python scripts/cpi_gate0_backtest.py [--json OUT.json] [--min-brier-margin 0.01]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import UTC, date, datetime

import httpx

from kalshi_bot.macro.backtest import (
    Gate0Verdict,
    build_monthly_series,
    evaluate_market_edge_for_event,
    evaluate_signal_quality,
    fit_baseline_sigma,
    fit_sigma_curve_from_history,
    naive_carry_forward_baseline,
    summarize_fillability,
)
from kalshi_bot.macro.ladder import (
    fetch_candlesticks,
    fetch_markets_for_event,
    fetch_settled_events,
    parse_ladder,
)
from kalshi_bot.macro.nowcast_source import ClevelandFedNowcastSource

# Default strike grids for the signal-quality (offline, all-history) evaluation. Chosen to
# comfortably span 13 years of realized prints (2020 COVID MoM deflation to the 2022 YoY spike),
# at the ~0.1pp MoM / ~0.1pp YoY granularity Kalshi actually lists strikes at.
DEFAULT_MOM_STRIKES = [round(-2.0 + 0.1 * i, 2) for i in range(46)]  # -2.0 .. 2.5
DEFAULT_YOY_STRIKES = [round(-1.0 + 0.25 * i, 2) for i in range(44)]  # -1.0 .. 9.75

MONTH_ABBREV = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def event_ticker_to_target_month(event_ticker: str) -> str | None:
    """``'KXCPI-26MAY'`` -> ``'2026-05'``; ``'CPI-24JUN'`` (legacy prefix) -> ``'2024-06'``."""
    suffix = event_ticker.rsplit("-", 1)[-1]
    if len(suffix) < 5:
        return None
    year_part, month_part = suffix[:2], suffix[2:5].upper()
    month = MONTH_ABBREV.get(month_part)
    if month is None or not year_part.isdigit():
        return None
    return f"20{year_part}-{month:02d}"


async def _run(args: argparse.Namespace) -> Gate0Verdict:
    source = ClevelandFedNowcastSource()
    mom_readings = await source.fetch("mom")
    yoy_readings = await source.fetch("yoy")

    mom_monthly = build_monthly_series(mom_readings, series="cpi", index_kind="mom")
    yoy_monthly = build_monthly_series(yoy_readings, series="cpi", index_kind="yoy")

    mom_sigma_curve = fit_sigma_curve_from_history(mom_monthly)
    yoy_sigma_curve = fit_sigma_curve_from_history(yoy_monthly)
    mom_baseline = naive_carry_forward_baseline(mom_monthly)
    yoy_baseline = naive_carry_forward_baseline(yoy_monthly)
    mom_baseline_sigma = fit_baseline_sigma(mom_monthly, mom_baseline)
    yoy_baseline_sigma = fit_baseline_sigma(yoy_monthly, yoy_baseline)

    mom_signal = evaluate_signal_quality(
        mom_monthly,
        strikes=DEFAULT_MOM_STRIKES,
        sigma_curve=mom_sigma_curve,
        baseline=mom_baseline,
        baseline_sigma_curve=mom_baseline_sigma,
    )
    yoy_signal = evaluate_signal_quality(
        yoy_monthly,
        strikes=DEFAULT_YOY_STRIKES,
        sigma_curve=yoy_sigma_curve,
        baseline=yoy_baseline,
        baseline_sigma_curve=yoy_baseline_sigma,
    )
    print(f"[signal] MoM:  nowcast_brier={mom_signal.nowcast_brier:.4f} baseline_brier={mom_signal.baseline_brier:.4f} "
          f"margin={mom_signal.margin:+.4f}  (n_months={mom_signal.n_months}, n_score_points={mom_signal.n_score_points})")
    print(f"[signal] YoY:  nowcast_brier={yoy_signal.nowcast_brier:.4f} baseline_brier={yoy_signal.baseline_brier:.4f} "
          f"margin={yoy_signal.margin:+.4f}  (n_months={yoy_signal.n_months}, n_score_points={yoy_signal.n_score_points})")
    # Pooled signal-quality result is reported per index kind above; the combined Gate0Verdict
    # takes the MoM result as primary (KXCPI is the higher-volume series) with YoY reported
    # alongside for the write-up.
    combined_signal = mom_signal

    market_edge_results = []
    fillability_results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for series_ticker, monthly, sigma_curve in (
            ("KXCPI", mom_monthly, mom_sigma_curve),
            ("KXCPIYOY", yoy_monthly, yoy_sigma_curve),
        ):
            settled_events = await fetch_settled_events(client, series_ticker)
            print(f"[kalshi] {series_ticker}: {len(settled_events)} settled events listed")
            for event in settled_events:
                event_ticker = event["event_ticker"]
                target_month = event_ticker_to_target_month(event_ticker)
                if target_month is None or target_month not in monthly:
                    continue
                monthly_series = monthly[target_month]
                if monthly_series.actual is None or monthly_series.release_date is None:
                    continue

                raw_markets = await fetch_markets_for_event(client, event_ticker)
                if not raw_markets:
                    continue  # outside the public API's price-history retention window
                rungs = parse_ladder(raw_markets)
                fillability_results.append(summarize_fillability(event_ticker, rungs))

                rung_by_ticker = {r.ticker: r for r in rungs}
                rungs_with_history: list[tuple[object, list[tuple[date, float, float]]]] = []
                for raw in raw_markets:
                    ticker = raw["ticker"]
                    rung = rung_by_ticker.get(ticker)
                    if rung is None:
                        continue
                    start_ts = int(datetime.fromisoformat(raw["open_time"].replace("Z", "+00:00")).timestamp())
                    end_ts = int(datetime.fromisoformat(raw["close_time"].replace("Z", "+00:00")).timestamp()) + 86400
                    candles = await fetch_candlesticks(
                        client, series_ticker, ticker, start_ts=start_ts, end_ts=end_ts
                    )
                    history = []
                    for c in candles:
                        yes_bid = (c.get("yes_bid") or {}).get("close_dollars")
                        yes_ask = (c.get("yes_ask") or {}).get("close_dollars")
                        if yes_bid is None or yes_ask is None:
                            continue
                        as_of = datetime.fromtimestamp(c["end_period_ts"], tz=UTC).date()
                        history.append((as_of, float(yes_bid), float(yes_ask)))
                    rungs_with_history.append((rung, history))

                result = evaluate_market_edge_for_event(
                    event_ticker, rungs_with_history, monthly_series, sigma_curve
                )
                market_edge_results.append(result)
                brier_note = (
                    f"nowcast_brier={result.nowcast_brier:.4f} mid_brier={result.kalshi_mid_brier:.4f}"
                    if result.nowcast_brier is not None
                    else "no near-money observations"
                )
                print(
                    f"[market-edge] {event_ticker}: n_obs={result.n_observations} {brier_note} "
                    f"net_pnl=${result.net_pnl_dollars:+.2f} fees=${result.fees_paid_dollars:.2f} "
                    f"beats_mid={result.beats_mid}"
                )

    verdict = Gate0Verdict(
        signal_quality=combined_signal,
        market_edge_results=market_edge_results,
        fillability_results=fillability_results,
        min_brier_margin=args.min_brier_margin,
    )
    return verdict


def _print_verdict(verdict: Gate0Verdict) -> None:
    print("\n" + "=" * 72)
    print("GATE-0 VERDICT")
    print("=" * 72)
    print(f"Part 1 (signal quality):      {'PASS' if verdict.part1_signal_passes else 'FAIL'} "
          f"(margin={verdict.signal_quality.margin:+.4f}, threshold={verdict.min_brier_margin})")
    print(f"Part 2 (market edge, DECISIVE): {'PASS' if verdict.part2_market_edge_passes else 'FAIL'} "
          f"({sum(1 for r in verdict.market_edge_results if r.n_observations > 0)} scored events)")
    print(f"Part 3 (fillability):         {'PASS' if verdict.part3_fillability_passes else 'FAIL'}")
    print("-" * 72)
    print(f"OVERALL: {'GO' if verdict.overall_go else 'NO-GO'}")
    print("=" * 72)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=str, default=None, help="write the verdict + raw results to this JSON path")
    parser.add_argument("--min-brier-margin", type=float, default=0.01, help="part-1 pass threshold")
    args = parser.parse_args(argv)

    verdict = asyncio.run(_run(args))
    _print_verdict(verdict)

    if args.json:
        payload = {
            "generated_at": datetime.now(UTC).isoformat(),
            "signal_quality": asdict(verdict.signal_quality),
            "market_edge_results": [asdict(r) for r in verdict.market_edge_results],
            "fillability_results": [asdict(r) for r in verdict.fillability_results],
            "part1_signal_passes": verdict.part1_signal_passes,
            "part2_market_edge_passes": verdict.part2_market_edge_passes,
            "part3_fillability_passes": verdict.part3_fillability_passes,
            "overall_go": verdict.overall_go,
        }
        with open(args.json, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"wrote {args.json}")

    return 0 if verdict.overall_go else 1


if __name__ == "__main__":
    sys.exit(main())
