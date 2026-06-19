"""Offline sigma (analytic vol fair-value) eval against SETTLED markets.

Distinct from `crypto-vol-eval`, which scores the feature store's baked-in
`spot_realized_volatility_32` and is therefore INSENSITIVE to the live σ̂
estimator swap (ewma vs realized). This reads the MM spine's per-tick JSONL —
which logs BOTH `sigma_realized` and `sigma_ewma` (commit 2497b87) — recomputes
the analytic fair value `Φ(ln(S/K)/(σ√τ))` with each σ̂, joins each tick's
market to its settlement (`crypto_market_snapshots.settlement_result`), and
reports Brier(sigma_realized) vs Brier(sigma_ewma) vs Brier(mid).

RAW (uncalibrated) fair value vs RAW mid — the honest head-to-head on σ̂ quality.
Isotonic calibration is deliberately omitted (it would need an out-of-sample
split to be honest; raw Brier isolates the estimator).

Run inside a container with the mm_data volume + DB reachable, e.g.:
    docker cp scripts/offline_sigma_eval.py infra-crypto_mm_production-1:/tmp/
    docker exec infra-crypto_mm_production-1 python /tmp/offline_sigma_eval.py
"""
from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from glob import glob

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import create_async_engine

from kalshi_bot.config import get_settings
from kalshi_bot.mm.fair_value import fair_up_normal

_MM_DIR = os.environ.get("MM_DATA_DIR", "/app/data/mm")
_MAX_ABS_Z = 3.0


def _dec(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _read_ticks() -> list[dict]:
    ticks: list[dict] = []
    for path in sorted(glob(os.path.join(_MM_DIR, "2026-*.jsonl"))):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    ticks.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return ticks


async def _settlements(tickers: list[str]) -> dict[str, int]:
    """market_ticker -> label_yes (1/0) for settled markets only."""
    settings = get_settings()
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    out: dict[str, int] = {}
    try:
        stmt = text(
            "SELECT DISTINCT ON (market_ticker) market_ticker, settlement_result "
            "FROM crypto_market_snapshots "
            "WHERE market_ticker IN :tickers AND settlement_result IN ('yes','no') "
            "ORDER BY market_ticker, observed_at DESC"
        ).bindparams(bindparam("tickers", expanding=True))
        async with engine.connect() as conn:
            for chunk_start in range(0, len(tickers), 500):
                chunk = tickers[chunk_start : chunk_start + 500]
                rows = (await conn.execute(stmt, {"tickers": chunk})).all()
                for ticker, result in rows:
                    out[ticker] = 1 if result == "yes" else 0
    finally:
        await engine.dispose()
    return out


def _brier(errors: list[float]) -> float | None:
    return (sum(errors) / len(errors)) if errors else None


async def main() -> None:
    ticks = _read_ticks()
    tickers = sorted({t["market_ticker"] for t in ticks if t.get("market_ticker")})
    settled = await _settlements(tickers)

    # per asset: list of squared errors for each predictor
    err: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {"realized": [], "ewma": [], "mid": []}
    )
    skipped = 0
    for t in ticks:
        ticker = t.get("market_ticker")
        if ticker not in settled:
            skipped += 1
            continue
        y = settled[ticker]
        spot = _dec(t.get("spot_price"))
        strike = _dec(t.get("floor_strike"))
        ttc = t.get("seconds_to_close")
        sig_r = _dec(t.get("sigma_realized"))
        sig_e = _dec(t.get("sigma_ewma"))
        mid = _dec(t.get("synthetic_mid_dollars"))
        step = float(t.get("step_interval_seconds") or 60.0)
        if not (spot and strike and ttc and sig_r and sig_e and mid is not None):
            skipped += 1
            continue
        p_r = fair_up_normal(spot=spot, strike=strike, sigma=sig_r, seconds_to_close=int(ttc), step_interval_seconds=step, max_abs_z=_MAX_ABS_Z)
        p_e = fair_up_normal(spot=spot, strike=strike, sigma=sig_e, seconds_to_close=int(ttc), step_interval_seconds=step, max_abs_z=_MAX_ABS_Z)
        if p_r is None or p_e is None:
            skipped += 1
            continue
        p_mid = max(Decimal(0), min(Decimal(1), mid))
        asset = t.get("asset_symbol", "?")
        err[asset]["realized"].append(float((p_r - y) ** 2))
        err[asset]["ewma"].append(float((p_e - y) ** 2))
        err[asset]["mid"].append(float((p_mid - y) ** 2))

    # aggregate
    totals = {"realized": [], "ewma": [], "mid": []}
    print(f"\n{'asset':6} {'n':>5} {'brier_mid':>10} {'brier_real':>11} {'brier_ewma':>11}  verdict")
    print("-" * 64)
    beats_ewma = 0
    asset_count = 0
    for asset in sorted(err):
        e = err[asset]
        n = len(e["mid"])
        if not n:
            continue
        asset_count += 1
        for k in totals:
            totals[k].extend(e[k])
        bm, br, be = _brier(e["mid"]), _brier(e["realized"]), _brier(e["ewma"])
        win = "ewma<mid" if be is not None and bm is not None and be < bm else "mid"
        if win == "ewma<mid":
            beats_ewma += 1
        print(f"{asset:6} {n:>5} {bm:>10.4f} {br:>11.4f} {be:>11.4f}  {win}")

    print("-" * 64)
    bm, br, be = _brier(totals["mid"]), _brier(totals["realized"]), _brier(totals["ewma"])
    if bm is not None:
        print(f"{'TOTAL':6} {len(totals['mid']):>5} {bm:>10.4f} {br:>11.4f} {be:>11.4f}")
        print(f"\nsettled ticks scored: {len(totals['mid'])}   skipped (unsettled/degenerate): {skipped}")
        print(f"assets where EWMA fair value beats mid (Brier): {beats_ewma}/{asset_count}")
        print(f"EWMA vs mid delta (negative = EWMA better): {be - bm:+.4f}")
        print(f"realized vs ewma delta (negative = realized better): {br - be:+.4f}")
    else:
        print("No settled ticks scored — settlement join empty (markets not yet settled?).")


if __name__ == "__main__":
    asyncio.run(main())
