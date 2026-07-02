"""Preliminary gate-0 criterion-(2) backtest on REAL settled CPI events (read-only).

For each settled KXCPI event with candlestick coverage, compares at the final
pre-release state:
  - market-implied ladder  = last candlestick yes-mid per strike
  - nowcast-implied ladder = Phi((k - nowcast_point)/sigma) per strike
scored against the realized per-strike outcome (settled yes=1/no=0) via Brier.
Criterion (2) needs nowcast Brier < market Brier. SMALL-N (API keeps ~69d of
candles => ~1-2 settled CPI events) — directional, not conclusive. Also runs a
naive divergence-trade P&L (buy the side the nowcast favors when |nowcastP-mid|
clears a threshold) net of rate*p*(1-p) fees. GET-only, no orders.

  docker exec infra-app_production_blue-1 python /app/diag_cpi_criterion2_backtest.py
"""
from __future__ import annotations

import asyncio
import math
import re
import time

import httpx

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

NOWCAST_URL = "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
FEE_RATE = 0.07
SIGMAS = (0.10, 0.13, 0.15)
EVENT_TO_TARGET = {  # KXCPI event suffix -> nowcast target 'YYYY-MM'
    "26JAN": "2026-01", "26FEB": "2026-02", "26MAR": "2026-03", "26APR": "2026-04",
    "26MAY": "2026-05", "26JUN": "2026-06", "26JUL": "2026-07",
}


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def norm_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def nowcast_final_by_target(doc) -> dict:
    out = {}
    for e in doc:
        sub = e.get("chart", {}).get("subcaption")
        if not sub:
            continue
        try:
            ser = next(s for s in e["dataset"] if s["seriesname"] == "CPI Inflation")
        except (StopIteration, KeyError):
            continue
        vals = [_num(x.get("value")) for x in ser["data"] if x.get("value") not in ("", None)]
        vals = [v for v in vals if v is not None]
        if vals:
            y, m = sub.split("-")
            out[f"{int(y):04d}-{int(m):02d}"] = round(vals[-1], 4)
    return out


async def main() -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": UA}) as http:
        r = await http.get(NOWCAST_URL)
        r.raise_for_status()
        nowcast = nowcast_final_by_target(r.json())

    c = KalshiClient(settings)
    settled = await c.list_markets(series_ticker="KXCPI", status="settled", limit=1000)
    evs: dict[str, list] = {}
    for m in settled.get("markets") or []:
        evs.setdefault(str(m.get("event_ticker")), []).append(m)

    now = int(time.time())
    start = now - 120 * 86400
    print(f"settled KXCPI events: {sorted(evs)}\n")

    for ev in sorted(evs):
        suffix = ev.split("-")[-1]
        target = EVENT_TO_TARGET.get(suffix)
        point = nowcast.get(target) if target else None
        if point is None:
            continue
        strikes = []
        for m in evs[ev]:
            mm = re.search(r"-T(-?\d+(?:\.\d+)?)$", str(m.get("ticker") or ""))
            res = m.get("result")
            if not mm or res not in ("yes", "no"):
                continue
            k = float(mm.group(1))
            # final pre-release market mid from last candlestick
            try:
                cs = await c.get_market_candlesticks(
                    series_ticker="KXCPI", market_ticker=m["ticker"],
                    start_ts=start, end_ts=now, period_interval=1440,
                )
                candles = cs.get("candlesticks") or []
            except Exception:
                candles = []
            mid = None
            for cd in reversed(candles):
                vol = _num(cd.get("volume_fp")) or 0.0
                yb = _num((cd.get("yes_bid") or {}).get("close_dollars"))
                ya = _num((cd.get("yes_ask") or {}).get("close_dollars"))
                if yb is None or ya is None:
                    continue
                # skip degenerate post-settlement (0,1) full-spread candles and no-market days
                if yb <= 0.0 and ya >= 1.0:
                    continue
                if (ya - yb) > 0.90:
                    continue
                px = _num((cd.get("price") or {}).get("close_dollars"))
                mid = px if (px is not None and vol > 0 and yb <= px <= ya) else (yb + ya) / 2.0
                break
            if mid is None:
                continue
            strikes.append((k, mid, 1.0 if res == "yes" else 0.0))
        if len(strikes) < 4:
            print(f"{ev}: insufficient candle/outcome coverage ({len(strikes)} strikes)")
            continue
        strikes.sort()
        actual_band = (max((k for k, _, o in strikes if o == 1.0), default=None),
                       min((k for k, _, o in strikes if o == 0.0), default=None))
        print(f"=== {ev}  nowcast_final={point:+.3f}%  outcome in ({actual_band[0]},{actual_band[1]}]  strikes={len(strikes)} ===")
        brier_mkt = sum((mid - o) ** 2 for _, mid, o in strikes) / len(strikes)
        print(f"  market-implied Brier = {brier_mkt:.4f}")
        for sigma in SIGMAS:
            preds = [(1.0 - norm_cdf((k - point) / sigma)) for k, _, _ in strikes]
            brier_now = sum((p - o) ** 2 for p, (_, _, o) in zip(preds, strikes)) / len(strikes)
            # divergence trade: pick strikes where nowcast disagrees with mid by > 0.15
            pnl, ntr = 0.0, 0
            for (k, mid, o), p in zip(strikes, preds):
                if abs(p - mid) <= 0.15 or not (0.05 <= mid <= 0.95):
                    continue
                if p > mid:   # buy yes at ~mid, settle o
                    entry = mid; gross = o - entry
                else:         # buy no
                    entry = 1 - mid; gross = (1 - o) - entry
                pnl += gross - FEE_RATE * entry * (1 - entry)
                ntr += 1
            flag = "  <== nowcast beats market" if brier_now < brier_mkt else ""
            print(f"  sigma={sigma:.2f}: nowcast Brier={brier_now:.4f}{flag}   divergence-trade net P&L={pnl:+.3f}/contract over {ntr} strikes")
        print()

    print("NOTE: small-n (~1-2 settled events in the candle window). Directional only; the live collector "
          "accrues the real sample. Brier over strikes within one event is not independent across strikes.")


if __name__ == "__main__":
    asyncio.run(main())
