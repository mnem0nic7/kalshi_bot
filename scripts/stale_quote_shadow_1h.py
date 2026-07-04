"""1h stale-quote SHADOW scanner — signals only, NEVER orders.

Measures whether hourly bracket/above-below books go stale the way 15m books
do (docs/research/2026-07-02-stale-quote-taker-edge.md). Emits signal JSONL +
settle records for the graduation backtest. Graduation to a capped live pilot
requires the same 3-check OOS validation as 15m, then operator go.

  docker exec infra-app_production_green-1 env STALE_1H_STDOUT=1 \
      python /app/stale_quote_shadow_1h.py
"""
from __future__ import annotations

import asyncio
import json
import math
import os
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.crypto.stale_quote_shadow_1h import (
    SERIES_1H, cap_moneyness, in_strike_band, range_fair,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.crypto.services import (
    CRYPTO_SPOT_CONTEXT_LIVE,
    _crypto_vol_normal_fair_up,
    _decimal,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

VOL_MODEL = {  # lockstep with scripts/stale_quote_pilot.py
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}
POLL_S = 5.0          # shadow: no fill race, keep request load low
LOOKBACK_S = 18.0
DFAIR_TH = 0.10
QUOTE_EPS = 0.01
MIN_TTC_S = 90
MAX_TTC_S = 3300      # up to 55 min out on an hourly market
MAX_SPREAD = 0.20

OUT_DIR = os.environ.get("STALE_1H_OUT", "/app/data/stale_shadow_1h")
STDOUT = os.environ.get("STALE_1H_STDOUT") == "1"


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def emit(rec: dict) -> None:
    line = json.dumps(rec)
    if STDOUT:
        print(line, flush=True)
    else:
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "shadow_1h.jsonl"), "a") as fh:
            fh.write(line + "\n")


def fair_for_market(row: dict, floor_strike: float, cap_strike: float | None):
    up_floor = _crypto_vol_normal_fair_up(row, VOL_MODEL)
    if up_floor is None:
        return None
    if cap_strike is None:
        return float(up_floor)
    mny = row.get("spot_moneyness_pct")
    if mny is None:
        return None
    cap_row = {**row, "spot_moneyness_pct": cap_moneyness(float(mny), floor_strike, cap_strike)}
    up_cap = _crypto_vol_normal_fair_up(cap_row, VOL_MODEL)
    if up_cap is None:
        return None
    return range_fair(float(up_floor), float(up_cap))


async def main() -> None:
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    client = KalshiClient(settings)
    hist: dict[str, list] = {}
    signaled: set[str] = set()
    last_seen: dict[str, datetime] = {}
    open_signals: list[dict] = []   # {ticker, side, entry, settle_by}
    emit({"type": "start", "ts": datetime.now(UTC).isoformat(),
          "mode": "shadow_1h", "assets": sorted(SERIES_1H)})

    async def reconcile() -> None:
        nowt = datetime.now(UTC)
        still = []
        for tr in open_signals:
            if nowt < tr["settle_by"]:
                still.append(tr)
                continue
            try:
                m = (await client.get_market(tr["ticker"])).get("market") or {}
            except Exception:
                still.append(tr)
                continue
            res = m.get("result")
            if res not in ("yes", "no"):
                still.append(tr)
                continue
            y = 1.0 if res == "yes" else 0.0
            gross = (y - tr["entry"]) if tr["side"] == "yes" else ((1.0 - y) - tr["entry"])
            net = gross - 0.07 * tr["entry"] * (1 - tr["entry"])
            emit({"type": "settle", "ts": nowt.isoformat(), "mode": "shadow_1h",
                  "ticker": tr["ticker"], "result": res, "net": round(net, 4)})
        open_signals[:] = still
        # Eviction: hourly markets are long closed 2h after last activity —
        # drop them so hist/signaled/last_seen don't grow unbounded over weeks.
        cutoff = nowt - timedelta(hours=2)
        stale_tickers = [tk for tk, ts in last_seen.items() if ts < cutoff]
        for tk in stale_tickers:
            hist.pop(tk, None)
            signaled.discard(tk)
            last_seen.pop(tk, None)

    last_reconcile = 0.0
    while True:
        cycle_t0 = asyncio.get_event_loop().time()
        if cycle_t0 - last_reconcile > 120:
            await reconcile()
            last_reconcile = cycle_t0
        now = datetime.now(UTC)
        for asset, series in SERIES_1H.items():
            try:
                async with factory() as session:
                    repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
                    rows = await repo.list_crypto_spot_ohlc(
                        frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
                        since=now - timedelta(hours=2), limit=100_000,
                    )
                rows = sorted(rows, key=lambda r: r.end_ts)
                prep = _prepare_spot_context_series(rows)
                spot_hint = None
                if rows and rows[-1].close_dollars is not None:
                    spot_hint = f(rows[-1].close_dollars)
                resp = await client.list_markets(series_ticker=series, status="open", limit=200)
            except Exception as e:
                emit({"type": "error", "ts": now.isoformat(), "asset": asset, "err": str(e)[:200]})
                continue
            for m in resp.get("markets") or []:
                tk = str(m.get("ticker"))
                yb, ya = f(m.get("yes_bid_dollars")), f(m.get("yes_ask_dollars"))
                strike = f(m.get("floor_strike"))
                cap = f(m.get("cap_strike"))
                ct = m.get("close_time")
                if yb is None or ya is None or strike is None or not ct:
                    continue
                # ladder-churn guard FIRST — ~50 strikes/hourly ladder, only the
                # near-money ones are worth the per-market spot-context cost.
                if spot_hint is None or not in_strike_band(spot_hint, strike):
                    continue
                last_seen[tk] = now
                try:
                    cl = datetime.fromisoformat(str(ct).replace("Z", "+00:00"))
                except ValueError:
                    continue
                ttc = (cl - now).total_seconds()
                if not (MIN_TTC_S <= ttc <= MAX_TTC_S) or (ya - yb) > MAX_SPREAD:
                    continue
                ctx = _spot_context_for_decision(
                    rows, prepared=prep, decision_ts=now, target_price=_decimal(str(strike)),
                    mid_yes=_decimal(str((yb + ya) / 2)), settings=settings,
                    mode=CRYPTO_SPOT_CONTEXT_LIVE,
                )
                if not ctx:
                    continue
                spot_now = f(ctx.get("spot_close_dollars"))
                mny = f(ctx.get("spot_moneyness_pct"))
                if spot_now is None:
                    continue
                row = {**ctx, "time_to_close_seconds": int(ttc)}
                fair_now = fair_for_market(row, strike, cap)
                mid_now = (yb + ya) / 2
                h = hist.setdefault(tk, [])
                h.append((now, mid_now, spot_now))
                if len(h) > 40:
                    del h[: len(h) - 40]
                if tk in signaled or fair_now is None or mny is None:
                    continue
                ref = None
                for ts0, mid0, sp0 in reversed(h[:-1]):
                    if (now - ts0).total_seconds() >= LOOKBACK_S:
                        ref = (ts0, mid0, sp0)
                        break
                if not ref or not ref[2]:
                    continue
                ref_row = {**row, "spot_moneyness_pct": mny - math.log(spot_now / ref[2])}
                fair_ref = fair_for_market(ref_row, strike, cap)
                if fair_ref is None:
                    continue
                dfair = float(fair_now) - float(fair_ref)
                stale = abs(mid_now - ref[1]) <= QUOTE_EPS
                if abs(dfair) < DFAIR_TH or not stale:
                    continue
                signaled.add(tk)
                side = "yes" if dfair > 0 else "no"
                entry = ya if side == "yes" else 1.0 - yb
                if not (0.03 <= entry <= 0.97):
                    continue
                emit({"type": "signal", "ts": now.isoformat(), "mode": "shadow_1h",
                      "asset": asset, "ticker": tk, "side": side, "is_range": cap is not None,
                      "dfair": round(dfair, 4), "entry": round(entry, 4),
                      "fair": round(float(fair_now), 4), "mid": round(mid_now, 4),
                      "ttc_s": int(ttc)})
                open_signals.append({"ticker": tk, "side": side, "entry": entry,
                                     "settle_by": cl + timedelta(seconds=120)})
        elapsed = asyncio.get_event_loop().time() - cycle_t0
        await asyncio.sleep(max(1.0, POLL_S - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
