"""Stale-quote SHADOW logger (GET-only, zero orders) with quote-persistence probing.

Runs the backtested stale-quote rule live (docs/research/2026-07-02-stale-quote-taker-edge.md):
every POLL_S seconds, per asset: freshest DB spot tick + live Kalshi 15m quotes.
Signal = fresh-spot move shifts analytic fair value >= DFAIR_TH while the quote
mid moved <= QUOTE_EPS over the same ~LOOKBACK_S window. On signal it does NOT
trade — it re-polls the SAME market at +2s and +5s and logs whether the quoted
side was still crossable at (or better than) the signal price. That directly
measures the fill-reality question (quote persistence) with zero money at risk.

Appends JSONL to $STALE_SHADOW_OUT (default /app/data/stale_shadow/signals.jsonl);
with STALE_SHADOW_STDOUT=1 emits JSONL lines on stdout for host-disk capture.

  docker exec infra-trainer_production-1 env STALE_SHADOW_STDOUT=1 \
      python /app/stale_quote_shadow.py BTC,BNB,HYPE,DOGE >> host.jsonl
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import sys
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.crypto.services import (
    CRYPTO_SPOT_CONTEXT_LIVE,
    _crypto_vol_normal_fair_up,
    _decimal,
    _optional_decimal,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

VOL_MODEL = {
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}
SERIES = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M",
          "BNB": "KXBNB15M", "DOGE": "KXDOGE15M", "HYPE": "KXHYPE15M"}
POLL_S = 5.0
LOOKBACK_S = 18.0     # match backtest snapshot cadence
DFAIR_TH = 0.10
QUOTE_EPS = 0.01
MIN_TTC_S = 90
MAX_TTC_S = 870
MAX_SPREAD = 0.20
PROBE_DELAYS = (2.0, 3.0)   # cumulative: +2s, +5s
SPOT_REFRESH_S = 0.0        # reload spot tail EVERY cycle — a cached (stale) spot zeroes dfair
                            # and defeats the reaction-speed premise (bug found 2026-07-02:
                            # 60s cache => spot_ref == spot_now for most pairs => 0 signals)
HEARTBEAT_S = 600.0         # emit max|dfair| seen periodically (quiet vs broken)

OUT_DIR = os.environ.get("STALE_SHADOW_OUT", "/app/data/stale_shadow")
STDOUT = os.environ.get("STALE_SHADOW_STDOUT") == "1"


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
        with open(os.path.join(OUT_DIR, "signals.jsonl"), "a") as fh:
            fh.write(line + "\n")


def parse_close(m) -> datetime | None:
    ct = m.get("close_time")
    if not ct:
        return None
    try:
        dt = datetime.fromisoformat(str(ct).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        return None


def strike_of(m) -> float | None:
    # 15m tickers end in a 2-digit index (e.g. ...1100-00), NOT the strike — the
    # strike is the floor_strike field (bug found 2026-07-02: regex gave 0/45 ->
    # garbage moneyness -> ctx fields missing -> dfair identically 0).
    return f(m.get("floor_strike"))


async def market_quotes(client, series_ticker):
    r = await client.list_markets(series_ticker=series_ticker, status="open", limit=100)
    out = {}
    for m in r.get("markets") or []:
        yb, ya = f(m.get("yes_bid_dollars")), f(m.get("yes_ask_dollars"))
        k = strike_of(m)
        cl = parse_close(m)
        if yb is None or ya is None or k is None or cl is None:
            continue
        out[str(m.get("ticker"))] = {"yes_bid": yb, "yes_ask": ya, "strike": k, "close": cl}
    return out


async def main() -> None:
    assets = (sys.argv[1] if len(sys.argv) > 1 else "BTC,BNB,HYPE,DOGE").split(",")
    assets = [a.strip().upper() for a in assets if a.strip().upper() in SERIES]
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    client = KalshiClient(settings)

    spot_cache: dict[str, tuple[datetime, list, object]] = {}
    hist: dict[str, list] = {}          # ticker -> [(ts, mid, spot, fair)]
    signaled: set[str] = set()          # one signal per market (dedup like backtest)
    emit({"type": "start", "ts": datetime.now(UTC).isoformat(), "assets": assets,
          "dfair_th": DFAIR_TH, "lookback_s": LOOKBACK_S, "poll_s": POLL_S})

    async def spot_rows(asset):
        nowt = datetime.now(UTC)
        cached = spot_cache.get(asset)
        if cached and SPOT_REFRESH_S > 0 and (nowt - cached[0]).total_seconds() < SPOT_REFRESH_S:
            return cached[1], cached[2]
        async with factory() as session:
            repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
            rows = await repo.list_crypto_spot_ohlc(
                frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
                since=nowt - timedelta(hours=2), limit=100_000,
            )
        rows = sorted(rows, key=lambda r: r.end_ts)
        prep = _prepare_spot_context_series(rows)
        spot_cache[asset] = (nowt, rows, prep)
        return rows, prep

    max_dfair_seen = 0.0
    evals = 0
    last_beat = asyncio.get_event_loop().time()
    while True:
        cycle_t0 = asyncio.get_event_loop().time()
        if cycle_t0 - last_beat >= HEARTBEAT_S:
            emit({"type": "heartbeat", "ts": datetime.now(UTC).isoformat(),
                  "max_abs_dfair": round(max_dfair_seen, 4), "evals": evals})
            max_dfair_seen, evals, last_beat = 0.0, 0, cycle_t0
        now = datetime.now(UTC)
        for asset in assets:
            try:
                rows, prep = await spot_rows(asset)
                quotes = await market_quotes(client, SERIES[asset])
            except Exception as e:  # transient API/DB error: log and continue
                emit({"type": "error", "ts": now.isoformat(), "asset": asset, "err": str(e)[:200]})
                continue
            for tk, q in quotes.items():
                ttc = (q["close"] - now).total_seconds()
                if not (MIN_TTC_S <= ttc <= MAX_TTC_S) or (q["yes_ask"] - q["yes_bid"]) > MAX_SPREAD:
                    continue
                ctx = _spot_context_for_decision(
                    rows, prepared=prep, decision_ts=now, target_price=_decimal(str(q["strike"])),
                    mid_yes=_decimal(str((q["yes_bid"] + q["yes_ask"]) / 2)), settings=settings,
                    mode=CRYPTO_SPOT_CONTEXT_LIVE,
                )
                if not ctx:
                    continue
                spot_now = f(ctx.get("spot_close_dollars"))
                row = {**ctx, "time_to_close_seconds": int(ttc)}
                fair_now = _crypto_vol_normal_fair_up(row, VOL_MODEL)
                mid_now = (q["yes_bid"] + q["yes_ask"]) / 2
                h = hist.setdefault(tk, [])
                h.append((now, mid_now, spot_now, float(fair_now) if fair_now is not None else None))
                if len(h) > 40:
                    del h[: len(h) - 40]
                if tk in signaled or fair_now is None or spot_now is None:
                    continue
                # find reference point ~LOOKBACK_S ago
                ref = None
                for ts, mid0, sp0, fv0 in reversed(h[:-1]):
                    if (now - ts).total_seconds() >= LOOKBACK_S:
                        ref = (ts, mid0, sp0, fv0)
                        break
                if not ref or ref[2] in (None, 0) or spot_now in (None, 0):
                    continue
                mny = f(ctx.get("spot_moneyness_pct"))
                if mny is None:
                    continue
                fair_ref = _crypto_vol_normal_fair_up(
                    {**row, "spot_moneyness_pct": mny - math.log(spot_now / ref[2])}, VOL_MODEL)
                if fair_ref is None:
                    continue
                dfair = float(fair_now) - float(fair_ref)
                evals += 1
                if abs(dfair) > max_dfair_seen:
                    max_dfair_seen = abs(dfair)
                stale = abs(mid_now - ref[1]) <= QUOTE_EPS
                if abs(dfair) < DFAIR_TH or not stale:
                    continue
                # SIGNAL (no order). Record, then probe quote persistence.
                side = "yes" if dfair > 0 else "no"
                sig_price = q["yes_ask"] if side == "yes" else 1.0 - q["yes_bid"]
                if not (0.03 <= sig_price <= 0.97):
                    continue
                signaled.add(tk)
                rec = {"type": "signal", "ts": now.isoformat(), "asset": asset, "ticker": tk,
                       "side": side, "dfair": round(dfair, 4), "signal_entry": round(sig_price, 4),
                       "yes_bid": q["yes_bid"], "yes_ask": q["yes_ask"], "ttc_s": int(ttc),
                       "spot": spot_now, "spot_ref": ref[2],
                       "gap_s": round((now - ref[0]).total_seconds(), 1), "probes": []}
                for d in PROBE_DELAYS:
                    await asyncio.sleep(d)
                    try:
                        q2 = (await market_quotes(client, SERIES[asset])).get(tk)
                    except Exception:
                        q2 = None
                    if q2 is None:
                        rec["probes"].append({"t_s": sum(PROBE_DELAYS[: len(rec['probes']) + 1]), "gone": True})
                        continue
                    p2 = q2["yes_ask"] if side == "yes" else 1.0 - q2["yes_bid"]
                    rec["probes"].append({
                        "t_s": sum(PROBE_DELAYS[: len(rec['probes']) + 1]),
                        "entry_now": round(p2, 4),
                        "still_crossable": bool(p2 <= sig_price + 1e-9),
                        "yes_bid": q2["yes_bid"], "yes_ask": q2["yes_ask"],
                    })
                emit(rec)
        elapsed = asyncio.get_event_loop().time() - cycle_t0
        await asyncio.sleep(max(0.5, POLL_S - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
