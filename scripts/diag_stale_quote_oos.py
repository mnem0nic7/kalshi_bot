"""TRUE time-OOS check of the stale-quote edge on the pre-v15 window (read-only).

The materialized rows in the OOS window (offset..offset+days ago) are v14 with
frozen candle-spot, so spot deltas are unusable there. This recomputes the fresh
point-in-time spot context per row from raw crypto_spot_ohlc ticks (the
diag_sigma_freshfix method, mode=LIVE), then applies the SAME stale-quote rule
tested on the recent window: cross the current quote when the fresh-spot move
shifts analytic fair value by >= threshold AND the quote didn't move. Quotes,
labels, strikes and ttc come from the rows (point-in-time regardless of schema).

  docker cp scripts/diag_stale_quote_oos.py infra-trainer_production-1:/tmp/ && \
    docker exec infra-trainer_production-1 python /tmp/diag_stale_quote_oos.py HYPE 7 7
"""
from __future__ import annotations

import asyncio
import math
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.crypto.services import (
    CRYPTO_SPOT_CONTEXT_LIVE,
    _crypto_training_row_payload,
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
FEE_RATE = 0.07
MAX_GAP_S = 150
QUOTE_EPS = 0.01
DFAIR_GRID = (0.05, 0.10, 0.15, 0.20)
MIN_TTC_S = 90
MAX_TTC_S = 870


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fee(p):
    return FEE_RATE * p * (1 - p)


async def main() -> None:
    asset = (sys.argv[1] if len(sys.argv) > 1 else "HYPE").upper()
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    offset = int(sys.argv[3]) if len(sys.argv) > 3 else 7
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    until = datetime.now(UTC) - timedelta(days=offset)
    since = until - timedelta(days=days)

    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        recs = await repo.list_crypto_training_feature_rows(
            frequency="15m", kalshi_env=settings.kalshi_env, asset_symbols=[asset],
            since=since, limit=settings.crypto_train_max_snapshots,
        )
        spot = await repo.list_crypto_spot_ohlc(
            frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
            since=since - timedelta(hours=2), limit=1_000_000,
        )
    rows = [_crypto_training_row_payload(r) for r in reversed(recs)]
    ok = []
    for r in rows:
        if r.get("label_yes") is None:
            continue
        d = r.get("decision_ts")
        if isinstance(d, str):
            d = datetime.fromisoformat(d)
        if d is None or d > until:
            continue
        r["_ts"] = d
        ok.append(r)
    spot = sorted(spot, key=lambda r: r.end_ts)
    prepared = _prepare_spot_context_series(spot)
    print(f"asset={asset} window={since:%m-%d}..{until:%m-%d} rows={len(ok)} spot_rows={len(spot)}", flush=True)

    # fresh context per row (memoized per (_ts, ticker))
    def fresh_ctx(r):
        tgt = _optional_decimal(r.get("target_price_dollars"))
        if tgt is None:
            return None
        return _spot_context_for_decision(
            spot, prepared=prepared, decision_ts=r["_ts"], target_price=tgt,
            mid_yes=_decimal(r.get("mid_yes_dollars")), settings=settings,
            mode=CRYPTO_SPOT_CONTEXT_LIVE,
        )

    by_mkt = defaultdict(list)
    for r in ok:
        by_mkt[r.get("market_ticker")].append(r)

    res = {(k, th): [0.0, 0, 0] for k in ("stale", "base") for th in DFAIR_GRID}
    pairs = 0
    for mkt, rs in by_mkt.items():
        rs.sort(key=lambda r: r["_ts"])
        ctxs = [None] * len(rs)
        for i, (prev, cur) in enumerate(zip(rs, rs[1:])):
            gap = (cur["_ts"] - prev["_ts"]).total_seconds()
            if gap <= 0 or gap > MAX_GAP_S:
                continue
            ttc = f(cur.get("time_to_close_seconds"))
            if ttc is None or not (MIN_TTC_S <= ttc <= MAX_TTC_S):
                continue
            yb, ya = f(cur.get("yes_bid_dollars")), f(cur.get("yes_ask_dollars"))
            pm, cm = f(prev.get("mid_yes_dollars")), f(cur.get("mid_yes_dollars"))
            if None in (yb, ya, pm, cm) or ya <= yb or (ya - yb) > 0.20:
                continue
            if ctxs[i] is None:
                ctxs[i] = fresh_ctx(prev)
            if ctxs[i + 1] is None:
                ctxs[i + 1] = fresh_ctx(cur)
            cp, cc = ctxs[i], ctxs[i + 1]
            if not cp or not cc:
                continue
            sp_prev, sp_cur = f(cp.get("spot_close_dollars")), f(cc.get("spot_close_dollars"))
            if not sp_prev or not sp_cur:
                continue
            row_cur = {**cur, **cc}
            mny = f(row_cur.get("spot_moneyness_pct"))
            if mny is None:
                continue
            fair_cur = _crypto_vol_normal_fair_up(row_cur, VOL_MODEL)
            fair_prev = _crypto_vol_normal_fair_up(
                {**row_cur, "spot_moneyness_pct": mny - math.log(sp_cur / sp_prev)}, VOL_MODEL)
            if fair_cur is None or fair_prev is None:
                continue
            dfair = float(fair_cur) - float(fair_prev)
            if abs(dfair) < DFAIR_GRID[0]:
                continue
            pairs += 1
            y = float(cur["label_yes"])
            if dfair > 0:
                entry = ya
                if not (0.03 <= entry <= 0.97):
                    continue
                net = (y - entry) - fee(entry)
            else:
                entry = 1.0 - yb
                if not (0.03 <= entry <= 0.97):
                    continue
                net = ((1.0 - y) - entry) - fee(entry)
            stale = abs(cm - pm) <= QUOTE_EPS
            for th in DFAIR_GRID:
                if abs(dfair) < th:
                    continue
                for key, cond in (("base", True), ("stale", stale)):
                    if cond:
                        res[(key, th)][0] += net
                        res[(key, th)][1] += 1
                        res[(key, th)][2] += 1 if net > 0 else 0

    print(f"signal_pairs={pairs}")
    for th in DFAIR_GRID:
        parts = []
        for key in ("stale", "base"):
            pnl, n, w = res[(key, th)]
            avg = pnl / n if n else 0.0
            parts.append(f"{key} n={n:<4} net={pnl:+7.2f} avg={avg:+.4f} win={100*w/n if n else 0:>3.0f}%")
        print(f"  dfair>={th:.2f}:  " + "   |   ".join(parts), flush=True)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
