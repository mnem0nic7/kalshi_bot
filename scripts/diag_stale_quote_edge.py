"""Stale-quote taking backtest on alts (offline, light-table only — no snapshot scans).

Hypothesis (from the settlement-basis study): residual 15m edge is "stale-quote-
on-alts" — when spot jumps but a thin alt quote hasn't updated, crossing it in the
spot's direction clears fees without needing to out-forecast anyone.

Method: time-ordered settled feature rows per market (crypto_training_feature_rows,
indexed; the 232GB snapshot table is untouched). For consecutive same-market rows
(gap <= MAX_GAP_S):
  dfair = vol_fair(cur spot) - vol_fair(prev spot)   [same sigma/tau -> pure spot move]
  stale  = |mid(cur) - mid(prev)| <= QUOTE_EPS       [quote didn't react]
Trade: cross the CURRENT quote in the direction of dfair when |dfair| >= MIN_DFAIR;
score against label_yes net of rate*p*(1-p) taker fee. Baseline = same rule WITHOUT
the stale condition. Read-only; no orders.

  docker cp scripts/diag_stale_quote_edge.py infra-trainer_production-1:/tmp/ && \
    docker exec infra-trainer_production-1 python /tmp/diag_stale_quote_edge.py HYPE,DOGE,XRP 7
"""
from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.crypto.services import _crypto_training_row_payload, _crypto_vol_normal_fair_up

VOL_MODEL = {
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}
FEE_RATE = 0.07
MAX_GAP_S = 150      # consecutive-snapshot max spacing
QUOTE_EPS = 0.01     # "quote didn't move"
DFAIR_GRID = (0.05, 0.10, 0.15, 0.20)   # sweep signal-strength threshold
MIN_TTC_S = 90       # skip the last seconds before close
MAX_TTC_S = 870


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fee(p):
    return FEE_RATE * p * (1 - p)


async def main() -> None:
    assets = (sys.argv[1] if len(sys.argv) > 1 else "HYPE,DOGE,XRP").split(",")
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    offset_days = int(sys.argv[3]) if len(sys.argv) > 3 else 0  # >0 = earlier disjoint window (OOS check)
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    until = datetime.now(UTC) - timedelta(days=offset_days)
    since = until - timedelta(days=days)

    grand = {(key, th): [0.0, 0, 0] for key in ("stale", "base") for th in DFAIR_GRID}
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        for asset in assets:
            recs = await repo.list_crypto_training_feature_rows(
                frequency="15m", kalshi_env=settings.kalshi_env, asset_symbols=[asset.strip()],
                since=since, limit=settings.crypto_train_max_snapshots,
            )
            rows = [_crypto_training_row_payload(r) for r in reversed(recs)]
            rows = [r for r in rows if r.get("label_yes") is not None]
            if offset_days:
                def _ts(r):
                    d = r.get("decision_ts")
                    return datetime.fromisoformat(d) if isinstance(d, str) else d
                rows = [r for r in rows if _ts(r) is not None and _ts(r) <= until]
            by_mkt = defaultdict(list)
            for r in rows:
                dts = r.get("decision_ts")
                if isinstance(dts, str):
                    dts = datetime.fromisoformat(dts)
                if dts is None:
                    continue
                r["_ts"] = dts
                by_mkt[r.get("market_ticker")].append(r)

            res = {(key, th): [0.0, 0, 0] for key in ("stale", "base") for th in DFAIR_GRID}
            gaps = 0
            for mkt, rs in by_mkt.items():
                rs.sort(key=lambda r: r["_ts"])
                for prev, cur in zip(rs, rs[1:]):
                    gap = (cur["_ts"] - prev["_ts"]).total_seconds()
                    if gap <= 0 or gap > MAX_GAP_S:
                        continue
                    ttc = f(cur.get("time_to_close_seconds"))
                    if ttc is None or not (MIN_TTC_S <= ttc <= MAX_TTC_S):
                        continue
                    yb, ya = f(cur.get("yes_bid_dollars")), f(cur.get("yes_ask_dollars"))
                    pm, cm = f(prev.get("mid_yes_dollars")), f(cur.get("mid_yes_dollars"))
                    sp_prev, sp_cur = f(prev.get("spot_close_dollars")), f(cur.get("spot_close_dollars"))
                    if None in (yb, ya, pm, cm, sp_prev, sp_cur) or ya <= yb or (ya - yb) > 0.20:
                        continue
                    # fair reads spot_moneyness_pct (= ln(S/K), raw fraction — units verified),
                    # so shift moneyness by the spot log-return; swapping spot_close is a no-op.
                    mny = f(cur.get("spot_moneyness_pct"))
                    if mny is None or sp_prev <= 0 or sp_cur <= 0:
                        continue
                    import math as _m
                    mny_prev = mny - _m.log(sp_cur / sp_prev)
                    fair_cur = _crypto_vol_normal_fair_up(cur, VOL_MODEL)
                    fair_prev = _crypto_vol_normal_fair_up({**cur, "spot_moneyness_pct": mny_prev}, VOL_MODEL)
                    if fair_cur is None or fair_prev is None:
                        continue
                    dfair = float(fair_cur) - float(fair_prev)
                    if abs(dfair) < DFAIR_GRID[0]:
                        continue
                    gaps += 1
                    y = float(cur["label_yes"])
                    if dfair > 0:   # buy yes at ask
                        entry = ya
                        if not (0.03 <= entry <= 0.97):
                            continue
                        net = (y - entry) - fee(entry)
                    else:           # buy no at 1-bid
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
            print(f"\n=== {asset.strip()}  settled_rows={len(rows)} markets={len(by_mkt)} signal_pairs(>= {DFAIR_GRID[0]:.2f})={gaps} ===")
            for th in DFAIR_GRID:
                parts = []
                for key in ("stale", "base"):
                    pnl, n, w = res[(key, th)]
                    avg = pnl / n if n else 0.0
                    parts.append(f"{key} n={n:<4} avg={avg:+.4f} win={100*w/n if n else 0:>3.0f}%")
                    g = grand[(key, th)]
                    g[0] += pnl; g[1] += n; g[2] += w
                print(f"  dfair>={th:.2f}:  " + "   |   ".join(parts))
    print("\n=== TOTAL (all assets) ===")
    for th in DFAIR_GRID:
        parts = []
        for key in ("stale", "base"):
            pnl, n, w = grand[(key, th)]
            avg = pnl / n if n else 0.0
            parts.append(f"{key} n={n:<5} net={pnl:+8.2f} avg={avg:+.4f} win={100*w/n if n else 0:>3.0f}%")
        print(f"  dfair>={th:.2f}:  " + "   |   ".join(parts))
    print("\nEdge is real only if STALE avg > 0 AND > BASE avg, and it must hold on a disjoint window (arg3=offset_days).")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
