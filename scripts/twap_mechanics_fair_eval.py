"""TWAP partial-realization fair value — offline eval (2026-07-08, READ-ONLY).

Kalshi 15m markets settle on a 60s TWAP of the CF index ending at close, not
the terminal price. Two mechanical corrections to the analytic vol fair value:

  1. Variance: settlement is a time-AVERAGE. Var(avg over [a,a+60] from now)
     = sigma^2 * (a + 60/3), so tau_eff = tau - 40 (tau >= 60s). Inside the
     window (tau < 60s) the realized fraction f = (60-tau)/60 is arithmetic,
     and the unrealized remainder has Var = sigma^2 * tau^3 / 10800.
  2. Mean: inside the window the effective underlying is the blend
     S_eff = f * TWAP_realized + (1-f) * S_now.

Arms per row (all recomputed with FRESH point-in-time spot, LIVE-mode ctx):
  plain   = production _crypto_vol_normal_fair_up (|z|<=3 cap)
  twap    = same math with (m_eff, tau_eff)       (|z|<=3 cap)
  twapnc  = twap without the z cap (mechanical truth near close)
  mid     = market mid
Brier per ttc bucket + dedup taker sim P&L (fees) for entries where the twap
fair disagrees with the executable price by >= th inside the final 180s.

  docker exec -i <app> python /tmp/twap_mechanics_fair_eval.py BNB 14
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
MAX_TTC_EVAL = 600.0
SIM_MAX_TTC = 180.0
TH_GRID = (0.03, 0.05, 0.10)
BUCKETS = ((0, 30), (30, 60), (60, 120), (120, 300), (300, 600))
Z_CAP = 3.0
STEP = 60.0


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fee(p):
    return FEE_RATE * p * (1 - p)


def phi(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def bucket(ttc):
    for lo, hi in BUCKETS:
        if lo <= ttc < hi:
            return f"{lo}-{hi}"
    return None


async def main() -> None:
    asset = (sys.argv[1] if len(sys.argv) > 1 else "BNB").upper()
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    until = datetime.now(UTC)
    since = until - timedelta(days=days)

    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        recs = await repo.list_crypto_training_feature_rows(
            frequency="15m", kalshi_env=settings.kalshi_env, asset_symbols=[asset],
            since=since, limit=settings.crypto_train_max_snapshots,
        )
        spot = await repo.list_crypto_spot_ohlc(
            frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
            since=since - timedelta(hours=2), limit=2_000_000,
        )
    rows = [_crypto_training_row_payload(r) for r in reversed(recs)]
    spot = sorted(spot, key=lambda r: r.end_ts)
    prepared = _prepare_spot_context_series(spot)
    ticks = []  # (ts, price) deduped by ts
    seen_ts = {}
    for r in spot:
        p = f(getattr(r, "close_dollars", None))
        if p:
            seen_ts.setdefault(r.end_ts, []).append(p)
    for ts_, ps in seen_ts.items():
        ticks.append((ts_, sum(ps) / len(ps)))
    ticks.sort()
    tick_ts = [t for t, _ in ticks]
    import bisect

    def twap_realized(win_start, now_ts):
        lo = bisect.bisect_left(tick_ts, win_start)
        hi = bisect.bisect_right(tick_ts, now_ts)
        pts = [p for _, p in ticks[lo:hi]]
        return (sum(pts) / len(pts)) if pts else None

    by_mkt = defaultdict(list)
    n_ok = 0
    for r in rows:
        if r.get("label_yes") is None:
            continue
        ttc = f(r.get("time_to_close_seconds"))
        if ttc is None or ttc <= 0 or ttc > MAX_TTC_EVAL:
            continue
        d = r.get("decision_ts")
        if isinstance(d, str):
            d = datetime.fromisoformat(d)
        if d is None:
            continue
        r["_ts"] = d
        r["_ttc"] = ttc
        by_mkt[r.get("market_ticker")].append(r)
        n_ok += 1
    print(f"asset={asset} window={since:%m-%d}..{until:%m-%d} eval_rows(ttc<={MAX_TTC_EVAL:.0f}s)={n_ok} markets={len(by_mkt)}", flush=True)

    briers = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0])  # bucket -> [plain, twap, twapnc, mid, n]
    sim = {th: [0.0, 0, 0] for th in TH_GRID}  # dedup final-180s: [net, n, wins]
    sim_taken = {th: set() for th in TH_GRID}

    for mkt, rs in by_mkt.items():
        rs.sort(key=lambda r: r["_ts"])
        for r in rs:
            ttc = r["_ttc"]
            b = bucket(ttc)
            if b is None:
                continue
            tgt = _optional_decimal(r.get("target_price_dollars"))
            mid = f(r.get("mid_yes_dollars"))
            yb, ya = f(r.get("yes_bid_dollars")), f(r.get("yes_ask_dollars"))
            if tgt is None or mid is None:
                continue
            ctx = _spot_context_for_decision(
                spot, prepared=prepared, decision_ts=r["_ts"], target_price=tgt,
                mid_yes=_decimal(r.get("mid_yes_dollars")), settings=settings,
                mode=CRYPTO_SPOT_CONTEXT_LIVE,
            )
            if not ctx:
                continue
            merged = {**r, **ctx}
            m = f(merged.get("spot_moneyness_pct"))
            sig = f(merged.get("spot_realized_volatility_32")) or f(merged.get("spot_realized_volatility"))
            s_now = f(merged.get("spot_close_dollars"))
            if m is None or not sig or sig <= 0 or not s_now:
                continue
            plain = _crypto_vol_normal_fair_up(merged, VOL_MODEL)
            if plain is None:
                continue
            plain = float(plain)
            # --- TWAP mechanics ---
            close_ts = r["_ts"] + timedelta(seconds=ttc)
            if ttc >= 60.0:
                tau_eff = (ttc - 60.0) + 60.0 / 3.0
                m_eff = m
            else:
                fr = (60.0 - ttc) / 60.0
                M = twap_realized(close_ts - timedelta(seconds=60), r["_ts"])
                s_eff = fr * M + (1 - fr) * s_now if M else s_now
                m_eff = m + math.log(s_eff / s_now)
                tau_eff = max(1e-6, ttc ** 3 / 10800.0)
            z = (m_eff / sig) * math.sqrt(STEP / tau_eff)
            zc = max(-Z_CAP, min(Z_CAP, z))
            twap = min(0.99, max(0.01, phi(zc)))
            twapnc = min(0.999, max(0.001, phi(z)))
            y = float(r["label_yes"])
            acc = briers[b]
            acc[0] += (plain - y) ** 2
            acc[1] += (twap - y) ** 2
            acc[2] += (twapnc - y) ** 2
            acc[3] += (mid - y) ** 2
            acc[4] += 1
            # --- sim: final-180s taker on twap (capped) vs executable ---
            if ttc <= SIM_MAX_TTC and yb is not None and ya is not None and ya > yb and (ya - yb) <= 0.20:
                for th in TH_GRID:
                    if mkt in sim_taken[th]:
                        continue
                    net = None
                    if twap - ya >= th and 0.03 <= ya <= 0.97:
                        net = (y - ya) - fee(ya)
                    elif yb - twap >= th and 0.03 <= (1.0 - yb) <= 0.97:
                        net = ((1.0 - y) - (1.0 - yb)) - fee(1.0 - yb)
                    if net is not None:
                        sim_taken[th].add(mkt)
                        sim[th][0] += net
                        sim[th][1] += 1
                        sim[th][2] += 1 if net > 0 else 0

    print(" bucket      n     plain    twap     twapnc   mid")
    for lo, hi in BUCKETS:
        b = f"{lo}-{hi}"
        p_, t_, tn_, m_, n = briers[b]
        if not n:
            continue
        print(f"  {b:<9} {n:<6} {p_/n:.4f}   {t_/n:.4f}   {tn_/n:.4f}   {m_/n:.4f}", flush=True)
    print(" sim (final<=180s taker, dedup/market, net of fees):")
    for th in TH_GRID:
        net, n, w = sim[th]
        print(f"  th>={th:.2f}: n={n:<4} net={net:+7.2f} avg={net/n if n else 0:+.4f} win={100*w/n if n else 0:>3.0f}%", flush=True)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
