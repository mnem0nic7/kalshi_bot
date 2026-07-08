"""Backtest of the operator's model-divergence + model-pegged-TP strategy (2026-07-08).

Rule under test (READ-ONLY, no orders):
  1. Model = analytic vol_normal fair value (v15 fresh-spot materialized rows).
  2. ENTER (taker, one trade per market) when the executable price is >= TH
     cheaper than the model: yes side fair - ask >= TH; no side bid - fair >= TH.
  3. Rest a take-profit sell pegged to the CURRENT model fair, re-pegged every
     snapshot (~18s). Fill sim: resting sell at TP_t fills if the NEXT
     snapshot's opposing best bid trades through it (maker exit, $0 fee).
  4. Confidence ride rule: while fair >= 0.80 (yes side; <= 0.20 for no) the
     sell is pulled; reinstated when it drops back.
  5. Unfilled TP -> settle at label. Entry pays taker fee 0.07*p*(1-p).

Arms: A = full rule; B = same entries HOLD to settlement (isolates TP effect).
For A's TP-filled trades we also print the counterfactual settle net (regret).

  docker exec <app> python /tmp/model_divergence_tp_backtest.py BNB 14
"""
from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.crypto.services import (
    _crypto_training_row_payload,
    _crypto_vol_normal_fair_up,
)

VOL_MODEL = {
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}
FEE_RATE = 0.07
TH_GRID = (0.05, 0.10, 0.15)
MIN_TTC_S = 90
MAX_TTC_S = 870
RIDE_CONF = 0.80


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fee(p):
    return FEE_RATE * p * (1 - p)


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
    rows = [_crypto_training_row_payload(r) for r in reversed(recs)]
    by_mkt = defaultdict(list)
    for r in rows:
        if r.get("label_yes") is None:
            continue
        d = r.get("decision_ts")
        if isinstance(d, str):
            d = datetime.fromisoformat(d)
        if d is None:
            continue
        r["_ts"] = d
        by_mkt[r.get("market_ticker")].append(r)
    print(f"asset={asset} window={since:%m-%d}..{until:%m-%d} rows={sum(len(v) for v in by_mkt.values())} markets={len(by_mkt)}", flush=True)

    # results[(arm, th)] = [net, n, wins, tp_exits, regret_sum]
    results = {(a, t): [0.0, 0, 0, 0, 0.0] for a in ("A", "B") for t in TH_GRID}

    for mkt, rs in by_mkt.items():
        rs.sort(key=lambda r: r["_ts"])
        fairs = [None] * len(rs)

        def fair_at(i):
            if fairs[i] is None:
                v = _crypto_vol_normal_fair_up(rs[i], VOL_MODEL)
                fairs[i] = float(v) if v is not None else -1.0
            return fairs[i] if fairs[i] >= 0 else None

        for th in TH_GRID:
            entered = False
            for i, r in enumerate(rs):
                ttc = f(r.get("time_to_close_seconds"))
                if ttc is None or not (MIN_TTC_S <= ttc <= MAX_TTC_S):
                    continue
                yb, ya = f(r.get("yes_bid_dollars")), f(r.get("yes_ask_dollars"))
                if None in (yb, ya) or ya <= yb or (ya - yb) > 0.20:
                    continue
                fair = fair_at(i)
                if fair is None:
                    continue
                side = None
                if fair - ya >= th and 0.03 <= ya <= 0.97:
                    side, entry = "yes", ya
                elif yb - fair >= th and 0.03 <= (1.0 - yb) <= 0.97:
                    side, entry = "no", 1.0 - yb
                if side is None:
                    continue
                entered = True
                y = float(r["label_yes"])
                # Arm B: hold to settlement
                settle_net = ((y - entry) if side == "yes" else ((1.0 - y) - entry)) - fee(entry)
                rb = results[("B", th)]
                rb[0] += settle_net; rb[1] += 1; rb[2] += 1 if settle_net > 0 else 0

                # Arm A: walk forward with model-pegged TP + ride rule
                a_net = None
                for j in range(i + 1, len(rs)):
                    fj = fair_at(j - 1)  # TP pegged at previous snapshot's model
                    if fj is None:
                        continue
                    riding = (fj >= RIDE_CONF) if side == "yes" else (fj <= 1 - RIDE_CONF)
                    if riding:
                        continue
                    nb = f(rs[j].get("yes_bid_dollars"))
                    na = f(rs[j].get("yes_ask_dollars"))
                    if side == "yes":
                        tp = min(0.99, max(0.01, fj))
                        if nb is not None and nb >= tp:
                            a_net = (tp - entry) - fee(entry)  # maker exit, $0 exit fee
                            break
                    else:
                        tp_no = min(0.99, max(0.01, 1.0 - fj))
                        no_bid = (1.0 - na) if na is not None else None
                        if no_bid is not None and no_bid >= tp_no:
                            a_net = (tp_no - entry) - fee(entry)
                            break
                ra = results[("A", th)]
                if a_net is None:
                    a_net = settle_net
                else:
                    ra[3] += 1
                    ra[4] += a_net - settle_net  # positive = TP beat holding
                ra[0] += a_net; ra[1] += 1; ra[2] += 1 if a_net > 0 else 0
                break  # one trade per market
            if entered:
                continue

    for th in TH_GRID:
        a, b = results[("A", th)], results[("B", th)]
        for tag, r in (("A full-rule ", a), ("B hold      ", b)):
            avg = r[0] / r[1] if r[1] else 0.0
            extra = f" tp_exits={r[3]} tp_vs_hold={r[4]:+.2f}" if tag.startswith("A") else ""
            print(f"  th>={th:.2f} {tag} n={r[1]:<4} net={r[0]:+8.2f} avg={avg:+.4f} win={100*r[2]/r[1] if r[1] else 0:>3.0f}%{extra}", flush=True)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
