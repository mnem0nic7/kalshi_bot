"""Validate the fix WITHOUT a retrain: does fresh-spot (LIVE-mode) context make the
raw vol sigma rank, vs the stale-spot (HISTORICAL-mode) context the trainer uses?

mode=LIVE keeps spot_tick rows (fresh ~20s spot); mode=HISTORICAL drops them
(stale ~450s candle spot). For each pre-materialized settled row we RECOMPUTE the
spot context both ways from raw crypto_spot_ohlc, then compare raw vol fair_up
Brier vs the mid. If LIVE-mode raw vol Brier drops toward/below mid while
HISTORICAL stays ~0.25, the stale-spot root cause + the fix are confirmed.

  docker cp scripts/diag_sigma_freshfix.py infra-trainer_production-1:/tmp/
  docker exec infra-trainer_production-1 python /tmp/diag_sigma_freshfix.py HYPE 5
"""
from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.crypto.services import (
    _crypto_training_row_payload,
    _spot_context_for_decision,
    _prepare_spot_context_series,
    _crypto_vol_normal_fair_up,
    _decimal,
    _optional_decimal,
    CRYPTO_SPOT_CONTEXT_HISTORICAL,
    CRYPTO_SPOT_CONTEXT_LIVE,
)

_VOL_MODEL = {
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}


def _brier(preds):
    return sum((p - y) ** 2 for p, y in preds) / len(preds) if preds else None


def _stale(ctx):
    s = ctx.get("spot_stale_seconds")
    return float(s) if s is not None else None


async def main() -> None:
    asset = (sys.argv[1] if len(sys.argv) > 1 else "HYPE").upper()
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    freq = sys.argv[3] if len(sys.argv) > 3 else "15m"
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    since = datetime.now(UTC) - timedelta(days=days)
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        records = await repo.list_crypto_training_feature_rows(
            frequency=freq, kalshi_env=settings.kalshi_env, asset_symbols=[asset],
            since=since, limit=settings.crypto_train_max_snapshots,
        )
        spot = await repo.list_crypto_spot_ohlc(
            frequency=freq, kalshi_env=settings.kalshi_env, asset_symbol=asset,
            since=since - timedelta(hours=2), limit=1_000_000,
        )
    rows = [_crypto_training_row_payload(r) for r in reversed(records)]
    rows = [r for r in rows if r.get("label_yes") is not None and r.get("decision_ts") is not None]
    spot = sorted(spot, key=lambda r: r.end_ts)
    prepared = _prepare_spot_context_series(spot)
    print(f"asset={asset} freq={freq} days={days} rows={len(rows)} spot_rows={len(spot)}", flush=True)

    mid_p, live_p = [], []
    nm_mid, nm_vol = [], []
    live_stale = []
    for r in rows:
        y = int(r["label_yes"])
        dts = r["decision_ts"]
        if isinstance(dts, str):
            dts = datetime.fromisoformat(dts)
        target = _optional_decimal(r.get("target_price_dollars"))
        mid = _decimal(r.get("mid_yes_dollars"))
        if target is None:
            continue
        nm = 0.30 <= float(mid) <= 0.70   # near-money: the tradeable subset
        mid_p.append((float(mid), y))
        if nm:
            nm_mid.append((float(mid), y))
        ctx_live = _spot_context_for_decision(
            spot, prepared=prepared, decision_ts=dts, target_price=target,
            mid_yes=mid, settings=settings, mode=CRYPTO_SPOT_CONTEXT_LIVE,
        )
        st = _stale(ctx_live)
        if st is not None:
            live_stale.append(st)
        fair = _crypto_vol_normal_fair_up({**r, **ctx_live}, _VOL_MODEL)
        fv = float(fair) if fair is not None else float(mid)
        live_p.append((fv, y))
        if nm:
            nm_vol.append((fv, y))

    def _med(xs):
        return sorted(xs)[len(xs) // 2] if xs else None

    print("\n=== BRIER (lower=better) ===", flush=True)
    print(f"  ALL       n={len(mid_p):<7} mid={_brier(mid_p)}  fresh-vol={_brier(live_p)}  (med_stale={_med(live_stale)}s)", flush=True)
    print(f"  NEAR-MONEY(0.30-0.70) n={len(nm_mid):<7} mid={_brier(nm_mid)}  fresh-vol={_brier(nm_vol)}", flush=True)
    print("\nEdge exists on the tradeable subset only if NEAR-MONEY fresh-vol < NEAR-MONEY mid.", flush=True)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
