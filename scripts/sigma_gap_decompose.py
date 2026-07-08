"""Decompose the v15 materialized-vs-fresh sigma gap (2026-07-08, READ-ONLY).

Memory: freshfix-estimated raw vol Brier ~0.153 vs ~0.227 evaluated on v15
materialized rows — is the residual loss in the materialized MONEYNESS, the
materialized SIGMA (spot_realized_volatility_32), or both? Per settled row:

  A: materialized row as-is
  B: fresh moneyness (LIVE ctx), materialized sigma
  C: materialized moneyness, fresh sigma
  D: full fresh LIVE ctx
  mid: market mid

  docker exec -i <app> python /tmp/sigma_gap_decompose.py HYPE 12
"""
from __future__ import annotations

import asyncio
import sys
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


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


async def main() -> None:
    asset = (sys.argv[1] if len(sys.argv) > 1 else "HYPE").upper()
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    since = datetime.now(UTC) - timedelta(days=days)
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

    sums = {k: 0.0 for k in ("A", "B", "C", "D", "mid")}
    n = 0
    dm_sum = 0.0
    ds_sum = 0.0
    for r in rows:
        if r.get("label_yes") is None:
            continue
        ttc = f(r.get("time_to_close_seconds"))
        if ttc is None or not (90 <= ttc <= 870):
            continue
        tgt = _optional_decimal(r.get("target_price_dollars"))
        mid = f(r.get("mid_yes_dollars"))
        if tgt is None or mid is None:
            continue
        d = r.get("decision_ts")
        if isinstance(d, str):
            d = datetime.fromisoformat(d)
        m_row = f(r.get("spot_realized_volatility_32")) is not None
        ctx = _spot_context_for_decision(
            spot, prepared=prepared, decision_ts=d, target_price=tgt,
            mid_yes=_decimal(r.get("mid_yes_dollars")), settings=settings,
            mode=CRYPTO_SPOT_CONTEXT_LIVE,
        )
        if not ctx:
            continue
        m_mat, s_mat = f(r.get("spot_moneyness_pct")), f(r.get("spot_realized_volatility_32"))
        m_fr, s_fr = f(ctx.get("spot_moneyness_pct")), f(ctx.get("spot_realized_volatility_32"))
        if None in (m_mat, s_mat, m_fr, s_fr) or s_mat <= 0 or s_fr <= 0:
            continue
        y = float(r["label_yes"])
        variants = {
            "A": {**r},
            "B": {**r, "spot_moneyness_pct": m_fr},
            "C": {**r, "spot_realized_volatility_32": s_fr},
            "D": {**r, **ctx},
        }
        ok = True
        preds = {}
        for k, row in variants.items():
            p = _crypto_vol_normal_fair_up(row, VOL_MODEL)
            if p is None:
                ok = False
                break
            preds[k] = float(p)
        if not ok:
            continue
        for k, p in preds.items():
            sums[k] += (p - y) ** 2
        sums["mid"] += (mid - y) ** 2
        n += 1
        dm_sum += abs(m_fr - m_mat)
        ds_sum += abs(s_fr - s_mat) / s_mat
    print(f"asset={asset} days={days} rows={n}")
    if n:
        print(f"  A mat-row      brier={sums['A']/n:.4f}")
        print(f"  B fresh-m      brier={sums['B']/n:.4f}")
        print(f"  C fresh-sigma  brier={sums['C']/n:.4f}")
        print(f"  D full-fresh   brier={sums['D']/n:.4f}")
        print(f"  mid            brier={sums['mid']/n:.4f}")
        print(f"  mean |dm|={dm_sum/n:.5f}  mean |dsigma|/sigma={ds_sum/n:.3f}")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
