"""Sigma-quality sweep: does a better sigma estimator make fresh-spot vol beat mid?

The stale-spot fix removed the collapse (raw vol Brier 0.25 -> ~0.15), but raw vol
is still ~20-25% worse than the (razor-sharp) recent mid. The offline eval (a more
volatile regime) had fresh-spot sigma BEATING mid — the remaining difference is the
sigma estimator. This sweeps realized vs EWMA sigma over several tick windows,
computing raw fair_up (mm fair_up_normal, step inferred from tick cadence) per
settled decision, and reports raw Brier vs mid for each. Read-only, bounded.

  docker cp scripts/diag_sigma_window_sweep.py infra-trainer_production-1:/tmp/
  docker exec infra-trainer_production-1 python /tmp/diag_sigma_window_sweep.py HYPE 4
"""
from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.crypto.services import _crypto_training_row_payload, _decimal, _optional_decimal
from kalshi_bot.mm.fair_value import fair_up_normal, realized_vol, ewma_vol, infer_step_seconds


def _brier(preds):
    return sum((p - y) ** 2 for p, y in preds) / len(preds) if preds else None


async def main() -> None:
    asset = (sys.argv[1] if len(sys.argv) > 1 else "HYPE").upper()
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    since = datetime.now(UTC) - timedelta(days=days)
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        records = await repo.list_crypto_training_feature_rows(
            frequency="15m", kalshi_env=settings.kalshi_env, asset_symbols=[asset],
            since=since, limit=settings.crypto_train_max_snapshots,
        )
        spot = await repo.list_crypto_spot_ohlc(
            frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
            since=since - timedelta(hours=2), limit=1_000_000,
        )
    rows = [_crypto_training_row_payload(r) for r in reversed(records)]
    rows = [r for r in rows if r.get("label_yes") is not None and r.get("decision_ts") is not None]
    # tick series sorted by end_ts (close, ts) — use the freshest point-in-time slice
    ticks = sorted(
        ((s.end_ts, _decimal(s.close_dollars)) for s in spot if s.close_dollars is not None),
        key=lambda x: x[0],
    )
    tick_ts = [t for t, _ in ticks]
    from bisect import bisect_right
    print(f"asset={asset} days={days} rows={len(rows)} ticks={len(ticks)}", flush=True)

    WINDOWS = [8, 16, 33, 66, 132]
    mid_p = []
    real_p = {w: [] for w in WINDOWS}
    ewma_p = {w: [] for w in WINDOWS}
    for r in rows:
        y = int(r["label_yes"])
        dts = r["decision_ts"]
        if isinstance(dts, str):
            dts = datetime.fromisoformat(dts)
        target = _optional_decimal(r.get("target_price_dollars"))
        mid = _decimal(r.get("mid_yes_dollars"))
        ttc = r.get("time_to_close_seconds")
        if target is None or ttc is None or int(ttc) <= 0:
            continue
        mid_p.append((float(mid), y))
        i = bisect_right(tick_ts, dts)
        if i < 3:
            continue
        for w in WINDOWS:
            sl = ticks[max(0, i - w):i]
            prices = [p for _, p in sl]
            tss = [t.timestamp() for t, _ in sl]
            step = infer_step_seconds(tss) or 20.0
            spot_now = prices[-1]
            for est, store in ((realized_vol, real_p), (ewma_vol, ewma_p)):
                sig = est(prices)
                fair = (fair_up_normal(spot=spot_now, strike=target, sigma=sig,
                                       seconds_to_close=int(ttc), step_interval_seconds=step,
                                       max_abs_z=3.0) if sig else None)
                store[w].append((float(fair) if fair is not None else float(mid), y))

    bm = _brier(mid_p)
    print(f"\n  mid Brier = {bm:.4f}  (n={len(mid_p)})\n", flush=True)
    print(f"  {'window':>7} {'realized':>10} {'ewma':>10}", flush=True)
    for w in WINDOWS:
        br = _brier(real_p[w]); be = _brier(ewma_p[w])
        flag = ""
        if br is not None and br < bm:
            flag += " <-realized BEATS mid"
        if be is not None and be < bm:
            flag += " <-ewma BEATS mid"
        print(f"  {w:>7} {br:>10.4f} {be:>10.4f}{flag}", flush=True)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
