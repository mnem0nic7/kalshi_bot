"""Diagnose WHERE the pipeline vol_normal_fair_value Brier collapse happens.

Two competing root causes for the ~0.25 pipeline Brier (vs mid ~0.13) while the
offline 5s-spine sigma beats mid raw on BTC/ETH/HYPE:
  (a) the candle-proxy 60s-cadence sigma's cross-sectional RANKING is uninformative
      -> even the RAW fair_up can't beat mid (substrate problem), OR
  (b) the raw fair_up ranks fine but the per-fold isotonic collapses it to base rate
      (calibration problem).

This loads the SAME feature rows the candidate report uses, runs the SAME
walk-forward folds + sigma fit, and compares Brier of: mid | raw vol fair_up
(NO isotonic) | calibrated vol (WITH isotonic). Bounded + read-only.

Run inside the trainer container:
  docker cp scripts/diag_sigma_collapse.py infra-trainer_production-1:/tmp/
  docker exec infra-trainer_production-1 python /tmp/diag_sigma_collapse.py HYPE 14
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
    _crypto_walk_forward_folds,
    _fit_crypto_heuristic_calibration,
    _fit_crypto_vol_fair_value_model,
    _crypto_vol_normal_fair_up,
    _predict_crypto_probability,
    _decimal,
)


def _brier(preds: list[tuple[float, int]]) -> float | None:
    if not preds:
        return None
    return sum((p - y) ** 2 for p, y in preds) / len(preds)


async def main() -> None:
    asset = (sys.argv[1] if len(sys.argv) > 1 else "HYPE").upper()
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    freq = "15m"
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    since = datetime.now(UTC) - timedelta(days=days)
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        records = await repo.list_crypto_training_feature_rows(
            frequency=freq,
            kalshi_env=settings.kalshi_env,
            asset_symbols=[asset],
            since=since,
            limit=settings.crypto_train_max_snapshots,
        )
    rows = [_crypto_training_row_payload(r) for r in reversed(records)]
    rows = [r for r in rows if r.get("label_yes") is not None]
    print(f"asset={asset} days={days} rows={len(rows)}", flush=True)
    if not rows:
        return

    folds = _crypto_walk_forward_folds(rows, min_train_rows=max(2, min(settings.crypto_min_training_samples, 20)))
    print(f"folds={len(folds)}", flush=True)

    mid_preds: list[tuple[float, int]] = []
    raw_preds: list[tuple[float, int]] = []
    cal_preds: list[tuple[float, int]] = []
    sigmas: list[float] = []
    raw_vals: list[float] = []
    n_raw_none = 0

    for fold in folds:
        fallback = _fit_crypto_heuristic_calibration(fold["train_rows"])
        vol_model = _fit_crypto_vol_fair_value_model(fold["train_rows"], fallback=fallback)
        for row in fold["test_rows"]:
            label = int(row["label_yes"])
            mid = _decimal(row.get("mid_yes_dollars"))
            mid_preds.append((float(mid), label))
            raw = _crypto_vol_normal_fair_up(row, vol_model)
            if raw is None:
                n_raw_none += 1
            else:
                raw_preds.append((float(raw), label))
                raw_vals.append(float(raw))
            cal = _predict_crypto_probability(row, vol_model)
            cal_preds.append((float(cal), label))
            sig = row.get("spot_realized_volatility_32") or row.get("spot_realized_volatility")
            if sig is not None:
                try:
                    sigmas.append(float(sig))
                except (TypeError, ValueError):
                    pass

    def _stats(xs: list[float]) -> str:
        if not xs:
            return "n=0"
        xs2 = sorted(xs)
        n = len(xs2)
        return (f"n={n} min={xs2[0]:.4f} p10={xs2[n//10]:.4f} med={xs2[n//2]:.4f} "
                f"p90={xs2[min(n-1,9*n//10)]:.4f} max={xs2[-1]:.4f}")

    print("\n=== BRIER (lower=better) ===", flush=True)
    print(f"  mid            : {_brier(mid_preds)}", flush=True)
    print(f"  vol RAW        : {_brier(raw_preds)}  (no isotonic; raw_none={n_raw_none})", flush=True)
    print(f"  vol CALIBRATED : {_brier(cal_preds)}  (with isotonic)", flush=True)
    print("\n=== distributions ===", flush=True)
    print(f"  sigma_32 : {_stats(sigmas)}", flush=True)
    print(f"  raw_fair : {_stats(raw_vals)}", flush=True)
    print("\nINTERPRETATION:", flush=True)
    print("  raw beats mid  -> isotonic collapses it (calibration bug)", flush=True)
    print("  raw ~= mid/0.25 too -> sigma ranking uninformative (substrate bug)", flush=True)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
