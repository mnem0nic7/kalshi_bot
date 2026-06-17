"""Light, standalone OOS evaluation of the analytic vol fair-value strategy.

The vol strategy needs no model training (the fair value is analytic; only the
isotonic calibration map is fitted). This path walk-forward-evaluates ONLY the
vol candidate vs the market mid — after fees + live edge-shrinkage — without
fitting any tree model, so the σ estimator and calibration can be iterated in
seconds instead of behind the GPU trainer.
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import _crypto_vol_fair_value_oos_eval


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/voleval.db",
        "web_auth_enabled": False,
        "risk_min_edge_bps": 500,
        "crypto_autonomy_enabled": False,
        "crypto_trading_enabled": False,
        "crypto_model_trained_replay_only": False,
        "crypto_min_training_samples": 4,
        "crypto_replay_min_trade_candidates": 1,
    }
    values.update(overrides)
    return Settings(**values)


def _rows(days=5, per_day=6):
    out = []
    for d in range(1, days + 1):
        day = f"2026-05-{d:02d}"
        for i in range(per_day):
            out.append(
                {
                    "market_ticker": f"KXBTC15M-{day}-{i}",
                    "asset_symbol": "BTC",
                    "market_day": day,
                    "decision_ts": datetime(2026, 5, d, 12, i, tzinfo=UTC),
                    "label_yes": 1,
                    "mid_yes_dollars": Decimal("0.5000"),
                    "yes_bid_dollars": Decimal("0.4800"),
                    "yes_ask_dollars": Decimal("0.5000"),
                    "no_bid_dollars": Decimal("0.4800"),
                    "no_ask_dollars": Decimal("0.5000"),
                    "spread_bps": 200,
                    "spot_feature_status": "available",
                    "spot_provider": "coinbase",
                    "spot_source_kind": "spot_ohlc",
                    "time_to_close_seconds": 60,
                    "spot_moneyness_pct": Decimal("0.0020"),
                    "spot_realized_volatility_32": Decimal("0.0010"),
                    "spot_realized_volatility": Decimal("0.0010"),
                }
            )
    return out


def test_vol_oos_eval_structure_and_folds(tmp_path):
    report = _crypto_vol_fair_value_oos_eval(_rows(), settings=_settings(tmp_path), max_folds=3)
    assert report["status"] == "ok"
    assert report["fold_count"] == 3
    assert report["vol_brier"] is not None
    assert report["mid_brier"] is not None
    assert "advantage_vs_mid" in report
    # vol predicts strongly yes (in-the-money), mid stands at 0.50 -> vol trades, mid does not.
    assert report["vol_selected_count"] > 0
    assert report["edge_shrinkage_applied"] is False


def test_vol_oos_eval_heavy_shrinkage_zeros_trades(tmp_path):
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 100}
    report = _crypto_vol_fair_value_oos_eval(
        _rows(), settings=_settings(tmp_path), max_folds=3, edge_shrinkage=shrinkage
    )
    assert report["edge_shrinkage_applied"] is True
    assert report["vol_selected_count"] == 0


def test_vol_oos_eval_insufficient_data(tmp_path):
    report = _crypto_vol_fair_value_oos_eval(_rows(days=1, per_day=2), settings=_settings(tmp_path), max_folds=3)
    assert report["status"] == "insufficient_walk_forward_data"
    assert report["fold_count"] == 0
