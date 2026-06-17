"""Selection/live parity for edge-shrinkage.

Live evidence (2026-06-17): the BTC 15m lightgbm champion showed +$1.99 / 11
trades in the trainer simulation yet placed ZERO live trades. Root cause: the
live decision path applies edge-shrinkage (beta floored at 0.2 — realized live
edge is <=20% of predicted) before the fee/edge gate, but the trainer candidate
simulation did NOT. Champion selection therefore optimized an edge ~5x larger
than what actually reaches the order book.

These tests pin the fix: the trainer simulation and champion-selection report
must apply the same edge-shrinkage fit as live, so "beats mid" means "beats mid
*after the live brake*".
"""
from __future__ import annotations

from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    _crypto_in_sample_candidate_report,
    _crypto_time_to_close_bucket,
    _simulate_crypto_trade,
)


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/parity.db",
        "web_auth_enabled": False,
        "risk_min_edge_bps": 500,
        "crypto_autonomy_enabled": False,
        "crypto_trading_enabled": False,
        "crypto_model_trained_replay_only": False,
    }
    values.update(overrides)
    return Settings(**values)


def _row(*, day: str, label_yes: int = 1, **overrides) -> dict[str, object]:
    values: dict[str, object] = {
        "market_ticker": f"KXBTC15M-{day}",
        "asset_symbol": "BTC",
        "market_day": day,
        "label_yes": label_yes,
        "mid_yes_dollars": Decimal("0.5000"),
        "yes_bid_dollars": Decimal("0.4800"),
        "yes_ask_dollars": Decimal("0.5000"),
        "no_bid_dollars": Decimal("0.4800"),
        "no_ask_dollars": Decimal("0.5000"),
        "spread_bps": 200,
        "spot_feature_status": "available",
        "spot_provider": "coinbase",
        "spot_source_kind": "spot_ohlc",
        "time_to_close_seconds": 600,
        "market_age_seconds": 300,
    }
    values.update(overrides)
    return values


# --- atomic: the per-trade simulation must honor live edge-shrinkage ----------


def test_simulate_crypto_trade_fillable_without_shrinkage(tmp_path) -> None:
    settings = _settings(tmp_path)
    trade = _simulate_crypto_trade(_row(day="2026-05-01"), Decimal("0.6000"), settings=settings)
    assert trade["status"] == "fillable"


def test_simulate_crypto_trade_blocked_by_enforced_shrinkage(tmp_path) -> None:
    settings = _settings(tmp_path)
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 100}

    trade = _simulate_crypto_trade(
        _row(day="2026-05-01"),
        Decimal("0.6000"),
        settings=settings,
        edge_shrinkage=shrinkage,
    )

    # beta=0.05 shrinks the edge below the fee, exactly as the live gate does,
    # so the trade is no longer selectable.
    assert trade["status"] == "not_selected"


def test_simulate_crypto_trade_shrinkage_not_enforced_below_min_fills(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_edge_shrinkage_min_fills=40)
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 10}

    trade = _simulate_crypto_trade(
        _row(day="2026-05-01"),
        Decimal("0.6000"),
        settings=settings,
        edge_shrinkage=shrinkage,
    )
    assert trade["status"] == "fillable"


# --- report level: champion selection flips to mid under live shrinkage -------


def _edgey_candidate_status() -> dict[str, dict[str, object]]:
    """A model that predicts well above mid (clear pre-shrinkage edge), plus the
    market-mid baseline. Uses asset_time_calibration so the prediction is a
    deterministic mid + bucket adjustment."""
    bucket = _crypto_time_to_close_bucket(600.0)
    return {
        "market_mid_baseline": {
            "name": "market_mid_baseline",
            "status": "available",
            "model": {"model_type": "market_mid_baseline"},
        },
        "asset_time_calibration": {
            "name": "asset_time_calibration",
            "status": "available",
            "model": {
                "model_type": "asset_time_calibration",
                "bucket_adjustments_bps": {f"BTC|{bucket}": 1500},
                "fallback_model": {"model_type": "market_mid_baseline"},
            },
        },
    }


def _selection_settings(tmp_path):
    return _settings(
        tmp_path,
        crypto_model_candidate_report_max_walk_forward_folds=0,
        crypto_replay_min_trade_candidates=5,
        crypto_model_max_brier_regression_vs_mid=0.5,
    )


def _selection_rows() -> list[dict[str, object]]:
    # 11 yes-wins / 3 yes-losses: the model (predicts ~0.65 yes) trades every
    # day and is comfortably net-profitable pre-shrinkage.
    labels = [1] * 11 + [0] * 3
    return [_row(day=f"2026-05-{d:02d}", label_yes=lbl) for d, lbl in enumerate(labels, start=1)]


def test_in_sample_report_selects_edge_model_without_shrinkage(tmp_path) -> None:
    settings = _selection_settings(tmp_path)
    report = _crypto_in_sample_candidate_report(
        _selection_rows(), _edgey_candidate_status(), settings=settings
    )
    assert report["champion_name"] == "asset_time_calibration"


def test_in_sample_report_falls_back_to_mid_under_live_shrinkage(tmp_path) -> None:
    settings = _selection_settings(tmp_path)
    shrinkage = {"beta": 0.05, "status": "ok", "sample_count": 100}

    report = _crypto_in_sample_candidate_report(
        _selection_rows(), _edgey_candidate_status(), settings=settings, edge_shrinkage=shrinkage
    )
    # Post-shrinkage the model's edge cannot cover the fee -> no fillable trades
    # -> not profit-deployable -> selection falls back to the market mid.
    assert report["champion_name"] == "market_mid_baseline"
