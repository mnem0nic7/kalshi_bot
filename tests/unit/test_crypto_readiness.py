"""Tests for color-scoped, windowed live-order check in _crypto_readiness_score (Change 2)."""
from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import _crypto_readiness_score


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/test.db",
        "web_auth_enabled": False,
        "crypto_trading_enabled": False,
        "app_color": "blue",
    }
    values.update(overrides)
    return Settings(**values)


def _minimal_shadow_evidence(**overrides) -> dict:
    base = {
        "live_order_count": 0,
        "recent_live_order_count": 0,
        "recent_rooms": 5,
        "recent_trade_tickets": 3,
        "recent_risk_verdicts": 3,
        "recent_shadow_skipped_receipts": 2,
    }
    base.update(overrides)
    return base


def _call(settings, shadow_evidence, active_color: str) -> dict:
    return _crypto_readiness_score(
        settings=settings,
        data_quality={"status": "ready"},
        spot_quality={"status": "ready", "coverage_pct": 1.0},
        shadow_evidence=shadow_evidence,
        model={"status": "trained"},
        backtest={"metrics": {
            "calibration_brier": 0.20,
            "market_mid_brier": 0.25,
            "calibration_log_loss": 0.50,
            "market_mid_log_loss": 0.60,
            "calibration_ece": 0.05,
            "market_mid_ece": 0.10,
            "net_simulated_pl_dollars": 100.0,
            "trade_candidate_count": 10,
            "current_model_live_quality_candidate_count": 10,
        }},
        gate={"status": "passed"},
        global_live_blockers=[],
        active_color=active_color,
    )


def test_inactive_color_ignores_sibling_live_orders(tmp_path) -> None:
    settings = _settings(tmp_path, app_color="blue")
    evidence = _minimal_shadow_evidence(recent_live_order_count=9, live_order_count=9)
    result = _call(settings, evidence, active_color="green")
    assert result["components"]["safety"] == 10
    assert "crypto_live_orders_detected" not in result["blockers"]


def test_active_color_counts_windowed_live_orders(tmp_path) -> None:
    settings = _settings(tmp_path, app_color="green", crypto_trading_enabled=True)
    evidence = _minimal_shadow_evidence(recent_live_order_count=9, live_order_count=9)
    result = _call(settings, evidence, active_color="green")
    assert result["components"]["safety"] == 0
    assert "crypto_live_orders_detected" in result["blockers"]


def test_windowed_count_zero_but_alltime_nonzero_gives_safety_10(tmp_path) -> None:
    settings = _settings(tmp_path, app_color="green", crypto_trading_enabled=False)
    evidence = _minimal_shadow_evidence(recent_live_order_count=0, live_order_count=50)
    result = _call(settings, evidence, active_color="green")
    assert result["components"]["safety"] == 10
    assert "crypto_live_orders_detected" not in result["blockers"]


def test_inactive_color_with_trading_enabled_still_safe(tmp_path) -> None:
    # Blue is inactive; even with crypto_trading_enabled=True, blue can't trade (deployment lock)
    settings = _settings(tmp_path, app_color="blue", crypto_trading_enabled=True)
    evidence = _minimal_shadow_evidence(recent_live_order_count=5, live_order_count=5)
    result = _call(settings, evidence, active_color="green")
    assert result["components"]["safety"] == 10
    assert "crypto_live_orders_detected" not in result["blockers"]


def test_active_color_no_live_orders_gives_safety_10(tmp_path) -> None:
    settings = _settings(tmp_path, app_color="green", crypto_trading_enabled=False)
    evidence = _minimal_shadow_evidence(recent_live_order_count=0, live_order_count=0)
    result = _call(settings, evidence, active_color="green")
    assert result["components"]["safety"] == 10


def test_active_color_with_trading_enabled_and_no_orders_is_unsafe(tmp_path) -> None:
    # trading_enabled=True means it's actively configured to trade, so safety=0 even with 0 orders
    settings = _settings(tmp_path, app_color="green", crypto_trading_enabled=True)
    evidence = _minimal_shadow_evidence(recent_live_order_count=0, live_order_count=0)
    result = _call(settings, evidence, active_color="green")
    assert result["components"]["safety"] == 0
