"""Unit tests for evaluate_cleanup_risk() and Session 7 enum additions."""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import (
    ContractSide,
    RiskStatus,
    StandDownReason,
    StrategyMode,
    WeatherResolutionState,
)
from kalshi_bot.services.risk import evaluate_cleanup_risk
from kalshi_bot.services.strategy_cleanup import CleanupSignal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _control(*, kill_switch: bool = False) -> MagicMock:
    ctrl = MagicMock()
    ctrl.kill_switch_enabled = kill_switch
    return ctrl


def _settings(**overrides) -> Settings:
    base = {
        "database_url": "sqlite+aiosqlite:///:memory:",
        "strategy_c_enabled": True,
        "strategy_c_shadow_only": True,
        "strategy_c_max_order_notional_dollars": 50.0,
        "strategy_c_max_position_notional_dollars": 50.0,
    }
    base.update(overrides)
    return Settings(**base)


def _signal(
    *,
    side: ContractSide = ContractSide.YES,
    target_price_cents: float = 95.0,
    edge_cents: float = 4.0,
    suppression_reason: str | None = None,
) -> CleanupSignal:
    return CleanupSignal(
        ticker="KXHIGHTBOS-26APR22-T58",
        station="KBOS",
        resolution_state=WeatherResolutionState.LOCKED_YES,
        observed_max_f=60.0,
        threshold_f=58.0,
        fair_value_dollars=Decimal("0.9900"),
        edge_cents=edge_cents,
        target_price_cents=target_price_cents,
        side=side,
        shadow=True,
        suppression_reason=suppression_reason,
    )


# ---------------------------------------------------------------------------
# Enum additions (smoke tests)
# ---------------------------------------------------------------------------

def test_strategy_mode_resolution_cleanup_exists() -> None:
    assert StrategyMode.RESOLUTION_CLEANUP == "resolution_cleanup"


def test_stand_down_reason_strategy_c_variants_exist() -> None:
    assert StandDownReason.STRATEGY_C_LOCK_UNCONFIRMED == "strategy_c_lock_unconfirmed"
    assert StandDownReason.STRATEGY_C_FORECAST_RESIDUAL_EXCEEDED == "strategy_c_forecast_residual_exceeded"
    assert StandDownReason.STRATEGY_C_BOOK_STALE == "strategy_c_book_stale"
    assert StandDownReason.STRATEGY_C_CLI_VARIANCE == "strategy_c_cli_variance"


# ---------------------------------------------------------------------------
# evaluate_cleanup_risk — happy path
# ---------------------------------------------------------------------------

def test_approved_when_all_gates_pass() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(),
        control=_control(),
        settings=_settings(),
    )
    assert verdict.status == RiskStatus.APPROVED


# ---------------------------------------------------------------------------
# Kill switch
# ---------------------------------------------------------------------------

def test_blocked_when_kill_switch_enabled() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(),
        control=_control(kill_switch=True),
        settings=_settings(),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("kill switch" in r.lower() for r in verdict.reasons)


# ---------------------------------------------------------------------------
# strategy_c_enabled flag
# ---------------------------------------------------------------------------

def test_blocked_when_strategy_c_disabled() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(),
        control=_control(),
        settings=_settings(strategy_c_enabled=False),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("strategy_c_enabled" in r for r in verdict.reasons)


# ---------------------------------------------------------------------------
# Per-trade notional cap (target_price_cents / 100)
# ---------------------------------------------------------------------------

def test_blocked_when_order_notional_exceeds_cap() -> None:
    # target_price_cents=95 → order_notional = 0.95, cap = 0.50
    verdict = evaluate_cleanup_risk(
        _signal(target_price_cents=95.0),
        control=_control(),
        settings=_settings(strategy_c_max_order_notional_dollars=0.50),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("order cap" in r for r in verdict.reasons)


def test_approved_when_order_notional_exactly_at_cap() -> None:
    # target_price_cents=50 → 0.50, cap = 0.50
    verdict = evaluate_cleanup_risk(
        _signal(target_price_cents=50.0),
        control=_control(),
        settings=_settings(strategy_c_max_order_notional_dollars=0.50),
    )
    assert verdict.status == RiskStatus.APPROVED


# ---------------------------------------------------------------------------
# Per-position notional cap
# ---------------------------------------------------------------------------

def test_blocked_when_projected_position_exceeds_cap() -> None:
    # target_price_cents=95 → 0.95; existing = 49.50; projected = 50.45 > 50
    verdict = evaluate_cleanup_risk(
        _signal(target_price_cents=95.0),
        control=_control(),
        settings=_settings(strategy_c_max_position_notional_dollars=50.0),
        current_position_notional_dollars=Decimal("49.50"),
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("position cap" in r for r in verdict.reasons)


def test_approved_when_projected_position_within_cap() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(target_price_cents=95.0),
        control=_control(),
        settings=_settings(strategy_c_max_position_notional_dollars=100.0),
        current_position_notional_dollars=Decimal("0"),
    )
    assert verdict.status == RiskStatus.APPROVED


# ---------------------------------------------------------------------------
# Opposite-side guard
# ---------------------------------------------------------------------------

def test_blocked_when_existing_position_on_opposite_side() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(side=ContractSide.YES),
        control=_control(),
        settings=_settings(),
        current_position_side="no",
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert any("opposite-side" in r for r in verdict.reasons)


def test_approved_when_existing_position_on_same_side() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(side=ContractSide.YES),
        control=_control(),
        settings=_settings(),
        current_position_side="yes",
    )
    assert verdict.status == RiskStatus.APPROVED


def test_approved_when_no_existing_position() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(side=ContractSide.NO),
        control=_control(),
        settings=_settings(),
        current_position_side=None,
    )
    assert verdict.status == RiskStatus.APPROVED


# ---------------------------------------------------------------------------
# Multiple simultaneous blocks accumulate
# ---------------------------------------------------------------------------

def test_multiple_blocks_all_reported() -> None:
    verdict = evaluate_cleanup_risk(
        _signal(target_price_cents=95.0),
        control=_control(kill_switch=True),
        settings=_settings(strategy_c_enabled=False, strategy_c_max_order_notional_dollars=0.10),
        current_position_side="no",
    )
    assert verdict.status == RiskStatus.BLOCKED
    assert len(verdict.reasons) >= 3
