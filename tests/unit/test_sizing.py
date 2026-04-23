"""Unit tests for services/sizing.py and its integration with
suggested_trade_count_fp (P2-2)."""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction, WeatherResolutionState
from kalshi_bot.services.signal import StrategySignal, suggested_trade_count_fp
from kalshi_bot.services.sizing import (
    edge_scaled_notional_dollars,
    kelly_fraction_no_side,
    kelly_fraction_yes_side,
)
from kalshi_bot.weather.scoring import WeatherSignalSnapshot


# ---------------------------------------------------------------------------
# Pure Kelly math
# ---------------------------------------------------------------------------


def test_kelly_yes_positive_edge() -> None:
    # p=0.70, P=0.50 → f* = (0.70 - 0.50) / (1 - 0.50) = 0.40
    f = kelly_fraction_yes_side(fair_yes=Decimal("0.70"), target_price=Decimal("0.50"))
    assert f == Decimal("0.4")


def test_kelly_yes_no_edge_returns_zero() -> None:
    # fair_yes == target → zero edge → zero stake
    assert kelly_fraction_yes_side(fair_yes=Decimal("0.50"), target_price=Decimal("0.50")) == Decimal("0")
    # fair_yes < target → negative edge → zero stake (don't short via bet-up)
    assert kelly_fraction_yes_side(fair_yes=Decimal("0.40"), target_price=Decimal("0.50")) == Decimal("0")


def test_kelly_yes_guards_against_target_price_ge_one() -> None:
    assert kelly_fraction_yes_side(fair_yes=Decimal("0.99"), target_price=Decimal("1.00")) == Decimal("0")
    assert kelly_fraction_yes_side(fair_yes=Decimal("0.99"), target_price=Decimal("1.50")) == Decimal("0")


def test_kelly_no_positive_edge() -> None:
    # fair_yes=0.30, target=0.50: NO buy at (1-0.50)=0.50, true prob of NO = 0.70
    # f* = (target - fair_yes) / target = (0.50 - 0.30) / 0.50 = 0.40
    f = kelly_fraction_no_side(fair_yes=Decimal("0.30"), target_price=Decimal("0.50"))
    assert f == Decimal("0.4")


def test_kelly_no_guards_against_zero_target_price() -> None:
    assert kelly_fraction_no_side(fair_yes=Decimal("0.0"), target_price=Decimal("0.0")) == Decimal("0")
    assert kelly_fraction_no_side(fair_yes=Decimal("0.1"), target_price=Decimal("0.0")) == Decimal("0")


def test_edge_scaled_notional_shrinks_by_confidence_and_kelly_multiplier() -> None:
    # Full Kelly yes side at p=0.70, P=0.50 is 0.40.
    # 0.25 Kelly × confidence 0.8 × bankroll $10k × 0.40 = $800
    notional = edge_scaled_notional_dollars(
        total_capital_dollars=Decimal("10000"),
        fair_yes=Decimal("0.70"),
        target_price=Decimal("0.50"),
        side="yes",
        confidence=0.8,
        kelly_multiplier=0.25,
    )
    assert notional == Decimal("800.00")


def test_edge_scaled_notional_zero_when_no_edge() -> None:
    notional = edge_scaled_notional_dollars(
        total_capital_dollars=Decimal("10000"),
        fair_yes=Decimal("0.50"),
        target_price=Decimal("0.50"),
        side="yes",
        confidence=1.0,
        kelly_multiplier=0.25,
    )
    assert notional == Decimal("0")


def test_edge_scaled_notional_zero_when_confidence_zero() -> None:
    notional = edge_scaled_notional_dollars(
        total_capital_dollars=Decimal("10000"),
        fair_yes=Decimal("0.70"),
        target_price=Decimal("0.50"),
        side="yes",
        confidence=0.0,
        kelly_multiplier=0.25,
    )
    assert notional == Decimal("0")


def test_edge_scaled_notional_clamps_out_of_range_inputs() -> None:
    # Confidence > 1 clamped to 1; negative kelly multiplier clamped to 0.
    over_conf = edge_scaled_notional_dollars(
        total_capital_dollars=Decimal("1000"),
        fair_yes=Decimal("0.70"),
        target_price=Decimal("0.50"),
        side="yes",
        confidence=10.0,
        kelly_multiplier=0.25,
    )
    # clamped confidence=1 → 0.25 × 1 × 0.40 × 1000 = 100
    assert over_conf == Decimal("100.00")

    neg_kelly = edge_scaled_notional_dollars(
        total_capital_dollars=Decimal("1000"),
        fair_yes=Decimal("0.70"),
        target_price=Decimal("0.50"),
        side="yes",
        confidence=1.0,
        kelly_multiplier=-1.0,
    )
    assert neg_kelly == Decimal("0")


def test_edge_scaled_notional_unknown_side_returns_zero() -> None:
    notional = edge_scaled_notional_dollars(
        total_capital_dollars=Decimal("10000"),
        fair_yes=Decimal("0.70"),
        target_price=Decimal("0.50"),
        side="banana",
        confidence=1.0,
        kelly_multiplier=0.25,
    )
    assert notional == Decimal("0")


# ---------------------------------------------------------------------------
# Integration with suggested_trade_count_fp
# ---------------------------------------------------------------------------


def _signal(
    *,
    fair_yes: str = "0.7000",
    target: str = "0.5000",
    confidence: float = 0.9,
    side: ContractSide = ContractSide.YES,
) -> StrategySignal:
    weather = WeatherSignalSnapshot(
        fair_yes_dollars=Decimal(fair_yes),
        confidence=confidence,
        forecast_high_f=86,
        current_temp_f=78,
        forecast_delta_f=6.0,
        confidence_band="high",
        trade_regime="standard",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        observation_time=datetime.now(UTC),
        forecast_updated_time=datetime.now(UTC),
        summary="t",
    )
    return StrategySignal(
        fair_yes_dollars=Decimal(fair_yes),
        confidence=confidence,
        edge_bps=int((Decimal(fair_yes) - Decimal(target)) * Decimal("10000")),
        recommended_action=TradeAction.BUY,
        recommended_side=side,
        target_yes_price_dollars=Decimal(target),
        summary="t",
        weather=weather,
        trade_regime="standard",
    )


def test_flag_off_keeps_legacy_sizing() -> None:
    settings = Settings(
        risk_edge_scaled_sizing_enabled=False,
        risk_max_order_notional_dollars=500.0,
        risk_max_order_count_fp=1000.0,
    )
    # Legacy path: notional $500 × confidence_factor(1.0)=1.0 / unit=$0.50 → 1000
    # Clamped to risk_max_order_count_fp (1000).
    count = suggested_trade_count_fp(
        settings=settings,
        signal=_signal(confidence=0.95),
        total_capital_dollars=Decimal("10000"),
    )
    assert count == Decimal("1000.00")


def test_flag_on_shrinks_order_to_kelly_notional() -> None:
    """Kelly at quarter (0.25) × confidence(0.95) × full_kelly(0.4) × $10k = $950.
    Flat cap is $500 so Kelly is larger than cap — result clamps to $500 (legacy)."""
    settings = Settings(
        risk_edge_scaled_sizing_enabled=True,
        risk_edge_scaled_kelly_multiplier=0.25,
        risk_max_order_notional_dollars=500.0,
        risk_max_order_count_fp=10000.0,
    )
    count = suggested_trade_count_fp(
        settings=settings,
        signal=_signal(confidence=0.95),
        max_order_notional_dollars=500.0,
        total_capital_dollars=Decimal("10000"),
    )
    # Flat cap wins: $500 × conf_factor(1.0) = $500 → 1000 contracts at $0.50.
    assert count == Decimal("1000.00")


def test_flag_on_kelly_smaller_than_flat_cap_shrinks_order() -> None:
    """Kelly 0.25 × confidence(0.95) × full_kelly(0.4) × $10k = $950. Flat cap
    is $2,000, so Kelly (smaller) wins: $950 / $0.50 = 1900 contracts."""
    settings = Settings(
        risk_edge_scaled_sizing_enabled=True,
        risk_edge_scaled_kelly_multiplier=0.25,
        risk_max_order_notional_dollars=2000.0,
        risk_max_order_count_fp=10000.0,
    )
    count = suggested_trade_count_fp(
        settings=settings,
        signal=_signal(confidence=0.95),
        max_order_notional_dollars=2000.0,
        total_capital_dollars=Decimal("10000"),
    )
    # $950 / $0.50 = 1900 contracts
    assert count == Decimal("1900.00")


def test_flag_on_without_total_capital_falls_back_to_legacy() -> None:
    """If caller doesn't supply total_capital (e.g. tests / historical replay),
    the new code path is skipped and legacy flat sizing is used."""
    settings = Settings(
        risk_edge_scaled_sizing_enabled=True,
        risk_edge_scaled_kelly_multiplier=0.25,
        risk_max_order_notional_dollars=500.0,
        risk_max_order_count_fp=10000.0,
    )
    count = suggested_trade_count_fp(
        settings=settings,
        signal=_signal(confidence=0.95),
        max_order_notional_dollars=500.0,
        total_capital_dollars=None,
    )
    # Legacy path: $500 × conf_factor(1.0) / $0.50 = 1000
    assert count == Decimal("1000.00")


def test_flag_on_zero_edge_signal_returns_none() -> None:
    """An on-by-a-cent signal at zero edge should produce no order."""
    settings = Settings(
        risk_edge_scaled_sizing_enabled=True,
        risk_edge_scaled_kelly_multiplier=0.25,
        risk_max_order_notional_dollars=500.0,
        risk_max_order_count_fp=1000.0,
    )
    count = suggested_trade_count_fp(
        settings=settings,
        signal=_signal(fair_yes="0.5000", target="0.5000"),
        max_order_notional_dollars=500.0,
        total_capital_dollars=Decimal("10000"),
    )
    assert count is None
