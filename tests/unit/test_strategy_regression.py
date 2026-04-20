from __future__ import annotations

import pytest
from kalshi_bot.services.strategy_regression import (
    STRATEGY_PRESETS,
    _would_have_traded,
    _thresholds_from_dict,
)


def _room(edge_bps: int, spread_bps: int | None = 100, remaining_payout_dollars: float | None = 0.60, stand_down_reason: str | None = None) -> dict:
    eligibility: dict = {}
    if spread_bps is not None:
        eligibility["market_spread_bps"] = spread_bps
    if remaining_payout_dollars is not None:
        eligibility["remaining_payout_dollars"] = str(remaining_payout_dollars)
    return {
        "edge_bps": edge_bps,
        "signal_payload": {
            "stand_down_reason": stand_down_reason,
            "eligibility": eligibility,
        },
    }


@pytest.fixture
def aggressive():
    return _thresholds_from_dict(next(p for p in STRATEGY_PRESETS if p["name"] == "aggressive")["thresholds"])


@pytest.fixture
def moderate():
    return _thresholds_from_dict(next(p for p in STRATEGY_PRESETS if p["name"] == "moderate")["thresholds"])


@pytest.fixture
def conservative():
    return _thresholds_from_dict(next(p for p in STRATEGY_PRESETS if p["name"] == "conservative")["thresholds"])


def test_aggressive_trades_on_min_edge(aggressive):
    # spread=10, net_edge=10 >= quality_buffer=0
    room = _room(edge_bps=20, spread_bps=10)
    assert _would_have_traded(room, aggressive) is True


def test_moderate_rejects_below_min_edge(moderate):
    room = _room(edge_bps=30, spread_bps=10)
    assert _would_have_traded(room, moderate) is False


def test_moderate_trades_on_sufficient_edge(moderate):
    # edge=80, spread=50, net=30 >= quality_buffer=20
    room = _room(edge_bps=80, spread_bps=50)
    assert _would_have_traded(room, moderate) is True


def test_conservative_rejects_wide_spread(conservative):
    room = _room(edge_bps=150, spread_bps=400)
    assert _would_have_traded(room, conservative) is False


def test_conservative_rejects_low_remaining_payout(conservative):
    # remaining_payout_bps = 0.05 * 10000 = 500, conservative requires 800
    room = _room(edge_bps=150, spread_bps=100, remaining_payout_dollars=0.05)
    assert _would_have_traded(room, conservative) is False


def test_conservative_trades_high_quality(conservative):
    room = _room(edge_bps=150, spread_bps=100, remaining_payout_dollars=0.15)
    assert _would_have_traded(room, conservative) is True


def test_quality_buffer_blocks_trade(moderate):
    # edge=60, spread=50, net=10 which is < quality_buffer=20
    room = _room(edge_bps=60, spread_bps=50)
    assert _would_have_traded(room, moderate) is False


def test_quality_buffer_allows_trade(moderate):
    # edge=80, spread=50, net=30 which is > quality_buffer=20
    room = _room(edge_bps=80, spread_bps=50)
    assert _would_have_traded(room, moderate) is True


def test_missing_spread_skips_spread_gate(moderate):
    # No spread data — spread gate is skipped, only edge gate applies
    room = _room(edge_bps=60, spread_bps=None)
    assert _would_have_traded(room, moderate) is True


def test_missing_remaining_payout_skips_payout_gate(moderate):
    # spread=30, net=30 >= quality_buffer=20; payout gate skipped (None)
    room = _room(edge_bps=60, spread_bps=30, remaining_payout_dollars=None)
    assert _would_have_traded(room, moderate) is True


def test_all_three_presets_have_required_fields():
    required = {
        "risk_min_edge_bps", "risk_max_order_notional_dollars", "risk_max_position_notional_dollars",
        "trigger_max_spread_bps", "trigger_cooldown_seconds", "strategy_quality_edge_buffer_bps",
        "strategy_min_remaining_payout_bps", "risk_safe_capital_reserve_ratio", "risk_risky_capital_max_ratio",
    }
    for preset in STRATEGY_PRESETS:
        assert required.issubset(preset["thresholds"].keys()), f"{preset['name']} missing fields"
