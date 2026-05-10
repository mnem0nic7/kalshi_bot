from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction, WeatherResolutionState
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor, _apply_city_strategy_override
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.signal import StrategySignal


@pytest.mark.asyncio
async def test_market_gates_do_not_overwrite_executable_ask_edge() -> None:
    supervisor = WorkflowSupervisor.__new__(WorkflowSupervisor)
    supervisor.settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6400"),
        confidence=0.90,
        edge_bps=800,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5600"),
        summary="ask edge should remain authoritative",
        resolution_state=WeatherResolutionState.UNRESOLVED,
        candidate_trace={},
    )

    allowed = await supervisor._run_market_gates(
        MagicMock(),
        signal,
        {
            "yes_bid_dollars": "0.4000",
            "yes_ask_dollars": "0.5600",
            "volume": 200,
        },
        "WX-TEST",
    )

    assert allowed is True
    assert signal.edge_bps == 800
    assert signal.candidate_trace["market_mid_edge_bps"] == 1600


def test_city_strategy_override_preserves_all_tunable_gate_fields() -> None:
    """A city-strategy assignment may change operational fields but must never
    override the 7 tunable fields owned by AutonomousGateTuningService."""
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    champion = AgentPackService(settings).runtime_thresholds()

    # Strategy preset with deliberately different values for the 3 previously-overridable fields.
    strategy_record = {
        "risk_min_edge_bps": 9999,
        "risk_max_order_notional_dollars": 50.0,
        "risk_max_position_notional_dollars": 100.0,
        "trigger_max_spread_bps": 9999,
        "trigger_cooldown_seconds": 30,
        "strategy_quality_edge_buffer_bps": 0,
        "strategy_min_remaining_payout_bps": 9999,
        "risk_safe_capital_reserve_ratio": 0.5,
        "risk_risky_capital_max_ratio": 0.5,
    }

    result = _apply_city_strategy_override(champion, strategy_record)

    # Tunable gate fields: city-strategy must NOT win.
    from kalshi_bot.services.autonomous_gate_tuning import TUNABLE_GATE_FIELDS
    for field in TUNABLE_GATE_FIELDS:
        assert getattr(result, field) == getattr(champion, field), (
            f"Tunable field '{field}' was overridden by city-strategy: "
            f"champion={getattr(champion, field)}, result={getattr(result, field)}"
        )

    # Non-tunable operational fields: city-strategy values should apply.
    assert result.risk_max_order_notional_dollars == 50.0
    assert result.risk_max_position_notional_dollars == 100.0
    assert result.trigger_cooldown_seconds == 30
    assert result.strategy_quality_edge_buffer_bps == 0
    assert result.risk_safe_capital_reserve_ratio == 0.5
