from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction, WeatherResolutionState
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
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
