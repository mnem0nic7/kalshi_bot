from __future__ import annotations

from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.strategy_eval import StrategyEvaluationService


@pytest.mark.asyncio
async def test_strategy_evaluation_service_legacy_edge_mutator_is_disabled() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./strategy-eval.db")
    service = StrategyEvaluationService(
        settings=settings,
        session_factory=SimpleNamespace(),
        agent_pack_service=AgentPackService(settings),
    )

    result = await service.maybe_adjust()

    assert result == {
        "status": "disabled",
        "reason": "replaced_by_autonomous_gate_tuning",
        "mutated": False,
    }
