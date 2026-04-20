from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import TrainingReadiness
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.self_improve import SelfImproveService


class FakeCorpusService:
    async def compute_readiness(self, *, persist: bool = False) -> TrainingReadiness:
        return TrainingReadiness(
            complete_room_count=2,
            market_diversity_count=1,
            settled_room_count=0,
            trade_positive_room_count=0,
            ready_for_sft_export=False,
            ready_for_critique=False,
            ready_for_evaluation=False,
            ready_for_promotion=False,
            missing_indicators=["not enough complete rooms", "not enough settled rooms"],
        )


@pytest.mark.asyncio
async def test_self_improve_critique_blocks_when_training_corpus_is_not_ready() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = SelfImproveService(  # type: ignore[arg-type]
        settings,
        None,
        None,
        FakeCorpusService(),  # type: ignore[arg-type]
        AgentPackService(settings),
        None,
    )

    with pytest.raises(ValueError, match="not ready for critique"):
        await service.critique_recent_rooms()
