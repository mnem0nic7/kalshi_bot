from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService


class StrategyEvaluationService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        agent_pack_service: AgentPackService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.agent_pack_service = agent_pack_service

    async def maybe_adjust(self) -> dict[str, Any] | None:
        return {
            "status": "disabled",
            "reason": "replaced_by_autonomous_gate_tuning",
            "mutated": False,
        }

    async def _maybe_adjust(self, repo: PlatformRepository) -> dict[str, Any] | None:
        _ = repo
        return {
            "status": "disabled",
            "reason": "replaced_by_autonomous_gate_tuning",
            "mutated": False,
        }
