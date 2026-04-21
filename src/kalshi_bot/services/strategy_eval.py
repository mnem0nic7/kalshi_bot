from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService

WIN_RATE_STRONG = 0.60
WIN_RATE_WEAK = 0.35
MIN_SETTLED_CONTRACTS = 50.0
STEP_BPS = 10
MIN_EDGE_BPS = 20
MAX_EDGE_BPS = 150
ADJUST_INTERVAL_HOURS = 24
TIGHTEN_COOLDOWN_HOURS = 48

_CHECKPOINT = "strategy_eval:edge_adjustment"


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
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            result = await self._maybe_adjust(repo)
            await session.commit()
        return result

    async def _maybe_adjust(self, repo: PlatformRepository) -> dict[str, Any] | None:
        checkpoint_name = f"{_CHECKPOINT}:{self.settings.kalshi_env}"
        win_rate_data = await repo.get_fill_win_rate_30d(kalshi_env=self.settings.kalshi_env)
        total = win_rate_data["total_contracts"]
        won = win_rate_data["won_contracts"]

        if total < MIN_SETTLED_CONTRACTS:
            return None

        win_rate = won / total
        now = datetime.now(UTC)

        checkpoint = await repo.get_checkpoint(checkpoint_name)
        if checkpoint is not None:
            last_at = datetime.fromisoformat(checkpoint.payload["adjusted_at"])
            last_dir = checkpoint.payload.get("direction")

            if now - last_at < timedelta(hours=ADJUST_INTERVAL_HOURS):
                return None

            if last_dir == "tightened" and win_rate >= WIN_RATE_STRONG:
                if now - last_at < timedelta(hours=TIGHTEN_COOLDOWN_HOURS):
                    return None

        if win_rate >= WIN_RATE_STRONG:
            delta = -STEP_BPS
            direction = "loosened"
        elif win_rate <= WIN_RATE_WEAK:
            delta = +STEP_BPS
            direction = "tightened"
        else:
            return None

        pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
        current_bps = pack.thresholds.risk_min_edge_bps or self.settings.risk_min_edge_bps
        new_bps = max(MIN_EDGE_BPS, min(MAX_EDGE_BPS, current_bps + delta))

        if new_bps == current_bps:
            return None

        new_version = f"auto-{direction}-{now.strftime('%Y%m%dT%H%M%SZ')}"
        new_pack = pack.model_copy(update={
            "version": new_version,
            "status": "champion",
            "thresholds": pack.thresholds.model_copy(update={"risk_min_edge_bps": new_bps}),
        })
        await repo.create_agent_pack(new_pack)
        await self.agent_pack_service.assign_pack_to_color(repo, color=self.settings.app_color, version=new_version)

        event_payload: dict[str, Any] = {
            "adjusted_at": now.isoformat(),
            "direction": direction,
            "old_bps": current_bps,
            "new_bps": new_bps,
            "win_rate": round(win_rate, 4),
            "total_contracts": total,
        }
        await repo.set_checkpoint(checkpoint_name, cursor=None, payload=event_payload)
        await repo.log_ops_event(
            severity="info",
            summary=(
                f"Auto-adjusted risk_min_edge_bps {current_bps}→{new_bps} "
                f"({direction}, win_rate={win_rate:.1%}, {total:.0f} settled contracts)"
            ),
            source="strategy_eval",
            payload=event_payload,
        )
        return event_payload
