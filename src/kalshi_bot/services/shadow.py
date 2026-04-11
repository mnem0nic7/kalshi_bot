from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.discovery import DiscoveryService


@dataclass(slots=True)
class ShadowRunResult:
    room_id: str
    market_ticker: str
    room_name: str
    stage: str


class ShadowTrainingService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        discovery_service: DiscoveryService,
        agent_pack_service: AgentPackService,
        supervisor,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.discovery_service = discovery_service
        self.agent_pack_service = agent_pack_service
        self.supervisor = supervisor

    async def create_shadow_room(
        self,
        market_ticker: str,
        *,
        name: str | None = None,
        prompt: str | None = None,
        campaign: dict[str, Any] | None = None,
    ) -> ShadowRunResult:
        room_name = name or self._default_room_name(market_ticker)
        room_prompt = prompt or f"Shadow training capture for {market_ticker}."
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
            room = await repo.create_room(
                RoomCreate(name=room_name, market_ticker=market_ticker, prompt=room_prompt),
                active_color=self.settings.app_color,
                shadow_mode=self.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self.settings.kalshi_env,
                agent_pack_version=pack.version,
            )
            if campaign is not None:
                await repo.save_room_campaign(
                    room_id=room.id,
                    campaign_id=str(campaign.get("campaign_id") or "shadow_campaign"),
                    trigger_source=str(campaign.get("trigger_source") or "shadow_campaign"),
                    city_bucket=campaign.get("city_bucket"),
                    market_regime_bucket=campaign.get("market_regime_bucket"),
                    difficulty_bucket=campaign.get("difficulty_bucket"),
                    outcome_bucket=campaign.get("outcome_bucket"),
                    payload=campaign,
                )
            await repo.log_ops_event(
                severity="info",
                summary=f"Created shadow room for {market_ticker}",
                source="shadow_training",
                payload={
                    "room_id": room.id,
                    "market_ticker": market_ticker,
                    "agent_pack_version": pack.version,
                    "campaign": campaign,
                },
                room_id=room.id,
            )
            await session.commit()
        return ShadowRunResult(
            room_id=room.id,
            market_ticker=market_ticker,
            room_name=room.name,
            stage=room.stage,
        )

    async def run_shadow_room(
        self,
        market_ticker: str,
        *,
        name: str | None = None,
        prompt: str | None = None,
        reason: str = "shadow_run",
        campaign: dict[str, Any] | None = None,
    ) -> ShadowRunResult:
        result = await self.create_shadow_room(market_ticker, name=name, prompt=prompt, campaign=campaign)
        await self.supervisor.run_room(result.room_id, reason=reason)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            room = await repo.get_room(result.room_id)
            await session.commit()
        return ShadowRunResult(
            room_id=result.room_id,
            market_ticker=market_ticker,
            room_name=result.room_name,
            stage=room.stage if room is not None else result.stage,
        )

    async def run_shadow_sweep(
        self,
        *,
        markets: list[str] | None = None,
        limit: int | None = None,
        reason: str = "shadow_sweep",
        campaign_factory: Any | None = None,
    ) -> list[ShadowRunResult]:
        selected_markets = markets or await self.discovery_service.list_stream_markets()
        if limit is not None:
            selected_markets = selected_markets[:limit]
        results: list[ShadowRunResult] = []
        for market_ticker in selected_markets:
            campaign = campaign_factory(market_ticker) if callable(campaign_factory) else None
            results.append(await self.run_shadow_room(market_ticker, reason=reason, campaign=campaign))
        return results

    @staticmethod
    def _default_room_name(market_ticker: str) -> str:
        stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        return f"shadow {market_ticker} {stamp}"
