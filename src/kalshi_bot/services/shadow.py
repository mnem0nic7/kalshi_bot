from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
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
        supervisor,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.discovery_service = discovery_service
        self.supervisor = supervisor

    async def create_shadow_room(
        self,
        market_ticker: str,
        *,
        name: str | None = None,
        prompt: str | None = None,
    ) -> ShadowRunResult:
        room_name = name or self._default_room_name(market_ticker)
        room_prompt = prompt or f"Shadow training capture for {market_ticker}."
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            room = await repo.create_room(
                RoomCreate(name=room_name, market_ticker=market_ticker, prompt=room_prompt),
                active_color=control.active_color,
                shadow_mode=self.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
            )
            await repo.log_ops_event(
                severity="info",
                summary=f"Created shadow room for {market_ticker}",
                source="shadow_training",
                payload={"room_id": room.id, "market_ticker": market_ticker},
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
    ) -> ShadowRunResult:
        result = await self.create_shadow_room(market_ticker, name=name, prompt=prompt)
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
    ) -> list[ShadowRunResult]:
        selected_markets = markets or await self.discovery_service.list_stream_markets()
        if limit is not None:
            selected_markets = selected_markets[:limit]
        results: list[ShadowRunResult] = []
        for market_ticker in selected_markets:
            results.append(await self.run_shadow_room(market_ticker, reason=reason))
        return results

    @staticmethod
    def _default_room_name(market_ticker: str) -> str:
        stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        return f"shadow {market_ticker} {stamp}"
