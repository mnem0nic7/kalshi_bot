from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import ShadowCampaignRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.reconcile import ReconciliationService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.shadow_campaign import ShadowCampaignService
from kalshi_bot.services.self_improve import SelfImproveService
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.services.streaming import MarketStreamService
from kalshi_bot.weather.mapping import WeatherMarketDirectory


class DaemonService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        discovery_service: DiscoveryService,
        stream_service: MarketStreamService,
        reconciliation_service: ReconciliationService,
        research_coordinator: ResearchCoordinator,
        auto_trigger_service: AutoTriggerService,
        shadow_training_service: ShadowTrainingService,
        shadow_campaign_service: ShadowCampaignService | None,
        self_improve_service: SelfImproveService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.discovery_service = discovery_service
        self.stream_service = stream_service
        self.reconciliation_service = reconciliation_service
        self.research_coordinator = research_coordinator
        self.auto_trigger_service = auto_trigger_service
        self.shadow_training_service = shadow_training_service
        self.shadow_campaign_service = shadow_campaign_service
        self.self_improve_service = self_improve_service
        self._auto_trigger_enabled_for_run = settings.trigger_enable_auto_rooms

    async def reconcile_once(self) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            summary = await self.reconciliation_service.reconcile(repo, subaccount=self.settings.kalshi_subaccount)
            await repo.set_checkpoint(
                f"daemon_reconcile:{self.settings.app_color}",
                None,
                {
                    "reconciled_at": self._now_iso(),
                    "summary": asdict(summary),
                    "kalshi_env": self.settings.kalshi_env,
                },
            )
            await session.commit()
        return asdict(summary)

    async def heartbeat_once(self) -> dict[str, Any]:
        payload = {
            "app_color": self.settings.app_color,
            "kalshi_env": self.settings.kalshi_env,
            "shadow_mode": self.settings.app_shadow_mode,
            "auto_rooms_enabled": self.settings.trigger_enable_auto_rooms,
            "heartbeat_at": self._now_iso(),
        }
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control()
            active_rooms = await repo.count_active_rooms()
            checkpoint = await repo.get_checkpoint("reconcile")
            self_improve_status = dict(control.notes.get("agent_packs") or {})
            payload.update(
                {
                    "active_color": control.active_color,
                    "kill_switch_enabled": control.kill_switch_enabled,
                    "active_rooms": active_rooms,
                    "has_reconcile_checkpoint": checkpoint is not None,
                    "agent_pack_status": self_improve_status,
                    "training_campaign_enabled": self.settings.training_campaign_enabled,
                }
            )
            await repo.log_ops_event(
                severity="info",
                summary="Daemon heartbeat",
                source="daemon",
                payload=payload,
            )
            last_reconcile = await repo.get_checkpoint(f"daemon_reconcile:{self.settings.app_color}")
            await repo.set_checkpoint(
                f"daemon_heartbeat:{self.settings.app_color}",
                None,
                {
                    **payload,
                    "last_reconcile_at": (
                        last_reconcile.payload.get("reconciled_at")
                        if last_reconcile is not None and isinstance(last_reconcile.payload, dict)
                        else None
                    ),
                },
            )
            await session.commit()
        if (
            self.shadow_campaign_service is not None
            and self.settings.training_campaign_enabled
            and self.settings.app_color == payload.get("active_color")
        ):
            await self.shadow_campaign_service.run(
                ShadowCampaignRequest(
                    limit=self.settings.training_campaign_rooms_per_run,
                    reason="daemon_shadow_campaign",
                )
            )
        rollout_result = await self.self_improve_service.monitor_rollouts()
        if rollout_result.status == "canary_running":
            canary = rollout_result.payload
            if self.settings.app_color == canary.get("color"):
                await self.shadow_training_service.run_shadow_sweep(limit=1, reason="canary_shadow")
        return payload

    async def run(
        self,
        *,
        markets: list[str] | None = None,
        public_only: bool = False,
        auto_trigger: bool | None = None,
        max_messages: int | None = None,
        run_seconds: float | None = None,
    ) -> dict[str, Any]:
        selected_markets = markets or await self.discovery_service.list_stream_markets()
        should_auto_trigger = self.settings.trigger_enable_auto_rooms if auto_trigger is None else auto_trigger
        self._auto_trigger_enabled_for_run = should_auto_trigger

        if self.settings.daemon_start_with_reconcile:
            await self.reconcile_once()
        await self.heartbeat_once()

        tasks: dict[str, asyncio.Task] = {
            "stream": asyncio.create_task(
                self.stream_service.stream(
                    market_tickers=selected_markets,
                    include_private=not public_only,
                    max_messages=max_messages,
                    on_market_update=self._handle_market_update,
                )
            ),
            "reconcile": asyncio.create_task(self._periodic_reconcile_loop()),
            "heartbeat": asyncio.create_task(self._periodic_heartbeat_loop()),
        }
        if run_seconds is not None:
            tasks["timer"] = asyncio.create_task(asyncio.sleep(run_seconds))

        try:
            done, pending = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
            completed_name = next(name for name, task in tasks.items() if task in done)

            if completed_name in {"reconcile", "heartbeat"}:
                done_task = tasks[completed_name]
                exc = done_task.exception()
                if exc is not None:
                    raise exc

            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await self.research_coordinator.wait_for_tasks()
            await self.auto_trigger_service.wait_for_tasks()

            result: dict[str, Any] = {"completed": completed_name, "markets": selected_markets}
            stream_task = tasks["stream"]
            if stream_task.done() and not stream_task.cancelled():
                exc = stream_task.exception()
                if exc is not None:
                    raise exc
                result["processed_messages"] = stream_task.result()
            return result
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def _handle_market_update(self, market_ticker: str) -> None:
        await self.research_coordinator.handle_market_update(market_ticker)
        if self._auto_trigger_enabled_for_run:
            await self.auto_trigger_service.handle_market_update(market_ticker)

    async def _periodic_reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.daemon_reconcile_interval_seconds)
            await self.reconcile_once()

    async def _periodic_heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.daemon_heartbeat_interval_seconds)
            await self.heartbeat_once()

    @staticmethod
    def _now_iso() -> str:
        from datetime import UTC, datetime

        return datetime.now(UTC).isoformat()
