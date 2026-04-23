from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import HistoricalIntelligenceRunRequest, ShadowCampaignRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.historical_training import HistoricalTrainingService
from kalshi_bot.services.historical_intelligence import HistoricalIntelligenceService
from kalshi_bot.services.historical_pipeline import HistoricalPipelineService
from kalshi_bot.services.market_history import MarketHistoryService
from kalshi_bot.services.reconcile import ReconciliationService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.shadow_campaign import ShadowCampaignService
from kalshi_bot.services.self_improve import SelfImproveService
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.services.strategy_eval import StrategyEvaluationService
from kalshi_bot.services.strategy_auto_evolve import StrategyAutoEvolveService
from kalshi_bot.services.strategy_codex import StrategyCodexService
from kalshi_bot.services.strategy_dashboard import StrategyDashboardService
from kalshi_bot.services.stop_loss import StopLossService
from kalshi_bot.services.strategy_cleanup_service import StrategyCleanupService
from kalshi_bot.services.monotonicity_scanner_service import MonotonicityArbScannerService
from kalshi_bot.services.strategy_regression import StrategyRegressionService, WINDOW_DAYS as DEFAULT_STRATEGY_WINDOW_DAYS
from kalshi_bot.services.streaming import MarketStreamService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.weather.mapping import WeatherMarketDirectory

logger = logging.getLogger(__name__)


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
        training_corpus_service: TrainingCorpusService | None = None,
        historical_training_service: HistoricalTrainingService | None = None,
        historical_intelligence_service: HistoricalIntelligenceService | None = None,
        historical_pipeline_service: HistoricalPipelineService | None = None,
        market_history_service: MarketHistoryService | None = None,
        strategy_eval_service: StrategyEvaluationService | None = None,
        strategy_regression_service: StrategyRegressionService | None = None,
        stop_loss_service: StopLossService | None = None,
        strategy_cleanup_service: StrategyCleanupService | None = None,
        monotonicity_arb_service: MonotonicityArbScannerService | None = None,
        strategy_codex_service: StrategyCodexService | None = None,
        strategy_dashboard_service: StrategyDashboardService | None = None,
        strategy_auto_evolve_service: StrategyAutoEvolveService | None = None,
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
        self.training_corpus_service = training_corpus_service
        self.historical_training_service = historical_training_service
        self.historical_intelligence_service = historical_intelligence_service
        self.historical_pipeline_service = historical_pipeline_service
        self.market_history_service = market_history_service
        self.strategy_eval_service = strategy_eval_service
        self.strategy_regression_service = strategy_regression_service
        self.strategy_cleanup_service = strategy_cleanup_service
        self.monotonicity_arb_service = monotonicity_arb_service
        self.strategy_codex_service = strategy_codex_service
        self.strategy_dashboard_service = strategy_dashboard_service
        self.strategy_auto_evolve_service = strategy_auto_evolve_service
        self.stop_loss_service = stop_loss_service
        self._auto_trigger_enabled_for_run = settings.trigger_enable_auto_rooms
        self._heartbeat_follow_up_task: asyncio.Task[None] | None = None

    async def _recover_orphaned_rooms(self) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            reaped_ids = await repo.reap_orphaned_rooms(
                color=self.settings.app_color,
                kalshi_env=self.settings.kalshi_env,
            )
            if reaped_ids:
                await repo.log_ops_event(
                    severity="warning",
                    summary=f"Daemon startup: reaped {len(reaped_ids)} orphaned room(s) from prior run",
                    source="daemon",
                    payload={
                        "room_ids": reaped_ids,
                        "color": self.settings.app_color,
                        "kalshi_env": self.settings.kalshi_env,
                    },
                )
                logger.warning("Reaped %d orphaned room(s) on startup: %s", len(reaped_ids), reaped_ids)
            await session.commit()

    async def reconcile_once(self) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            summary = await self.reconciliation_service.reconcile(
                repo,
                subaccount=self.settings.kalshi_subaccount,
                kalshi_env=self.settings.kalshi_env,
            )
            await repo.set_checkpoint(
                f"daemon_reconcile:{self.settings.kalshi_env}:{self.settings.app_color}",
                None,
                {
                    "reconciled_at": self._now_iso(),
                    "summary": asdict(summary),
                    "kalshi_env": self.settings.kalshi_env,
                },
            )
            purged = await repo.vacuum_memory_notes(older_than_days=self.settings.daemon_memory_note_retention_days)
            if purged:
                logger.info("Vacuumed %d memory notes older than %d days", purged, self.settings.daemon_memory_note_retention_days)
            await session.commit()
        result = asdict(summary)
        if summary.settlements_count > 0 and self.strategy_eval_service is not None:
            adjustment = await self.strategy_eval_service.maybe_adjust()
            if adjustment:
                result["edge_adjustment"] = adjustment
        return result

    async def heartbeat_once(self, *, run_follow_up: bool = True) -> dict[str, Any]:
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
            active_rooms = await repo.count_active_rooms(kalshi_env=self.settings.kalshi_env)
            checkpoint = await repo.get_checkpoint(f"reconcile:{self.settings.kalshi_env}")
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
            last_reconcile = await repo.get_checkpoint(
                f"daemon_reconcile:{self.settings.kalshi_env}:{self.settings.app_color}"
            )
            await repo.set_checkpoint(
                f"daemon_heartbeat:{self.settings.kalshi_env}:{self.settings.app_color}",
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
        if run_follow_up:
            await self._run_heartbeat_follow_up(payload)
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

        await self.self_improve_service.apply_pending_pack_promotion(app_color=self.settings.app_color)
        await self._recover_orphaned_rooms()
        if self.settings.daemon_start_with_reconcile:
            await self.reconcile_once()
        self._schedule_heartbeat_follow_up(await self.heartbeat_once(run_follow_up=False))

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
            "market_history": asyncio.create_task(self._periodic_market_history_loop()),
            "stop_loss": asyncio.create_task(self._periodic_stop_loss_loop()),
            "strategy_c": asyncio.create_task(self._periodic_strategy_c_loop()),
            "monotonicity_arb": asyncio.create_task(self._periodic_monotonicity_arb_loop()),
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
            await self._await_heartbeat_follow_up()

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
            follow_up_task = self._heartbeat_follow_up_task
            if follow_up_task is not None and not follow_up_task.done():
                follow_up_task.cancel()
                await asyncio.gather(follow_up_task, return_exceptions=True)

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
            self._schedule_heartbeat_follow_up(await self.heartbeat_once(run_follow_up=False))

    async def _periodic_stop_loss_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.stop_loss_check_interval_seconds)
            if self.stop_loss_service is None:
                continue
            try:
                await self.stop_loss_service.check_once()
            except Exception:
                logger.warning("stop_loss check failed", exc_info=True)

    async def _periodic_market_history_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.daemon_market_history_interval_seconds)
            if self.market_history_service is None:
                continue
            try:
                await self.market_history_service.snapshot_once()
                await self.market_history_service.purge_once()
            except Exception:
                logger.warning("market_history loop error", exc_info=True)

    async def _periodic_strategy_c_loop(self) -> None:
        interval = self.settings.strategy_c_cadence_idle_seconds
        while True:
            await asyncio.sleep(interval)
            if self.strategy_cleanup_service is None:
                continue
            try:
                await self.strategy_cleanup_service.sweep()
            except Exception:
                logger.warning("strategy_c sweep error", exc_info=True)

    async def _periodic_monotonicity_arb_loop(self) -> None:
        interval = self.settings.monotonicity_arb_cadence_seconds
        while True:
            await asyncio.sleep(interval)
            if self.monotonicity_arb_service is None:
                continue
            try:
                await self.monotonicity_arb_service.sweep()
            except Exception:
                logger.warning("monotonicity_arb sweep error", exc_info=True)

    async def _run_heartbeat_follow_up(self, payload: dict[str, Any]) -> None:
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
        if self.settings.app_color == payload.get("active_color"):
            checkpoint_capture = await self._maybe_capture_checkpoint_archives()
            if checkpoint_capture is not None:
                payload["checkpoint_capture"] = checkpoint_capture
            settlement_follow_up = await self._maybe_run_settlement_follow_up()
            if settlement_follow_up is not None:
                payload["settlement_follow_up"] = settlement_follow_up
            strategy_regression = await self._maybe_run_strategy_regression()
            if strategy_regression is not None:
                payload["strategy_regression"] = strategy_regression
            strategy_codex_nightly = await self._maybe_run_strategy_codex_nightly()
            if strategy_codex_nightly is not None:
                if strategy_codex_nightly.get("mode") == "auto_evolve":
                    payload["strategy_auto_evolve"] = strategy_codex_nightly
                else:
                    payload["strategy_codex_nightly"] = strategy_codex_nightly
            historical_pipeline = await self._maybe_run_historical_pipeline()
            if historical_pipeline is not None:
                payload["historical_pipeline"] = historical_pipeline
            elif self.historical_pipeline_service is None:
                historical_intelligence = await self._maybe_run_historical_intelligence()
                if historical_intelligence is not None:
                    payload["historical_intelligence"] = historical_intelligence
        rollout_result = await self.self_improve_service.monitor_rollouts()
        if rollout_result.status == "canary_running":
            canary = rollout_result.payload
            if self.settings.app_color == canary.get("color"):
                await self.shadow_training_service.run_shadow_sweep(limit=1, reason="canary_shadow")

    def _schedule_heartbeat_follow_up(self, payload: dict[str, Any]) -> None:
        if self._heartbeat_follow_up_task is not None and not self._heartbeat_follow_up_task.done():
            return
        self._heartbeat_follow_up_task = asyncio.create_task(self._heartbeat_follow_up_runner(dict(payload)))

    async def _heartbeat_follow_up_runner(self, payload: dict[str, Any]) -> None:
        try:
            await self._run_heartbeat_follow_up(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("daemon heartbeat follow-up failed")
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.log_ops_event(
                    severity="error",
                    summary="Daemon heartbeat follow-up error",
                    source="daemon",
                    payload={"error": str(exc), "app_color": self.settings.app_color},
                )
                await session.commit()
        finally:
            if asyncio.current_task() is self._heartbeat_follow_up_task:
                self._heartbeat_follow_up_task = None

    async def _await_heartbeat_follow_up(self) -> None:
        task = self._heartbeat_follow_up_task
        if task is None:
            return
        await asyncio.gather(task, return_exceptions=True)

    async def _maybe_run_settlement_follow_up(self) -> dict[str, Any] | None:
        if self.training_corpus_service is None:
            return None
        summary = await self.training_corpus_service.get_settlement_focus_summary()
        actionable = int(summary.get("status_counts", {}).get("awaiting_settlement", 0)) + int(
            summary.get("status_counts", {}).get("possible_ingestion_gap", 0)
        )
        if actionable <= 0:
            return summary

        last_reconcile_at = await self._checkpoint_time(
            f"daemon_reconcile:{self.settings.kalshi_env}:{self.settings.app_color}"
        )
        last_follow_up_at = await self._checkpoint_time(
            f"daemon_settlement_followup:{self.settings.kalshi_env}:{self.settings.app_color}"
        )
        min_interval = timedelta(seconds=max(30, min(self.settings.daemon_reconcile_interval_seconds, 120)))
        now = datetime.now(UTC)
        if last_reconcile_at is not None and now - last_reconcile_at < min_interval:
            return summary
        if last_follow_up_at is not None and now - last_follow_up_at < min_interval:
            return summary

        settlement_backfill = None
        if self.historical_training_service is not None:
            settlement_backfill = await self.historical_training_service.backfill_settlements(
                date_from=(now - timedelta(days=self.settings.training_window_days)).date(),
                date_to=now.date(),
            )
            summary = await self.training_corpus_service.get_settlement_focus_summary()

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.log_ops_event(
                severity="info",
                summary="Settlement follow-up reconcile triggered",
                source="daemon",
                payload={
                    "app_color": self.settings.app_color,
                    "unsettled_count": summary.get("unsettled_count"),
                    "status_counts": summary.get("status_counts"),
                    "settlement_backfill": settlement_backfill,
                },
            )
            await repo.set_checkpoint(
                f"daemon_settlement_followup:{self.settings.kalshi_env}:{self.settings.app_color}",
                None,
                {
                    "followed_at": now.isoformat(),
                    "summary": summary,
                    "settlement_backfill": settlement_backfill,
                },
            )
            await session.commit()
        try:
            await self.reconcile_once()
        except Exception as exc:
            logger.warning("settlement follow-up reconcile failed", exc_info=True)
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.log_ops_event(
                    severity="warning",
                    summary="Settlement follow-up reconcile failed",
                    source="daemon",
                    payload={
                        "app_color": self.settings.app_color,
                        "error": str(exc),
                        "unsettled_count": summary.get("unsettled_count"),
                        "status_counts": summary.get("status_counts"),
                        "settlement_backfill": settlement_backfill,
                    },
                )
                await session.commit()
            summary = {**summary, "reconcile_error": str(exc)}
        if settlement_backfill is not None:
            summary["settlement_backfill"] = settlement_backfill
        return summary

    async def _maybe_capture_checkpoint_archives(self) -> dict[str, Any] | None:
        if self.historical_training_service is None:
            return None
        result = await self.historical_training_service.capture_checkpoint_archives_once(
            due_only=True,
            source_kind="daemon_checkpoint_capture",
        )
        if result.get("captured_checkpoint_count", 0) <= 0:
            return None
        return result

    async def _maybe_run_historical_intelligence(self) -> dict[str, Any] | None:
        if self.historical_intelligence_service is None:
            return None
        last_run_at = await self._checkpoint_time(
            f"daemon_historical_intelligence:{self.settings.kalshi_env}:{self.settings.app_color}"
        )
        now = datetime.now(UTC)
        min_interval = timedelta(seconds=max(3600, self.settings.historical_intelligence_daily_run_seconds))
        if last_run_at is not None and now - last_run_at < min_interval:
            return None
        date_from = (now - timedelta(days=self.settings.historical_intelligence_window_days)).date().isoformat()
        date_to = now.date().isoformat()
        result = await self.historical_intelligence_service.run(
            HistoricalIntelligenceRunRequest(
                date_from=date_from,
                date_to=date_to,
                origins=["historical_replay"],
                auto_promote=self.settings.historical_intelligence_auto_promote,
            )
        )
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint(
                f"daemon_historical_intelligence:{self.settings.kalshi_env}:{self.settings.app_color}",
                None,
                {
                    "ran_at": now.isoformat(),
                    "result": result,
                },
            )
            await session.commit()
        return result

    async def _maybe_run_historical_pipeline(self) -> dict[str, Any] | None:
        if self.historical_pipeline_service is None:
            return None
        last_run_at = await self._checkpoint_time(
            f"daemon_historical_pipeline:{self.settings.kalshi_env}:{self.settings.app_color}"
        )
        now = datetime.now(UTC)
        min_interval = timedelta(seconds=max(3600, self.settings.historical_pipeline_daily_run_seconds))
        if last_run_at is not None and now - last_run_at < min_interval:
            return None
        result = await self.historical_pipeline_service.daily()
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint(
                f"daemon_historical_pipeline:{self.settings.kalshi_env}:{self.settings.app_color}",
                None,
                {
                    "ran_at": now.isoformat(),
                    "result": result,
                },
            )
            await session.commit()
        return result

    async def _maybe_run_strategy_regression(self) -> dict[str, Any] | None:
        if self.strategy_regression_service is None:
            return None
        last_run_at = await self._checkpoint_time("strategy_regression")
        now = self._utc_now()
        min_interval = timedelta(seconds=max(3600, self.settings.strategy_regression_daily_run_seconds))
        if last_run_at is not None and now - last_run_at < min_interval:
            return None
        try:
            return await self.strategy_regression_service.run_regression()
        except Exception:
            logger.warning("strategy_regression failed", exc_info=True)
            return None

    async def _maybe_run_strategy_codex_nightly(self) -> dict[str, Any] | None:
        if not self.settings.strategy_codex_nightly_enabled:
            return None
        if self.strategy_regression_service is None or self.strategy_codex_service is None or self.strategy_dashboard_service is None:
            return None

        night_state = self._strategy_codex_nightly_state()
        if not night_state["due"]:
            return None

        if self.settings.strategy_auto_evolve_enabled and self.strategy_auto_evolve_service is not None:
            return await self.strategy_auto_evolve_service.run_once(trigger_source="nightly")

        checkpoint_name = f"daemon_strategy_codex_nightly:{self.settings.kalshi_env}:{self.settings.app_color}"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            checkpoint = await repo.get_checkpoint(checkpoint_name)
            await session.commit()
        if checkpoint is not None and isinstance(checkpoint.payload, dict):
            if checkpoint.payload.get("local_date") == night_state["local_date"]:
                return None

        if not self.strategy_codex_service.is_available():
            payload = {
                "status": "skipped",
                "reason": "codex_unavailable",
                "local_date": night_state["local_date"],
                "timezone": self.settings.strategy_codex_nightly_timezone,
                "hour_local": self.settings.strategy_codex_nightly_hour_local,
                "run_ids": [],
                "regression_refreshed": False,
            }
            await self._set_nightly_codex_checkpoint(checkpoint_name, payload)
            await self._log_daemon_event(
                severity="warning",
                summary="Nightly strategy Codex skipped: Codex provider unavailable",
                payload=payload,
            )
            return payload

        regression_refreshed = False
        regression_result = None
        target_utc = night_state["target_local"].astimezone(UTC)
        regression_run_at = await self._checkpoint_time("strategy_regression")
        if regression_run_at is None or regression_run_at < target_utc:
            try:
                regression_result = await self.strategy_regression_service.run_regression()
            except Exception as exc:
                logger.warning("nightly strategy codex regression refresh failed", exc_info=True)
                payload = {
                    "status": "failed",
                    "reason": "regression_refresh_failed",
                    "error": str(exc),
                    "local_date": night_state["local_date"],
                    "timezone": self.settings.strategy_codex_nightly_timezone,
                    "hour_local": self.settings.strategy_codex_nightly_hour_local,
                    "run_ids": [],
                    "regression_refreshed": regression_refreshed,
                }
                await self._set_nightly_codex_checkpoint(checkpoint_name, payload)
                await self._log_daemon_event(
                    severity="warning",
                    summary="Nightly strategy Codex skipped: regression refresh failed",
                    payload=payload,
                )
                return payload
            regression_refreshed = True
            regression_run_at = await self._checkpoint_time("strategy_regression")

        if regression_run_at is None or regression_run_at < target_utc:
            payload = {
                "status": "skipped",
                "reason": "fresh_regression_unavailable",
                "local_date": night_state["local_date"],
                "timezone": self.settings.strategy_codex_nightly_timezone,
                "hour_local": self.settings.strategy_codex_nightly_hour_local,
                "run_ids": [],
                "regression_refreshed": regression_refreshed,
                "regression_result": regression_result,
            }
            await self._set_nightly_codex_checkpoint(checkpoint_name, payload)
            await self._log_daemon_event(
                severity="warning",
                summary="Nightly strategy Codex skipped: fresh 180d regression snapshot unavailable",
                payload=payload,
            )
            return payload

        try:
            dashboard_snapshot = await self.strategy_dashboard_service.build_dashboard(
                window_days=DEFAULT_STRATEGY_WINDOW_DAYS,
                include_codex_lab=False,
            )
            run_views = await self.strategy_codex_service.execute_modes_for_snapshot(
                modes=["evaluate", "suggest"],
                dashboard_snapshot=dashboard_snapshot,
                window_days=DEFAULT_STRATEGY_WINDOW_DAYS,
                trigger_source="nightly",
            )
        except Exception as exc:
            logger.warning("nightly strategy codex execution failed", exc_info=True)
            payload = {
                "status": "failed",
                "reason": "nightly_execution_failed",
                "error": str(exc),
                "local_date": night_state["local_date"],
                "timezone": self.settings.strategy_codex_nightly_timezone,
                "hour_local": self.settings.strategy_codex_nightly_hour_local,
                "run_ids": [],
                "regression_refreshed": regression_refreshed,
                "regression_result": regression_result,
            }
            await self._set_nightly_codex_checkpoint(checkpoint_name, payload)
            await self._log_daemon_event(
                severity="warning",
                summary="Nightly strategy Codex failed during execution",
                payload=payload,
            )
            return payload
        run_ids = [run_view["id"] for run_view in run_views if run_view.get("id")]
        run_statuses = [run_view.get("status") for run_view in run_views]
        status = "completed" if run_statuses and all(item == "completed" for item in run_statuses) else "completed_with_failures"
        payload = {
            "status": status,
            "local_date": night_state["local_date"],
            "timezone": self.settings.strategy_codex_nightly_timezone,
            "hour_local": self.settings.strategy_codex_nightly_hour_local,
            "run_ids": run_ids,
            "run_statuses": run_statuses,
            "regression_refreshed": regression_refreshed,
            "regression_result": regression_result,
        }
        await self._set_nightly_codex_checkpoint(checkpoint_name, payload)
        return payload

    async def _set_nightly_codex_checkpoint(self, stream_name: str, payload: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint(
                stream_name,
                None,
                {
                    **payload,
                    "ran_at": self._now_iso(),
                },
            )
            await session.commit()

    async def _log_daemon_event(self, *, severity: str, summary: str, payload: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.log_ops_event(
                severity=severity,
                summary=summary,
                source="daemon",
                payload=payload,
            )
            await session.commit()

    def _strategy_codex_nightly_state(self) -> dict[str, Any]:
        now = self._utc_now()
        timezone = ZoneInfo(self.settings.strategy_codex_nightly_timezone)
        local_now = now.astimezone(timezone)
        target_local = local_now.replace(
            hour=self.settings.strategy_codex_nightly_hour_local,
            minute=0,
            second=0,
            microsecond=0,
        )
        return {
            "due": local_now >= target_local,
            "local_date": local_now.date().isoformat(),
            "local_now": local_now,
            "target_local": target_local,
        }

    async def _checkpoint_time(self, stream_name: str) -> datetime | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            checkpoint = await repo.get_checkpoint(stream_name)
            await session.commit()
        if checkpoint is None or not isinstance(checkpoint.payload, dict):
            return None
        timestamp = (
            checkpoint.payload.get("reconciled_at")
            or checkpoint.payload.get("followed_at")
            or checkpoint.payload.get("ran_at")
        )
        if not isinstance(timestamp, str) or not timestamp:
            return None
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    def _utc_now(self) -> datetime:
        return datetime.now(UTC)
