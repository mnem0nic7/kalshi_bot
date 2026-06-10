from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
import time
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import HistoricalIntelligenceRunRequest, ShadowCampaignRequest
from kalshi_bot.crypto.services import (
    CryptoAutonomyService,
    CryptoForecastService,
    CryptoHistoryService,
    CryptoReplayService,
    CryptoSpotService,
    CryptoTrainingBackfillService,
    crypto_autonomy_15m_assets,
    crypto_entry_override_key,
    crypto_pnl_sizing_target_pct,
    enabled_crypto_frequencies,
)
from kalshi_bot.integrations.crypto_spot import COINBASE_PRODUCT_IDS
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.services.autonomous_gate_tuning import AutonomousGateTuningService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.historical_training import HistoricalTrainingService
from kalshi_bot.services.historical_intelligence import HistoricalIntelligenceService
from kalshi_bot.services.historical_pipeline import HistoricalPipelineService
from kalshi_bot.services.decision_corpus import DecisionCorpusService
from kalshi_bot.services.market_history import MarketHistoryService
from kalshi_bot.services.reconcile import ReconciliationService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.signal_attention import SignalAttentionService
from kalshi_bot.services.shadow_campaign import ShadowCampaignService
from kalshi_bot.services.self_improve import SelfImproveService
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.services.strategy_eval import StrategyEvaluationService
from kalshi_bot.services.strategy_auto_evolve import StrategyAutoEvolveService
from kalshi_bot.services.strategy_codex import StrategyCodexService
from kalshi_bot.services.strategy_dashboard import StrategyDashboardService
from kalshi_bot.services.crypto_take_profit import CryptoTakeProfitService
from kalshi_bot.services.stop_loss import StopLossService
from kalshi_bot.services.strategy_cleanup_service import StrategyCleanupService
from kalshi_bot.services.momentum_calibration import MomentumCalibrationService
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
        autonomous_gate_tuning_service: AutonomousGateTuningService | None = None,
        strategy_regression_service: StrategyRegressionService | None = None,
        stop_loss_service: StopLossService | None = None,
        crypto_take_profit_service: CryptoTakeProfitService | None = None,
        strategy_cleanup_service: StrategyCleanupService | None = None,
        monotonicity_arb_service: MonotonicityArbScannerService | None = None,
        strategy_codex_service: StrategyCodexService | None = None,
        strategy_dashboard_service: StrategyDashboardService | None = None,
        strategy_auto_evolve_service: StrategyAutoEvolveService | None = None,
        momentum_calibration_service: MomentumCalibrationService | None = None,
        decision_corpus_service: DecisionCorpusService | None = None,
        crypto_history_service: CryptoHistoryService | None = None,
        crypto_spot_service: CryptoSpotService | None = None,
        crypto_training_backfill_service: CryptoTrainingBackfillService | None = None,
        crypto_autonomy_service: CryptoAutonomyService | None = None,
        crypto_forecast_service: CryptoForecastService | None = None,
        crypto_replay_service: CryptoReplayService | None = None,
        crypto_non_model_touch20_service: Any | None = None,
        weather_live_service: Any | None = None,
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
        self.autonomous_gate_tuning_service = autonomous_gate_tuning_service
        self.strategy_regression_service = strategy_regression_service
        self.strategy_cleanup_service = strategy_cleanup_service
        self.monotonicity_arb_service = monotonicity_arb_service
        self.strategy_codex_service = strategy_codex_service
        self.strategy_dashboard_service = strategy_dashboard_service
        self.strategy_auto_evolve_service = strategy_auto_evolve_service
        self.momentum_calibration_service = momentum_calibration_service
        self.decision_corpus_service = decision_corpus_service
        self.crypto_history_service = crypto_history_service
        self.crypto_spot_service = crypto_spot_service
        self.crypto_training_backfill_service = crypto_training_backfill_service
        self.crypto_autonomy_service = crypto_autonomy_service
        self.crypto_forecast_service = crypto_forecast_service
        self.crypto_replay_service = crypto_replay_service
        self.crypto_non_model_touch20_service = crypto_non_model_touch20_service
        self.weather_live_service = weather_live_service
        self.stop_loss_service = stop_loss_service
        self.crypto_take_profit_service = crypto_take_profit_service
        self._auto_trigger_enabled_for_run = settings.trigger_enable_auto_rooms
        self._heartbeat_follow_up_task: asyncio.Task[None] | None = None
        self._crypto_model_nightly_lock = asyncio.Lock()
        self._active_color_cache: tuple[float, bool] | None = None
        self._last_market_update_dispatched_at: dict[str, float] = {}
        self._threaded_liveness_stop: threading.Event | None = None
        self._threaded_liveness_thread: threading.Thread | None = None
        self._heartbeat_role = "daemon"

    @staticmethod
    def _normalize_heartbeat_role(role: str | None) -> str:
        normalized = "".join(ch if ch.isalnum() else "_" for ch in str(role or "daemon").strip().lower())
        normalized = "_".join(part for part in normalized.split("_") if part)
        return normalized or "daemon"

    def _checkpoint_name(self, prefix: str) -> str:
        base = f"{prefix}:{self.settings.kalshi_env}:{self.settings.app_color}"
        return base if self._heartbeat_role == "daemon" else f"{base}:{self._heartbeat_role}"

    def _reconcile_checkpoint_name(self) -> str:
        return self._checkpoint_name("daemon_reconcile")

    def _heartbeat_checkpoint_name(self) -> str:
        return self._checkpoint_name("daemon_heartbeat")

    async def _recover_orphaned_rooms(self) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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

    async def reconcile_once(self, *, run_settlement_gate_tuning: bool = True) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            summary = await self.reconciliation_service.reconcile(
                repo,
                subaccount=self.settings.kalshi_subaccount,
                kalshi_env=self.settings.kalshi_env,
            )
            await repo.set_checkpoint(
                self._reconcile_checkpoint_name(),
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
        if summary.settlements_count > 0:
            if self.settings.autonomous_gate_tuning_enabled and self.autonomous_gate_tuning_service is not None:
                if not run_settlement_gate_tuning:
                    result["autonomous_gate_tuning"] = {"status": "deferred", "reason": "daemon_liveness"}
                elif await self._is_active_color():
                    result["autonomous_gate_tuning"] = await self.autonomous_gate_tuning_service.run(
                        kalshi_env=self.settings.kalshi_env,
                        source=self.settings.autonomous_gate_tuning_source,
                        days=self.settings.autonomous_gate_tuning_days,
                        min_support=self.settings.autonomous_gate_tuning_min_support,
                        triggered_by="settlement_reconcile",
                        domain="all",
                    )
                else:
                    result["autonomous_gate_tuning"] = {"status": "skipped", "reason": "inactive_color"}
            else:
                result["autonomous_gate_tuning"] = {"status": "disabled"}
            result["crypto_edge_shrinkage"] = await self._refresh_crypto_edge_shrinkage()
        return result

    async def _refresh_crypto_edge_shrinkage(self) -> dict[str, Any]:
        """Refit edge shrinkage after settlements update fill results."""
        if not self.settings.crypto_edge_shrinkage_enabled:
            return {"status": "disabled"}
        if self.crypto_forecast_service is None:
            return {"status": "skipped", "reason": "crypto_forecast_service_unavailable"}
        if not await self._is_active_color():
            return {"status": "skipped", "reason": "inactive_color"}
        results: dict[str, Any] = {"status": "ok", "frequencies": {}}
        for frequency in enabled_crypto_frequencies(self.settings):
            try:
                results["frequencies"][frequency] = await self.crypto_forecast_service.refresh_edge_shrinkage(
                    frequency=frequency,
                )
            except Exception as exc:  # pragma: no cover - defensive; shrinkage must not break reconcile
                logger.warning("crypto edge shrinkage refresh failed for %s: %s", frequency, exc)
                results["frequencies"][frequency] = {"status": "error", "error": str(exc)}
        return results

    async def heartbeat_once(self, *, run_follow_up: bool = True) -> dict[str, Any]:
        payload = {
            "app_color": self.settings.app_color,
            "kalshi_env": self.settings.kalshi_env,
            "shadow_mode": self.settings.app_shadow_mode,
            "auto_rooms_enabled": self.settings.trigger_enable_auto_rooms,
            "crypto_autonomy_enabled": self.settings.crypto_autonomy_enabled,
            "crypto_history_auto_enabled": self.settings.crypto_history_auto_enabled,
            "crypto_quote_evidence_enabled": self.settings.crypto_quote_evidence_enabled,
            "crypto_auto_frequencies": enabled_crypto_frequencies(self.settings),
            "crypto_spot_current_auto_enabled": self.settings.crypto_spot_current_auto_enabled,
            "crypto_spot_current_interval_seconds": self.settings.crypto_spot_current_interval_seconds,
            "crypto_spot_history_auto_enabled": self.settings.crypto_spot_history_auto_enabled,
            "heartbeat_at": self._now_iso(),
            "daemon_role": self._heartbeat_role,
        }
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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
            last_reconcile = await repo.get_checkpoint(self._reconcile_checkpoint_name())
            await repo.set_checkpoint(
                self._heartbeat_checkpoint_name(),
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

    async def heartbeat_liveness_tick(
        self,
        *,
        reason: str = "liveness",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Refresh the daemon heartbeat checkpoint without expensive follow-up work."""
        payload: dict[str, Any] = {
            "app_color": self.settings.app_color,
            "kalshi_env": self.settings.kalshi_env,
            "heartbeat_at": self._now_iso(),
            "lightweight": True,
            "reason": reason,
            "daemon_role": self._heartbeat_role,
        }
        if details is not None:
            payload["details"] = details
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            last_reconcile = await repo.get_checkpoint(self._reconcile_checkpoint_name())
            payload.update(
                {
                    "active_color": control.active_color,
                    "kill_switch_enabled": control.kill_switch_enabled,
                    "last_reconcile_at": (
                        last_reconcile.payload.get("reconciled_at")
                        if last_reconcile is not None and isinstance(last_reconcile.payload, dict)
                        else None
                    ),
                }
            )
            await repo.set_checkpoint(
                self._heartbeat_checkpoint_name(),
                None,
                payload,
            )
            await session.commit()
        return payload

    async def run(
        self,
        *,
        markets: list[str] | None = None,
        public_only: bool = False,
        auto_trigger: bool | None = None,
        max_messages: int | None = None,
        run_seconds: float | None = None,
        crypto_only: bool = False,
        heartbeat_role: str | None = None,
    ) -> dict[str, Any]:
        previous_heartbeat_role = self._heartbeat_role
        self._heartbeat_role = self._normalize_heartbeat_role(heartbeat_role or ("crypto" if crypto_only else "daemon"))
        if crypto_only:
            try:
                return await self._run_crypto_only(run_seconds=run_seconds)
            finally:
                self._heartbeat_role = previous_heartbeat_role

        should_auto_trigger = self.settings.trigger_enable_auto_rooms if auto_trigger is None else auto_trigger
        self._auto_trigger_enabled_for_run = should_auto_trigger

        tasks: dict[str, asyncio.Task] = {}
        try:
            await self.heartbeat_once(run_follow_up=False)
            if max_messages is None and run_seconds is None:
                self._start_threaded_liveness_heartbeat()
            tasks["heartbeat"] = asyncio.create_task(self._periodic_heartbeat_loop())

            startup_delay = self._startup_delay_seconds()
            if startup_delay > 0:
                logger.info(
                    "Daemon startup warmup delaying heavy work for %.1f seconds env=%s color=%s",
                    startup_delay,
                    self.settings.kalshi_env,
                    self.settings.app_color,
                )
                await asyncio.sleep(startup_delay)

            await self.self_improve_service.apply_pending_pack_promotion(app_color=self.settings.app_color)
            await self._recover_orphaned_rooms()
            if self.settings.daemon_start_with_reconcile:
                await self.reconcile_once(run_settlement_gate_tuning=False)
            await self.heartbeat_once(run_follow_up=False)
            selected_markets = await self._select_stream_markets(markets)

            stream_coro = (
                self.stream_service.stream(
                    market_tickers=selected_markets,
                    include_private=not public_only,
                    max_messages=max_messages,
                    on_market_update=self._handle_market_update,
                )
                if max_messages is not None
                else self._stream_forever(
                    market_tickers=selected_markets,
                    include_private=not public_only,
                    on_market_update=self._handle_market_update,
                )
            )
            periodic_tasks = {
                "stream": asyncio.create_task(stream_coro),
                "reconcile": asyncio.create_task(self._periodic_reconcile_loop()),
                "market_history": asyncio.create_task(self._periodic_market_history_loop()),
                "strategy_c": asyncio.create_task(self._periodic_strategy_c_loop()),
                "monotonicity_arb": asyncio.create_task(self._periodic_monotonicity_arb_loop()),
                "crypto_quote_evidence": asyncio.create_task(self._periodic_crypto_quote_evidence_loop()),
                "crypto_history": asyncio.create_task(self._periodic_crypto_history_loop()),
                "crypto_spot_current": asyncio.create_task(self._periodic_crypto_spot_current_loop()),
                "crypto_spot_history": asyncio.create_task(self._periodic_crypto_spot_history_loop()),
                "crypto_funding_rates": asyncio.create_task(self._periodic_crypto_funding_rate_loop()),
                "crypto_autonomy_1h": asyncio.create_task(self._periodic_crypto_autonomy_loop()),
            }
            if "15m" in enabled_crypto_frequencies(self.settings):
                for _asset in crypto_autonomy_15m_assets(self.settings):
                    periodic_tasks[f"crypto_autonomy_15m_{_asset}"] = asyncio.create_task(
                        self._periodic_crypto_autonomy_asset_loop(_asset)
                    )
            if self.settings.crypto_model_nightly_auto_enabled:
                periodic_tasks["crypto_model_nightly"] = asyncio.create_task(
                    self._periodic_crypto_model_nightly_loop()
                )
            if self.settings.weather_research_refresh_interval_seconds > 0:
                periodic_tasks["weather_research_refresh"] = asyncio.create_task(
                    self._periodic_weather_research_refresh_loop()
                )
            if self.settings.stop_loss_enabled:
                periodic_tasks["stop_loss"] = asyncio.create_task(self._periodic_stop_loss_loop())
            if self.settings.crypto_take_profit_enabled:
                periodic_tasks["crypto_take_profit"] = asyncio.create_task(self._periodic_crypto_take_profit_loop())
            if self.settings.crypto_winrate_guard_enabled:
                periodic_tasks["crypto_winrate_guard"] = asyncio.create_task(
                    self._periodic_crypto_winrate_guard_loop()
                )
            tasks.update(periodic_tasks)
            if run_seconds is not None:
                tasks["timer"] = asyncio.create_task(asyncio.sleep(run_seconds))

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
            self._stop_threaded_liveness_heartbeat()
            self._heartbeat_role = previous_heartbeat_role

    async def _run_crypto_only(self, *, run_seconds: float | None = None) -> dict[str, Any]:
        tasks: dict[str, asyncio.Task] = {}
        try:
            await self.heartbeat_once(run_follow_up=False)
            if run_seconds is None:
                self._start_threaded_liveness_heartbeat()
            tasks["heartbeat"] = asyncio.create_task(self._periodic_heartbeat_loop())

            startup_delay = self._startup_delay_seconds()
            if startup_delay > 0:
                logger.info(
                    "Crypto-only daemon startup warmup delaying work for %.1f seconds env=%s color=%s role=%s",
                    startup_delay,
                    self.settings.kalshi_env,
                    self.settings.app_color,
                    self._heartbeat_role,
                )
                await asyncio.sleep(startup_delay)

            await self.heartbeat_once(run_follow_up=False)
            _restart_tasks: dict[str, asyncio.Task[Any]] = {
                "crypto_quote_evidence": asyncio.create_task(self._periodic_crypto_quote_evidence_loop()),
                "crypto_history": asyncio.create_task(self._periodic_crypto_history_loop()),
                "crypto_spot_current": asyncio.create_task(self._periodic_crypto_spot_current_loop()),
                "crypto_spot_history": asyncio.create_task(self._periodic_crypto_spot_history_loop()),
                "crypto_funding_rates": asyncio.create_task(self._periodic_crypto_funding_rate_loop()),
                "crypto_autonomy_1h": asyncio.create_task(self._periodic_crypto_autonomy_loop()),
            }
            if "15m" in enabled_crypto_frequencies(self.settings):
                for _asset in crypto_autonomy_15m_assets(self.settings):
                    _restart_tasks[f"crypto_autonomy_15m_{_asset}"] = asyncio.create_task(
                        self._periodic_crypto_autonomy_asset_loop(_asset)
                    )
            tasks.update(_restart_tasks)
            if self.settings.crypto_winrate_guard_enabled:
                tasks["crypto_winrate_guard"] = asyncio.create_task(
                    self._periodic_crypto_winrate_guard_loop()
                )
            if run_seconds is not None:
                tasks["timer"] = asyncio.create_task(asyncio.sleep(run_seconds))

            done, pending = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
            completed_name = next(name for name, task in tasks.items() if task in done)
            done_task = tasks[completed_name]
            exc = None if done_task.cancelled() else done_task.exception()
            if exc is not None:
                raise exc

            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            return {
                "completed": completed_name,
                "mode": "crypto_only",
                "daemon_role": self._heartbeat_role,
                "crypto_auto_frequencies": enabled_crypto_frequencies(self.settings),
            }
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            self._stop_threaded_liveness_heartbeat()

    def _start_threaded_liveness_heartbeat(self) -> None:
        if self._threaded_liveness_thread is not None and self._threaded_liveness_thread.is_alive():
            return
        if self.settings.daemon_heartbeat_interval_seconds <= 0:
            return
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._threaded_liveness_heartbeat_loop,
            args=(stop_event,),
            name=f"daemon-liveness-{self.settings.kalshi_env}-{self.settings.app_color}-{self._heartbeat_role}",
            daemon=True,
        )
        self._threaded_liveness_stop = stop_event
        self._threaded_liveness_thread = thread
        thread.start()

    def _stop_threaded_liveness_heartbeat(self) -> None:
        stop_event = self._threaded_liveness_stop
        thread = self._threaded_liveness_thread
        self._threaded_liveness_stop = None
        self._threaded_liveness_thread = None
        if stop_event is None or thread is None:
            return
        stop_event.set()
        thread.join(timeout=5)

    def _threaded_liveness_heartbeat_loop(self, stop_event: threading.Event) -> None:
        interval = max(1.0, float(self.settings.daemon_heartbeat_interval_seconds))
        while not stop_event.wait(interval):
            try:
                asyncio.run(
                    self._write_threaded_liveness_checkpoint(
                        reason="threaded_liveness",
                        details={"interval_seconds": interval},
                    )
                )
            except Exception:
                logger.warning("threaded daemon liveness heartbeat failed", exc_info=True)

    async def _write_threaded_liveness_checkpoint(
        self,
        *,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "app_color": self.settings.app_color,
            "kalshi_env": self.settings.kalshi_env,
            "heartbeat_at": self._now_iso(),
            "lightweight": True,
            "threaded": True,
            "reason": reason,
            "daemon_role": self._heartbeat_role,
        }
        if details is not None:
            payload["details"] = details

        engine = create_async_engine(self.settings.database_url, pool_pre_ping=True)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            async with session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
                last_reconcile = await repo.get_checkpoint(self._reconcile_checkpoint_name())
                payload.update(
                    {
                        "active_color": control.active_color,
                        "kill_switch_enabled": control.kill_switch_enabled,
                        "last_reconcile_at": (
                            last_reconcile.payload.get("reconciled_at")
                            if last_reconcile is not None and isinstance(last_reconcile.payload, dict)
                            else None
                        ),
                    }
                )
                await repo.set_checkpoint(
                    self._heartbeat_checkpoint_name(),
                    None,
                    payload,
                )
                await session.commit()
        finally:
            await engine.dispose()
        return payload

    async def _stream_forever(
        self,
        *,
        market_tickers: list[str],
        include_private: bool,
        on_market_update: Any,
    ) -> None:
        restart_count = 0
        while True:
            processed_messages: Any = None
            try:
                processed_messages = await self.stream_service.stream(
                    market_tickers=market_tickers,
                    include_private=include_private,
                    max_messages=None,
                    on_market_update=on_market_update,
                )
                logger.warning(
                    "Market stream ended unexpectedly; restarting env=%s color=%s processed_messages=%s",
                    self.settings.kalshi_env,
                    self.settings.app_color,
                    processed_messages,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("market stream crashed; restarting", exc_info=True)
            restart_count += 1
            try:
                await self.heartbeat_liveness_tick(
                    reason="market_stream_restart",
                    details={
                        "restart_count": restart_count,
                        "processed_messages": processed_messages,
                    },
                )
            except Exception:
                logger.warning("stream restart heartbeat tick failed", exc_info=True)
            await asyncio.sleep(5)

    async def _select_stream_markets(self, markets: list[str] | None) -> list[str]:
        if markets is not None:
            selected_markets = list(dict.fromkeys(markets))
        elif self._weather_market_updates_enabled():
            selected_markets = list(dict.fromkeys(await self.discovery_service.list_stream_markets()))
        else:
            selected_markets = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            positions = await repo.list_positions(
                limit=5000,
                kalshi_env=self.settings.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
            )
            await session.commit()

        for position in positions:
            if position.market_ticker not in selected_markets:
                selected_markets.append(position.market_ticker)
        return selected_markets

    async def _handle_market_update(self, market_ticker: str) -> None:
        if not self._market_update_dispatch_due(market_ticker):
            return
        if not await self._is_active_color():
            return
        if self._is_weather_market(market_ticker) and not self._weather_market_updates_enabled():
            return
        await self.research_coordinator.handle_market_update(market_ticker)
        if self._auto_trigger_enabled_for_run:
            await self.auto_trigger_service.handle_market_update(market_ticker)

    @staticmethod
    def _is_weather_market(market_ticker: str) -> bool:
        return str(market_ticker or "").upper().startswith("KXHIGH")

    def _weather_market_updates_enabled(self) -> bool:
        return bool(
            getattr(self, "_auto_trigger_enabled_for_run", False)
            or self.settings.weather_prediction_enabled
            or self.settings.weather_residual_model_enabled
            or self.settings.weather_intraday_model_enabled
            or self.settings.weather_research_refresh_interval_seconds > 0
            or self.settings.weather_rejected_opportunity_scorer_enabled
        )

    async def _is_active_color(self) -> bool:
        cache_ttl = max(0.0, float(self.settings.daemon_active_color_cache_seconds))
        now = time.monotonic()
        if cache_ttl > 0.0 and self._active_color_cache is not None:
            expires_at, cached_result = self._active_color_cache
            if now < expires_at:
                return cached_result

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            await session.commit()
        is_active = control.active_color == self.settings.app_color
        if cache_ttl > 0.0:
            self._active_color_cache = (now + cache_ttl, is_active)
        return is_active

    def _market_update_dispatch_due(self, market_ticker: str) -> bool:
        interval = max(0.0, float(self.settings.daemon_market_update_throttle_seconds))
        if interval <= 0.0:
            return True
        now = time.monotonic()
        last_dispatched_at = self._last_market_update_dispatched_at.get(market_ticker)
        if last_dispatched_at is not None and now - last_dispatched_at < interval:
            return False
        self._last_market_update_dispatched_at[market_ticker] = now
        return True

    async def _periodic_reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.daemon_reconcile_interval_seconds)
            await self.reconcile_once(run_settlement_gate_tuning=False)

    async def _periodic_heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.daemon_heartbeat_interval_seconds)
            payload = await self.heartbeat_once(run_follow_up=False)
            self._schedule_heartbeat_follow_up(payload)

    @staticmethod
    def _position_exit_loop_interval(
        *,
        enabled: bool,
        service: Any,
        idle_seconds: float,
        hot_seconds: float,
    ) -> float:
        """Idle interval while flat; hot interval (floored at 0.2s) while any
        position is open. 30s is an eternity on a 15-minute binary."""
        if not enabled or service is None or int(getattr(service, "last_position_count", 0) or 0) <= 0:
            return max(0.2, float(idle_seconds))
        return max(0.2, min(float(idle_seconds), float(hot_seconds)))

    async def _periodic_stop_loss_loop(self) -> None:
        while True:
            await asyncio.sleep(
                self._position_exit_loop_interval(
                    enabled=self.settings.stop_loss_enabled,
                    service=self.stop_loss_service,
                    idle_seconds=self.settings.stop_loss_check_interval_seconds,
                    hot_seconds=self.settings.position_exit_hot_loop_interval_seconds,
                )
            )
            if not self.settings.stop_loss_enabled:
                continue
            if self.stop_loss_service is None:
                continue
            try:
                await self.stop_loss_service.check_once()
            except Exception:
                logger.warning("stop_loss check failed", exc_info=True)

    async def _periodic_crypto_take_profit_loop(self) -> None:
        while True:
            await asyncio.sleep(
                self._position_exit_loop_interval(
                    enabled=self.settings.crypto_take_profit_enabled,
                    service=self.crypto_take_profit_service,
                    idle_seconds=self.settings.crypto_take_profit_check_interval_seconds,
                    hot_seconds=self.settings.position_exit_hot_loop_interval_seconds,
                )
            )
            if not self.settings.crypto_take_profit_enabled:
                continue
            if self.crypto_take_profit_service is None:
                continue
            try:
                await self.crypto_take_profit_service.check_once()
            except Exception:
                logger.warning("crypto_take_profit check failed", exc_info=True)

    async def _periodic_crypto_winrate_guard_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.crypto_winrate_guard_check_interval_seconds)
            if not self.settings.crypto_winrate_guard_enabled:
                continue
            service = self.crypto_autonomy_service
            if service is None:
                continue
            asset_control = getattr(service, "asset_control_service", None)
            if asset_control is None:
                continue
            if not await self._is_active_color():
                continue
            try:
                for frequency in enabled_crypto_frequencies(self.settings):
                    await asset_control.evaluate_winrate_guard(frequency=frequency)
            except Exception:
                logger.warning("crypto_winrate_guard check failed", exc_info=True)

    async def _periodic_market_history_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.daemon_market_history_interval_seconds)
            if self.market_history_service is None:
                continue
            if not await self._is_active_color():
                continue
            if not self._weather_market_updates_enabled():
                continue
            try:
                await self.market_history_service.snapshot_once()
                await self.market_history_service.purge_once()
                if self._auto_trigger_enabled_for_run:
                    await self.auto_trigger_service.recheck_marketability_waitlist_once()
            except Exception:
                logger.warning("market_history loop error", exc_info=True)

    async def _periodic_weather_research_refresh_loop(self) -> None:
        interval = max(0, int(self.settings.weather_research_refresh_interval_seconds))
        if interval <= 0:
            return
        while True:
            if await self._is_active_color():
                try:
                    market_tickers = await self.discovery_service.list_stream_markets()

                    async def refresh_heartbeat(progress: dict[str, Any]) -> None:
                        try:
                            await self.heartbeat_liveness_tick(
                                reason="weather_research_refresh",
                                details=progress,
                            )
                        except Exception:
                            logger.warning("weather refresh heartbeat tick failed", exc_info=True)

                    await refresh_heartbeat(
                        {
                            "phase": "before_sweep",
                            "market_count": len(market_tickers),
                        }
                    )
                    result = await self.research_coordinator.refresh_live_weather_dossiers(
                        market_tickers,
                        dry_run=False,
                        concurrency=self.settings.weather_research_refresh_concurrency,
                        batch_size=self.settings.weather_research_refresh_concurrency,
                        progress_callback=refresh_heartbeat,
                        refresh_margin_seconds=self.settings.weather_research_refresh_margin_seconds,
                        trigger_reason="daemon_live_weather_refresh",
                    )
                    await refresh_heartbeat(
                        {
                            "phase": "after_sweep",
                            "market_count": len(market_tickers),
                            "considered": result.get("considered"),
                            "selected": result.get("selected"),
                            "refreshed": result.get("refreshed"),
                            "failed": result.get("failed"),
                        }
                    )
                    logger.info(
                        "Live weather research refresh sweep considered=%s selected=%s refreshed=%s failed=%s",
                        result.get("considered"),
                        result.get("selected"),
                        result.get("refreshed"),
                        result.get("failed"),
                    )
                    scorer = await self._maybe_run_rejected_weather_scorer_daily()
                    if scorer is not None:
                        logger.info(
                            "Rejected weather opportunity scorer status=%s settled=%s unlock=%s",
                            scorer.get("status"),
                            scorer.get("settled_count"),
                            ((scorer.get("unlock") or {}).get("passed") if isinstance(scorer.get("unlock"), dict) else None),
                        )
                except Exception:
                    logger.warning("weather research refresh loop error", exc_info=True)
            await asyncio.sleep(interval)

    async def _maybe_run_rejected_weather_scorer_daily(self) -> dict[str, Any] | None:
        if not bool(getattr(self.settings, "weather_rejected_opportunity_scorer_enabled", True)):
            return None
        if self.weather_live_service is None:
            return {"status": "skipped", "reason": "weather_live_service_missing"}
        today = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")
        checkpoint_name = f"daemon_rejected_weather_scorer:{self.settings.kalshi_env}:{self.settings.app_color}:{today}"
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            checkpoint = await repo.get_checkpoint(checkpoint_name)
            if checkpoint is not None:
                await session.commit()
                return None
            service = SignalAttentionService(self.settings)
            report = await service.score_rejected_weather_opportunities(
                session,
                kalshi_env=self.settings.kalshi_env,
                lookback_hours=self.settings.weather_rejected_opportunity_lookback_hours,
                dedupe="first-qualifying",
                persist_bootstrap_evidence=True,
                dry_run=False,
            )
            await session.commit()
            activation = None
            if (
                bool(getattr(self.settings, "weather_rejected_opportunity_auto_enable", True))
                and bool((report.get("unlock") or {}).get("passed"))
            ):
                activation = await self.weather_live_service.auto_enable_close_strike_probes(
                    kalshi_env=self.settings.kalshi_env,
                    actor=f"daemon:{self.settings.app_color}",
                    evidence_report=report,
                    dry_run=False,
                )
            payload = {
                "ran_at": datetime.now(UTC).isoformat(),
                "status": "ok",
                "candidate_count": report.get("candidate_count"),
                "settled_count": report.get("settled_count"),
                "unlock": report.get("unlock"),
                "activation": activation,
            }
            await repo.set_checkpoint(checkpoint_name, cursor=None, payload=payload)
            await session.commit()
            return payload

    async def _periodic_strategy_c_loop(self) -> None:
        interval = self.settings.strategy_c_cadence_idle_seconds
        while True:
            await asyncio.sleep(interval)
            if self.strategy_cleanup_service is None:
                continue
            if not await self._is_active_color():
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
            if not await self._is_active_color():
                continue
            try:
                await self.monotonicity_arb_service.sweep()
            except Exception:
                logger.warning("monotonicity_arb sweep error", exc_info=True)

    async def _periodic_crypto_autonomy_loop(self) -> None:
        """1h autonomy loop — runs all 1h assets together in a single pass."""
        idle_interval = max(1, int(self.settings.crypto_autonomy_idle_interval_seconds))
        while True:
            if self.crypto_autonomy_service is None or "1h" not in enabled_crypto_frequencies(self.settings):
                await asyncio.sleep(idle_interval)
                continue
            if not await self._is_active_color():
                await asyncio.sleep(idle_interval)
                continue
            try:
                await self.crypto_autonomy_service.run_once(frequency="1h")
            except Exception:
                logger.warning("crypto autonomy 1h loop error", exc_info=True)
            await asyncio.sleep(0)

    async def _periodic_crypto_autonomy_asset_loop(self, asset_symbol: str) -> None:
        """Per-asset 15m autonomy loop — each asset runs independently so no asset blocks another."""
        idle_interval = max(1, int(self.settings.crypto_autonomy_idle_interval_seconds))
        while True:
            if self.crypto_autonomy_service is None:
                await asyncio.sleep(idle_interval)
                continue
            if not await self._is_active_color():
                await asyncio.sleep(idle_interval)
                continue
            try:
                await self.crypto_autonomy_service.run_once(frequency="15m", asset_symbols=[asset_symbol])
            except Exception:
                logger.warning("crypto autonomy 15m loop error asset=%s", asset_symbol, exc_info=True)
            await asyncio.sleep(0)

    async def _periodic_crypto_history_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.crypto_history_auto_interval_seconds)
            try:
                await self._run_crypto_history_cycle()
            except Exception:
                logger.warning("crypto history loop error", exc_info=True)

    async def _run_crypto_history_cycle(self) -> None:
        if self.crypto_history_service is None or not self.settings.crypto_history_auto_enabled:
            return
        if not await self._is_active_color():
            return
        for frequency in enabled_crypto_frequencies(self.settings):
            await self.crypto_history_service.collect_settled(
                days=self.settings.crypto_history_auto_lookback_days,
                frequency=frequency,
            )
            await self.crypto_history_service.bootstrap(
                days=self.settings.crypto_history_auto_lookback_days,
                frequency=frequency,
            )

    async def _periodic_crypto_quote_evidence_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.crypto_quote_evidence_interval_seconds)
            if self.crypto_history_service is None or not self.settings.crypto_quote_evidence_enabled:
                continue
            if not await self._is_active_color():
                continue
            try:
                for frequency in enabled_crypto_frequencies(self.settings):
                    await self.crypto_history_service.collect_open(frequency=frequency)
            except Exception:
                logger.warning("crypto quote evidence loop error", exc_info=True)

    async def _periodic_crypto_spot_current_loop(self) -> None:
        interval = max(1, int(self.settings.crypto_spot_current_interval_seconds))
        while True:
            if self.crypto_spot_service is not None and self.settings.crypto_spot_current_auto_enabled:
                if await self._is_active_color():
                    try:
                        configured_assets = sorted(COINBASE_PRODUCT_IDS)
                        for frequency in enabled_crypto_frequencies(self.settings):
                            await self.crypto_spot_service.collect_current(
                                frequency=frequency, asset_symbols=configured_assets
                            )
                    except Exception:
                        logger.warning("crypto current spot loop error", exc_info=True)
            await asyncio.sleep(interval)

    async def _periodic_crypto_spot_history_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.crypto_history_auto_interval_seconds)
            if self.crypto_spot_service is None or not self.settings.crypto_spot_history_auto_enabled:
                continue
            if not await self._is_active_color():
                continue
            try:
                for frequency in enabled_crypto_frequencies(self.settings):
                    await self.crypto_spot_service.backfill(
                        days=self.settings.crypto_spot_history_auto_lookback_days,
                        frequency=frequency,
                    )
            except Exception:
                logger.warning("crypto spot history loop error", exc_info=True)

    # Ops-event escalation cadence for the funding loop: warn-log every failure,
    # but only write an ops event on every Nth consecutive failure so transient
    # OKX hiccups (one missed 30-minute poll is harmless) don't spam the feed.
    _CRYPTO_FUNDING_FAILURE_OPS_EVENT_EVERY = 3

    @staticmethod
    def _crypto_funding_failure_should_log_ops_event(consecutive_failures: int) -> bool:
        return (
            consecutive_failures > 0
            and consecutive_failures % DaemonService._CRYPTO_FUNDING_FAILURE_OPS_EVENT_EVERY == 0
        )

    async def _run_crypto_funding_rate_cycle(self) -> dict[str, Any] | None:
        """One funding-rate collection pass; returns the collect summary, or None when skipped."""
        if self.crypto_spot_service is None or not self.settings.crypto_funding_collect_enabled:
            return None
        if not await self._is_active_color():
            return None
        assets = [a.strip().upper() for a in self.settings.crypto_model_nightly_assets.split(",") if a.strip()]
        result = await self.crypto_spot_service.collect_funding_rates(asset_symbols=assets or None)
        errors = result.get("errors") or []
        if errors and not result.get("stored"):
            # collect_funding_rates swallows per-asset errors; an all-asset wipeout
            # should count as a loop failure so repeated outages reach ops events.
            raise RuntimeError(f"crypto funding rate collection failed for all assets: {errors}")
        return result

    async def _periodic_crypto_funding_rate_loop(self) -> None:
        interval = max(60, int(self.settings.crypto_funding_collect_interval_seconds))
        consecutive_failures = 0
        while True:
            await asyncio.sleep(interval)
            try:
                result = await self._run_crypto_funding_rate_cycle()
            except Exception as exc:
                consecutive_failures += 1
                logger.warning("crypto funding rate loop error", exc_info=True)
                if self._crypto_funding_failure_should_log_ops_event(consecutive_failures):
                    try:
                        async with self.session_factory() as session:
                            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                            await repo.log_ops_event(
                                severity="warning",
                                summary="Crypto funding rate collection failing repeatedly",
                                source="daemon",
                                payload={
                                    "app_color": self.settings.app_color,
                                    "consecutive_failures": consecutive_failures,
                                    "error": str(exc),
                                },
                            )
                            await session.commit()
                    except Exception:
                        logger.warning("crypto funding rate ops-event log failed", exc_info=True)
            else:
                if result is not None:
                    consecutive_failures = 0

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
            strategy_promotion_watchdog = await self._maybe_run_strategy_promotion_watchdog()
            if strategy_promotion_watchdog is not None:
                payload["strategy_promotion_watchdog"] = strategy_promotion_watchdog
            strategy_codex_nightly = await self._maybe_run_strategy_codex_nightly()
            if strategy_codex_nightly is not None:
                if strategy_codex_nightly.get("mode") == "auto_evolve":
                    payload["strategy_auto_evolve"] = strategy_codex_nightly
                else:
                    payload["strategy_codex_nightly"] = strategy_codex_nightly
            momentum_calibration_nightly = await self._maybe_run_momentum_calibration_nightly()
            if momentum_calibration_nightly is not None:
                payload["momentum_calibration_nightly"] = momentum_calibration_nightly
            historical_pipeline = await self._maybe_run_historical_pipeline()
            if historical_pipeline is not None:
                payload["historical_pipeline"] = historical_pipeline
            elif self.historical_pipeline_service is None:
                historical_intelligence = await self._maybe_run_historical_intelligence()
                if historical_intelligence is not None:
                    payload["historical_intelligence"] = historical_intelligence
            decision_corpus_promotion = await self._maybe_run_decision_corpus_promotion()
            if decision_corpus_promotion is not None:
                payload["decision_corpus_promotion"] = decision_corpus_promotion
            autonomous_gate_tuning = await self._maybe_run_autonomous_gate_tuning()
            if autonomous_gate_tuning is not None:
                payload["autonomous_gate_tuning"] = autonomous_gate_tuning
            if self.autonomous_gate_tuning_service is not None:
                await self.autonomous_gate_tuning_service.maybe_emit_drift_alert(
                    kalshi_env=self.settings.kalshi_env,
                )
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
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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

    async def _periodic_crypto_model_nightly_loop(self) -> None:
        interval = max(300, int(self.settings.daemon_heartbeat_interval_seconds))
        while True:
            await asyncio.sleep(interval)
            try:
                await self._maybe_run_crypto_model_nightly_if_active()
            except Exception:
                logger.warning("crypto_model_nightly loop error", exc_info=True)

    async def _maybe_run_crypto_model_nightly_if_active(self) -> dict[str, Any] | None:
        if not self.settings.crypto_model_nightly_auto_enabled:
            return None
        if not await self._is_active_color():
            return None
        result = await self._maybe_run_crypto_model_nightly()
        if result is not None:
            logger.info(
                "crypto_model_nightly completed refreshed_count=%s",
                result.get("refreshed_count"),
            )
        return result

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
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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
        if not self._weather_market_updates_enabled():
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
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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

    async def _maybe_run_decision_corpus_promotion(self) -> dict[str, Any] | None:
        if self.decision_corpus_service is None:
            return None
        checkpoint_name = f"daemon_decision_corpus_promotion:{self.settings.kalshi_env}:{self.settings.app_color}"
        last_run_at = await self._checkpoint_time(checkpoint_name)
        now = self._utc_now()
        min_interval = timedelta(seconds=max(3600, self.settings.decision_corpus_auto_promote_interval_seconds))
        if last_run_at is not None and now - last_run_at < min_interval:
            return None
        try:
            result = await self.decision_corpus_service.nightly_auto_promote(
                kalshi_env=self.settings.kalshi_env,
                actor=f"daemon:{self.settings.app_color}",
            )
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.set_checkpoint(
                    checkpoint_name,
                    None,
                    {
                        "ran_at": now.isoformat(),
                        "result": result,
                    },
                )
                await session.commit()
            return result
        except Exception:
            logger.warning("decision corpus auto-promotion failed", exc_info=True)
            return None

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

    async def _maybe_run_autonomous_gate_tuning(self) -> dict[str, Any] | None:
        if not self.settings.autonomous_gate_tuning_enabled or self.autonomous_gate_tuning_service is None:
            return None
        checkpoint_name = f"daemon_autonomous_gate_tuning:{self.settings.kalshi_env}:{self.settings.app_color}"
        last_run_at = await self._checkpoint_time(checkpoint_name)
        now = self._utc_now()
        interval = timedelta(seconds=max(60, int(self.settings.autonomous_gate_tuning_periodic_interval_seconds)))
        if last_run_at is not None and now - last_run_at < interval:
            return None
        try:
            result = await self.autonomous_gate_tuning_service.run(
                kalshi_env=self.settings.kalshi_env,
                source=self.settings.autonomous_gate_tuning_source,
                days=self.settings.autonomous_gate_tuning_days,
                min_support=self.settings.autonomous_gate_tuning_min_support,
                triggered_by="active_periodic",
                domain="all",
            )
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.set_checkpoint(
                    checkpoint_name,
                    None,
                    {
                        "ran_at": now.isoformat(),
                        "result": result,
                    },
                )
                await session.commit()
            return result
        except Exception:
            logger.warning("autonomous gate tuning periodic run failed", exc_info=True)
            return None

    async def _maybe_run_strategy_promotion_watchdog(self) -> dict[str, Any] | None:
        if self.strategy_auto_evolve_service is None:
            return None
        try:
            return await self.strategy_auto_evolve_service.run_promotion_watchdog_once(trigger_source="nightly")
        except Exception:
            logger.warning("strategy promotion watchdog failed", exc_info=True)
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
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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

    async def _maybe_run_momentum_calibration_nightly(self) -> dict[str, Any] | None:
        if not self.settings.momentum_calibration_auto_enabled:
            return None
        if self.momentum_calibration_service is None:
            return None

        night_state = self._momentum_calibration_nightly_state()
        if not night_state["due"]:
            return None

        checkpoint_name = f"nightly_momentum_calibration_run:{self.settings.kalshi_env}:{self.settings.app_color}"
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            checkpoint = await repo.get_checkpoint(checkpoint_name)
            await session.commit()
        if checkpoint is not None and isinstance(checkpoint.payload, dict):
            if checkpoint.payload.get("ran_at"):
                try:
                    ran_at = datetime.fromisoformat(checkpoint.payload["ran_at"])
                    local_ran = ran_at.astimezone(ZoneInfo(self.settings.momentum_calibration_nightly_timezone))
                    if local_ran.date().isoformat() == night_state["local_date"]:
                        return None
                except (ValueError, TypeError):
                    pass

        try:
            return await self.momentum_calibration_service.nightly_auto_run(
                app_color=self.settings.app_color
            )
        except Exception:
            logger.warning("momentum_calibration nightly run error", exc_info=True)
            return None

    def _momentum_calibration_nightly_state(self) -> dict[str, Any]:
        now = self._utc_now()
        timezone = ZoneInfo(self.settings.momentum_calibration_nightly_timezone)
        local_now = now.astimezone(timezone)
        target_local = local_now.replace(
            hour=self.settings.momentum_calibration_nightly_hour_local,
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

    async def _maybe_run_crypto_model_nightly(self) -> dict[str, Any] | None:
        if not self.settings.crypto_model_nightly_auto_enabled:
            return None
        if self._crypto_model_nightly_lock.locked():
            return None
        if (
            self.crypto_history_service is None
            or self.crypto_forecast_service is None
            or self.crypto_replay_service is None
        ):
            return None

        async with self._crypto_model_nightly_lock:
            night_state = self._crypto_model_nightly_state()
            if not night_state["due"]:
                return None

            checkpoint_name = f"daemon_crypto_model_nightly:{self.settings.kalshi_env}:{self.settings.app_color}"
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                checkpoint = await repo.get_checkpoint(checkpoint_name)
                await session.commit()
            if checkpoint is not None and isinstance(checkpoint.payload, dict):
                if checkpoint.payload.get("ran_at"):
                    try:
                        ran_at = datetime.fromisoformat(checkpoint.payload["ran_at"])
                        local_ran = ran_at.astimezone(ZoneInfo(self.settings.crypto_model_nightly_timezone))
                        if local_ran.date().isoformat() == night_state["local_date"]:
                            return None
                    except (ValueError, TypeError):
                        pass

            try:
                logger.info(
                    "crypto_model_nightly starting local_date=%s target_local=%s",
                    night_state["local_date"],
                    night_state["target_local"].isoformat(),
                )
                return await self._run_crypto_model_nightly_for_env(checkpoint_name)
            except Exception:
                logger.warning("crypto_model nightly run error", exc_info=True)
                return None

    def _crypto_model_nightly_state(self) -> dict[str, Any]:
        now = self._utc_now()
        timezone = ZoneInfo(self.settings.crypto_model_nightly_timezone)
        local_now = now.astimezone(timezone)
        target_local = local_now.replace(
            hour=self.settings.crypto_model_nightly_hour_local,
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

    @staticmethod
    def _crypto_model_refresh_reason(
        *,
        artifact_status: str | None,
        trained_at: datetime | None,
        now: datetime,
        max_age: timedelta,
        new_rows: int,
        min_new_rows: int,
    ) -> str | None:
        """Decide whether the nightly should (re)train a model for one asset.

        Returns the refresh reason, or ``None`` to skip. Note that successfully
        trained models are recorded with status ``"trained"`` (never ``"ready"``);
        a non-trained artifact such as ``"insufficient_data"`` means a prior
        attempt found nothing to learn from, so we stay dormant until genuinely
        new strict-as-of rows arrive rather than failing training every night.
        """
        has_new_data = new_rows >= min_new_rows
        if artifact_status is None:
            return "missing"
        if artifact_status == "trained":
            if trained_at is not None:
                if trained_at.tzinfo is None:
                    trained_at = trained_at.replace(tzinfo=UTC)
                if (now - trained_at.astimezone(UTC)) > max_age:
                    return "aged_out"
            return "new_data" if has_new_data else None
        return "new_data" if has_new_data else None

    @staticmethod
    def _crypto_model_nightly_rotation(
        *,
        local_date: date,
        configured_assets: list[str],
        training_freqs: list[str],
        pooled_only: bool,
    ) -> tuple[list[str], list[str], dict[str, int]]:
        """Pick tonight's training scope from a stateless date-ordinal rotation.

        Per-asset mode: ``slot = ordinal % (assets × frequencies)`` selects one
        (asset, frequency) pair. Pooled mode: ``slot = ordinal % frequencies``
        selects tonight's frequency only and the full configured asset list is
        trained as a single pooled model.
        """
        if pooled_only:
            if not training_freqs:
                return [], [], {"slot": 0, "slot_count": 0}
            slot = local_date.toordinal() % len(training_freqs)
            return list(configured_assets), [training_freqs[slot]], {"slot": slot, "slot_count": len(training_freqs)}
        if not configured_assets or not training_freqs:
            return [], [], {"slot": 0, "slot_count": 0}
        rotation_pairs = [(a, f) for f in training_freqs for a in configured_assets]
        slot = local_date.toordinal() % len(rotation_pairs)
        selected_asset, selected_freq = rotation_pairs[slot]
        return [selected_asset], [selected_freq], {"slot": slot, "slot_count": len(rotation_pairs)}

    async def _run_crypto_model_nightly_for_env(self, checkpoint_name: str) -> dict[str, Any]:
        configured_assets = [a.strip().upper() for a in self.settings.crypto_model_nightly_assets.split(",") if a.strip()]
        max_age_td = timedelta(hours=self.settings.crypto_model_nightly_max_age_hours)
        now = self._utc_now()

        # Build training frequencies from crypto_model_nightly_frequencies (independent
        # of CRYPTO_AUTO_FREQUENCIES which controls live trading, not training).
        from kalshi_bot.crypto.services import normalize_frequency as _nf
        training_freqs = [_nf(f.strip()) for f in self.settings.crypto_model_nightly_frequencies.replace(";", ",").split(",") if f.strip()]
        training_freqs = [f for f in training_freqs if f]
        if not training_freqs:
            training_freqs = ["15m"]

        # Stateless date-ordinal rotation. Per-asset mode trains exactly one
        # (asset, frequency) pair per night over a 14-slot rotation (7 assets ×
        # 2 frequencies; 15m pairs occupy slots 0-6, 1h pairs slots 7-13).
        # Pooled mode collapses the rotation to one slot per frequency and
        # trains a single pooled (all-asset) model per night.
        pooled_only = self.settings.crypto_model_nightly_pooled_only
        local_date = now.astimezone(ZoneInfo(self.settings.crypto_model_nightly_timezone)).date()
        assets, frequencies, rotation = self._crypto_model_nightly_rotation(
            local_date=local_date,
            configured_assets=configured_assets,
            training_freqs=training_freqs,
            pooled_only=pooled_only,
        )
        if frequencies:
            logger.info(
                "crypto_model_nightly rotation selected assets=%s freq=%s mode=%s (slot %d of %d) for local_date=%s",
                ",".join(assets) or "ALL",
                frequencies[0],
                "pooled" if pooled_only else "per_asset",
                rotation["slot"] + 1,
                rotation["slot_count"],
                local_date.isoformat(),
            )

        asset_decisions: dict[str, dict[str, str]] = {}
        sizing_policy_results: dict[str, Any] = {}
        total_refreshed = 0

        for frequency in frequencies:
            # status() scans the large snapshots table; bound it so a slow/hung
            # query degrades to age-based refresh rather than aborting the whole
            # nightly (which would also skip writing the completion checkpoint).
            by_asset_rows: dict[str, int] = {}
            try:
                status = await asyncio.wait_for(
                    self.crypto_history_service.status(days=1, frequency=frequency),
                    timeout=self.settings.crypto_model_nightly_status_timeout_seconds,
                )
                for asset, info in status.get("quote_evidence", {}).get("trade_candidate_support_by_asset", {}).items():
                    by_asset_rows[asset.upper()] = int(info.get("strict_trade_eligible_rows") or 0)
            except Exception:
                # Includes asyncio.TimeoutError. Intentionally broad: this is an
                # optional optimization input, never a reason to skip the regen.
                logger.warning(
                    "crypto_model_nightly status() unavailable freq=%s; using age-based refresh only",
                    frequency,
                    exc_info=True,
                )

            freq_decisions: dict[str, str] = {}
            refreshed_assets: list[str] = []

            if pooled_only:
                freq_decisions = await self._run_crypto_model_nightly_pooled_frequency(
                    frequency=frequency,
                    assets=assets,
                    by_asset_rows=by_asset_rows,
                    now=now,
                    max_age=max_age_td,
                )
                if freq_decisions.get("pooled") == "refreshed":
                    total_refreshed += 1
                asset_decisions[frequency] = freq_decisions
                try:
                    sizing_policy_results[frequency] = await self._update_crypto_pnl_sizing_policy(
                        frequency=frequency,
                        assets=assets,
                        evaluated_at=now,
                    )
                except Exception:
                    sizing_policy_results[frequency] = {"status": "error"}
                    logger.warning("crypto_model_nightly sizing policy update error freq=%s", frequency, exc_info=True)
                continue

            for asset in assets:
                refresh_reason: str | None = None

                async with self.session_factory() as session:
                    repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                    artifact = await repo.get_latest_crypto_model_artifact(
                        frequency=frequency,
                        artifact_type=f"model:{asset}",
                        kalshi_env=self.settings.kalshi_env,
                    )
                    await session.commit()

                refresh_reason = self._crypto_model_refresh_reason(
                    artifact_status=(artifact.status if artifact is not None else None),
                    trained_at=(artifact.trained_at if artifact is not None else None),
                    now=now,
                    max_age=max_age_td,
                    new_rows=by_asset_rows.get(asset, 0),
                    min_new_rows=self.settings.crypto_model_nightly_min_new_strict_rows,
                )

                if refresh_reason is None:
                    freq_decisions[asset] = "skipped_fresh"
                    logger.info(
                        "crypto_model_nightly skip asset=%s freq=%s strict_rows_24h=%d",
                        asset,
                        frequency,
                        by_asset_rows.get(asset, 0),
                    )
                    continue

                try:
                    preflight: dict[str, Any] | None = None
                    if self.settings.crypto_training_preflight_enabled and self.crypto_training_backfill_service is not None:
                        preflight = await self.crypto_training_backfill_service.prepare(
                            frequency=frequency,
                            asset_symbols=[asset],
                            run_source_backfill=True,
                        )
                        if preflight.get("status") != "ok":
                            freq_decisions[asset] = "skipped_preflight_blocked"
                            logger.warning(
                                "crypto_model_nightly preflight blocked asset=%s freq=%s blockers=%s",
                                asset,
                                frequency,
                                preflight.get("blockers"),
                            )
                            continue
                    else:
                        await self.crypto_history_service.collect_settled(frequency=frequency, asset_symbols=[asset])
                        await self.crypto_history_service.bootstrap(frequency=frequency, asset_symbols=[asset])
                        if self.crypto_spot_service is not None:
                            try:
                                await self.crypto_spot_service.collect_current(frequency=frequency, asset_symbols=[asset])
                            except Exception:
                                logger.warning(
                                    "crypto_model_nightly spot collect failed asset=%s freq=%s",
                                    asset,
                                    frequency,
                                    exc_info=True,
                                )
                    await self.crypto_forecast_service.train(
                        frequency=frequency,
                        asset_symbols=[asset],
                        use_feature_store=bool(preflight),
                        feature_store_only=bool(preflight),
                    )
                    await self.crypto_replay_service.run(frequency=frequency, asset_symbols=[asset])
                    await self.crypto_replay_service.gate(frequency=frequency, asset_symbols=[asset])
                    freq_decisions[asset] = "refreshed"
                    refreshed_assets.append(asset)
                    logger.info(
                        "crypto_model_nightly refreshed asset=%s freq=%s reason=%s",
                        asset,
                        frequency,
                        refresh_reason,
                    )
                except Exception:
                    freq_decisions[asset] = "error"
                    logger.warning(
                        "crypto_model_nightly refresh error asset=%s freq=%s",
                        asset,
                        frequency,
                        exc_info=True,
                    )

            if refreshed_assets:
                try:
                    await self.crypto_replay_service.gate(frequency=frequency, asset_symbols=assets)
                except Exception:
                    logger.warning("crypto_model_nightly pooled gate error freq=%s", frequency, exc_info=True)

            try:
                sizing_policy_results[frequency] = await self._update_crypto_pnl_sizing_policy(
                    frequency=frequency,
                    assets=assets,
                    evaluated_at=now,
                )
            except Exception:
                sizing_policy_results[frequency] = {"status": "error"}
                logger.warning("crypto_model_nightly sizing policy update error freq=%s", frequency, exc_info=True)

            asset_decisions[frequency] = freq_decisions
            total_refreshed += len(refreshed_assets)

        result: dict[str, Any] = {
            "ran_at": now.isoformat(),
            "asset_decisions": asset_decisions,
            "sizing_policy": sizing_policy_results,
            "refreshed_count": total_refreshed,
        }
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            await repo.set_checkpoint(checkpoint_name, None, result)
            await session.commit()
        return result

    async def _run_crypto_model_nightly_pooled_frequency(
        self,
        *,
        frequency: str,
        assets: list[str],
        by_asset_rows: dict[str, int],
        now: datetime,
        max_age: timedelta,
    ) -> dict[str, str]:
        """Train one pooled (all-asset) model for ``frequency`` if it needs a refresh.

        Multi-asset training records the generic pooled ``model`` artifact
        (``_crypto_artifact_type`` only suffixes single-asset runs). The pooled
        replay run records the pooled backtest plus per-asset backtests, and the
        per-asset gate calls preserve per-asset go/no-go visibility.
        """
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            artifact = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()

        pooled_new_rows = sum(by_asset_rows.get(asset, 0) for asset in assets) if assets else sum(by_asset_rows.values())
        refresh_reason = self._crypto_model_refresh_reason(
            artifact_status=(artifact.status if artifact is not None else None),
            trained_at=(artifact.trained_at if artifact is not None else None),
            now=now,
            max_age=max_age,
            new_rows=pooled_new_rows,
            min_new_rows=self.settings.crypto_model_nightly_min_new_strict_rows,
        )
        if refresh_reason is None:
            logger.info(
                "crypto_model_nightly skip pooled freq=%s strict_rows_24h=%d",
                frequency,
                pooled_new_rows,
            )
            return {"pooled": "skipped_fresh"}

        try:
            preflight: dict[str, Any] | None = None
            if self.settings.crypto_training_preflight_enabled and self.crypto_training_backfill_service is not None:
                preflight = await self.crypto_training_backfill_service.prepare(
                    frequency=frequency,
                    asset_symbols=assets or None,
                    run_source_backfill=True,
                )
                if preflight.get("status") != "ok":
                    logger.warning(
                        "crypto_model_nightly preflight blocked pooled freq=%s blockers=%s",
                        frequency,
                        preflight.get("blockers"),
                    )
                    return {"pooled": "skipped_preflight_blocked"}
            else:
                await self.crypto_history_service.collect_settled(frequency=frequency, asset_symbols=assets or None)
                await self.crypto_history_service.bootstrap(frequency=frequency, asset_symbols=assets or None)
                if self.crypto_spot_service is not None:
                    try:
                        await self.crypto_spot_service.collect_current(frequency=frequency, asset_symbols=assets or None)
                    except Exception:
                        logger.warning(
                            "crypto_model_nightly spot collect failed pooled freq=%s",
                            frequency,
                            exc_info=True,
                        )
            await self.crypto_forecast_service.train(
                frequency=frequency,
                asset_symbols=assets or None,
                use_feature_store=bool(preflight),
                feature_store_only=bool(preflight),
            )
            await self.crypto_replay_service.run(frequency=frequency, asset_symbols=assets or None)
        except Exception:
            logger.warning("crypto_model_nightly pooled refresh error freq=%s", frequency, exc_info=True)
            return {"pooled": "error"}

        decisions: dict[str, str] = {"pooled": "refreshed"}
        # Per-asset gates keep per-asset go/no-go visibility on top of the
        # pooled model (the pooled replay run records per-asset backtests).
        for asset in assets:
            try:
                await self.crypto_replay_service.gate(frequency=frequency, asset_symbols=[asset])
                decisions[asset] = "gated"
            except Exception:
                decisions[asset] = "gate_error"
                logger.warning(
                    "crypto_model_nightly per-asset gate error asset=%s freq=%s",
                    asset,
                    frequency,
                    exc_info=True,
                )
        try:
            await self.crypto_replay_service.gate(frequency=frequency, asset_symbols=assets or None)
        except Exception:
            logger.warning("crypto_model_nightly pooled gate error freq=%s", frequency, exc_info=True)
        logger.info(
            "crypto_model_nightly refreshed pooled freq=%s reason=%s assets=%s",
            frequency,
            refresh_reason,
            ",".join(assets) or "ALL",
        )
        return decisions

    async def _update_crypto_pnl_sizing_policy(
        self,
        *,
        frequency: str,
        assets: list[str],
        evaluated_at: datetime,
    ) -> dict[str, Any]:
        if not self.settings.crypto_pnl_sizing_auto_enabled:
            return {"status": "disabled"}

        from kalshi_bot.core.schemas import AgentPack, AgentPackCryptoEntryPolicy
        from kalshi_bot.services.agent_packs import (
            AgentPackService,
            DETERMINISTIC_AGENT_PACK_OVERRIDES_VERSION,
        )

        service = AgentPackService(self.settings)
        per_asset: dict[str, Any] = {}
        optimizer_result: dict[str, Any] | None = None
        optimizer_error: str | None = None
        optimizer_by_asset: dict[str, dict[str, Any]] = {}
        if self.settings.crypto_entry_policy_optimizer_auto_enabled and self.crypto_replay_service is not None:
            try:
                optimizer_result = await self.crypto_replay_service.optimize_entry_policy(
                    frequency=frequency,
                    days=self.settings.crypto_entry_policy_optimizer_days,
                    asset_symbols=assets,
                )
                for report in optimizer_result.get("asset_reports") or []:
                    if isinstance(report, dict) and report.get("asset"):
                        optimizer_by_asset[str(report["asset"]).upper()] = report
            except Exception as exc:
                optimizer_error = str(exc)
                logger.warning("crypto_model_nightly entry policy optimizer error freq=%s", frequency, exc_info=True)

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            existing_record = await repo.get_agent_pack(DETERMINISTIC_AGENT_PACK_OVERRIDES_VERSION)
            if existing_record is not None:
                pack = AgentPack.model_validate(existing_record.payload)
            else:
                pack = await service.get_pack_for_color(repo, control.active_color)
            pack = pack.model_copy(
                deep=True,
                update={
                    "version": DETERMINISTIC_AGENT_PACK_OVERRIDES_VERSION,
                    "source": "operator_override",
                    "description": "Operator and nightly crypto asset-level entry overrides on top of builtin defaults.",
                },
            )
            overrides = dict(pack.crypto_policy.asset_entry_overrides or {})
            planned_updates: dict[str, dict[str, Any]] = {}

            for raw_asset in assets:
                asset = raw_asset.strip().upper()
                if not asset:
                    continue
                gate = await repo.get_latest_crypto_model_artifact(
                    frequency=frequency,
                    artifact_type=f"replay_gate:{asset}",
                    kalshi_env=self.settings.kalshi_env,
                )
                gate_metrics = dict(getattr(gate, "metrics", None) or {}) if gate is not None else {}
                sizing = crypto_pnl_sizing_target_pct(gate_metrics, settings=self.settings)
                optimizer_report = optimizer_by_asset.get(asset) or {}
                winner = optimizer_report.get("winner") if isinstance(optimizer_report.get("winner"), dict) else None
                winner_entry = dict(winner.get("entry_policy") or {}) if winner else {}
                winner_metrics = dict(winner.get("metrics") or {}) if winner else {}
                planned_updates[asset] = {
                    "override_key": crypto_entry_override_key(asset, frequency),
                    "raw_target_position_pct": float(sizing["target_position_pct"]),
                    "sizing_max_spread_bps": int(sizing["max_spread_bps"]),
                    "optimizer_entry_policy": winner_entry,
                    "gate_status": getattr(gate, "status", "missing") if gate is not None else "missing",
                    "gate_version": getattr(gate, "version", None),
                    "sizing_diagnostics": sizing["diagnostics"],
                    "optimizer": {
                        "status": optimizer_report.get("status", "not_run") if optimizer_report else "not_run",
                        "evaluated_policy_count": optimizer_report.get("evaluated_policy_count"),
                        "oos_fold_count": optimizer_report.get("oos_fold_count"),
                        "strict_trade_eligible_count": optimizer_report.get("strict_trade_eligible_count"),
                        "spot_feature_coverage_pct": optimizer_report.get("spot_feature_coverage_pct"),
                        "blockers": list(optimizer_report.get("blockers") or []),
                        "winner_entry_policy": winner_entry or None,
                        "winner_metrics": {
                            "selected_count": winner_metrics.get("selected_count"),
                            "net_pnl": winner_metrics.get("net_pnl"),
                            "pnl_advantage_vs_market_mid": winner_metrics.get("pnl_advantage_vs_market_mid"),
                            "hard_cap_breaches": winner_metrics.get("hard_cap_breaches"),
                        }
                        if winner_metrics
                        else None,
                    },
                }

            portfolio_cap = min(max(float(self.settings.crypto_portfolio_max_allocation_pct), 0.0), 1.0)
            raw_total_target_pct = sum(float(item["raw_target_position_pct"]) for item in planned_updates.values())
            allocation_scale = (
                min(1.0, portfolio_cap / raw_total_target_pct)
                if raw_total_target_pct > 0
                else 1.0
            )

            for asset, plan in planned_updates.items():
                override_key = str(plan["override_key"])
                optimizer_entry = dict(plan.get("optimizer_entry_policy") or {})
                target_pct = min(
                    max(float(plan["raw_target_position_pct"]) * allocation_scale, 0.0),
                    min(float(self.settings.crypto_dynamic_order_max_position_pct), 0.20),
                )
                entry_update: dict[str, Any] = {
                    "target_position_pct": target_pct,
                    "max_spread_bps": int(optimizer_entry.get("max_spread_bps") or plan["sizing_max_spread_bps"]),
                }
                for key in (
                    "min_fee_adjusted_edge_bps",
                    "min_confidence",
                    "min_contract_price_dollars",
                    "min_remaining_payout_bps",
                    "max_credible_edge_bps",
                ):
                    if optimizer_entry.get(key) is not None:
                        entry_update[key] = optimizer_entry[key]
                existing_override = (
                    overrides.get(override_key)
                    or overrides.get(asset)
                    or AgentPackCryptoEntryPolicy()
                )
                overrides[override_key] = existing_override.model_copy(update=entry_update)
                per_asset[asset] = {
                    "override_key": override_key,
                    "target_position_pct": target_pct,
                    "raw_target_position_pct": plan["raw_target_position_pct"],
                    "allocation_scale": allocation_scale,
                    "max_spread_bps": entry_update["max_spread_bps"],
                    "gate_status": plan["gate_status"],
                    "gate_version": plan["gate_version"],
                    **dict(plan["sizing_diagnostics"]),
                    "optimizer": plan["optimizer"],
                }

            pack.crypto_policy.asset_entry_overrides = overrides
            pack.metadata = {
                **dict(pack.metadata or {}),
                "crypto_pnl_sizing": {
                    "status": "updated",
                    "frequency": frequency,
                    "evaluated_at": evaluated_at.isoformat(),
                    "assets": per_asset,
                    "max_allocation_pct": portfolio_cap,
                    "raw_total_target_pct": raw_total_target_pct,
                    "allocation_scale": allocation_scale,
                    "max_market_target_pct": min(
                        max(float(self.settings.crypto_dynamic_order_max_position_pct), 0.0),
                        0.15,
                    ),
                    "entry_policy_optimizer": {
                        "enabled": bool(self.settings.crypto_entry_policy_optimizer_auto_enabled),
                        "status": (
                            "error"
                            if optimizer_error
                            else (optimizer_result or {}).get("status", "not_run")
                        ),
                        "days": self.settings.crypto_entry_policy_optimizer_days,
                        "error": optimizer_error,
                        "stageable_assets": list((optimizer_result or {}).get("stageable_assets") or []),
                    },
                },
            }
            await repo.update_agent_pack(pack)
            await service.assign_pack_to_color(
                repo,
                color=control.active_color,
                version=DETERMINISTIC_AGENT_PACK_OVERRIDES_VERSION,
            )
            await session.commit()

        return {
            "status": "updated",
            "pack_version": DETERMINISTIC_AGENT_PACK_OVERRIDES_VERSION,
            "assets": per_asset,
        }

    async def _checkpoint_time(self, stream_name: str) -> datetime | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
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

    def _startup_delay_seconds(self) -> float:
        grace = max(0, float(self.settings.daemon_startup_grace_seconds))
        jitter_window = max(0, int(self.settings.daemon_startup_jitter_seconds))
        if jitter_window <= 0:
            return grace
        key = f"{self.settings.kalshi_env}:{self.settings.app_color}".encode("utf-8")
        jitter = int.from_bytes(hashlib.sha256(key).digest()[:4], "big") % (jitter_window + 1)
        return grace + float(jitter)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    def _utc_now(self) -> datetime:
        return datetime.now(UTC)
