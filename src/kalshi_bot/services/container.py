from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings, get_settings
from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.forecast_archive import OpenMeteoForecastArchiveClient
from kalshi_bot.integrations.kalshi import KalshiClient, KalshiWebSocketClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.historical_heuristics import HistoricalHeuristicService
from kalshi_bot.services.historical_intelligence import HistoricalIntelligenceService
from kalshi_bot.services.historical_pipeline import HistoricalPipelineService
from kalshi_bot.services.historical_training import HistoricalTrainingService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.services.daemon import DaemonService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.reconcile import ReconciliationService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.services.signal_calibration import SignalCalibrationService
from kalshi_bot.services.streaming import MarketStreamService
from kalshi_bot.services.self_improve import SelfImproveService
from kalshi_bot.services.stop_loss import StopLossService
from kalshi_bot.services.strategy_auto_evolve import StrategyAutoEvolveService
from kalshi_bot.services.strategy_eval import StrategyEvaluationService
from kalshi_bot.services.strategy_codex import StrategyCodexService
from kalshi_bot.services.strategy_dashboard import StrategyDashboardService
from kalshi_bot.services.strategy_regression import StrategyRegressionService
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.services.market_history import MarketHistoryService
from kalshi_bot.services.watchdog import WatchdogService
from kalshi_bot.services.shadow_campaign import ShadowCampaignService
from kalshi_bot.services.strategy_cleanup_service import StrategyCleanupService
from kalshi_bot.services.monotonicity_scanner_service import MonotonicityArbScannerService
from kalshi_bot.weather.mapping import WeatherMarketDirectory


@dataclass(slots=True)
class AppContainer:
    settings: Settings
    engine: AsyncEngine
    secondary_engine: AsyncEngine | None
    session_factory: async_sessionmaker[AsyncSession]
    secondary_session_factory: async_sessionmaker[AsyncSession] | None
    regression_read_session_factory: async_sessionmaker[AsyncSession]
    regression_read_source: str
    providers: ProviderRouter
    kalshi: KalshiClient
    kalshi_ws: KalshiWebSocketClient
    weather: NWSWeatherClient
    forecast_archive: OpenMeteoForecastArchiveClient
    weather_directory: WeatherMarketDirectory
    agent_pack_service: AgentPackService
    signal_engine: WeatherSignalEngine
    signal_calibration_service: SignalCalibrationService
    risk_engine: DeterministicRiskEngine
    execution_service: ExecutionService
    memory_service: MemoryService
    research_coordinator: ResearchCoordinator
    auto_trigger_service: AutoTriggerService
    daemon_service: DaemonService
    discovery_service: DiscoveryService
    reconciliation_service: ReconciliationService
    stream_service: MarketStreamService
    training_export_service: TrainingExportService
    training_corpus_service: TrainingCorpusService
    historical_training_service: HistoricalTrainingService
    historical_heuristic_service: HistoricalHeuristicService
    historical_intelligence_service: HistoricalIntelligenceService
    historical_pipeline_service: HistoricalPipelineService
    shadow_training_service: ShadowTrainingService
    shadow_campaign_service: ShadowCampaignService
    self_improve_service: SelfImproveService
    strategy_cleanup_service: StrategyCleanupService
    monotonicity_arb_service: MonotonicityArbScannerService
    strategy_codex_service: StrategyCodexService
    strategy_dashboard_service: StrategyDashboardService
    strategy_auto_evolve_service: StrategyAutoEvolveService
    market_history_service: MarketHistoryService
    watchdog_service: WatchdogService
    agents: AgentSuite
    supervisor: WorkflowSupervisor

    @classmethod
    async def build(cls, *, bootstrap_db: bool = True) -> "AppContainer":
        settings = get_settings()
        engine = create_engine(settings)
        session_factory = create_session_factory(engine)
        if settings.app_auto_init_db:
            await init_models(engine)
        secondary_engine: AsyncEngine | None = None
        secondary_session_factory: async_sessionmaker[AsyncSession] | None = None
        if settings.secondary_database_url:
            secondary_engine = create_async_engine(settings.secondary_database_url, pool_pre_ping=True)
            secondary_session_factory = create_session_factory(secondary_engine)

        requested_read_source = (settings.strategy_regression_read_source or "primary").lower()
        if requested_read_source not in {"primary", "secondary"}:
            logger.error(
                "Unknown strategy_regression_read_source=%r; falling back to 'primary'",
                settings.strategy_regression_read_source,
            )
            requested_read_source = "primary"
        if requested_read_source == "secondary" and secondary_session_factory is None:
            logger.error(
                "strategy_regression_read_source=secondary but no secondary DB configured "
                "(set POSTGRES_SECONDARY_HOST); falling back to primary"
            )
            regression_read_session_factory = session_factory
            regression_read_source_active = "primary"
        elif requested_read_source == "secondary":
            regression_read_session_factory = secondary_session_factory
            regression_read_source_active = "secondary"
            logger.info("Regression reads: using secondary DB")
        else:
            regression_read_session_factory = session_factory
            regression_read_source_active = "primary"

        providers = ProviderRouter(settings)
        kalshi = KalshiClient(settings)
        kalshi_ws = KalshiWebSocketClient(settings, kalshi)
        weather = NWSWeatherClient(settings)
        forecast_archive = OpenMeteoForecastArchiveClient(settings)
        weather_directory = WeatherMarketDirectory.from_file(settings.weather_market_map_file)
        for warning in weather_directory.validate():
            logger.warning("Market map validation: %s", warning)
        agent_pack_service = AgentPackService(settings)
        signal_engine = WeatherSignalEngine(settings)
        signal_calibration_service = SignalCalibrationService(session_factory)
        risk_engine = DeterministicRiskEngine(settings)
        execution_service = ExecutionService(settings, kalshi)
        memory_service = MemoryService()
        watchdog_service = WatchdogService(settings)
        discovery_service = DiscoveryService(kalshi, weather_directory)
        market_history_service = MarketHistoryService(
            session_factory,
            kalshi,
            discovery_service,
            retention_hours=settings.daemon_market_history_retention_hours,
        )
        reconciliation_service = ReconciliationService(kalshi)
        stream_service = MarketStreamService(settings, session_factory, kalshi_ws)
        training_export_service = TrainingExportService(session_factory)
        historical_heuristic_service = HistoricalHeuristicService(settings)
        training_corpus_service = TrainingCorpusService(
            settings,
            session_factory,
            discovery_service,
            training_export_service,
            weather_directory,
        )
        research_coordinator = ResearchCoordinator(
            settings,
            session_factory,
            kalshi,
            weather,
            weather_directory,
            providers,
            signal_engine,
            agent_pack_service,
        )
        agents = AgentSuite(settings, providers)
        historical_training_service = HistoricalTrainingService(
            settings,
            session_factory,
            kalshi,
            forecast_archive,
            weather_directory,
            agent_pack_service,
            historical_heuristic_service,
            research_coordinator,
            risk_engine,
            memory_service,
            training_export_service,
            training_corpus_service,
            agents,
        )
        historical_intelligence_service = HistoricalIntelligenceService(
            settings,
            session_factory,
            weather_directory,
            agent_pack_service,
            historical_heuristic_service,
            research_coordinator,
            training_export_service,
            historical_training_service,
        )
        historical_pipeline_service = HistoricalPipelineService(
            settings,
            session_factory,
            historical_training_service,
            historical_intelligence_service,
        )
        self_improve_service = SelfImproveService(
            settings,
            session_factory,
            training_export_service,
            training_corpus_service,
            agent_pack_service,
            risk_engine,
        )
        supervisor = WorkflowSupervisor(
            settings=settings,
            session_factory=session_factory,
            kalshi=kalshi,
            weather=weather,
            weather_directory=weather_directory,
            agent_pack_service=agent_pack_service,
            signal_engine=signal_engine,
            risk_engine=risk_engine,
            execution_service=execution_service,
            memory_service=memory_service,
            historical_heuristic_service=historical_heuristic_service,
            research_coordinator=research_coordinator,
            training_corpus_service=training_corpus_service,
            agents=agents,
        )
        shadow_training_service = ShadowTrainingService(
            settings,
            session_factory,
            discovery_service,
            agent_pack_service,
            supervisor,
        )
        shadow_campaign_service = ShadowCampaignService(
            settings,
            session_factory,
            discovery_service,
            research_coordinator,
            shadow_training_service,
        )
        auto_trigger_service = AutoTriggerService(settings, session_factory, weather_directory, agent_pack_service, supervisor)
        stop_loss_service = StopLossService(settings, session_factory, execution_service)
        strategy_eval_service = StrategyEvaluationService(settings, session_factory, agent_pack_service)
        strategy_regression_service = StrategyRegressionService(
            settings,
            session_factory,
            weather_directory,
            agent_pack_service,
            read_session_factory=regression_read_session_factory,
        )
        strategy_codex_service = StrategyCodexService(
            settings,
            session_factory,
            strategy_regression_service,
            providers,
        )
        strategy_cleanup_service = StrategyCleanupService(
            settings,
            session_factory,
            kalshi,
            weather,
            weather_directory,
        )
        monotonicity_arb_service = MonotonicityArbScannerService(
            settings,
            session_factory,
            kalshi,
        )
        strategy_dashboard_service = StrategyDashboardService(
            settings=settings,
            session_factory=session_factory,
            regression_read_session_factory=regression_read_session_factory,
            weather_directory=weather_directory,
            strategy_codex_service=strategy_codex_service,
        )
        strategy_auto_evolve_service = StrategyAutoEvolveService(
            settings=settings,
            session_factory=session_factory,
            secondary_session_factory=secondary_session_factory,
            strategy_regression_service=strategy_regression_service,
            strategy_codex_service=strategy_codex_service,
            strategy_dashboard_service=strategy_dashboard_service,
        )
        daemon_service = DaemonService(
            settings,
            session_factory,
            weather_directory,
            discovery_service,
            stream_service,
            reconciliation_service,
            research_coordinator,
            auto_trigger_service,
            shadow_training_service,
            shadow_campaign_service,
            self_improve_service,
            training_corpus_service=training_corpus_service,
            historical_training_service=historical_training_service,
            historical_intelligence_service=historical_intelligence_service,
            historical_pipeline_service=historical_pipeline_service,
            market_history_service=market_history_service,
            strategy_eval_service=strategy_eval_service,
            strategy_regression_service=strategy_regression_service,
            strategy_cleanup_service=strategy_cleanup_service,
            monotonicity_arb_service=monotonicity_arb_service,
            strategy_codex_service=strategy_codex_service,
            strategy_dashboard_service=strategy_dashboard_service,
            strategy_auto_evolve_service=strategy_auto_evolve_service,
            stop_loss_service=stop_loss_service,
        )
        container = cls(
            settings=settings,
            engine=engine,
            secondary_engine=secondary_engine,
            session_factory=session_factory,
            secondary_session_factory=secondary_session_factory,
            regression_read_session_factory=regression_read_session_factory,
            regression_read_source=regression_read_source_active,
            providers=providers,
            kalshi=kalshi,
            kalshi_ws=kalshi_ws,
            weather=weather,
            forecast_archive=forecast_archive,
            weather_directory=weather_directory,
            agent_pack_service=agent_pack_service,
            signal_engine=signal_engine,
            signal_calibration_service=signal_calibration_service,
            risk_engine=risk_engine,
            execution_service=execution_service,
            memory_service=memory_service,
            research_coordinator=research_coordinator,
            auto_trigger_service=auto_trigger_service,
            daemon_service=daemon_service,
            discovery_service=discovery_service,
            reconciliation_service=reconciliation_service,
            stream_service=stream_service,
            training_export_service=training_export_service,
            training_corpus_service=training_corpus_service,
            historical_training_service=historical_training_service,
            historical_heuristic_service=historical_heuristic_service,
            historical_intelligence_service=historical_intelligence_service,
            historical_pipeline_service=historical_pipeline_service,
            shadow_training_service=shadow_training_service,
            shadow_campaign_service=shadow_campaign_service,
            self_improve_service=self_improve_service,
            strategy_cleanup_service=strategy_cleanup_service,
            monotonicity_arb_service=monotonicity_arb_service,
            strategy_codex_service=strategy_codex_service,
            strategy_dashboard_service=strategy_dashboard_service,
            strategy_auto_evolve_service=strategy_auto_evolve_service,
            market_history_service=market_history_service,
            watchdog_service=watchdog_service,
            agents=agents,
            supervisor=supervisor,
        )
        if bootstrap_db:
            async with session_factory() as session:
                repo = PlatformRepository(session)
                await repo.ensure_deployment_control(
                    settings.app_color,
                    initial_active_color=DeploymentColor.BLUE.value,
                    initial_kill_switch_enabled=settings.app_enable_kill_switch,
                )
                await agent_pack_service.ensure_initialized(repo)
                await historical_heuristic_service.ensure_initialized(repo)
                await strategy_regression_service.seed_strategies(repo)
                await session.commit()
        return container

    async def close(self) -> None:
        await self.kalshi_ws.close()
        await self.kalshi.close()
        await self.weather.close()
        await self.strategy_codex_service.close()
        await self.historical_training_service.close()
        await self.providers.close()
        await self.engine.dispose()
        if self.secondary_engine is not None:
            await self.secondary_engine.dispose()
