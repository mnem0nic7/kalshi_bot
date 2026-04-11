from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings, get_settings
from kalshi_bot.core.enums import DeploymentColor
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.kalshi import KalshiClient, KalshiWebSocketClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.services.daemon import DaemonService
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.reconcile import ReconciliationService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.services.signal import WeatherSignalEngine
from kalshi_bot.services.streaming import MarketStreamService
from kalshi_bot.services.self_improve import SelfImproveService
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.watchdog import WatchdogService
from kalshi_bot.weather.mapping import WeatherMarketDirectory


@dataclass(slots=True)
class AppContainer:
    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    providers: ProviderRouter
    kalshi: KalshiClient
    kalshi_ws: KalshiWebSocketClient
    weather: NWSWeatherClient
    weather_directory: WeatherMarketDirectory
    agent_pack_service: AgentPackService
    signal_engine: WeatherSignalEngine
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
    shadow_training_service: ShadowTrainingService
    self_improve_service: SelfImproveService
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

        providers = ProviderRouter(settings)
        kalshi = KalshiClient(settings)
        kalshi_ws = KalshiWebSocketClient(settings, kalshi)
        weather = NWSWeatherClient(settings)
        weather_directory = WeatherMarketDirectory.from_file(settings.weather_market_map_file)
        agent_pack_service = AgentPackService(settings)
        signal_engine = WeatherSignalEngine(settings)
        risk_engine = DeterministicRiskEngine(settings)
        execution_service = ExecutionService(settings, kalshi)
        memory_service = MemoryService(providers)
        watchdog_service = WatchdogService(settings)
        discovery_service = DiscoveryService(kalshi, weather_directory)
        reconciliation_service = ReconciliationService(kalshi)
        stream_service = MarketStreamService(settings, session_factory, kalshi_ws)
        training_export_service = TrainingExportService(session_factory)
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
        self_improve_service = SelfImproveService(
            settings,
            session_factory,
            providers,
            training_export_service,
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
            research_coordinator=research_coordinator,
            agents=agents,
        )
        shadow_training_service = ShadowTrainingService(
            settings,
            session_factory,
            discovery_service,
            agent_pack_service,
            supervisor,
        )
        auto_trigger_service = AutoTriggerService(settings, session_factory, weather_directory, agent_pack_service, supervisor)
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
            self_improve_service,
        )
        container = cls(
            settings=settings,
            engine=engine,
            session_factory=session_factory,
            providers=providers,
            kalshi=kalshi,
            kalshi_ws=kalshi_ws,
            weather=weather,
            weather_directory=weather_directory,
            agent_pack_service=agent_pack_service,
            signal_engine=signal_engine,
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
            shadow_training_service=shadow_training_service,
            self_improve_service=self_improve_service,
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
                await session.commit()
        return container

    async def close(self) -> None:
        await self.kalshi_ws.close()
        await self.kalshi.close()
        await self.weather.close()
        await self.providers.close()
        await self.engine.dispose()
