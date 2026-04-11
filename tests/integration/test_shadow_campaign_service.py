from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RoomStage
from kalshi_bot.core.schemas import ShadowCampaignRequest, RoomCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.discovery import MarketDiscovery
from kalshi_bot.services.shadow import ShadowTrainingService
from kalshi_bot.services.shadow_campaign import ShadowCampaignService
from kalshi_bot.weather.models import WeatherMarketMapping


class FakeDiscoveryService:
    def __init__(self, discoveries: list[MarketDiscovery]) -> None:
        self.discoveries = discoveries

    async def discover_configured_markets(self) -> list[MarketDiscovery]:
        return self.discoveries

    async def list_stream_markets(self) -> list[str]:
        return [item.mapping.market_ticker for item in self.discoveries]


class FakeSupervisor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def run_room(self, room_id: str, reason: str = "manual") -> None:
        self.calls.append((room_id, reason))


class FakeResearchCoordinator:
    def build_signal_from_dossier(self, dossier, market_response, *, min_edge_bps=None):
        return None


def _discovery(mapping: WeatherMarketMapping, *, bid: str, ask: str, no_ask: str) -> MarketDiscovery:
    return MarketDiscovery(
        mapping=mapping,
        status="open",
        close_ts=None,
        yes_bid_dollars=Decimal(bid),
        yes_ask_dollars=Decimal(ask),
        no_ask_dollars=Decimal(no_ask),
        can_trade=True,
        notes=[],
        raw={"market": {"ticker": mapping.market_ticker, "yes_bid_dollars": bid, "yes_ask_dollars": ask, "no_ask_dollars": no_ask}},
    )


@pytest.mark.asyncio
async def test_shadow_campaign_service_balances_selection_and_skips_recent_market(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/shadow-campaign.db",
        app_shadow_mode=True,
        app_enable_kill_switch=True,
        training_campaign_cooldown_seconds=3600,
        training_campaign_max_recent_per_market=1,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    mappings = [
        WeatherMarketMapping(
            market_ticker="WX-A",
            market_type="weather",
            station_id="KNYC",
            location_name="NYC",
            latitude=40.0,
            longitude=-73.0,
            threshold_f=80,
        ),
        WeatherMarketMapping(
            market_ticker="WX-B",
            market_type="weather",
            station_id="KORD",
            location_name="Chicago",
            latitude=41.0,
            longitude=-87.0,
            threshold_f=78,
        ),
        WeatherMarketMapping(
            market_ticker="WX-C",
            market_type="weather",
            station_id="KMIA",
            location_name="Miami",
            latitude=25.0,
            longitude=-80.0,
            threshold_f=84,
        ),
    ]
    discoveries = [
        _discovery(mappings[0], bid="0.54", ask="0.56", no_ask="0.45"),
        _discovery(mappings[1], bid="0.43", ask="0.45", no_ask="0.56"),
        _discovery(mappings[2], bid="0.62", ask="0.65", no_ask="0.38"),
    ]

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        old_room = await repo.create_room(
            RoomCreate(name="old", market_ticker="WX-A"),
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env=settings.kalshi_env,
        )
        await repo.update_room_stage(old_room.id, RoomStage.COMPLETE)
        await repo.save_room_campaign(
            room_id=old_room.id,
            campaign_id="existing",
            trigger_source="shadow_campaign",
            city_bucket="NYC",
            market_regime_bucket="tight",
            difficulty_bucket="near_threshold",
            outcome_bucket="trade_yes",
            payload={"market_ticker": "WX-A"},
        )
        await session.commit()

    supervisor = FakeSupervisor()
    shadow_service = ShadowTrainingService(
        settings,
        session_factory,
        FakeDiscoveryService(discoveries),  # type: ignore[arg-type]
        AgentPackService(settings),
        supervisor,
    )
    campaign_service = ShadowCampaignService(
        settings,
        session_factory,
        FakeDiscoveryService(discoveries),  # type: ignore[arg-type]
        FakeResearchCoordinator(),  # type: ignore[arg-type]
        shadow_service,
    )

    results = await campaign_service.run(ShadowCampaignRequest(limit=2, reason="test_shadow_campaign"))

    async with session_factory() as session:
        repo = PlatformRepository(session)
        campaigns = await repo.list_room_campaigns(limit=10)
        await session.commit()

    assert len(results) == 2
    assert {result.market_ticker for result in results} == {"WX-B", "WX-C"}
    assert len(supervisor.calls) == 2
    assert any(campaign.payload.get("market_ticker") == "WX-B" for campaign in campaigns)
    assert any(campaign.payload.get("market_ticker") == "WX-C" for campaign in campaigns)

    await engine.dispose()
