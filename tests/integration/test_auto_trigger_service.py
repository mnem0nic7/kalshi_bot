from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason, WeatherResolutionState
from kalshi_bot.core.schemas import ResearchDossier, ResearchFreshness, ResearchGateVerdict, ResearchSummary, ResearchTraderContext
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.auto_trigger import AutoTriggerService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping

_GOOD_SNAPSHOT = {
    "market_ticker": "WX-TEST",
    "market": {
        "yes_bid_dollars": "0.4400",
        "yes_ask_dollars": "0.4800",
        "no_ask_dollars": "0.5600",
    },
}


class FakeSupervisor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def run_room(self, room_id: str, reason: str = "manual") -> None:
        self.calls.append((room_id, reason))


def _directory() -> WeatherMarketDirectory:
    return WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )


def _resolved_research_dossier(*, now: datetime) -> ResearchDossier:
    return ResearchDossier(
        market_ticker="WX-TEST",
        status="ready",
        mode="structured",
        summary=ResearchSummary(
            narrative="Contract is locked.",
            bullish_case="Current temp crossed threshold.",
            bearish_case="None.",
            unresolved_uncertainties=[],
            settlement_mechanics="NWS station observation",
            current_numeric_facts={
                "current_temp_f": 84.0,
                "threshold_f": 80.0,
                "resolution_state": WeatherResolutionState.LOCKED_YES.value,
            },
            source_coverage="structured weather",
            research_confidence=1.0,
        ),
        freshness=ResearchFreshness(
            refreshed_at=now,
            expires_at=now + timedelta(minutes=10),
            stale=False,
            max_source_age_seconds=0,
        ),
        trader_context=ResearchTraderContext(
            fair_yes_dollars="1.0000",
            confidence=1.0,
            thesis="Current observed temperature has already crossed the threshold.",
            structured_source_used=True,
            autonomous_ready=True,
            resolution_state=WeatherResolutionState.LOCKED_YES,
        ),
        gate=ResearchGateVerdict(
            passed=True,
            reasons=["Research gate passed."],
            cited_source_keys=[],
        ),
        settlement_covered=True,
    )


@pytest.mark.asyncio
async def test_auto_trigger_creates_one_room_for_actionable_market(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
        trigger_max_spread_bps=1200,
        trigger_max_concurrent_rooms=4,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = _directory()
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()
    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        checkpoint = await repo.get_checkpoint("auto_trigger:demo:WX-TEST")
        await session.commit()

    assert len(rooms) == 1
    assert checkpoint is not None
    assert len(supervisor.calls) == 1
    assert supervisor.calls[0][1] == "auto_trigger"

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_skips_fresh_resolved_research_before_room_creation(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_resolved_research.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
        trigger_max_spread_bps=1200,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    service = AutoTriggerService(settings, session_factory, _directory(), agent_pack_service, supervisor)
    now = datetime.now(UTC)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.upsert_research_dossier(_resolved_research_dossier(now=now))
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        checkpoint = await repo.get_checkpoint("auto_trigger_block:demo:WX-TEST:resolved_contract")
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert checkpoint is not None
    assert checkpoint.payload["reason"] == "resolved_contract"
    assert checkpoint.payload["resolution_state"] == "locked_yes"
    assert any("latest research says contract is resolved" in event.summary for event in ops_events)

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_skips_terminal_market_lifecycle_before_room_creation(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_terminal_lifecycle.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
        trigger_max_spread_bps=1200,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    service = AutoTriggerService(settings, session_factory, _directory(), agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot={**_GOOD_SNAPSHOT, "lifecycle": {"status": "closed", "result": "yes"}},
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        checkpoint = await repo.get_checkpoint("auto_trigger_block:demo:WX-TEST:terminal_market")
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert checkpoint is not None
    assert checkpoint.payload["reason"] == "terminal_market"
    assert checkpoint.payload["lifecycle_status"] == "closed"
    assert any("market lifecycle is terminal" in event.summary for event in ops_events)

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_skips_when_color_is_inactive(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_inactive.db",
        trigger_enable_auto_rooms=True,
        app_color="blue",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control("green")
        control.active_color = "green"
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_uses_settings_kalshi_env_for_control_state(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_prod_env.db",
        kalshi_env="production",
        trigger_enable_auto_rooms=True,
        app_color="blue",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("green", kalshi_env="demo", initial_active_color="green")
        await repo.ensure_deployment_control("blue", kalshi_env="production", initial_active_color="blue")
        await repo.upsert_market_state(
            "WX-TEST",
            kalshi_env="production",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        checkpoint = await repo.get_checkpoint("auto_trigger:production:WX-TEST")
        await session.commit()

    assert len(rooms) == 1
    assert rooms[0].kalshi_env == "production"
    assert checkpoint is not None
    assert len(supervisor.calls) == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_blocks_when_live_position_exists(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_position_block.db",
        trigger_enable_auto_rooms=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.upsert_position(
            market_ticker="WX-TEST",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("12.00"),
            average_price_dollars=Decimal("0.4100"),
            raw={},
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert any("live position already open" in event.summary for event in ops_events)

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_throttles_repeated_live_position_block_logs(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_position_block_throttle.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.upsert_position(
            market_ticker="WX-TEST",
            subaccount=settings.kalshi_subaccount,
            kalshi_env=settings.kalshi_env,
            side="yes",
            count_fp=Decimal("12.00"),
            average_price_dollars=Decimal("0.4100"),
            raw={},
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        checkpoint = await repo.get_checkpoint("auto_trigger_block:demo:WX-TEST:open_position_governance")
        await session.commit()

    matching = [event for event in ops_events if "live position already open" in event.summary]
    assert len(matching) == 1
    assert checkpoint is not None
    assert supervisor.calls == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_suppresses_recent_identical_risk_block_unless_price_moves(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_recent_risk_block.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
        trigger_price_move_bypass_bps=50,
        trigger_max_spread_bps=1200,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    service = AutoTriggerService(settings, session_factory, _directory(), agent_pack_service, supervisor)
    blocked_at = datetime.now(UTC) - timedelta(seconds=1000)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.set_checkpoint(
            "auto_trigger:demo:WX-TEST",
            cursor=None,
            payload={
                "last_triggered_at": blocked_at.isoformat(),
                "last_trigger_mid": "0.4600",
            },
        )
        await repo.save_decision_trace(
            room_id=None,
            ticket_id=None,
            market_ticker="WX-TEST",
            kalshi_env=settings.kalshi_env,
            decision_kind="risk_block",
            path_version="deterministic",
            source_snapshot_ids={},
            input_hash="input",
            trace_hash="trace",
            decision_time=blocked_at,
            trace={
                "decision_kind": "risk_block",
                "final_status": "blocked",
                "ticket": {"side": "no", "yes_price_dollars": "0.4800"},
                "risk": {"status": "blocked", "reasons": ["Edge exceeds credibility limit."]},
                "candidate_trace": {"selected_side": "no"},
            },
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        block_cp = await repo.get_checkpoint("auto_trigger_block:demo:WX-TEST:recent_risk_block")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4600",  # type: ignore[arg-type]
            yes_ask_dollars="0.5000",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert block_cp is not None
    assert any("recent risk block" in event.summary for event in ops_events)

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        await session.commit()

    assert len(rooms) == 1
    assert len(supervisor.calls) == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_suppresses_recent_extreme_edge_diagnostic(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_extreme_edge_diag.db",
        trigger_enable_auto_rooms=True,
        trigger_cooldown_seconds=600,
        trigger_price_move_bypass_bps=50,
        trigger_max_spread_bps=1200,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    service = AutoTriggerService(settings, session_factory, _directory(), agent_pack_service, supervisor)
    blocked_at = datetime.now(UTC) - timedelta(seconds=1000)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.set_checkpoint(
            "auto_trigger:demo:WX-TEST",
            cursor=None,
            payload={
                "last_triggered_at": blocked_at.isoformat(),
                "last_trigger_mid": "0.4600",
            },
        )
        await repo.save_decision_trace(
            room_id=None,
            ticket_id=None,
            market_ticker="WX-TEST",
            kalshi_env=settings.kalshi_env,
            decision_kind="stand_down",
            path_version="deterministic",
            source_snapshot_ids={},
            input_hash="input",
            trace_hash="trace",
            decision_time=blocked_at,
            trace={
                "decision_kind": "stand_down",
                "final_status": "stand_down",
                "candidate_trace": {
                    "eligibility_stand_down_reason": StandDownReason.EXTREME_EDGE_DIAGNOSTIC_FAILED.value,
                    "extreme_edge_diagnostic": {
                        "passed": False,
                        "reason_codes": ["station_daily_high_source_disagreement"],
                    },
                },
                "normalized_intent": {
                    "stand_down_reason": StandDownReason.EXTREME_EDGE_DIAGNOSTIC_FAILED.value,
                },
            },
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        block_cp = await repo.get_checkpoint("auto_trigger_block:demo:WX-TEST:recent_extreme_edge_diagnostic")
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert block_cp is not None
    assert any("recent extreme-edge diagnostic" in event.summary for event in ops_events)

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_blocks_when_stop_loss_is_unresolved(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_stop_loss_block.db",
        trigger_enable_auto_rooms=True,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.set_checkpoint(
            "stop_loss_submit:demo:WX-TEST",
            cursor=None,
            payload={
                "submitted_at": "2026-04-22T21:00:00+00:00",
                "stopped_at": "2026-04-22T21:00:00+00:00",
                "stopped_side": "yes",
                "submit_error": "submit failed",
                "outcome_status": "submit_failed",
            },
        )
        await repo.set_checkpoint(
            "stop_loss_reentry:demo:WX-TEST",
            cursor=None,
            payload={
                "stopped_at": "2026-04-22T21:00:00+00:00",
                "stopped_side": "yes",
                "outcome_status": "submit_failed",
                "reverse_evaluated": False,
            },
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert any("stop-loss still unresolved" in event.summary for event in ops_events)

    await engine.dispose()


@pytest.mark.asyncio
async def test_auto_trigger_blocks_filled_stop_loss_reentry_during_cooldown(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auto_trigger_stop_loss_reentry_cooldown.db",
        trigger_enable_auto_rooms=True,
        stop_loss_reentry_cooldown_seconds=4 * 60 * 60,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    supervisor = FakeSupervisor()
    agent_pack_service = AgentPackService(settings)
    directory = WeatherMarketDirectory(
        {
            "WX-TEST": WeatherMarketMapping(
                market_ticker="WX-TEST",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
                threshold_f=80,
            )
        }
    )
    service = AutoTriggerService(settings, session_factory, directory, agent_pack_service, supervisor)

    stopped_at = datetime.now(UTC) - timedelta(minutes=12)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control("blue")
        await repo.upsert_market_state(
            "WX-TEST",
            snapshot=_GOOD_SNAPSHOT,
            yes_bid_dollars="0.4400",  # type: ignore[arg-type]
            yes_ask_dollars="0.4800",  # type: ignore[arg-type]
            last_trade_dollars=None,
        )
        await repo.set_checkpoint(
            "stop_loss_reentry:demo:WX-TEST",
            cursor=None,
            payload={
                "stopped_at": stopped_at.isoformat(),
                "stopped_side": "no",
                "outcome_status": "filled_exit",
                "reverse_evaluated": False,
            },
        )
        await session.commit()

    await service.handle_market_update("WX-TEST")
    await service.wait_for_tasks()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms(limit=10)
        ops_events = await repo.list_ops_events(limit=10, kalshi_env=settings.kalshi_env)
        reentry_cp = await repo.get_checkpoint("stop_loss_reentry:demo:WX-TEST")
        block_cp = await repo.get_checkpoint("auto_trigger_block:demo:WX-TEST:stop_loss_reentry_cooldown")
        await session.commit()

    assert rooms == []
    assert supervisor.calls == []
    assert reentry_cp is not None
    assert reentry_cp.payload["reverse_evaluated"] is False
    assert block_cp is not None
    assert any("stop-loss re-entry cooldown active" in event.summary for event in ops_events)

    await engine.dispose()
