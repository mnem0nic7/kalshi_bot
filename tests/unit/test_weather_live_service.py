from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.weather_live import WeatherLiveService


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)


class FakeWatchdog:
    async def daemon_health(self, repo, *, color: str, kalshi_env: str | None = None):
        return {
            "kalshi_env": kalshi_env,
            "color": color,
            "healthy": True,
            "reason": "heartbeat fresh",
            "heartbeat_at": NOW.isoformat(),
            "heartbeat_age_seconds": 0,
            "last_reconcile_at": NOW.isoformat(),
            "last_reconcile_age_seconds": 0,
            "threshold_seconds": 60,
        }


class FakeWeatherPrediction:
    async def status(self, *, kalshi_env: str):
        return {
            "kalshi_env": kalshi_env,
            "intraday_model_usable": True,
            "intraday_model": {"active": True, "version": "intraday-test"},
        }


class FakeAudit:
    async def build_report(self, *, kalshi_env: str, days: int, focus: str, now: datetime):
        return {
            "domains": {
                "weather": {
                    "orders": 0,
                    "fills": 0,
                    "positions": 0,
                    "pnl": {"gross_pnl_dollars": "0.0000", "net_pnl_dollars": None},
                    "top_blockers": [],
                }
            }
        }


@pytest.fixture
async def weather_live_harness(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/weather-live.db",
        kalshi_env="production",
        app_shadow_mode=False,
        weather_intraday_model_enabled=True,
        daemon_reconcile_stale_kill_switch_seconds=300,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    agent_pack_service = AgentPackService(settings)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.ensure_deployment_control("blue", kalshi_env="production", initial_active_color="blue")
        await agent_pack_service.ensure_initialized(repo)
        await repo.set_checkpoint(
            "daemon_reconcile:production:blue",
            cursor=None,
            payload={"reconciled_at": NOW.isoformat()},
        )
        await session.commit()

    service = WeatherLiveService(
        settings=settings,
        session_factory=session_factory,
        agent_pack_service=agent_pack_service,
        watchdog_service=FakeWatchdog(),
        weather_prediction_service=FakeWeatherPrediction(),
        trading_audit_service=FakeAudit(),
        has_write_credentials=True,
    )
    yield settings, session_factory, agent_pack_service, service
    await engine.dispose()


@pytest.mark.asyncio
async def test_weather_live_activate_stages_cold_bootstrap_policy(weather_live_harness) -> None:
    _settings, session_factory, _agent_pack_service, service = weather_live_harness

    result = await service.activate(kalshi_env="production", actor="pytest", now=NOW)

    assert result["activated"] is True
    assert result["live_capable"] is True
    assert result["deployment"]["weather_live"]["enabled"] is True
    assert result["deployment"]["weather_live"]["daily_realized_loss_cap_pct"] == 0.02
    assert result["deployment"]["weather_live"]["max_order_count_fp"] == 200
    policy = result["active_weather_policy"]
    assert policy["mode"] == "live"
    assert policy["bootstrap"]["tiers"]["cold"]["live_enabled"] is True
    assert policy["bootstrap"]["tiers"]["cold"]["min_confidence"] == 0.90
    assert policy["bootstrap"]["tiers"]["cold"]["min_edge_bps"] == 3000
    assert policy["bootstrap"]["tiers"]["warming"]["live_enabled"] is False
    assert policy["bootstrap"]["tiers"]["maturing"]["live_enabled"] is False
    assert policy["threshold_consistency"]["consistent"] is True

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        control = await repo.get_deployment_control(kalshi_env="production")
        assert control.notes["weather_live"]["active_policy_pack_version"].startswith("weather-full-live-")


@pytest.mark.asyncio
async def test_weather_live_auto_enables_close_strike_probe_policy(weather_live_harness) -> None:
    _settings, session_factory, _agent_pack_service, service = weather_live_harness
    report = {
        "unlock": {"passed": True, "reason_codes": ["close_strike_probe_unlocked"]},
        "candidate_count": 5,
        "settled_count": 5,
        "directional_accuracy": 0.80,
        "ask_net_pnl_dollars": "2.5000",
        "generated_at": NOW.isoformat(),
    }

    result = await service.auto_enable_close_strike_probes(
        kalshi_env="production",
        actor="pytest",
        evidence_report=report,
        now=NOW,
    )

    assert result["enabled"] is True
    assert result["policy_pack_version"].startswith("weather-close-strike-probe-")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        control = await repo.get_deployment_control(kalshi_env="production")
        active_pack = await _agent_pack_service.get_active_pack(repo)
        await session.commit()

    assert control.notes["weather_live"]["bootstrap"]["max_order_notional_usd"] == 0.25
    assert control.notes["weather_live"]["bootstrap"]["cooldown_seconds"] == 3600
    policy = active_pack.weather_policy.policies["weather/a/global/any/any/all_season/all_month/any/any/entry_gate"]
    close_strike = policy.bootstrap.local_overrides["close_strike_probe"]
    assert close_strike["enabled"] is True
    assert close_strike["tiers"][2]["min_confidence"] == 0.60


@pytest.mark.asyncio
async def test_weather_live_status_blocks_on_kill_switch(weather_live_harness) -> None:
    _settings, session_factory, _agent_pack_service, service = weather_live_harness
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.set_kill_switch(True, kalshi_env="production", source="test", reason="unit")
        await session.commit()

    result = await service.status(kalshi_env="production", now=NOW)

    assert result["live_capable"] is False
    assert any(blocker["code"] == "kill_switch_enabled" for blocker in result["blockers"])


@pytest.mark.asyncio
async def test_weather_live_rollback_sets_kill_switch_and_disables_policy(weather_live_harness) -> None:
    _settings, session_factory, _agent_pack_service, service = weather_live_harness
    await service.activate(kalshi_env="production", actor="pytest", now=NOW)

    result = await service.rollback(
        kalshi_env="production",
        actor="pytest",
        reason="unit_rollback",
        now=NOW,
    )

    assert result["rolled_back"] is True
    assert result["deployment"]["kill_switch_enabled"] is True
    assert result["deployment"]["weather_live"]["enabled"] is False
    assert result["deployment"]["weather_live"]["policy_state"] == "rollback"

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        control = await repo.get_deployment_control(kalshi_env="production")
        assert control.kill_switch_enabled is True
        assert control.notes["weather_live"]["rollback_reason"] == "unit_rollback"
        active_pack = await _agent_pack_service.get_active_pack(repo)
        rollback_policy = next(
            policy
            for policy in active_pack.weather_policy.policies.values()
            if policy.strategy_code == "A" and policy.lane == "entry_gate"
        )
        assert rollback_policy.mode == "paused"
        assert all(tier.live_enabled is False for tier in rollback_policy.bootstrap.tiers.values())
