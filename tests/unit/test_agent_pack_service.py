from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import AgentPack, AgentPackCryptoEntryPolicy, AgentPackCryptoPolicy, AgentPackThresholds
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.agent_packs import AgentPackService


def test_agent_pack_service_builds_deterministic_default_pack() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)

    pack = service.default_pack()

    assert pack.version == settings.active_agent_pack_version
    assert pack.roles[AgentRole.RESEARCHER.value].provider == "none"
    assert pack.roles[AgentRole.TRADER.value].model is None
    assert pack.thresholds.risk_min_contract_price_dollars == settings.risk_min_contract_price_dollars
    assert pack.thresholds.risk_min_confidence == settings.risk_min_confidence
    assert pack.thresholds.strategy_min_abs_delta_f == settings.strategy_min_abs_delta_f
    assert pack.thresholds.risk_max_credible_edge_bps == settings.risk_max_credible_edge_bps
    assert pack.crypto_policy.entry.min_fee_adjusted_edge_bps == settings.risk_min_edge_bps
    assert pack.crypto_policy.replay.min_resolved_markets == settings.crypto_replay_min_resolved_markets
    assert pack.crypto_policy.live.trading_enabled == settings.crypto_trading_enabled


def test_agent_pack_runtime_thresholds_override_all_tunable_gates() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)
    pack = service.default_pack().model_copy(
        update={
            "thresholds": AgentPackThresholds(
                risk_min_edge_bps=1500,
                risk_max_credible_edge_bps=7500,
                risk_min_confidence=0.70,
                risk_min_contract_price_dollars=0.05,
                trigger_max_spread_bps=500,
                strategy_min_abs_delta_f=2.0,
                strategy_min_remaining_payout_bps=1000,
            )
        }
    )

    thresholds = service.runtime_thresholds(pack)

    assert thresholds.risk_min_edge_bps == 1500
    assert thresholds.risk_max_credible_edge_bps == 7500
    assert thresholds.risk_min_confidence == 0.70
    assert thresholds.risk_min_contract_price_dollars == 0.05
    assert thresholds.trigger_max_spread_bps == 500
    assert thresholds.strategy_min_abs_delta_f == 2.0
    assert thresholds.strategy_min_remaining_payout_bps == 1000


def test_agent_pack_runtime_crypto_policy_overrides_per_asset() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)
    pack = service.default_pack().model_copy(
        update={
            "crypto_policy": AgentPackCryptoPolicy(
                entry=AgentPackCryptoEntryPolicy(
                    min_fee_adjusted_edge_bps=1000,
                    max_spread_bps=800,
                    min_confidence=0.75,
                    min_contract_price_dollars=0.08,
                    min_remaining_payout_bps=1500,
                    max_credible_edge_bps=7500,
                ),
                asset_entry_overrides={
                    "btc": AgentPackCryptoEntryPolicy(
                        min_fee_adjusted_edge_bps=1500,
                        max_spread_bps=250,
                    )
                },
            )
        }
    )

    policy = service.runtime_crypto_policy(pack)
    thresholds = service.runtime_crypto_thresholds(policy, asset_symbol="BTC")

    assert policy.entry_for_asset("eth")["min_fee_adjusted_edge_bps"] == 1000
    assert policy.entry_for_asset("BTC")["min_fee_adjusted_edge_bps"] == 1500
    assert policy.entry_for_asset("BTC")["max_spread_bps"] == 250
    assert thresholds.risk_min_edge_bps == 1500
    assert thresholds.trigger_max_spread_bps == 250
    assert thresholds.strategy_min_remaining_payout_bps == 1500


def test_agent_pack_service_sanitizes_mutable_threshold_bounds() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)
    payload = service.default_pack().model_dump(mode="json")
    payload["version"] = "candidate-test"
    payload["thresholds"] = AgentPackThresholds(
        risk_min_edge_bps=9999,
        risk_max_order_notional_dollars=9999,
        risk_max_position_notional_dollars=1,
        trigger_max_spread_bps=1,
        trigger_cooldown_seconds=99999,
    )
    candidate = AgentPack(**payload)

    sanitized = service.sanitize_candidate_pack(candidate, parent_version="builtin")

    assert sanitized.thresholds.risk_min_edge_bps == 5000
    assert sanitized.thresholds.risk_max_order_notional_dollars == 250.0
    assert sanitized.thresholds.risk_max_position_notional_dollars == 25.0
    assert sanitized.thresholds.trigger_max_spread_bps == 50
    assert sanitized.thresholds.trigger_cooldown_seconds == 3600


def test_agent_pack_service_sanitizes_crypto_policy_bounds() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)
    payload = service.default_pack().model_dump(mode="json")
    payload["version"] = "candidate-crypto-test"
    payload["crypto_policy"]["entry"] = {
        "min_fee_adjusted_edge_bps": 9999,
        "max_spread_bps": 1,
        "min_confidence": 0.01,
        "min_contract_price_dollars": 0.001,
        "min_remaining_payout_bps": 1,
        "max_credible_edge_bps": 99999,
    }
    payload["crypto_policy"]["live"]["asset_modes"] = {"btc": "live", "eth": "nonsense"}
    payload["crypto_policy"]["asset_entry_overrides"] = {
        "btc": {"min_fee_adjusted_edge_bps": 9999, "max_spread_bps": 9999}
    }

    sanitized = service.sanitize_candidate_pack(AgentPack(**payload), parent_version="builtin")

    assert sanitized.crypto_policy.entry.min_fee_adjusted_edge_bps == 5000
    assert sanitized.crypto_policy.entry.max_spread_bps == 50
    assert sanitized.crypto_policy.entry.min_confidence == 0.50
    assert sanitized.crypto_policy.entry.min_contract_price_dollars == 0.01
    assert sanitized.crypto_policy.entry.min_remaining_payout_bps == 100
    assert sanitized.crypto_policy.entry.max_credible_edge_bps == 10000
    assert sanitized.crypto_policy.live.asset_modes == {"BTC": "live", "ETH": "shadow"}
    assert sanitized.crypto_policy.asset_entry_overrides["BTC"].max_spread_bps == 2500


def test_agent_pack_service_clamps_role_temperature() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)
    payload = service.default_pack().model_dump(mode="json")
    payload["version"] = "candidate-temp-test"
    payload["roles"][AgentRole.RESEARCHER.value]["temperature"] = 9.0

    sanitized = service.sanitize_candidate_pack(AgentPack(**payload), parent_version="builtin")

    assert sanitized.roles[AgentRole.RESEARCHER.value].temperature == 1.0


@pytest.mark.asyncio
async def test_agent_pack_service_migrates_legacy_builtin_notes_to_deterministic(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/agent-packs.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = AgentPackService(settings)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.ensure_deployment_control(settings.app_color)
        control.notes = {
            "agent_packs": {
                "blue_version": "builtin-gemini-v1",
                "green_version": "autonomous-pack-v2",
                "champion_version": "builtin-gemini-v1",
                "active_version": "builtin-gemini-v1",
            }
        }
        await service.ensure_initialized(repo)
        migrated = await repo.get_deployment_control()
        await session.commit()

    notes = migrated.notes["agent_packs"]
    assert notes["blue_version"] == "builtin-deterministic-v1"
    assert notes["champion_version"] == "builtin-deterministic-v1"
    assert notes["active_version"] == "builtin-deterministic-v1"
    assert notes["green_version"] == "autonomous-pack-v2"

    await engine.dispose()
