from __future__ import annotations

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import AgentPack, AgentPackThresholds
from kalshi_bot.services.agent_packs import AgentPackService


def test_agent_pack_service_builds_gemini_first_default_pack() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)

    pack = service.default_pack()

    assert pack.version == settings.active_agent_pack_version
    assert pack.roles[AgentRole.RESEARCHER.value].provider == "gemini"
    assert pack.roles[AgentRole.TRADER.value].model == settings.gemini_model_trader


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

    assert sanitized.thresholds.risk_min_edge_bps == 500
    assert sanitized.thresholds.risk_max_order_notional_dollars == 250.0
    assert sanitized.thresholds.risk_max_position_notional_dollars == 25.0
    assert sanitized.thresholds.trigger_max_spread_bps == 50
    assert sanitized.thresholds.trigger_cooldown_seconds == 3600


def test_agent_pack_service_clamps_role_temperature() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = AgentPackService(settings)
    payload = service.default_pack().model_dump(mode="json")
    payload["version"] = "candidate-temp-test"
    payload["roles"][AgentRole.RESEARCHER.value]["temperature"] = 9.0

    sanitized = service.sanitize_candidate_pack(AgentPack(**payload), parent_version="builtin")

    assert sanitized.roles[AgentRole.RESEARCHER.value].temperature == 1.0
