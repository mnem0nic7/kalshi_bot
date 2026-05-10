from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.autonomous_gate_tuning import AutonomousGateTuningService
from kalshi_bot.services.gate_learning import GateLearningRow


def _recommendation(settings: Settings) -> dict[str, Any]:
    recommended_settings = {
        "risk_min_contract_price_dollars": {
            "current": settings.risk_min_contract_price_dollars,
            "recommended": 0.05,
            "changed": True,
            "candidate_policy": "risk_min_contract_price_dollars=0.05",
            "reason": "promoted_by_walk_forward_pnl_and_drawdown",
        }
    }
    for field in (
        "strategy_min_remaining_payout_bps",
        "trigger_max_spread_bps",
        "risk_min_confidence",
        "risk_min_edge_bps",
        "strategy_min_abs_delta_f",
        "risk_max_credible_edge_bps",
    ):
        recommended_settings[field] = {
            "current": getattr(settings, field),
            "recommended": getattr(settings, field),
            "changed": False,
            "candidate_policy": None,
            "reason": "holdout_net_pnl_not_improved",
        }
    return {
        "schema_version": "gate-learning-recommendations-v1",
        "source": "combined",
        "source_files": {"historical": ["fixture.jsonl"]},
        "row_counts": {"total_rows": 40, "labeled_rows": 40, "train_rows": 30, "holdout_rows": 10},
        "recommended_settings": recommended_settings,
        "confidence_warnings": [],
    }


class FakeGateLearningService:
    rows: list[GateLearningRow] = []

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def build_recommendation_report(self, **_kwargs: Any) -> dict[str, Any]:
        return _recommendation(self.settings)

    def load_bundle_rows(self, *, source: str) -> list[GateLearningRow]:
        return list(self.rows)


async def _passing_backtesting(**_kwargs: Any) -> dict[str, Any]:
    return {"status": "pass", "dataset": {"row_count": 40}, "issues": [], "promotion_gates": {"status": "pass"}}


async def _passing_modeling(**_kwargs: Any) -> dict[str, Any]:
    return {"status": "pass", "dataset": {"row_count": 40}, "issues": []}


async def _failing_modeling(**_kwargs: Any) -> dict[str, Any]:
    return {"status": "fail", "dataset": {"row_count": 40}, "issues": [{"severity": "fail", "code": "bad"}]}


async def _service(tmp_path, *, modeling_builder=_passing_modeling):
    FakeGateLearningService.rows = []
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/autonomous-gates.db",
        autonomous_gate_tuning_canary_min_settled_rows=1,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    agent_pack_service = AgentPackService(settings)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(settings.app_color)
        await agent_pack_service.ensure_initialized(repo)
        await session.commit()
    service = AutonomousGateTuningService(
        settings=settings,
        session_factory=session_factory,
        agent_pack_service=agent_pack_service,
        decision_corpus_service=None,
        trade_analysis_service=None,
        trading_audit_service=None,
        backtesting_builder=_passing_backtesting,
        modeling_builder=modeling_builder,
        gate_learning_service_factory=FakeGateLearningService,
    )
    return settings, engine, session_factory, agent_pack_service, service


@pytest.mark.asyncio
async def test_autonomous_gate_tuning_dry_run_does_not_stage(tmp_path) -> None:
    _settings, engine, _session_factory, _agent_pack_service, service = await _service(tmp_path)

    result = await service.run(dry_run=True, now=datetime(2026, 5, 10, tzinfo=UTC))
    status = await service.status()

    assert result["status"] == "dry_run"
    assert result["changes"]["risk_min_contract_price_dollars"]["recommended"] == 0.05
    assert status["status"] == "not_started"

    await engine.dispose()


@pytest.mark.asyncio
async def test_autonomous_gate_tuning_stages_candidate_pack(tmp_path) -> None:
    _settings, engine, session_factory, _agent_pack_service, service = await _service(tmp_path)

    result = await service.run(now=datetime(2026, 5, 10, tzinfo=UTC))

    async with session_factory() as session:
        repo = PlatformRepository(session)
        candidate = await repo.get_agent_pack(result["candidate_version"])
        checkpoint = await repo.get_checkpoint("autonomous_gate_tuning:demo")
        await session.commit()

    assert result["status"] == "staged"
    assert candidate is not None
    assert candidate.status == "staged"
    assert candidate.payload["thresholds"]["risk_min_contract_price_dollars"] == 0.05
    assert checkpoint is not None
    assert checkpoint.payload["status"] == "staged"

    await engine.dispose()


@pytest.mark.asyncio
async def test_autonomous_gate_tuning_promotes_after_canary_passes(tmp_path) -> None:
    settings, engine, session_factory, agent_pack_service, service = await _service(tmp_path)
    staged_at = datetime(2026, 5, 10, tzinfo=UTC)
    staged = await service.run(now=staged_at)
    FakeGateLearningService.rows = [
        GateLearningRow(
            source="fixture",
            room_id="room-1",
            market_ticker="KXHIGHNY-26MAY10-T70",
            decision_time=staged_at + timedelta(minutes=5),
            market_day="2026-05-10",
            side="yes",
            entry_price=Decimal("0.10"),
            remaining_payout_bps=9000,
            spread_bps=100,
            confidence=0.90,
            edge_bps=2000,
            quality_adjusted_edge_bps=2000,
            forecast_delta_f=9.0,
            counterfactual_pnl_dollars=Decimal("0.90"),
            settlement_ts=staged_at + timedelta(hours=1),
        )
    ]

    promoted = await service.run(now=staged_at + timedelta(hours=2))

    async with session_factory() as session:
        repo = PlatformRepository(session)
        active_pack = await agent_pack_service.get_pack_for_color(repo, settings.app_color)
        candidate = await repo.get_agent_pack(staged["candidate_version"])
        await session.commit()

    assert promoted["status"] == "promoted"
    assert active_pack.version == staged["candidate_version"]
    assert active_pack.thresholds.risk_min_contract_price_dollars == 0.05
    assert candidate is not None
    assert candidate.status == "champion"

    FakeGateLearningService.rows = []
    await engine.dispose()


@pytest.mark.asyncio
async def test_autonomous_gate_tuning_validation_failure_does_not_stage(tmp_path) -> None:
    _settings, engine, _session_factory, _agent_pack_service, service = await _service(
        tmp_path,
        modeling_builder=_failing_modeling,
    )

    result = await service.run(now=datetime(2026, 5, 10, tzinfo=UTC))

    assert result["status"] == "validation_failed"
    assert "modeling:bad" in result["validation"]["failures"]

    await engine.dispose()
