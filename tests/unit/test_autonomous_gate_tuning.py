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


async def _seed_live_canary_row(
    session_factory,
    *,
    settings: Settings,
    observed_at: datetime,
) -> None:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        build = await repo.create_decision_corpus_build(
            version="canary-corpus",
            date_from=observed_at.date(),
            date_to=observed_at.date(),
            source={"kind": "unit"},
            filters={},
        )
        await repo.add_decision_corpus_row(
            corpus_build_id=build.id,
            room_id="room-1",
            market_ticker="KXHIGHNY-26MAY10-T70",
            series_ticker="KXHIGHNY",
            local_market_day="2026-05-10",
            checkpoint_ts=observed_at,
            kalshi_env=settings.kalshi_env,
            deployment_color=settings.app_color,
            model_version="unit",
            policy_version="unit",
            fair_yes_dollars=Decimal("0.3000"),
            confidence=0.90,
            edge_bps=2000,
            recommended_side="yes",
            target_yes_price_dollars=Decimal("0.1000"),
            eligibility_status="eligible",
            support_status="supported",
            support_level="L5_global",
            support_n=40,
            support_market_days=40,
            settlement_result="yes",
            settlement_value_dollars=Decimal("1.0000"),
            pnl_counterfactual_target_frictionless=Decimal("0.9000"),
            pnl_counterfactual_target_with_fees=Decimal("0.9000"),
            source_provenance="historical_replay_full_checkpoint",
            signal_payload={
                "forecast_delta_f": 9.0,
                "confidence": 0.90,
                "candidate_trace": {
                    "selected_side": "yes",
                    "selected_candidate": {
                        "side": "yes",
                        "target_yes_price_dollars": "0.1000",
                        "quality_adjusted_edge_bps": 2000,
                        "edge_bps": 2000,
                        "remaining_payout_dollars": "0.9000",
                        "spread_bps": 100,
                    },
                },
            },
            quote_snapshot={"yes_bid_dollars": "0.09", "yes_ask_dollars": "0.10"},
            diagnostics={"forecast_delta_f": 9.0},
            created_at=observed_at,
        )
        await repo.mark_decision_corpus_build_successful(build.id, row_count=1)
        await repo.promote_decision_corpus_build(build.id, kalshi_env=settings.kalshi_env, actor="unit")
        await session.commit()


async def _passing_backtesting(**_kwargs: Any) -> dict[str, Any]:
    return {"status": "pass", "dataset": {"row_count": 40}, "issues": [], "promotion_gates": {"status": "pass"}}


async def _passing_modeling(**_kwargs: Any) -> dict[str, Any]:
    return {"status": "pass", "dataset": {"row_count": 40}, "issues": []}


async def _failing_modeling(**_kwargs: Any) -> dict[str, Any]:
    return {"status": "fail", "dataset": {"row_count": 40}, "issues": [{"severity": "fail", "code": "bad"}]}


async def _freeze_only_failing_modeling(**_kwargs: Any) -> dict[str, Any]:
    return {
        "status": "fail",
        "dataset": {"row_count": 40},
        "issues": [{"severity": "fail", "code": "production_entry_freeze_disabled"}],
    }


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
    assert status["llm_calls_enabled"] is False
    assert status["deterministic_runtime"] is True
    assert status["active_pack_version"] == "builtin-deterministic-v1"
    assert status["active_thresholds"]["risk_min_edge_bps"] == status["settings_thresholds"]["risk_min_edge_bps"]

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
    await _seed_live_canary_row(session_factory, settings=settings, observed_at=staged_at + timedelta(hours=1))

    promoted = await service.run(now=staged_at + timedelta(hours=2))

    async with session_factory() as session:
        repo = PlatformRepository(session)
        active_pack = await agent_pack_service.get_pack_for_color(repo, settings.app_color)
        candidate = await repo.get_agent_pack(staged["candidate_version"])
        await session.commit()

    assert promoted["status"] == "promoted"
    assert promoted["canary"]["evidence_source"] == "live_decision_corpus"
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


@pytest.mark.asyncio
async def test_autonomous_gate_tuning_validation_ignores_production_entry_freeze(tmp_path) -> None:
    _settings, engine, _session_factory, _agent_pack_service, service = await _service(
        tmp_path,
        modeling_builder=_freeze_only_failing_modeling,
    )

    result = await service.run(now=datetime(2026, 5, 10, tzinfo=UTC))

    assert result["status"] == "staged"
    assert result["validation"]["failures"] == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_autonomous_crypto_gate_tuning_stages_per_asset_policy(tmp_path, monkeypatch) -> None:
    _settings, engine, session_factory, _agent_pack_service, service = await _service(tmp_path)

    async def fake_crypto_recommendation(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "schema_version": "crypto-autonomous-gate-recommendations-v1",
            "kalshi_env": "demo",
            "domain": "crypto",
            "window_days": 3650,
            "min_support": 1,
            "row_counts": {
                "snapshot_rows": 20,
                "decision_rows": 20,
                "labeled_rows": 20,
                "assets": ["BTC", "ETH"],
            },
            "current_policy": {},
            "asset_diagnostics": [],
            "promoted_assets": {
                "BTC": {
                    "promotion_status": "promoted",
                    "candidate_policy": "crypto.BTC.min_fee_adjusted_edge_bps=1000",
                    "entry": {
                        "min_fee_adjusted_edge_bps": 1000,
                        "max_spread_bps": 250,
                        "min_confidence": 0.75,
                        "min_contract_price_dollars": 0.05,
                        "min_remaining_payout_bps": 1000,
                        "max_credible_edge_bps": 7500,
                    },
                    "current_score": {"selected_count": 1, "net_pnl": "0.1000", "drawdown_proxy": "0.0000"},
                    "candidate_score": {"selected_count": 2, "net_pnl": "1.0000", "drawdown_proxy": "0.0000"},
                    "promotion_reason": "walk_forward_pnl_improved_without_worse_drawdown",
                }
            },
        }

    monkeypatch.setattr(service, "_build_crypto_recommendation", fake_crypto_recommendation)

    result = await service.run(domain="crypto", dry_run=False, min_support=1, now=datetime(2026, 5, 10, tzinfo=UTC))

    async with session_factory() as session:
        repo = PlatformRepository(session)
        candidate = await repo.get_agent_pack(result["candidate_version"])
        checkpoint = await repo.get_checkpoint("autonomous_gate_tuning:crypto:demo")
        await session.commit()

    assert result["status"] == "staged"
    assert result["domain"] == "crypto"
    assert candidate is not None
    crypto_policy = candidate.payload["crypto_policy"]
    assert crypto_policy["live"]["trading_enabled"] is True
    assert crypto_policy["live"]["production_autonomy_enabled"] is True
    assert crypto_policy["live"]["asset_modes"] == {"BTC": "live"}
    assert "ETH" not in crypto_policy["asset_entry_overrides"]
    assert crypto_policy["asset_entry_overrides"]["BTC"]["min_fee_adjusted_edge_bps"] == 1000
    assert checkpoint is not None
    assert checkpoint.payload["changes"]["BTC"]["live_mode"] == "live"

    await engine.dispose()
