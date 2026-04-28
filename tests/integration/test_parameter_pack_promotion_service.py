from __future__ import annotations

from dataclasses import replace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.learning.hard_caps import load_hard_caps
from kalshi_bot.learning.parameter_pack import default_parameter_pack
from kalshi_bot.services.parameter_packs import ParameterPackPromotionService


def _holdout_report(pack_hash: str, *, max_drawdown: float = 0.08) -> dict:
    return {
        "coverage": 0.98,
        "brier": 0.19,
        "ece": 0.04,
        "sharpe": 1.0,
        "max_drawdown": max_drawdown,
        "city_win_rates": {"NY": 0.56},
        "hard_cap_touches": 0,
        "pack_hash": pack_hash,
        "rerun_pack_hash": pack_hash,
    }


@pytest.mark.asyncio
async def test_parameter_pack_stage_records_gate_evidence_without_changing_active_color(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_stage.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(
        default_parameter_pack(version="candidate-params-v1"),
        status="candidate",
        parameters={**current.parameters, "pseudo_count": 10},
    )

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        result = await service.stage_candidate(
            repo,
            candidate_pack=candidate,
            candidate_report=_holdout_report(candidate.pack_hash),
            current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
            hard_caps=load_hard_caps(),
            reason="test_stage",
        )
        control = await repo.get_deployment_control()
        promotion = await repo.get_promotion_event(result.promotion_event_id)
        stored_candidate = await repo.get_parameter_pack(candidate.version)
        await session.commit()

    assert result.status == "staged"
    assert result.target_color == "green"
    assert control.active_color == "blue"
    assert control.notes["parameter_packs"]["candidate_version"] == candidate.version
    assert control.notes["parameter_packs"]["target_color"] == "green"
    assert control.notes["parameter_packs"]["hard_caps"]["max_drawdown_pct"] == 0.20
    assert promotion is not None
    assert promotion.payload["kind"] == "parameter_pack"
    assert promotion.payload["gate"]["passed"] is True
    assert stored_candidate is not None
    assert stored_candidate.status == "staged"
    assert stored_candidate.parent_version == current.version

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_stage_rejects_failed_gate_without_promotion_event(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_stage_reject.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(default_parameter_pack(version="candidate-params-v1"), status="candidate")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        with pytest.raises(ValueError, match="drawdown_regression"):
            await service.stage_candidate(
                repo,
                candidate_pack=candidate,
                candidate_report=_holdout_report(candidate.pack_hash, max_drawdown=0.25),
                current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
                hard_caps=load_hard_caps(),
            )
        await session.rollback()

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        promotions = await repo.list_promotion_events(limit=10)
        await session.commit()

    assert promotions == []

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_rollback_staged_marks_candidate_rejected_and_preserves_active_color(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_rollback.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(
        default_parameter_pack(version="candidate-params-v1"),
        status="candidate",
        parameters={**current.parameters, "pseudo_count": 10},
    )

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        staged = await service.stage_candidate(
            repo,
            candidate_pack=candidate,
            candidate_report=_holdout_report(candidate.pack_hash),
            current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
            hard_caps=load_hard_caps(),
        )
        rolled_back = await service.rollback_staged(repo, reason="test_rollback")
        control = await repo.get_deployment_control()
        promotion = await repo.get_promotion_event(staged.promotion_event_id)
        stored_candidate = await repo.get_parameter_pack(candidate.version)
        await session.commit()

    assert rolled_back.status == "rolled_back"
    assert rolled_back.candidate_version == candidate.version
    assert control.active_color == "blue"
    assert control.notes["parameter_packs"]["status"] == "rolled_back"
    assert control.notes["parameter_packs"]["rollback_reason"] == "test_rollback"
    assert promotion is not None
    assert promotion.status == "rolled_back"
    assert promotion.rollback_reason == "test_rollback"
    assert stored_candidate is not None
    assert stored_candidate.status == "rejected"

    await engine.dispose()
