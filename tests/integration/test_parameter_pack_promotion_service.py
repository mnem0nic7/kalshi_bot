from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

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
        "resolved_trades": 100,
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


@pytest.mark.asyncio
async def test_parameter_pack_canary_passes_without_activating_candidate(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_canary_pass.db")
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
            candidate_report=_holdout_report(candidate.pack_hash, max_drawdown=0.10),
            current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
            hard_caps=load_hard_caps(),
        )
        result = await service.evaluate_staged_canary(
            repo,
            canary_report={
                "completed_shadow_rooms": 25,
                "elapsed_seconds": 7200,
                "brier": 0.20,
                "risk_engine_bypasses": 0,
                "data_source_kill_events": 0,
            },
        )
        control = await repo.get_deployment_control()
        promotion = await repo.get_promotion_event(staged.promotion_event_id)
        stored_candidate = await repo.get_parameter_pack(candidate.version)
        await session.commit()

    assert result.status == "canary_passed"
    assert result.passed is True
    assert control.active_color == "blue"
    assert control.notes["parameter_packs"]["status"] == "canary_passed"
    assert promotion is not None
    assert promotion.status == "canary_passed"
    assert stored_candidate is not None
    assert stored_candidate.status == "canary_passed"

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_canary_pending_keeps_staged_candidate(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_canary_pending.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(default_parameter_pack(version="candidate-params-v1"), status="candidate")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        await service.stage_candidate(
            repo,
            candidate_pack=candidate,
            candidate_report=_holdout_report(candidate.pack_hash),
            current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
            hard_caps=load_hard_caps(),
        )
        result = await service.evaluate_staged_canary(
            repo,
            canary_report={"completed_shadow_rooms": 3, "elapsed_seconds": 120, "brier": 0.08},
        )
        control = await repo.get_deployment_control()
        stored_candidate = await repo.get_parameter_pack(candidate.version)
        await session.commit()

    assert result.status == "canary_pending"
    assert result.failures == ["insufficient_shadow_rooms", "insufficient_canary_duration"]
    assert control.notes["parameter_packs"]["status"] == "canary_pending"
    assert stored_candidate is not None
    assert stored_candidate.status == "staged"

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_canary_failure_rolls_back_candidate(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_canary_fail.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(default_parameter_pack(version="candidate-params-v1"), status="candidate")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        staged = await service.stage_candidate(
            repo,
            candidate_pack=candidate,
            candidate_report=_holdout_report(candidate.pack_hash, max_drawdown=0.10),
            current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
            hard_caps=load_hard_caps(),
        )
        result = await service.evaluate_staged_canary(
            repo,
            canary_report={
                "completed_shadow_rooms": 25,
                "elapsed_seconds": 7200,
                "brier": 0.30,
                "risk_engine_bypasses": 1,
            },
        )
        control = await repo.get_deployment_control()
        promotion = await repo.get_promotion_event(staged.promotion_event_id)
        stored_candidate = await repo.get_parameter_pack(candidate.version)
        await session.commit()

    assert result.status == "canary_failed"
    assert result.rollback is not None
    assert result.failures == ["canary_brier_regression", "risk_engine_bypass"]
    assert control.notes["parameter_packs"]["status"] == "rolled_back"
    assert promotion is not None
    assert promotion.status == "rolled_back"
    assert stored_candidate is not None
    assert stored_candidate.status == "rejected"

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_promote_canary_passed_archives_previous_champion(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_promote.db")
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
        await service.evaluate_staged_canary(
            repo,
            canary_report={
                "completed_shadow_rooms": 25,
                "elapsed_seconds": 7200,
                "brier": 0.20,
            },
        )
        result = await service.promote_canary_passed(repo, reason="test_promote")
        control = await repo.get_deployment_control()
        promotion = await repo.get_promotion_event(staged.promotion_event_id)
        stored_candidate = await repo.get_parameter_pack(candidate.version)
        stored_previous = await repo.get_parameter_pack(current.version)
        champion = await repo.get_champion_parameter_pack()
        await session.commit()

    assert result.status == "champion"
    assert result.candidate_version == candidate.version
    assert control.active_color == "blue"
    assert control.notes["parameter_packs"]["status"] == "champion"
    assert control.notes["parameter_packs"]["champion_version"] == candidate.version
    assert promotion is not None
    assert promotion.status == "stable"
    assert stored_candidate is not None
    assert stored_candidate.status == "champion"
    assert stored_previous is not None
    assert stored_previous.status == "archived"
    assert champion is not None
    assert champion.version == candidate.version

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_mark_stalled_if_expired_updates_notes_and_promotion(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_stalled.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(default_parameter_pack(version="candidate-params-v1"), status="candidate")

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
        control = await repo.get_deployment_control()
        notes = dict(control.notes)
        parameter_notes = dict(notes["parameter_packs"])
        parameter_notes["staged_at"] = (datetime.now(UTC) - timedelta(hours=8)).isoformat()
        notes["parameter_packs"] = parameter_notes
        await repo.update_deployment_notes(notes)

        stalled = await service.mark_stalled_if_expired(repo, max_age_seconds=60)
        promotion = await repo.get_promotion_event(staged.promotion_event_id)
        control = await repo.get_deployment_control()
        await session.commit()

    assert stalled is not None
    assert stalled["status"] == "stalled"
    assert control.notes["parameter_packs"]["status"] == "stalled"
    assert promotion is not None
    assert promotion.status == "canary_stalled"

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_record_starvation_escalates_after_three_failures(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_starvation.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    selection = {
        "selected": False,
        "promotion_starvation": True,
        "starvation_tolerance": 2,
        "evaluated": [{"version": "bad-1", "failures": ["coverage_below_minimum"]}],
    }

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        first = await service.record_promotion_starvation(
            repo,
            selection_payload=selection,
            reason="test_starvation",
            escalation_threshold=3,
        )
        second = await service.record_promotion_starvation(
            repo,
            selection_payload=selection,
            reason="test_starvation",
            escalation_threshold=3,
        )
        third = await service.record_promotion_starvation(
            repo,
            selection_payload=selection,
            reason="test_starvation",
            escalation_threshold=3,
        )
        checkpoint = await repo.get_checkpoint("parameter_pack_promotion_starvation:demo")
        events = await repo.list_ops_events(limit=10, sources=["parameter_pack"], kalshi_env="demo")
        await session.commit()

    assert first.status == "promotion_starvation"
    assert first.consecutive_starvations == 1
    assert first.escalated is False
    assert second.consecutive_starvations == 2
    assert second.escalated is False
    assert third.consecutive_starvations == 3
    assert third.escalated is True
    assert checkpoint is not None
    assert checkpoint.payload["event_kind"] == "parameter_pack_promotion_starvation"
    assert checkpoint.payload["consecutive_starvations"] == 3
    assert checkpoint.payload["escalated"] is True
    assert checkpoint.payload["evaluated_count"] == 1
    assert checkpoint.payload["selection"]["evaluated"][0]["failures"] == ["coverage_below_minimum"]
    warning_events = [event for event in events if event.severity == "warning"]
    error_events = [event for event in events if event.severity == "error"]
    assert len(warning_events) == 2
    assert len(error_events) == 1
    assert error_events[0].payload["consecutive_starvations"] == 3
    assert error_events[0].payload["escalated"] is True

    await engine.dispose()


@pytest.mark.asyncio
async def test_parameter_pack_stage_clears_existing_starvation_checkpoint(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack_starvation_clear.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    current = default_parameter_pack()
    candidate = replace(
        default_parameter_pack(version="candidate-params-v1"),
        status="candidate",
        parameters={**current.parameters, "pseudo_count": 10},
    )
    selection = {
        "selected": False,
        "promotion_starvation": True,
        "starvation_tolerance": 2,
        "evaluated": [{"version": "bad-1", "failures": ["coverage_below_minimum"]}],
    }

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        service = ParameterPackPromotionService()
        await service.record_promotion_starvation(
            repo,
            selection_payload=selection,
            reason="test_starvation",
            escalation_threshold=3,
        )
        await service.record_promotion_starvation(
            repo,
            selection_payload=selection,
            reason="test_starvation",
            escalation_threshold=3,
        )
        staged = await service.stage_candidate(
            repo,
            candidate_pack=candidate,
            candidate_report=_holdout_report(candidate.pack_hash),
            current_report=_holdout_report(current.pack_hash, max_drawdown=0.10),
            hard_caps=load_hard_caps(),
            reason="test_stage_after_starvation",
        )
        checkpoint = await repo.get_checkpoint("parameter_pack_promotion_starvation:demo")
        events = await repo.list_ops_events(limit=10, sources=["parameter_pack"], kalshi_env="demo")
        await session.commit()

    assert staged.status == "staged"
    assert checkpoint is not None
    assert checkpoint.payload["status"] == "cleared"
    assert checkpoint.payload["candidate_version"] == candidate.version
    assert checkpoint.payload["consecutive_starvations"] == 0
    assert checkpoint.payload["previous_consecutive_starvations"] == 2
    info_events = [event for event in events if event.severity == "info"]
    assert len(info_events) == 1
    assert info_events[0].payload["status"] == "cleared"
    assert info_events[0].payload["candidate_version"] == candidate.version

    await engine.dispose()
