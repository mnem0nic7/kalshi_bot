from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RoomStage, WeatherResolutionState
from kalshi_bot.core.schemas import (
    ResearchDossier,
    ResearchFreshness,
    ResearchGateVerdict,
    ResearchSummary,
    ResearchTraderContext,
    RoomCreate,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.research_health_backfill import ResearchHealthBackfillService


class FakeResearchCoordinator:
    def __init__(self) -> None:
        self.reference_times: list[datetime | None] = []

    def training_quality_snapshot(self, dossier: ResearchDossier, *, reference_time: datetime | None = None):
        self.reference_times.append(reference_time)
        return {
            "dossier_status": dossier.status,
            "gate_passed": True,
            "valid_dossier": True,
            "good_for_training": True,
            "quality_score": 0.91,
            "citation_coverage_score": 0.8,
            "settlement_clarity_score": 0.9,
            "freshness_score": 0.95,
            "contradiction_count": 0,
            "structured_completeness_score": 0.9,
            "fair_value_score": 0.85,
            "payload": {"mode": dossier.mode},
        }


def _dossier(now: datetime) -> ResearchDossier:
    return ResearchDossier(
        market_ticker="KXHIGHNY-26MAY08-T80",
        status="ready",
        mode="structured",
        summary=ResearchSummary(
            narrative="Structured weather dossier.",
            bullish_case="Observed conditions support yes.",
            bearish_case="Main risk is late station revision.",
            unresolved_uncertainties=[],
            settlement_mechanics="NWS station high temperature.",
            current_numeric_facts={"threshold_f": 80.0},
            source_coverage="structured weather",
            research_confidence=0.8,
        ),
        freshness=ResearchFreshness(
            refreshed_at=now,
            expires_at=now + timedelta(minutes=10),
            stale=False,
            max_source_age_seconds=0,
        ),
        trader_context=ResearchTraderContext(
            fair_yes_dollars="0.6500",
            confidence=0.8,
            thesis="Fresh structured evidence clears the threshold.",
            structured_source_used=True,
            autonomous_ready=True,
            resolution_state=WeatherResolutionState.UNRESOLVED,
        ),
        gate=ResearchGateVerdict(
            passed=True,
            reasons=["Research gate passed."],
            cited_source_keys=[],
        ),
        settlement_covered=True,
    )


@pytest.mark.asyncio
async def test_research_health_backfill_is_idempotent_for_shadow_weather_rooms(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/research-health.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    now = datetime.now(UTC)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        room = await repo.create_room(
            RoomCreate(name="weather shadow", market_ticker="KXHIGHNY-26MAY08-T80"),
            active_color="green",
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
        )
        await repo.update_room_stage(room.id, RoomStage.COMPLETE)
        artifact = await repo.save_artifact(
            room_id=room.id,
            artifact_type="research_dossier_snapshot",
            source="research",
            title="Research dossier snapshot",
            payload=_dossier(now).model_dump(mode="json"),
        )
        await session.commit()

    coordinator = FakeResearchCoordinator()
    service = ResearchHealthBackfillService(
        session_factory=session_factory,
        research_coordinator=coordinator,  # type: ignore[arg-type]
    )

    first = await service.backfill_research_health(
        origins=["shadow"],
        days=30,
        market_prefixes=["KXHIGH"],
    )
    second = await service.backfill_research_health(
        origins=["shadow"],
        days=30,
        market_prefixes=["KXHIGH"],
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        health = await repo.get_room_research_health(room.id)
        await session.commit()

    assert first["backfilled_count"] == 1
    assert second["backfilled_count"] == 0
    assert health is not None
    assert health.dossier_artifact_id == artifact.id
    assert health.good_for_training is True
    assert health.payload["backfilled"] is True
    assert coordinator.reference_times[0] == artifact.created_at

    await engine.dispose()
