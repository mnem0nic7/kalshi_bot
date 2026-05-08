from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.core.enums import RoomStage
from kalshi_bot.core.schemas import ResearchDossier
from kalshi_bot.db.models import Room, RoomResearchHealthRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.research import ResearchCoordinator


class ResearchHealthBackfillService:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker,
        research_coordinator: ResearchCoordinator,
    ) -> None:
        self.session_factory = session_factory
        self.research_coordinator = research_coordinator

    async def backfill_research_health(
        self,
        *,
        origins: list[str] | None = None,
        days: int | None = 30,
        market_prefixes: list[str] | None = None,
        limit: int = 2000,
        overwrite: bool = False,
        include_non_complete: bool = False,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        since = now - timedelta(days=days) if days and days > 0 else None
        prefixes = [prefix.strip() for prefix in market_prefixes or [] if prefix and prefix.strip()]
        async with self.session_factory() as session:
            stmt = select(Room)
            if origins:
                stmt = stmt.where(Room.room_origin.in_(origins))
            if since is not None:
                stmt = stmt.where(Room.created_at >= since)
            if prefixes:
                stmt = stmt.where(or_(*(Room.market_ticker.startswith(prefix) for prefix in prefixes)))
            if not include_non_complete:
                stmt = stmt.where(Room.stage == RoomStage.COMPLETE.value)
            if not overwrite:
                stmt = stmt.outerjoin(
                    RoomResearchHealthRecord,
                    RoomResearchHealthRecord.room_id == Room.id,
                ).where(RoomResearchHealthRecord.room_id.is_(None))
            stmt = stmt.order_by(Room.updated_at.desc()).limit(limit)
            rooms = list((await session.execute(stmt)).scalars())
            repo = PlatformRepository(session)
            updated: list[dict[str, Any]] = []
            skipped: list[dict[str, str]] = []
            skip_counts: Counter[str] = Counter()
            for room in rooms:
                artifact = await repo.get_latest_artifact(
                    room_id=room.id,
                    artifact_type="research_dossier_snapshot",
                )
                dossier_payload = artifact.payload if artifact is not None else None
                dossier_artifact_id = artifact.id if artifact is not None else None
                reference_time = _ensure_utc(artifact.created_at if artifact is not None else room.created_at)
                if dossier_payload is None:
                    dossier_record = await repo.get_research_dossier(room.market_ticker)
                    dossier_payload = dossier_record.payload if dossier_record is not None else None
                    reference_time = _ensure_utc(dossier_record.updated_at) if dossier_record is not None else reference_time
                if not dossier_payload:
                    skip_counts["dossier_missing"] += 1
                    skipped.append({"room_id": room.id, "market_ticker": room.market_ticker, "reason": "dossier_missing"})
                    continue
                try:
                    dossier = ResearchDossier.model_validate(dossier_payload)
                    research_health = self.research_coordinator.training_quality_snapshot(
                        dossier,
                        reference_time=reference_time,
                    )
                except Exception as exc:
                    skip_counts["dossier_invalid"] += 1
                    skipped.append(
                        {
                            "room_id": room.id,
                            "market_ticker": room.market_ticker,
                            "reason": "dossier_invalid",
                            "error": str(exc),
                        }
                    )
                    continue
                await repo.upsert_room_research_health(
                    room_id=room.id,
                    market_ticker=room.market_ticker,
                    dossier_status=research_health["dossier_status"],
                    gate_passed=research_health["gate_passed"],
                    valid_dossier=research_health["valid_dossier"],
                    good_for_training=research_health["good_for_training"],
                    quality_score=research_health["quality_score"],
                    citation_coverage_score=research_health["citation_coverage_score"],
                    settlement_clarity_score=research_health["settlement_clarity_score"],
                    freshness_score=research_health["freshness_score"],
                    contradiction_count=research_health["contradiction_count"],
                    structured_completeness_score=research_health["structured_completeness_score"],
                    fair_value_score=research_health["fair_value_score"],
                    dossier_artifact_id=dossier_artifact_id,
                    payload={
                        **research_health["payload"],
                        "backfilled": True,
                        "backfilled_at": now.isoformat(),
                        "reference_time": reference_time.isoformat() if reference_time is not None else None,
                    },
                )
                updated.append({"room_id": room.id, "market_ticker": room.market_ticker})
            await session.commit()
        return {
            "schema_version": "research-health-backfill-v1",
            "status": "ok",
            "matched_room_count": len(rooms),
            "backfilled_count": len(updated),
            "skipped_count": len(skipped),
            "skip_reasons": dict(skip_counts),
            "updated": updated,
            "skipped": skipped[:50],
            "filters": {
                "origins": origins or [],
                "days": days,
                "market_prefixes": prefixes,
                "limit": limit,
                "overwrite": overwrite,
                "include_non_complete": include_non_complete,
            },
        }


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
