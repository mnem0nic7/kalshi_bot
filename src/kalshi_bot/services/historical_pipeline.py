from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RoomOrigin
from kalshi_bot.core.schemas import HistoricalIntelligenceRunRequest
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.historical_intelligence import HistoricalIntelligenceService
from kalshi_bot.services.historical_training import HistoricalTrainingService


class HistoricalPipelineService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        historical_training_service: HistoricalTrainingService,
        historical_intelligence_service: HistoricalIntelligenceService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.historical_training_service = historical_training_service
        self.historical_intelligence_service = historical_intelligence_service

    def rolling_window(
        self,
        *,
        days: int | None = None,
        reference_date: date | None = None,
    ) -> tuple[date, date]:
        rolling_days = max(1, int(days or self.settings.historical_pipeline_bootstrap_days))
        end_date = (reference_date or datetime.now(UTC).date()) - timedelta(days=1)
        start_date = end_date - timedelta(days=rolling_days - 1)
        return start_date, end_date

    async def bootstrap(
        self,
        *,
        days: int | None = None,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        date_from, date_to = self.rolling_window(days=days)
        return await self._run_pipeline(
            pipeline_kind="bootstrap",
            rolling_days=max(1, int(days or self.settings.historical_pipeline_bootstrap_days)),
            date_from=date_from,
            date_to=date_to,
            process_from=date_from,
            process_to=date_to,
            series=series,
        )

    async def daily(self, *, series: list[str] | None = None) -> dict[str, Any]:
        date_from, date_to = self.rolling_window(days=self.settings.historical_pipeline_bootstrap_days)
        incremental_days = max(1, int(self.settings.historical_pipeline_incremental_days))
        process_from = max(date_from, date_to - timedelta(days=incremental_days - 1))
        return await self._run_pipeline(
            pipeline_kind="daily",
            rolling_days=self.settings.historical_pipeline_bootstrap_days,
            date_from=date_from,
            date_to=date_to,
            process_from=process_from,
            process_to=date_to,
            series=series,
        )

    async def status(self) -> dict[str, Any]:
        date_from, date_to = self.rolling_window(days=self.settings.historical_pipeline_bootstrap_days)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            recent_runs = await repo.list_historical_pipeline_runs(limit=10)
            await session.commit()
        historical_status = await self.historical_training_service.get_status()
        intelligence_status = await self.historical_intelligence_service.get_status()
        latest_run = recent_runs[0].payload if recent_runs else None
        return {
            "rolling_window": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "days": self.settings.historical_pipeline_bootstrap_days,
            },
            "latest_run": latest_run,
            "recent_runs": [
                {
                    "id": run.id,
                    "pipeline_kind": run.pipeline_kind,
                    "status": run.status,
                    "date_from": run.date_from,
                    "date_to": run.date_to,
                    "rolling_days": run.rolling_days,
                    "started_at": run.started_at.isoformat(),
                    "finished_at": run.finished_at.isoformat() if run.finished_at is not None else None,
                    "payload": run.payload,
                }
                for run in recent_runs
            ],
            "historical_status": historical_status,
            "historical_intelligence": intelligence_status,
        }

    async def _run_pipeline(
        self,
        *,
        pipeline_kind: str,
        rolling_days: int,
        date_from: date,
        date_to: date,
        process_from: date,
        process_to: date,
        series: list[str] | None,
    ) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.create_historical_pipeline_run(
                pipeline_kind=pipeline_kind,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                rolling_days=rolling_days,
                payload={
                    "status": "running",
                    "pipeline_kind": pipeline_kind,
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                    "process_from": process_from.isoformat(),
                    "process_to": process_to.isoformat(),
                    "rolling_days": rolling_days,
                    "series": list(series or []),
                },
            )
            await session.commit()

        try:
            import_result = await self.historical_training_service.import_weather_history(
                date_from=process_from,
                date_to=process_to,
                series=series,
            )
            market_backfill = await self.historical_training_service.backfill_market_checkpoints(
                date_from=process_from,
                date_to=process_to,
                series=series,
            )
            weather_archive = await self.historical_training_service.backfill_weather_archives(
                date_from=process_from,
                date_to=process_to,
                series=series,
            )
            settlement_backfill = await self.historical_training_service.backfill_settlements(
                date_from=process_from,
                date_to=process_to,
                series=series,
            )
            audit = await self.historical_training_service.audit_historical_replay(
                date_from=date_from,
                date_to=date_to,
                series=series,
                verbose=True,
            )
            refresh_result = {
                "status": "noop",
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "series": list(series or []),
            }
            if audit.get("refresh_needed"):
                affected_days = [
                    date.fromisoformat(str(item["local_market_day"]))
                    for item in (audit.get("affected_market_days") or [])
                    if item.get("local_market_day")
                ]
                refresh_from = min(affected_days) if affected_days else date_from
                refresh_to = max(affected_days) if affected_days else date_to
                refresh_result = await self.historical_training_service.refresh_historical_replay(
                    date_from=refresh_from,
                    date_to=refresh_to,
                    series=series,
                )

            intelligence_result = await self.historical_intelligence_service.run(
                HistoricalIntelligenceRunRequest(
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    origins=[RoomOrigin.HISTORICAL_REPLAY.value],
                    auto_promote=self.settings.historical_intelligence_auto_promote,
                )
            )
            historical_status = await self.historical_training_service.get_status()
            result = {
                "status": "completed",
                "pipeline_kind": pipeline_kind,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "process_from": process_from.isoformat(),
                "process_to": process_to.isoformat(),
                "rolling_days": rolling_days,
                "series": list(series or []),
                "steps": {
                    "historical_import": import_result,
                    "market_checkpoint_backfill": market_backfill,
                    "weather_archive_backfill": weather_archive,
                    "settlement_backfill": settlement_backfill,
                    "replay_refresh": refresh_result,
                    "historical_intelligence": intelligence_result,
                },
                "confidence_state": historical_status.get("confidence_state"),
                "confidence_scorecard": historical_status.get("confidence_scorecard"),
                "historical_build_readiness": historical_status.get("historical_build_readiness"),
            }
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_pipeline_run(
                    run.id,
                    status="completed",
                    payload=result,
                )
                await session.commit()
            return result
        except Exception as exc:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_pipeline_run(
                    run.id,
                    status="failed",
                    payload={
                        "status": "failed",
                        "pipeline_kind": pipeline_kind,
                        "date_from": date_from.isoformat(),
                        "date_to": date_to.isoformat(),
                        "process_from": process_from.isoformat(),
                        "process_to": process_to.isoformat(),
                        "rolling_days": rolling_days,
                        "series": list(series or []),
                    },
                    error_text=str(exc),
                )
                await session.commit()
            raise
