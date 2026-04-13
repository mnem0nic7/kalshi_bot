from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RoomOrigin
from kalshi_bot.core.schemas import HistoricalIntelligenceRunRequest, HistoricalTrainingBuildRequest
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
        chunk_days: int | None = None,
    ) -> dict[str, Any]:
        rolling_days = max(1, int(days or self.settings.historical_pipeline_bootstrap_days))
        date_from, date_to = self.rolling_window(days=rolling_days)
        return await self._run_pipeline_chunked(
            pipeline_kind="bootstrap",
            rolling_days=rolling_days,
            date_from=date_from,
            date_to=date_to,
            process_from=date_from,
            process_to=date_to,
            series=series,
            chunk_days=max(1, int(chunk_days or self.settings.historical_pipeline_chunk_days)),
        )

    async def daily(
        self,
        *,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        rolling_days = max(1, int(self.settings.historical_pipeline_bootstrap_days))
        date_from, date_to = self.rolling_window(days=rolling_days)
        incremental_days = max(1, int(self.settings.historical_pipeline_incremental_days))
        process_from = max(date_from, date_to - timedelta(days=incremental_days - 1))
        return await self._run_pipeline_chunked(
            pipeline_kind="daily",
            rolling_days=rolling_days,
            date_from=date_from,
            date_to=date_to,
            process_from=process_from,
            process_to=date_to,
            series=series,
            chunk_days=max(1, int(self.settings.historical_pipeline_chunk_days)),
        )

    async def resume(self, *, series: list[str] | None = None) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            recent_runs = await repo.list_historical_pipeline_runs(limit=50)
            await session.commit()

        resumable = next(
            (
                run
                for run in recent_runs
                if run.pipeline_kind in {"bootstrap", "bootstrap_resume"}
                and run.status in {"running", "failed"}
                and isinstance(run.payload, dict)
                and isinstance(run.payload.get("bootstrap_progress"), dict)
            ),
            None,
        )
        if resumable is None:
            return {"status": "noop", "reason": "no_resumable_bootstrap"}

        payload = resumable.payload or {}
        progress = dict(payload.get("bootstrap_progress") or {})
        next_chunk_start = progress.get("next_chunk_start")
        if not next_chunk_start:
            return {
                "status": "noop",
                "reason": "bootstrap_already_complete",
                "run_id": resumable.id,
            }

        resume_series = list(series or payload.get("series") or [])
        resume_from = date.fromisoformat(str(next_chunk_start))
        process_to = date.fromisoformat(str(payload.get("process_to") or payload.get("date_to")))
        date_from = date.fromisoformat(str(payload["date_from"]))
        date_to = date.fromisoformat(str(payload["date_to"]))
        rolling_days = int(payload.get("rolling_days") or self.settings.historical_pipeline_bootstrap_days)
        chunk_days = int(progress.get("chunk_days") or self.settings.historical_pipeline_chunk_days)

        return await self._run_pipeline_chunked(
            pipeline_kind="bootstrap_resume",
            rolling_days=rolling_days,
            date_from=date_from,
            date_to=date_to,
            process_from=resume_from,
            process_to=process_to,
            series=resume_series or None,
            chunk_days=chunk_days,
            resume_from_run_id=resumable.id,
            prior_progress=progress,
            original_process_from=date.fromisoformat(str(payload.get("process_from") or payload["date_from"])),
        )

    async def status(self, *, verbose: bool = False) -> dict[str, Any]:
        date_from, date_to = self.rolling_window(days=self.settings.historical_pipeline_bootstrap_days)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            recent_runs = await repo.list_historical_pipeline_runs(limit=10)
            await session.commit()
        historical_status = await self.historical_training_service.get_status(verbose=verbose)
        intelligence_status = await self.historical_intelligence_service.get_status()
        latest_run = recent_runs[0].payload if recent_runs else None
        return {
            "rolling_window": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "days": self.settings.historical_pipeline_bootstrap_days,
                "chunk_days": self.settings.historical_pipeline_chunk_days,
            },
            "latest_run": latest_run,
            "bootstrap_progress": historical_status.get("bootstrap_progress") or self._bootstrap_progress_from_runs(recent_runs),
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

    async def _run_pipeline_chunked(
        self,
        *,
        pipeline_kind: str,
        rolling_days: int,
        date_from: date,
        date_to: date,
        process_from: date,
        process_to: date,
        series: list[str] | None,
        chunk_days: int,
        resume_from_run_id: str | None = None,
        prior_progress: dict[str, Any] | None = None,
        original_process_from: date | None = None,
    ) -> dict[str, Any]:
        normalized_series = list(series or [])
        effective_process_from = original_process_from or process_from
        all_chunks = self._chunk_ranges(effective_process_from, process_to, chunk_days)
        remaining_chunks = [chunk for chunk in all_chunks if chunk[1] >= process_from]
        completed_chunk_count = int(prior_progress.get("completed_chunk_count", 0)) if prior_progress else 0

        initial_payload = {
            "status": "running",
            "pipeline_kind": pipeline_kind,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "process_from": effective_process_from.isoformat(),
            "process_to": process_to.isoformat(),
            "rolling_days": rolling_days,
            "series": normalized_series,
            "bootstrap_progress": {
                "chunk_days": chunk_days,
                "total_chunks": len(all_chunks),
                "completed_chunk_count": completed_chunk_count,
                "remaining_chunk_count": max(0, len(all_chunks) - completed_chunk_count),
                "next_chunk_start": process_from.isoformat() if remaining_chunks else None,
                "remaining_date_window": {
                    "date_from": process_from.isoformat(),
                    "date_to": process_to.isoformat(),
                }
                if remaining_chunks
                else None,
                "last_refreshed_replay_date_range": prior_progress.get("last_refreshed_replay_date_range") if prior_progress else None,
                "latest_completed_chunk": prior_progress.get("latest_completed_chunk") if prior_progress else None,
                "completed_chunks": list(prior_progress.get("completed_chunks") or []) if prior_progress else [],
                "resumed_from_run_id": resume_from_run_id,
                "resumable": True,
            },
        }

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.create_historical_pipeline_run(
                pipeline_kind=pipeline_kind,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                rolling_days=rolling_days,
                payload=initial_payload,
            )
            await session.commit()

        latest_historical_status = await self.historical_training_service.get_status()
        previous_full_market_days = int(
            (latest_historical_status.get("historical_build_readiness") or {}).get("distinct_full_coverage_market_days", 0)
        )

        try:
            chunk_summaries: list[dict[str, Any]] = list(initial_payload["bootstrap_progress"]["completed_chunks"])
            for chunk_index, (chunk_from, chunk_to) in enumerate(
                remaining_chunks,
                start=completed_chunk_count + 1,
            ):
                chunk_result = await self._run_chunk(
                    chunk_from=chunk_from,
                    chunk_to=chunk_to,
                    rolling_date_from=date_from,
                    rolling_date_to=date_to,
                    series=normalized_series or None,
                    previous_full_market_days=previous_full_market_days,
                )
                latest_historical_status = chunk_result["historical_status"]
                current_full_market_days = int(
                    (latest_historical_status.get("historical_build_readiness") or {}).get("distinct_full_coverage_market_days", 0)
                )
                previous_full_market_days = current_full_market_days

                chunk_summary = {
                    "chunk_index": chunk_index,
                    "date_from": chunk_from.isoformat(),
                    "date_to": chunk_to.isoformat(),
                    "steps": chunk_result["steps"],
                    "auto_builds": chunk_result["auto_builds"],
                    "confidence_state": latest_historical_status.get("confidence_state"),
                    "historical_build_readiness": latest_historical_status.get("historical_build_readiness"),
                }
                chunk_summaries.append(chunk_summary)
                next_chunk_start = chunk_to + timedelta(days=1)
                progress = {
                    "chunk_days": chunk_days,
                    "total_chunks": len(all_chunks),
                    "completed_chunk_count": chunk_index,
                    "remaining_chunk_count": max(0, len(all_chunks) - chunk_index),
                    "next_chunk_start": next_chunk_start.isoformat() if next_chunk_start <= process_to else None,
                    "remaining_date_window": {
                        "date_from": next_chunk_start.isoformat(),
                        "date_to": process_to.isoformat(),
                    }
                    if next_chunk_start <= process_to
                    else None,
                    "last_refreshed_replay_date_range": chunk_result["last_refreshed_replay_date_range"],
                    "latest_completed_chunk": {
                        "date_from": chunk_from.isoformat(),
                        "date_to": chunk_to.isoformat(),
                        "chunk_index": chunk_index,
                    },
                    "completed_chunks": chunk_summaries[-10:],
                    "resumed_from_run_id": resume_from_run_id,
                    "resumable": next_chunk_start <= process_to,
                }
                payload = {
                    **initial_payload,
                    "status": "running",
                    "bootstrap_progress": progress,
                    "latest_chunk": chunk_summary,
                    "confidence_state": latest_historical_status.get("confidence_state"),
                    "confidence_scorecard": latest_historical_status.get("confidence_scorecard"),
                    "historical_build_readiness": latest_historical_status.get("historical_build_readiness"),
                }
                async with self.session_factory() as session:
                    repo = PlatformRepository(session)
                    await repo.update_historical_pipeline_run(
                        run.id,
                        status="running",
                        payload=payload,
                    )
                    await session.commit()

            final_payload = {
                "status": "completed",
                "pipeline_kind": pipeline_kind,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "process_from": effective_process_from.isoformat(),
                "process_to": process_to.isoformat(),
                "rolling_days": rolling_days,
                "series": normalized_series,
                "chunk_count": len(all_chunks),
                "chunk_days": chunk_days,
                "chunks": chunk_summaries[-10:],
                "bootstrap_progress": {
                    "chunk_days": chunk_days,
                    "total_chunks": len(all_chunks),
                    "completed_chunk_count": len(all_chunks),
                    "remaining_chunk_count": 0,
                    "next_chunk_start": None,
                    "remaining_date_window": None,
                    "last_refreshed_replay_date_range": {
                        "date_from": chunk_summaries[-1]["steps"]["replay_refresh"].get("date_from"),
                        "date_to": chunk_summaries[-1]["steps"]["replay_refresh"].get("date_to"),
                    }
                    if chunk_summaries
                    else None,
                    "latest_completed_chunk": chunk_summaries[-1] if chunk_summaries else None,
                    "completed_chunks": chunk_summaries[-10:],
                    "resumed_from_run_id": resume_from_run_id,
                    "resumable": False,
                },
                "confidence_state": latest_historical_status.get("confidence_state"),
                "confidence_scorecard": latest_historical_status.get("confidence_scorecard"),
                "historical_build_readiness": latest_historical_status.get("historical_build_readiness"),
                "coverage_backlog": latest_historical_status.get("coverage_backlog"),
                "promotable_market_day_counts": latest_historical_status.get("promotable_market_day_counts"),
                "confidence_progress": latest_historical_status.get("confidence_progress"),
            }
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_pipeline_run(
                    run.id,
                    status="completed",
                    payload=final_payload,
                )
                await session.commit()
            return final_payload
        except Exception as exc:
            failure_status = await self.historical_training_service.get_status()
            failed_payload = {
                "status": "failed",
                "pipeline_kind": pipeline_kind,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "process_from": effective_process_from.isoformat(),
                "process_to": process_to.isoformat(),
                "rolling_days": rolling_days,
                "series": normalized_series,
                "bootstrap_progress": (
                    initial_payload.get("bootstrap_progress")
                    if "progress" not in locals()
                    else progress
                ),
                "confidence_state": failure_status.get("confidence_state"),
                "confidence_scorecard": failure_status.get("confidence_scorecard"),
                "historical_build_readiness": failure_status.get("historical_build_readiness"),
            }
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_pipeline_run(
                    run.id,
                    status="failed",
                    payload=failed_payload,
                    error_text=str(exc),
                )
                await session.commit()
            raise

    async def _run_chunk(
        self,
        *,
        chunk_from: date,
        chunk_to: date,
        rolling_date_from: date,
        rolling_date_to: date,
        series: list[str] | None,
        previous_full_market_days: int,
    ) -> dict[str, Any]:
        import_result = await self.historical_training_service.import_weather_history(
            date_from=chunk_from,
            date_to=chunk_to,
            series=series,
        )
        market_backfill = await self.historical_training_service.backfill_market_checkpoints(
            date_from=chunk_from,
            date_to=chunk_to,
            series=series,
        )
        weather_archive = await self.historical_training_service.backfill_weather_archives(
            date_from=chunk_from,
            date_to=chunk_to,
            series=series,
        )
        settlement_backfill = await self.historical_training_service.backfill_settlements(
            date_from=chunk_from,
            date_to=chunk_to,
            series=series,
        )
        audit = await self.historical_training_service.audit_historical_replay(
            date_from=chunk_from,
            date_to=chunk_to,
            series=series,
            verbose=True,
        )
        refresh_result: dict[str, Any] = {
            "status": "noop",
            "date_from": chunk_from.isoformat(),
            "date_to": chunk_to.isoformat(),
            "series": list(series or []),
        }
        if audit.get("refresh_needed"):
            affected_days = [
                date.fromisoformat(str(item["local_market_day"]))
                for item in (audit.get("affected_market_days") or [])
                if item.get("local_market_day")
            ]
            refresh_from = min(affected_days) if affected_days else chunk_from
            refresh_to = max(affected_days) if affected_days else chunk_to
            refresh_result = await self.historical_training_service.refresh_historical_replay(
                date_from=refresh_from,
                date_to=refresh_to,
                series=series,
            )

        intelligence_result = await self.historical_intelligence_service.run(
            HistoricalIntelligenceRunRequest(
                date_from=rolling_date_from.isoformat(),
                date_to=rolling_date_to.isoformat(),
                origins=[RoomOrigin.HISTORICAL_REPLAY.value],
                auto_promote=self.settings.historical_intelligence_auto_promote,
            )
        )
        historical_status = await self.historical_training_service.get_status()
        auto_builds = await self._auto_refresh_historical_outputs(
            rolling_date_from=rolling_date_from,
            rolling_date_to=rolling_date_to,
            series=series,
            previous_full_market_days=previous_full_market_days,
            historical_status=historical_status,
        )
        return {
            "steps": {
                "historical_import": import_result,
                "market_checkpoint_backfill": market_backfill,
                "weather_archive_backfill": weather_archive,
                "settlement_backfill": settlement_backfill,
                "replay_audit": audit,
                "replay_refresh": refresh_result,
                "historical_intelligence": intelligence_result,
            },
            "historical_status": historical_status,
            "auto_builds": auto_builds,
            "last_refreshed_replay_date_range": {
                "date_from": refresh_result.get("date_from"),
                "date_to": refresh_result.get("date_to"),
            },
        }

    async def _auto_refresh_historical_outputs(
        self,
        *,
        rolling_date_from: date,
        rolling_date_to: date,
        series: list[str] | None,
        previous_full_market_days: int,
        historical_status: dict[str, Any],
    ) -> dict[str, Any]:
        builds: dict[str, Any] = {}
        builds["outcome_eval"] = await self.historical_training_service.build_historical_dataset(
            HistoricalTrainingBuildRequest(
                mode="outcome-eval",
                date_from=rolling_date_from.isoformat(),
                date_to=rolling_date_to.isoformat(),
                series=list(series or []),
                quality_cleaned_only=True,
                include_pathology_examples=False,
                require_full_checkpoints=False,
                late_only_ok=True,
                origins=[RoomOrigin.HISTORICAL_REPLAY.value],
                output=str(self._auto_output_path("historical_outcome_eval_latest.jsonl")),
            )
        )
        current_full_market_days = int(
            (historical_status.get("historical_build_readiness") or {}).get("distinct_full_coverage_market_days", 0)
        )
        if current_full_market_days != previous_full_market_days:
            builds["decision_eval"] = await self.historical_training_service.build_historical_dataset(
                HistoricalTrainingBuildRequest(
                    mode="decision-eval",
                    date_from=rolling_date_from.isoformat(),
                    date_to=rolling_date_to.isoformat(),
                    series=list(series or []),
                    quality_cleaned_only=True,
                    include_pathology_examples=False,
                    require_full_checkpoints=True,
                    late_only_ok=False,
                    origins=[RoomOrigin.HISTORICAL_REPLAY.value],
                    output=str(self._auto_output_path("historical_decision_eval_latest.jsonl")),
                )
            )
        else:
            builds["decision_eval"] = {
                "status": "skipped",
                "reason": "full_coverage_market_day_count_unchanged",
                "previous_full_market_days": previous_full_market_days,
                "current_full_market_days": current_full_market_days,
            }
        return builds

    def _chunk_ranges(self, process_from: date, process_to: date, chunk_days: int) -> list[tuple[date, date]]:
        ranges: list[tuple[date, date]] = []
        cursor = process_from
        step = max(1, int(chunk_days))
        while cursor <= process_to:
            chunk_end = min(process_to, cursor + timedelta(days=step - 1))
            ranges.append((cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)
        return ranges

    def _bootstrap_progress_from_runs(self, runs: list[Any]) -> dict[str, Any] | None:
        for run in runs:
            payload = run.payload or {}
            progress = payload.get("bootstrap_progress")
            if isinstance(progress, dict):
                return progress
        return None

    def _auto_output_path(self, filename: str) -> Path:
        return Path("data") / "training" / filename
