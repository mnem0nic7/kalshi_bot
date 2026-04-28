from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import RoomOrigin, WeatherResolutionState
from kalshi_bot.core.schemas import (
    ResearchAuditIssue,
    StrategyAuditResult,
    StrategyAuditSummary,
    TrainingBuildRequest,
    TrainingDatasetBuildSummary,
    TrainingReadiness,
    TrainingRoomBundle,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.discovery import DiscoveryService
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.weather.mapping import WeatherMarketDirectory


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str))
            handle.write("\n")


STRATEGY_AUDIT_VERSION = "weather-quality-v1"
RECENT_QUALITY_WINDOW_HOURS = 24
SETTLEMENT_NEAR_SECONDS = 6 * 60 * 60
SETTLEMENT_GAP_GRACE_SECONDS = 2 * 60 * 60
SETTLEMENT_BACKLOG_PREVIEW_LIMIT = 10


class TrainingCorpusService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        discovery_service: DiscoveryService,
        training_export_service: TrainingExportService,
        weather_directory: WeatherMarketDirectory,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.discovery_service = discovery_service
        self.training_export_service = training_export_service
        self.weather_directory = weather_directory

    async def get_status(self, *, persist_readiness: bool = False) -> dict[str, Any]:
        now = datetime.now(UTC)
        request = TrainingBuildRequest(
            mode="room-bundles",
            limit=self.settings.training_status_room_limit,
            days=self.settings.training_window_days,
            good_research_only=False,
            quality_cleaned_only=False,
        )
        bundles = await self._selected_bundles(request)
        audits = [self._bundle_strategy_audit(bundle) for bundle in bundles]
        readiness_bundles = await self._selected_bundles(request.model_copy(update={"quality_cleaned_only": True}))
        readiness = self._readiness_for_bundles(readiness_bundles)
        if persist_readiness:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.create_training_readiness_snapshot(readiness)
                await session.commit()

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            failed_runs = await repo.list_research_runs(status="failed", limit=200)
            builds = await repo.list_training_dataset_builds(limit=10)
            latest_snapshot = await repo.get_latest_training_readiness()
            settlement_events = await repo.list_exchange_events(
                stream_name="reconcile",
                event_type="settlements",
                limit=200,
            )
            await session.commit()

        complete_by_market = Counter(bundle.room["market_ticker"] for bundle in bundles)
        complete_by_day = Counter(str(bundle.room["created_at"])[:10] for bundle in bundles)
        research_healths = [bundle.research_health or {} for bundle in bundles]
        gate_pass_rate = self._rate(
            [1.0 for health in research_healths if health.get("gate_passed") is True],
            total=len(research_healths),
        )
        freshness_pass_rate = self._rate(
            [1.0 for health in research_healths if float(health.get("freshness_score") or 0.0) >= 0.5],
            total=len(research_healths),
        )
        good_research_count = sum(1 for health in research_healths if health.get("good_for_training") is True)
        settled_count = sum(1 for bundle in bundles if bundle.outcome.settlement_seen)
        unsettled_count = sum(1 for bundle in bundles if not bundle.outcome.settlement_seen)
        trade_positive_count = sum(1 for bundle in bundles if bundle.outcome.ticket_generated)
        active_failed_reason_counts, legacy_failed_reason_counts = self._partition_failed_reason_counts(failed_runs)
        pack_versions = sorted({bundle.room.get("agent_pack_version") for bundle in bundles if bundle.room.get("agent_pack_version")})
        room_origin_counts = Counter(bundle.room_origin or bundle.room.get("room_origin") or "unknown" for bundle in bundles)
        recent_builds = [self._summary_from_record(record).model_dump(mode="json") for record in builds]

        trainable_request = request.model_copy(update={"good_research_only": True, "quality_cleaned_only": True})
        trainable_bundles = await self._selected_bundles(trainable_request)
        holdout_count = max(1, int(len(trainable_bundles) * self.settings.self_improve_holdout_ratio)) if trainable_bundles else 0
        oldest_unsettled_age_seconds = self._oldest_unsettled_age_seconds(bundles)
        exclusion_reason_counts = Counter(
            bundle.exclude_reason for bundle in bundles if bundle.exclude_reason
        )
        forward_audit_count = sum(1 for bundle in bundles if bundle.audit_source == "live_forward")
        backfilled_audit_count = sum(1 for bundle in bundles if bundle.audit_source == "historical_backfill")
        audited_room_count = forward_audit_count + backfilled_audit_count
        quality_debt_summary = {
            "stale_mismatch_count": sum(1 for audit in audits if audit.stale_data_mismatch),
            "missed_stand_down_count": sum(1 for audit in audits if audit.missed_stand_down),
            "weak_resolved_trade_count": sum(
                1
                for audit in audits
                if audit.trade_quality == "weak_trade"
                and audit.resolution_state in {WeatherResolutionState.LOCKED_NO.value, WeatherResolutionState.LOCKED_YES.value}
            ),
            "cleaned_trainable_room_count": len(trainable_bundles),
        }
        recent_quality_debt = self._recent_quality_debt(
            bundles,
            window_hours=RECENT_QUALITY_WINDOW_HOURS,
            now=now,
        )
        quality_debt_summary.update(recent_quality_debt)
        quality_debt_summary["recent_window_hours"] = RECENT_QUALITY_WINDOW_HOURS
        quality_debt_summary["cleaned_trainable_share_by_day"] = self._cleaned_trainable_share_by_day(bundles)
        audit_progress = {
            "audited_room_count": audited_room_count,
            "forward_audit_count": forward_audit_count,
            "backfilled_audit_count": backfilled_audit_count,
            "pending_backfill_room_count": max(0, len(bundles) - audited_room_count),
        }
        recent_exclusion_memory = self._recent_exclusion_memory(
            bundles,
            window_hours=self.settings.training_campaign_lookback_hours,
            now=now,
        )
        settlement_maturity = self._settlement_focus_summary_from_bundles(
            bundles,
            settlement_events=settlement_events,
            now=now,
            limit=SETTLEMENT_BACKLOG_PREVIEW_LIMIT,
        )
        top_blockers = self._top_blockers(
            readiness=readiness,
            quality_debt_summary=quality_debt_summary,
            settlement_maturity=settlement_maturity,
            recent_exclusion_memory=recent_exclusion_memory,
        )
        next_actions = self._next_actions(
            readiness=readiness,
            unsettled_count=unsettled_count,
            settlement_maturity=settlement_maturity,
            quality_debt_summary=quality_debt_summary,
            recent_exclusion_memory=recent_exclusion_memory,
        )

        return {
            "window_days": request.days,
            "room_count": len(bundles),
            "complete_rooms_per_market": dict(complete_by_market.most_common()),
            "complete_rooms_per_day": dict(sorted(complete_by_day.items())),
            "research_gate_pass_rate": gate_pass_rate,
            "dossier_freshness_pass_rate": freshness_pass_rate,
            "good_research_room_count": good_research_count,
            "unsettled_complete_room_count": unsettled_count,
            "oldest_unsettled_room_age_seconds": oldest_unsettled_age_seconds,
            "unsettled_near_settlement_count": settlement_maturity["near_settlement_count"],
            "unsettled_backlog_by_market": settlement_maturity["backlog_by_market"],
            "unsettled_backlog_by_day": settlement_maturity["backlog_by_day"],
            "settled_label_coverage": round(settled_count / len(bundles), 4) if bundles else 0.0,
            "settled_label_velocity": settlement_maturity["settled_label_velocity"],
            "trade_positive_coverage": round(trade_positive_count / len(bundles), 4) if bundles else 0.0,
            "failed_research_reasons": dict(active_failed_reason_counts.most_common()),
            "active_failed_research_reasons": dict(active_failed_reason_counts.most_common()),
            "legacy_failed_research_reasons": dict(legacy_failed_reason_counts.most_common()),
            "trainable_room_count": len(trainable_bundles),
            "cleaned_trainable_room_count": len(trainable_bundles),
            "evaluation_holdout_room_count": holdout_count,
            "room_origin_counts": dict(room_origin_counts),
            "pack_versions": pack_versions,
            "recent_dataset_builds": recent_builds,
            "campaign_settings": self._campaign_settings_snapshot(),
            "quality_debt_summary": quality_debt_summary,
            "strategy_audit_progress": audit_progress,
            "quality_exclusion_reasons": dict(exclusion_reason_counts.most_common()),
            "recent_exclusion_memory": recent_exclusion_memory,
            "settlement_maturity": settlement_maturity,
            "readiness": readiness.model_dump(mode="json"),
            "last_readiness_snapshot": latest_snapshot.payload if latest_snapshot is not None else None,
            "top_missing_data": readiness.missing_indicators,
            "top_blockers": top_blockers,
            "next_actions": next_actions,
        }

    async def get_dashboard_status(
        self,
        *,
        room_limit: int = 60,
        bundles: list[TrainingRoomBundle] | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        dashboard_bundles = list(bundles or [])
        if not dashboard_bundles:
            dashboard_bundles = await self.training_export_service.export_room_bundles(
                limit=room_limit,
                include_non_complete=False,
                origins=[RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value],
            )
        dashboard_bundles = await self._attach_strategy_audits(dashboard_bundles, persist_missing=False)

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_events = await repo.list_exchange_events(
                stream_name="reconcile",
                event_type="settlements",
                limit=200,
            )
            await session.commit()

        audits = [self._bundle_strategy_audit(bundle) for bundle in dashboard_bundles]
        quality_debt_summary = {
            "stale_mismatch_count": sum(1 for audit in audits if audit.stale_data_mismatch),
            "missed_stand_down_count": sum(1 for audit in audits if audit.missed_stand_down),
            "weak_resolved_trade_count": sum(
                1
                for audit in audits
                if audit.trade_quality == "weak_trade"
                and audit.resolution_state in {WeatherResolutionState.LOCKED_NO.value, WeatherResolutionState.LOCKED_YES.value}
            ),
            "cleaned_trainable_room_count": sum(1 for bundle in dashboard_bundles if bundle.trainable_default is not False),
        }
        quality_debt_summary.update(
            self._recent_quality_debt(
                dashboard_bundles,
                window_hours=RECENT_QUALITY_WINDOW_HOURS,
                now=now,
            )
        )
        quality_debt_summary["recent_window_hours"] = RECENT_QUALITY_WINDOW_HOURS

        recent_exclusion_memory = self._recent_exclusion_memory(
            dashboard_bundles,
            window_hours=self.settings.training_campaign_lookback_hours,
            now=now,
        )
        settlement_maturity = self._settlement_focus_summary_from_bundles(
            dashboard_bundles,
            settlement_events=settlement_events,
            now=now,
            limit=SETTLEMENT_BACKLOG_PREVIEW_LIMIT,
        )
        readiness = self._readiness_for_bundles(
            [bundle for bundle in dashboard_bundles if bundle.trainable_default is not False]
        )
        top_blockers = self._top_blockers(
            readiness=readiness,
            quality_debt_summary=quality_debt_summary,
            settlement_maturity=settlement_maturity,
            recent_exclusion_memory=recent_exclusion_memory,
        )
        next_actions = self._next_actions(
            readiness=readiness,
            unsettled_count=sum(1 for bundle in dashboard_bundles if not bundle.outcome.settlement_seen),
            settlement_maturity=settlement_maturity,
            quality_debt_summary=quality_debt_summary,
            recent_exclusion_memory=recent_exclusion_memory,
        )

        return {
            "room_count": len(dashboard_bundles),
            "quality_debt_summary": quality_debt_summary,
            "top_blockers": top_blockers,
            "next_actions": next_actions,
        }

    async def build_dataset(self, request: TrainingBuildRequest) -> dict[str, Any]:
        candidate_request = request.model_copy(update={"quality_cleaned_only": False})
        candidate_bundles = await self._selected_bundles(candidate_request, persist_missing_audits=True)
        bundles = self._filter_bundles_by_request(candidate_bundles, request)
        selected_bundles = self._apply_mode_slice(request.mode, bundles)
        if request.mode == "role-sft":
            export_records = [
                example.model_dump(mode="json")
                for bundle in selected_bundles
                for example in self.training_export_service.build_role_training_examples(bundle)
            ]
        else:
            export_records = [bundle.model_dump(mode="json") for bundle in selected_bundles]

        build_version = f"{request.mode}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
        room_items = [self._dataset_item(bundle) for bundle in selected_bundles]
        selection_window_start, selection_window_end = self._selection_window(selected_bundles)
        label_stats = self._label_stats(selected_bundles, candidate_bundles=candidate_bundles)
        pack_versions = sorted(
            {
                bundle.room.get("agent_pack_version")
                for bundle in selected_bundles
                if bundle.room.get("agent_pack_version")
            }
        )
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            record = await repo.create_training_dataset_build(
                build_version=build_version,
                mode=request.mode,
                status="completed",
                selection_window_start=selection_window_start,
                selection_window_end=selection_window_end,
                room_count=len(selected_bundles),
                filters=request.model_dump(mode="json"),
                label_stats=label_stats,
                pack_versions=pack_versions,
                payload={
                    "room_ids": [bundle.room["id"] for bundle in selected_bundles],
                    "export_record_count": len(export_records),
                },
                completed_at=datetime.now(UTC),
            )
            await repo.set_training_dataset_build_items(dataset_build_id=record.id, items=room_items)
            await session.commit()
        output_path = None
        if request.output:
            output_path = Path(request.output)
            _write_jsonl(output_path, export_records)
        return {
            "build": self._summary_from_values(
                id=record.id,
                build_version=build_version,
                mode=request.mode,
                status="completed",
                room_count=len(selected_bundles),
                filters=request.model_dump(mode="json"),
                label_stats=label_stats,
                pack_versions=pack_versions,
                created_at=record.created_at,
                completed_at=record.completed_at,
            ).model_dump(mode="json"),
            "output": str(output_path) if output_path is not None else None,
            "export_record_count": len(export_records),
        }

    async def list_builds(self, *, limit: int = 20) -> list[TrainingDatasetBuildSummary]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            records = await repo.list_training_dataset_builds(limit=limit)
            await session.commit()
        return [self._summary_from_record(record) for record in records]

    async def compute_readiness(self, *, persist: bool = False) -> TrainingReadiness:
        bundles = await self._selected_bundles(
            TrainingBuildRequest(
                mode="room-bundles",
                limit=self.settings.training_status_room_limit,
                days=self.settings.training_window_days,
                good_research_only=False,
                quality_cleaned_only=True,
            )
        )
        readiness = self._readiness_for_bundles(bundles)
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.create_training_readiness_snapshot(readiness)
                await session.commit()
        return readiness

    async def get_settlement_focus_summary(self, *, limit: int = SETTLEMENT_BACKLOG_PREVIEW_LIMIT) -> dict[str, Any]:
        request = TrainingBuildRequest(
            mode="room-bundles",
            limit=self.settings.training_status_room_limit,
            days=self.settings.training_window_days,
            good_research_only=False,
            quality_cleaned_only=False,
        )
        bundles = await self._selected_bundles(request)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_events = await repo.list_exchange_events(
                stream_name="reconcile",
                event_type="settlements",
                limit=200,
            )
            await session.commit()
        return self._settlement_focus_summary_from_bundles(
            bundles,
            settlement_events=settlement_events,
            now=datetime.now(UTC),
            limit=limit,
        )

    async def research_audit(self, *, limit: int = 100) -> list[ResearchAuditIssue]:
        discoveries = await self.discovery_service.discover_configured_markets()
        discovery_by_market = {item.mapping.market_ticker: item for item in discoveries}
        monitored_mappings = {
            item.mapping.market_ticker: item.mapping
            for item in discoveries
            if item.mapping.market_type == "weather"
        }
        for mapping in self.weather_directory.all():
            if mapping.market_type != "weather":
                continue
            monitored_mappings.setdefault(mapping.market_ticker, mapping)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            dossiers = {record.market_ticker: record for record in await repo.list_research_dossiers(limit=limit * 4)}
            recent_runs = await repo.list_research_runs(limit=limit * 16)
            await session.commit()

        recent_runs_by_market = defaultdict(list)
        for run in recent_runs:
            recent_runs_by_market[run.market_ticker].append(run)
        issues: list[ResearchAuditIssue] = []
        for mapping in monitored_mappings.values():
            if not mapping.supports_structured_weather:
                issues.append(
                    ResearchAuditIssue(
                        market_ticker=mapping.market_ticker,
                        severity="high",
                        code="missing_structured_fields",
                        summary="Structured weather mapping is incomplete.",
                        details={"mapping": mapping.model_dump(mode="json")},
                    )
                )
            discovery = discovery_by_market.get(mapping.market_ticker)
            if discovery is not None and discovery.status == "error":
                issues.append(
                    ResearchAuditIssue(
                        market_ticker=mapping.market_ticker,
                        severity="high",
                        code="market_lookup_error",
                        summary="Configured weather market could not be discovered.",
                        details={"notes": discovery.notes, "raw": discovery.raw},
                    )
                )
            failure_streak = self._research_failure_streak(recent_runs_by_market.get(mapping.market_ticker, []))
            if failure_streak["failure_count"] >= 3:
                issues.append(
                    ResearchAuditIssue(
                        market_ticker=mapping.market_ticker,
                        severity="medium",
                        code="repeated_refresh_failures",
                        summary="Research refresh is repeatedly failing for this market.",
                        details=failure_streak,
                    )
                )
            dossier_record = dossiers.get(mapping.market_ticker)
            if dossier_record is not None:
                payload = dossier_record.payload
                freshness = ((payload.get("freshness") or {}) if isinstance(payload, dict) else {})
                if freshness.get("stale"):
                    issues.append(
                        ResearchAuditIssue(
                            market_ticker=mapping.market_ticker,
                            severity="medium",
                            code="stale_dossier",
                            summary="Latest research dossier is stale.",
                            details={"freshness": freshness},
                        )
                    )
                if not bool(payload.get("settlement_covered")):
                    issues.append(
                        ResearchAuditIssue(
                            market_ticker=mapping.market_ticker,
                            severity="medium",
                            code="settlement_gap",
                            summary="Settlement mechanics are not fully covered in the current dossier.",
                            details={"status": payload.get("status"), "mode": payload.get("mode")},
                        )
                    )
                quality = ((payload.get("quality") or {}) if isinstance(payload, dict) else {})
                if float(quality.get("structured_completeness_score") or 0.0) < 1.0 and mapping.supports_structured_weather:
                    issues.append(
                        ResearchAuditIssue(
                            market_ticker=mapping.market_ticker,
                            severity="medium",
                            code="structured_weather_incomplete",
                            summary="Structured weather facts are incomplete in the latest dossier.",
                            details={"quality": quality},
                    )
                )
        return issues[:limit]

    @staticmethod
    def _research_failure_streak(runs: list[Any]) -> dict[str, Any]:
        failure_count = 0
        latest_failed_at: str | None = None
        last_success_at: str | None = None
        for run in runs:
            status = str(getattr(run, "status", "") or "")
            finished_at = getattr(run, "finished_at", None) or getattr(run, "started_at", None)
            if status == "completed":
                last_success_at = finished_at.astimezone(UTC).isoformat() if finished_at is not None else None
                break
            if status == "failed":
                failure_count += 1
                if latest_failed_at is None and finished_at is not None:
                    latest_failed_at = finished_at.astimezone(UTC).isoformat()
        return {
            "failure_count": failure_count,
            "latest_failed_at": latest_failed_at,
            "last_success_at": last_success_at,
        }

    async def strategy_audit_room(self, room_id: str) -> StrategyAuditResult:
        bundle = await self.training_export_service.build_room_bundle(room_id)
        if bundle.strategy_audit is not None:
            return self._bundle_strategy_audit(bundle)
        audit = self._audit_bundle(bundle, audit_source="historical_backfill")
        await self._persist_audit(audit)
        return audit

    async def strategy_audit_summary(self, *, days: int | None = None, limit: int = 100) -> StrategyAuditSummary:
        request = TrainingBuildRequest(
            mode="room-bundles",
            limit=limit,
            days=days or self.settings.training_window_days,
            good_research_only=False,
            quality_cleaned_only=False,
        )
        bundles = await self._selected_bundles(request)
        audits = [self._bundle_strategy_audit(bundle) for bundle in bundles]
        thesis_counts = Counter(audit.thesis_correctness for audit in audits)
        trade_quality_counts = Counter(audit.trade_quality for audit in audits)
        block_correctness_counts = Counter(audit.block_correctness for audit in audits)
        exclusion_reason_counts = Counter(audit.exclude_reason for audit in audits if audit.exclude_reason)
        return StrategyAuditSummary(
            room_count=len(audits),
            audited_room_count=sum(1 for audit in audits if audit.audit_source in {"live_forward", "historical_backfill"}),
            forward_audit_count=sum(1 for audit in audits if audit.audit_source == "live_forward"),
            backfilled_audit_count=sum(1 for audit in audits if audit.audit_source == "historical_backfill"),
            stale_mismatch_count=sum(1 for audit in audits if audit.stale_data_mismatch),
            low_upside_proposal_count=sum(1 for audit in audits if audit.trade_quality == "weak_trade"),
            resolved_contract_proposal_count=sum(
                1
                for audit in audits
                if audit.resolution_state in {WeatherResolutionState.LOCKED_NO.value, WeatherResolutionState.LOCKED_YES.value}
                and audit.trade_quality == "weak_trade"
            ),
            missed_stand_down_count=sum(1 for audit in audits if audit.missed_stand_down),
            cleaned_trainable_room_count=sum(1 for audit in audits if audit.trainable_default),
            exclusion_reason_counts=dict(exclusion_reason_counts.most_common()),
            thesis_counts=dict(thesis_counts),
            trade_quality_counts=dict(trade_quality_counts),
            block_correctness_counts=dict(block_correctness_counts),
            samples=audits[: min(10, len(audits))],
        )

    async def backfill_strategy_audits(self, *, days: int = 30, limit: int = 200) -> dict[str, Any]:
        request = TrainingBuildRequest(
            mode="room-bundles",
            limit=limit,
            days=days,
            good_research_only=False,
            quality_cleaned_only=False,
        )
        bundles = await self._selected_bundles(request, persist_missing_audits=False)
        created = 0
        updated = 0
        audits: list[StrategyAuditResult] = []
        for bundle in bundles:
            existing = bundle.audit_source in {"live_forward", "historical_backfill"}
            audit = self._audit_bundle(bundle, audit_source="historical_backfill")
            await self._persist_audit(audit)
            audits.append(audit)
            if existing:
                updated += 1
            else:
                created += 1
        return {
            "status": "completed",
            "days": days,
            "room_count": len(bundles),
            "created_count": created,
            "updated_count": updated,
            "audit_version": STRATEGY_AUDIT_VERSION,
            "samples": [audit.model_dump(mode="json") for audit in audits[: min(10, len(audits))]],
        }

    async def persist_strategy_audit_for_room(self, room_id: str, *, audit_source: str = "live_forward") -> StrategyAuditResult:
        bundle = await self.training_export_service.build_room_bundle(room_id)
        audit = self._audit_bundle(bundle, audit_source=audit_source)
        await self._persist_audit(audit)
        return audit

    async def select_learning_room_ids(
        self,
        *,
        days: int,
        limit: int,
        settled_only: bool = False,
        good_research_only: bool = True,
    ) -> list[str]:
        request = TrainingBuildRequest(
            mode="room-bundles",
            limit=limit,
            days=days,
            settled_only=settled_only,
            good_research_only=good_research_only,
        )
        bundles = await self._selected_bundles(request)
        return [bundle.room["id"] for bundle in bundles]

    async def _selected_bundles(
        self,
        request: TrainingBuildRequest,
        *,
        persist_missing_audits: bool = False,
    ) -> list[TrainingRoomBundle]:
        since = datetime.now(UTC) - timedelta(days=request.days)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            room_limit = max(request.limit * 5, request.limit, 50)
            rooms = await repo.list_rooms_for_learning(
                since=since,
                limit=room_limit,
                market_ticker=request.market_ticker,
            )
            await session.commit()
        bundles = await self.training_export_service.export_room_bundles(
            room_ids=[room.id for room in rooms],
            limit=len(rooms),
            include_non_complete=request.include_non_complete,
        )
        audited_bundles = await self._attach_strategy_audits(bundles, persist_missing=persist_missing_audits)
        return self._filter_bundles_by_request(audited_bundles, request)

    async def _attach_strategy_audits(
        self,
        bundles: list[TrainingRoomBundle],
        *,
        persist_missing: bool,
    ) -> list[TrainingRoomBundle]:
        attached: list[TrainingRoomBundle] = []
        for bundle in bundles:
            if bundle.strategy_audit is not None:
                attached.append(bundle)
                continue
            audit = self._audit_bundle(bundle, audit_source="historical_backfill")
            if persist_missing:
                await self._persist_audit(audit)
                next_source = audit.audit_source
                next_version = audit.audit_version
                next_trainable = audit.trainable_default
                next_exclude_reason = audit.exclude_reason
                next_payload = audit.model_dump(mode="json")
            else:
                preview_audit = audit.model_copy(update={"audit_source": "computed_preview"})
                next_source = preview_audit.audit_source
                next_version = preview_audit.audit_version
                next_trainable = preview_audit.trainable_default
                next_exclude_reason = preview_audit.exclude_reason
                next_payload = preview_audit.model_dump(mode="json")
            attached.append(
                bundle.model_copy(
                    update={
                        "strategy_audit": next_payload,
                        "audit_source": next_source,
                        "audit_version": next_version,
                        "trainable_default": next_trainable,
                        "exclude_reason": next_exclude_reason,
                    }
                )
            )
        return attached

    def _filter_bundles_by_request(
        self,
        bundles: list[TrainingRoomBundle],
        request: TrainingBuildRequest,
    ) -> list[TrainingRoomBundle]:
        filtered: list[TrainingRoomBundle] = []
        for bundle in bundles:
            if request.good_research_only and not bool((bundle.research_health or {}).get("good_for_training")):
                continue
            if request.settled_only and not bundle.outcome.settlement_seen:
                continue
            if request.quality_cleaned_only and bundle.trainable_default is False:
                continue
            if not request.include_non_complete and bundle.room.get("stage") != "complete":
                continue
            filtered.append(bundle)
            if len(filtered) >= request.limit:
                break
        return filtered

    def _apply_mode_slice(self, mode: str, bundles: list[TrainingRoomBundle]) -> list[TrainingRoomBundle]:
        if mode != "evaluation-holdout":
            return bundles
        if not bundles:
            return []
        holdout_count = max(1, int(len(bundles) * self.settings.self_improve_holdout_ratio))
        return bundles[-holdout_count:]

    def _readiness_for_bundles(self, bundles: list[TrainingRoomBundle]) -> TrainingReadiness:
        complete_room_count = len(bundles)
        market_diversity_count = len({bundle.room["market_ticker"] for bundle in bundles})
        settled_room_count = sum(1 for bundle in bundles if bundle.outcome.settlement_seen)
        trade_positive_room_count = sum(1 for bundle in bundles if bundle.outcome.ticket_generated)
        city_counts = Counter(
            str((bundle.campaign or {}).get("city_bucket") or bundle.room["market_ticker"])
            for bundle in bundles
        )
        market_counts = Counter(bundle.room["market_ticker"] for bundle in bundles)
        missing: list[str] = []
        if complete_room_count < self.settings.training_min_complete_rooms:
            missing.append("not enough complete rooms")
        if market_diversity_count < self.settings.training_min_market_diversity:
            missing.append("not enough market diversity")
        if settled_room_count < self.settings.training_min_settled_rooms:
            missing.append("not enough settled rooms")
        if trade_positive_room_count < self.settings.training_min_trade_positive_rooms:
            missing.append("not enough trade-positive rooms")
        if complete_room_count and trade_positive_room_count <= max(1, int(complete_room_count * 0.1)):
            missing.append("too many no-trade examples")
        if market_counts and market_counts.most_common(1)[0][1] > max(2, int(complete_room_count * 0.5)):
            missing.append("too many rooms from one market")
        if city_counts and city_counts.most_common(1)[0][1] > max(2, int(complete_room_count * 0.6)):
            missing.append("too many rooms from one city or strike regime")
        ready_for_sft_export = (
            complete_room_count >= self.settings.training_min_complete_rooms
            and market_diversity_count >= self.settings.training_min_market_diversity
        )
        ready_for_critique = ready_for_sft_export and trade_positive_room_count >= self.settings.training_min_trade_positive_rooms
        ready_for_evaluation = ready_for_critique and settled_room_count >= self.settings.training_min_settled_rooms
        ready_for_promotion = ready_for_evaluation
        return TrainingReadiness(
            complete_room_count=complete_room_count,
            market_diversity_count=market_diversity_count,
            settled_room_count=settled_room_count,
            trade_positive_room_count=trade_positive_room_count,
            ready_for_sft_export=ready_for_sft_export,
            ready_for_critique=ready_for_critique,
            ready_for_evaluation=ready_for_evaluation,
            ready_for_promotion=ready_for_promotion,
            missing_indicators=missing,
            thresholds={
                "training_min_complete_rooms": self.settings.training_min_complete_rooms,
                "training_min_market_diversity": self.settings.training_min_market_diversity,
                "training_min_settled_rooms": self.settings.training_min_settled_rooms,
                "training_min_trade_positive_rooms": self.settings.training_min_trade_positive_rooms,
            },
            stats={
                "good_research_room_count": sum(1 for bundle in bundles if bool((bundle.research_health or {}).get("good_for_training"))),
                "research_gate_pass_rate": round(
                    sum(1 for bundle in bundles if bundle.outcome.research_gate_passed is True) / len(bundles),
                    4,
                )
                if bundles
                else 0.0,
                "settled_label_coverage": round(settled_room_count / len(bundles), 4) if bundles else 0.0,
                "dominant_market_share": round((market_counts.most_common(1)[0][1] / len(bundles)), 4) if bundles else 0.0,
                "dominant_city_share": round((city_counts.most_common(1)[0][1] / len(bundles)), 4) if bundles else 0.0,
            },
        )

    def _bundle_strategy_audit(self, bundle: TrainingRoomBundle) -> StrategyAuditResult:
        if bundle.strategy_audit is not None:
            return StrategyAuditResult.model_validate(bundle.strategy_audit)
        return self._audit_bundle(bundle)

    def _audit_bundle(
        self,
        bundle: TrainingRoomBundle,
        *,
        audit_source: str | None = None,
    ) -> StrategyAuditResult:
        signal = bundle.signal or {}
        signal_payload = signal.get("payload") or {}
        risk = bundle.risk_verdict or {}
        risk_reasons = [str(reason) for reason in ((risk.get("reasons") if isinstance(risk, dict) else None) or [])]
        eligibility = signal_payload.get("eligibility") if isinstance(signal_payload, dict) else None
        resolution_state = self._bundle_resolution_state(bundle)
        thesis_correctness = self._thesis_correctness(bundle, resolution_state)
        trade_quality = self._trade_quality(bundle, resolution_state, signal_payload)
        stale_mismatch = bool(
            bundle.outcome.research_gate_passed
            and any("stale" in reason.lower() for reason in risk_reasons)
            and not (isinstance(eligibility, dict) and eligibility.get("research_stale"))
        )
        effective_freshness_agreement = not stale_mismatch
        missed_stand_down = trade_quality == "weak_trade" and bundle.outcome.ticket_generated
        if bundle.outcome.blocked_by == "risk":
            block_correctness = "correct_block" if risk_reasons else "blocked"
        elif bundle.outcome.blocked_by == "eligibility":
            block_correctness = "early_stand_down"
        elif bundle.outcome.blocked_by == "research_gate":
            block_correctness = "research_gate_block"
        else:
            block_correctness = "not_applicable"

        reasons: list[str] = []
        if thesis_correctness == "correct" and trade_quality == "weak_trade":
            reasons.append("Directional thesis was reasonable, but the setup should have stood down earlier.")
        if resolution_state != WeatherResolutionState.UNRESOLVED.value:
            reasons.append(f"Observed weather state implies {resolution_state}.")
        if stale_mismatch:
            reasons.append("Room allowed analysis through research gate but stale data only surfaced downstream in risk.")
        if isinstance(eligibility, dict) and eligibility.get("stand_down_reason"):
            reasons.append(f"Eligibility stand-down reason: {eligibility['stand_down_reason']}.")
        reasons.extend(risk_reasons[:2])

        quality_warnings = self._quality_warnings(
            trade_quality=trade_quality,
            resolution_state=resolution_state,
            missed_stand_down=missed_stand_down,
            stale_data_mismatch=stale_mismatch,
        )
        exclude_reason = self._exclude_reason(
            trade_quality=trade_quality,
            resolution_state=resolution_state,
            missed_stand_down=missed_stand_down,
            stale_data_mismatch=stale_mismatch,
        )
        trainable_default = exclude_reason is None

        return StrategyAuditResult(
            room_id=bundle.room["id"],
            market_ticker=bundle.room["market_ticker"],
            thesis_correctness=thesis_correctness,
            trade_quality=trade_quality,
            block_correctness=block_correctness,
            missed_stand_down=missed_stand_down,
            stale_data_mismatch=stale_mismatch,
            effective_freshness_agreement=effective_freshness_agreement,
            resolution_state=resolution_state,
            eligibility_passed=bundle.outcome.eligibility_passed,
            stand_down_reason=bundle.outcome.stand_down_reason,
            blocked_by=bundle.outcome.blocked_by,
            audit_source=audit_source,
            audit_version=STRATEGY_AUDIT_VERSION,
            trainable_default=trainable_default,
            exclude_reason=exclude_reason,
            quality_warnings=quality_warnings,
            trade_regime=(
                str(signal_payload.get("trade_regime"))
                if isinstance(signal_payload, dict) and signal_payload.get("trade_regime")
                else None
            ),
            capital_bucket=(
                str(signal_payload.get("capital_bucket"))
                if isinstance(signal_payload, dict) and signal_payload.get("capital_bucket")
                else None
            ),
            model_quality_status=(
                str(signal_payload.get("model_quality_status") or "pass")
                if isinstance(signal_payload, dict)
                else "pass"
            ),
            model_quality_reasons=(
                [str(reason) for reason in (signal_payload.get("model_quality_reasons") or [])]
                if isinstance(signal_payload, dict)
                else []
            ),
            recommended_size_cap_fp=(
                signal_payload.get("recommended_size_cap_fp")
                if isinstance(signal_payload, dict)
                else None
            ),
            warn_only_blocked=bool(signal_payload.get("warn_only_blocked")) if isinstance(signal_payload, dict) else False,
            audited_at=datetime.now(UTC),
            reasons=reasons,
        )

    def _bundle_resolution_state(self, bundle: TrainingRoomBundle) -> str:
        signal = bundle.signal or {}
        signal_payload = signal.get("payload") or {}
        resolution = signal_payload.get("resolution_state")
        if resolution:
            return str(resolution)
        dossier = bundle.research_dossier or {}
        trader_context = dossier.get("trader_context") or {}
        resolution = trader_context.get("resolution_state")
        if resolution:
            return str(resolution)
        weather_bundle = bundle.weather_bundle or {}
        mapping = weather_bundle.get("mapping") or {}
        operator = str(mapping.get("operator") or "")
        threshold = mapping.get("threshold_f")
        observation = ((weather_bundle.get("observation") or {}).get("properties") or {}).get("temperature") or {}
        current_c = observation.get("value")
        current_temp_f = None if current_c in (None, "") else (float(current_c) * 9 / 5) + 32
        if threshold is None or current_temp_f is None:
            return WeatherResolutionState.UNRESOLVED.value
        if operator in (">", ">=") and current_temp_f >= float(threshold):
            return WeatherResolutionState.LOCKED_YES.value
        if operator in ("<", "<=") and current_temp_f > float(threshold):
            return WeatherResolutionState.LOCKED_NO.value
        return WeatherResolutionState.UNRESOLVED.value

    def _thesis_correctness(self, bundle: TrainingRoomBundle, resolution_state: str) -> str:
        fair_yes = bundle.signal.get("fair_yes_dollars") if bundle.signal else None
        try:
            fair_yes_value = Decimal(str(fair_yes)) if fair_yes not in (None, "") else None
        except Exception:
            fair_yes_value = None
        if resolution_state == WeatherResolutionState.LOCKED_NO.value:
            if fair_yes_value is not None and fair_yes_value <= Decimal("0.5000"):
                return "correct"
            return "incorrect"
        if resolution_state == WeatherResolutionState.LOCKED_YES.value:
            if fair_yes_value is not None and fair_yes_value >= Decimal("0.5000"):
                return "correct"
            return "incorrect"
        return "unresolved"

    def _trade_quality(self, bundle: TrainingRoomBundle, resolution_state: str, signal_payload: dict[str, Any]) -> str:
        if not bundle.outcome.ticket_generated:
            return "stand_down"
        if resolution_state != WeatherResolutionState.UNRESOLVED.value:
            return "weak_trade"
        eligibility = signal_payload.get("eligibility") if isinstance(signal_payload, dict) else None
        if isinstance(eligibility, dict):
            remaining = eligibility.get("remaining_payout_dollars")
            try:
                if remaining is not None and Decimal(str(remaining)) < Decimal("0.0300"):
                    return "weak_trade"
            except Exception:
                pass
            spread = eligibility.get("market_spread_bps")
            if spread is not None and int(spread) > self.settings.trigger_max_spread_bps:
                return "weak_trade"
        return "good_trade"

    @staticmethod
    def _quality_warnings(
        *,
        trade_quality: str,
        resolution_state: str | None,
        missed_stand_down: bool,
        stale_data_mismatch: bool,
    ) -> list[str]:
        warnings: list[str] = []
        if stale_data_mismatch:
            warnings.append("stale_data_mismatch")
        if missed_stand_down:
            warnings.append("missed_stand_down")
        if trade_quality == "weak_trade":
            warnings.append("weak_trade")
        if resolution_state in {WeatherResolutionState.LOCKED_NO.value, WeatherResolutionState.LOCKED_YES.value}:
            warnings.append("resolved_contract")
        return warnings

    @staticmethod
    def _exclude_reason(
        *,
        trade_quality: str,
        resolution_state: str | None,
        missed_stand_down: bool,
        stale_data_mismatch: bool,
    ) -> str | None:
        if stale_data_mismatch:
            return "stale_data_mismatch"
        if resolution_state in {WeatherResolutionState.LOCKED_NO.value, WeatherResolutionState.LOCKED_YES.value} and trade_quality == "weak_trade":
            return "resolved_contract_proposal"
        if missed_stand_down:
            return "missed_stand_down"
        return None

    async def _persist_audit(self, audit: StrategyAuditResult) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.upsert_room_strategy_audit(
                room_id=audit.room_id,
                market_ticker=audit.market_ticker,
                audit_source=audit.audit_source or "historical_backfill",
                audit_version=audit.audit_version or STRATEGY_AUDIT_VERSION,
                thesis_correctness=audit.thesis_correctness,
                trade_quality=audit.trade_quality,
                block_correctness=audit.block_correctness,
                missed_stand_down=audit.missed_stand_down,
                stale_data_mismatch=audit.stale_data_mismatch,
                effective_freshness_agreement=audit.effective_freshness_agreement,
                resolution_state=audit.resolution_state,
                eligibility_passed=audit.eligibility_passed,
                stand_down_reason=audit.stand_down_reason,
                trainable_default=audit.trainable_default,
                exclude_reason=audit.exclude_reason,
                quality_warnings=audit.quality_warnings,
                payload=audit.model_dump(mode="json"),
            )
            await session.commit()

    def _dataset_item(self, bundle: TrainingRoomBundle) -> dict[str, Any]:
        audit = self._bundle_strategy_audit(bundle)
        return {
            "room_id": bundle.room["id"],
            "market_ticker": bundle.room["market_ticker"],
            "agent_pack_version": bundle.room.get("agent_pack_version"),
            "campaign": bundle.campaign,
            "research_health": bundle.research_health,
            "strategy_audit": audit.model_dump(mode="json"),
            "audit_source": audit.audit_source,
            "audit_version": audit.audit_version,
            "trainable_default": audit.trainable_default,
            "exclude_reason": audit.exclude_reason,
            "labels": {
                "research_gate_passed": bundle.outcome.research_gate_passed,
                "trade_proposed": bundle.outcome.ticket_generated,
                "risk_blocked": bundle.outcome.risk_status == "blocked" or bundle.outcome.final_status == "blocked",
                "orders_present": bundle.outcome.orders_submitted > 0,
                "fills_present": bundle.outcome.fills_observed > 0,
                "settlement_seen": bundle.outcome.settlement_seen,
                "settlement_pnl_dollars": (
                    str(bundle.outcome.settlement_pnl_dollars)
                    if bundle.outcome.settlement_pnl_dollars is not None
                    else None
                ),
                "good_for_training": bool((bundle.research_health or {}).get("good_for_training")),
            },
        }

    def _label_stats(
        self,
        bundles: list[TrainingRoomBundle],
        *,
        candidate_bundles: list[TrainingRoomBundle] | None = None,
    ) -> dict[str, Any]:
        candidates = candidate_bundles or bundles
        selected_room_ids = {selected.room["id"] for selected in bundles}
        excluded_audits = [
            self._bundle_strategy_audit(bundle)
            for bundle in candidates
            if bundle.room["id"] not in selected_room_ids
        ]
        return {
            "research_gate_passed": sum(1 for bundle in bundles if bundle.outcome.research_gate_passed is True),
            "trade_proposed": sum(1 for bundle in bundles if bundle.outcome.ticket_generated),
            "risk_blocked": sum(
                1 for bundle in bundles if bundle.outcome.risk_status == "blocked" or bundle.outcome.final_status == "blocked"
            ),
            "orders_present": sum(1 for bundle in bundles if bundle.outcome.orders_submitted > 0),
            "fills_present": sum(1 for bundle in bundles if bundle.outcome.fills_observed > 0),
            "settlement_seen": sum(1 for bundle in bundles if bundle.outcome.settlement_seen),
            "good_research": sum(1 for bundle in bundles if bool((bundle.research_health or {}).get("good_for_training"))),
            "excluded_weak_trades": sum(1 for audit in excluded_audits if audit.trade_quality == "weak_trade"),
            "excluded_stale_mismatches": sum(1 for audit in excluded_audits if audit.stale_data_mismatch),
            "excluded_resolved_contract_proposals": sum(
                1 for audit in excluded_audits if audit.exclude_reason == "resolved_contract_proposal"
            ),
            "forward_audit_count": sum(1 for bundle in bundles if bundle.audit_source == "live_forward"),
            "backfilled_audit_count": sum(1 for bundle in bundles if bundle.audit_source == "historical_backfill"),
        }

    def _selection_window(
        self,
        bundles: list[TrainingRoomBundle],
    ) -> tuple[datetime | None, datetime | None]:
        if not bundles:
            return None, None
        created_at = [datetime.fromisoformat(bundle.room["created_at"]) for bundle in bundles]
        return min(created_at), max(created_at)

    def _summary_from_record(self, record: Any) -> TrainingDatasetBuildSummary:
        return self._summary_from_values(
            id=record.id,
            build_version=record.build_version,
            mode=record.mode,
            status=record.status,
            room_count=record.room_count,
            filters=record.filters,
            label_stats=record.label_stats,
            pack_versions=record.pack_versions,
            created_at=record.created_at,
            completed_at=record.completed_at,
        )

    @staticmethod
    def _summary_from_values(
        *,
        id: str,
        build_version: str,
        mode: str,
        status: str,
        room_count: int,
        filters: dict[str, Any],
        label_stats: dict[str, Any],
        pack_versions: list[str],
        created_at: datetime,
        completed_at: datetime | None,
    ) -> TrainingDatasetBuildSummary:
        return TrainingDatasetBuildSummary(
            id=id,
            build_version=build_version,
            mode=mode,
            status=status,
            room_count=room_count,
            filters=filters,
            label_stats=label_stats,
            pack_versions=pack_versions,
            created_at=created_at,
            completed_at=completed_at,
        )

    @staticmethod
    def _failed_reason_bucket(error_text: str | None) -> str:
        text = (error_text or "").lower()
        if "404" in text or "not found" in text:
            return "market lookup failures"
        if "settlement" in text:
            return "settlement coverage failures"
        if "weather" in text:
            return "weather source failures"
        if not text:
            return "unknown"
        return "other"

    def _partition_failed_reason_counts(self, failed_runs: list[Any]) -> tuple[Counter[str], Counter[str]]:
        active = Counter()
        legacy = Counter()
        for run in failed_runs:
            bucket = self._failed_reason_bucket(getattr(run, "error_text", None))
            market_ticker = getattr(run, "market_ticker", None)
            if self._is_supported_market_ticker(market_ticker):
                active[bucket] += 1
            else:
                legacy[bucket] += 1
        return active, legacy

    def _is_supported_market_ticker(self, market_ticker: str | None) -> bool:
        if not market_ticker:
            return False
        return self.weather_directory.supports_market_ticker(market_ticker)

    def _oldest_unsettled_age_seconds(self, bundles: list[TrainingRoomBundle]) -> int | None:
        now = datetime.now(UTC)
        ages = [
            int((now - created_at).total_seconds())
            for bundle in bundles
            if not bundle.outcome.settlement_seen
            for created_at in [self._room_created_at(bundle)]
            if created_at is not None
        ]
        if not ages:
            return None
        return max(ages)

    @staticmethod
    def _room_created_at(bundle: TrainingRoomBundle) -> datetime | None:
        created_at = bundle.room.get("created_at")
        if isinstance(created_at, datetime):
            if created_at.tzinfo is None:
                return created_at.replace(tzinfo=UTC)
            return created_at
        if isinstance(created_at, str) and created_at:
            parsed = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed
        return None

    def _campaign_settings_snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self.settings.training_campaign_enabled,
            "rooms_per_run": self.settings.training_campaign_rooms_per_run,
            "lookback_hours": self.settings.training_campaign_lookback_hours,
            "cooldown_seconds": self.settings.training_campaign_cooldown_seconds,
            "max_recent_per_market": self.settings.training_campaign_max_recent_per_market,
            "daemon_reconcile_interval_seconds": self.settings.daemon_reconcile_interval_seconds,
        }

    def _recent_quality_debt(
        self,
        bundles: list[TrainingRoomBundle],
        *,
        window_hours: int,
        now: datetime,
    ) -> dict[str, int]:
        cutoff = now - timedelta(hours=window_hours)
        recent_audits = [
            self._bundle_strategy_audit(bundle)
            for bundle in bundles
            if (created_at := self._room_created_at(bundle)) is not None and created_at >= cutoff
        ]
        return {
            "recent_stale_mismatch_count": sum(1 for audit in recent_audits if audit.stale_data_mismatch),
            "recent_missed_stand_down_count": sum(1 for audit in recent_audits if audit.missed_stand_down),
            "recent_weak_resolved_trade_count": sum(
                1
                for audit in recent_audits
                if audit.trade_quality == "weak_trade"
                and audit.resolution_state in {WeatherResolutionState.LOCKED_NO.value, WeatherResolutionState.LOCKED_YES.value}
            ),
        }

    def _cleaned_trainable_share_by_day(self, bundles: list[TrainingRoomBundle]) -> dict[str, dict[str, float | int]]:
        by_day: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "trainable": 0})
        for bundle in bundles:
            created_at = self._room_created_at(bundle)
            if created_at is None:
                continue
            day = created_at.date().isoformat()
            by_day[day]["total"] += 1
            if bundle.trainable_default is not False:
                by_day[day]["trainable"] += 1
        summary: dict[str, dict[str, float | int]] = {}
        for day, counts in sorted(by_day.items()):
            total = counts["total"]
            trainable = counts["trainable"]
            summary[day] = {
                "total": total,
                "trainable": trainable,
                "share": round(trainable / total, 4) if total else 0.0,
            }
        return summary

    def _recent_exclusion_memory(
        self,
        bundles: list[TrainingRoomBundle],
        *,
        window_hours: int,
        now: datetime,
    ) -> dict[str, Any]:
        cutoff = now - timedelta(hours=window_hours)
        by_market: dict[str, Counter[str]] = defaultdict(Counter)
        for bundle in bundles:
            if not bundle.exclude_reason:
                continue
            created_at = self._room_created_at(bundle)
            if created_at is None or created_at < cutoff:
                continue
            by_market[bundle.room["market_ticker"]][bundle.exclude_reason] += 1

        ranked_markets = sorted(
            by_market.items(),
            key=lambda item: (-sum(item[1].values()), item[0]),
        )
        return {
            "window_hours": window_hours,
            "by_reason": dict(
                Counter(
                    reason
                    for reason_counts in by_market.values()
                    for reason, count in reason_counts.items()
                    for _ in range(count)
                ).most_common()
            ),
            "by_market": [
                {
                    "market_ticker": market_ticker,
                    "count": sum(reason_counts.values()),
                    "reasons": dict(reason_counts.most_common()),
                }
                for market_ticker, reason_counts in ranked_markets[:5]
            ],
        }

    def _settlement_focus_summary_from_bundles(
        self,
        bundles: list[TrainingRoomBundle],
        *,
        settlement_events: list[Any],
        now: datetime,
        limit: int,
    ) -> dict[str, Any]:
        backlog: list[dict[str, Any]] = []
        backlog_by_market = Counter()
        backlog_by_day = Counter()
        status_counts = Counter()

        for bundle in bundles:
            if bundle.outcome.settlement_seen:
                continue
            created_at = self._room_created_at(bundle)
            close_at = self._bundle_close_at(bundle)
            status = self._settlement_backlog_status(close_at, now=now)
            entry = {
                "room_id": bundle.room["id"],
                "market_ticker": bundle.room["market_ticker"],
                "created_at": created_at.isoformat() if created_at is not None else None,
                "age_seconds": int((now - created_at).total_seconds()) if created_at is not None else None,
                "close_at": close_at.isoformat() if close_at is not None else None,
                "status": status,
                "blocked_by": bundle.outcome.blocked_by,
                "exclude_reason": bundle.exclude_reason,
            }
            backlog.append(entry)
            backlog_by_market[entry["market_ticker"]] += 1
            backlog_day = (close_at or created_at)
            if backlog_day is not None:
                backlog_by_day[backlog_day.date().isoformat()] += 1
            status_counts[status] += 1

        backlog.sort(
            key=lambda item: (
                -(item["age_seconds"] or -1),
                item["close_at"] or "9999-12-31T23:59:59+00:00",
                item["market_ticker"],
            )
        )
        near_settlement_count = sum(
            1 for entry in backlog if entry["status"] in {"near_settlement", "awaiting_settlement"}
        )

        settled_markets_by_window: dict[str, set[str]] = {"24h": set(), "7d": set()}
        for event in settlement_events:
            created_at = getattr(event, "created_at", None)
            if created_at is None:
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            else:
                created_at = created_at.astimezone(UTC)
            event_payload = getattr(event, "payload", {}) or {}
            settlements = event_payload.get("settlements")
            if not isinstance(settlements, list):
                continue
            if created_at >= now - timedelta(hours=24):
                settled_markets_by_window["24h"].update(
                    str(settlement.get("market_ticker") or settlement.get("ticker"))
                    for settlement in settlements
                    if isinstance(settlement, dict) and (settlement.get("market_ticker") or settlement.get("ticker"))
                )
            if created_at >= now - timedelta(days=7):
                settled_markets_by_window["7d"].update(
                    str(settlement.get("market_ticker") or settlement.get("ticker"))
                    for settlement in settlements
                    if isinstance(settlement, dict) and (settlement.get("market_ticker") or settlement.get("ticker"))
                )

        settled_label_velocity = {
            window: sum(
                1
                for bundle in bundles
                if bundle.outcome.settlement_seen and bundle.room["market_ticker"] in markets
            )
            for window, markets in settled_markets_by_window.items()
        }

        return {
            "unsettled_count": len(backlog),
            "near_settlement_count": near_settlement_count,
            "status_counts": dict(status_counts.most_common()),
            "backlog_by_market": dict(backlog_by_market.most_common()),
            "backlog_by_day": dict(sorted(backlog_by_day.items())),
            "settled_label_velocity": settled_label_velocity,
            "backlog": backlog[:limit],
        }

    @staticmethod
    def _bundle_close_at(bundle: TrainingRoomBundle) -> datetime | None:
        close_ts = None
        market = (bundle.market_snapshot or {}).get("market", bundle.market_snapshot or {})
        if isinstance(market, dict):
            close_ts = market.get("close_ts")
        if close_ts in (None, "") and isinstance(bundle.campaign, dict):
            close_ts = bundle.campaign.get("close_ts")
            if close_ts in (None, ""):
                payload = bundle.campaign.get("payload") or {}
                if isinstance(payload, dict):
                    close_ts = payload.get("close_ts")
        if close_ts in (None, ""):
            return TrainingCorpusService._ticker_close_at(bundle.room.get("market_ticker"))
        try:
            return datetime.fromtimestamp(int(close_ts), tz=UTC)
        except (TypeError, ValueError, OSError):
            return TrainingCorpusService._ticker_close_at(bundle.room.get("market_ticker"))

    @staticmethod
    def _ticker_close_at(market_ticker: Any) -> datetime | None:
        if not isinstance(market_ticker, str):
            return None
        parts = market_ticker.split("-")
        if len(parts) < 3:
            return None
        date_token = parts[1]
        if len(date_token) != 7:
            return None
        normalized = f"{date_token[:2]}{date_token[2:5].title()}{date_token[5:]}"
        try:
            parsed = datetime.strptime(normalized, "%y%b%d")
        except ValueError:
            return None
        return parsed.replace(tzinfo=UTC, hour=23, minute=59, second=59)

    @staticmethod
    def _settlement_backlog_status(close_at: datetime | None, *, now: datetime) -> str:
        if close_at is None:
            return "missing_close_metadata"
        seconds_to_close = int((close_at - now).total_seconds())
        if seconds_to_close > SETTLEMENT_NEAR_SECONDS:
            return "awaiting_close"
        if seconds_to_close > 0:
            return "near_settlement"
        if abs(seconds_to_close) <= SETTLEMENT_GAP_GRACE_SECONDS:
            return "awaiting_settlement"
        return "possible_ingestion_gap"

    @staticmethod
    def _top_blockers(
        *,
        readiness: TrainingReadiness,
        quality_debt_summary: dict[str, Any],
        settlement_maturity: dict[str, Any],
        recent_exclusion_memory: dict[str, Any],
    ) -> list[str]:
        blockers = list(readiness.missing_indicators)
        if quality_debt_summary.get("recent_stale_mismatch_count"):
            blockers.append(f"{quality_debt_summary['recent_stale_mismatch_count']} recent stale-data mismatches")
        if settlement_maturity.get("status_counts", {}).get("possible_ingestion_gap"):
            blockers.append(
                f"{settlement_maturity['status_counts']['possible_ingestion_gap']} unsettled rooms look like ingestion gaps"
            )
        top_market = ((recent_exclusion_memory.get("by_market") or [None])[0]) or None
        if top_market is not None:
            blockers.append(f"repeat exclusions on {top_market['market_ticker']}")
        deduped: list[str] = []
        for blocker in blockers:
            if blocker not in deduped:
                deduped.append(blocker)
        return deduped[:5]

    @staticmethod
    def _next_actions(
        *,
        readiness: TrainingReadiness,
        unsettled_count: int,
        settlement_maturity: dict[str, Any],
        quality_debt_summary: dict[str, Any],
        recent_exclusion_memory: dict[str, Any],
    ) -> list[str]:
        actions: list[str] = []
        if "not enough settled rooms" in readiness.missing_indicators and unsettled_count:
            actions.append(
                f"Keep the daemon and reconciliation running; {unsettled_count} complete rooms are still waiting on settlement labels."
            )
        near_settlement_count = int(settlement_maturity.get("near_settlement_count") or 0)
        if near_settlement_count:
            actions.append(f"Watch the {near_settlement_count} rooms nearest settlement for the next label wave.")
        if quality_debt_summary.get("recent_stale_mismatch_count"):
            actions.append("Review recent stale-data mismatches and keep repeat offenders out of the clean corpus slice.")
        repeat_markets = recent_exclusion_memory.get("by_market") or []
        if repeat_markets:
            top_markets = ", ".join(item["market_ticker"] for item in repeat_markets[:3])
            actions.append(f"Temporarily deprioritize repeat-exclusion markets: {top_markets}.")
        if not actions:
            actions.append("Corpus looks healthy; keep collecting unresolved structured weather rooms.")
        return actions[:4]

    @staticmethod
    def _rate(matches: list[float], *, total: int) -> float:
        if total <= 0:
            return 0.0
        return round(sum(matches) / total, 4)
