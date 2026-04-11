from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import (
    ResearchAuditIssue,
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
        request = TrainingBuildRequest(
            mode="room-bundles",
            limit=self.settings.training_status_room_limit,
            days=self.settings.training_window_days,
            good_research_only=False,
        )
        bundles = await self._selected_bundles(request)
        readiness = self._readiness_for_bundles(bundles)
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
        recent_builds = [self._summary_from_record(record).model_dump(mode="json") for record in builds]

        trainable_request = request.model_copy(update={"good_research_only": True})
        trainable_bundles = await self._selected_bundles(trainable_request)
        holdout_count = max(1, int(len(trainable_bundles) * self.settings.self_improve_holdout_ratio)) if trainable_bundles else 0
        oldest_unsettled_age_seconds = self._oldest_unsettled_age_seconds(bundles)

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
            "settled_label_coverage": round(settled_count / len(bundles), 4) if bundles else 0.0,
            "trade_positive_coverage": round(trade_positive_count / len(bundles), 4) if bundles else 0.0,
            "failed_research_reasons": dict(active_failed_reason_counts.most_common()),
            "active_failed_research_reasons": dict(active_failed_reason_counts.most_common()),
            "legacy_failed_research_reasons": dict(legacy_failed_reason_counts.most_common()),
            "trainable_room_count": len(trainable_bundles),
            "evaluation_holdout_room_count": holdout_count,
            "pack_versions": pack_versions,
            "recent_dataset_builds": recent_builds,
            "campaign_settings": self._campaign_settings_snapshot(),
            "readiness": readiness.model_dump(mode="json"),
            "last_readiness_snapshot": latest_snapshot.payload if latest_snapshot is not None else None,
            "top_missing_data": readiness.missing_indicators,
        }

    async def build_dataset(self, request: TrainingBuildRequest) -> dict[str, Any]:
        bundles = await self._selected_bundles(request)
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
        label_stats = self._label_stats(selected_bundles)
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
            )
        )
        readiness = self._readiness_for_bundles(bundles)
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.create_training_readiness_snapshot(readiness)
                await session.commit()
        return readiness

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
            failed_runs = await repo.list_research_runs(status="failed", limit=limit * 8)
            await session.commit()

        failed_counts = Counter(run.market_ticker for run in failed_runs)
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
            if failed_counts[mapping.market_ticker] >= 3:
                issues.append(
                    ResearchAuditIssue(
                        market_ticker=mapping.market_ticker,
                        severity="medium",
                        code="repeated_refresh_failures",
                        summary="Research refresh is repeatedly failing for this market.",
                        details={"failure_count": failed_counts[mapping.market_ticker]},
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

    async def _selected_bundles(self, request: TrainingBuildRequest) -> list[TrainingRoomBundle]:
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
        filtered: list[TrainingRoomBundle] = []
        for bundle in bundles:
            if request.good_research_only and not bool((bundle.research_health or {}).get("good_for_training")):
                continue
            if request.settled_only and not bundle.outcome.settlement_seen:
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

    def _dataset_item(self, bundle: TrainingRoomBundle) -> dict[str, Any]:
        return {
            "room_id": bundle.room["id"],
            "market_ticker": bundle.room["market_ticker"],
            "agent_pack_version": bundle.room.get("agent_pack_version"),
            "campaign": bundle.campaign,
            "research_health": bundle.research_health,
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

    def _label_stats(self, bundles: list[TrainingRoomBundle]) -> dict[str, Any]:
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

    @staticmethod
    def _rate(matches: list[float], *, total: int) -> float:
        if total <= 0:
            return 0.0
        return round(sum(matches) / total, 4)
