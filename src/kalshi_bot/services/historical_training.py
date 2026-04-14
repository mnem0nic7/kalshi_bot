from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
import hashlib
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, MessageKind, RiskStatus, RoomOrigin, RoomStage
from kalshi_bot.core.schemas import (
    HistoricalTrainingBuildRequest,
    RoomCreate,
    RoomMessageCreate,
)
from kalshi_bot.db.models import DeploymentControl
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
from kalshi_bot.services.historical_heuristics import HistoricalHeuristicService
from kalshi_bot.services.signal import (
    apply_heuristic_application_to_signal,
    evaluate_trade_eligibility,
    is_market_stale,
)
from kalshi_bot.services.training import TrainingExportService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherMarketMapping, WeatherSeriesTemplate
from kalshi_bot.weather.scoring import extract_current_temp_f, extract_forecast_high_f
from kalshi_bot.integrations.forecast_archive import OpenMeteoForecastArchiveClient
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.historical_archive import append_weather_bundle_archive, weather_bundle_archive_metadata


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value)).quantize(Decimal("0.0001"))


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat() if _as_utc(value) is not None else None
    return value


def _market_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("market", payload)


def _date_range(start: date, end: date) -> list[date]:
    current = start
    values: list[date] = []
    while current <= end:
        values.append(current)
        current += timedelta(days=1)
    return values


@dataclass(slots=True)
class HistoricalBuildSplit:
    train: list[str]
    validation: list[str]
    holdout: list[str]


@dataclass(slots=True)
class HistoricalCheckpointSelection:
    checkpoint_label: str
    checkpoint_ts: datetime
    market_snapshot: Any | None
    weather_snapshot: Any | None
    market_source_kind: str | None
    weather_source_kind: str | None
    missing_reasons: list[str]

    @property
    def replayable(self) -> bool:
        return self.market_snapshot is not None and self.weather_snapshot is not None


class HistoricalTrainingService:
    CHECKPOINTS = (
        ("open_0900", 9),
        ("midday_1300", 13),
        ("late_1700", 17),
    )
    REPLAY_LOGIC_VERSION = "historical_replay_2026_04_14_external_forecast_archive_v1"
    CHECKPOINT_CAPTURED_MARKET_SOURCE = "checkpoint_captured_market_snapshot"
    CHECKPOINT_CAPTURED_WEATHER_SOURCE = "checkpoint_archived_weather_bundle"
    PROMOTED_CHECKPOINT_ARCHIVE_SOURCE = "coverage_repair_checkpoint_promotion"
    CAPTURED_MARKET_SOURCE = "captured_market_snapshot"
    RECONSTRUCTED_MARKET_SOURCE = "reconstructed_market_checkpoint"
    CAPTURED_WEATHER_SOURCE = "captured_weather_bundle"
    ARCHIVED_WEATHER_SOURCE = "archived_weather_bundle"
    LEGACY_ARCHIVED_WEATHER_SOURCE = "file_weather_bundle"
    EXTERNAL_FORECAST_ARCHIVE_SOURCE = "external_forecast_archive_weather_bundle"
    FINAL_MARKET_SOURCE = "kalshi_final_market"
    SETTLEMENT_MATCH = "match"
    SETTLEMENT_MISMATCH = "mismatch"
    SETTLEMENT_MISSING = "missing"
    SETTLEMENT_BACKFILL_SOURCE = "kalshi_settlement_backfill"
    SETTLEMENT_MISMATCH_REASON_THRESHOLD_EDGE = "threshold_edge_strictness"
    SETTLEMENT_MISMATCH_REASON_DISAGREEMENT = "daily_summary_disagreement"
    SETTLEMENT_MISMATCH_REASON_MISSING = "crosscheck_missing"
    NCEI_DAILY_SUMMARY_URL = "https://www.ncei.noaa.gov/access/services/data/v1"
    COVERAGE_FULL = "full_checkpoint_coverage"
    COVERAGE_LATE_ONLY = "late_only_coverage"
    COVERAGE_PARTIAL = "partial_checkpoint_coverage"
    COVERAGE_OUTCOME_ONLY = "outcome_only_coverage"
    COVERAGE_NONE = "no_replayable_coverage"

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        kalshi: KalshiClient,
        forecast_archive_client: OpenMeteoForecastArchiveClient,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        historical_heuristic_service: HistoricalHeuristicService | None,
        research_coordinator: ResearchCoordinator,
        risk_engine: DeterministicRiskEngine,
        memory_service: MemoryService,
        training_export_service: TrainingExportService,
        training_corpus_service: TrainingCorpusService,
        agents: AgentSuite,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.forecast_archive_client = forecast_archive_client
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service
        self.historical_heuristic_service = historical_heuristic_service
        self.research_coordinator = research_coordinator
        self.risk_engine = risk_engine
        self.memory_service = memory_service
        self.training_export_service = training_export_service
        self.training_corpus_service = training_corpus_service
        self.agents = agents
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": settings.weather_user_agent,
                "Accept": "application/json",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()
        await self.forecast_archive_client.close()

    @classmethod
    def replay_logic_version(cls) -> str:
        return cls.REPLAY_LOGIC_VERSION

    async def import_weather_history(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            run = await repo.create_historical_import_run(
                import_kind="weather",
                source="kalshi_history",
                payload={
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                    "series": [template.series_ticker for template in templates],
                },
            )
            await session.commit()

        try:
            imported_markets = await self._import_market_definitions(date_from=date_from, date_to=date_to, templates=templates)
            imported_captured_markets = await self._import_captured_market_snapshots(
                date_from=date_from,
                date_to=date_to,
                templates=templates,
            )
            imported_captured_weather = await self._import_captured_weather_snapshots(
                date_from=date_from,
                date_to=date_to,
                templates=templates,
            )
            imported_file_weather = await self._import_file_weather_archives(
                date_from=date_from,
                date_to=date_to,
                templates=templates,
            )
            summary = {
                "status": "completed",
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "series": [template.series_ticker for template in templates],
                "imported_market_days": imported_markets["market_day_count"],
                "kalshi_market_count": imported_markets["market_count"],
                "captured_market_snapshot_count": imported_captured_markets,
                "captured_weather_snapshot_count": imported_captured_weather,
                "file_weather_snapshot_count": imported_file_weather,
                "settlement_mismatch_count": imported_markets["settlement_mismatch_count"],
                "crosscheck_missing_count": imported_markets["crosscheck_missing_count"],
            }
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_import_run(run.id, status="completed", payload=summary)
                await session.commit()
            return summary
        except Exception as exc:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.complete_historical_import_run(
                    run.id,
                    status="failed",
                    payload={},
                    error_text=str(exc),
                )
                await session.commit()
            raise

    async def replay_weather_history(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_labels = await repo.list_historical_settlement_labels(
                series_tickers=series_tickers,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            await session.commit()

        created_rooms = 0
        skipped_existing = 0
        skipped_missing_counts: Counter[str] = Counter()
        replayed_market_days = Counter()
        samples: list[dict[str, Any]] = []

        for label in settlement_labels:
            mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
            if mapping is None or not mapping.supports_structured_weather:
                continue
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                selections = await self._resolve_market_day_selections(repo, label=label, mapping=mapping)
                existing = await repo.list_historical_replay_runs(
                    status="completed",
                    date_from=label.local_market_day,
                    date_to=label.local_market_day,
                    limit=500,
                )
                existing_keys = {
                    (record.market_ticker, record.checkpoint_ts)
                    for record in existing
                }
                await session.commit()

            coverage_class = self._coverage_class(selections)
            for selection in selections:
                if (label.market_ticker, selection.checkpoint_ts) in existing_keys:
                    skipped_existing += 1
                    continue
                if not selection.replayable:
                    skipped_missing_counts.update(selection.missing_reasons)
                    continue
                room_id = await self._run_replay_room(
                    mapping=mapping,
                    settlement_label=label,
                    market_snapshot=selection.market_snapshot,
                    weather_snapshot=selection.weather_snapshot,
                    checkpoint_label=selection.checkpoint_label,
                    checkpoint_ts=selection.checkpoint_ts,
                    market_source_kind=selection.market_source_kind,
                    weather_source_kind=selection.weather_source_kind,
                    coverage_class=coverage_class,
                )
                created_rooms += 1
                replayed_market_days[label.local_market_day] += 1
                if len(samples) < 10:
                    samples.append(
                        {
                            "room_id": room_id,
                            "market_ticker": label.market_ticker,
                            "local_market_day": label.local_market_day,
                            "checkpoint_label": selection.checkpoint_label,
                            "checkpoint_ts": selection.checkpoint_ts.isoformat(),
                            "market_source_kind": selection.market_source_kind,
                            "weather_source_kind": selection.weather_source_kind,
                            "coverage_class": coverage_class,
                        }
                    )

        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            "created_room_count": created_rooms,
            "replayed_market_day_count": len(replayed_market_days),
            "skipped_existing_count": skipped_existing,
            "missing_reason_counts": dict(skipped_missing_counts),
            "samples": samples,
        }

    async def backfill_market_checkpoints(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_labels = await repo.list_historical_settlement_labels(
                series_tickers=series_tickers,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            await session.commit()

        created = 0
        already_present = 0
        missing_candlesticks = 0
        samples: list[dict[str, Any]] = []

        for label in settlement_labels:
            mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
            if mapping is None or not mapping.supports_structured_weather:
                continue
            checkpoints = self._checkpoint_times(
                mapping,
                local_market_day=label.local_market_day,
                market_payload=(label.payload or {}).get("market", {}),
            )
            for checkpoint_label, checkpoint_ts in checkpoints:
                async with self.session_factory() as session:
                    repo = PlatformRepository(session)
                    captured = await repo.get_latest_historical_market_snapshot(
                        market_ticker=label.market_ticker,
                        before_asof=checkpoint_ts,
                        source_kind=self.CAPTURED_MARKET_SOURCE,
                        local_market_day=label.local_market_day,
                    )
                    reconstructed = await repo.get_latest_historical_market_snapshot(
                        market_ticker=label.market_ticker,
                        before_asof=checkpoint_ts,
                        source_kind=self.RECONSTRUCTED_MARKET_SOURCE,
                        local_market_day=label.local_market_day,
                    )
                    await session.commit()
                captured_valid = captured is not None and self._historical_market_snapshot_valid(
                    captured,
                    checkpoint_ts=checkpoint_ts,
                )
                reconstructed_valid = reconstructed is not None and self._historical_market_snapshot_valid(
                    reconstructed,
                    checkpoint_ts=checkpoint_ts,
                )
                if captured_valid or reconstructed_valid:
                    already_present += 1
                    continue
                snapshot = await self._reconstruct_market_checkpoint(
                    mapping=mapping,
                    settlement_label=label,
                    checkpoint_label=checkpoint_label,
                    checkpoint_ts=checkpoint_ts,
                )
                if snapshot is None:
                    missing_candlesticks += 1
                    continue
                created += 1
                if len(samples) < 10:
                    samples.append(snapshot)

        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            "created_checkpoint_count": created,
            "already_present_count": already_present,
            "missing_candlestick_count": missing_candlesticks,
            "samples": samples,
        }

    async def backfill_weather_archives(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        station_ids = {template.station_id for template in templates}
        after = datetime.combine(date_from, time.min, tzinfo=UTC) - timedelta(hours=self.settings.historical_replay_market_snapshot_lookback_hours)
        before = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=UTC)
        archived = 0
        imported = 0
        samples: list[dict[str, Any]] = []

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            events = await repo.list_weather_events(created_after=after, created_before=before, limit=50000)
            await session.commit()

        for event in events:
            if event.station_id not in station_ids:
                continue
            payload = event.payload if isinstance(event.payload, dict) else {}
            archive_record = append_weather_bundle_archive(
                self.settings,
                payload,
                source_id=f"weather-event:{event.id}",
                archive_source="raw_weather_backfill",
                captured_at=_as_utc(event.created_at),
            )
            if archive_record is None:
                continue
            archived += 1
            if len(samples) < 10:
                samples.append(
                    {
                        "station_id": archive_record["station_id"],
                        "local_market_day": archive_record["local_market_day"],
                        "archive_path": archive_record["archive_path"],
                    }
                )
        imported = await self._import_file_weather_archives(date_from=date_from, date_to=date_to, templates=templates)
        checkpoint_promotions = await self._promote_checkpoint_archives_from_existing_weather(
            date_from=date_from,
            date_to=date_to,
            series=[template.series_ticker for template in templates],
        )
        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": [template.series_ticker for template in templates],
            "archived_event_count": archived,
            "imported_snapshot_count": imported,
            "checkpoint_archive_promotion_count": checkpoint_promotions["checkpoint_archive_promotion_count"],
            "checkpoint_archive_promotions": checkpoint_promotions,
            "samples": samples,
        }

    async def backfill_external_forecast_archives(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        if not self.settings.historical_forecast_archive_provider_enabled:
            return {
                "status": "disabled",
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "series": series_tickers,
                "provider": "open_meteo_forecast_archive",
                "inserted_snapshot_count": 0,
                "checkpoint_archive_promotion_count": 0,
                "skipped_existing_count": 0,
                "skipped_unavailable_count": 0,
                "samples": [],
            }

        inserted_snapshots = 0
        checkpoint_promotions = 0
        skipped_existing = 0
        skipped_unavailable = 0
        samples: list[dict[str, Any]] = []

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_labels = await repo.list_historical_settlement_labels(
                series_tickers=series_tickers or None,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            for label in settlement_labels:
                mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
                if mapping is None or not mapping.supports_structured_weather or not mapping.series_ticker:
                    continue
                checkpoints = self._checkpoint_times(
                    mapping,
                    local_market_day=label.local_market_day,
                    market_payload=(label.payload or {}).get("market", {}),
                )
                for checkpoint_label, checkpoint_ts in checkpoints:
                    existing_archive = await repo.get_historical_checkpoint_archive(
                        series_ticker=mapping.series_ticker,
                        local_market_day=label.local_market_day,
                        checkpoint_label=checkpoint_label,
                    )
                    if existing_archive is not None:
                        skipped_existing += 1
                        continue
                    snapshot = await self.forecast_archive_client.fetch_point_in_time_forecast(
                        mapping,
                        local_market_day=label.local_market_day,
                        checkpoint_ts=checkpoint_ts,
                        checkpoint_label=checkpoint_label,
                    )
                    if snapshot is None:
                        skipped_unavailable += 1
                        continue
                    existing_weather = await repo.get_historical_weather_snapshot_by_source(
                        station_id=mapping.station_id,
                        source_kind=self.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
                        source_id=snapshot.source_id,
                    )
                    if existing_weather is None:
                        weather_record = await repo.upsert_historical_weather_snapshot(
                            station_id=mapping.station_id,
                            series_ticker=mapping.series_ticker,
                            local_market_day=label.local_market_day,
                            asof_ts=checkpoint_ts,
                            source_kind=self.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
                            source_id=snapshot.source_id,
                            source_hash=_hash_payload(snapshot.payload),
                            observation_ts=checkpoint_ts,
                            forecast_updated_ts=snapshot.run_ts,
                            forecast_high_f=snapshot.forecast_high_f,
                            current_temp_f=snapshot.current_temp_f,
                            payload=snapshot.payload,
                        )
                        inserted_snapshots += 1
                    else:
                        weather_record = existing_weather
                    metadata = self._weather_snapshot_checkpoint_metadata(weather_record)
                    if not self._checkpoint_archive_metadata_valid(metadata, checkpoint_ts):
                        skipped_unavailable += 1
                        continue
                    source_id = (
                        f"external-promotion:{mapping.series_ticker}:{label.local_market_day}:{checkpoint_label}:{weather_record.id}"
                    )
                    await repo.upsert_historical_checkpoint_archive(
                        series_ticker=mapping.series_ticker,
                        market_ticker=label.market_ticker,
                        station_id=mapping.station_id,
                        local_market_day=label.local_market_day,
                        checkpoint_label=checkpoint_label,
                        checkpoint_ts=checkpoint_ts,
                        captured_at=datetime.now(UTC),
                        source_kind=self.PROMOTED_CHECKPOINT_ARCHIVE_SOURCE,
                        source_id=source_id,
                        source_hash=getattr(weather_record, "source_hash", None),
                        observation_ts=getattr(weather_record, "observation_ts", None),
                        forecast_updated_ts=getattr(weather_record, "forecast_updated_ts", None),
                        archive_path=None,
                        payload={
                            "series_ticker": mapping.series_ticker,
                            "market_ticker": label.market_ticker,
                            "station_id": mapping.station_id,
                            "local_market_day": label.local_market_day,
                            "checkpoint_label": checkpoint_label,
                            "checkpoint_ts": checkpoint_ts.isoformat(),
                            "archive_source": "external_forecast_archive_backfill",
                            "weather_source_kind": self.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
                            "weather_source_id": snapshot.source_id,
                            "weather_snapshot_id": weather_record.id,
                            "promoted_from_source": {
                                "weather_source_kind": self.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
                                "weather_source_id": snapshot.source_id,
                                "weather_snapshot_id": weather_record.id,
                                "weather_asof_ts": self._dt_to_iso(getattr(weather_record, "asof_ts", None)),
                            },
                            "external_archive": {
                                "provider": snapshot.provider,
                                "model": snapshot.model,
                                "run_ts": snapshot.run_ts.isoformat(),
                            },
                        },
                    )
                    checkpoint_promotions += 1
                    if len(samples) < 10:
                        samples.append(
                            {
                                "series_ticker": mapping.series_ticker,
                                "market_ticker": label.market_ticker,
                                "local_market_day": label.local_market_day,
                                "checkpoint_label": checkpoint_label,
                                "checkpoint_ts": checkpoint_ts.isoformat(),
                                "weather_source_kind": self.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
                                "weather_source_id": snapshot.source_id,
                                "provider": snapshot.provider,
                                "model": snapshot.model,
                                "run_ts": snapshot.run_ts.isoformat(),
                            }
                        )
            await session.commit()

        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            "provider": "open_meteo_forecast_archive",
            "inserted_snapshot_count": inserted_snapshots,
            "checkpoint_archive_promotion_count": checkpoint_promotions,
            "skipped_existing_count": skipped_existing,
            "skipped_unavailable_count": skipped_unavailable,
            "samples": samples,
        }

    async def _promote_checkpoint_archives_from_existing_weather(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        promoted = 0
        skipped_existing = 0
        skipped_missing_weather = 0
        skipped_invalid_source = 0
        samples: list[dict[str, Any]] = []
        now = datetime.now(UTC)

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_labels = await repo.list_historical_settlement_labels(
                series_tickers=series_tickers or None,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            for label in settlement_labels:
                mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
                if mapping is None or not mapping.supports_structured_weather or not mapping.series_ticker:
                    continue
                checkpoints = self._checkpoint_times(
                    mapping,
                    local_market_day=label.local_market_day,
                    market_payload=(label.payload or {}).get("market", {}),
                )
                for checkpoint_label, checkpoint_ts in checkpoints:
                    existing = await repo.get_historical_checkpoint_archive(
                        series_ticker=mapping.series_ticker,
                        local_market_day=label.local_market_day,
                        checkpoint_label=checkpoint_label,
                    )
                    if existing is not None:
                        skipped_existing += 1
                        continue
                    weather_snapshot = await self._select_weather_snapshot(
                        repo,
                        station_id=mapping.station_id,
                        series_ticker=mapping.series_ticker,
                        local_market_day=label.local_market_day,
                        checkpoint_label=checkpoint_label,
                        checkpoint_ts=checkpoint_ts,
                    )
                    if weather_snapshot is None:
                        skipped_missing_weather += 1
                        continue
                    weather_metadata = self._weather_snapshot_checkpoint_metadata(weather_snapshot)
                    if not self._checkpoint_archive_metadata_valid(weather_metadata, checkpoint_ts):
                        skipped_invalid_source += 1
                        continue
                    weather_payload = dict(weather_snapshot.payload or {})
                    weather_source_id = str(getattr(weather_snapshot, "source_id", "") or "")
                    weather_source_kind = str(getattr(weather_snapshot, "source_kind", "") or "")
                    archive_path = str(((weather_payload.get("_archive") or {}).get("archive_path") or "")) or None
                    source_id = f"promoted:{mapping.series_ticker}:{label.local_market_day}:{checkpoint_label}:{weather_snapshot.id}"
                    await repo.upsert_historical_checkpoint_archive(
                        series_ticker=mapping.series_ticker,
                        market_ticker=label.market_ticker,
                        station_id=mapping.station_id,
                        local_market_day=label.local_market_day,
                        checkpoint_label=checkpoint_label,
                        checkpoint_ts=checkpoint_ts,
                        captured_at=now,
                        source_kind=self.PROMOTED_CHECKPOINT_ARCHIVE_SOURCE,
                        source_id=source_id,
                        source_hash=getattr(weather_snapshot, "source_hash", None),
                        observation_ts=getattr(weather_snapshot, "observation_ts", None),
                        forecast_updated_ts=getattr(weather_snapshot, "forecast_updated_ts", None),
                        archive_path=archive_path,
                        payload={
                            "series_ticker": mapping.series_ticker,
                            "market_ticker": label.market_ticker,
                            "station_id": mapping.station_id,
                            "local_market_day": label.local_market_day,
                            "checkpoint_label": checkpoint_label,
                            "checkpoint_ts": checkpoint_ts.isoformat(),
                            "captured_at": now.isoformat(),
                            "archive_source": self.PROMOTED_CHECKPOINT_ARCHIVE_SOURCE,
                            "weather_source_kind": weather_source_kind,
                            "weather_source_id": weather_source_id,
                            "weather_snapshot_id": getattr(weather_snapshot, "id", None),
                            "promoted_from_source": {
                                "weather_source_kind": weather_source_kind,
                                "weather_source_id": weather_source_id,
                                "weather_snapshot_id": getattr(weather_snapshot, "id", None),
                                "weather_asof_ts": self._dt_to_iso(getattr(weather_snapshot, "asof_ts", None)),
                            },
                        },
                    )
                    promoted += 1
                    if len(samples) < 10:
                        samples.append(
                            {
                                "series_ticker": mapping.series_ticker,
                                "market_ticker": label.market_ticker,
                                "local_market_day": label.local_market_day,
                                "checkpoint_label": checkpoint_label,
                                "weather_source_kind": weather_source_kind,
                                "weather_source_id": weather_source_id,
                            }
                        )
            await session.commit()

        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            "checkpoint_archive_promotion_count": promoted,
            "skipped_existing_count": skipped_existing,
            "skipped_missing_weather_count": skipped_missing_weather,
            "skipped_invalid_source_count": skipped_invalid_source,
            "samples": samples,
        }

    async def capture_weather_archives_once(self, *, series: list[str] | None = None) -> dict[str, Any]:
        templates = self._selected_templates(series)
        captured = 0
        samples: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for template in templates:
                mapping = WeatherMarketMapping(
                    market_ticker=template.series_ticker,
                    market_type="weather",
                    display_name=template.display_name,
                    description=template.description,
                    research_queries=list(template.research_queries),
                    research_urls=list(template.research_urls),
                    station_id=template.station_id,
                    daily_summary_station_id=template.daily_summary_station_id,
                    location_name=template.location_name,
                    timezone_name=template.timezone_name,
                    latitude=template.latitude,
                    longitude=template.longitude,
                    threshold_f=0,
                    operator=">",
                    metric=template.metric,
                    settlement_source=template.settlement_source,
                    series_ticker=template.series_ticker,
                )
                weather_bundle = await self.research_coordinator.weather.build_market_snapshot(mapping)
                source_id = f"capture:{template.station_id}:{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
                archive_record = append_weather_bundle_archive(
                    self.settings,
                    weather_bundle,
                    source_id=source_id,
                    archive_source="manual_capture_once",
                )
                archive_meta = weather_bundle_archive_metadata(weather_bundle)
                if archive_meta is None:
                    continue
                await repo.upsert_historical_weather_snapshot(
                    station_id=archive_meta["station_id"],
                    series_ticker=archive_meta["series_ticker"],
                    local_market_day=archive_meta["local_market_day"],
                    asof_ts=archive_meta["asof_ts"],
                    source_kind=self.ARCHIVED_WEATHER_SOURCE,
                    source_id=source_id,
                    source_hash=_hash_payload(weather_bundle),
                    observation_ts=archive_meta["observation_ts"],
                    forecast_updated_ts=archive_meta["forecast_updated_ts"],
                    forecast_high_f=archive_meta["forecast_high_f"],
                    current_temp_f=archive_meta["current_temp_f"],
                    payload={
                        **weather_bundle,
                        "_archive": {
                            "archive_path": archive_record["archive_path"] if archive_record is not None else None,
                            "archive_source": "manual_capture_once",
                            "source_id": source_id,
                        },
                    },
                )
                await repo.log_weather_event(template.station_id, "historical_archive_capture", weather_bundle)
                captured += 1
                if len(samples) < 10:
                    samples.append(
                        {
                            "station_id": template.station_id,
                            "local_market_day": archive_meta["local_market_day"],
                            "archive_path": archive_record["archive_path"] if archive_record is not None else None,
                        }
                    )
            await session.commit()
        return {
            "status": "completed",
            "series": [template.series_ticker for template in templates],
            "captured_bundle_count": captured,
            "imported_snapshot_count": captured,
            "samples": samples,
        }

    async def capture_checkpoint_archives_once(
        self,
        *,
        series: list[str] | None = None,
        reference_time: datetime | None = None,
        due_only: bool = True,
        source_kind: str = "manual_checkpoint_capture_once",
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        now = _as_utc(reference_time) or datetime.now(UTC)
        captured = 0
        skipped_existing = 0
        skipped_not_due = 0
        skipped_future_source = 0
        skipped_missing_metadata = 0
        samples: list[dict[str, Any]] = []
        captured_market_snapshots = 0
        skipped_market_existing = 0
        skipped_market_not_due = 0
        skipped_market_future_source = 0
        skipped_market_stale = 0
        skipped_market_missing_metadata = 0
        market_samples: list[dict[str, Any]] = []

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for template in templates:
                local_market_day = now.astimezone(ZoneInfo(template.timezone_name or "UTC")).date().isoformat()
                day_markets = await self._list_template_day_markets(template, local_market_day=local_market_day)
                market = day_markets[0] if day_markets else None
                mapping = self._checkpoint_mapping(template, market=market)
                checkpoints = self._checkpoint_times(mapping, local_market_day=local_market_day, market_payload=market or {})
                for checkpoint_label, checkpoint_ts in checkpoints:
                    capture_due = self._checkpoint_capture_due(checkpoint_ts, now=now)
                    if due_only and not capture_due:
                        skipped_not_due += 1
                    else:
                        existing = await repo.get_historical_checkpoint_archive(
                            series_ticker=template.series_ticker,
                            local_market_day=local_market_day,
                            checkpoint_label=checkpoint_label,
                        )
                        if existing is not None:
                            skipped_existing += 1
                        else:
                            weather_bundle = await self.research_coordinator.weather.build_market_snapshot(mapping)
                            archive_meta = weather_bundle_archive_metadata(weather_bundle, captured_at=checkpoint_ts)
                            if archive_meta is None:
                                skipped_missing_metadata += 1
                            elif not self._checkpoint_archive_metadata_valid(archive_meta, checkpoint_ts):
                                skipped_future_source += 1
                            else:
                                source_id = f"checkpoint:{template.series_ticker}:{local_market_day}:{checkpoint_label}"
                                archive_record = append_weather_bundle_archive(
                                    self.settings,
                                    weather_bundle,
                                    source_id=source_id,
                                    archive_source=source_kind,
                                    captured_at=checkpoint_ts,
                                )
                                await repo.upsert_historical_weather_snapshot(
                                    station_id=template.station_id,
                                    series_ticker=template.series_ticker,
                                    local_market_day=local_market_day,
                                    asof_ts=checkpoint_ts,
                                    source_kind=self.CHECKPOINT_CAPTURED_WEATHER_SOURCE,
                                    source_id=source_id,
                                    source_hash=_hash_payload(weather_bundle),
                                    observation_ts=archive_meta["observation_ts"],
                                    forecast_updated_ts=archive_meta["forecast_updated_ts"],
                                    forecast_high_f=archive_meta["forecast_high_f"],
                                    current_temp_f=archive_meta["current_temp_f"],
                                    payload={
                                        **weather_bundle,
                                        "_checkpoint_archive": {
                                            "checkpoint_label": checkpoint_label,
                                            "checkpoint_ts": checkpoint_ts.isoformat(),
                                            "captured_at": now.isoformat(),
                                            "archive_source": source_kind,
                                        },
                                        "_archive": {
                                            "archive_path": archive_record["archive_path"] if archive_record is not None else None,
                                            "archive_source": source_kind,
                                            "source_id": source_id,
                                            "captured_at": checkpoint_ts.isoformat(),
                                        },
                                    },
                                )
                                await repo.upsert_historical_checkpoint_archive(
                                    series_ticker=template.series_ticker,
                                    market_ticker=(str(market.get("ticker")) if isinstance(market, dict) and market.get("ticker") else None),
                                    station_id=template.station_id,
                                    local_market_day=local_market_day,
                                    checkpoint_label=checkpoint_label,
                                    checkpoint_ts=checkpoint_ts,
                                    captured_at=now,
                                    source_kind=source_kind,
                                    source_id=source_id,
                                    source_hash=_hash_payload(weather_bundle),
                                    observation_ts=archive_meta["observation_ts"],
                                    forecast_updated_ts=archive_meta["forecast_updated_ts"],
                                    archive_path=(archive_record["archive_path"] if archive_record is not None else None),
                                    payload={
                                        "series_ticker": template.series_ticker,
                                        "market_ticker": (str(market.get("ticker")) if isinstance(market, dict) and market.get("ticker") else None),
                                        "station_id": template.station_id,
                                        "local_market_day": local_market_day,
                                        "checkpoint_label": checkpoint_label,
                                        "checkpoint_ts": checkpoint_ts.isoformat(),
                                        "captured_at": now.isoformat(),
                                        "weather_source_kind": self.CHECKPOINT_CAPTURED_WEATHER_SOURCE,
                                        "weather_source_id": source_id,
                                        "archive_source": source_kind,
                                    },
                                )
                                await repo.log_weather_event(template.station_id, "historical_checkpoint_capture", weather_bundle)
                                captured += 1
                                if len(samples) < 10:
                                    samples.append(
                                        {
                                            "series_ticker": template.series_ticker,
                                            "market_ticker": (str(market.get("ticker")) if isinstance(market, dict) and market.get("ticker") else None),
                                            "local_market_day": local_market_day,
                                            "checkpoint_label": checkpoint_label,
                                            "checkpoint_ts": checkpoint_ts.isoformat(),
                                            "archive_path": archive_record["archive_path"] if archive_record is not None else None,
                                        }
                                    )

                    for day_market in day_markets:
                        market_ticker = str(day_market.get("ticker") or "")
                        if due_only and not capture_due:
                            skipped_market_not_due += 1
                            continue
                        existing_market = await repo.get_latest_historical_market_snapshot(
                            market_ticker=market_ticker,
                            before_asof=checkpoint_ts,
                            source_kind=self.CHECKPOINT_CAPTURED_MARKET_SOURCE,
                            local_market_day=local_market_day,
                        )
                        if existing_market is not None:
                            skipped_market_existing += 1
                            continue
                        asof_ts, market_skip_reason = self._checkpoint_market_snapshot_asof(
                            day_market,
                            checkpoint_ts=checkpoint_ts,
                        )
                        if market_skip_reason == "market_snapshot_future":
                            skipped_market_future_source += 1
                            continue
                        if market_skip_reason == "market_snapshot_stale":
                            skipped_market_stale += 1
                            continue
                        if asof_ts is None:
                            skipped_market_missing_metadata += 1
                            continue
                        market_source_id = (
                            f"checkpoint:{template.series_ticker}:{local_market_day}:{checkpoint_label}:{market_ticker}"
                        )
                        await repo.upsert_historical_market_snapshot(
                            market_ticker=market_ticker,
                            series_ticker=template.series_ticker,
                            station_id=template.station_id,
                            local_market_day=local_market_day,
                            asof_ts=asof_ts,
                            source_kind=self.CHECKPOINT_CAPTURED_MARKET_SOURCE,
                            source_id=market_source_id,
                            source_hash=_hash_payload({"market": day_market}),
                            close_ts=self._market_timestamp(day_market, "close_time", "close_ts"),
                            settlement_ts=self._market_timestamp(day_market, "settlement_ts", "settlement_time"),
                            yes_bid_dollars=_parse_decimal(day_market.get("yes_bid_dollars")),
                            yes_ask_dollars=_parse_decimal(day_market.get("yes_ask_dollars")),
                            no_ask_dollars=_parse_decimal(day_market.get("no_ask_dollars")),
                            last_price_dollars=_parse_decimal(day_market.get("last_price_dollars")),
                            payload={
                                "market": day_market,
                                "_checkpoint_capture": {
                                    "checkpoint_label": checkpoint_label,
                                    "checkpoint_ts": checkpoint_ts.isoformat(),
                                    "captured_at": now.isoformat(),
                                    "source_kind": source_kind,
                                },
                            },
                        )
                        captured_market_snapshots += 1
                        if len(market_samples) < 10:
                            market_samples.append(
                                {
                                    "series_ticker": template.series_ticker,
                                    "market_ticker": market_ticker,
                                    "local_market_day": local_market_day,
                                    "checkpoint_label": checkpoint_label,
                                    "checkpoint_ts": checkpoint_ts.isoformat(),
                                    "market_asof_ts": asof_ts.isoformat(),
                                }
                            )
            await session.commit()
        return {
            "status": "completed",
            "series": [template.series_ticker for template in templates],
            "captured_checkpoint_count": captured,
            "skipped_existing_count": skipped_existing,
            "skipped_not_due_count": skipped_not_due,
            "skipped_future_source_count": skipped_future_source,
            "skipped_missing_metadata_count": skipped_missing_metadata,
            "samples": samples,
            "captured_market_snapshot_count": captured_market_snapshots,
            "skipped_market_existing_count": skipped_market_existing,
            "skipped_market_not_due_count": skipped_market_not_due,
            "skipped_market_future_source_count": skipped_market_future_source,
            "skipped_market_stale_count": skipped_market_stale,
            "skipped_market_missing_metadata_count": skipped_market_missing_metadata,
            "market_samples": market_samples,
        }

    async def checkpoint_capture_status(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        template_by_series = {template.series_ticker: template for template in templates}
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_labels = await repo.list_historical_settlement_labels(
                series_tickers=[template.series_ticker for template in templates],
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            archives = await repo.list_historical_checkpoint_archives(
                series_tickers=[template.series_ticker for template in templates],
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            await session.commit()

        archive_index = {
            (record.series_ticker, record.local_market_day, record.checkpoint_label): record
            for record in archives
        }
        rows: list[dict[str, Any]] = []
        gap_counts: Counter[str] = Counter()
        gap_samples: list[dict[str, Any]] = []
        covered_days = 0
        late_only_days = 0
        none_days = 0

        for label in settlement_labels:
            template = template_by_series.get(label.series_ticker or "")
            mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
            if template is None or mapping is None:
                continue
            checkpoints = self._checkpoint_times(mapping, local_market_day=label.local_market_day, market_payload=(label.payload or {}).get("market", {}))
            checkpoint_rows: list[dict[str, Any]] = []
            replayable_flags: list[bool] = []
            for checkpoint_label, checkpoint_ts in checkpoints:
                record = archive_index.get((template.series_ticker, label.local_market_day, checkpoint_label))
                replayable = record is not None
                replayable_flags.append(replayable)
                if not replayable:
                    gap_counts["checkpoint_archive_missing"] += 1
                    if len(gap_samples) < 10:
                        gap_samples.append(
                            {
                                "series_ticker": template.series_ticker,
                                "market_ticker": label.market_ticker,
                                "local_market_day": label.local_market_day,
                                "checkpoint_label": checkpoint_label,
                                "gap": "checkpoint_archive_missing",
                            }
                        )
                checkpoint_rows.append(
                    {
                        "checkpoint_label": checkpoint_label,
                        "checkpoint_ts": checkpoint_ts.isoformat(),
                        "captured": replayable,
                        "source_kind": (record.source_kind if record is not None else None),
                        "captured_at": (record.captured_at.isoformat() if record is not None else None),
                    }
                )
            coverage_class = self._coverage_class(
                [
                    HistoricalCheckpointSelection(
                        checkpoint_label=checkpoint_row["checkpoint_label"],
                        checkpoint_ts=datetime.fromisoformat(checkpoint_row["checkpoint_ts"]),
                        market_snapshot=object() if captured else None,
                        weather_snapshot=object() if captured else None,
                        market_source_kind=None,
                        weather_source_kind=self.CHECKPOINT_CAPTURED_WEATHER_SOURCE if captured else None,
                        missing_reasons=[] if captured else ["checkpoint_archive_missing"],
                    )
                    for checkpoint_row, captured in zip(checkpoint_rows, replayable_flags, strict=False)
                ]
            )
            if coverage_class == self.COVERAGE_FULL:
                covered_days += 1
            elif coverage_class == self.COVERAGE_LATE_ONLY:
                late_only_days += 1
            else:
                none_days += 1
            rows.append(
                {
                    "series_ticker": template.series_ticker,
                    "market_ticker": label.market_ticker,
                    "local_market_day": label.local_market_day,
                    "coverage_class": coverage_class,
                    "checkpoints": checkpoint_rows,
                }
            )

        return {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": [template.series_ticker for template in templates],
            "checkpoint_coverage_counts": {
                self.COVERAGE_FULL: covered_days,
                self.COVERAGE_LATE_ONLY: late_only_days,
                self.COVERAGE_NONE: none_days,
            },
            "checkpoint_capture_gaps": {
                "reason_counts": dict(gap_counts),
                "samples": gap_samples,
            },
            "market_day_coverage": rows if verbose else rows[:12],
        }

    async def backfill_settlements(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        historical_refresh = await self._refresh_historical_settlement_crosschecks(
            date_from=date_from,
            date_to=date_to,
            series=series,
        )
        room_backfill = await self._backfill_room_settlements(
            date_from=date_from,
            date_to=date_to,
            series=series,
        )
        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": historical_refresh["series"],
            "settlement_label_refresh_count": historical_refresh["refreshed_count"],
            "settlement_label_changed_count": historical_refresh["changed_count"],
            "settlement_mismatch_breakdown": historical_refresh["settlement_mismatch_breakdown"],
            "historical_crosscheck_refresh": historical_refresh,
            "room_settlement_backfill": room_backfill,
            "target_market_count": room_backfill["target_market_count"],
            "backfilled_count": room_backfill["backfilled_count"],
            "already_labeled_count": room_backfill["already_labeled_count"],
            "not_settled_count": room_backfill["not_settled_count"],
            "fetch_error_count": room_backfill["fetch_error_count"],
            "samples": room_backfill["samples"],
            "refresh_samples": historical_refresh["samples"],
        }

    async def _refresh_historical_settlement_crosschecks(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        refreshed = 0
        changed = 0
        samples: list[dict[str, Any]] = []
        mismatch_breakdown: Counter[str] = Counter()
        now = datetime.now(UTC)

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            labels = await repo.list_historical_settlement_labels(
                series_tickers=series_tickers or None,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            for label in labels:
                payload = dict(label.payload or {})
                market_payload = dict(payload.get("market") or {})
                mapping = self._mapping_for_market(label.market_ticker, market_payload)
                if mapping is None or not mapping.supports_structured_weather or mapping.threshold_f is None:
                    crosscheck = {
                        "status": self.SETTLEMENT_MISSING,
                        "daily_high_f": None,
                        "result": None,
                        "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
                    }
                else:
                    try:
                        crosscheck = await self._daily_summary_crosscheck(
                            mapping,
                            label.local_market_day,
                            kalshi_result=label.kalshi_result,
                        )
                    except Exception:
                        crosscheck = {
                            "status": self.SETTLEMENT_MISSING,
                            "daily_high_f": None,
                            "result": None,
                            "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
                        }
                previous_reason = self._crosscheck_mismatch_reason_from_label(label)
                if (
                    label.crosscheck_status != crosscheck["status"]
                    or label.crosscheck_result != crosscheck["result"]
                    or label.crosscheck_high_f != crosscheck["daily_high_f"]
                    or previous_reason != crosscheck.get("mismatch_reason")
                ):
                    changed += 1
                    if len(samples) < 10:
                        samples.append(
                            {
                                "market_ticker": label.market_ticker,
                                "local_market_day": label.local_market_day,
                                "previous_status": label.crosscheck_status,
                                "new_status": crosscheck["status"],
                                "previous_mismatch_reason": previous_reason,
                                "new_mismatch_reason": crosscheck.get("mismatch_reason"),
                            }
                        )
                payload["crosscheck"] = {
                    **crosscheck,
                    "refreshed_at": now.isoformat(),
                }
                await repo.upsert_historical_settlement_label(
                    market_ticker=label.market_ticker,
                    series_ticker=label.series_ticker,
                    local_market_day=label.local_market_day,
                    source_kind=label.source_kind,
                    kalshi_result=label.kalshi_result,
                    settlement_value_dollars=label.settlement_value_dollars,
                    settlement_ts=label.settlement_ts,
                    crosscheck_status=crosscheck["status"],
                    crosscheck_high_f=crosscheck["daily_high_f"],
                    crosscheck_result=crosscheck["result"],
                    payload=_json_safe(payload),
                )
                refreshed += 1
                mismatch_key = (
                    crosscheck.get("mismatch_reason")
                    or (
                        self.SETTLEMENT_MISMATCH_REASON_DISAGREEMENT
                        if crosscheck["status"] == self.SETTLEMENT_MISMATCH
                        else (
                            self.SETTLEMENT_MISMATCH_REASON_MISSING
                            if crosscheck["status"] == self.SETTLEMENT_MISSING
                            else None
                        )
                    )
                )
                if mismatch_key:
                    mismatch_breakdown[str(mismatch_key)] += 1
            await session.commit()

        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            "refreshed_count": refreshed,
            "changed_count": changed,
            "settlement_mismatch_breakdown": dict(mismatch_breakdown),
            "samples": samples,
        }

    async def _backfill_room_settlements(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        backlog_summary = await self.training_corpus_service.get_settlement_focus_summary(limit=5000)
        backlog = backlog_summary.get("backlog") or []
        allowed_series = {item.upper() for item in (series or [])}
        now = datetime.now(UTC)
        targeted: list[dict[str, Any]] = []
        for item in backlog:
            market_ticker = str(item.get("market_ticker") or "")
            local_market_day = self._market_day_from_ticker_or_close(
                market_ticker,
                _parse_iso(item.get("close_at")),
            )
            if local_market_day is None or not (date_from.isoformat() <= local_market_day <= date_to.isoformat()):
                continue
            if allowed_series and self._series_from_market_ticker(market_ticker) not in allowed_series:
                continue
            if str(item.get("status") or "") not in {"awaiting_settlement", "possible_ingestion_gap"}:
                continue
            targeted.append(item)

        seen_markets: set[str] = set()
        backfilled = 0
        already_labeled = 0
        not_settled = 0
        fetch_errors = 0
        samples: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for item in targeted:
                market_ticker = str(item.get("market_ticker") or "")
                if not market_ticker or market_ticker in seen_markets:
                    continue
                seen_markets.add(market_ticker)
                existing = await repo.get_historical_settlement_label(market_ticker)
                if existing is not None and existing.settlement_value_dollars is not None:
                    already_labeled += 1
                    continue
                try:
                    market_response = await self._fetch_market_for_backfill(market_ticker)
                except Exception:
                    fetch_errors += 1
                    continue
                market = _market_payload(market_response)
                settlement_value = self._market_settlement_value(market)
                kalshi_result = str(market.get("result") or "").strip().lower() or None
                close_ts = self._market_timestamp(market, "close_time", "close_ts")
                if settlement_value is None or kalshi_result not in {"yes", "no"}:
                    if close_ts is None or close_ts <= now:
                        not_settled += 1
                    continue
                mapping = self._mapping_for_market(market_ticker, market)
                template = self._template_for_market_ticker(market_ticker)
                local_market_day = self._market_day_from_ticker_or_close(market_ticker, close_ts)
                if local_market_day is None:
                    not_settled += 1
                    continue
                series_ticker = (
                    mapping.series_ticker
                    if mapping is not None and mapping.series_ticker
                    else (template.series_ticker if template is not None else self._series_from_market_ticker(market_ticker))
                )
                if not series_ticker:
                    not_settled += 1
                    continue
                crosscheck = {
                    "status": self.SETTLEMENT_MISSING,
                    "daily_high_f": None,
                    "result": None,
                    "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
                }
                if mapping is not None and mapping.threshold_f is not None:
                    try:
                        crosscheck = await self._daily_summary_crosscheck(mapping, local_market_day, kalshi_result=kalshi_result)
                    except Exception:
                        crosscheck = {
                            "status": self.SETTLEMENT_MISSING,
                            "daily_high_f": None,
                            "result": None,
                            "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
                        }
                await repo.upsert_historical_settlement_label(
                    market_ticker=market_ticker,
                    series_ticker=series_ticker,
                    local_market_day=local_market_day,
                    source_kind=self.SETTLEMENT_BACKFILL_SOURCE,
                    kalshi_result=kalshi_result,
                    settlement_value_dollars=settlement_value,
                    settlement_ts=self._market_timestamp(market, "settlement_ts", "settlement_time"),
                    crosscheck_status=crosscheck["status"],
                    crosscheck_high_f=crosscheck["daily_high_f"],
                    crosscheck_result=crosscheck["result"],
                    payload=_json_safe(
                        {
                            "market": market,
                            "crosscheck": crosscheck,
                            "backfilled_at": now.isoformat(),
                            "backfill_reason": "room_settlement_backfill",
                        }
                    ),
                )
                backfilled += 1
                if len(samples) < 10:
                    samples.append(
                        {
                            "market_ticker": market_ticker,
                            "local_market_day": local_market_day,
                            "source_kind": self.SETTLEMENT_BACKFILL_SOURCE,
                            "crosscheck_status": crosscheck["status"],
                        }
                    )
            await session.commit()
        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": sorted(allowed_series),
            "target_market_count": len(seen_markets),
            "backfilled_count": backfilled,
            "already_labeled_count": already_labeled,
            "not_settled_count": not_settled,
            "fetch_error_count": fetch_errors,
            "samples": samples,
        }

    async def build_historical_dataset(self, request: HistoricalTrainingBuildRequest) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            runs = await repo.list_historical_replay_runs(
                series_tickers=request.series or None,
                date_from=request.date_from,
                date_to=request.date_to,
                status="completed",
                limit=max(request.limit * 5, 500),
            )
            await session.commit()
        room_ids = [record.room_id for record in runs if record.room_id][: request.limit]
        bundles = await self.training_export_service.export_room_bundles(
            room_ids=room_ids,
            limit=len(room_ids),
            include_non_complete=False,
            origins=request.origins or [RoomOrigin.HISTORICAL_REPLAY.value],
        )
        bundles = await self._hydrate_historical_bundle_coverage(bundles)
        selected = self._filter_historical_bundles(
            bundles,
            quality_cleaned_only=request.quality_cleaned_only,
            include_pathology_examples=request.include_pathology_examples,
            require_full_checkpoints=request.require_full_checkpoints,
            late_only_ok=request.late_only_ok,
            mode=request.mode,
        )
        split = self._split_historical_bundles(selected)
        training_ready, draft_only = self._build_training_readiness(selected, split=split, mode=request.mode)
        if request.mode == "bundles":
            export_records = [self._bundle_with_split(bundle, split, draft_only=draft_only) for bundle in selected]
            output = self._write_single_jsonl(request.output, export_records)
        elif request.mode == "role-sft":
            export_records = self._historical_role_examples(selected, split, draft_only=draft_only)
            output = self._write_single_jsonl(request.output, export_records)
        elif request.mode == "decision-eval":
            export_records = [self._decision_eval_item(bundle, split, draft_only=draft_only) for bundle in selected]
            output = self._write_single_jsonl(request.output, export_records)
        elif request.mode == "outcome-eval":
            export_records = [self._outcome_eval_item(bundle, split, draft_only=draft_only) for bundle in selected if bundle.settlement_label is not None]
            output = self._write_single_jsonl(request.output, export_records)
        elif request.mode == "gemini-finetune":
            export_records = self._historical_role_examples(selected, split, draft_only=draft_only)
            output = self._write_gemini_export(
                request.output,
                export_records,
                bundles=selected,
                split=split,
                draft_only=draft_only,
                training_ready=training_ready,
            )
        else:
            raise ValueError(f"Unsupported historical build mode: {request.mode}")

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            latest_intelligence_runs = await repo.list_historical_intelligence_runs(limit=1)
            await session.commit()
        latest_intelligence_payload = latest_intelligence_runs[0].payload if latest_intelligence_runs else None
        confidence = self._confidence_story(
            latest_run_payload=latest_intelligence_payload,
            historical_build_readiness={
                "distinct_full_coverage_market_days": len(
                    self._historical_market_days_for_coverage(
                        selected,
                        coverage_class=self.COVERAGE_FULL,
                    )
                ),
                "holdout_full_coverage_market_days": len(
                    self._historical_market_days_for_coverage(
                        selected,
                        coverage_class=self.COVERAGE_FULL,
                        room_ids=set(split.holdout),
                    )
                ),
            },
            source_replay_coverage={
                "full_checkpoint_coverage_count": sum(1 for bundle in selected if (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == self.COVERAGE_FULL),
                "late_only_coverage_count": sum(1 for bundle in selected if (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == self.COVERAGE_LATE_ONLY),
                "partial_checkpoint_coverage_count": sum(1 for bundle in selected if (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == self.COVERAGE_PARTIAL),
                "outcome_only_coverage_count": sum(1 for bundle in selected if (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == self.COVERAGE_OUTCOME_ONLY),
                "no_replayable_coverage_count": sum(1 for bundle in selected if (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == self.COVERAGE_NONE),
            },
        )
        label_stats = self._historical_label_stats(selected, split, draft_only=draft_only, training_ready=training_ready)
        label_stats["confidence_state"] = confidence["confidence_state"]
        label_stats["confidence_scorecard"] = confidence["confidence_scorecard"]
        label_stats["confidence_progress"] = confidence["confidence_progress"]
        build_version = f"historical-{request.mode}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            record = await repo.create_training_dataset_build(
                build_version=build_version,
                mode=f"historical-{request.mode}",
                status="completed_draft" if draft_only else "completed",
                selection_window_start=min((_parse_iso(bundle.replay_checkpoint_ts) if isinstance(bundle.replay_checkpoint_ts, str) else bundle.replay_checkpoint_ts) for bundle in selected) if selected else None,
                selection_window_end=max((_parse_iso(bundle.replay_checkpoint_ts) if isinstance(bundle.replay_checkpoint_ts, str) else bundle.replay_checkpoint_ts) for bundle in selected) if selected else None,
                room_count=len(selected),
                filters=request.model_dump(mode="json"),
                label_stats=label_stats,
                pack_versions=sorted({bundle.room.get("agent_pack_version") for bundle in selected if bundle.room.get("agent_pack_version")}),
                payload={
                    "room_ids": [bundle.room["id"] for bundle in selected],
                    "split_counts": {
                        "train": len(split.train),
                        "validation": len(split.validation),
                        "holdout": len(split.holdout),
                    },
                    "draft_only": draft_only,
                    "training_ready": training_ready,
                    "confidence_state": confidence["confidence_state"],
                    "confidence_scorecard": confidence["confidence_scorecard"],
                    "confidence_progress": confidence["confidence_progress"],
                    "output": output,
                },
                completed_at=datetime.now(UTC),
            )
            await repo.set_training_dataset_build_items(
                dataset_build_id=record.id,
                items=[self._historical_dataset_item(bundle, split, draft_only=draft_only) for bundle in selected],
            )
            await session.commit()

        return {
            "build": {
                "id": record.id,
                "build_version": build_version,
                "mode": f"historical-{request.mode}",
                "room_count": len(selected),
                "label_stats": label_stats,
                "draft_only": draft_only,
                "training_ready": training_ready,
            },
            "output": output,
        }

    async def audit_historical_replay(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            settlement_labels = await repo.list_historical_settlement_labels(
                series_tickers=series_tickers or None,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            replay_runs = await repo.list_historical_replay_runs(
                series_tickers=series_tickers or None,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                limit=5000,
            )
            await session.commit()
        coverage = await self._coverage_status(settlement_labels, verbose=True)
        audit = self._build_replay_audit(
            coverage_rows=coverage["all_market_day_coverage"],
            replay_runs=replay_runs,
            verbose=verbose,
        )
        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            **audit,
        }

    async def refresh_historical_replay(
        self,
        *,
        date_from: date,
        date_to: date,
        series: list[str] | None = None,
    ) -> dict[str, Any]:
        templates = self._selected_templates(series)
        series_tickers = [template.series_ticker for template in templates]
        audit_before = await self.audit_historical_replay(
            date_from=date_from,
            date_to=date_to,
            series=series_tickers,
            verbose=True,
        )
        if not audit_before.get("refresh_needed"):
            return {
                "status": "noop",
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "series": series_tickers,
                "audit": audit_before,
                "deleted_room_count": 0,
                "deleted_run_count": 0,
                "stale_build_count": 0,
                "replay": {
                    "created_room_count": 0,
                    "replayed_market_day_count": 0,
                    "skipped_existing_count": 0,
                    "missing_reason_counts": {},
                    "samples": [],
                },
            }

        stale_room_ids = sorted({str(room_id) for room_id in (audit_before.get("affected_room_ids") or []) if room_id})
        affected_run_ids = sorted({str(run_id) for run_id in (audit_before.get("affected_run_ids") or []) if run_id})
        stale_build_ids = await self._mark_historical_builds_stale(
            date_from=date_from,
            date_to=date_to,
            affected_room_ids=stale_room_ids,
            audit=audit_before,
        )

        deleted_room_count = 0
        deleted_run_count = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for run_id in affected_run_ids:
                deleted_run_count += int(await repo.delete_historical_replay_run(run_id))
            for room_id in stale_room_ids:
                deleted_room_count += int(await repo.delete_room(room_id))
            await session.commit()

        replay_result = await self.replay_weather_history(
            date_from=date_from,
            date_to=date_to,
            series=series_tickers or None,
        )
        audit_after = await self.audit_historical_replay(
            date_from=date_from,
            date_to=date_to,
            series=series_tickers,
            verbose=True,
        )
        return {
            "status": "completed",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "series": series_tickers,
            "deleted_room_count": deleted_room_count,
            "deleted_run_count": deleted_run_count,
            "stale_build_count": len(stale_build_ids),
            "stale_build_ids": stale_build_ids,
            "audit_before": audit_before,
            "replay": replay_result,
            "audit_after": audit_after,
        }

    async def get_status(self, *, verbose: bool = False) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            import_runs = await repo.list_historical_import_runs(import_kind="weather", limit=10)
            settlement_labels = await repo.list_historical_settlement_labels(limit=5000)
            replay_runs = await repo.list_historical_replay_runs(limit=5000)
            historical_builds = await repo.list_training_dataset_builds(limit=1000, mode_prefix="historical-")
            intelligence_runs = await repo.list_historical_intelligence_runs(limit=10)
            pipeline_runs = await repo.list_historical_pipeline_runs(limit=10)
            await session.commit()

        replay_room_ids = [run.room_id for run in replay_runs if run.room_id]
        replay_bundles = await self.training_export_service.export_room_bundles(
            room_ids=replay_room_ids,
            limit=len(replay_room_ids),
            include_non_complete=False,
            origins=[RoomOrigin.HISTORICAL_REPLAY.value],
        ) if replay_room_ids else []
        replay_bundles = await self._hydrate_historical_bundle_coverage(replay_bundles)

        clean_trainable = [
            bundle for bundle in replay_bundles
            if bundle.trainable_default is not False
            and ((bundle.settlement_label or {}).get("crosscheck_status") != self.SETTLEMENT_MISMATCH)
            and (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == self.COVERAGE_FULL
        ]
        coverage = await self._coverage_status(settlement_labels, verbose=verbose)
        checkpoint_capture = await self._checkpoint_capture_status(settlement_labels, verbose=verbose)
        market_checkpoint_capture = await self._market_checkpoint_capture_status(settlement_labels, verbose=verbose)
        external_archive_coverage = await self._external_forecast_archive_status(settlement_labels, verbose=verbose)
        replay_audit = self._build_replay_audit(
            coverage_rows=coverage["all_market_day_coverage"],
            replay_runs=replay_runs,
            verbose=verbose,
        )
        public_coverage = dict(coverage)
        public_coverage.pop("all_market_day_coverage", None)
        public_checkpoint_capture = dict(checkpoint_capture)
        public_checkpoint_capture.pop("all_market_day_coverage", None)
        public_market_checkpoint_capture = dict(market_checkpoint_capture)
        public_market_checkpoint_capture.pop("all_market_day_coverage", None)
        public_external_archive_coverage = dict(external_archive_coverage)
        public_external_archive_coverage.pop("all_market_day_coverage", None)
        coverage_backlog = self._coverage_backlog(
            coverage_rows=coverage["all_market_day_coverage"],
            checkpoint_rows=checkpoint_capture["all_market_day_coverage"],
            settlement_labels=settlement_labels,
            verbose=verbose,
        )
        settlement_mismatch_breakdown = self._settlement_mismatch_breakdown(settlement_labels)
        coverage_repair_summary = self._coverage_repair_summary(
            coverage_backlog=coverage_backlog,
            checkpoint_rows=checkpoint_capture["all_market_day_coverage"],
            external_archive_coverage=external_archive_coverage,
        )
        public_coverage_backlog = dict(coverage_backlog)
        public_coverage_backlog.pop("all_samples", None)
        settlement_focus = await self.training_corpus_service.get_settlement_focus_summary(limit=200)
        origin_counts = Counter(bundle.room_origin or bundle.room.get("room_origin") for bundle in replay_bundles)
        readiness_split = self._split_historical_bundles(clean_trainable)
        training_ready, draft_only = self._build_training_readiness(clean_trainable, split=readiness_split, mode="gemini-finetune")
        live_shadow_bundles = await self.training_export_service.export_room_bundles(
            limit=self.settings.training_status_room_limit,
            include_non_complete=False,
            origins=[RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value],
        )
        settlement_backfilled_count = sum(
            1
            for bundle in live_shadow_bundles
            if (
                bundle.settlement_label is not None
                and str((bundle.settlement_label or {}).get("source_kind") or "") == self.SETTLEMENT_BACKFILL_SOURCE
            )
        )
        historical_build_readiness = {
            "training_ready": training_ready,
            "draft_only": draft_only,
            "distinct_full_coverage_market_days": len(
                self._historical_market_days_for_coverage(
                    clean_trainable,
                    coverage_class=self.COVERAGE_FULL,
                )
            ),
            "holdout_full_coverage_market_days": len(
                self._historical_market_days_for_coverage(
                    clean_trainable,
                    coverage_class=self.COVERAGE_FULL,
                    room_ids=set(readiness_split.holdout),
                )
            ),
            "split_counts": {
                "train": len(readiness_split.train),
                "validation": len(readiness_split.validation),
                "holdout": len(readiness_split.holdout),
            },
            "settlement_mismatch_count": sum(
                1 for label in settlement_labels if label.crosscheck_status == self.SETTLEMENT_MISMATCH
            ),
            "clean_trainable_count": len(clean_trainable),
        }
        replay_corpus = self._replay_corpus_status(
            replay_runs=replay_runs,
            replay_bundles=replay_bundles,
            replay_audit=replay_audit,
            verbose=verbose,
        )
        stale_build_count = sum(1 for build in historical_builds if build.status == "stale")
        latest_intelligence_payload = intelligence_runs[0].payload if intelligence_runs else None
        confidence = self._confidence_story(
            latest_run_payload=latest_intelligence_payload,
            historical_build_readiness=historical_build_readiness,
            source_replay_coverage=public_coverage,
        )
        latest_pipeline_payload = pipeline_runs[0].payload if pipeline_runs else None
        bootstrap_progress = self._bootstrap_progress_from_pipeline_runs(pipeline_runs)
        return {
            "imported_market_days": len({label.local_market_day for label in settlement_labels}),
            "imported_market_count": len(settlement_labels),
            "replayed_checkpoint_count": len(replay_runs),
            "replayable_market_day_count": coverage["replayable_market_day_count"],
            "full_checkpoint_coverage_count": coverage["full_checkpoint_coverage_count"],
            "late_only_coverage_count": coverage["late_only_coverage_count"],
            "partial_checkpoint_coverage_count": coverage["partial_checkpoint_coverage_count"],
            "outcome_only_coverage_count": coverage["outcome_only_coverage_count"],
            "clean_historical_trainable_count": len(clean_trainable),
            "settlement_mismatch_count": sum(1 for label in settlement_labels if label.crosscheck_status == self.SETTLEMENT_MISMATCH),
            "settlement_mismatch_breakdown": settlement_mismatch_breakdown,
            "source_replay_coverage": public_coverage,
            "checkpoint_archive_coverage": public_checkpoint_capture,
            "market_checkpoint_capture_coverage": public_market_checkpoint_capture,
            "external_archive_coverage": public_external_archive_coverage,
            "external_archive_recovery": public_external_archive_coverage.get("recovery_summary") or {},
            "replay_corpus": replay_corpus,
            "refresh_needed": replay_audit["refresh_needed"],
            "stale_build_count": stale_build_count,
            "coverage_backlog": public_coverage_backlog,
            "promotable_market_day_counts": public_coverage_backlog["promotable_market_day_counts"],
            "checkpoint_archive_promotion_count": coverage_repair_summary["checkpoint_archive_promotion_count"],
            "coverage_repair_summary": coverage_repair_summary,
            "replay_refresh_counts_by_cause": replay_audit.get("refresh_counts_by_cause") or {},
            "source_coverage_gaps": public_coverage["source_coverage_gaps"],
            "missing_checkpoint_reason_counts": public_coverage["missing_checkpoint_reason_counts"],
            "checkpoint_coverage_counts": public_checkpoint_capture["checkpoint_coverage_counts"],
            "checkpoint_capture_gaps": public_checkpoint_capture["checkpoint_capture_gaps"],
            "market_checkpoint_coverage_counts": public_market_checkpoint_capture["checkpoint_coverage_counts"],
            "market_checkpoint_capture_gaps": public_market_checkpoint_capture["checkpoint_capture_gaps"],
            "external_archive_source_counts": public_external_archive_coverage.get("source_counts") or {},
            "external_archive_recovery_summary": public_external_archive_coverage.get("recovery_summary") or {},
            "draft_training_ready": draft_only,
            "training_ready": training_ready,
            "historical_dataset_readiness": historical_build_readiness,
            "historical_build_readiness": historical_build_readiness,
            "confidence_state": confidence["confidence_state"],
            "confidence_scorecard": confidence["confidence_scorecard"],
            "confidence_progress": confidence["confidence_progress"],
            "unsettled_backlog_by_status": dict(settlement_focus.get("status_counts") or {}),
            "possible_ingestion_gap_count": int((settlement_focus.get("status_counts") or {}).get("possible_ingestion_gap", 0)),
            "settlement_backfilled_count": settlement_backfilled_count,
            "origin_room_counts": dict(origin_counts),
            "bootstrap_progress": bootstrap_progress,
            "market_day_coverage": public_coverage.get("market_day_coverage", []),
            "checkpoint_market_day_coverage": public_checkpoint_capture.get("market_day_coverage", []),
            "market_checkpoint_market_day_coverage": public_market_checkpoint_capture.get("market_day_coverage", []),
            "external_archive_market_day_coverage": public_external_archive_coverage.get("market_day_coverage", []),
            "replay_audit": replay_audit,
            "recent_pipeline_runs": [
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
                for run in pipeline_runs
            ],
            "recent_import_runs": [
                {
                    "id": run.id,
                    "status": run.status,
                    "source": run.source,
                    "started_at": run.started_at.isoformat(),
                    "finished_at": run.finished_at.isoformat() if run.finished_at is not None else None,
                    "payload": run.payload,
                }
                for run in import_runs
            ],
            "latest_intelligence_run": latest_intelligence_payload,
            "latest_pipeline_run": latest_pipeline_payload,
        }

    async def _mark_historical_builds_stale(
        self,
        *,
        date_from: date,
        date_to: date,
        affected_room_ids: list[str],
        audit: dict[str, Any],
    ) -> list[str]:
        affected_room_id_set = set(affected_room_ids)
        affected_build_ids: list[str] = []
        now = datetime.now(UTC)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            builds = await repo.list_training_dataset_builds(limit=1000, mode_prefix="historical-")
            for build in builds:
                if build.status == "stale":
                    continue
                build_room_ids = {str(room_id) for room_id in ((build.payload or {}).get("room_ids") or []) if room_id}
                if affected_room_id_set.intersection(build_room_ids) or self._historical_build_overlaps(
                    build,
                    date_from=date_from,
                    date_to=date_to,
                ):
                    await repo.update_training_dataset_build(
                        build.id,
                        status="stale",
                        payload_updates={
                            "stale": {
                                "reason": "historical_replay_refresh",
                                "superseded_at": now.isoformat(),
                                "affected_date_range": {
                                    "date_from": date_from.isoformat(),
                                    "date_to": date_to.isoformat(),
                                },
                                "affected_market_days": audit.get("affected_market_days") or [],
                                "affected_checkpoints": audit.get("affected_checkpoints") or [],
                                "replay_issue_counts": audit.get("issue_counts") or {},
                                "refresh_counts_by_cause": audit.get("refresh_counts_by_cause") or {},
                            }
                        },
                    )
                    affected_build_ids.append(build.id)
            await session.commit()
        return affected_build_ids

    @staticmethod
    def _historical_build_overlaps(build: Any, *, date_from: date, date_to: date) -> bool:
        filters = dict(build.filters or {})
        filter_start = filters.get("date_from")
        filter_end = filters.get("date_to")
        if filter_start and filter_end:
            try:
                return not (
                    date.fromisoformat(str(filter_end)) < date_from
                    or date.fromisoformat(str(filter_start)) > date_to
                )
            except ValueError:
                return False
        if build.selection_window_start is not None and build.selection_window_end is not None:
            range_start = datetime.combine(date_from, time.min, tzinfo=UTC)
            range_end = datetime.combine(date_to, time.max, tzinfo=UTC)
            return not (
                _as_utc(build.selection_window_end) < range_start
                or _as_utc(build.selection_window_start) > range_end
            )
        return False

    def _replay_corpus_status(
        self,
        *,
        replay_runs: list[Any],
        replay_bundles: list[Any],
        replay_audit: dict[str, Any],
        verbose: bool,
    ) -> dict[str, Any]:
        rows: dict[tuple[str, str], dict[str, Any]] = {}
        coverage_counts: Counter[str] = Counter()
        room_ids = {bundle.room["id"] for bundle in replay_bundles}
        for run in replay_runs:
            provenance = self._historical_provenance_from_run(run)
            key = (run.market_ticker, run.local_market_day)
            row = rows.setdefault(
                key,
                {
                    "market_ticker": run.market_ticker,
                    "series_ticker": run.series_ticker,
                    "local_market_day": run.local_market_day,
                    "coverage_class": provenance.get("coverage_class") or self.COVERAGE_NONE,
                    "checkpoint_labels": [],
                    "checkpoint_count": 0,
                    "materialized_room_count": 0,
                    "room_ids": [],
                },
            )
            row["coverage_class"] = provenance.get("coverage_class") or row["coverage_class"]
            row["checkpoint_labels"].append(run.checkpoint_label)
            row["checkpoint_count"] += 1
            if run.room_id:
                row["room_ids"].append(run.room_id)
                if run.room_id in room_ids:
                    row["materialized_room_count"] += 1
        for row in rows.values():
            row["checkpoint_labels"] = sorted(set(row["checkpoint_labels"]))
            row["room_ids"] = sorted(set(row["room_ids"]))
            coverage_counts[row["coverage_class"]] += 1
        return {
            "room_count": len(room_ids),
            "checkpoint_count": len(replay_runs),
            "market_day_count": len(rows),
            "coverage_class_counts": {
                self.COVERAGE_FULL: coverage_counts.get(self.COVERAGE_FULL, 0),
                self.COVERAGE_LATE_ONLY: coverage_counts.get(self.COVERAGE_LATE_ONLY, 0),
                self.COVERAGE_PARTIAL: coverage_counts.get(self.COVERAGE_PARTIAL, 0),
                self.COVERAGE_OUTCOME_ONLY: coverage_counts.get(self.COVERAGE_OUTCOME_ONLY, 0),
                self.COVERAGE_NONE: coverage_counts.get(self.COVERAGE_NONE, 0),
            },
            "stale_replay_count": int((replay_audit.get("issue_counts") or {}).get("stale_replay", 0)),
            "missing_replay_count": int((replay_audit.get("issue_counts") or {}).get("missing_replay", 0)),
            "orphan_replay_count": int((replay_audit.get("issue_counts") or {}).get("orphan_replay", 0)),
            "market_day_coverage": list(rows.values()) if verbose else list(rows.values())[:12],
        }

    def _bootstrap_progress_from_pipeline_runs(self, pipeline_runs: list[Any]) -> dict[str, Any] | None:
        for run in pipeline_runs:
            payload = run.payload or {}
            progress = payload.get("bootstrap_progress")
            if isinstance(progress, dict):
                return progress
        return None

    def _coverage_backlog(
        self,
        *,
        coverage_rows: list[dict[str, Any]],
        checkpoint_rows: list[dict[str, Any]],
        settlement_labels: list[Any],
        verbose: bool,
    ) -> dict[str, Any]:
        checkpoint_index = {
            (str(row["market_ticker"]), str(row["local_market_day"])): row
            for row in checkpoint_rows
        }
        settlement_index = {
            (str(label.market_ticker), str(label.local_market_day)): label
            for label in settlement_labels
        }
        checkpoint_reason_counts: Counter[str] = Counter()
        market_day_reason_counts: Counter[str] = Counter()
        promotable_market_day_counts: Counter[str] = Counter()
        backlog_rows: list[dict[str, Any]] = []

        for row in coverage_rows:
            key = (str(row["market_ticker"]), str(row["local_market_day"]))
            archive_row = checkpoint_index.get(key, {})
            label = settlement_index.get(key)
            day_reasons: set[str] = set()
            checkpoint_reason_counter: Counter[str] = Counter()
            market_source_present = False
            weather_source_present = False

            for checkpoint in row.get("checkpoints") or []:
                if checkpoint.get("market_snapshot_id") or checkpoint.get("market_source_kind"):
                    market_source_present = True
                if checkpoint.get("weather_snapshot_id") or checkpoint.get("weather_source_kind"):
                    weather_source_present = True
                checkpoint_reason_counter.update(checkpoint.get("missing_reasons") or [])
                day_reasons.update(checkpoint.get("missing_reasons") or [])

            for checkpoint in archive_row.get("checkpoints") or []:
                if not checkpoint.get("captured"):
                    checkpoint_reason_counter["checkpoint_archive_missing"] += 1
                    day_reasons.add("checkpoint_archive_missing")

            if label is not None and label.crosscheck_status == self.SETTLEMENT_MISSING:
                day_reasons.add("settlement_crosscheck_missing")

            checkpoint_reason_counts.update(checkpoint_reason_counter)
            market_day_reason_counts.update(day_reasons)

            coverage_class = str(row.get("coverage_class") or self.COVERAGE_NONE)
            if coverage_class == self.COVERAGE_FULL:
                promotable_bucket = "already_full_checkpoint_coverage"
            elif coverage_class in {self.COVERAGE_LATE_ONLY, self.COVERAGE_PARTIAL}:
                promotable_bucket = "promotable_to_full_checkpoint_coverage"
            elif market_source_present or weather_source_present:
                promotable_bucket = "promotable_to_partial_or_late_only"
            else:
                promotable_bucket = "permanently_outcome_only_with_current_sources"
            promotable_market_day_counts[promotable_bucket] += 1

            backlog_rows.append(
                {
                    "market_ticker": row["market_ticker"],
                    "series_ticker": row.get("series_ticker"),
                    "local_market_day": row["local_market_day"],
                    "coverage_class": coverage_class,
                    "promotable_status": promotable_bucket,
                    "market_source_present": market_source_present,
                    "weather_source_present": weather_source_present,
                    "day_reasons": sorted(day_reasons),
                    "checkpoint_reason_counts": dict(checkpoint_reason_counter),
                    "crosscheck_missing": bool(label is not None and label.crosscheck_status == self.SETTLEMENT_MISSING),
                }
            )

        return {
            "reason_counts": dict(checkpoint_reason_counts),
            "market_day_reason_counts": dict(market_day_reason_counts),
            "samples": backlog_rows if verbose else backlog_rows[:12],
            "all_samples": backlog_rows,
            "promotable_market_day_counts": {
                "already_full_checkpoint_coverage": promotable_market_day_counts.get("already_full_checkpoint_coverage", 0),
                "promotable_to_full_checkpoint_coverage": promotable_market_day_counts.get("promotable_to_full_checkpoint_coverage", 0),
                "promotable_to_partial_or_late_only": promotable_market_day_counts.get("promotable_to_partial_or_late_only", 0),
                "permanently_outcome_only_with_current_sources": promotable_market_day_counts.get(
                    "permanently_outcome_only_with_current_sources",
                    0,
                ),
            },
        }

    def _coverage_repair_summary(
        self,
        *,
        coverage_backlog: dict[str, Any],
        checkpoint_rows: list[dict[str, Any]],
        external_archive_coverage: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        external_archive_coverage = external_archive_coverage or {}
        promoted_checkpoint_archives = 0
        recoverable_weather_gap_market_days = 0
        permanent_weather_gap_market_days = 0
        recoverable_checkpoint_archive_gap_market_days = 0
        permanent_checkpoint_archive_gap_market_days = 0

        for row in checkpoint_rows:
            for checkpoint in row.get("checkpoints") or []:
                if checkpoint.get("source_kind") == self.PROMOTED_CHECKPOINT_ARCHIVE_SOURCE:
                    promoted_checkpoint_archives += 1

        for sample in coverage_backlog.get("all_samples") or []:
            promotable_status = str(sample.get("promotable_status") or "")
            day_reasons = set(sample.get("day_reasons") or [])
            recoverable = promotable_status != "permanently_outcome_only_with_current_sources"
            if "weather_snapshot_missing" in day_reasons:
                if recoverable:
                    recoverable_weather_gap_market_days += 1
                else:
                    permanent_weather_gap_market_days += 1
            if "checkpoint_archive_missing" in day_reasons:
                if recoverable:
                    recoverable_checkpoint_archive_gap_market_days += 1
                else:
                    permanent_checkpoint_archive_gap_market_days += 1

        return {
            "checkpoint_archive_promotion_count": promoted_checkpoint_archives,
            "recoverable_market_day_count": int(
                (coverage_backlog.get("promotable_market_day_counts") or {}).get("promotable_to_full_checkpoint_coverage", 0)
                + (coverage_backlog.get("promotable_market_day_counts") or {}).get("promotable_to_partial_or_late_only", 0)
            ),
            "permanent_outcome_only_market_day_count": int(
                (coverage_backlog.get("promotable_market_day_counts") or {}).get("permanently_outcome_only_with_current_sources", 0)
            ),
            "recoverable_weather_gap_market_day_count": recoverable_weather_gap_market_days,
            "permanent_weather_gap_market_day_count": permanent_weather_gap_market_days,
            "recoverable_checkpoint_archive_gap_market_day_count": recoverable_checkpoint_archive_gap_market_days,
            "permanent_checkpoint_archive_gap_market_day_count": permanent_checkpoint_archive_gap_market_days,
            "external_archive_assisted_checkpoint_count": int(
                ((external_archive_coverage.get("source_counts") or {}).get("assisted_checkpoint_count") or 0)
            ),
            "recovered_via_external_archive_market_day_count": int(
                ((external_archive_coverage.get("recovery_summary") or {}).get("recovered_via_external_archive_market_day_count") or 0)
            ),
            "missing_native_archive_but_recoverable_via_external_market_day_count": int(
                ((external_archive_coverage.get("recovery_summary") or {}).get(
                    "missing_native_archive_but_recoverable_via_external_market_day_count"
                ) or 0)
            ),
            "still_unrecoverable_even_with_external_market_day_count": int(
                ((external_archive_coverage.get("recovery_summary") or {}).get(
                    "still_unrecoverable_even_with_external_market_day_count"
                ) or 0)
            ),
        }

    def _build_replay_audit(
        self,
        *,
        coverage_rows: list[dict[str, Any]],
        replay_runs: list[Any],
        verbose: bool,
    ) -> dict[str, Any]:
        source_index: dict[tuple[str, str], dict[str, Any]] = {}
        for row in coverage_rows:
            for checkpoint in row.get("checkpoints") or []:
                source_index[(str(row["market_ticker"]), str(checkpoint["checkpoint_ts"]))] = {
                    "market_ticker": row["market_ticker"],
                    "series_ticker": row.get("series_ticker"),
                    "local_market_day": row["local_market_day"],
                    "coverage_class": row["coverage_class"],
                    "replay_logic_version": self.replay_logic_version(),
                    "checkpoint_label": checkpoint["checkpoint_label"],
                    "checkpoint_ts": checkpoint["checkpoint_ts"],
                    "replayable": bool(checkpoint.get("replayable")),
                    "market_source_kind": checkpoint.get("market_source_kind"),
                    "weather_source_kind": checkpoint.get("weather_source_kind"),
                    "market_snapshot_id": checkpoint.get("market_snapshot_id"),
                    "weather_snapshot_id": checkpoint.get("weather_snapshot_id"),
                    "settlement_crosscheck_status": row.get("settlement_crosscheck_status"),
                    "settlement_mismatch_reason": row.get("settlement_mismatch_reason"),
                    "settlement_label_signature": row.get("settlement_label_signature"),
                    "missing_reasons": list(checkpoint.get("missing_reasons") or []),
                }
        replay_index = {
            (run.market_ticker, _as_utc(run.checkpoint_ts).isoformat()): {
                "run_id": run.id,
                "room_id": run.room_id,
                "status": run.status,
                "market_ticker": run.market_ticker,
                "series_ticker": run.series_ticker,
                "local_market_day": run.local_market_day,
                "checkpoint_label": run.checkpoint_label,
                "checkpoint_ts": _as_utc(run.checkpoint_ts).isoformat(),
                **self._historical_provenance_from_run(run),
            }
            for run in replay_runs
        }
        counts: Counter[str] = Counter()
        refresh_cause_counts: Counter[str] = Counter()
        affected_room_ids: set[str] = set()
        affected_run_ids: set[str] = set()
        affected_market_days: set[tuple[str, str]] = set()
        affected_checkpoints: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []

        def _refresh_causes(classification: str, reasons: list[str]) -> list[str]:
            causes: set[str] = set()
            if classification in {"missing_replay", "orphan_replay"}:
                causes.add("coverage_repair")
            for reason in reasons:
                if reason.startswith("settlement_"):
                    causes.add("settlement_repair")
                elif reason == "replay_logic_version_changed":
                    causes.add("replay_logic_change")
                else:
                    causes.add("coverage_repair")
            return sorted(causes)

        def _record_issue(classification: str, source: dict[str, Any] | None, replay: dict[str, Any] | None, reasons: list[str]) -> None:
            counts[classification] += 1
            refresh_causes = _refresh_causes(classification, reasons)
            refresh_cause_counts.update(refresh_causes)
            market_ticker = str((source or replay or {}).get("market_ticker") or "")
            local_market_day = str((source or replay or {}).get("local_market_day") or "")
            checkpoint_ts = str((source or replay or {}).get("checkpoint_ts") or "")
            checkpoint_label = str((source or replay or {}).get("checkpoint_label") or "")
            if replay and replay.get("room_id"):
                affected_room_ids.add(str(replay["room_id"]))
            if replay and replay.get("run_id"):
                affected_run_ids.add(str(replay["run_id"]))
            if market_ticker and local_market_day:
                affected_market_days.add((market_ticker, local_market_day))
            affected_checkpoints.append(
                {
                    "market_ticker": market_ticker,
                    "local_market_day": local_market_day,
                    "checkpoint_label": checkpoint_label,
                    "checkpoint_ts": checkpoint_ts,
                    "classification": classification,
                    "refresh_causes": refresh_causes,
                }
            )
            issues.append(
                {
                    "classification": classification,
                    "market_ticker": market_ticker,
                    "series_ticker": (source or replay or {}).get("series_ticker"),
                    "local_market_day": local_market_day,
                    "checkpoint_label": checkpoint_label,
                    "checkpoint_ts": checkpoint_ts,
                    "replay_room_id": (replay or {}).get("room_id"),
                    "replay_run_id": (replay or {}).get("run_id"),
                    "source_coverage_class": (source or {}).get("coverage_class"),
                    "replay_coverage_class": (replay or {}).get("coverage_class"),
                    "source_replay_logic_version": (source or {}).get("replay_logic_version"),
                    "replay_logic_version": (replay or {}).get("replay_logic_version"),
                    "source_market_snapshot_id": (source or {}).get("market_snapshot_id"),
                    "source_weather_snapshot_id": (source or {}).get("weather_snapshot_id"),
                    "replay_market_snapshot_id": (replay or {}).get("market_snapshot_source_id"),
                    "replay_weather_snapshot_id": (replay or {}).get("weather_snapshot_source_id"),
                    "source_settlement_crosscheck_status": (source or {}).get("settlement_crosscheck_status"),
                    "source_settlement_mismatch_reason": (source or {}).get("settlement_mismatch_reason"),
                    "replay_settlement_crosscheck_status": (replay or {}).get("settlement_crosscheck_status"),
                    "replay_settlement_mismatch_reason": (replay or {}).get("settlement_mismatch_reason"),
                    "refresh_causes": refresh_causes,
                    "reasons": reasons,
                    "missing_reasons": (source or {}).get("missing_reasons") or [],
                }
            )

        for key, source in source_index.items():
            replay = replay_index.pop(key, None)
            if source["replayable"]:
                if replay is None:
                    _record_issue("missing_replay", source, None, ["replay_missing_for_source_coverage"])
                    continue
                mismatch_reasons: list[str] = []
                if replay.get("status") != "completed":
                    mismatch_reasons.append("replay_status_not_completed")
                if not replay.get("room_id"):
                    mismatch_reasons.append("replay_room_missing")
                for field_name, replay_field in (
                    ("coverage_class", "coverage_class"),
                    ("replay_logic_version", "replay_logic_version"),
                    ("market_source_kind", "market_source_kind"),
                    ("weather_source_kind", "weather_source_kind"),
                    ("market_snapshot_id", "market_snapshot_source_id"),
                    ("weather_snapshot_id", "weather_snapshot_source_id"),
                    ("settlement_crosscheck_status", "settlement_crosscheck_status"),
                    ("settlement_mismatch_reason", "settlement_mismatch_reason"),
                    ("settlement_label_signature", "settlement_label_signature"),
                ):
                    source_value = source.get(field_name)
                    replay_value = replay.get(replay_field)
                    if source_value != replay_value:
                        mismatch_reasons.append(f"{field_name}_changed")
                if mismatch_reasons:
                    _record_issue("stale_replay", source, replay, mismatch_reasons)
            elif replay is not None:
                orphan_reasons = list(source.get("missing_reasons") or []) or ["source_no_longer_replayable"]
                _record_issue("orphan_replay", source, replay, orphan_reasons)

        for replay in replay_index.values():
            _record_issue("orphan_replay", None, replay, ["source_checkpoint_missing"])

        return {
            "refresh_needed": bool(issues),
            "issue_counts": dict(counts),
            "refresh_counts_by_cause": {
                "coverage_repair": refresh_cause_counts.get("coverage_repair", 0),
                "settlement_repair": refresh_cause_counts.get("settlement_repair", 0),
                "replay_logic_change": refresh_cause_counts.get("replay_logic_change", 0),
            },
            "affected_room_ids": sorted(affected_room_ids),
            "affected_run_ids": sorted(affected_run_ids),
            "affected_market_days": [
                {"market_ticker": market_ticker, "local_market_day": local_market_day}
                for market_ticker, local_market_day in sorted(affected_market_days)
            ],
            "affected_checkpoints": affected_checkpoints[:50] if not verbose else affected_checkpoints,
            "issues": issues[:50] if not verbose else issues,
        }

    @staticmethod
    def _historical_provenance_from_run(run: Any) -> dict[str, Any]:
        return dict((run.payload or {}).get("historical_provenance") or {})

    def _crosscheck_mismatch_reason_from_label(self, label: Any) -> str | None:
        payload = dict(label.payload or {})
        crosscheck = dict(payload.get("crosscheck") or {})
        mismatch_reason = crosscheck.get("mismatch_reason")
        if mismatch_reason:
            return str(mismatch_reason)
        if label.crosscheck_status == self.SETTLEMENT_MISMATCH:
            return self.SETTLEMENT_MISMATCH_REASON_DISAGREEMENT
        if label.crosscheck_status == self.SETTLEMENT_MISSING:
            return self.SETTLEMENT_MISMATCH_REASON_MISSING
        return None

    def _settlement_label_signature(self, label: Any) -> str:
        return json.dumps(
            {
                "crosscheck_status": label.crosscheck_status,
                "crosscheck_result": label.crosscheck_result,
                "crosscheck_high_f": str(label.crosscheck_high_f) if label.crosscheck_high_f is not None else None,
                "mismatch_reason": self._crosscheck_mismatch_reason_from_label(label),
                "kalshi_result": label.kalshi_result,
                "settlement_value_dollars": (
                    str(label.settlement_value_dollars)
                    if label.settlement_value_dollars is not None
                    else None
                ),
            },
            sort_keys=True,
        )

    def _settlement_mismatch_breakdown(self, settlement_labels: list[Any]) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for label in settlement_labels:
            mismatch_reason = self._crosscheck_mismatch_reason_from_label(label)
            if mismatch_reason is not None:
                counts[mismatch_reason] += 1
        return {
            self.SETTLEMENT_MISMATCH_REASON_THRESHOLD_EDGE: counts.get(self.SETTLEMENT_MISMATCH_REASON_THRESHOLD_EDGE, 0),
            self.SETTLEMENT_MISMATCH_REASON_DISAGREEMENT: counts.get(self.SETTLEMENT_MISMATCH_REASON_DISAGREEMENT, 0),
            self.SETTLEMENT_MISMATCH_REASON_MISSING: counts.get(self.SETTLEMENT_MISMATCH_REASON_MISSING, 0),
        }

    async def _import_market_definitions(
        self,
        *,
        date_from: date,
        date_to: date,
        templates: list[WeatherSeriesTemplate],
    ) -> dict[str, int]:
        by_ticker: dict[str, tuple[WeatherMarketMapping, dict[str, Any]]] = {}
        for template in templates:
            for market in await self._list_recent_markets(template, date_from=date_from, date_to=date_to):
                mapping = template.resolve_market(market)
                if mapping is not None:
                    by_ticker[mapping.market_ticker] = (mapping, market)
            for market in await self._list_historical_markets(template, date_from=date_from, date_to=date_to):
                mapping = template.resolve_market(market)
                if mapping is not None:
                    by_ticker.setdefault(mapping.market_ticker, (mapping, market))

        mismatch_count = 0
        missing_count = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for mapping, market in by_ticker.values():
                local_day = self._market_local_day(mapping, market)
                settlement_ts = self._market_timestamp(market, "settlement_ts", "settlement_time")
                close_ts = self._market_timestamp(market, "close_time", "close_ts")
                settlement_value = self._market_settlement_value(market)
                crosscheck = await self._daily_summary_crosscheck(
                    mapping,
                    local_day,
                    kalshi_result=str(market.get("result") or "") or None,
                )
                if crosscheck["status"] == self.SETTLEMENT_MISMATCH:
                    mismatch_count += 1
                elif crosscheck["status"] == self.SETTLEMENT_MISSING:
                    missing_count += 1
                await repo.upsert_historical_market_snapshot(
                    market_ticker=mapping.market_ticker,
                    series_ticker=mapping.series_ticker,
                    station_id=mapping.station_id,
                    local_market_day=local_day,
                    asof_ts=settlement_ts or close_ts or datetime.now(UTC),
                    source_kind=self.FINAL_MARKET_SOURCE,
                    source_id=f"final:{mapping.market_ticker}",
                    source_hash=_hash_payload(market),
                    close_ts=close_ts,
                    settlement_ts=settlement_ts,
                    yes_bid_dollars=_parse_decimal(market.get("yes_bid_dollars")),
                    yes_ask_dollars=_parse_decimal(market.get("yes_ask_dollars")),
                    no_ask_dollars=_parse_decimal(market.get("no_ask_dollars")),
                    last_price_dollars=_parse_decimal(market.get("last_price_dollars")),
                    payload={"market": market},
                )
                await repo.upsert_historical_settlement_label(
                    market_ticker=mapping.market_ticker,
                    series_ticker=mapping.series_ticker,
                    local_market_day=local_day,
                    source_kind="kalshi_primary",
                    kalshi_result=str(market.get("result") or "") or None,
                    settlement_value_dollars=settlement_value,
                    settlement_ts=settlement_ts,
                    crosscheck_status=crosscheck["status"],
                    crosscheck_high_f=crosscheck["daily_high_f"],
                    crosscheck_result=crosscheck["result"],
                    payload=_json_safe({
                        "market": market,
                        "crosscheck": crosscheck,
                    }),
                )
            await session.commit()
        return {
            "market_day_count": len({self._market_local_day(mapping, market) for mapping, market in by_ticker.values()}),
            "market_count": len(by_ticker),
            "settlement_mismatch_count": mismatch_count,
            "crosscheck_missing_count": missing_count,
        }

    async def _import_captured_market_snapshots(
        self,
        *,
        date_from: date,
        date_to: date,
        templates: list[WeatherSeriesTemplate],
    ) -> int:
        after = datetime.combine(date_from, time.min, tzinfo=UTC) - timedelta(hours=self.settings.historical_replay_market_snapshot_lookback_hours)
        before = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=UTC)
        prefixes = tuple(template.series_ticker for template in templates)
        created = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            events = await repo.list_exchange_events(
                event_type="market_snapshot",
                created_after=after,
                created_before=before,
                limit=20000,
            )
            for event in events:
                payload = _market_payload(event.payload)
                ticker = str(payload.get("ticker") or event.market_ticker or "")
                if not ticker.startswith(prefixes):
                    continue
                mapping = self._mapping_for_market(ticker, payload)
                if mapping is None:
                    continue
                local_day = self._market_local_day(mapping, payload)
                if not (date_from.isoformat() <= local_day <= date_to.isoformat()):
                    continue
                asof_ts = self._market_timestamp(payload, "updated_time") or _as_utc(event.created_at) or datetime.now(UTC)
                await repo.upsert_historical_market_snapshot(
                    market_ticker=ticker,
                    series_ticker=mapping.series_ticker,
                    station_id=mapping.station_id,
                    local_market_day=local_day,
                    asof_ts=asof_ts,
                    source_kind=self.CAPTURED_MARKET_SOURCE,
                    source_id=event.id,
                    source_hash=_hash_payload(event.payload),
                    close_ts=self._market_timestamp(payload, "close_time", "close_ts"),
                    settlement_ts=self._market_timestamp(payload, "settlement_ts", "settlement_time"),
                    yes_bid_dollars=_parse_decimal(payload.get("yes_bid_dollars")),
                    yes_ask_dollars=_parse_decimal(payload.get("yes_ask_dollars")),
                    no_ask_dollars=_parse_decimal(payload.get("no_ask_dollars")),
                    last_price_dollars=_parse_decimal(payload.get("last_price_dollars")),
                    payload=event.payload,
                )
                created += 1
            await session.commit()
        return created

    async def _import_captured_weather_snapshots(
        self,
        *,
        date_from: date,
        date_to: date,
        templates: list[WeatherSeriesTemplate],
    ) -> int:
        after = datetime.combine(date_from, time.min, tzinfo=UTC) - timedelta(hours=self.settings.historical_replay_market_snapshot_lookback_hours)
        before = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=UTC)
        station_ids = {template.station_id for template in templates}
        created = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            events = await repo.list_weather_events(created_after=after, created_before=before, limit=20000)
            for event in events:
                if event.station_id not in station_ids:
                    continue
                payload = event.payload if isinstance(event.payload, dict) else {}
                mapping_payload = payload.get("mapping") or {}
                station_id = str(mapping_payload.get("station_id") or event.station_id)
                template = next((item for item in templates if item.station_id == station_id), None)
                if template is None:
                    continue
                mapping = WeatherMarketMapping(
                    market_ticker=str(mapping_payload.get("market_ticker") or template.series_ticker),
                    market_type="weather",
                    display_name=mapping_payload.get("display_name") or template.display_name,
                    description=mapping_payload.get("description") or template.description,
                    research_queries=list(mapping_payload.get("research_queries") or template.research_queries),
                    research_urls=list(mapping_payload.get("research_urls") or template.research_urls),
                    station_id=template.station_id,
                    daily_summary_station_id=template.daily_summary_station_id,
                    location_name=template.location_name,
                    timezone_name=template.timezone_name,
                    latitude=template.latitude,
                    longitude=template.longitude,
                    threshold_f=float(mapping_payload.get("threshold_f") or 0) if mapping_payload.get("threshold_f") not in (None, "") else None,
                    operator=str(mapping_payload.get("operator") or ">"),
                    metric=template.metric,
                    settlement_source=mapping_payload.get("settlement_source") or template.settlement_source,
                    series_ticker=template.series_ticker,
                )
                local_day = self._weather_local_day(mapping, payload, fallback=_as_utc(event.created_at) or datetime.now(UTC))
                if not (date_from.isoformat() <= local_day <= date_to.isoformat()):
                    continue
                observation_ts = _parse_iso(((payload.get("observation") or {}).get("properties") or {}).get("timestamp"))
                forecast_updated_ts = _parse_iso(((payload.get("forecast") or {}).get("properties") or {}).get("updated"))
                asof_ts = _as_utc(event.created_at) or observation_ts or forecast_updated_ts or datetime.now(UTC)
                await repo.upsert_historical_weather_snapshot(
                    station_id=station_id,
                    series_ticker=template.series_ticker,
                    local_market_day=local_day,
                    asof_ts=asof_ts,
                    source_kind=self.CAPTURED_WEATHER_SOURCE,
                    source_id=event.id,
                    source_hash=_hash_payload(payload),
                    observation_ts=observation_ts,
                    forecast_updated_ts=forecast_updated_ts,
                    forecast_high_f=self._quantize_two(extract_forecast_high_f((payload.get("forecast") or {}))),
                    current_temp_f=self._quantize_two(extract_current_temp_f((payload.get("observation") or {}))),
                    payload=payload,
                )
                created += 1
            await session.commit()
        return created

    async def _import_file_weather_archives(
        self,
        *,
        date_from: date,
        date_to: date,
        templates: list[WeatherSeriesTemplate],
    ) -> int:
        archive_dir = Path(self.settings.historical_weather_archive_path)
        if not archive_dir.exists():
            return 0
        templates_by_station = {template.station_id: template for template in templates}
        created = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for path in sorted(archive_dir.rglob("*.jsonl")):
                with path.open("r", encoding="utf-8") as handle:
                    for line_number, line in enumerate(handle, start=1):
                        line = line.strip()
                        if not line:
                            continue
                        payload = json.loads(line)
                        mapping_payload = payload.get("mapping") or {}
                        station_id = str(mapping_payload.get("station_id") or "")
                        template = templates_by_station.get(station_id)
                        if template is None:
                            continue
                        mapping = template.resolve_market({"ticker": f"{template.series_ticker}-00JAN00-T0", "strike_type": "greater", "floor_strike": 0}) or WeatherMarketMapping(
                            market_ticker=template.series_ticker,
                            market_type="weather",
                            display_name=template.display_name,
                            description=template.description,
                            research_queries=list(template.research_queries),
                            research_urls=list(template.research_urls),
                            station_id=template.station_id,
                            daily_summary_station_id=template.daily_summary_station_id,
                            location_name=template.location_name,
                            timezone_name=template.timezone_name,
                            latitude=template.latitude,
                            longitude=template.longitude,
                            threshold_f=0,
                            operator=">",
                            metric=template.metric,
                            settlement_source=template.settlement_source,
                            series_ticker=template.series_ticker,
                        )
                        local_day = self._weather_local_day(mapping, payload, fallback=datetime.now(UTC))
                        if not (date_from.isoformat() <= local_day <= date_to.isoformat()):
                            continue
                        observation_ts = _parse_iso(((payload.get("observation") or {}).get("properties") or {}).get("timestamp"))
                        forecast_updated_ts = _parse_iso(((payload.get("forecast") or {}).get("properties") or {}).get("updated"))
                        archive_meta = weather_bundle_archive_metadata(payload, captured_at=_parse_iso(((payload.get("_archive") or {}).get("captured_at"))))
                        asof_ts = archive_meta["asof_ts"] if archive_meta is not None else max(item for item in [observation_ts, forecast_updated_ts, datetime.now(UTC)] if item is not None)
                        source_id = str(((payload.get("_archive") or {}).get("source_id")) or f"{path.relative_to(archive_dir)}:{line_number}")
                        source_kind = str(((payload.get("_archive") or {}).get("archive_source")) or self.ARCHIVED_WEATHER_SOURCE)
                        await repo.upsert_historical_weather_snapshot(
                            station_id=template.station_id,
                            series_ticker=template.series_ticker,
                            local_market_day=local_day,
                            asof_ts=asof_ts,
                            source_kind=self.ARCHIVED_WEATHER_SOURCE if source_kind != self.CAPTURED_WEATHER_SOURCE else source_kind,
                            source_id=source_id,
                            source_hash=_hash_payload(payload),
                            observation_ts=observation_ts,
                            forecast_updated_ts=forecast_updated_ts,
                            forecast_high_f=self._quantize_two(extract_forecast_high_f((payload.get("forecast") or {}))),
                            current_temp_f=self._quantize_two(extract_current_temp_f((payload.get("observation") or {}))),
                            payload=payload,
                        )
                        created += 1
            await session.commit()
        return created

    async def _run_replay_room(
        self,
        *,
        mapping: WeatherMarketMapping,
        settlement_label,
        market_snapshot,
        weather_snapshot,
        checkpoint_label: str,
        checkpoint_ts: datetime,
        market_source_kind: str | None,
        weather_source_kind: str | None,
        coverage_class: str,
    ) -> str:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            pack = await self.agent_pack_service.get_pack(repo, self.settings.active_agent_pack_version)
            thresholds = self.agent_pack_service.runtime_thresholds(pack)
            heuristic_pack = await self.historical_heuristic_service.get_active_pack(repo) if self.historical_heuristic_service is not None else None
            role_models = {
                role_name: {
                    "provider": config.provider,
                    "model": config.model,
                    "temperature": config.temperature,
                }
                for role_name, config in pack.roles.items()
            }
            room = await repo.create_room(
                RoomCreate(
                    name=f"Historical Replay {mapping.market_ticker} {checkpoint_label}",
                    market_ticker=mapping.market_ticker,
                    prompt=f"Historical replay for {settlement_label.local_market_day} at {checkpoint_label}",
                ),
                active_color=self.settings.app_color,
                shadow_mode=False,
                kill_switch_enabled=False,
                kalshi_env=self.settings.kalshi_env,
                room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                agent_pack_version=pack.version,
                role_models=role_models,
            )
            await repo.append_message(
                room.id,
                RoomMessageCreate(
                    role=AgentRole.SUPERVISOR,
                    kind=MessageKind.OBSERVATION,
                    stage=RoomStage.TRIGGERED,
                    content=(
                        f"Supervisor started historical replay for {room.market_ticker} at "
                        f"{checkpoint_ts.isoformat()}."
                    ),
                    payload={
                        "reason": "historical_replay",
                        "checkpoint_label": checkpoint_label,
                        "checkpoint_ts": checkpoint_ts.isoformat(),
                    },
                ),
            )

            market_response = market_snapshot.payload
            weather_bundle = weather_snapshot.payload
            dossier = self.research_coordinator.build_structured_dossier_from_snapshot(
                market_ticker=room.market_ticker,
                market_response=market_response,
                mapping=mapping,
                weather_bundle=weather_bundle,
                reference_time=checkpoint_ts,
                last_run_id=f"historical:{settlement_label.id}:{checkpoint_label}",
            )
            delta = self.research_coordinator.build_room_delta(
                dossier=dossier,
                market_response=market_response,
                weather_bundle=weather_bundle,
            )
            signal = self.research_coordinator.build_signal_from_dossier(
                dossier,
                market_response,
                min_edge_bps=thresholds.risk_min_edge_bps,
            )
            if self.historical_heuristic_service is not None:
                heuristic_application = self.historical_heuristic_service.apply_to_signal(
                    pack=heuristic_pack,
                    mapping=mapping,
                    signal=signal,
                    market_snapshot=market_response,
                    reference_time=checkpoint_ts,
                    base_thresholds=thresholds,
                    market_stale=is_market_stale(
                        observed_at=market_snapshot.asof_ts,
                        stale_after_seconds=self.settings.risk_stale_market_seconds,
                        reference_time=checkpoint_ts,
                    ),
                    research_stale=dossier.freshness.stale,
                    coverage_class=coverage_class,
                )
                thresholds = self.historical_heuristic_service.runtime_thresholds(
                    base_thresholds=thresholds,
                    application=heuristic_application,
                )
                signal.heuristic_application = heuristic_application
                signal = apply_heuristic_application_to_signal(
                    settings=self.settings,
                    signal=signal,
                    market_snapshot=market_response,
                    min_edge_bps=thresholds.risk_min_edge_bps,
                    spread_limit_bps=thresholds.trigger_max_spread_bps,
                )
            signal.eligibility = evaluate_trade_eligibility(
                settings=self.settings,
                signal=signal,
                market_snapshot=market_response,
                market_observed_at=market_snapshot.asof_ts,
                research_freshness=dossier.freshness,
                thresholds=thresholds,
                decision_time=checkpoint_ts,
                market_stale_after_seconds=self.settings.historical_replay_market_stale_seconds,
            )
            signal.strategy_mode = signal.eligibility.strategy_mode
            signal.stand_down_reason = signal.eligibility.stand_down_reason
            if signal.eligibility.reasons and not signal.eligibility.eligible:
                signal.summary = f"{signal.summary} Stand down: {' '.join(signal.eligibility.reasons)}"

            await repo.save_signal(
                room_id=room.id,
                market_ticker=room.market_ticker,
                fair_yes_dollars=signal.fair_yes_dollars,
                edge_bps=signal.edge_bps,
                confidence=signal.confidence,
                summary=signal.summary,
                payload={
                    "research_mode": dossier.mode,
                    "research_gate_passed": dossier.gate.passed,
                    "research_last_run_id": dossier.last_run_id,
                    "research_delta": delta.model_dump(mode="json"),
                    "trader_context": dossier.trader_context.model_dump(mode="json"),
                    "research_freshness": dossier.freshness.model_dump(mode="json"),
                    "effective_research_freshness": dossier.freshness.model_dump(mode="json"),
                    "resolution_state": signal.resolution_state.value,
                    "strategy_mode": signal.strategy_mode.value,
                    "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None,
                    "stand_down_reason": signal.stand_down_reason.value if signal.stand_down_reason is not None else None,
                    "agent_pack_version": pack.version,
                    "heuristic_pack_version": (signal.heuristic_application or {}).get("heuristic_pack_version"),
                    "intelligence_run_id": (signal.heuristic_application or {}).get("intelligence_run_id"),
                    "candidate_pack_id": (signal.heuristic_application or {}).get("candidate_pack_id"),
                    "heuristic_summary": (signal.heuristic_application or {}).get("agent_summary"),
                    "rule_trace": list((signal.heuristic_application or {}).get("rule_trace") or []),
                    "support_window": dict((signal.heuristic_application or {}).get("support_window") or {}),
                    "historical_replay": True,
                    "historical_replay_logic_version": self.replay_logic_version(),
                },
            )

            recent_memories = [note.summary for note in await repo.list_recent_memory_notes(limit=5)]
            await repo.save_artifact(
                room_id=room.id,
                artifact_type="historical_provenance",
                source="historical_replay",
                title=f"Historical replay provenance for {room.market_ticker}",
                payload={
                    "historical_provenance": {
                        "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                        "local_market_day": settlement_label.local_market_day,
                        "checkpoint_label": checkpoint_label,
                        "checkpoint_ts": checkpoint_ts.isoformat(),
                        "timezone_name": self._timezone_name(mapping),
                        "market_snapshot_source_id": market_snapshot.id,
                        "weather_snapshot_source_id": weather_snapshot.id,
                        "market_source_kind": market_source_kind,
                        "weather_source_kind": weather_source_kind,
                        "settlement_label_id": settlement_label.id,
                        "settlement_crosscheck_status": settlement_label.crosscheck_status,
                        "settlement_mismatch_reason": self._crosscheck_mismatch_reason_from_label(settlement_label),
                        "settlement_label_signature": self._settlement_label_signature(settlement_label),
                        "coverage_class": coverage_class,
                        "replay_logic_version": self.replay_logic_version(),
                        "source_coverage": {
                            "market_snapshot": True,
                            "weather_snapshot": True,
                            "settlement_label": True,
                        },
                    }
                },
            )
            await repo.update_room_stage(room.id, RoomStage.RESEARCHING)
            researcher_message, researcher_usage = await self.agents.researcher_message(
                signal=signal,
                dossier=dossier,
                delta=delta,
                room=room,
                recent_memories=recent_memories,
                role_config=self.agent_pack_service.role_config(pack, AgentRole.RESEARCHER),
            )
            researcher_record = await repo.append_message(room.id, researcher_message)
            role_models[AgentRole.RESEARCHER.value] = researcher_usage
            dossier_artifact = await repo.save_artifact(
                room_id=room.id,
                message_id=researcher_record.id,
                artifact_type="research_dossier_snapshot",
                source="historical_replay",
                title=f"Research dossier snapshot for {room.market_ticker}",
                payload=dossier.model_dump(mode="json"),
            )
            await repo.save_artifact(
                room_id=room.id,
                message_id=researcher_record.id,
                artifact_type="research_delta",
                source="historical_replay",
                title=f"Research delta for {room.market_ticker}",
                payload=delta.model_dump(mode="json"),
            )
            await repo.save_artifact(
                room_id=room.id,
                message_id=researcher_record.id,
                artifact_type="market_snapshot",
                source="historical_replay",
                title=f"Market snapshot for {room.market_ticker}",
                payload=market_response,
            )
            await repo.save_artifact(
                room_id=room.id,
                message_id=researcher_record.id,
                artifact_type="weather_bundle",
                source="historical_replay",
                title=f"Weather bundle for {room.market_ticker}",
                payload=weather_bundle,
            )
            for source in dossier.sources:
                await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="research_source",
                    source=source.source_class,
                    title=source.title,
                    payload=source.model_dump(mode="json"),
                    url=source.url,
                    external_id=source.source_key,
                )
            research_health = self.research_coordinator.training_quality_snapshot(dossier, reference_time=checkpoint_ts)
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
                dossier_artifact_id=dossier_artifact.id,
                payload=research_health["payload"],
            )
            await repo.save_artifact(
                room_id=room.id,
                artifact_type="historical_settlement_label",
                source="historical_replay",
                title=f"Historical settlement label for {room.market_ticker}",
                payload={
                    "market_ticker": settlement_label.market_ticker,
                    "local_market_day": settlement_label.local_market_day,
                    "kalshi_result": settlement_label.kalshi_result,
                    "settlement_value_dollars": str(settlement_label.settlement_value_dollars)
                    if settlement_label.settlement_value_dollars is not None
                    else None,
                    "crosscheck_status": settlement_label.crosscheck_status,
                    "crosscheck_result": settlement_label.crosscheck_result,
                    "crosscheck_high_f": (
                        str(settlement_label.crosscheck_high_f) if settlement_label.crosscheck_high_f is not None else None
                    ),
                },
            )
            await repo.save_room_campaign(
                room_id=room.id,
                campaign_id=f"historical-{settlement_label.local_market_day}",
                trigger_source="historical_replay",
                city_bucket=mapping.location_name,
                market_regime_bucket="historical_replay",
                difficulty_bucket="historical",
                outcome_bucket="historical",
                dossier_artifact_id=dossier_artifact.id,
                payload={
                    "market_ticker": room.market_ticker,
                    "checkpoint_label": checkpoint_label,
                    "checkpoint_ts": checkpoint_ts.isoformat(),
                    "local_market_day": settlement_label.local_market_day,
                },
            )

            final_status = "no_trade"
            rationale_ids = [researcher_record.id]

            if not dossier.gate.passed:
                ops_record = await repo.append_message(
                    room.id,
                    await self.agents.ops_message(
                        summary=f"Research gate blocked the historical replay room: {' '.join(dossier.gate.reasons)}",
                        payload=dossier.gate.model_dump(mode="json"),
                    ),
                )
                rationale_ids.append(ops_record.id)
                final_status = "research_blocked"
            else:
                await repo.update_room_stage(room.id, RoomStage.POSTURE)
                president_message, president_usage = await self.agents.president_message(
                    signal=signal,
                    role_config=self.agent_pack_service.role_config(pack, AgentRole.PRESIDENT),
                )
                president_record = await repo.append_message(room.id, president_message)
                role_models[AgentRole.PRESIDENT.value] = president_usage
                rationale_ids.append(president_record.id)

                await repo.update_room_stage(room.id, RoomStage.PROPOSING)
                trader_message, ticket, client_order_id, trader_usage = await self.agents.trader_message(
                    signal=signal,
                    room_id=room.id,
                    market_ticker=room.market_ticker,
                    rationale_ids=rationale_ids.copy(),
                    role_config=self.agent_pack_service.role_config(pack, AgentRole.TRADER),
                    max_order_notional_dollars=thresholds.risk_max_order_notional_dollars,
                )
                trader_record = await repo.append_message(room.id, trader_message)
                role_models[AgentRole.TRADER.value] = trader_usage
                rationale_ids.append(trader_record.id)

                if ticket is not None and client_order_id is not None:
                    ticket_record = await repo.save_trade_ticket(room.id, ticket, client_order_id, message_id=trader_record.id)
                    historical_control = DeploymentControl(
                        id="historical",
                        active_color=self.settings.app_color,
                        kill_switch_enabled=False,
                        shadow_color=self.settings.app_color,
                    )
                    verdict = self.risk_engine.evaluate(
                        room=room,
                        control=historical_control,
                        ticket=ticket,
                        signal=signal,
                        context=RiskContext(
                            market_observed_at=market_snapshot.asof_ts,
                            research_observed_at=dossier.freshness.refreshed_at,
                            decision_time=checkpoint_ts,
                            current_position_notional_dollars=Decimal("0"),
                        ),
                        thresholds=thresholds,
                    )
                    await repo.save_risk_verdict(
                        room_id=room.id,
                        ticket_id=ticket_record.id,
                        status=verdict.status,
                        reasons=verdict.reasons,
                        approved_notional_dollars=verdict.approved_notional_dollars,
                        approved_count_fp=verdict.approved_count_fp,
                        payload=verdict.model_dump(mode="json"),
                    )
                    risk_message, risk_usage = await self.agents.risk_message(
                        verdict=verdict,
                        role_config=self.agent_pack_service.role_config(pack, AgentRole.RISK_OFFICER),
                    )
                    risk_record = await repo.append_message(room.id, risk_message)
                    role_models[AgentRole.RISK_OFFICER.value] = risk_usage
                    rationale_ids.append(risk_record.id)

                    counterfactual = self.training_export_service._counterfactual_pnl(
                        trade_ticket=ticket.model_dump(mode="json"),
                        settlement={
                            "settlement_value_dollars": (
                                str(settlement_label.settlement_value_dollars)
                                if settlement_label.settlement_value_dollars is not None
                                else None
                            )
                        },
                    )
                    execution_record = await repo.append_message(
                        room.id,
                        await self.agents.execution_message(
                            "historical_skipped",
                            {
                                "status": "historical_skipped",
                                "dry_run": True,
                                "counterfactual_pnl_dollars": str(counterfactual) if counterfactual is not None else None,
                            },
                        ),
                    )
                    rationale_ids.append(execution_record.id)
                    final_status = "historical_skipped" if verdict.status == RiskStatus.APPROVED else "blocked"
                else:
                    ops_record = await repo.append_message(
                        room.id,
                        await self.agents.ops_message(
                            summary="Historical replay stood down before risk or execution because the setup was not actionable.",
                            payload={
                                "market_ticker": room.market_ticker,
                                "status": "stand_down",
                                "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None,
                            },
                        ),
                    )
                    rationale_ids.append(ops_record.id)
                    final_status = "stand_down"

            await repo.update_room_stage(room.id, RoomStage.AUDITING)
            auditor_record = await repo.append_message(
                room.id,
                await self.agents.auditor_message(final_status=final_status, rationale_ids=rationale_ids),
            )
            rationale_ids.append(auditor_record.id)
            all_messages = [message async for message in self._iter_room_messages(repo, room.id)]
            memory_payload, memory_usage = await self.memory_service.build_note(
                room,
                all_messages,
                memory_config=pack.memory,
                role_config=self.agent_pack_service.role_config(pack, AgentRole.MEMORY_LIBRARIAN),
            )
            await repo.update_room_stage(room.id, RoomStage.MEMORY)
            await repo.append_message(room.id, await self.agents.memory_message(memory_payload))
            role_models[AgentRole.MEMORY_LIBRARIAN.value] = memory_usage
            await repo.save_memory_note(
                room_id=room.id,
                payload=memory_payload,
                embedding=self.agents.providers.embed_text(memory_payload.summary),
                provider="hash-router-v1",
            )
            await repo.update_room_runtime(room.id, role_models=role_models)
            await repo.update_room_stage(room.id, RoomStage.COMPLETE)
            await repo.create_historical_replay_run(
                room_id=room.id,
                market_ticker=room.market_ticker,
                series_ticker=mapping.series_ticker,
                local_market_day=settlement_label.local_market_day,
                checkpoint_label=checkpoint_label,
                checkpoint_ts=checkpoint_ts,
                status="completed",
                agent_pack_version=pack.version,
                payload={
                    "historical_provenance": {
                        "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                        "local_market_day": settlement_label.local_market_day,
                        "checkpoint_label": checkpoint_label,
                        "checkpoint_ts": checkpoint_ts.isoformat(),
                        "timezone_name": self._timezone_name(mapping),
                        "market_snapshot_source_id": market_snapshot.id,
                        "weather_snapshot_source_id": weather_snapshot.id,
                        "market_source_kind": market_source_kind,
                        "weather_source_kind": weather_source_kind,
                        "settlement_label_id": settlement_label.id,
                        "settlement_crosscheck_status": settlement_label.crosscheck_status,
                        "settlement_mismatch_reason": self._crosscheck_mismatch_reason_from_label(settlement_label),
                        "settlement_label_signature": self._settlement_label_signature(settlement_label),
                        "coverage_class": coverage_class,
                        "replay_logic_version": self.replay_logic_version(),
                        "source_coverage": {
                            "market_snapshot": True,
                            "weather_snapshot": True,
                            "settlement_label": True,
                        },
                    }
                },
            )
            await session.commit()

        try:
            await self.training_corpus_service.persist_strategy_audit_for_room(
                room.id,
                audit_source="historical_replay",
            )
        except Exception:
            pass
        return room.id

    async def _iter_room_messages(self, repo: PlatformRepository, room_id: str):
        for message in await repo.list_messages(room_id):
            yield self.training_export_service._message_read(message)

    async def _list_recent_markets(
        self,
        template: WeatherSeriesTemplate,
        *,
        date_from: date,
        date_to: date,
    ) -> list[dict[str, Any]]:
        min_close_ts = int(datetime.combine(date_from, time.min, tzinfo=UTC).timestamp())
        max_close_ts = int(datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=UTC).timestamp())
        markets: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            response = await self.kalshi.list_markets(
                series_ticker=template.series_ticker,
                min_close_ts=min_close_ts,
                max_close_ts=max_close_ts,
                limit=self.settings.historical_import_page_size,
                cursor=cursor,
            )
            page = response.get("markets", [])
            for market in page:
                status = str(market.get("status") or "").lower()
                result = str(market.get("result") or "").lower()
                if status in {"settled", "determined", "closed"} or result in {"yes", "no"}:
                    markets.append(market)
            cursor = response.get("cursor")
            if not cursor or not page:
                break
        return markets

    async def _list_historical_markets(
        self,
        template: WeatherSeriesTemplate,
        *,
        date_from: date,
        date_to: date,
    ) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        cursor: str | None = None
        pages = 0
        prefix = f"{template.series_ticker}-"
        while pages < self.settings.historical_import_max_pages:
            response = await self.kalshi.list_historical_markets(
                limit=self.settings.historical_import_page_size,
                cursor=cursor,
            )
            page = response.get("markets", [])
            if not page:
                break
            for market in page:
                ticker = str(market.get("ticker") or "")
                if not ticker.startswith(prefix):
                    continue
                local_day = self._market_local_day(template.resolve_market(market) or WeatherMarketMapping(
                    market_ticker=ticker,
                    market_type="weather",
                    station_id=template.station_id,
                    daily_summary_station_id=template.daily_summary_station_id,
                    location_name=template.location_name,
                    timezone_name=template.timezone_name,
                    latitude=template.latitude,
                    longitude=template.longitude,
                    threshold_f=float(market.get("floor_strike") or market.get("cap_strike") or 0),
                    operator=">" if str(market.get("strike_type") or "") == "greater" else "<",
                    metric=template.metric,
                    settlement_source=template.settlement_source,
                    series_ticker=template.series_ticker,
                ), market)
                if date_from.isoformat() <= local_day <= date_to.isoformat():
                    markets.append(market)
            cursor = response.get("cursor")
            pages += 1
            if not cursor:
                break
        return markets

    async def _daily_summary_crosscheck(
        self,
        mapping: WeatherMarketMapping,
        local_day: str,
        *,
        kalshi_result: str | None,
    ) -> dict[str, Any]:
        station = mapping.daily_summary_station_id or mapping.station_id
        if not station or mapping.threshold_f is None:
            return {
                "status": self.SETTLEMENT_MISSING,
                "daily_high_f": None,
                "result": None,
                "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
            }
        params = {
            "dataset": "daily-summaries",
            "stations": station,
            "startDate": local_day,
            "endDate": local_day,
            "dataTypes": "TMAX",
            "units": "standard",
            "format": "json",
        }
        response = await self.client.get(self.NCEI_DAILY_SUMMARY_URL, params=params)
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list) or not rows:
            return {
                "status": self.SETTLEMENT_MISSING,
                "daily_high_f": None,
                "result": None,
                "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
            }
        value = rows[0].get("TMAX")
        if value in (None, ""):
            return {
                "status": self.SETTLEMENT_MISSING,
                "daily_high_f": None,
                "result": None,
                "mismatch_reason": self.SETTLEMENT_MISMATCH_REASON_MISSING,
            }
        high_f = Decimal(str(value)).quantize(Decimal("0.01"))
        result = self._settlement_result_from_high(mapping, high_f)
        kalshi_result_normalized = str(kalshi_result or "").strip().lower() or None
        mismatch_reason: str | None = None
        if kalshi_result_normalized not in (None, result):
            threshold = Decimal(str(mapping.threshold_f)).quantize(Decimal("0.01"))
            if high_f == threshold and mapping.operator in (">", "<"):
                mismatch_reason = self.SETTLEMENT_MISMATCH_REASON_THRESHOLD_EDGE
            else:
                mismatch_reason = self.SETTLEMENT_MISMATCH_REASON_DISAGREEMENT
        status = self.SETTLEMENT_MATCH if mismatch_reason is None else self.SETTLEMENT_MISMATCH
        return {
            "status": status,
            "daily_high_f": high_f,
            "result": result,
            "mismatch_reason": mismatch_reason,
        }

    async def _coverage_status(self, settlement_labels: list[Any], *, verbose: bool) -> dict[str, Any]:
        missing_reason_counts: Counter[str] = Counter()
        coverage_rows: list[dict[str, Any]] = []
        source_gap_samples: list[dict[str, Any]] = []

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for label in settlement_labels:
                mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
                if mapping is None or not mapping.supports_structured_weather:
                    continue
                selections = await self._resolve_market_day_selections(repo, label=label, mapping=mapping)
                coverage_class = self._coverage_class(selections)
                replayable = any(selection.replayable for selection in selections)
                checkpoint_rows: list[dict[str, Any]] = []
                for selection in selections:
                    missing_reason_counts.update(selection.missing_reasons)
                    if selection.missing_reasons and len(source_gap_samples) < 20:
                        for reason in selection.missing_reasons:
                            source_gap_samples.append(
                                {
                                    "market_ticker": label.market_ticker,
                                    "local_market_day": label.local_market_day,
                                    "checkpoint_label": selection.checkpoint_label,
                                    "gap": reason,
                                }
                            )
                    checkpoint_rows.append(
                        {
                            "checkpoint_label": selection.checkpoint_label,
                            "checkpoint_ts": selection.checkpoint_ts.isoformat(),
                            "replayable": selection.replayable,
                            "market_source_kind": selection.market_source_kind,
                            "weather_source_kind": selection.weather_source_kind,
                            "market_snapshot_id": (selection.market_snapshot.id if selection.market_snapshot is not None else None),
                            "weather_snapshot_id": (selection.weather_snapshot.id if selection.weather_snapshot is not None else None),
                            "missing_reasons": selection.missing_reasons,
                        }
                    )
                coverage_rows.append(
                    {
                        "market_ticker": label.market_ticker,
                        "series_ticker": label.series_ticker,
                        "local_market_day": label.local_market_day,
                        "coverage_class": coverage_class,
                        "replayable": replayable,
                        "settlement_crosscheck_status": label.crosscheck_status,
                        "settlement_mismatch_reason": self._crosscheck_mismatch_reason_from_label(label),
                        "settlement_label_signature": self._settlement_label_signature(label),
                        "checkpoints": checkpoint_rows,
                    }
                )
            await session.commit()

        full_count = sum(1 for row in coverage_rows if row["coverage_class"] == self.COVERAGE_FULL)
        late_only_count = sum(1 for row in coverage_rows if row["coverage_class"] == self.COVERAGE_LATE_ONLY)
        partial_count = sum(1 for row in coverage_rows if row["coverage_class"] == self.COVERAGE_PARTIAL)
        outcome_only_count = sum(1 for row in coverage_rows if row["coverage_class"] == self.COVERAGE_OUTCOME_ONLY)
        replayable_count = sum(1 for row in coverage_rows if row["replayable"])
        source_coverage_gaps = {
            "missing_market_snapshot_count": missing_reason_counts.get("market_snapshot_missing", 0),
            "missing_weather_snapshot_count": missing_reason_counts.get("weather_snapshot_missing", 0),
            "samples": source_gap_samples[:10],
        }
        return {
            "replayable_market_day_count": replayable_count,
            "full_checkpoint_coverage_count": full_count,
            "late_only_coverage_count": late_only_count,
            "partial_checkpoint_coverage_count": partial_count,
            "outcome_only_coverage_count": outcome_only_count,
            "no_replayable_coverage_count": sum(1 for row in coverage_rows if row["coverage_class"] == self.COVERAGE_NONE),
            "missing_checkpoint_reason_counts": dict(missing_reason_counts),
            "source_coverage_gaps": source_coverage_gaps,
            "market_day_coverage": coverage_rows if verbose else coverage_rows[:12],
            "all_market_day_coverage": coverage_rows,
        }

    async def _checkpoint_capture_status(self, settlement_labels: list[Any], *, verbose: bool) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            archives = await repo.list_historical_checkpoint_archives(limit=5000)
            await session.commit()

        archive_index = {
            (record.series_ticker, record.local_market_day, record.checkpoint_label): record
            for record in archives
        }
        gap_counts: Counter[str] = Counter()
        gap_samples: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        coverage_counts = Counter()
        native_checkpoint_archive_count = 0
        external_archive_assisted_checkpoint_count = 0

        for label in settlement_labels:
            mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
            if mapping is None or not mapping.supports_structured_weather or not mapping.series_ticker:
                continue
            checkpoints = self._checkpoint_times(
                mapping,
                local_market_day=label.local_market_day,
                market_payload=(label.payload or {}).get("market", {}),
            )
            selections: list[HistoricalCheckpointSelection] = []
            checkpoint_rows: list[dict[str, Any]] = []
            for checkpoint_label, checkpoint_ts in checkpoints:
                archive = archive_index.get((mapping.series_ticker, label.local_market_day, checkpoint_label))
                captured = archive is not None
                archive_payload = dict(archive.payload or {}) if archive is not None else {}
                weather_source_kind = str(archive_payload.get("weather_source_kind") or "")
                external_assisted = weather_source_kind == self.EXTERNAL_FORECAST_ARCHIVE_SOURCE
                if not captured:
                    gap_counts["checkpoint_archive_missing"] += 1
                    if len(gap_samples) < 10:
                        gap_samples.append(
                            {
                                "series_ticker": mapping.series_ticker,
                                "market_ticker": label.market_ticker,
                                "local_market_day": label.local_market_day,
                                "checkpoint_label": checkpoint_label,
                                "gap": "checkpoint_archive_missing",
                            }
                        )
                selections.append(
                    HistoricalCheckpointSelection(
                        checkpoint_label=checkpoint_label,
                        checkpoint_ts=checkpoint_ts,
                        market_snapshot=(archive if captured else None),
                        weather_snapshot=(archive if captured else None),
                        market_source_kind=None,
                        weather_source_kind=(weather_source_kind or self.CHECKPOINT_CAPTURED_WEATHER_SOURCE if captured else None),
                        missing_reasons=[] if captured else ["checkpoint_archive_missing"],
                    )
                )
                if captured:
                    if external_assisted:
                        external_archive_assisted_checkpoint_count += 1
                    else:
                        native_checkpoint_archive_count += 1
                checkpoint_rows.append(
                    {
                        "checkpoint_label": checkpoint_label,
                        "checkpoint_ts": checkpoint_ts.isoformat(),
                        "captured": captured,
                        "source_kind": (archive.source_kind if archive is not None else None),
                        "weather_source_kind": (weather_source_kind or None),
                        "external_archive_assisted": external_assisted,
                        "captured_at": (archive.captured_at.isoformat() if archive is not None else None),
                        "archive_path": (archive.archive_path if archive is not None else None),
                    }
                )
            coverage_class = self._coverage_class(selections, use_outcome_only=False)
            coverage_counts[coverage_class] += 1
            rows.append(
                {
                    "series_ticker": mapping.series_ticker,
                    "market_ticker": label.market_ticker,
                    "local_market_day": label.local_market_day,
                    "coverage_class": coverage_class,
                    "checkpoints": checkpoint_rows,
                }
            )
        return {
            "checkpoint_coverage_counts": {
                self.COVERAGE_FULL: coverage_counts.get(self.COVERAGE_FULL, 0),
                self.COVERAGE_LATE_ONLY: coverage_counts.get(self.COVERAGE_LATE_ONLY, 0),
                self.COVERAGE_PARTIAL: coverage_counts.get(self.COVERAGE_PARTIAL, 0),
                self.COVERAGE_OUTCOME_ONLY: coverage_counts.get(self.COVERAGE_OUTCOME_ONLY, 0),
                self.COVERAGE_NONE: coverage_counts.get(self.COVERAGE_NONE, 0),
            },
            "checkpoint_capture_gaps": {
                "reason_counts": dict(gap_counts),
                "samples": gap_samples,
            },
            "source_counts": {
                "native_checkpoint_archive_count": native_checkpoint_archive_count,
                "external_archive_assisted_checkpoint_count": external_archive_assisted_checkpoint_count,
            },
            "market_day_coverage": rows if verbose else rows[:12],
            "all_market_day_coverage": rows,
        }

    async def _market_checkpoint_capture_status(self, settlement_labels: list[Any], *, verbose: bool) -> dict[str, Any]:
        gap_counts: Counter[str] = Counter()
        gap_samples: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        coverage_counts = Counter()

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for label in settlement_labels:
                mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
                if mapping is None or not mapping.supports_structured_weather:
                    continue
                checkpoints = self._checkpoint_times(
                    mapping,
                    local_market_day=label.local_market_day,
                    market_payload=(label.payload or {}).get("market", {}),
                )
                selections: list[HistoricalCheckpointSelection] = []
                checkpoint_rows: list[dict[str, Any]] = []
                for checkpoint_label, checkpoint_ts in checkpoints:
                    captured = await repo.get_latest_historical_market_snapshot(
                        market_ticker=label.market_ticker,
                        before_asof=checkpoint_ts,
                        source_kind=self.CHECKPOINT_CAPTURED_MARKET_SOURCE,
                        local_market_day=label.local_market_day,
                    )
                    if captured is None:
                        gap_counts["market_checkpoint_missing"] += 1
                        if len(gap_samples) < 10:
                            gap_samples.append(
                                {
                                    "series_ticker": mapping.series_ticker,
                                    "market_ticker": label.market_ticker,
                                    "local_market_day": label.local_market_day,
                                    "checkpoint_label": checkpoint_label,
                                    "gap": "market_checkpoint_missing",
                                }
                            )
                    selections.append(
                        HistoricalCheckpointSelection(
                            checkpoint_label=checkpoint_label,
                            checkpoint_ts=checkpoint_ts,
                            market_snapshot=captured,
                            weather_snapshot=captured,
                            market_source_kind=(captured.source_kind if captured is not None else None),
                            weather_source_kind=(captured.source_kind if captured is not None else None),
                            missing_reasons=[] if captured else ["market_checkpoint_missing"],
                        )
                    )
                    checkpoint_rows.append(
                        {
                            "checkpoint_label": checkpoint_label,
                            "checkpoint_ts": checkpoint_ts.isoformat(),
                            "captured": captured is not None,
                            "source_kind": (captured.source_kind if captured is not None else None),
                            "captured_at": (captured.created_at.isoformat() if captured is not None else None),
                            "market_snapshot_id": (captured.id if captured is not None else None),
                            "market_asof_ts": (captured.asof_ts.isoformat() if captured is not None else None),
                        }
                    )
                coverage_class = self._coverage_class(selections, use_outcome_only=False)
                coverage_counts[coverage_class] += 1
                rows.append(
                    {
                        "series_ticker": mapping.series_ticker,
                        "market_ticker": label.market_ticker,
                        "local_market_day": label.local_market_day,
                        "coverage_class": coverage_class,
                        "checkpoints": checkpoint_rows,
                    }
                )
            await session.commit()
        return {
            "checkpoint_coverage_counts": {
                self.COVERAGE_FULL: coverage_counts.get(self.COVERAGE_FULL, 0),
                self.COVERAGE_LATE_ONLY: coverage_counts.get(self.COVERAGE_LATE_ONLY, 0),
                self.COVERAGE_PARTIAL: coverage_counts.get(self.COVERAGE_PARTIAL, 0),
                self.COVERAGE_OUTCOME_ONLY: coverage_counts.get(self.COVERAGE_OUTCOME_ONLY, 0),
                self.COVERAGE_NONE: coverage_counts.get(self.COVERAGE_NONE, 0),
            },
            "checkpoint_capture_gaps": {
                "reason_counts": dict(gap_counts),
                "samples": gap_samples,
            },
            "market_day_coverage": rows if verbose else rows[:12],
            "all_market_day_coverage": rows,
        }

    async def _external_forecast_archive_status(self, settlement_labels: list[Any], *, verbose: bool) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        provider_counts: Counter[str] = Counter()
        model_counts: Counter[str] = Counter()
        snapshot_count = 0
        assisted_checkpoint_count = 0
        recovered_market_days = 0
        recoverable_market_days = 0
        unrecoverable_market_days = 0

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            external_snapshots = [
                snapshot
                for snapshot in await repo.list_historical_weather_snapshots(limit=5000)
                if snapshot.source_kind == self.EXTERNAL_FORECAST_ARCHIVE_SOURCE
            ]
            archives = await repo.list_historical_checkpoint_archives(limit=5000)
            await session.commit()

        snapshot_index: dict[tuple[str, str], list[Any]] = defaultdict(list)
        for snapshot in external_snapshots:
            snapshot_count += 1
            metadata = dict((snapshot.payload or {}).get("_external_archive") or {})
            provider_counts[str(metadata.get("provider") or "unknown")] += 1
            model_counts[str(metadata.get("model") or "unknown")] += 1
            snapshot_index[(snapshot.station_id, snapshot.local_market_day)].append(snapshot)

        for key in snapshot_index:
            snapshot_index[key].sort(key=lambda item: (item.asof_ts, item.source_id), reverse=True)

        archive_index = {
            (archive.series_ticker, archive.local_market_day, archive.checkpoint_label): archive
            for archive in archives
        }

        for label in settlement_labels:
            mapping = self._mapping_for_market(label.market_ticker, (label.payload or {}).get("market"))
            if mapping is None or not mapping.supports_structured_weather or not mapping.series_ticker:
                continue
            checkpoints = self._checkpoint_times(
                mapping,
                local_market_day=label.local_market_day,
                market_payload=(label.payload or {}).get("market", {}),
            )
            checkpoint_rows: list[dict[str, Any]] = []
            missing_checkpoints = 0
            recoverable_missing_checkpoints = 0
            external_assisted_present = False
            for checkpoint_label, checkpoint_ts in checkpoints:
                archive = archive_index.get((mapping.series_ticker, label.local_market_day, checkpoint_label))
                archive_payload = dict(archive.payload or {}) if archive is not None else {}
                external_assisted = (
                    archive is not None
                    and str(archive_payload.get("weather_source_kind") or "") == self.EXTERNAL_FORECAST_ARCHIVE_SOURCE
                )
                if external_assisted:
                    assisted_checkpoint_count += 1
                    external_assisted_present = True
                snapshot = next(
                    (
                        item
                        for item in snapshot_index.get((mapping.station_id, label.local_market_day), [])
                        if _as_utc(item.asof_ts) is not None
                        and _as_utc(item.asof_ts) <= checkpoint_ts
                        and self._checkpoint_archive_metadata_valid(
                            self._weather_snapshot_checkpoint_metadata(item),
                            checkpoint_ts,
                        )
                    ),
                    None,
                )
                external_available = snapshot is not None
                if archive is None:
                    missing_checkpoints += 1
                    if external_available:
                        recoverable_missing_checkpoints += 1
                checkpoint_rows.append(
                    {
                        "checkpoint_label": checkpoint_label,
                        "checkpoint_ts": checkpoint_ts.isoformat(),
                        "external_snapshot_available": external_available,
                        "external_archive_assisted": external_assisted,
                        "weather_source_id": (snapshot.source_id if snapshot is not None else None),
                        "provider": (
                            str((((snapshot.payload or {}).get("_external_archive") or {}).get("provider")) or "")
                            if snapshot is not None
                            else None
                        ),
                        "model": (
                            str((((snapshot.payload or {}).get("_external_archive") or {}).get("model")) or "")
                            if snapshot is not None
                            else None
                        ),
                        "run_ts": (
                            str((((snapshot.payload or {}).get("_external_archive") or {}).get("run_ts")) or "")
                            if snapshot is not None
                            else None
                        ),
                    }
                )
            recovery_status = "native_only"
            if missing_checkpoints == 0 and external_assisted_present:
                recovered_market_days += 1
                recovery_status = "recovered_via_external_archive"
            elif missing_checkpoints > 0 and recoverable_missing_checkpoints == missing_checkpoints:
                recoverable_market_days += 1
                recovery_status = "missing_native_archive_but_recoverable_via_external"
            elif missing_checkpoints > 0:
                unrecoverable_market_days += 1
                recovery_status = "still_unrecoverable_even_with_external"
            rows.append(
                {
                    "series_ticker": mapping.series_ticker,
                    "market_ticker": label.market_ticker,
                    "local_market_day": label.local_market_day,
                    "recovery_status": recovery_status,
                    "checkpoints": checkpoint_rows,
                }
            )

        return {
            "source_counts": {
                "snapshot_count": snapshot_count,
                "provider_counts": dict(provider_counts),
                "model_counts": dict(model_counts),
                "assisted_checkpoint_count": assisted_checkpoint_count,
            },
            "recovery_summary": {
                "recovered_via_external_archive_market_day_count": recovered_market_days,
                "missing_native_archive_but_recoverable_via_external_market_day_count": recoverable_market_days,
                "still_unrecoverable_even_with_external_market_day_count": unrecoverable_market_days,
            },
            "market_day_coverage": rows if verbose else rows[:12],
            "all_market_day_coverage": rows,
        }

    async def _resolve_market_day_selections(self, repo: PlatformRepository, *, label: Any, mapping: WeatherMarketMapping) -> list[HistoricalCheckpointSelection]:
        selections: list[HistoricalCheckpointSelection] = []
        for checkpoint_label, checkpoint_ts in self._checkpoint_times(
            mapping,
            local_market_day=label.local_market_day,
            market_payload=(label.payload or {}).get("market", {}),
        ):
            market_snapshot, market_snapshot_reason = await self._select_market_snapshot(
                repo,
                market_ticker=label.market_ticker,
                local_market_day=label.local_market_day,
                checkpoint_ts=checkpoint_ts,
            )
            weather_snapshot = await self._select_weather_snapshot(
                repo,
                station_id=mapping.station_id,
                series_ticker=mapping.series_ticker,
                local_market_day=label.local_market_day,
                checkpoint_label=checkpoint_label,
                checkpoint_ts=checkpoint_ts,
            )
            missing_reasons: list[str] = []
            if market_snapshot is None:
                missing_reasons.append(market_snapshot_reason or "market_snapshot_missing")
            if weather_snapshot is None:
                missing_reasons.append("weather_snapshot_missing")
            selections.append(
                HistoricalCheckpointSelection(
                    checkpoint_label=checkpoint_label,
                    checkpoint_ts=checkpoint_ts,
                    market_snapshot=market_snapshot,
                    weather_snapshot=weather_snapshot,
                    market_source_kind=(market_snapshot.source_kind if market_snapshot is not None else None),
                    weather_source_kind=(weather_snapshot.source_kind if weather_snapshot is not None else None),
                    missing_reasons=missing_reasons,
                )
            )
        return selections

    def _historical_market_snapshot_valid(self, snapshot: Any, *, checkpoint_ts: datetime) -> bool:
        return not is_market_stale(
            observed_at=getattr(snapshot, "asof_ts", None),
            stale_after_seconds=self.settings.historical_replay_market_stale_seconds,
            reference_time=checkpoint_ts,
        )

    async def _select_market_snapshot(
        self,
        repo: PlatformRepository,
        *,
        market_ticker: str,
        local_market_day: str,
        checkpoint_ts: datetime,
    ) -> tuple[Any | None, str | None]:
        saw_stale = False
        for source_kind in (
            self.CHECKPOINT_CAPTURED_MARKET_SOURCE,
            self.CAPTURED_MARKET_SOURCE,
            self.RECONSTRUCTED_MARKET_SOURCE,
        ):
            snapshot = await repo.get_latest_historical_market_snapshot(
                market_ticker=market_ticker,
                before_asof=checkpoint_ts,
                source_kind=source_kind,
                local_market_day=local_market_day,
            )
            if snapshot is None:
                continue
            if self._historical_market_snapshot_valid(snapshot, checkpoint_ts=checkpoint_ts):
                return snapshot, None
            saw_stale = True
        if saw_stale:
            return None, "market_snapshot_stale"
        return None, None

    async def _select_weather_snapshot(
        self,
        repo: PlatformRepository,
        *,
        station_id: str,
        series_ticker: str | None,
        local_market_day: str,
        checkpoint_label: str,
        checkpoint_ts: datetime,
    ):
        if series_ticker:
            archive = await repo.get_historical_checkpoint_archive(
                series_ticker=series_ticker,
                local_market_day=local_market_day,
                checkpoint_label=checkpoint_label,
            )
            if archive is not None:
                archive_payload = dict(archive.payload or {})
                weather_source_kind = str(
                    archive_payload.get("weather_source_kind")
                    or self.CHECKPOINT_CAPTURED_WEATHER_SOURCE
                )
                weather_source_id = str(
                    archive_payload.get("weather_source_id")
                    or archive.source_id
                    or ""
                )
                if weather_source_id:
                    archive_snapshot = await repo.get_historical_weather_snapshot_by_source(
                        station_id=station_id,
                        source_kind=weather_source_kind,
                        source_id=weather_source_id,
                    )
                    if archive_snapshot is not None:
                        return archive_snapshot
        snapshots = await repo.list_historical_weather_snapshots(
            station_id=station_id,
            local_market_day=local_market_day,
            before_asof=checkpoint_ts,
            limit=100,
        )
        for source_kind in (
            self.CHECKPOINT_CAPTURED_WEATHER_SOURCE,
            self.ARCHIVED_WEATHER_SOURCE,
            self.LEGACY_ARCHIVED_WEATHER_SOURCE,
            self.CAPTURED_WEATHER_SOURCE,
            self.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
        ):
            match = next((record for record in snapshots if record.source_kind == source_kind), None)
            if match is not None:
                return match
        return None

    @staticmethod
    def _weather_snapshot_checkpoint_metadata(snapshot: Any) -> dict[str, Any]:
        return {
            "observation_ts": getattr(snapshot, "observation_ts", None),
            "forecast_updated_ts": getattr(snapshot, "forecast_updated_ts", None),
            "asof_ts": getattr(snapshot, "asof_ts", None),
        }

    def _coverage_class(self, selections: list[HistoricalCheckpointSelection], *, use_outcome_only: bool = True) -> str:
        if not selections:
            return self.COVERAGE_NONE
        replayable_flags = [selection.replayable for selection in selections]
        if all(replayable_flags):
            return self.COVERAGE_FULL
        if replayable_flags[-1] and replayable_flags.count(True) == 1:
            return self.COVERAGE_LATE_ONLY
        if any(replayable_flags):
            return self.COVERAGE_PARTIAL
        if use_outcome_only:
            return self.COVERAGE_OUTCOME_ONLY
        return self.COVERAGE_NONE

    async def _hydrate_historical_bundle_coverage(self, bundles: list[Any]) -> list[Any]:
        if not bundles:
            return bundles
        hydrated: list[Any] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            coverage_cache: dict[tuple[str, str], tuple[str, list[HistoricalCheckpointSelection]]] = {}
            for bundle in bundles:
                if bundle.room_origin != RoomOrigin.HISTORICAL_REPLAY.value:
                    hydrated.append(bundle)
                    continue
                if bundle.coverage_class and bundle.market_source_kind and bundle.weather_source_kind:
                    hydrated.append(bundle)
                    continue
                local_market_day = str((bundle.historical_provenance or {}).get("local_market_day") or "")
                market_ticker = str(bundle.room.get("market_ticker") or "")
                if not local_market_day or not market_ticker:
                    hydrated.append(bundle)
                    continue
                cache_key = (market_ticker, local_market_day)
                cached = coverage_cache.get(cache_key)
                if cached is None:
                    label = await repo.get_historical_settlement_label(market_ticker)
                    mapping = self._mapping_for_market(market_ticker, (label.payload or {}).get("market") if label is not None else None)
                    if label is None or mapping is None or not mapping.supports_structured_weather:
                        hydrated.append(bundle)
                        continue
                    selections = await self._resolve_market_day_selections(repo, label=label, mapping=mapping)
                    cached = (self._coverage_class(selections), selections)
                    coverage_cache[cache_key] = cached
                coverage_class, selections = cached
                checkpoint_ts = _parse_iso(bundle.replay_checkpoint_ts) if isinstance(bundle.replay_checkpoint_ts, str) else bundle.replay_checkpoint_ts
                selection = next((item for item in selections if item.checkpoint_ts == checkpoint_ts), None)
                provenance = dict(bundle.historical_provenance or {})
                provenance.setdefault("coverage_class", coverage_class)
                if selection is not None:
                    provenance.setdefault("market_source_kind", selection.market_source_kind)
                    provenance.setdefault("weather_source_kind", selection.weather_source_kind)
                hydrated.append(
                    bundle.model_copy(
                        update={
                            "historical_provenance": provenance,
                            "coverage_class": coverage_class,
                            "market_source_kind": provenance.get("market_source_kind"),
                            "weather_source_kind": provenance.get("weather_source_kind"),
                        }
                    )
                )
            await session.commit()
        return hydrated

    async def _reconstruct_market_checkpoint(
        self,
        *,
        mapping: WeatherMarketMapping,
        settlement_label: Any,
        checkpoint_label: str,
        checkpoint_ts: datetime,
    ) -> dict[str, Any] | None:
        window_start = checkpoint_ts - timedelta(hours=self.settings.historical_replay_market_snapshot_lookback_hours)
        selected = None
        for period_interval in (1, 60):
            try:
                response = await self.kalshi.get_market_candlesticks(
                    mapping.series_ticker,
                    settlement_label.market_ticker,
                    period_interval=period_interval,
                    start_ts=int(window_start.timestamp()),
                    end_ts=int(checkpoint_ts.timestamp()),
                )
            except httpx.HTTPStatusError:
                continue
            candlesticks = response.get("candlesticks") or []
            for candlestick in candlesticks:
                end_period_ts = candlestick.get("end_period_ts")
                if end_period_ts in (None, ""):
                    continue
                try:
                    end_at = datetime.fromtimestamp(int(end_period_ts), tz=UTC)
                except (OverflowError, OSError, ValueError):
                    continue
                if end_at <= checkpoint_ts:
                    selected = (candlestick, end_at)
            if selected is not None and not is_market_stale(
                observed_at=selected[1],
                stale_after_seconds=self.settings.historical_replay_market_stale_seconds,
                reference_time=checkpoint_ts,
            ):
                break
        if selected is None:
            return None

        candlestick, asof_ts = selected
        market_payload = dict(((settlement_label.payload or {}).get("market") or {}))
        yes_bid = _parse_decimal(((candlestick.get("yes_bid") or {}).get("close_dollars")))
        yes_ask = _parse_decimal(((candlestick.get("yes_ask") or {}).get("close_dollars")))
        last_price = _parse_decimal(((candlestick.get("price") or {}).get("close_dollars")))
        no_ask = None
        if yes_bid is not None:
            no_ask = (Decimal("1.0000") - yes_bid).quantize(Decimal("0.0001"))
        elif market_payload.get("no_ask_dollars") not in (None, ""):
            no_ask = _parse_decimal(market_payload.get("no_ask_dollars"))
        market_payload.update(
            {
                "ticker": settlement_label.market_ticker,
                "updated_time": asof_ts.isoformat(),
                "yes_bid_dollars": str(yes_bid) if yes_bid is not None else market_payload.get("yes_bid_dollars"),
                "yes_ask_dollars": str(yes_ask) if yes_ask is not None else market_payload.get("yes_ask_dollars"),
                "no_ask_dollars": str(no_ask) if no_ask is not None else market_payload.get("no_ask_dollars"),
                "last_price_dollars": str(last_price) if last_price is not None else market_payload.get("last_price_dollars"),
            }
        )
        payload = {
            "market": market_payload,
            "candlestick": candlestick,
            "reconstructed_from": "candlesticks",
            "checkpoint_label": checkpoint_label,
            "checkpoint_ts": checkpoint_ts.isoformat(),
        }
        source_id = f"{settlement_label.market_ticker}:{checkpoint_label}:{int(asof_ts.timestamp())}"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            record = await repo.upsert_historical_market_snapshot(
                market_ticker=settlement_label.market_ticker,
                series_ticker=mapping.series_ticker,
                station_id=mapping.station_id,
                local_market_day=settlement_label.local_market_day,
                asof_ts=asof_ts,
                source_kind=self.RECONSTRUCTED_MARKET_SOURCE,
                source_id=source_id,
                source_hash=_hash_payload(payload),
                close_ts=self._market_timestamp(market_payload, "close_time", "close_ts"),
                settlement_ts=self._market_timestamp(market_payload, "settlement_ts", "settlement_time"),
                yes_bid_dollars=yes_bid,
                yes_ask_dollars=yes_ask,
                no_ask_dollars=no_ask,
                last_price_dollars=last_price,
                payload=payload,
            )
            await session.commit()
        return {
            "market_ticker": settlement_label.market_ticker,
            "local_market_day": settlement_label.local_market_day,
            "checkpoint_label": checkpoint_label,
            "checkpoint_ts": checkpoint_ts.isoformat(),
            "source_id": record.source_id,
            "market_source_kind": record.source_kind,
        }

    def _build_training_readiness(self, bundles: list[Any], *, split: HistoricalBuildSplit, mode: str) -> tuple[bool, bool]:
        if mode != "gemini-finetune":
            return True, False
        local_days = {
            str((bundle.historical_provenance or {}).get("local_market_day") or "")
            for bundle in bundles
            if (bundle.historical_provenance or {}).get("local_market_day")
        }
        training_ready = len(local_days) >= 3 and bool(split.validation) and bool(split.holdout)
        return training_ready, not training_ready

    def _confidence_story(
        self,
        *,
        latest_run_payload: dict[str, Any] | None,
        historical_build_readiness: dict[str, Any],
        source_replay_coverage: dict[str, Any],
    ) -> dict[str, Any]:
        if latest_run_payload:
            scorecard = dict(latest_run_payload.get("confidence_scorecard") or {})
            confidence_state = str(
                latest_run_payload.get("confidence_state")
                or scorecard.get("confidence_state")
                or "insufficient_support"
            )
            if scorecard:
                scorecard.setdefault("confidence_state", confidence_state)
                return {
                    "confidence_state": confidence_state,
                    "confidence_scorecard": scorecard,
                    "confidence_progress": self._confidence_progress(scorecard),
                }

        support_counts = {
            self.COVERAGE_FULL: int(source_replay_coverage.get("full_checkpoint_coverage_count", 0)),
            self.COVERAGE_LATE_ONLY: int(source_replay_coverage.get("late_only_coverage_count", 0)),
            self.COVERAGE_PARTIAL: int(source_replay_coverage.get("partial_checkpoint_coverage_count", 0)),
            self.COVERAGE_OUTCOME_ONLY: int(source_replay_coverage.get("outcome_only_coverage_count", 0)),
            self.COVERAGE_NONE: int(source_replay_coverage.get("no_replayable_coverage_count", 0)),
        }
        execution_market_days = (
            support_counts[self.COVERAGE_FULL]
            + support_counts[self.COVERAGE_LATE_ONLY]
            + support_counts[self.COVERAGE_PARTIAL]
        )
        full_market_days = int(historical_build_readiness.get("distinct_full_coverage_market_days", 0))
        holdout_market_days = int(historical_build_readiness.get("holdout_full_coverage_market_days", 0))
        execution_confident = execution_market_days >= self.settings.historical_execution_confidence_min_market_days
        directional_confident = (
            execution_confident
            and
            full_market_days >= self.settings.historical_directional_confidence_min_full_market_days
            and holdout_market_days >= self.settings.historical_directional_confidence_min_holdout_market_days
        )
        confidence_state = (
            "directional_confident"
            if directional_confident
            else "execution_confident_only"
            if execution_confident
            else "insufficient_support"
        )
        return {
            "confidence_state": confidence_state,
            "confidence_scorecard": {
                "confidence_state": confidence_state,
                "support_counts_by_coverage_class": support_counts,
                "distinct_full_market_days": full_market_days,
                "distinct_execution_market_days": execution_market_days,
                "full_coverage_holdout_market_days": holdout_market_days,
                "execution_confident": execution_confident,
                "directional_confident": directional_confident,
                "execution_confidence_threshold_market_days": self.settings.historical_execution_confidence_min_market_days,
                "directional_confidence_threshold_market_days": self.settings.historical_directional_confidence_min_full_market_days,
                "directional_confidence_threshold_holdout_market_days": self.settings.historical_directional_confidence_min_holdout_market_days,
            },
            "confidence_progress": self._confidence_progress(
                {
                    "confidence_state": confidence_state,
                    "distinct_execution_market_days": execution_market_days,
                    "distinct_full_market_days": full_market_days,
                    "full_coverage_holdout_market_days": holdout_market_days,
                    "execution_confidence_threshold_market_days": self.settings.historical_execution_confidence_min_market_days,
                    "directional_confidence_threshold_market_days": self.settings.historical_directional_confidence_min_full_market_days,
                    "directional_confidence_threshold_holdout_market_days": self.settings.historical_directional_confidence_min_holdout_market_days,
                }
            ),
        }

    def _confidence_progress(self, scorecard: dict[str, Any]) -> dict[str, Any]:
        def lane(current: int, target: int) -> dict[str, Any]:
            remaining = max(0, target - current)
            percent = 1.0 if target <= 0 else min(1.0, current / target)
            return {
                "current": current,
                "target": target,
                "remaining": remaining,
                "met": remaining == 0,
                "progress_ratio": round(percent, 4),
            }

        execution_lane = lane(
            int(scorecard.get("distinct_execution_market_days", 0)),
            int(
                scorecard.get(
                    "execution_confidence_threshold_market_days",
                    self.settings.historical_execution_confidence_min_market_days,
                )
            ),
        )
        directional_lane = lane(
            int(scorecard.get("distinct_full_market_days", 0)),
            int(
                scorecard.get(
                    "directional_confidence_threshold_market_days",
                    self.settings.historical_directional_confidence_min_full_market_days,
                )
            ),
        )
        holdout_lane = lane(
            int(scorecard.get("full_coverage_holdout_market_days", 0)),
            int(
                scorecard.get(
                    "directional_confidence_threshold_holdout_market_days",
                    self.settings.historical_directional_confidence_min_holdout_market_days,
                )
            ),
        )
        blockers: list[str] = []
        if not execution_lane["met"]:
            blockers.append("lack_of_execution_support")
        if not directional_lane["met"]:
            blockers.append("lack_of_full_coverage_support")
        if not holdout_lane["met"]:
            blockers.append("lack_of_holdout_support")
        return {
            "confidence_state": str(scorecard.get("confidence_state") or "insufficient_support"),
            "execution_support": execution_lane,
            "directional_support": directional_lane,
            "holdout_support": holdout_lane,
            "promotion_blockers": blockers,
            "execution_candidate_evaluation_allowed": execution_lane["met"],
            "directional_candidate_evaluation_allowed": execution_lane["met"]
            and directional_lane["met"]
            and holdout_lane["met"],
        }

    def _historical_market_days_for_coverage(
        self,
        bundles: list[Any],
        *,
        coverage_class: str,
        room_ids: set[str] | None = None,
    ) -> set[str]:
        return {
            str((bundle.historical_provenance or {}).get("local_market_day") or "")
            for bundle in bundles
            if (bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")) == coverage_class
            and (room_ids is None or bundle.room["id"] in room_ids)
            and (bundle.historical_provenance or {}).get("local_market_day")
        }

    def _split_historical_bundles(self, bundles: list[Any]) -> HistoricalBuildSplit:
        grouped: dict[str, list[str]] = defaultdict(list)
        for bundle in bundles:
            local_day = str((bundle.historical_provenance or {}).get("local_market_day") or (bundle.settlement_label or {}).get("local_market_day") or "")
            if not local_day:
                continue
            grouped[local_day].append(bundle.room["id"])
        ordered_days = sorted(grouped)
        if not ordered_days:
            return HistoricalBuildSplit(train=[], validation=[], holdout=[])
        day_count = len(ordered_days)
        if day_count == 1:
            train_days = 1
            validation_days = 0
            holdout_days = 0
        elif day_count == 2:
            train_days = 1
            validation_days = 0
            holdout_days = 1
        elif day_count == 3:
            train_days = 1
            validation_days = 1
            holdout_days = 1
        else:
            validation_days = max(1, int(day_count * 0.15))
            train_days = max(1, int(day_count * 0.70))
            train_days = min(train_days, day_count - validation_days - 1)
            holdout_days = max(1, day_count - train_days - validation_days)
        train = ordered_days[:train_days]
        validation = ordered_days[train_days : train_days + validation_days]
        holdout = ordered_days[train_days + validation_days :]
        return HistoricalBuildSplit(
            train=[room_id for day in train for room_id in grouped[day]],
            validation=[room_id for day in validation for room_id in grouped[day]],
            holdout=[room_id for day in holdout for room_id in grouped[day]],
        )

    def _filter_historical_bundles(
        self,
        bundles: list[Any],
        *,
        quality_cleaned_only: bool,
        include_pathology_examples: bool,
        require_full_checkpoints: bool,
        late_only_ok: bool,
        mode: str,
    ) -> list[Any]:
        selected: list[Any] = []
        for bundle in bundles:
            if bundle.settlement_label is None:
                continue
            coverage_class = bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")
            if not include_pathology_examples and (bundle.settlement_label or {}).get("crosscheck_status") == self.SETTLEMENT_MISMATCH:
                continue
            if quality_cleaned_only and bundle.trainable_default is False:
                continue
            if require_full_checkpoints and mode in {"role-sft", "decision-eval", "gemini-finetune"} and coverage_class != self.COVERAGE_FULL:
                continue
            if mode == "outcome-eval" and not late_only_ok and coverage_class not in {self.COVERAGE_FULL}:
                continue
            if coverage_class in {self.COVERAGE_NONE, self.COVERAGE_OUTCOME_ONLY}:
                continue
            selected.append(bundle)
        return selected

    def _historical_role_examples(self, bundles: list[Any], split: HistoricalBuildSplit, *, draft_only: bool) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for bundle in bundles:
            split_name = self._split_name(bundle.room["id"], split)
            for example in self.training_export_service.build_role_training_examples(bundle):
                payload = example.model_dump(mode="json")
                payload["metadata"]["split"] = split_name
                payload["metadata"]["room_origin"] = RoomOrigin.HISTORICAL_REPLAY.value
                payload["metadata"]["coverage_class"] = bundle.coverage_class or (bundle.historical_provenance or {}).get("coverage_class")
                payload["metadata"]["draft_only"] = draft_only
                records.append(payload)
        return records

    def _decision_eval_item(self, bundle: Any, split: HistoricalBuildSplit, *, draft_only: bool) -> dict[str, Any]:
        return {
            "room_id": bundle.room["id"],
            "market_ticker": bundle.room["market_ticker"],
            "room_origin": bundle.room_origin,
            "split": self._split_name(bundle.room["id"], split),
            "replay_checkpoint_ts": self._dt_to_iso(bundle.replay_checkpoint_ts),
            "historical_provenance": bundle.historical_provenance,
            "coverage_class": bundle.coverage_class,
            "draft_only": draft_only,
            "signal": bundle.signal,
            "strategy_audit": bundle.strategy_audit,
            "research_health": bundle.research_health,
            "trade_ticket": bundle.trade_ticket,
            "risk_verdict": bundle.risk_verdict,
            "outcome": bundle.outcome.model_dump(mode="json"),
        }

    def _outcome_eval_item(self, bundle: Any, split: HistoricalBuildSplit, *, draft_only: bool) -> dict[str, Any]:
        return {
            "room_id": bundle.room["id"],
            "market_ticker": bundle.room["market_ticker"],
            "room_origin": bundle.room_origin,
            "split": self._split_name(bundle.room["id"], split),
            "replay_checkpoint_ts": self._dt_to_iso(bundle.replay_checkpoint_ts),
            "historical_provenance": bundle.historical_provenance,
            "coverage_class": bundle.coverage_class,
            "draft_only": draft_only,
            "fair_yes_dollars": (bundle.signal or {}).get("fair_yes_dollars"),
            "resolution_state": ((bundle.signal or {}).get("payload") or {}).get("resolution_state"),
            "strategy_audit": bundle.strategy_audit,
            "settlement_label": bundle.settlement_label,
            "counterfactual_pnl_dollars": (
                str(bundle.counterfactual_pnl_dollars) if bundle.counterfactual_pnl_dollars is not None else None
            ),
            "outcome": bundle.outcome.model_dump(mode="json"),
        }

    def _bundle_with_split(self, bundle: Any, split: HistoricalBuildSplit, *, draft_only: bool) -> dict[str, Any]:
        payload = bundle.model_dump(mode="json")
        payload["split"] = self._split_name(bundle.room["id"], split)
        payload["draft_only"] = draft_only
        return payload

    def _historical_dataset_item(self, bundle: Any, split: HistoricalBuildSplit, *, draft_only: bool) -> dict[str, Any]:
        return {
            "room_id": bundle.room["id"],
            "market_ticker": bundle.room["market_ticker"],
            "room_origin": bundle.room_origin,
            "split": self._split_name(bundle.room["id"], split),
            "replay_checkpoint_ts": self._dt_to_iso(bundle.replay_checkpoint_ts),
            "historical_provenance": bundle.historical_provenance,
            "market_source_kind": bundle.market_source_kind,
            "weather_source_kind": bundle.weather_source_kind,
            "coverage_class": bundle.coverage_class,
            "draft_only": draft_only,
            "strategy_audit": bundle.strategy_audit,
            "audit_source": bundle.audit_source,
            "audit_version": bundle.audit_version,
            "trainable_default": bundle.trainable_default,
            "exclude_reason": bundle.exclude_reason,
            "settlement_label": bundle.settlement_label,
            "counterfactual_pnl_dollars": (
                str(bundle.counterfactual_pnl_dollars) if bundle.counterfactual_pnl_dollars is not None else None
            ),
            "heuristic_pack_version": bundle.heuristic_pack_version,
            "intelligence_run_id": bundle.intelligence_run_id,
            "candidate_pack_id": bundle.candidate_pack_id,
            "rule_trace": bundle.rule_trace,
            "support_window": bundle.support_window,
        }

    def _historical_label_stats(self, bundles: list[Any], split: HistoricalBuildSplit, *, draft_only: bool, training_ready: bool) -> dict[str, Any]:
        market_source_kind_counts = Counter(
            getattr(bundle, "market_source_kind", None) or (bundle.historical_provenance or {}).get("market_source_kind")
            for bundle in bundles
            if getattr(bundle, "market_source_kind", None) or (bundle.historical_provenance or {}).get("market_source_kind")
        )
        weather_source_kind_counts = Counter(
            getattr(bundle, "weather_source_kind", None) or (bundle.historical_provenance or {}).get("weather_source_kind")
            for bundle in bundles
            if getattr(bundle, "weather_source_kind", None) or (bundle.historical_provenance or {}).get("weather_source_kind")
        )
        return {
            "origin_counts": dict(Counter(bundle.room_origin for bundle in bundles)),
            "audit_source_counts": dict(Counter(bundle.audit_source for bundle in bundles if bundle.audit_source)),
            "coverage_class_counts": dict(Counter(bundle.coverage_class for bundle in bundles if bundle.coverage_class)),
            "market_source_kind_counts": dict(market_source_kind_counts),
            "weather_source_kind_counts": dict(weather_source_kind_counts),
            "external_archive_weather_count": weather_source_kind_counts.get(self.EXTERNAL_FORECAST_ARCHIVE_SOURCE, 0),
            "split_counts": {
                "train": len(split.train),
                "validation": len(split.validation),
                "holdout": len(split.holdout),
            },
            "draft_only": draft_only,
            "training_ready": training_ready,
            "settlement_mismatch_count": sum(
                1 for bundle in bundles if (bundle.settlement_label or {}).get("crosscheck_status") == self.SETTLEMENT_MISMATCH
            ),
            "trainable_default_count": sum(1 for bundle in bundles if bundle.trainable_default is not False),
            "exclude_reason_counts": dict(Counter(bundle.exclude_reason for bundle in bundles if bundle.exclude_reason)),
            "role_example_count": sum(len(self.training_export_service.build_role_training_examples(bundle)) for bundle in bundles),
        }

    def _write_single_jsonl(self, output: str | None, records: list[dict[str, Any]]) -> str | None:
        if output is None:
            return None
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, default=str))
                handle.write("\n")
        return str(path)

    def _write_gemini_export(
        self,
        output: str | None,
        records: list[dict[str, Any]],
        *,
        bundles: list[Any],
        split: HistoricalBuildSplit,
        draft_only: bool,
        training_ready: bool,
    ) -> dict[str, str] | None:
        if output is None:
            return None
        output_dir = Path(output)
        output_dir.mkdir(parents=True, exist_ok=True)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            grouped[str((record.get("metadata") or {}).get("split") or "train")].append(self._gemini_example(record))
        paths: dict[str, str] = {}
        for split_name in ("train", "validation", "holdout"):
            path = output_dir / f"{split_name}.jsonl"
            with path.open("w", encoding="utf-8") as handle:
                for record in grouped.get(split_name, []):
                    handle.write(json.dumps(record, default=str))
                    handle.write("\n")
            paths[split_name] = str(path)
        local_days = sorted(
            {
                str((bundle.historical_provenance or {}).get("local_market_day") or "")
                for bundle in bundles
                if (bundle.historical_provenance or {}).get("local_market_day")
            }
        )
        checkpoint_values = [
            self._dt_to_iso(bundle.replay_checkpoint_ts)
            for bundle in bundles
            if self._dt_to_iso(bundle.replay_checkpoint_ts) is not None
        ]
        exclusion_counts = dict(Counter(bundle.exclude_reason for bundle in bundles if bundle.exclude_reason))
        audit_source_counts = dict(Counter(bundle.audit_source for bundle in bundles if bundle.audit_source))
        pack_versions = sorted({bundle.room.get("agent_pack_version") for bundle in bundles if bundle.room.get("agent_pack_version")})
        weather_source_kind_counts = dict(
            Counter(
                getattr(bundle, "weather_source_kind", None) or (bundle.historical_provenance or {}).get("weather_source_kind")
                for bundle in bundles
                if getattr(bundle, "weather_source_kind", None) or (bundle.historical_provenance or {}).get("weather_source_kind")
            )
        )
        market_source_kind_counts = dict(
            Counter(
                getattr(bundle, "market_source_kind", None) or (bundle.historical_provenance or {}).get("market_source_kind")
                for bundle in bundles
                if getattr(bundle, "market_source_kind", None) or (bundle.historical_provenance or {}).get("market_source_kind")
            )
        )
        manifest = {
            "format": "gemini-finetune.v1",
            "format_target": "gemini_vertex_chat_jsonl",
            "provider": "gemini",
            "draft_only": draft_only,
            "training_ready": training_ready,
            "paths": paths,
            "counts": {split_name: len(grouped.get(split_name, [])) for split_name in ("train", "validation", "holdout")},
            "split_boundaries": {
                "train_room_ids": split.train,
                "validation_room_ids": split.validation,
                "holdout_room_ids": split.holdout,
            },
            "source_windows": {
                "local_market_day_start": local_days[0] if local_days else None,
                "local_market_day_end": local_days[-1] if local_days else None,
                "checkpoint_ts_start": checkpoint_values[0] if checkpoint_values else None,
                "checkpoint_ts_end": checkpoint_values[-1] if checkpoint_values else None,
            },
            "pack_versions": pack_versions,
            "audit_stats": {
                "audit_sources": audit_source_counts,
                "coverage_class_counts": dict(
                    Counter(
                        getattr(bundle, "coverage_class", None)
                        for bundle in bundles
                        if getattr(bundle, "coverage_class", None)
                    )
                ),
                "market_source_kind_counts": market_source_kind_counts,
                "weather_source_kind_counts": weather_source_kind_counts,
                "external_archive_weather_count": int(
                    weather_source_kind_counts.get(self.EXTERNAL_FORECAST_ARCHIVE_SOURCE, 0)
                ),
                "settlement_mismatch_count": sum(
                    1
                    for bundle in bundles
                    if (bundle.settlement_label or {}).get("crosscheck_status") == self.SETTLEMENT_MISMATCH
                ),
                "trainable_default_count": sum(1 for bundle in bundles if bundle.trainable_default is not False),
                "excluded_count": sum(1 for bundle in bundles if bundle.exclude_reason),
                "exclusion_counts": exclusion_counts,
            },
        }
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        paths["manifest"] = str(manifest_path)
        return paths

    @staticmethod
    def _gemini_example(example: dict[str, Any]) -> dict[str, Any]:
        messages = example.get("messages") or []
        system_text = next((message.get("content") for message in messages if message.get("role") == "system"), "")
        user_text = next((message.get("content") for message in messages if message.get("role") == "user"), "")
        assistant_text = next((message.get("content") for message in messages if message.get("role") == "assistant"), "")
        return {
            "system_instruction": {
                "parts": [{"text": system_text}],
            },
            "contents": [
                {"role": "user", "parts": [{"text": user_text}]},
                {"role": "model", "parts": [{"text": assistant_text}]},
            ],
            "metadata": example.get("metadata") or {},
        }

    def _split_name(self, room_id: str, split: HistoricalBuildSplit) -> str:
        if room_id in split.validation:
            return "validation"
        if room_id in split.holdout:
            return "holdout"
        return "train"

    def _selected_templates(self, series: list[str] | None) -> list[WeatherSeriesTemplate]:
        templates = self.weather_directory.templates()
        if not series:
            return templates
        wanted = {item.upper() for item in series}
        return [template for template in templates if template.series_ticker.upper() in wanted]

    def _mapping_for_market(self, market_ticker: str, market_payload: dict[str, Any] | None) -> WeatherMarketMapping | None:
        mapping = self.weather_directory.get(market_ticker)
        if mapping is not None:
            return mapping
        return self.weather_directory.resolve_market(market_ticker, market_payload or {})

    def _template_for_market_ticker(self, market_ticker: str) -> WeatherSeriesTemplate | None:
        for template in self.weather_directory.templates():
            if template.supports_market_ticker(market_ticker):
                return template
        return None

    def _market_local_day(self, mapping: WeatherMarketMapping, market: dict[str, Any]) -> str:
        ticker = str(market.get("ticker") or mapping.market_ticker)
        parts = ticker.split("-")
        if len(parts) >= 3 and len(parts[1]) == 7:
            try:
                parsed = datetime.strptime(parts[1], "%y%b%d")
                return parsed.date().isoformat()
            except ValueError:
                pass
        close_at = self._market_timestamp(market, "close_time", "close_ts")
        zone = ZoneInfo(self._timezone_name(mapping))
        if close_at is not None:
            return close_at.astimezone(zone).date().isoformat()
        return datetime.now(zone).date().isoformat()

    def _weather_local_day(self, mapping: WeatherMarketMapping, payload: dict[str, Any], *, fallback: datetime) -> str:
        observation_ts = _parse_iso(((payload.get("observation") or {}).get("properties") or {}).get("timestamp"))
        zone = ZoneInfo(self._timezone_name(mapping))
        return (observation_ts or fallback).astimezone(zone).date().isoformat()

    def _checkpoint_times(
        self,
        mapping: WeatherMarketMapping,
        *,
        local_market_day: str,
        market_payload: dict[str, Any],
    ) -> list[tuple[str, datetime]]:
        zone = ZoneInfo(self._timezone_name(mapping))
        local_day = datetime.fromisoformat(local_market_day).date()
        close_at = self._market_timestamp(market_payload, "close_time", "close_ts")
        close_local = close_at.astimezone(zone) if close_at is not None else None
        checkpoints: list[tuple[str, datetime]] = []
        for label, hour in self.CHECKPOINTS:
            local_dt = datetime.combine(local_day, time(hour=hour), tzinfo=zone)
            if label == "late_1700" and close_local is not None:
                candidate = close_local - timedelta(hours=1)
                if candidate < local_dt:
                    local_dt = candidate
            checkpoints.append((label, local_dt.astimezone(UTC)))
        deduped: dict[str, datetime] = {}
        for label, checkpoint in checkpoints:
            deduped[checkpoint.isoformat()] = checkpoint
        return [
            (f"checkpoint_{index+1}", checkpoint)
            for index, checkpoint in enumerate(sorted(deduped.values()))
        ]

    async def _find_market_for_template_day(
        self,
        template: WeatherSeriesTemplate,
        *,
        local_market_day: str,
    ) -> dict[str, Any] | None:
        markets = await self._list_template_day_markets(template, local_market_day=local_market_day)
        return markets[0] if markets else None

    async def _list_template_day_markets(
        self,
        template: WeatherSeriesTemplate,
        *,
        local_market_day: str,
    ) -> list[dict[str, Any]]:
        start_local = datetime.fromisoformat(local_market_day)
        zone = ZoneInfo(template.timezone_name or "UTC")
        start_ts = int(datetime.combine(start_local.date(), time.min, tzinfo=zone).astimezone(UTC).timestamp())
        end_ts = int(datetime.combine(start_local.date() + timedelta(days=1), time.min, tzinfo=zone).astimezone(UTC).timestamp())
        markets: dict[str, dict[str, Any]] = {}
        cursor: str | None = None
        while True:
            response = await self.kalshi.list_markets(
                series_ticker=template.series_ticker,
                min_close_ts=start_ts,
                max_close_ts=end_ts,
                limit=self.settings.historical_import_page_size,
                cursor=cursor,
            )
            page = response.get("markets") or []
            for market in page:
                resolved = template.resolve_market(market)
                if resolved is None:
                    continue
                if self._market_local_day(resolved, market) == local_market_day:
                    markets[resolved.market_ticker] = market
            cursor = response.get("cursor")
            if not cursor or not page:
                break
        return [markets[ticker] for ticker in sorted(markets)]

    def _checkpoint_mapping(self, template: WeatherSeriesTemplate, *, market: dict[str, Any] | None) -> WeatherMarketMapping:
        if market is not None:
            resolved = template.resolve_market(market)
            if resolved is not None:
                return resolved
        return WeatherMarketMapping(
            market_ticker=template.series_ticker,
            market_type="weather",
            display_name=template.display_name,
            description=template.description,
            research_queries=list(template.research_queries),
            research_urls=list(template.research_urls),
            station_id=template.station_id,
            daily_summary_station_id=template.daily_summary_station_id,
            location_name=template.location_name,
            timezone_name=template.timezone_name,
            latitude=template.latitude,
            longitude=template.longitude,
            threshold_f=0,
            operator=">",
            metric=template.metric,
            settlement_source=template.settlement_source,
            series_ticker=template.series_ticker,
        )

    def _checkpoint_capture_due(self, checkpoint_ts: datetime, *, now: datetime) -> bool:
        lead = timedelta(seconds=max(0, self.settings.historical_checkpoint_capture_lead_seconds))
        grace = timedelta(seconds=max(60, self.settings.historical_checkpoint_capture_grace_seconds))
        return checkpoint_ts - lead <= now <= checkpoint_ts + grace

    def _checkpoint_market_snapshot_asof(
        self,
        market_payload: dict[str, Any],
        *,
        checkpoint_ts: datetime,
    ) -> tuple[datetime | None, str | None]:
        asof_ts = self._market_timestamp(market_payload, "updated_time")
        if asof_ts is None:
            return None, "market_snapshot_missing_metadata"
        if asof_ts > checkpoint_ts:
            return None, "market_snapshot_future"
        if is_market_stale(
            observed_at=asof_ts,
            stale_after_seconds=self.settings.historical_replay_market_stale_seconds,
            reference_time=checkpoint_ts,
        ):
            return None, "market_snapshot_stale"
        return asof_ts, None

    @staticmethod
    def _checkpoint_archive_metadata_valid(metadata: dict[str, Any], checkpoint_ts: datetime) -> bool:
        observation_ts = _as_utc(metadata.get("observation_ts"))
        forecast_updated_ts = _as_utc(metadata.get("forecast_updated_ts"))
        asof_ts = _as_utc(metadata.get("asof_ts"))
        if observation_ts is None and forecast_updated_ts is None and asof_ts is None:
            return False
        for candidate in (observation_ts, forecast_updated_ts, asof_ts):
            if candidate is not None and candidate > checkpoint_ts:
                return False
        return True

    async def _fetch_market_for_backfill(self, market_ticker: str) -> dict[str, Any]:
        try:
            return await self.kalshi.get_market(market_ticker)
        except httpx.HTTPStatusError:
            cursor: str | None = None
            pages = 0
            while pages < self.settings.historical_import_max_pages:
                response = await self.kalshi.list_historical_markets(
                    limit=self.settings.historical_import_page_size,
                    cursor=cursor,
                )
                page = response.get("markets") or []
                for market in page:
                    if str(market.get("ticker") or "") == market_ticker:
                        return {"market": market}
                cursor = response.get("cursor")
                pages += 1
                if not cursor or not page:
                    break
            raise

    def _market_day_from_ticker_or_close(self, market_ticker: str, close_at: datetime | None) -> str | None:
        parts = market_ticker.split("-")
        if len(parts) >= 3 and len(parts[1]) == 7:
            try:
                return datetime.strptime(parts[1], "%y%b%d").date().isoformat()
            except ValueError:
                pass
        if close_at is not None:
            return close_at.astimezone(UTC).date().isoformat()
        return None

    @staticmethod
    def _series_from_market_ticker(market_ticker: str) -> str:
        return str(market_ticker.split("-")[0] if market_ticker else "").upper()

    def _timezone_name(self, mapping: WeatherMarketMapping) -> str:
        if mapping.timezone_name:
            return mapping.timezone_name
        longitude = mapping.longitude
        if longitude is not None:
            if longitude <= -85.0 and longitude > -100.0:
                return "America/Chicago"
            if longitude <= -60.0 and longitude > -85.0:
                return "America/New_York"
        if mapping.series_ticker == "KXHIGHCHI":
            return "America/Chicago"
        return "America/New_York"

    @staticmethod
    def _market_timestamp(market: dict[str, Any], *keys: str) -> datetime | None:
        for key in keys:
            value = market.get(key)
            if value in (None, ""):
                continue
            if isinstance(value, (int, float)):
                try:
                    return datetime.fromtimestamp(int(value), tz=UTC)
                except (OverflowError, OSError, ValueError):
                    continue
            parsed = _parse_iso(value)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _market_settlement_value(market: dict[str, Any]) -> Decimal | None:
        if market.get("settlement_value_dollars") not in (None, ""):
            return _parse_decimal(market.get("settlement_value_dollars"))
        result = str(market.get("result") or "").lower()
        if result == "yes":
            return Decimal("1.0000")
        if result == "no":
            return Decimal("0.0000")
        return None

    @staticmethod
    def _settlement_result_from_high(mapping: WeatherMarketMapping, high_f: Decimal) -> str:
        threshold = Decimal(str(mapping.threshold_f))
        if mapping.operator == ">":
            return "yes" if high_f > threshold else "no"
        if mapping.operator == ">=":
            return "yes" if high_f >= threshold else "no"
        if mapping.operator == "<":
            return "yes" if high_f < threshold else "no"
        return "yes" if high_f <= threshold else "no"

    @staticmethod
    def _quantize_two(value: float | None) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value)).quantize(Decimal("0.01"))

    @staticmethod
    def _dt_to_iso(value: Any) -> str | None:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            return value
        return None
