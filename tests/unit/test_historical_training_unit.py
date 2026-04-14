from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
import asyncio

import httpx
import pytest

from decimal import Decimal

from kalshi_bot.services.historical_training import (
    HistoricalBuildSplit,
    HistoricalCheckpointSelection,
    HistoricalTrainingService,
    _json_safe,
)
from kalshi_bot.weather.mapping import WeatherSeriesTemplate
from kalshi_bot.weather.models import WeatherMarketMapping


class _DummyResponse:
    def __init__(self, rows):
        self._rows = rows

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._rows


class _DummyClient:
    def __init__(self, rows):
        self.rows = rows

    async def get(self, url: str, params: dict[str, str]):
        return _DummyResponse(self.rows)


class _DummyKalshi:
    def __init__(self, responses=None, candlestick_responses=None):
        self.responses = list(responses or [])
        self.candlestick_responses = list(candlestick_responses or [])
        self.calls = []

    async def list_markets(self, **params):
        self.calls.append(params)
        return self.responses.pop(0)

    async def get_market_candlesticks(self, series_ticker: str, market_ticker: str, **params):
        self.calls.append(
            {
                "series_ticker": series_ticker,
                "market_ticker": market_ticker,
                **params,
            }
        )
        response = self.candlestick_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.mark.asyncio
async def test_daily_summary_crosscheck_detects_mismatch() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.client = _DummyClient([{"TMAX": "82"}])

    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHNY-26APR13-T80",
        market_type="weather",
        station_id="KNYC",
        daily_summary_station_id="USW00094728",
        location_name="New York City",
        latitude=40.7146,
        longitude=-74.0071,
        threshold_f=80,
        operator=">",
        settlement_source="NWS daily summary",
        series_ticker="KXHIGHNY",
    )

    result = await HistoricalTrainingService._daily_summary_crosscheck(
        service,
        mapping,
        "2026-04-13",
        kalshi_result="no",
    )

    assert result["status"] == HistoricalTrainingService.SETTLEMENT_MISMATCH
    assert str(result["daily_high_f"]) == "82.00"
    assert result["result"] == "yes"
    assert result["mismatch_reason"] == HistoricalTrainingService.SETTLEMENT_MISMATCH_REASON_DISAGREEMENT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operator", "kalshi_result"),
    [
        (">", "no"),
        ("<", "no"),
    ],
)
async def test_daily_summary_crosscheck_treats_strict_threshold_equality_as_no(operator: str, kalshi_result: str) -> None:
    service = object.__new__(HistoricalTrainingService)
    service.client = _DummyClient([{"TMAX": "80"}])

    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHNY-26APR13-T80",
        market_type="weather",
        station_id="KNYC",
        daily_summary_station_id="USW00094728",
        location_name="New York City",
        latitude=40.7146,
        longitude=-74.0071,
        threshold_f=80,
        operator=operator,
        settlement_source="NWS daily summary",
        series_ticker="KXHIGHNY",
    )

    result = await HistoricalTrainingService._daily_summary_crosscheck(
        service,
        mapping,
        "2026-04-13",
        kalshi_result=kalshi_result,
    )

    assert result["status"] == HistoricalTrainingService.SETTLEMENT_MATCH
    assert result["result"] == "no"
    assert result["mismatch_reason"] is None


@pytest.mark.asyncio
async def test_daily_summary_crosscheck_classifies_threshold_edge_strictness() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.client = _DummyClient([{"TMAX": "80"}])

    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHNY-26APR13-T80",
        market_type="weather",
        station_id="KNYC",
        daily_summary_station_id="USW00094728",
        location_name="New York City",
        latitude=40.7146,
        longitude=-74.0071,
        threshold_f=80,
        operator=">",
        settlement_source="NWS daily summary",
        series_ticker="KXHIGHNY",
    )

    result = await HistoricalTrainingService._daily_summary_crosscheck(
        service,
        mapping,
        "2026-04-13",
        kalshi_result="yes",
    )

    assert result["status"] == HistoricalTrainingService.SETTLEMENT_MISMATCH
    assert result["result"] == "no"
    assert result["mismatch_reason"] == HistoricalTrainingService.SETTLEMENT_MISMATCH_REASON_THRESHOLD_EDGE


def test_historical_split_keeps_market_day_together() -> None:
    service = object.__new__(HistoricalTrainingService)
    bundles = [
        SimpleNamespace(room={"id": "room-a"}, historical_provenance={"local_market_day": "2026-04-10"}),
        SimpleNamespace(room={"id": "room-b"}, historical_provenance={"local_market_day": "2026-04-10"}),
        SimpleNamespace(room={"id": "room-c"}, historical_provenance={"local_market_day": "2026-04-11"}),
        SimpleNamespace(room={"id": "room-d"}, historical_provenance={"local_market_day": "2026-04-12"}),
    ]

    split = HistoricalTrainingService._split_historical_bundles(service, bundles)

    split_by_room = {}
    for room_id in split.train:
        split_by_room[room_id] = "train"
    for room_id in split.validation:
        split_by_room[room_id] = "validation"
    for room_id in split.holdout:
        split_by_room[room_id] = "holdout"

    assert split_by_room["room-a"] == split_by_room["room-b"]


def test_historical_split_two_days_creates_train_and_holdout_only() -> None:
    service = object.__new__(HistoricalTrainingService)
    bundles = [
        SimpleNamespace(room={"id": "room-a"}, historical_provenance={"local_market_day": "2026-04-10"}),
        SimpleNamespace(room={"id": "room-b"}, historical_provenance={"local_market_day": "2026-04-11"}),
    ]

    split = HistoricalTrainingService._split_historical_bundles(service, bundles)

    assert split.train == ["room-a"]
    assert split.validation == []
    assert split.holdout == ["room-b"]


def test_historical_split_three_days_creates_train_validation_and_holdout() -> None:
    service = object.__new__(HistoricalTrainingService)
    bundles = [
        SimpleNamespace(room={"id": "room-a"}, historical_provenance={"local_market_day": "2026-04-10"}),
        SimpleNamespace(room={"id": "room-b"}, historical_provenance={"local_market_day": "2026-04-11"}),
        SimpleNamespace(room={"id": "room-c"}, historical_provenance={"local_market_day": "2026-04-12"}),
    ]

    split = HistoricalTrainingService._split_historical_bundles(service, bundles)

    assert split.train == ["room-a"]
    assert split.validation == ["room-b"]
    assert split.holdout == ["room-c"]


def test_historical_market_days_for_coverage_counts_only_matching_coverage() -> None:
    service = object.__new__(HistoricalTrainingService)
    bundles = [
        SimpleNamespace(
            room={"id": "room-full-1"},
            coverage_class=HistoricalTrainingService.COVERAGE_FULL,
            historical_provenance={"local_market_day": "2026-04-10", "coverage_class": HistoricalTrainingService.COVERAGE_FULL},
        ),
        SimpleNamespace(
            room={"id": "room-full-2"},
            coverage_class=HistoricalTrainingService.COVERAGE_FULL,
            historical_provenance={"local_market_day": "2026-04-10", "coverage_class": HistoricalTrainingService.COVERAGE_FULL},
        ),
        SimpleNamespace(
            room={"id": "room-late"},
            coverage_class=HistoricalTrainingService.COVERAGE_LATE_ONLY,
            historical_provenance={"local_market_day": "2026-04-11", "coverage_class": HistoricalTrainingService.COVERAGE_LATE_ONLY},
        ),
    ]

    all_full_days = HistoricalTrainingService._historical_market_days_for_coverage(
        service,
        bundles,
        coverage_class=HistoricalTrainingService.COVERAGE_FULL,
    )
    holdout_full_days = HistoricalTrainingService._historical_market_days_for_coverage(
        service,
        bundles,
        coverage_class=HistoricalTrainingService.COVERAGE_FULL,
        room_ids={"room-full-2", "room-late"},
    )
    empty_holdout_full_days = HistoricalTrainingService._historical_market_days_for_coverage(
        service,
        bundles,
        coverage_class=HistoricalTrainingService.COVERAGE_FULL,
        room_ids=set(),
    )

    assert all_full_days == {"2026-04-10"}
    assert holdout_full_days == {"2026-04-10"}
    assert empty_holdout_full_days == set()


def test_coverage_class_detects_full_and_late_only() -> None:
    service = object.__new__(HistoricalTrainingService)

    full = HistoricalTrainingService._coverage_class(
        service,
        [
            HistoricalCheckpointSelection("c1", datetime(2026, 4, 10, 13, tzinfo=UTC), object(), object(), "captured_market_snapshot", "archived_weather_bundle", []),
            HistoricalCheckpointSelection("c2", datetime(2026, 4, 10, 17, tzinfo=UTC), object(), object(), "captured_market_snapshot", "archived_weather_bundle", []),
            HistoricalCheckpointSelection("c3", datetime(2026, 4, 10, 21, tzinfo=UTC), object(), object(), "captured_market_snapshot", "archived_weather_bundle", []),
        ],
    )
    late_only = HistoricalTrainingService._coverage_class(
        service,
        [
            HistoricalCheckpointSelection("c1", datetime(2026, 4, 10, 13, tzinfo=UTC), None, None, None, None, ["market_snapshot_missing"]),
            HistoricalCheckpointSelection("c2", datetime(2026, 4, 10, 17, tzinfo=UTC), None, None, None, None, ["weather_snapshot_missing"]),
            HistoricalCheckpointSelection("c3", datetime(2026, 4, 10, 21, tzinfo=UTC), object(), object(), "reconstructed_market_checkpoint", "archived_weather_bundle", []),
        ],
    )

    assert full == HistoricalTrainingService.COVERAGE_FULL
    assert late_only == HistoricalTrainingService.COVERAGE_LATE_ONLY


def test_coverage_class_detects_partial_and_outcome_only() -> None:
    service = object.__new__(HistoricalTrainingService)

    partial = HistoricalTrainingService._coverage_class(
        service,
        [
            HistoricalCheckpointSelection("c1", datetime(2026, 4, 10, 13, tzinfo=UTC), object(), object(), "captured_market_snapshot", "archived_weather_bundle", []),
            HistoricalCheckpointSelection("c2", datetime(2026, 4, 10, 17, tzinfo=UTC), None, None, None, None, ["weather_snapshot_missing"]),
            HistoricalCheckpointSelection("c3", datetime(2026, 4, 10, 21, tzinfo=UTC), object(), object(), "reconstructed_market_checkpoint", "archived_weather_bundle", []),
        ],
    )
    outcome_only = HistoricalTrainingService._coverage_class(
        service,
        [
            HistoricalCheckpointSelection("c1", datetime(2026, 4, 10, 13, tzinfo=UTC), None, None, None, None, ["market_snapshot_missing"]),
            HistoricalCheckpointSelection("c2", datetime(2026, 4, 10, 17, tzinfo=UTC), None, None, None, None, ["weather_snapshot_missing"]),
            HistoricalCheckpointSelection("c3", datetime(2026, 4, 10, 21, tzinfo=UTC), None, None, None, None, ["weather_snapshot_missing"]),
        ],
    )
    checkpoint_none = HistoricalTrainingService._coverage_class(
        service,
        [
            HistoricalCheckpointSelection("c1", datetime(2026, 4, 10, 13, tzinfo=UTC), None, None, None, None, ["market_snapshot_missing"]),
        ],
        use_outcome_only=False,
    )

    assert partial == HistoricalTrainingService.COVERAGE_PARTIAL
    assert outcome_only == HistoricalTrainingService.COVERAGE_OUTCOME_ONLY
    assert checkpoint_none == HistoricalTrainingService.COVERAGE_NONE


def test_checkpoint_capture_due_and_metadata_validation() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(
        historical_checkpoint_capture_lead_seconds=300,
        historical_checkpoint_capture_grace_seconds=900,
    )
    checkpoint_ts = datetime(2026, 4, 10, 13, 0, tzinfo=UTC)

    assert HistoricalTrainingService._checkpoint_capture_due(
        service,
        checkpoint_ts,
        now=datetime(2026, 4, 10, 12, 56, tzinfo=UTC),
    ) is True
    assert HistoricalTrainingService._checkpoint_capture_due(
        service,
        checkpoint_ts,
        now=datetime(2026, 4, 10, 12, 54, tzinfo=UTC),
    ) is False
    assert HistoricalTrainingService._checkpoint_capture_due(service, checkpoint_ts, now=checkpoint_ts) is True
    assert HistoricalTrainingService._checkpoint_capture_due(
        service,
        checkpoint_ts,
        now=datetime(2026, 4, 10, 13, 10, tzinfo=UTC),
    ) is True
    assert HistoricalTrainingService._checkpoint_capture_due(
        service,
        checkpoint_ts,
        now=datetime(2026, 4, 10, 13, 16, tzinfo=UTC),
    ) is False


def test_checkpoint_market_snapshot_asof_rejects_future_and_stale_quotes() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(historical_replay_market_stale_seconds=900)
    checkpoint_ts = datetime(2026, 4, 10, 13, 0, tzinfo=UTC)

    future_asof, future_reason = HistoricalTrainingService._checkpoint_market_snapshot_asof(
        service,
        {"updated_time": "2026-04-10T13:01:00+00:00"},
        checkpoint_ts=checkpoint_ts,
    )
    stale_asof, stale_reason = HistoricalTrainingService._checkpoint_market_snapshot_asof(
        service,
        {"updated_time": "2026-04-10T12:30:00+00:00"},
        checkpoint_ts=checkpoint_ts,
    )
    fresh_asof, fresh_reason = HistoricalTrainingService._checkpoint_market_snapshot_asof(
        service,
        {"updated_time": "2026-04-10T12:55:00+00:00"},
        checkpoint_ts=checkpoint_ts,
    )

    assert future_asof is None
    assert future_reason == "market_snapshot_future"
    assert stale_asof is None
    assert stale_reason == "market_snapshot_stale"
    assert fresh_asof == datetime(2026, 4, 10, 12, 55, tzinfo=UTC)
    assert fresh_reason is None

    assert HistoricalTrainingService._checkpoint_archive_metadata_valid(
        {
            "observation_ts": datetime(2026, 4, 10, 12, 55, tzinfo=UTC),
            "forecast_updated_ts": datetime(2026, 4, 10, 12, 40, tzinfo=UTC),
            "asof_ts": datetime(2026, 4, 10, 12, 55, tzinfo=UTC),
        },
        checkpoint_ts,
    ) is True
    assert HistoricalTrainingService._checkpoint_archive_metadata_valid(
        {
            "observation_ts": datetime(2026, 4, 10, 13, 1, tzinfo=UTC),
            "forecast_updated_ts": datetime(2026, 4, 10, 12, 40, tzinfo=UTC),
            "asof_ts": datetime(2026, 4, 10, 13, 1, tzinfo=UTC),
        },
        checkpoint_ts,
    ) is False


def test_select_weather_snapshot_prefers_checkpoint_archives() -> None:
    service = object.__new__(HistoricalTrainingService)

    class _Repo:
        async def get_historical_checkpoint_archive(self, **kwargs):
            return SimpleNamespace(
                source_id="checkpoint",
                payload={
                    "weather_source_kind": HistoricalTrainingService.CHECKPOINT_CAPTURED_WEATHER_SOURCE,
                    "weather_source_id": "checkpoint",
                },
            )

        async def get_historical_weather_snapshot_by_source(self, **kwargs):
            return SimpleNamespace(
                source_kind=HistoricalTrainingService.CHECKPOINT_CAPTURED_WEATHER_SOURCE,
                source_id="checkpoint",
            )

        async def list_historical_weather_snapshots(self, **kwargs):
            return [
                SimpleNamespace(source_kind=HistoricalTrainingService.CAPTURED_WEATHER_SOURCE, source_id="captured"),
                SimpleNamespace(source_kind=HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE, source_id="archived"),
                SimpleNamespace(source_kind=HistoricalTrainingService.CHECKPOINT_CAPTURED_WEATHER_SOURCE, source_id="checkpoint"),
            ]

    result = asyncio.run(
        HistoricalTrainingService._select_weather_snapshot(
            service,
            _Repo(),
            station_id="KNYC",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-10",
            checkpoint_label="open_0900",
            checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert result.source_kind == HistoricalTrainingService.CHECKPOINT_CAPTURED_WEATHER_SOURCE


def test_select_weather_snapshot_falls_back_to_asof_sources_when_archive_missing() -> None:
    service = object.__new__(HistoricalTrainingService)

    class _Repo:
        async def get_historical_checkpoint_archive(self, **kwargs):
            return None

        async def get_historical_weather_snapshot_by_source(self, **kwargs):
            return None

        async def list_historical_weather_snapshots(self, **kwargs):
            return [
                SimpleNamespace(source_kind=HistoricalTrainingService.CAPTURED_WEATHER_SOURCE, source_id="captured"),
                SimpleNamespace(source_kind=HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE, source_id="archived"),
            ]

    result = asyncio.run(
        HistoricalTrainingService._select_weather_snapshot(
            service,
            _Repo(),
            station_id="KNYC",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-10",
            checkpoint_label="open_0900",
            checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert result.source_kind == HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE


def test_select_weather_snapshot_uses_external_archive_when_native_sources_absent() -> None:
    service = object.__new__(HistoricalTrainingService)

    class _Repo:
        async def get_historical_checkpoint_archive(self, **kwargs):
            return None

        async def get_historical_weather_snapshot_by_source(self, **kwargs):
            return None

        async def list_historical_weather_snapshots(self, **kwargs):
            return [
                SimpleNamespace(
                    source_kind=HistoricalTrainingService.EXTERNAL_FORECAST_ARCHIVE_SOURCE,
                    source_id="external",
                )
            ]

    result = asyncio.run(
        HistoricalTrainingService._select_weather_snapshot(
            service,
            _Repo(),
            station_id="KNYC",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-10",
            checkpoint_label="open_0900",
            checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert result.source_kind == HistoricalTrainingService.EXTERNAL_FORECAST_ARCHIVE_SOURCE


def test_coverage_backlog_distinguishes_promotable_and_permanent_outcome_only() -> None:
    service = object.__new__(HistoricalTrainingService)
    coverage_rows = [
        {
            "market_ticker": "KXHIGHNY-26APR10-T80",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-10",
            "coverage_class": HistoricalTrainingService.COVERAGE_PARTIAL,
            "checkpoints": [
                {
                    "market_snapshot_id": "market-1",
                    "market_source_kind": "captured_market_snapshot",
                    "weather_snapshot_id": None,
                    "weather_source_kind": None,
                    "missing_reasons": ["weather_snapshot_missing"],
                }
            ],
        },
        {
            "market_ticker": "KXHIGHNY-26APR11-T80",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_OUTCOME_ONLY,
            "checkpoints": [
                {
                    "market_snapshot_id": None,
                    "market_source_kind": None,
                    "weather_snapshot_id": None,
                    "weather_source_kind": None,
                    "missing_reasons": ["market_snapshot_missing", "weather_snapshot_missing"],
                }
            ],
        },
    ]
    checkpoint_rows = [
        {
            "market_ticker": "KXHIGHNY-26APR10-T80",
            "local_market_day": "2026-04-10",
            "checkpoints": [{"captured": False}],
        },
        {
            "market_ticker": "KXHIGHNY-26APR11-T80",
            "local_market_day": "2026-04-11",
            "checkpoints": [{"captured": False}],
        },
    ]
    settlement_labels = [
        SimpleNamespace(
            market_ticker="KXHIGHNY-26APR10-T80",
            local_market_day="2026-04-10",
            crosscheck_status=HistoricalTrainingService.SETTLEMENT_MATCH,
        ),
        SimpleNamespace(
            market_ticker="KXHIGHNY-26APR11-T80",
            local_market_day="2026-04-11",
            crosscheck_status=HistoricalTrainingService.SETTLEMENT_MISSING,
        ),
    ]

    backlog = HistoricalTrainingService._coverage_backlog(
        service,
        coverage_rows=coverage_rows,
        checkpoint_rows=checkpoint_rows,
        settlement_labels=settlement_labels,
        verbose=True,
    )

    assert backlog["reason_counts"]["weather_snapshot_missing"] == 2
    assert backlog["market_day_reason_counts"]["settlement_crosscheck_missing"] == 1
    assert backlog["promotable_market_day_counts"]["promotable_to_full_checkpoint_coverage"] == 1
    assert backlog["promotable_market_day_counts"]["permanently_outcome_only_with_current_sources"] == 1


def test_coverage_repair_summary_counts_promoted_and_recoverable_gaps() -> None:
    service = object.__new__(HistoricalTrainingService)
    backlog = {
        "all_samples": [
            {
                "promotable_status": "promotable_to_full_checkpoint_coverage",
                "day_reasons": ["weather_snapshot_missing", "checkpoint_archive_missing"],
            },
            {
                "promotable_status": "permanently_outcome_only_with_current_sources",
                "day_reasons": ["weather_snapshot_missing", "checkpoint_archive_missing"],
            },
        ],
        "promotable_market_day_counts": {
            "promotable_to_full_checkpoint_coverage": 1,
            "promotable_to_partial_or_late_only": 2,
            "permanently_outcome_only_with_current_sources": 1,
        },
    }
    checkpoint_rows = [
        {
            "checkpoints": [
                {"source_kind": HistoricalTrainingService.PROMOTED_CHECKPOINT_ARCHIVE_SOURCE},
                {"source_kind": "manual_checkpoint_capture_once"},
            ]
        }
    ]

    summary = HistoricalTrainingService._coverage_repair_summary(
        service,
        coverage_backlog=backlog,
        checkpoint_rows=checkpoint_rows,
    )

    assert summary["checkpoint_archive_promotion_count"] == 1
    assert summary["recoverable_market_day_count"] == 3
    assert summary["permanent_outcome_only_market_day_count"] == 1
    assert summary["recoverable_weather_gap_market_day_count"] == 1
    assert summary["permanent_weather_gap_market_day_count"] == 1


def test_confidence_progress_exposes_support_blockers() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(
        historical_execution_confidence_min_market_days=60,
        historical_directional_confidence_min_full_market_days=30,
        historical_directional_confidence_min_holdout_market_days=7,
    )

    progress = HistoricalTrainingService._confidence_progress(
        service,
        {
            "confidence_state": "insufficient_support",
            "distinct_execution_market_days": 12,
            "distinct_full_market_days": 2,
            "full_coverage_holdout_market_days": 1,
            "execution_confidence_threshold_market_days": 60,
            "directional_confidence_threshold_market_days": 30,
            "directional_confidence_threshold_holdout_market_days": 7,
        },
    )

    assert progress["execution_support"]["remaining"] == 48
    assert progress["directional_support"]["remaining"] == 28
    assert progress["holdout_support"]["remaining"] == 6
    assert progress["promotion_blockers"] == [
        "lack_of_execution_support",
        "lack_of_full_coverage_support",
        "lack_of_holdout_support",
    ]


def test_select_market_snapshot_rejects_stale_captured_and_uses_fresh_reconstructed() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(risk_stale_market_seconds=300, historical_replay_market_stale_seconds=900)
    checkpoint_ts = datetime(2026, 4, 10, 21, 0, tzinfo=UTC)

    stale_captured = SimpleNamespace(
        asof_ts=datetime(2026, 4, 9, 18, 33, tzinfo=UTC),
        source_kind=HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
    )
    fresh_reconstructed = SimpleNamespace(
        asof_ts=datetime(2026, 4, 10, 20, 59, tzinfo=UTC),
        source_kind=HistoricalTrainingService.RECONSTRUCTED_MARKET_SOURCE,
    )

    class _Repo:
        async def get_latest_historical_market_snapshot(self, **kwargs):
            if kwargs.get("source_kind") == HistoricalTrainingService.CAPTURED_MARKET_SOURCE:
                return stale_captured
            if kwargs.get("source_kind") == HistoricalTrainingService.RECONSTRUCTED_MARKET_SOURCE:
                return fresh_reconstructed
            return None

    snapshot, reason = asyncio.run(
        HistoricalTrainingService._select_market_snapshot(
            service,
            _Repo(),
            market_ticker="KXHIGHMIA-26APR10-T76",
            local_market_day="2026-04-10",
            checkpoint_ts=checkpoint_ts,
        )
    )

    assert snapshot is fresh_reconstructed
    assert reason is None


def test_select_market_snapshot_prefers_exact_checkpoint_capture() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(risk_stale_market_seconds=300, historical_replay_market_stale_seconds=900)
    checkpoint_ts = datetime(2026, 4, 10, 21, 0, tzinfo=UTC)

    checkpoint_captured = SimpleNamespace(
        asof_ts=datetime(2026, 4, 10, 20, 58, tzinfo=UTC),
        source_kind=HistoricalTrainingService.CHECKPOINT_CAPTURED_MARKET_SOURCE,
    )
    captured = SimpleNamespace(
        asof_ts=datetime(2026, 4, 10, 20, 57, tzinfo=UTC),
        source_kind=HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
    )
    reconstructed = SimpleNamespace(
        asof_ts=datetime(2026, 4, 10, 20, 56, tzinfo=UTC),
        source_kind=HistoricalTrainingService.RECONSTRUCTED_MARKET_SOURCE,
    )

    class _Repo:
        async def get_latest_historical_market_snapshot(self, **kwargs):
            if kwargs.get("source_kind") == HistoricalTrainingService.CHECKPOINT_CAPTURED_MARKET_SOURCE:
                return checkpoint_captured
            if kwargs.get("source_kind") == HistoricalTrainingService.CAPTURED_MARKET_SOURCE:
                return captured
            if kwargs.get("source_kind") == HistoricalTrainingService.RECONSTRUCTED_MARKET_SOURCE:
                return reconstructed
            return None

    snapshot, reason = asyncio.run(
        HistoricalTrainingService._select_market_snapshot(
            service,
            _Repo(),
            market_ticker="KXHIGHMIA-26APR10-T76",
            local_market_day="2026-04-10",
            checkpoint_ts=checkpoint_ts,
        )
    )

    assert snapshot is checkpoint_captured
    assert reason is None


def test_reconstruct_market_checkpoint_prefers_one_minute_candlesticks(monkeypatch) -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(
        historical_replay_market_snapshot_lookback_hours=36,
        historical_replay_market_stale_seconds=900,
    )
    calls: list[dict[str, object]] = []

    class _FakeRepo:
        def __init__(self, session):
            self.session = session

        async def upsert_historical_market_snapshot(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(source_id=kwargs["source_id"], source_kind=kwargs["source_kind"])

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr("kalshi_bot.services.historical_training.PlatformRepository", _FakeRepo)
    service.session_factory = lambda: _FakeSession()
    service.kalshi = _DummyKalshi(
        candlestick_responses=[
            {
                "candlesticks": [
                    {
                        "end_period_ts": int(datetime(2026, 4, 10, 20, 59, tzinfo=UTC).timestamp()),
                        "yes_bid": {"close_dollars": "0.5100"},
                        "yes_ask": {"close_dollars": "0.5500"},
                        "price": {"close_dollars": "0.5300"},
                    }
                ]
            }
        ]
    )
    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHNY-26APR10-T68",
        market_type="weather",
        station_id="KNYC",
        location_name="New York City",
        latitude=40.7146,
        longitude=-74.0071,
        threshold_f=68,
        operator=">",
        settlement_source="NWS daily summary",
        series_ticker="KXHIGHNY",
    )
    settlement_label = SimpleNamespace(
        market_ticker="KXHIGHNY-26APR10-T68",
        local_market_day="2026-04-10",
        payload={"market": {"ticker": "KXHIGHNY-26APR10-T68", "close_time": "2026-04-10T23:59:59+00:00"}},
    )

    result = asyncio.run(
        HistoricalTrainingService._reconstruct_market_checkpoint(
            service,
            mapping=mapping,
            settlement_label=settlement_label,
            checkpoint_label="late_1700",
            checkpoint_ts=datetime(2026, 4, 10, 21, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert service.kalshi.calls[0]["period_interval"] == 1
    assert calls[0]["asof_ts"] == datetime(2026, 4, 10, 20, 59, tzinfo=UTC)


def test_reconstruct_market_checkpoint_falls_back_to_hourly_when_one_minute_unavailable(monkeypatch) -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(
        historical_replay_market_snapshot_lookback_hours=36,
        historical_replay_market_stale_seconds=900,
    )
    calls: list[dict[str, object]] = []

    class _FakeRepo:
        def __init__(self, session):
            self.session = session

        async def upsert_historical_market_snapshot(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(source_id=kwargs["source_id"], source_kind=kwargs["source_kind"])

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            return None

    request = httpx.Request("GET", "https://kalshi.test/candlesticks")
    response = httpx.Response(400, request=request)
    monkeypatch.setattr("kalshi_bot.services.historical_training.PlatformRepository", _FakeRepo)
    service.session_factory = lambda: _FakeSession()
    service.kalshi = _DummyKalshi(
        candlestick_responses=[
            httpx.HTTPStatusError("bad request", request=request, response=response),
            {
                "candlesticks": [
                    {
                        "end_period_ts": int(datetime(2026, 4, 10, 20, 55, tzinfo=UTC).timestamp()),
                        "yes_bid": {"close_dollars": "0.4900"},
                        "yes_ask": {"close_dollars": "0.5300"},
                        "price": {"close_dollars": "0.5100"},
                    }
                ]
            },
        ]
    )
    mapping = WeatherMarketMapping(
        market_ticker="KXHIGHNY-26APR10-T68",
        market_type="weather",
        station_id="KNYC",
        location_name="New York City",
        latitude=40.7146,
        longitude=-74.0071,
        threshold_f=68,
        operator=">",
        settlement_source="NWS daily summary",
        series_ticker="KXHIGHNY",
    )
    settlement_label = SimpleNamespace(
        market_ticker="KXHIGHNY-26APR10-T68",
        local_market_day="2026-04-10",
        payload={"market": {"ticker": "KXHIGHNY-26APR10-T68", "close_time": "2026-04-10T23:59:59+00:00"}},
    )

    result = asyncio.run(
        HistoricalTrainingService._reconstruct_market_checkpoint(
            service,
            mapping=mapping,
            settlement_label=settlement_label,
            checkpoint_label="late_1700",
            checkpoint_ts=datetime(2026, 4, 10, 21, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert [call["period_interval"] for call in service.kalshi.calls] == [1, 60]
    assert calls[0]["asof_ts"] == datetime(2026, 4, 10, 20, 55, tzinfo=UTC)


def test_gemini_build_readiness_requires_multiple_market_days_and_splits() -> None:
    service = object.__new__(HistoricalTrainingService)
    split = HistoricalBuildSplit(train=["room-a"], validation=[], holdout=[])
    bundles = [
        SimpleNamespace(historical_provenance={"local_market_day": "2026-04-10"}),
        SimpleNamespace(historical_provenance={"local_market_day": "2026-04-10"}),
    ]

    training_ready, draft_only = HistoricalTrainingService._build_training_readiness(
        service,
        bundles,
        split=split,
        mode="gemini-finetune",
    )

    assert training_ready is False
    assert draft_only is True


def test_confidence_story_reports_execution_only_and_directional_states() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.settings = SimpleNamespace(
        historical_execution_confidence_min_market_days=60,
        historical_directional_confidence_min_full_market_days=30,
        historical_directional_confidence_min_holdout_market_days=7,
    )

    insufficient = HistoricalTrainingService._confidence_story(
        service,
        latest_run_payload=None,
        historical_build_readiness={
            "distinct_full_coverage_market_days": 2,
            "holdout_full_coverage_market_days": 0,
        },
        source_replay_coverage={
            "full_checkpoint_coverage_count": 2,
            "late_only_coverage_count": 10,
            "partial_checkpoint_coverage_count": 5,
            "outcome_only_coverage_count": 20,
            "no_replayable_coverage_count": 0,
        },
    )
    execution_only = HistoricalTrainingService._confidence_story(
        service,
        latest_run_payload=None,
        historical_build_readiness={
            "distinct_full_coverage_market_days": 12,
            "holdout_full_coverage_market_days": 3,
        },
        source_replay_coverage={
            "full_checkpoint_coverage_count": 12,
            "late_only_coverage_count": 30,
            "partial_checkpoint_coverage_count": 20,
            "outcome_only_coverage_count": 5,
            "no_replayable_coverage_count": 0,
        },
    )
    directional = HistoricalTrainingService._confidence_story(
        service,
        latest_run_payload=None,
        historical_build_readiness={
            "distinct_full_coverage_market_days": 35,
            "holdout_full_coverage_market_days": 8,
        },
        source_replay_coverage={
            "full_checkpoint_coverage_count": 35,
            "late_only_coverage_count": 20,
            "partial_checkpoint_coverage_count": 10,
            "outcome_only_coverage_count": 4,
            "no_replayable_coverage_count": 0,
        },
    )

    assert insufficient["confidence_state"] == "insufficient_support"
    assert execution_only["confidence_state"] == "execution_confident_only"
    assert directional["confidence_state"] == "directional_confident"


def test_gemini_export_manifest_contains_boundaries_and_audit_stats(tmp_path) -> None:
    service = object.__new__(HistoricalTrainingService)
    output_dir = tmp_path / "gemini"
    split = HistoricalBuildSplit(train=["room-a"], validation=["room-b"], holdout=[])
    records = [
        {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "User"},
                {"role": "assistant", "content": "Assistant"},
            ],
            "metadata": {"split": "train", "room_id": "room-a"},
        },
        {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "User"},
                {"role": "assistant", "content": "Assistant"},
            ],
            "metadata": {"split": "validation", "room_id": "room-b"},
        },
    ]
    bundles = [
        SimpleNamespace(
            historical_provenance={"local_market_day": "2026-04-10"},
            replay_checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
            room={"agent_pack_version": "builtin-gemini-v1"},
            market_source_kind="checkpoint_captured_market_snapshot",
            weather_source_kind="external_forecast_archive_weather_bundle",
            exclude_reason=None,
            audit_source="historical_replay",
            settlement_label={"crosscheck_status": "match"},
            trainable_default=True,
        ),
        SimpleNamespace(
            historical_provenance={"local_market_day": "2026-04-11"},
            replay_checkpoint_ts=datetime(2026, 4, 11, 13, 0, tzinfo=UTC),
            room={"agent_pack_version": "builtin-gemini-v1"},
            market_source_kind="checkpoint_captured_market_snapshot",
            weather_source_kind="archived_weather_bundle",
            exclude_reason="stale_data_mismatch",
            audit_source="historical_replay",
            settlement_label={"crosscheck_status": "mismatch"},
            trainable_default=False,
        ),
    ]

    paths = HistoricalTrainingService._write_gemini_export(
        service,
        str(output_dir),
        records,
        bundles=bundles,
        split=split,
        draft_only=True,
        training_ready=False,
    )

    assert paths is not None
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["format_target"] == "gemini_vertex_chat_jsonl"
    assert manifest["draft_only"] is True
    assert manifest["training_ready"] is False
    assert manifest["split_boundaries"]["train_room_ids"] == ["room-a"]
    assert manifest["audit_stats"]["settlement_mismatch_count"] == 1
    assert manifest["audit_stats"]["exclusion_counts"]["stale_data_mismatch"] == 1
    assert manifest["audit_stats"]["weather_source_kind_counts"]["external_forecast_archive_weather_bundle"] == 1
    assert manifest["audit_stats"]["external_archive_weather_count"] == 1
    assert manifest["source_windows"]["local_market_day_start"] == "2026-04-10"


def test_replay_audit_detects_missing_stale_and_orphan_replays() -> None:
    service = object.__new__(HistoricalTrainingService)
    coverage_rows = [
        {
            "market_ticker": "KXHIGHNY-26APR11-T61",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
            "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MATCH,
            "settlement_mismatch_reason": None,
            "settlement_label_signature": '{"crosscheck_high_f":"81.00","crosscheck_result":"yes","crosscheck_status":"match","kalshi_result":"yes","mismatch_reason":null,"settlement_value_dollars":"1.0000"}',
            "checkpoints": [
                {
                    "checkpoint_label": "open_0900",
                    "checkpoint_ts": "2026-04-11T13:00:00+00:00",
                    "replayable": True,
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_id": "market-source-1",
                    "weather_snapshot_id": "weather-source-1",
                    "missing_reasons": [],
                }
            ],
        },
        {
            "market_ticker": "KXHIGHNY-26APR11-T68",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
            "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MATCH,
            "settlement_mismatch_reason": None,
            "settlement_label_signature": '{"crosscheck_high_f":"82.00","crosscheck_result":"yes","crosscheck_status":"match","kalshi_result":"yes","mismatch_reason":null,"settlement_value_dollars":"1.0000"}',
            "checkpoints": [
                {
                    "checkpoint_label": "midday_1300",
                    "checkpoint_ts": "2026-04-11T17:00:00+00:00",
                    "replayable": True,
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_id": "market-source-2",
                    "weather_snapshot_id": "weather-source-2",
                    "missing_reasons": [],
                }
            ],
        },
        {
            "market_ticker": "KXHIGHNY-26APR11-T75",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_NONE,
            "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MISSING,
            "settlement_mismatch_reason": HistoricalTrainingService.SETTLEMENT_MISMATCH_REASON_MISSING,
            "settlement_label_signature": '{"crosscheck_high_f":null,"crosscheck_result":null,"crosscheck_status":"missing","kalshi_result":"no","mismatch_reason":"crosscheck_missing","settlement_value_dollars":"0.0000"}',
            "checkpoints": [
                {
                    "checkpoint_label": "late_1700",
                    "checkpoint_ts": "2026-04-11T21:00:00+00:00",
                    "replayable": False,
                    "market_source_kind": None,
                    "weather_source_kind": None,
                    "market_snapshot_id": None,
                    "weather_snapshot_id": None,
                    "missing_reasons": ["weather_snapshot_missing"],
                }
            ],
        },
    ]
    replay_runs = [
        SimpleNamespace(
            id="run-stale",
            room_id="room-stale",
            status="completed",
            market_ticker="KXHIGHNY-26APR11-T61",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-11",
            checkpoint_label="open_0900",
            checkpoint_ts=datetime(2026, 4, 11, 13, 0, tzinfo=UTC),
            payload={
                "historical_provenance": {
                    "coverage_class": HistoricalTrainingService.COVERAGE_LATE_ONLY,
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_source_id": "stale-market-source",
                    "weather_snapshot_source_id": "weather-source-1",
                    "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MATCH,
                    "settlement_mismatch_reason": None,
                    "settlement_label_signature": '{"crosscheck_high_f":"81.00","crosscheck_result":"yes","crosscheck_status":"match","kalshi_result":"yes","mismatch_reason":null,"settlement_value_dollars":"1.0000"}',
                }
            },
        ),
        SimpleNamespace(
            id="run-orphan",
            room_id="room-orphan",
            status="completed",
            market_ticker="KXHIGHNY-26APR11-T75",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-11",
            checkpoint_label="late_1700",
            checkpoint_ts=datetime(2026, 4, 11, 21, 0, tzinfo=UTC),
            payload={
                "historical_provenance": {
                    "coverage_class": HistoricalTrainingService.COVERAGE_LATE_ONLY,
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_source_id": "market-source-orphan",
                    "weather_snapshot_source_id": "weather-source-orphan",
                    "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MISSING,
                    "settlement_mismatch_reason": HistoricalTrainingService.SETTLEMENT_MISMATCH_REASON_MISSING,
                    "settlement_label_signature": '{"crosscheck_high_f":null,"crosscheck_result":null,"crosscheck_status":"missing","kalshi_result":"no","mismatch_reason":"crosscheck_missing","settlement_value_dollars":"0.0000"}',
                }
            },
        ),
    ]

    audit = HistoricalTrainingService._build_replay_audit(
        service,
        coverage_rows=coverage_rows,
        replay_runs=replay_runs,
        verbose=True,
    )

    assert audit["refresh_needed"] is True
    assert audit["issue_counts"]["stale_replay"] == 1
    assert audit["issue_counts"]["missing_replay"] == 1
    assert audit["issue_counts"]["orphan_replay"] == 1
    assert audit["refresh_counts_by_cause"]["coverage_repair"] == 3
    assert sorted(audit["affected_room_ids"]) == ["room-orphan", "room-stale"]


def test_json_safe_serializes_decimal_values() -> None:
    payload = {"crosscheck": {"daily_high_f": Decimal("82.00")}}

    assert _json_safe(payload) == {"crosscheck": {"daily_high_f": "82.00"}}


def test_replay_audit_flags_logic_version_mismatch_as_stale() -> None:
    service = object.__new__(HistoricalTrainingService)
    coverage_rows = [
        {
            "market_ticker": "KXHIGHNY-26APR11-T61",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
            "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MATCH,
            "settlement_mismatch_reason": None,
            "settlement_label_signature": '{"crosscheck_high_f":"81.00","crosscheck_result":"yes","crosscheck_status":"match","kalshi_result":"yes","mismatch_reason":null,"settlement_value_dollars":"1.0000"}',
            "checkpoints": [
                {
                    "checkpoint_label": "open_0900",
                    "checkpoint_ts": "2026-04-11T13:00:00+00:00",
                    "replayable": True,
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_id": "market-source-1",
                    "weather_snapshot_id": "weather-source-1",
                    "missing_reasons": [],
                }
            ],
        }
    ]
    replay_runs = [
        SimpleNamespace(
            id="run-logic",
            room_id="room-logic",
            status="completed",
            market_ticker="KXHIGHNY-26APR11-T61",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-11",
            checkpoint_label="open_0900",
            checkpoint_ts=datetime(2026, 4, 11, 13, 0, tzinfo=UTC),
            payload={
                "historical_provenance": {
                    "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
                    "replay_logic_version": "historical_replay_old_logic",
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_source_id": "market-source-1",
                    "weather_snapshot_source_id": "weather-source-1",
                    "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MATCH,
                    "settlement_mismatch_reason": None,
                    "settlement_label_signature": '{"crosscheck_high_f":"81.00","crosscheck_result":"yes","crosscheck_status":"match","kalshi_result":"yes","mismatch_reason":null,"settlement_value_dollars":"1.0000"}',
                }
            },
        )
    ]

    audit = HistoricalTrainingService._build_replay_audit(
        service,
        coverage_rows=coverage_rows,
        replay_runs=replay_runs,
        verbose=True,
    )

    assert audit["refresh_needed"] is True
    assert audit["issue_counts"]["stale_replay"] == 1
    assert audit["issues"][0]["reasons"] == ["replay_logic_version_changed"]
    assert audit["refresh_counts_by_cause"]["replay_logic_change"] == 1


def test_replay_audit_flags_settlement_signature_change_as_settlement_repair() -> None:
    service = object.__new__(HistoricalTrainingService)
    coverage_rows = [
        {
            "market_ticker": "KXHIGHNY-26APR11-T61",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
            "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MATCH,
            "settlement_mismatch_reason": None,
            "settlement_label_signature": "new-signature",
            "checkpoints": [
                {
                    "checkpoint_label": "open_0900",
                    "checkpoint_ts": "2026-04-11T13:00:00+00:00",
                    "replayable": True,
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_id": "market-source-1",
                    "weather_snapshot_id": "weather-source-1",
                    "missing_reasons": [],
                }
            ],
        }
    ]
    replay_runs = [
        SimpleNamespace(
            id="run-settlement",
            room_id="room-settlement",
            status="completed",
            market_ticker="KXHIGHNY-26APR11-T61",
            series_ticker="KXHIGHNY",
            local_market_day="2026-04-11",
            checkpoint_label="open_0900",
            checkpoint_ts=datetime(2026, 4, 11, 13, 0, tzinfo=UTC),
            payload={
                "historical_provenance": {
                    "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
                    "replay_logic_version": HistoricalTrainingService.replay_logic_version(),
                    "market_source_kind": HistoricalTrainingService.CAPTURED_MARKET_SOURCE,
                    "weather_source_kind": HistoricalTrainingService.ARCHIVED_WEATHER_SOURCE,
                    "market_snapshot_source_id": "market-source-1",
                    "weather_snapshot_source_id": "weather-source-1",
                    "settlement_crosscheck_status": HistoricalTrainingService.SETTLEMENT_MISMATCH,
                    "settlement_mismatch_reason": HistoricalTrainingService.SETTLEMENT_MISMATCH_REASON_THRESHOLD_EDGE,
                    "settlement_label_signature": "old-signature",
                }
            },
        )
    ]

    audit = HistoricalTrainingService._build_replay_audit(
        service,
        coverage_rows=coverage_rows,
        replay_runs=replay_runs,
        verbose=True,
    )

    assert audit["refresh_needed"] is True
    assert audit["issue_counts"]["stale_replay"] == 1
    assert "settlement_label_signature_changed" in audit["issues"][0]["reasons"]
    assert audit["refresh_counts_by_cause"]["settlement_repair"] == 1


@pytest.mark.asyncio
async def test_list_recent_markets_filters_closed_markets_without_invalid_status_param() -> None:
    service = object.__new__(HistoricalTrainingService)
    service.kalshi = _DummyKalshi(
        [
            {
                "markets": [
                    {"ticker": "KXHIGHNY-26APR10-T70", "status": "open", "result": ""},
                    {"ticker": "KXHIGHNY-26APR10-T71", "status": "closed", "result": ""},
                    {"ticker": "KXHIGHNY-26APR10-T72", "status": "active", "result": "yes"},
                ],
                "cursor": None,
            }
        ]
    )
    service.settings = SimpleNamespace(historical_import_page_size=500)

    template = WeatherSeriesTemplate(
        series_ticker="KXHIGHNY",
        station_id="KNYC",
        daily_summary_station_id="USW00094728",
        location_name="New York City",
        timezone_name="America/New_York",
        latitude=40.7146,
        longitude=-74.0071,
        metric="daily_high_temp_f",
        settlement_source="NWS daily summary",
    )

    markets = await HistoricalTrainingService._list_recent_markets(
        service,
        template,
        date_from=datetime(2026, 4, 10, tzinfo=UTC).date(),
        date_to=datetime(2026, 4, 10, tzinfo=UTC).date(),
    )

    assert [market["ticker"] for market in markets] == [
        "KXHIGHNY-26APR10-T71",
        "KXHIGHNY-26APR10-T72",
    ]
    assert "status" not in service.kalshi.calls[0]
