from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
import asyncio

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
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def list_markets(self, **params):
        self.calls.append(params)
        return self.responses.pop(0)


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

    assert all_full_days == {"2026-04-10"}
    assert holdout_full_days == {"2026-04-10"}


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
    service.settings = SimpleNamespace(historical_checkpoint_capture_grace_seconds=900)
    checkpoint_ts = datetime(2026, 4, 10, 13, 0, tzinfo=UTC)

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
            local_market_day="2026-04-10",
            checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert result.source_kind == HistoricalTrainingService.CHECKPOINT_CAPTURED_WEATHER_SOURCE


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
            exclude_reason=None,
            audit_source="historical_replay",
            settlement_label={"crosscheck_status": "match"},
            trainable_default=True,
        ),
        SimpleNamespace(
            historical_provenance={"local_market_day": "2026-04-11"},
            replay_checkpoint_ts=datetime(2026, 4, 11, 13, 0, tzinfo=UTC),
            room={"agent_pack_version": "builtin-gemini-v1"},
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
    assert manifest["source_windows"]["local_market_day_start"] == "2026-04-10"


def test_replay_audit_detects_missing_stale_and_orphan_replays() -> None:
    service = object.__new__(HistoricalTrainingService)
    coverage_rows = [
        {
            "market_ticker": "KXHIGHNY-26APR11-T61",
            "series_ticker": "KXHIGHNY",
            "local_market_day": "2026-04-11",
            "coverage_class": HistoricalTrainingService.COVERAGE_FULL,
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
