from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

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


def test_json_safe_serializes_decimal_values() -> None:
    payload = {"crosscheck": {"daily_high_f": Decimal("82.00")}}

    assert _json_safe(payload) == {"crosscheck": {"daily_high_f": "82.00"}}


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
