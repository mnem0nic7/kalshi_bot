from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.integrations.forecast_archive import OpenMeteoForecastArchiveClient
from kalshi_bot.weather.models import WeatherMarketMapping


def _mapping() -> WeatherMarketMapping:
    return WeatherMarketMapping(
        market_ticker="KXHIGHNY-26APR10-T68",
        market_type="weather",
        station_id="KNYC",
        location_name="New York City",
        timezone_name="America/New_York",
        latitude=40.7146,
        longitude=-74.0071,
        threshold_f=68,
        operator=">",
        settlement_source="NWS daily summary",
        series_ticker="KXHIGHNY",
    )


def test_open_meteo_archive_normalizes_point_in_time_bundle() -> None:
    client = object.__new__(OpenMeteoForecastArchiveClient)
    client.settings = Settings()

    snapshot = OpenMeteoForecastArchiveClient._normalize_snapshot(
        client,
        _mapping(),
        payload={
            "timezone": "America/New_York",
            "hourly": {
                "time": [
                    "2026-04-10T09:00",
                    "2026-04-10T12:00",
                    "2026-04-10T15:00",
                ],
                "temperature_2m": [69.0, 77.0, 81.0],
            },
        },
        local_market_day="2026-04-10",
        checkpoint_ts=datetime(2026, 4, 10, 17, 0, tzinfo=UTC),
        checkpoint_label="checkpoint_1",
        model="gfs_seamless",
        run_ts=datetime(2026, 4, 10, 16, 0, tzinfo=UTC),
    )

    assert snapshot is not None
    assert str(snapshot.forecast_high_f) == "81.00"
    assert snapshot.current_temp_f is None
    assert snapshot.payload["forecast"]["properties"]["updated"] == "2026-04-10T16:00:00+00:00"
    assert snapshot.payload["_external_archive"]["model"] == "gfs_seamless"
    assert "checkpoint_1" in snapshot.source_id


def test_open_meteo_archive_rejects_future_runs() -> None:
    client = object.__new__(OpenMeteoForecastArchiveClient)
    client.settings = Settings()

    snapshot = OpenMeteoForecastArchiveClient._normalize_snapshot(
        client,
        _mapping(),
        payload={
            "timezone": "America/New_York",
            "hourly": {
                "time": ["2026-04-10T12:00"],
                "temperature_2m": [77.0],
            },
        },
        local_market_day="2026-04-10",
        checkpoint_ts=datetime(2026, 4, 10, 17, 0, tzinfo=UTC),
        checkpoint_label="checkpoint_1",
        model="gfs_seamless",
        run_ts=datetime(2026, 4, 10, 18, 0, tzinfo=UTC),
    )

    assert snapshot is None


def test_open_meteo_archive_rejects_missing_temperature_series() -> None:
    client = object.__new__(OpenMeteoForecastArchiveClient)
    client.settings = Settings()

    snapshot = OpenMeteoForecastArchiveClient._normalize_snapshot(
        client,
        _mapping(),
        payload={
            "timezone": "America/New_York",
            "hourly": {
                "time": ["2026-04-10T12:00"],
                "temperature_2m": [],
            },
        },
        local_market_day="2026-04-10",
        checkpoint_ts=datetime(2026, 4, 10, 17, 0, tzinfo=UTC),
        checkpoint_label="checkpoint_1",
        model="gfs_seamless",
        run_ts=datetime(2026, 4, 10, 16, 0, tzinfo=UTC),
    )

    assert snapshot is None


def test_open_meteo_archive_fetch_uses_local_cycle_run_and_forecast_days() -> None:
    captured: dict[str, str] = {}

    async def _run() -> None:
        client = OpenMeteoForecastArchiveClient(Settings(historical_forecast_archive_base_url="https://example.test/v1/forecast"))

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured
            captured = dict(request.url.params)
            return httpx.Response(
                200,
                json={
                    "timezone": "America/New_York",
                    "hourly": {
                        "time": [
                            "2026-04-10T12:00",
                            "2026-04-10T15:00",
                        ],
                        "temperature_2m": [77.0, 81.0],
                    },
                },
            )

        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await client.fetch_point_in_time_forecast_with_diagnostics(
                _mapping(),
                local_market_day="2026-04-10",
                checkpoint_ts=datetime(2026, 4, 10, 17, 0, tzinfo=UTC),
                checkpoint_label="checkpoint_1",
            )
            assert result.snapshot is not None
        finally:
            await client.close()

    asyncio.run(_run())

    assert captured["forecast_days"] == "2"
    assert captured["run"] == "2026-04-10T12:00"
    assert "start_date" not in captured
    assert "end_date" not in captured


def test_open_meteo_archive_fetch_reports_bad_request_reason() -> None:
    async def _run() -> None:
        client = OpenMeteoForecastArchiveClient(Settings(historical_forecast_archive_base_url="https://example.test/v1/forecast"))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": True, "reason": "bad request"})

        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await client.fetch_point_in_time_forecast_with_diagnostics(
                _mapping(),
                local_market_day="2026-04-10",
                checkpoint_ts=datetime(2026, 4, 10, 17, 0, tzinfo=UTC),
                checkpoint_label="checkpoint_1",
            )
        finally:
            await client.close()

        assert result.snapshot is None
        assert result.failure_reason == "request_bad_request"
        assert result.reason_counts["request_bad_request"] >= 1

    asyncio.run(_run())
