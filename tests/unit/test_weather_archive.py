"""Open-Meteo ARCHIVE-API observed-hourly fetch (weather rework Stage 0.5 substrate).

Distinct from OpenMeteoForecastArchiveClient (single-runs *forecast* API): this hits
archive-api.open-meteo.com for OBSERVED/reanalysis hourly temperature_2m, from which
observed-high-so-far is reconstructed for past market days to train/backtest the
terminal-certainty model. See docs/research/2026-06-22-weather-strategy-rework.md.
"""
from __future__ import annotations

from datetime import date

import httpx
import pytest

from kalshi_bot.integrations.weather_archive import (
    OpenMeteoArchiveClient,
    parse_archive_hourly_payload,
)

ARCHIVE_FIXTURE = {
    "hourly": {
        "time": ["2026-06-22T06:00", "2026-06-22T07:00", "2026-06-22T08:00"],
        "temperature_2m": [60.0, 62.5, None],
    }
}


def test_parse_archive_hourly_skips_missing_and_pairs_time_temp() -> None:
    rows = parse_archive_hourly_payload(ARCHIVE_FIXTURE)
    assert len(rows) == 2  # the None temperature row is dropped
    (t0, v0), (t1, v1) = rows
    assert (t0.year, t0.month, t0.day, t0.hour) == (2026, 6, 22, 6)
    assert v0 == 60.0
    assert (t1.hour, v1) == (7, 62.5)


def test_parse_archive_hourly_handles_garbage() -> None:
    for bad in ({}, {"hourly": {}}, {"hourly": {"time": [], "temperature_2m": []}}, None):
        assert parse_archive_hourly_payload(bad) == []


@pytest.mark.asyncio
async def test_archive_client_fetch_observed_hourly_no_network() -> None:
    captured: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url)
        return httpx.Response(200, json=ARCHIVE_FIXTURE)

    client = OpenMeteoArchiveClient(user_agent="kalshi-bot-test")
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        rows = await client.fetch_observed_hourly(
            latitude=40.78, longitude=-73.97,
            start=date(2026, 6, 1), end=date(2026, 6, 22),
            timezone="America/New_York",
        )
    finally:
        await client.aclose()

    assert len(rows) == 2
    assert len(captured) == 1
    url = captured[0]
    assert url.host == "archive-api.open-meteo.com"
    assert url.params["hourly"] == "temperature_2m"
    assert url.params["start_date"] == "2026-06-01"
    assert url.params["end_date"] == "2026-06-22"
    assert url.params["temperature_unit"] == "fahrenheit"
    assert str(url.params["latitude"]) == "40.78"
