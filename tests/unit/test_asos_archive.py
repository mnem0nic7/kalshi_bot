"""IEM ASOS archive client — official-station hourly observations (weather rework fix #1).

The backtest showed our high-so-far must come from the station Kalshi settles on (the
NWS Daily Climate Report station, e.g. KNYC/Central Park), NOT the Open-Meteo gridpoint.
The Iowa Environmental Mesonet ASOS archive serves historical hourly tmpf (°F) by station
with no API token. Output feeds reconstruct_high_so_far(). See
docs/research/2026-06-22-weather-strategy-rework.md.
"""
from __future__ import annotations

from datetime import date

import httpx
import pytest

from kalshi_bot.integrations.asos_archive import IemAsosClient, parse_asos_csv

# IEM `format=onlycomma` CSV: station,valid,tmpf  (missing = "M")
ASOS_CSV = (
    "station,valid,tmpf\n"
    "NYC,2026-06-22 06:00,60.80\n"
    "NYC,2026-06-22 07:00,62.50\n"
    "NYC,2026-06-22 08:00,M\n"
    "NYC,2026-06-22 09:00,68.00\n"
)


def test_parse_asos_csv_skips_missing_and_parses_temps() -> None:
    rows = parse_asos_csv(ASOS_CSV)
    assert len(rows) == 3  # the "M" missing row is dropped
    (t0, v0), (t1, v1), (t2, v2) = rows
    assert (t0.hour, v0) == (6, 60.80)
    assert (t1.hour, v1) == (7, 62.50)
    assert (t2.hour, v2) == (9, 68.00)


def test_parse_asos_csv_handles_garbage() -> None:
    assert parse_asos_csv("") == []
    assert parse_asos_csv("station,valid,tmpf\n") == []
    assert parse_asos_csv("garbage\nlines\n") == []


@pytest.mark.asyncio
async def test_iem_asos_client_fetch_hourly_no_network() -> None:
    captured: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url)
        return httpx.Response(200, text=ASOS_CSV)

    client = IemAsosClient()
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        rows = await client.fetch_hourly(
            station="NYC", start=date(2026, 6, 1), end=date(2026, 6, 22), timezone="America/New_York",
        )
    finally:
        await client.aclose()

    assert len(rows) == 3
    assert len(captured) == 1
    url = captured[0]
    assert "mesonet.agron.iastate.edu" in url.host
    assert url.params["station"] == "NYC"
    assert url.params["data"] == "tmpf"
    assert url.params["year1"] == "2026" and url.params["month1"] == "6" and url.params["day1"] == "1"
    assert url.params["year2"] == "2026" and url.params["month2"] == "6" and url.params["day2"] == "22"
