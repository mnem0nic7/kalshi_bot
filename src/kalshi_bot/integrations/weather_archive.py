"""Open-Meteo archive-api client: OBSERVED hourly temperature for high-so-far.

Weather rework Stage 0.5 (2026-06-22). Fetches reanalysis/observed hourly
temperature_2m from archive-api.open-meteo.com so observed-high-so-far can be
reconstructed (weather/high_so_far.py) for past market days to train + backtest the
terminal-certainty model. Distinct from OpenMeteoForecastArchiveClient, which hits
the single-runs *forecast* API. See docs/research/2026-06-22-weather-strategy-rework.md.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import httpx


def parse_archive_hourly_payload(payload: Any) -> list[tuple[datetime, float]]:
    """Parse {"hourly": {"time": [...], "temperature_2m": [...]}} into (ts, temp_f).

    Drops samples with a missing temperature. Returns ascending by the input order
    (archive-api returns chronological).
    """
    if not isinstance(payload, dict):
        return []
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return []
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    rows: list[tuple[datetime, float]] = []
    for raw_ts, raw_temp in zip(times, temps):
        if raw_temp is None:
            continue
        try:
            ts = datetime.fromisoformat(str(raw_ts))
            temp = float(raw_temp)
        except (TypeError, ValueError):
            continue
        rows.append((ts, temp))
    return rows


class OpenMeteoArchiveClient:
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, *, user_agent: str = "kalshi-bot/weather-archive", timeout_seconds: float = 30.0) -> None:
        self.client = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"Accept": "application/json", "User-Agent": user_agent},
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def fetch_observed_hourly(
        self,
        *,
        latitude: float,
        longitude: float,
        start: date,
        end: date,
        timezone: str = "UTC",
    ) -> list[tuple[datetime, float]]:
        """Observed hourly temperature (°F) for a station/date range, local to `timezone`."""
        response = await self.client.get(
            self.BASE_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "hourly": "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone": timezone,
            },
        )
        response.raise_for_status()
        return parse_archive_hourly_payload(response.json())
