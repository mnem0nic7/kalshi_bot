"""Iowa Environmental Mesonet (IEM) ASOS archive client — official-station hourly obs.

Weather rework fix #1 (2026-06-22): the backtest showed high-so-far must be sourced
from the station Kalshi settles on (the NWS Daily Climate Report station), not the
Open-Meteo gridpoint. IEM serves historical hourly tmpf (°F) by ASOS station with no
API token. Feed parse output to weather/high_so_far.reconstruct_high_so_far().
"""
from __future__ import annotations

from datetime import date, datetime

import httpx


def parse_asos_csv(text: str) -> list[tuple[datetime, float]]:
    """Parse IEM `format=onlycomma` CSV (station,valid,tmpf) -> [(ts, temp_f)].

    Drops the header, comment (#) lines, and missing ("M"/blank/non-numeric) temps.
    `valid` is "YYYY-MM-DD HH:MM" in the requested timezone (parsed naive).
    """
    rows: list[tuple[datetime, float]] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",")
        if len(parts) < 3 or parts[0] == "station" or parts[1] == "valid":
            continue
        raw_ts, raw_temp = parts[1].strip(), parts[2].strip()
        if raw_temp in ("", "M", "T"):
            continue
        try:
            ts = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M")
            temp = float(raw_temp)
        except ValueError:
            continue
        rows.append((ts, temp))
    return rows


class IemAsosClient:
    BASE_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

    def __init__(self, *, user_agent: str = "kalshi-bot/asos-archive", timeout_seconds: float = 60.0) -> None:
        self.client = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"Accept": "text/plain", "User-Agent": user_agent},
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def fetch_hourly(
        self,
        *,
        station: str,
        start: date,
        end: date,
        timezone: str = "UTC",
    ) -> list[tuple[datetime, float]]:
        """Historical hourly temperature (°F) for an ASOS station, times in `timezone`."""
        response = await self.client.get(
            self.BASE_URL,
            params={
                "station": station,
                "data": "tmpf",
                "year1": start.year, "month1": start.month, "day1": start.day,
                "year2": end.year, "month2": end.month, "day2": end.day,
                "tz": timezone,
                "format": "onlycomma",
                "missing": "M",
                "trace": "T",
            },
        )
        response.raise_for_status()
        return parse_asos_csv(response.text)
