"""Fetch official-station (IEM ASOS) intraday temps for the lock-in harness.

The DB intraday weather snapshots only start the day shadow collection was enabled, but the
settled markets we score are older. ASOS is the official settlement-station archive, so we use it
to reconstruct historical intraday high-so-far. Writes /tmp/wxlockin/obs.csv in the same shape the
harness reads: series_ticker, station_id, local_market_day, obs_ts, current_temp_f.

Usage: python scripts/weather_lockin_fetch_asos.py 2026-06-01 2026-06-22 /tmp/wxlockin
"""
from __future__ import annotations

import asyncio
import csv
import sys
from datetime import date

from kalshi_bot.integrations.asos_archive import IemAsosClient

# series_ticker -> (IEM ASOS station id without leading K, IANA tz)
SERIES = {
    "KXHIGHAUS": ("AUS", "America/Chicago"),
    "KXHIGHCHI": ("MDW", "America/Chicago"),
    "KXHIGHDEN": ("DEN", "America/Denver"),
    "KXHIGHLAX": ("LAX", "America/Los_Angeles"),
    "KXHIGHMIA": ("MIA", "America/New_York"),
    "KXHIGHNY": ("NYC", "America/New_York"),
    "KXHIGHPHIL": ("PHL", "America/New_York"),
    "KXHIGHTATL": ("ATL", "America/New_York"),
    "KXHIGHTBOS": ("BOS", "America/New_York"),
    "KXHIGHTDAL": ("DAL", "America/Chicago"),
    "KXHIGHTDC": ("DCA", "America/New_York"),
    "KXHIGHTHOU": ("HOU", "America/Chicago"),
    "KXHIGHTLV": ("LAS", "America/Los_Angeles"),
    "KXHIGHTMIN": ("MSP", "America/Chicago"),
    "KXHIGHTNOLA": ("MSY", "America/Chicago"),
    "KXHIGHTOKC": ("OKC", "America/Chicago"),
    "KXHIGHTPHX": ("PHX", "America/Phoenix"),
    "KXHIGHTSATX": ("SAT", "America/Chicago"),
    "KXHIGHTSEA": ("SEA", "America/Los_Angeles"),
    "KXHIGHTSFO": ("SFO", "America/Los_Angeles"),
}


async def main(start: str, end: str, base: str) -> None:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    client = IemAsosClient()
    rows = []
    try:
        for i, (series, (station, tz)) in enumerate(SERIES.items()):
            if i > 0:
                await asyncio.sleep(6.0)  # IEM rate limit
            try:
                hourly = await client.fetch_hourly(station=station, start=d0, end=d1, timezone=tz)
            except Exception as exc:  # noqa: BLE001 - log and continue per-station
                print(f"  WARN {series}/{station}: {exc}", file=sys.stderr)
                continue
            for ts, temp in hourly:
                rows.append((series, f"K{station}", ts.date().isoformat(), ts.isoformat(), f"{temp:.2f}"))
            print(f"  {series}/{station}: {len(hourly)} hourly obs", file=sys.stderr)
    finally:
        await client.aclose()

    out = f"{base}/obs.csv"
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["series_ticker", "station_id", "local_market_day", "obs_ts", "current_temp_f"])
        w.writerows(rows)
    print(f"wrote {len(rows)} obs rows -> {out}")


if __name__ == "__main__":
    asyncio.run(main(
        sys.argv[1] if len(sys.argv) > 1 else "2026-06-01",
        sys.argv[2] if len(sys.argv) > 2 else "2026-06-22",
        sys.argv[3] if len(sys.argv) > 3 else "/tmp/wxlockin",
    ))
