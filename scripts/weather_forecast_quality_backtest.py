"""Weather rework — FORECAST-QUALITY backtest (offline, no market prices).

Validates the terminal-certainty model against actual settlements as a function of
intraday hour, using Open-Meteo observed hourly + the 918 settlement labels. Proves
whether the model is sound and quantifies the high-so-far lock-in (Brier should fall /
sharpness rise through the day). Market-edge-after-fees is NOT tested here (needs live
intraday quotes via shadow — historical weather prices are all post-settlement).

Run on host .venv (light: 1 Open-Meteo archive call per city). Reads settlements.csv
(market_ticker,kalshi_result) dumped via psql. Usage:
  python scripts/weather_forecast_quality_backtest.py /tmp/wxbt/settlements.csv
"""
from __future__ import annotations

import asyncio
import re
import sys
from collections import defaultdict
from datetime import date, datetime

import yaml

from kalshi_bot.integrations.weather_archive import OpenMeteoArchiveClient
from kalshi_bot.weather.high_so_far import (
    fit_remaining_rise_model,
    remaining_rise,
    reconstruct_high_so_far,
    terminal_high_ge_probability,
)
from kalshi_bot.weather.intraday import season_for_month_number

YAML_PATH = "docs/examples/weather_markets.example.yaml"
CHECKPOINT_HOURS = [8, 10, 12, 14, 16, 18]
TICKER_RE = re.compile(r"^KXHIGH([A-Z]+)-(\d{2}[A-Z]{3}\d{2})-T(\d+)")


def load_stations() -> dict[str, dict]:
    doc = yaml.safe_load(open(YAML_PATH))
    out = {}
    for t in doc.get("series_templates", []):
        city = str(t["series_ticker"]).replace("KXHIGH", "")
        out[city] = {"lat": float(t["latitude"]), "lon": float(t["longitude"]), "tz": t["timezone_name"]}
    return out


def parse_ticker(ticker: str):
    m = TICKER_RE.match(ticker)
    if not m:
        return None
    city, dstr, strike = m.group(1), m.group(2), int(m.group(3))
    try:
        d = datetime.strptime(dstr, "%y%b%d").date()
    except ValueError:
        return None
    return city, d, strike


def brier(pairs):  # list[(prob, outcome01)]
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs) if pairs else float("nan")


async def main(settle_csv: str) -> None:
    stations = load_stations()
    # settled markets: city -> list[(date, strike, outcome01)]
    markets = defaultdict(list)
    for line in open(settle_csv):
        parts = line.strip().split(",")
        if len(parts) < 2:
            continue
        parsed = parse_ticker(parts[0])
        if parsed is None or parts[1] not in ("yes", "no"):
            continue
        city, d, strike = parsed
        if city not in stations:
            continue
        markets[city].append((d, strike, 1 if parts[1] == "yes" else 0))

    # fetch observed hourly per city; build per-local-day high-so-far series
    client = OpenMeteoArchiveClient()
    # day -> list[(hour, high_so_far)] per city
    hsf_by_city_day: dict[tuple[str, date], dict[int, float]] = {}
    daily_high_by_city_day: dict[tuple[str, date], float] = {}
    try:
        for city, rows in markets.items():
            days = [d for d, _, _ in rows]
            st = stations[city]
            hourly = await client.fetch_observed_hourly(
                latitude=st["lat"], longitude=st["lon"],
                start=min(days), end=max(days), timezone=st["tz"],
            )
            by_day: dict[date, list[tuple[datetime, float]]] = defaultdict(list)
            for ts, temp in hourly:
                by_day[ts.date()].append((ts, temp))
            for d, samples in by_day.items():
                series = reconstruct_high_so_far(samples)
                hsf_by_city_day[(city, d)] = {ts.hour: v for ts, v in series}
                if series:
                    daily_high_by_city_day[(city, d)] = series[-1][1]
    finally:
        await client.aclose()

    # fit diurnal remaining-rise model from all observed city-days
    fit_samples = []
    for (city, d), hourmap in hsf_by_city_day.items():
        dh = daily_high_by_city_day.get((city, d))
        if dh is None:
            continue
        season = season_for_month_number(d.month)
        for hour, hsf in hourmap.items():
            fit_samples.append({"season": season, "hour": hour, "high_so_far_f": hsf, "daily_high_f": dh})
    model = fit_remaining_rise_model(fit_samples)

    # evaluate terminal probability vs settlement, by intraday checkpoint hour
    by_hour: dict[int, list[tuple[float, int]]] = defaultdict(list)
    base_rate = []
    for city, rows in markets.items():
        for d, strike, outcome in rows:
            hourmap = hsf_by_city_day.get((city, d))
            if not hourmap:
                continue
            base_rate.append(outcome)
            season = season_for_month_number(d.month)
            for h in CHECKPOINT_HOURS:
                # high-so-far at hour h = max temp up to h
                hsf = max([v for hr, v in hourmap.items() if hr <= h], default=None)
                if hsf is None:
                    continue
                mean, sigma = remaining_rise(model, season=season, hour=h)
                p = terminal_high_ge_probability(
                    high_so_far_f=hsf, strike_f=float(strike),
                    remaining_rise_mean_f=mean, remaining_rise_sigma_f=sigma,
                )
                by_hour[h].append((p, outcome))

    n_mkts = len(base_rate)
    br = sum(base_rate) / n_mkts if n_mkts else float("nan")
    # baseline Brier = predicting the base rate for everything
    base_brier = brier([(br, o) for o in base_rate])
    print(f"cities={sorted(markets)} settled_markets={n_mkts} yes_base_rate={br:.3f}")
    print(f"baseline Brier (predict base rate) = {base_brier:.4f}")
    print("intraday terminal-certainty model Brier by checkpoint hour (local):")
    for h in CHECKPOINT_HOURS:
        pairs = by_hour[h]
        if not pairs:
            continue
        # sharpness = mean distance of prob from 0.5 (higher = more confident/locked)
        sharp = sum(abs(p - 0.5) for p, _ in pairs) / len(pairs)
        print(f"  {h:02d}:00  n={len(pairs):4d}  Brier={brier(pairs):.4f}  sharpness={sharp:.3f}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/wxbt/settlements.csv"))
