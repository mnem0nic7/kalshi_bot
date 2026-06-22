"""Weather rework — ORIENTED backtest (both fixes applied).

Uses the real Kalshi contract spec per market (strike_type via floor/cap from
market_specs.csv) + official-station observed high (IEM ASOS, fix #1) + the bracket
probability (fix #2, which handles less/greater/between via floor/cap presence).
This is the corrected go/no-go the two earlier (mis-oriented) runs set up.

  python scripts/weather_oriented_backtest.py /tmp/wxbt/market_specs.csv [n_cities]
"""
from __future__ import annotations

import asyncio
import re
import sys
from collections import defaultdict
from datetime import date, datetime

import yaml

from kalshi_bot.integrations.asos_archive import IemAsosClient
from kalshi_bot.weather.high_so_far import (
    fit_remaining_rise_model,
    reconstruct_high_so_far,
    remaining_rise,
    terminal_high_in_bracket_probability,
)
from kalshi_bot.weather.intraday import season_for_month_number

YAML_PATH = "docs/examples/weather_markets.example.yaml"
CHECKPOINT_HOURS = [8, 11, 14, 17]
TICKER_RE = re.compile(r"^KXHIGH([A-Z]+)-(\d{2}[A-Z]{3}\d{2})-")


def load_stations():
    doc = yaml.safe_load(open(YAML_PATH))
    return {
        str(t["series_ticker"]).replace("KXHIGH", ""): {"tz": t["timezone_name"], "asos": str(t.get("station_id", "")).lstrip("K")}
        for t in doc.get("series_templates", [])
    }


def fnum(s):
    s = (s or "").strip()
    return None if s in ("", "None") else float(s)


def brier(pairs):
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs) if pairs else float("nan")


async def main(specs_csv: str, n_cities: int) -> None:
    stations = load_stations()
    # city -> list of (date, floor, cap, outcome01)
    markets = defaultdict(list)
    for line in open(specs_csv):
        ticker, stype, floor, cap, result = (line.strip().split(",") + ["", "", "", "", ""])[:5]
        if result not in ("yes", "no"):
            continue
        m = TICKER_RE.match(ticker)
        if not m or m.group(1) not in stations:
            continue
        try:
            d = datetime.strptime(m.group(2), "%y%b%d").date()
        except ValueError:
            continue
        markets[m.group(1)].append((d, fnum(floor), fnum(cap), 1 if result == "yes" else 0))

    cities = sorted(markets)[:n_cities]
    client = IemAsosClient()
    hsf: dict[tuple[str, date], dict[int, float]] = {}
    dhigh: dict[tuple[str, date], float] = {}
    try:
        for i, city in enumerate(cities):
            if i > 0:
                await asyncio.sleep(6.0)  # IEM rate limit
            days = [d for d, _, _, _ in markets[city]]
            rows = await client.fetch_hourly(station=stations[city]["asos"], start=min(days), end=max(days), timezone=stations[city]["tz"])
            byday = defaultdict(list)
            for ts, t in rows:
                byday[ts.date()].append((ts, t))
            for d, samp in byday.items():
                ser = reconstruct_high_so_far(samp)
                hsf[(city, d)] = {ts.hour: v for ts, v in ser}
                if ser:
                    dhigh[(city, d)] = ser[-1][1]
    finally:
        await client.aclose()

    fit = []
    for (city, d), hm in hsf.items():
        dh = dhigh.get((city, d))
        if dh is None:
            continue
        s = season_for_month_number(d.month)
        for hour, v in hm.items():
            fit.append({"season": s, "hour": hour, "high_so_far_f": v, "daily_high_f": dh})
    model = fit_remaining_rise_model(fit)

    by_hour = defaultdict(list)
    by_type = defaultdict(list)
    outcomes = []
    for city in cities:
        for d, floor, cap, outcome in markets[city]:
            hm = hsf.get((city, d))
            if not hm:
                continue
            outcomes.append(outcome)
            s = season_for_month_number(d.month)
            stype = "between" if (floor is not None and cap is not None) else ("less" if cap is not None else "greater")
            for h in CHECKPOINT_HOURS:
                hs = max([v for hr, v in hm.items() if hr <= h], default=None)
                if hs is None:
                    continue
                mean, sigma = remaining_rise(model, season=s, hour=h)
                p = terminal_high_in_bracket_probability(
                    high_so_far_f=hs, bracket_lo_f=floor, bracket_hi_f=cap,
                    remaining_rise_mean_f=mean, remaining_rise_sigma_f=sigma,
                )
                by_hour[h].append((p, outcome))
                by_type[stype].append((p, outcome))

    n = len(outcomes)
    br = sum(outcomes) / n if n else float("nan")
    print(f"cities={cities} markets={n} yes_base_rate={br:.3f} baseline_Brier={brier([(br,o) for o in outcomes]):.4f}")
    print("ORIENTED bracket model Brier by checkpoint hour (local):")
    for h in CHECKPOINT_HOURS:
        ps = by_hour[h]
        if ps:
            sharp = sum(abs(p - 0.5) for p, _ in ps) / len(ps)
            print(f"  {h:02d}:00  n={len(ps):4d}  Brier={brier(ps):.4f}  sharpness={sharp:.3f}")
    print("by strike_type (all hours pooled):")
    for t, ps in by_type.items():
        print(f"  {t:8s} n={len(ps):4d} Brier={brier(ps):.4f}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/wxbt/market_specs.csv",
                     int(sys.argv[2]) if len(sys.argv) > 2 else 2))
