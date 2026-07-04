"""Offline weather lock-in market-edge eval (design 2026-07-04, Leg 3, Task 4).

For every settled KXHIGH market with intraday quote snapshots collected since
2026-06-22 (`historical_market_snapshots`): compute the settlement station's
running high (IEM ASOS, official station — NOT Open-Meteo), find the first
snapshot where the market is deterministically LOCKED by high-so-far alone
(strict inequality — see alignment notes below), then simulate a taker BUY at
the NEXT snapshot's ask (honest-fill rule, per Task 4 brief) with the 7%
taker fee. LOCKED_YES buys YES on 'greater' markets (high-so-far > floor);
LOCKED_NO buys NO on 'less' markets (high-so-far > cap). 'between' brackets
are skipped per the MVP spec (and do not exist in the current 20-city map
anyway — verified 0 rows with a '-B' ticker suffix in Step 1).

This re-runs the same question the 2026-06-22 harness
(`scripts/weather_lockin_fee_edge_harness.py`) answered NO-GO on 280 markets
(06-14->06-20), against the newer/larger 06-22->07-04 window, using the
already-tested lock predicate (`WeatherResolutionState`) and fee/edge gate
(`kalshi_bot.weather.lockin_gate.evaluate_lockin_fee_edge_gate`) rather than
re-deriving the fee math.

Alignment points resolved from real data/code (Step 1, do not re-guess these):
  1. `IemAsosClient()` takes no settings arg; the fetch method is
     `fetch_hourly(station=<bare IEM code, NO leading 'K'>, start=date, end=date,
     timezone=<IANA tz str>)` -> list[(naive_local_datetime, temp_f)]. Confirmed
     via `grep -n "class IemAsosClient" -A 40 src/kalshi_bot/integrations/asos_archive.py`
     and the already-working `scripts/weather_lockin_fetch_asos.py`.
  2. Strike/result fields live under `payload["market"][...]`, not top-level:
     `floor_strike` (only present on 'greater' markets), `cap_strike` (only on
     'less'), `strike_type` ('greater'/'less'/'between'), `result` (empty
     string "" while open, "yes"/"no" once settled). Confirmed by inspecting 5
     real rows (KXHIGHNY-26JUN23-T85 / -T78) per Step 1.
  3. Strike orientation + boundary semantics: KXHIGH 'greater' settles YES iff
     official high is STRICTLY > floor_strike (a boundary tie settles NO); the
     06-22 spec doc documents this was found the hard way (2 boundary
     false-locks before the strict-inequality fix). 'less' is the mirror:
     YES iff high STRICTLY <= cap, so LOCKED_NO fires once high STRICTLY > cap.
     This eval uses strict '>' throughout, matching the validated harness.
  4. Station map: reused verbatim from `scripts/weather_lockin_fetch_asos.py`,
     which matches the deployed `WEATHER_MARKET_MAP_PATH` YAML (default
     `docs/examples/weather_markets.example.yaml`, confirmed 20 series/stations
     inside the running app container).

Usage (inside the app container, DB + integrations available):
  docker cp scripts/weather_lockin_eval.py infra-app_production_green-1:/app/
  docker exec -d infra-app_production_green-1 python /app/weather_lockin_eval.py
  # output (JSONL, docker-exec-disconnect-safe) is ALSO written inside the
  # container to /tmp/lockin_eval_out.jsonl; ASOS obs are cached to
  # /tmp/lockin_obs_cache.json so a rerun does not re-hit IEM.
  docker cp infra-app_production_green-1:/tmp/lockin_eval_out.jsonl <dest>
"""
from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import text

from kalshi_bot.config import get_settings
from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.integrations.asos_archive import IemAsosClient
from kalshi_bot.weather.lockin_gate import evaluate_lockin_fee_edge_gate

SINCE = "2026-06-22"
OUT_PATH = os.environ.get("LOCKIN_EVAL_OUT", "/tmp/lockin_eval_out.jsonl")
OBS_CACHE_PATH = os.environ.get("LOCKIN_OBS_CACHE", "/tmp/lockin_obs_cache.json")

_out_fh = open(OUT_PATH, "w")  # noqa: SIM115 - deliberately long-lived


def emit(line: str) -> None:
    """Print AND persist to a file inside the container (docker exec's stdout
    dies with the client; the file survives)."""
    print(line, flush=True)
    _out_fh.write(line + "\n")
    _out_fh.flush()

# series_ticker -> (bare IEM ASOS station code, IANA tz) — verbatim from
# scripts/weather_lockin_fetch_asos.py, which matches the deployed
# WEATHER_MARKET_MAP_PATH YAML (docs/examples/weather_markets.example.yaml).
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


def ticker_day(market_ticker: str) -> date:
    """Parse the local market day from the ticker's date segment (e.g. '26JUN23' -> 2026-06-23).

    `local_market_day` in the DB is INCONSISTENTLY formatted (some rows ISO
    'YYYY-MM-DD', others the raw ticker date code like '26JUL02' — verified via
    Step 1 data inspection: every KXHIGH series has a mix of both). The ticker
    itself is always `<SERIES>-<YYMMMDD>-<STRIKE>`, so parse the date from
    there instead of trusting the column.
    """
    date_code = market_ticker.split("-")[1]
    return datetime.strptime(date_code.title(), "%y%b%d").date()


def lock_state(floor: float | None, cap: float | None, high_so_far: float) -> tuple[WeatherResolutionState, str]:
    """Return (WeatherResolutionState, strike_type) for an early-deterministic lock."""
    if floor is not None and cap is None:
        if high_so_far > floor:
            return WeatherResolutionState.LOCKED_YES, "greater"
        return WeatherResolutionState.UNRESOLVED, "greater"
    if cap is not None and floor is None:
        if high_so_far > cap:
            return WeatherResolutionState.LOCKED_NO, "less"
        return WeatherResolutionState.UNRESOLVED, "less"
    if floor is not None and cap is not None:
        if high_so_far > cap:
            return WeatherResolutionState.LOCKED_NO, "between"
        return WeatherResolutionState.UNRESOLVED, "between"
    return WeatherResolutionState.UNRESOLVED, "unknown"


async def main() -> None:
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    async with factory() as session:
        rows = (await session.execute(text("""
            SELECT market_ticker, series_ticker, station_id, local_market_day,
                   asof_ts, yes_ask_dollars, no_ask_dollars, payload
            FROM historical_market_snapshots
            WHERE asof_ts >= :since AND series_ticker LIKE 'KXHIGH%'
            ORDER BY market_ticker, asof_ts
        """), {"since": datetime.fromisoformat(SINCE).replace(tzinfo=UTC)})).mappings().all()

    by_market: dict[str, list] = defaultdict(list)
    for r in rows:
        by_market[r["market_ticker"]].append(r)

    # One fetch_hourly() call per series covering the whole window (politeness:
    # small sleep between the ~20 series calls to the external IEM API).
    day_bounds: dict[str, tuple[date, date]] = {}
    for ticker, snaps in by_market.items():
        series = snaps[0]["series_ticker"]
        lo = hi = ticker_day(ticker)
        if series not in day_bounds:
            day_bounds[series] = (lo, hi)
        else:
            plo, phi = day_bounds[series]
            day_bounds[series] = (min(lo, plo), max(hi, phi))

    obs_by_series: dict[str, list[tuple[datetime, float]]] = {}
    cache: dict[str, list] = {}
    if os.path.exists(OBS_CACHE_PATH):
        try:
            with open(OBS_CACHE_PATH) as fh:
                cache = json.load(fh)
        except Exception:  # noqa: BLE001 - cache is best-effort
            cache = {}

    asos = IemAsosClient()
    fetched_any = False
    try:
        for series, (lo, hi) in sorted(day_bounds.items()):
            station_tz = SERIES.get(series)
            if not station_tz:
                continue
            station, tz = station_tz
            cache_key = f"{series}:{lo.isoformat()}:{hi.isoformat()}"
            if cache_key in cache:
                obs_by_series[series] = sorted(
                    (datetime.fromisoformat(ts), tf) for ts, tf in cache[cache_key]
                )
                emit(f"# obs cache hit {series} ({len(obs_by_series[series])} rows)")
                continue
            if fetched_any:
                await asyncio.sleep(30.0)  # IEM rate limit courtesy
            fetched_any = True
            hourly: list[tuple[datetime, float]] = []
            for attempt in range(5):
                try:
                    hourly = await asos.fetch_hourly(station=station, start=lo, end=hi, timezone=tz)
                    break
                except Exception as exc:  # noqa: BLE001 - retry with backoff, then give up
                    wait_s = 20.0 * (attempt + 1)
                    emit(f"WARN fetch_hourly failed for {series}/{station} "
                         f"(attempt {attempt + 1}/5): {exc} — retrying in {wait_s:.0f}s")
                    await asyncio.sleep(wait_s)
            obs_by_series[series] = sorted(hourly)
            emit(f"# fetched {series}/{station}: {len(hourly)} obs")
            if hourly:
                cache[cache_key] = [(ts.isoformat(), tf) for ts, tf in hourly]
                with open(OBS_CACHE_PATH, "w") as fh:
                    json.dump(cache, fh)
    finally:
        await asos.aclose()

    trades, skipped = [], defaultdict(int)
    for ticker, snaps in sorted(by_market.items()):
        spec = snaps[-1]  # latest snapshot carries the final payload (result set once settled)
        market = (spec["payload"] or {}).get("market", {})
        floor = market.get("floor_strike")
        cap = market.get("cap_strike")
        strike_type = market.get("strike_type")
        result = (market.get("result") or "").strip().lower()

        if strike_type == "between" or (floor is not None and cap is not None):
            skipped["between_or_no_strike"] += 1
            continue
        if strike_type not in ("greater", "less"):
            skipped["unknown_strike_type"] += 1
            continue
        if result not in ("yes", "no"):
            skipped["unsettled"] += 1
            continue

        series = spec["series_ticker"]
        station_tz = SERIES.get(series or "")
        if not station_tz:
            skipped["no_station_map"] += 1
            continue
        _, tz = station_tz
        day = ticker_day(ticker)
        series_obs = [(ts, tf) for ts, tf in obs_by_series.get(series, []) if ts.date() == day]
        if not series_obs:
            skipped["no_obs"] += 1
            continue

        run_high, highs = float("-inf"), []
        for ts, tf in series_obs:
            run_high = max(run_high, tf)
            highs.append((ts, run_high))

        def high_at(t_utc: datetime) -> float | None:
            t_local = t_utc.astimezone(ZoneInfo(tz)).replace(tzinfo=None)
            best = None
            for ts, h in highs:
                if ts <= t_local:
                    best = h
                else:
                    break
            return best

        lock_i, lock_state_val = None, None
        for i, s in enumerate(snaps):
            h = high_at(s["asof_ts"].astimezone(UTC))
            if h is None:
                continue
            state, _ = lock_state(floor, cap, h)
            if state != WeatherResolutionState.UNRESOLVED:
                lock_i, lock_state_val = i, state
                break
        if lock_i is None or lock_i + 1 >= len(snaps):
            skipped["never_locked_or_no_next_snap"] += 1
            continue

        fill = snaps[lock_i + 1]  # honest fill: NEXT snapshot after the lock is detected
        side = "yes" if lock_state_val == WeatherResolutionState.LOCKED_YES else "no"
        ask = fill["yes_ask_dollars"] if side == "yes" else fill["no_ask_dollars"]
        if ask is None:
            skipped["no_ask_at_fill"] += 1
            continue

        gate = evaluate_lockin_fee_edge_gate(
            resolution_state=lock_state_val,
            yes_ask_dollars=Decimal(str(ask)) if side == "yes" else None,
            no_ask_dollars=Decimal(str(ask)) if side == "no" else None,
        )
        if not gate.should_trade:
            skipped[f"gate_{gate.reason}"] += 1
            continue

        won = (side == "yes" and result == "yes") or (side == "no" and result == "no")
        net = float(gate.net_edge_dollars) if won else -(float(gate.entry_price_dollars) + float(gate.fee_dollars))
        trades.append({
            "ticker": ticker, "series": series, "day": day.isoformat(), "side": side,
            "ask": round(float(gate.entry_price_dollars), 4),
            "edge": round(float(gate.gross_edge_dollars), 4),
            "won": won, "net": round(net, 4),
            "lock_ts": snaps[lock_i]["asof_ts"].isoformat(),
            "fill_ts": fill["asof_ts"].isoformat(),
        })
        emit(json.dumps(trades[-1]))

    wins = sum(1 for t in trades if t["won"])
    per_city: dict[str, dict] = defaultdict(lambda: {"trades": 0, "wins": 0, "net": 0.0})
    for t in trades:
        c = per_city[t["series"]]
        c["trades"] += 1
        c["wins"] += 1 if t["won"] else 0
        c["net"] += t["net"]

    emit(json.dumps({"summary": {
        "trades": len(trades), "wins": wins,
        "lock_violations": len(trades) - wins,  # >0 => basis/lag/tz bug, investigate before trusting ANY of it
        "net_total": round(sum(t["net"] for t in trades), 4),
        "avg_net": round(sum(t["net"] for t in trades) / len(trades), 4) if trades else None,
        "avg_edge": round(sum(t["edge"] for t in trades) / len(trades), 4) if trades else None,
        "per_city": {k: v for k, v in sorted(per_city.items())},
        "skipped": dict(skipped)}}))


if __name__ == "__main__":
    asyncio.run(main())
