"""Forward-collect a CPI nowcast-vs-market shadow snapshot (read-only, GET only).

Captures, per run, the triple needed to eventually test gate-0 criterion (2)
(does the Cleveland Fed nowcast beat the Kalshi market?):
  1. current Cleveland Fed CPI MoM nowcast per front target month (nowcast_month.json, UA-gated)
  2. the live KXCPI (MoM) + KXCPIYOY (YoY) ladders (strike, yes_bid/ask, vol, OI)
  3. run timestamp
Appends one JSON line per run to $CPI_SHADOW_DIR (default /app/data/cpi_shadow/snapshots.jsonl).
Kalshi settlement later supplies the outcome, completing each nowcast-vs-market-vs-actual triple.

Monthly cadence means this must run on a schedule (daily is ample) to accrue data — one run is n=1.
Places NO orders; GET-only. Run in a prod container (live creds + outbound):
  docker exec infra-app_production_blue-1 python /app/collect_cpi_shadow.py
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import UTC, datetime

import httpx

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

NOWCAST_URL = "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
OUT_DIR = os.environ.get("CPI_SHADOW_DIR", "/app/data/cpi_shadow")


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_nowcast(doc) -> dict:
    """subcaption(target month 'YYYY-M') -> latest MoM %% nowcast (last non-empty 'CPI Inflation')."""
    out = {}
    for e in doc:
        sub = e.get("chart", {}).get("subcaption")
        if not sub:
            continue
        try:
            ser = next(s for s in e["dataset"] if s["seriesname"] == "CPI Inflation")
        except (StopIteration, KeyError):
            continue
        vals = [_num(x.get("value")) for x in ser["data"] if x.get("value") not in ("", None)]
        vals = [v for v in vals if v is not None]
        if vals:
            y, m = sub.split("-")
            out[f"{int(y):04d}-{int(m):02d}"] = round(vals[-1], 4)
    return out


def parse_ladder(markets) -> dict:
    """event_ticker -> sorted [{strike, yes_bid, yes_ask, vol, oi}] for '-T<strike>' markets."""
    evs: dict[str, list] = {}
    for m in markets:
        mm = re.search(r"-T(-?\d+(?:\.\d+)?)$", str(m.get("ticker") or ""))
        yb, ya = _num(m.get("yes_bid_dollars")), _num(m.get("yes_ask_dollars"))
        if not mm or yb is None or ya is None:
            continue
        evs.setdefault(str(m.get("event_ticker")), []).append({
            "strike": float(mm.group(1)), "yes_bid": yb, "yes_ask": ya,
            "vol": _num(m.get("volume_24h_fp")) or 0, "oi": _num(m.get("open_interest_fp")) or 0,
        })
    for ev in evs:
        evs[ev].sort(key=lambda r: r["strike"])
    return evs


async def main() -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": UA}) as http:
        resp = await http.get(NOWCAST_URL)
        resp.raise_for_status()
        nowcast = parse_nowcast(resp.json())

    client = KalshiClient(settings)
    ladders = {}
    for series in ("KXCPI", "KXCPIYOY"):
        r = await client.list_markets(series_ticker=series, status="open", limit=1000)
        ladders[series] = parse_ladder(r.get("markets") or [])

    snapshot = {
        "captured_at": datetime.now(UTC).isoformat(),
        "kalshi_env": settings.kalshi_env,
        "nowcast_mom_pct_by_target": nowcast,
        "ladders": ladders,
    }

    line = json.dumps(snapshot)
    # Stdout mode: emit ONLY the JSON line so a host cron can append to persistent host disk
    # (`docker exec ... env CPI_SHADOW_STDOUT=1 python /app/collect_cpi_shadow.py >> host.jsonl`),
    # surviving container recreates. Default mode appends to the (ephemeral) container path + prints a summary.
    if os.environ.get("CPI_SHADOW_STDOUT") == "1":
        print(line)
        return

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "snapshots.jsonl")
    with open(path, "a") as f:
        f.write(line + "\n")

    n_now = len(nowcast)
    n_mkts = sum(len(v) for lad in ladders.values() for v in lad.values())
    print(f"appended snapshot -> {path}")
    print(f"  nowcast target months: {n_now}  (front: "
          + ", ".join(f"{k}={nowcast[k]:+.3f}%" for k in sorted(nowcast)[-3:]) + ")")
    print(f"  ladders: KXCPI events={len(ladders['KXCPI'])} KXCPIYOY events={len(ladders['KXCPIYOY'])} "
          f"total strikes={n_mkts}")


if __name__ == "__main__":
    asyncio.run(main())
