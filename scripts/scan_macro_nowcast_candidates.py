"""Scope macro/econ/financial Kalshi series as NOWCAST targets (read-only, GET).

For a nowcast edge we need a series that is simultaneously: (1) FILLABLE (real
24h volume + tight-ish spread), (2) settles OFTEN enough to OOS-validate, and
(3) has a tradeable WINDOW (doesn't resolve to 0.99/0.01 long before close).
This scan reports, per Financials/Economics/Commodities series with any open
market: open-market count, total 24h volume, median spread, near-money share,
and the distribution of hours-to-close (the tradeable window). GET-only; no orders.

  docker cp scripts/scan_macro_nowcast_candidates.py infra-app_production_blue-1:/app/ \
    && docker exec infra-app_production_blue-1 python /app/scan_macro_nowcast_candidates.py
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime
from statistics import median

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

CATS = ("Financials", "Economics", "Commodities")


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _hours_to_close(m, now):
    ct = m.get("close_time") or m.get("expiration_time")
    if not ct:
        return None
    try:
        dt = datetime.fromisoformat(str(ct).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - now).total_seconds() / 3600.0


async def main() -> None:
    c = KalshiClient(get_settings())
    now = datetime.now(UTC)

    series = []
    cursor, pages = None, 0
    while pages < 25:
        p = {"limit": 1000}
        if cursor:
            p["cursor"] = cursor
        r = await c.list_series(**p)
        s = r.get("series") or []
        if not s:
            break
        for x in s:
            if str(x.get("category")) in CATS:
                series.append((x.get("ticker"), str(x.get("category")), x.get("title") or ""))
        cursor = r.get("cursor")
        pages += 1
        if not cursor:
            break

    rows = []
    for st, cat, title in series:
        try:
            r = await c.list_markets(series_ticker=st, status="open", limit=1000)
        except Exception:
            continue
        ms = r.get("markets") or []
        vol = sum(num(m.get("volume_24h_fp")) or 0 for m in ms)
        if vol <= 0 or not ms:
            continue
        spreads, mids, htc = [], [], []
        for m in ms:
            yb, ya = num(m.get("yes_bid_dollars")), num(m.get("yes_ask_dollars"))
            if yb is not None and ya is not None and ya >= yb:
                spreads.append((ya - yb) * 100.0)
                mid = (yb + ya) / 2.0
                if 0.30 <= mid <= 0.70:
                    mids.append(mid)
            h = _hours_to_close(m, now)
            if h is not None and h > 0:
                htc.append(h)
        oi = sum(num(m.get("open_interest_fp")) or 0 for m in ms)
        rows.append({
            "st": st, "cat": cat[:4], "title": title[:34], "n": len(ms), "vol": vol, "oi": oi,
            "sprd": median(spreads) if spreads else None,
            "nm": len(mids), "medhtc": median(htc) if htc else None,
        })

    rows.sort(key=lambda x: -x["vol"])
    print(f"macro/econ/financial series with open volume: {len(rows)}  ({now:%Y-%m-%d %H:%MZ})\n")
    print(f"{'series':<20}{'cat':>5}{'mkts':>5}{'vol24fp':>10}{'openInt':>9}{'medSprd¢':>9}{'nearMny':>8}{'medHrsToClose':>14}  title")
    for r in rows[:40]:
        sp = f"{r['sprd']:.1f}" if r["sprd"] is not None else "-"
        hc = f"{r['medhtc']:.1f}" if r["medhtc"] is not None else "-"
        print(f"{r['st']:<20}{r['cat']:>5}{r['n']:>5}{r['vol']:>10.0f}{r['oi']:>9.0f}{sp:>9}{r['nm']:>8}{hc:>14}  {r['title']}")


if __name__ == "__main__":
    asyncio.run(main())
