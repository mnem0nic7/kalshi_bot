"""Inspect Kalshi CPI market structure for the nowcast design (read-only, GET)."""
from __future__ import annotations

import asyncio
import json

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient


async def dump(c, st):
    print(f"\n========== SERIES {st} ==========")
    try:
        sr = await c.get_series(st) if hasattr(c, "get_series") else {}
        s = sr.get("series") or sr
        for k in ("title", "category", "frequency", "settlement_sources", "contract_url"):
            if isinstance(s, dict) and s.get(k):
                print(f"  {k}: {s.get(k)}")
    except Exception as e:
        print(f"  (series meta unavailable: {e})")
    r = await c.list_markets(series_ticker=st, status="open", limit=1000)
    ms = r.get("markets") or []
    print(f"  open markets: {len(ms)}")
    evs = {}
    for m in ms:
        evs.setdefault(m.get("event_ticker"), []).append(m)
    for ev, mk in list(evs.items())[:3]:
        print(f"\n  --- event {ev}  ({len(mk)} strikes) ---")
        m0 = mk[0]
        for k in ("ticker", "title", "subtitle", "yes_sub_title", "rules_primary",
                  "close_time", "expiration_time", "settlement_value", "cap_strike",
                  "floor_strike", "strike_type", "yes_bid_dollars", "yes_ask_dollars",
                  "volume_24h_fp", "open_interest_fp"):
            v = m0.get(k)
            if v not in (None, ""):
                sv = str(v)
                print(f"      {k}: {sv[:180]}")


async def main():
    c = KalshiClient(get_settings())
    for st in ("KXCPIYOY", "KXCPI", "KXCPICOMBO"):
        await dump(c, st)


if __name__ == "__main__":
    asyncio.run(main())
