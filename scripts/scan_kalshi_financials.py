"""Scan the liquid Kalshi financial / commodity / macro families for edge.

For each Financials/Economics/Commodities series with open markets + volume:
report volume + ladder size, and check multi-strike threshold ladders for
dutch-book / monotonicity violations exceeding fees (the only forecasting-free
edge). Flags anything non-efficient; otherwise confirms the "liquid == efficient"
wall. GET-only; places no orders. Run inside a prod container (live creds):

  docker cp scripts/scan_kalshi_financials.py infra-app_production_blue-1:/app/ \
    && docker exec infra-app_production_blue-1 python /app/scan_kalshi_financials.py
"""
from __future__ import annotations

import asyncio
import re
from collections import defaultdict

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

FEE = 0.07  # rough per-leg taker fee fraction (rate * p*(1-p) peaks ~1.75c at p=.5)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ladder_violations(strikes):
    """strikes: sorted [(strike, yes_bid, yes_ask, vol)]. P(>=strike) must be
    monotone decreasing; a higher strike bidding above a lower strike's ask is a
    fee-clearing dutch book. Returns (violation_count, best_net_edge)."""
    viol, best = 0, 0.0
    for i in range(len(strikes)):
        x1, yb1, ya1, v1 = strikes[i]
        for j in range(i + 1, len(strikes)):
            x2, yb2, ya2, v2 = strikes[j]  # x2 > x1
            if v1 <= 0 or v2 <= 0:
                continue
            net = (yb2 - ya1) - FEE * (ya1 * (1 - ya1) + yb2 * (1 - yb2))
            if net > 0.01:
                viol += 1
                best = max(best, net)
    return viol, best


async def main() -> None:
    c = KalshiClient(get_settings())
    series = []
    cursor, pages = None, 0
    while pages < 20:
        p = {"limit": 1000}
        if cursor:
            p["cursor"] = cursor
        r = await c.list_series(**p)
        s = r.get("series") or []
        if not s:
            break
        series += [x.get("ticker") for x in s if str(x.get("category")) in ("Financials", "Economics", "Commodities")]
        cursor = r.get("cursor")
        pages += 1
        if not cursor:
            break

    hits = []
    for st in series:
        try:
            r = await c.list_markets(series_ticker=st, status="open", limit=1000)
        except Exception:
            continue
        ms = r.get("markets") or []
        vol = sum(num(m.get("volume_24h_fp")) or 0 for m in ms)
        if vol <= 0:
            continue
        evs = defaultdict(list)
        for m in ms:
            evs[m.get("event_ticker")].append(m)
        ev = max(evs, key=lambda e: sum(num(m.get("volume_24h_fp")) or 0 for m in evs[e]))
        strikes = []
        for m in evs[ev]:
            mm = re.search(r"-T(\d+(?:\.\d+)?)$", str(m.get("ticker") or ""))
            yb, ya = num(m.get("yes_bid_dollars")), num(m.get("yes_ask_dollars"))
            if mm and yb is not None and ya is not None:
                strikes.append((float(mm.group(1)), yb, ya, num(m.get("volume_24h_fp")) or 0))
        strikes.sort()
        viol, best = _ladder_violations(strikes)
        hits.append((st, len(ms), vol, len(strikes), viol, best))

    hits.sort(key=lambda x: -x[2])
    print(f"liquid financial/commodity/macro series: {len(hits)}\n")
    print(f"{'series':<22}{'mkts':>5}{'vol24fp':>11}{'Tstrk':>6}{'arbViol':>8}{'bestNet':>8}")
    flagged = []
    for st, n, v, ns, viol, best in hits[:30]:
        print(f"{st:<22}{n:>5}{v:>11.0f}{ns:>6}{viol:>8}{best:>8.3f}")
        if viol > 0:
            flagged.append((st, viol, best))
    print()
    if flagged:
        print("!!! LADDER ARB FLAGGED (investigate):")
        for st, viol, best in flagged:
            print(f"    {st}: {viol} violations, best net {best:.3f}/contract")
    else:
        print("No fee-clearing ladder arb found — liquid financial families remain efficient.")


if __name__ == "__main__":
    asyncio.run(main())
