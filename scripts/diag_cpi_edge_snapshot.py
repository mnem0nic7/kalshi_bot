"""One-shot nowcast-vs-market edge snapshot on the live Kalshi CPI ladder (read-only).

Feeds a Cleveland Fed CPI MoM nowcast point + error-sigma (extracted offline from
nowcast_month.json) and compares nowcast-implied P(MoM > strike) to the live Kalshi
KXCPI ladder, net of the rate*p*(1-p) fee + spread. Illustrative single snapshot,
NOT a backtest (criterion-2 needs forward-collected price history). GET-only.

  # nowcast MoM % and sigma(pp) per target month, from the offline spike:
  #   2026-6 (KXCPI-26JUN): point=-0.061  sigma~0.13
  #   2026-7 (KXCPI-26JUL): point=-0.188  sigma~0.16
  docker cp scripts/diag_cpi_edge_snapshot.py infra-app_production_blue-1:/app/ && \
    docker exec infra-app_production_blue-1 python /app/diag_cpi_edge_snapshot.py KXCPI-26JUN -0.061 0.13
"""
from __future__ import annotations

import asyncio
import math
import re
import sys

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

FEE_RATE = 0.07  # taker fee fraction; cost ~ rate*p*(1-p)


def norm_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


async def main():
    event = sys.argv[1] if len(sys.argv) > 1 else "KXCPI-26JUN"
    point = float(sys.argv[2]) if len(sys.argv) > 2 else -0.061
    sigma = float(sys.argv[3]) if len(sys.argv) > 3 else 0.13
    series = event.split("-")[0]
    c = KalshiClient(get_settings())
    r = await c.list_markets(series_ticker=series, status="open", limit=1000)
    ms = [m for m in (r.get("markets") or []) if str(m.get("event_ticker")) == event]
    rows = []
    for m in ms:
        mm = re.search(r"-T(-?\d+(?:\.\d+)?)$", str(m.get("ticker") or ""))
        yb, ya = num(m.get("yes_bid_dollars")), num(m.get("yes_ask_dollars"))
        if not mm or yb is None or ya is None:
            continue
        k = float(mm.group(1))
        mid = (yb + ya) / 2.0
        # nowcast-implied P(MoM > k)  (strike_type=greater)
        p = 1.0 - norm_cdf((k - point) / sigma)
        rows.append((k, yb, ya, mid, p, num(m.get("volume_24h_fp")) or 0, num(m.get("open_interest_fp")) or 0))
    rows.sort()
    print(f"event={event}  nowcast MoM={point:+.3f}%  sigma={sigma:.3f}pp  strikes={len(rows)}\n")
    print(f"{'strike>':>8}{'yBid':>6}{'yAsk':>6}{'mid':>6}{'nowP':>7}{'edge':>7}{'feeBuy':>7}{'net':>7}{'vol':>8}{'OI':>8}  action")
    for k, yb, ya, mid, p, vol, oi in rows:
        edge = p - mid
        # to capture edge: buy yes if p>mid (pay ask), sell/buy-no if p<mid (hit bid)
        if edge >= 0:
            entry = ya
            gross = p - ya            # EV of buying yes at ask
        else:
            entry = yb
            gross = yb - p            # EV of selling yes at bid (buying no)
        fee = FEE_RATE * entry * (1 - entry)
        net = gross - fee
        act = ""
        if net > 0.01 and 0.05 <= mid <= 0.95:
            act = "  <== BUY YES" if edge >= 0 else "  <== BUY NO"
        print(f"{k:>8.1f}{yb:>6.2f}{ya:>6.2f}{mid:>6.2f}{p:>7.3f}{edge:>+7.3f}{fee:>7.3f}{net:>+7.3f}{vol:>8.0f}{oi:>8.0f}{act}")
    print("\nNOTE: single live snapshot, illustrative only. Net>0 uses rate*p*(1-p) fee + full ask/bid cross.")


if __name__ == "__main__":
    asyncio.run(main())
