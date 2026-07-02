"""Analyze the stale-quote shadow log: quote persistence + live would-trade P&L.

Reads signals.jsonl (from scripts/stale_quote_shadow.py), fetches settlement
results for signaled markets from the Kalshi API (GET-only), and reports:
  - persistence: share of signals still crossable at +2s / +5s (fill-reality proxy)
  - would-trade P&L: entry at the signal price, settled outcome, net of the
    rate*p*(1-p) taker fee — i.e. live replication of the backtest, minus fills.

  docker cp scripts/stale_quote_shadow_report.py infra-trainer_production-1:/tmp/ && \
    docker exec -i infra-trainer_production-1 python /tmp/stale_quote_shadow_report.py \
      < /home/user1/kalshi_stale_shadow/signals.jsonl
"""
from __future__ import annotations

import asyncio
import json
import sys
from collections import defaultdict

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

FEE_RATE = 0.07


async def main() -> None:
    signals = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("type") == "signal":
            signals.append(rec)
    print(f"signals: {len(signals)}")
    if not signals:
        return

    # persistence
    p2 = [s for s in signals if any(p.get("t_s") == 2.0 and "still_crossable" in p for p in s["probes"])]
    p5 = [s for s in signals if any(p.get("t_s") == 5.0 and "still_crossable" in p for p in s["probes"])]
    c2 = sum(1 for s in p2 if any(p.get("t_s") == 2.0 and p["still_crossable"] for p in s["probes"]))
    c5 = sum(1 for s in p5 if any(p.get("t_s") == 5.0 and p["still_crossable"] for p in s["probes"]))
    print(f"persistence: +2s {c2}/{len(p2)} = {100*c2/len(p2) if p2 else 0:.0f}%   "
          f"+5s {c5}/{len(p5)} = {100*c5/len(p5) if p5 else 0:.0f}%")

    # settlements (GET-only)
    client = KalshiClient(get_settings())
    results: dict[str, str | None] = {}
    for tk in {s["ticker"] for s in signals}:
        try:
            r = await client.get_market(tk)
            m = r.get("market") or {}
            results[tk] = m.get("result") or None
        except Exception:
            results[tk] = None

    pnl, n, w = 0.0, 0, 0
    by_asset = defaultdict(lambda: [0.0, 0, 0])
    pnl_p2, n_p2 = 0.0, 0   # persistence-filtered: only signals crossable at +2s
    for s in signals:
        res = results.get(s["ticker"])
        if res not in ("yes", "no"):
            continue
        y = 1.0 if res == "yes" else 0.0
        entry = float(s["signal_entry"])
        gross = (y - entry) if s["side"] == "yes" else ((1.0 - y) - entry)
        net = gross - FEE_RATE * entry * (1 - entry)
        pnl += net; n += 1; w += 1 if net > 0 else 0
        a = by_asset[s["asset"]]
        a[0] += net; a[1] += 1; a[2] += 1 if net > 0 else 0
        if any(p.get("t_s") == 2.0 and p.get("still_crossable") for p in s["probes"]):
            pnl_p2 += net; n_p2 += 1
    print(f"\nwould-trade (settled {n}/{len(signals)}): net=${pnl:+.2f}  avg/ct={pnl/n if n else 0:+.4f}  win={100*w/n if n else 0:.0f}%")
    print(f"persistence-filtered (+2s crossable only): n={n_p2} net=${pnl_p2:+.2f} avg={pnl_p2/n_p2 if n_p2 else 0:+.4f}")
    for a, (ap, an, aw) in sorted(by_asset.items()):
        print(f"  {a}: n={an} net=${ap:+.2f} avg={ap/an if an else 0:+.4f} win={100*aw/an if an else 0:.0f}%")


if __name__ == "__main__":
    asyncio.run(main())
