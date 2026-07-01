"""Read-only scan of the live Kalshi open-market catalog, aggregated by family.

Pages GET /markets?status=open and rolls up by series prefix (ticker up to the
first '-'): market count, 24h volume, open interest, median spread, and the
near-money share (yes-mid in 0.30-0.70 — where there's a real question, vs
trivially-priced far-money markets). Surfaces which families are liquid AND have
tradeable (non-degenerate) pricing, to decide where a non-crypto edge study is
worth it. GET-only; places no orders.

  python3 -m scripts.scan_kalshi_markets           # (from repo root, venv active)
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from statistics import median

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient


def _mid(m: dict) -> float | None:
    yb, ya = m.get("yes_bid"), m.get("yes_ask")
    if yb is None or ya is None or (yb == 0 and ya == 0):
        return None
    return (yb + ya) / 200.0  # cents -> dollars


async def main() -> None:
    settings = get_settings()
    client = KalshiClient(settings)
    fam: dict[str, dict] = defaultdict(lambda: {"n": 0, "vol24": 0, "oi": 0, "spreads": [], "nearmoney": 0, "priced": 0})
    cursor = None
    pages = 0
    total = 0
    while pages < 25:
        params = {"status": "open", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = await client.list_markets(**params)
        markets = resp.get("markets") or []
        if not markets:
            break
        for m in markets:
            total += 1
            tk = str(m.get("ticker") or "")
            family = tk.split("-")[0] if "-" in tk else tk
            f = fam[family]
            f["n"] += 1
            f["vol24"] += int(m.get("volume_24h") or 0)
            f["oi"] += int(m.get("open_interest") or 0)
            yb, ya = m.get("yes_bid"), m.get("yes_ask")
            if yb is not None and ya is not None and ya >= yb:
                f["spreads"].append(ya - yb)
            mid = _mid(m)
            if mid is not None:
                f["priced"] += 1
                if 0.30 <= mid <= 0.70:
                    f["nearmoney"] += 1
        cursor = resp.get("cursor")
        pages += 1
        if not cursor:
            break
    await client.aclose() if hasattr(client, "aclose") else None

    rows = []
    for family, f in fam.items():
        rows.append((
            family, f["n"], f["vol24"], f["oi"],
            (median(f["spreads"]) if f["spreads"] else None),
            f["nearmoney"], f["priced"],
        ))
    rows.sort(key=lambda r: r[2], reverse=True)  # by 24h volume
    print(f"total open markets scanned: {total}  families: {len(fam)}  pages: {pages}\n")
    print(f"{'family':<14}{'mkts':>6}{'vol24h':>10}{'openInt':>10}{'medSprd¢':>9}{'nearMoney':>10}{'priced':>8}")
    for family, n, v, oi, sp, nm, pr in rows[:35]:
        sps = f"{sp:.0f}" if sp is not None else "-"
        print(f"{family:<14}{n:>6}{v:>10}{oi:>10}{sps:>9}{nm:>10}{pr:>8}")


if __name__ == "__main__":
    asyncio.run(main())
