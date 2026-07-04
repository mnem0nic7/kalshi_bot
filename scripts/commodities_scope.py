# scripts/commodities_scope.py
"""Commodities vertical scoping (design 2026-07-04, Leg 4). Public API only.

For each series in Kalshi's Commodities category: markets, settle cadence,
volume/open interest, spread. Output: JSON rows to rank in the research doc.
Run: source .venv/bin/activate && python scripts/commodities_scope.py
"""
from __future__ import annotations

import json
import statistics
import time
import urllib.error
import urllib.request

BASE = "https://api.elections.kalshi.com/trade-api/v2"


def get(path: str, retries: int = 6) -> dict:
    req = urllib.request.Request(BASE + path, headers={"User-Agent": "kalshi-bot-scope/1.0"})
    delay = 2.0
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    raise RuntimeError("unreachable")


def main() -> None:
    series = get("/series?category=Commodities").get("series") or []
    print(f"# {len(series)} commodity series", flush=True)
    for s in series:
        st = s.get("ticker")
        try:
            mk = get(f"/markets?series_ticker={st}&status=open&limit=200").get("markets") or []
            settled = get(f"/markets?series_ticker={st}&status=settled&limit=200").get("markets") or []
        except Exception as e:
            print(json.dumps({"series": st, "error": str(e)[:120]}), flush=True)
            continue
        spreads, vols, ois = [], [], []
        for m in mk:
            yb, ya = m.get("yes_bid_dollars"), m.get("yes_ask_dollars")
            if yb is not None and ya is not None:
                try:
                    spreads.append(float(ya) - float(yb))
                except (TypeError, ValueError):
                    pass
            # NOTE (2026-07-04): the public API does not expose bare "volume" /
            # "open_interest" fields as the original spec assumed; it returns
            # "volume_fp" / "open_interest_fp" as decimal strings. Adapted here.
            for key, acc in (("volume_fp", vols), ("open_interest_fp", ois)):
                v = m.get(key)
                if v is not None:
                    try:
                        acc.append(float(v))
                    except (TypeError, ValueError):
                        pass
        closes = sorted(str(m.get("close_time") or "") for m in settled if m.get("close_time"))
        row = {"series": st, "title": s.get("title"), "frequency": s.get("frequency"),
               "open_markets": len(mk),
               "settled_recent": len(settled),
               "first_settled_close": closes[0][:10] if closes else None,
               "last_settled_close": closes[-1][:10] if closes else None,
               "median_spread": round(statistics.median(spreads), 3) if spreads else None,
               "total_volume_open": sum(vols) or None,
               "total_oi_open": sum(ois) or None}
        print(json.dumps(row), flush=True)
        time.sleep(1.0)   # be polite; unauthenticated endpoint; public API rate-limits aggressively


if __name__ == "__main__":
    main()
