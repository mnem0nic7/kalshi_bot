"""Per-asset rollup for stale-quote pilot JSONL (Leg 1 kill-rule visibility).

Kill rule (design 2026-07-04): flag an asset once it is >= $2 cumulative
negative after >= 15 settles while at least one other asset is positive.
Flag only — dropping the asset stays an operator action on the runner env.
"""
from __future__ import annotations

SERIES_15M = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M",
              "XRP": "KXXRP15M", "BNB": "KXBNB15M", "DOGE": "KXDOGE15M",
              "HYPE": "KXHYPE15M"}
KILL_NET_DOLLARS = -2.0
KILL_MIN_SETTLES = 15


def asset_for_ticker(ticker: str) -> str | None:
    for asset, series in SERIES_15M.items():
        if ticker.startswith(series + "-"):
            return asset
    return None


def summarize_pilot_records(records: list[dict]) -> dict:
    per: dict[str, dict] = {}
    for rec in records:
        if rec.get("type") != "settle":
            continue
        asset = asset_for_ticker(str(rec.get("ticker", "")))
        if asset is None:
            continue
        row = per.setdefault(asset, {"settles": 0, "wins": 0, "losses": 0, "net": 0.0})
        net = float(rec.get("net") or 0.0)
        row["settles"] += 1
        row["net"] += net
        row["wins" if net > 0 else "losses"] += 1
    any_positive = {a for a, r in per.items() if r["net"] > 0}
    for asset, row in per.items():
        row["kill"] = (row["net"] <= KILL_NET_DOLLARS
                       and row["settles"] >= KILL_MIN_SETTLES
                       and bool(any_positive - {asset}))
        row["net"] = round(row["net"], 4)
    return {"per_asset": per,
            "total_net": round(sum(r["net"] for r in per.values()), 4),
            "total_settles": sum(r["settles"] for r in per.values())}
