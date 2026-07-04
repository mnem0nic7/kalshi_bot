"""Pure helpers for the 1h stale-quote SHADOW scanner (no orders).

The hourly ladders are wide (~50 strikes); the band bound is mandatory —
scanning the full ladder is the working-set churn that drove the crypto_1h
daemon to its 8g cap (docs/operations/2026-07-02-daemon-reconcile-wedge.md).
"""
from __future__ import annotations

import math

SERIES_1H = {"BTC": "KXBTC", "ETH": "KXETH", "SOL": "KXSOL", "XRP": "KXXRP",
             "BNB": "KXBNB", "DOGE": "KXDOGE", "HYPE": "KXHYPE"}

STRIKE_BAND = 0.02  # |ln(S/K)| <= 2% — plenty for a <=1h horizon


def in_strike_band(spot: float, strike: float, band: float = STRIKE_BAND) -> bool:
    if not spot or not strike or spot <= 0 or strike <= 0:
        return False
    return abs(math.log(spot / strike)) <= band


def range_fair(fair_up_floor: float, fair_up_cap: float | None) -> float:
    if fair_up_cap is None:
        return fair_up_floor
    return max(0.0, fair_up_floor - fair_up_cap)


def cap_moneyness(mny_floor: float, floor_strike: float, cap_strike: float) -> float:
    return mny_floor + math.log(floor_strike / cap_strike)
