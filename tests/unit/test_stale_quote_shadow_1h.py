"""1h shadow scanner helpers (Leg 2a). Range markets: P(floor<S<cap) =
fair_up(floor) - fair_up(cap); moneyness is a log-ratio ln(S/K) so the cap
row is mny_floor + ln(floor/cap)."""
import math

import pytest

from kalshi_bot.crypto.stale_quote_shadow_1h import (
    SERIES_1H, cap_moneyness, in_strike_band, range_fair,
)


def test_series_map_covers_active_assets_hourly():
    assert SERIES_1H["BTC"] == "KXBTC"
    assert set(SERIES_1H) == {"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"}


def test_strike_band_is_log_symmetric():
    assert in_strike_band(100.0, 101.9)          # +1.9%
    assert in_strike_band(100.0, 98.1)           # -1.9%
    assert not in_strike_band(100.0, 103.0)      # +3%
    assert not in_strike_band(100.0, 0.0)        # degenerate strike
    assert not in_strike_band(0.0, 100.0)        # degenerate spot


def test_range_fair_is_two_sided_and_clamped():
    assert range_fair(0.8, 0.3) == pytest.approx(0.5)
    assert range_fair(0.8, None) == pytest.approx(0.8)   # above/below market
    assert range_fair(0.3, 0.8) == 0.0                   # numeric noise clamps


def test_cap_moneyness_shifts_log_ratio():
    # mny = ln(S/floor); ln(S/cap) = mny + ln(floor/cap)
    spot, floor, cap = 105.0, 100.0, 110.0
    mny_floor = math.log(spot / floor)
    assert cap_moneyness(mny_floor, floor, cap) == pytest.approx(math.log(spot / cap))
