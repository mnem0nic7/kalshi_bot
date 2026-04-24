from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from datetime import UTC, datetime, timedelta

import numpy as np

from kalshi_bot.services.stop_loss import (
    _midpoint, _momentum_slope, _peak_price_from_history,
    _position_opened_at_from_fills, _sell_price, _side_price, _trailing_loss_ratio,
)


def _ms(yes_bid: str | None, yes_ask: str | None) -> MagicMock:
    ms = MagicMock()
    ms.yes_bid_dollars = Decimal(yes_bid) if yes_bid is not None else None
    ms.yes_ask_dollars = Decimal(yes_ask) if yes_ask is not None else None
    return ms


def _pos(side: str, count: str, avg: str) -> MagicMock:
    pos = MagicMock()
    pos.side = side
    pos.count_fp = Decimal(count)
    pos.average_price_dollars = Decimal(avg)
    return pos


# ── midpoint ────────────────────────────────────────────────────────────────

def test_midpoint_yes():
    ms = _ms("0.50", "0.60")
    assert _midpoint(ms, "yes") == Decimal("0.55")


def test_midpoint_no():
    ms = _ms("0.50", "0.60")
    # mid_yes = 0.55, mid_no = 1 - 0.55 = 0.45
    assert _midpoint(ms, "no") == Decimal("0.45")


def test_midpoint_none_when_bid_missing():
    ms = _ms(None, "0.60")
    assert _midpoint(ms, "yes") is None


def test_midpoint_returns_none_when_ask_missing_yes():
    # Broken book (ask withdrawn near settlement) — skip rather than use stale bid.
    ms = _ms("0.03", None)
    assert _midpoint(ms, "yes") is None


def test_midpoint_returns_none_when_ask_missing_no():
    ms = _ms("0.03", None)
    assert _midpoint(ms, "no") is None


# ── sell price ───────────────────────────────────────────────────────────────

def test_sell_price_yes_is_bid():
    ms = _ms("0.22", "0.30")
    assert _sell_price(ms, "yes") == Decimal("0.22")


def test_sell_price_no_is_yes_ask():
    # SELL NO at yes_price_dollars=yes_ask: "fill when NO bid ≥ 1-yes_ask = no_bid". ✓
    ms = _ms("0.22", "0.30")
    assert _sell_price(ms, "no") == Decimal("0.30")


def test_sell_price_none_when_ask_missing_for_no():
    ms = _ms("0.22", None)
    assert _sell_price(ms, "no") is None


# ── trailing loss ratio ──────────────────────────────────────────────────────

def test_trailing_loss_ratio_no_drop():
    # peak == current → 0% trailing loss
    assert _trailing_loss_ratio(Decimal("0.80"), Decimal("0.80")) == pytest.approx(0.0)


def test_trailing_loss_ratio_10_pct():
    # peak=0.80, current=0.72 → (0.80-0.72)/0.80 = 0.10
    assert _trailing_loss_ratio(Decimal("0.80"), Decimal("0.72")) == pytest.approx(0.10)


def test_trailing_loss_ratio_exceeds_threshold():
    # peak=0.80, current=0.70 → 12.5% drop
    ratio = _trailing_loss_ratio(Decimal("0.80"), Decimal("0.70"))
    assert ratio > 0.10


def test_trailing_loss_ratio_zero_peak():
    assert _trailing_loss_ratio(Decimal("0.00"), Decimal("0.50")) == 0.0


# ── peak price from history ──────────────────────────────────────────────────

def _ph(mid: str, observed_at: datetime | None = None) -> MagicMock:
    row = MagicMock()
    row.mid_dollars = Decimal(mid)
    row.observed_at = observed_at or datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    return row


def test_peak_price_yes_side():
    rows = [_ph("0.70"), _ph("0.85"), _ph("0.78")]
    assert _peak_price_from_history(rows, "yes") == Decimal("0.85")


def test_peak_price_no_side():
    # NO side price = 1 - mid_yes; peak for NO is where mid_yes is lowest
    rows = [_ph("0.30"), _ph("0.20"), _ph("0.25")]
    # side prices: 0.70, 0.80, 0.75 → peak = 0.80
    assert _peak_price_from_history(rows, "no") == Decimal("0.80")


def test_peak_price_none_on_empty():
    assert _peak_price_from_history([], "yes") is None


def test_peak_price_skips_none_mid():
    rows = [_ph("0.70"), MagicMock(mid_dollars=None), _ph("0.85")]
    assert _peak_price_from_history(rows, "yes") == Decimal("0.85")


def test_peak_price_ignores_highs_before_position_opened():
    opened_at = datetime(2025, 1, 1, 12, 5, tzinfo=UTC)
    rows = [
        _ph("0.92", datetime(2025, 1, 1, 12, 0, tzinfo=UTC)),
        _ph("0.70", datetime(2025, 1, 1, 12, 5, tzinfo=UTC)),
        _ph("0.74", datetime(2025, 1, 1, 12, 6, tzinfo=UTC)),
    ]

    assert _peak_price_from_history(rows, "yes", opened_at=opened_at) == Decimal("0.74")


def _fill(action: str, count: str, created_at: datetime, *, side: str = "yes") -> MagicMock:
    fill = MagicMock()
    fill.market_ticker = "WX-TEST"
    fill.side = side
    fill.action = action
    fill.count_fp = Decimal(count)
    fill.created_at = created_at
    return fill


def test_position_opened_at_from_fills_uses_current_open_lot():
    position = MagicMock()
    position.market_ticker = "WX-TEST"
    position.side = "yes"
    first_open = datetime(2025, 1, 1, 10, 0, tzinfo=UTC)
    closed = datetime(2025, 1, 1, 10, 30, tzinfo=UTC)
    reopened = datetime(2025, 1, 1, 11, 15, tzinfo=UTC)

    opened_at = _position_opened_at_from_fills(
        position,
        [
            _fill("buy", "5.00", first_open),
            _fill("sell", "5.00", closed),
            _fill("buy", "3.00", reopened),
            _fill("buy", "2.00", reopened + timedelta(minutes=5)),
        ],
    )

    assert opened_at == reopened


# ── momentum slope ───────────────────────────────────────────────────────────

def _price_rows(slope_cents_per_min: float, n: int = 10, start_price: float = 0.50) -> list:
    """Build synthetic MarketPriceHistory-like mocks with a given slope in ¢/min."""
    base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    rows = []
    for i in range(n):
        row = MagicMock()
        t = base + timedelta(seconds=i * 60)
        row.observed_at = t
        price_dollars = start_price + (slope_cents_per_min / 100) * i  # ¢/min → $/step (1 step = 1 min)
        row.mid_dollars = Decimal(str(round(price_dollars, 6)))
        rows.append(row)
    return rows


def test_momentum_slope_flat():
    rows = _price_rows(0.0)
    slope = _momentum_slope(rows)
    assert slope is not None
    assert abs(slope) < 0.01  # flat ≈ 0 ¢/min


def test_momentum_slope_negative():
    rows = _price_rows(-1.0)
    slope = _momentum_slope(rows)
    assert slope is not None
    assert slope == pytest.approx(-1.0, abs=0.05)


def test_momentum_slope_positive():
    rows = _price_rows(2.0)
    slope = _momentum_slope(rows)
    assert slope is not None
    assert slope == pytest.approx(2.0, abs=0.05)


def test_momentum_slope_none_on_too_few_points():
    rows = _price_rows(-1.0, n=4)
    assert _momentum_slope(rows) is None


def test_momentum_slope_none_when_mid_dollars_missing():
    rows = _price_rows(-1.0, n=10)
    for row in rows:
        row.mid_dollars = None
    assert _momentum_slope(rows) is None


def test_momentum_slope_ignores_none_mid_dollars():
    rows = _price_rows(-1.0, n=10)
    # Null out 4 rows, leaving 6 valid — still enough
    for row in rows[:4]:
        row.mid_dollars = None
    slope = _momentum_slope(rows)
    assert slope is not None
    assert slope < 0
