from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from kalshi_bot.services.stop_loss import _loss_ratio, _midpoint, _sell_price


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


def test_midpoint_none_when_ask_missing():
    ms = _ms("0.50", None)
    assert _midpoint(ms, "yes") is None


# ── sell price ───────────────────────────────────────────────────────────────

def test_sell_price_yes_is_bid():
    ms = _ms("0.22", "0.30")
    assert _sell_price(ms, "yes") == Decimal("0.22")


def test_sell_price_no_is_complement_of_ask():
    # no_bid = 1 - yes_ask = 1 - 0.30 = 0.70
    ms = _ms("0.22", "0.30")
    assert _sell_price(ms, "no") == Decimal("0.7000")


def test_sell_price_none_when_ask_missing_for_no():
    ms = _ms("0.22", None)
    assert _sell_price(ms, "no") is None


# ── loss ratio ───────────────────────────────────────────────────────────────

def test_loss_ratio_no_loss():
    pos = _pos("yes", "10", "0.60")
    # mid same as avg → no loss
    ratio = _loss_ratio(pos, Decimal("0.60"))
    assert ratio == pytest.approx(0.0)


def test_loss_ratio_50_pct():
    pos = _pos("yes", "10", "0.60")
    # cost = 6.00, mark = 10 * 0.30 = 3.00, loss = 3.00 / 6.00 = 0.50
    ratio = _loss_ratio(pos, Decimal("0.30"))
    assert ratio == pytest.approx(0.50)


def test_loss_ratio_exact_threshold_triggers():
    pos = _pos("yes", "5", "0.40")
    # cost = 2.00, mark = 5 * 0.20 = 1.00, loss = 1.00/2.00 = 0.50
    ratio = _loss_ratio(pos, Decimal("0.20"))
    assert ratio is not None and ratio >= 0.50


def test_loss_ratio_below_threshold():
    pos = _pos("yes", "10", "0.60")
    # mid = 0.40 → loss = (6.00 - 4.00) / 6.00 = 0.333
    ratio = _loss_ratio(pos, Decimal("0.40"))
    assert ratio is not None and ratio < 0.50


def test_loss_ratio_none_on_zero_count():
    pos = _pos("yes", "0", "0.60")
    assert _loss_ratio(pos, Decimal("0.30")) is None


def test_loss_ratio_none_on_zero_avg():
    pos = _pos("yes", "10", "0.00")
    assert _loss_ratio(pos, Decimal("0.30")) is None


def test_loss_ratio_profit_is_negative():
    pos = _pos("yes", "10", "0.40")
    # mid > avg → profit, loss_ratio negative
    ratio = _loss_ratio(pos, Decimal("0.70"))
    assert ratio is not None and ratio < 0.0


def test_loss_ratio_no_position():
    pos = _pos("no", "10", "0.35")
    # mid_no = 0.20, cost = 3.50, mark = 2.00, loss = 1.50/3.50 ≈ 0.43
    ratio = _loss_ratio(pos, Decimal("0.20"))
    assert ratio is not None
    assert ratio == pytest.approx(1.50 / 3.50, rel=1e-4)
