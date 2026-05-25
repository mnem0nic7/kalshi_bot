"""Unit tests for CryptoTakeProfitService helpers and profit threshold logic."""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from kalshi_bot.services.crypto_take_profit import (
    _crypto_market_identity,
    _crypto_mid,
    _crypto_sell_price,
    _crypto_take_profit_frequencies,
    _profit_ratio,
)


def _snapshot(yes_bid: str | None, yes_ask: str | None, status: str = "open") -> MagicMock:
    snap = MagicMock()
    snap.yes_bid_dollars = Decimal(yes_bid) if yes_bid else None
    snap.yes_ask_dollars = Decimal(yes_ask) if yes_ask else None
    snap.status = status
    return snap


def _position(avg: str, count: str = "100.00") -> MagicMock:
    pos = MagicMock()
    pos.average_price_dollars = Decimal(avg)
    pos.count_fp = Decimal(count)
    return pos


# --- _crypto_mid ---


def test_crypto_market_identity_detects_1h_frequency():
    assert _crypto_market_identity("KXBTC1H-26MAY24-B123456-T123456") == ("BTC", "1h")


def test_crypto_market_identity_detects_15m_frequency():
    assert _crypto_market_identity("KXHYPE15M-26MAY24-B123456-T123456") == ("HYPE", "15m")


def test_take_profit_frequencies_accepts_hour_aliases():
    assert _crypto_take_profit_frequencies("1hour") == {"1h"}
    assert _crypto_take_profit_frequencies("15m,1h") == {"15m", "1h"}


def test_mid_yes_side():
    snap = _snapshot("0.58", "0.62")
    assert _crypto_mid(snap, "yes") == Decimal("0.60")


def test_mid_no_side():
    snap = _snapshot("0.58", "0.62")
    assert _crypto_mid(snap, "no") == Decimal("0.40")


def test_mid_missing_bid_returns_none():
    snap = _snapshot(None, "0.62")
    assert _crypto_mid(snap, "yes") is None


def test_mid_missing_ask_returns_none():
    snap = _snapshot("0.58", None)
    assert _crypto_mid(snap, "yes") is None


# --- _crypto_sell_price ---

def test_sell_price_yes_uses_bid():
    snap = _snapshot("0.58", "0.62")
    assert _crypto_sell_price(snap, "yes") == Decimal("0.58")


def test_sell_price_no_uses_ask():
    # NO sell → corresponding YES buy price is the YES ask
    snap = _snapshot("0.58", "0.62")
    assert _crypto_sell_price(snap, "no") == Decimal("0.62")


def test_sell_price_zero_bid_returns_none():
    snap = _snapshot("0.00", "0.62")
    assert _crypto_sell_price(snap, "yes") is None


# --- _profit_ratio ---

def test_profit_ratio_at_exactly_20pct():
    pos = _position("0.50")
    mid = Decimal("0.60")
    ratio = _profit_ratio(pos, mid)
    assert ratio == pytest.approx(0.20)


def test_profit_ratio_below_threshold():
    pos = _position("0.50")
    mid = Decimal("0.55")
    ratio = _profit_ratio(pos, mid)
    assert ratio == pytest.approx(0.10)


def test_profit_ratio_above_threshold():
    pos = _position("0.50")
    mid = Decimal("0.65")
    ratio = _profit_ratio(pos, mid)
    assert ratio == pytest.approx(0.30)


def test_profit_ratio_zero_avg_returns_none():
    pos = _position("0.00")
    assert _profit_ratio(pos, Decimal("0.60")) is None


def test_profit_ratio_zero_count_returns_none():
    pos = _position("0.50", count="0.00")
    assert _profit_ratio(pos, Decimal("0.60")) is None


# --- threshold boundary ---

def test_threshold_exactly_at_boundary_triggers():
    pos = _position("0.50")
    mid = Decimal("0.60")  # exactly +20%
    ratio = _profit_ratio(pos, mid)
    assert ratio is not None
    assert ratio >= 0.20


def test_threshold_just_below_does_not_trigger():
    pos = _position("0.50")
    mid = Decimal("0.5999")  # ~19.98%
    ratio = _profit_ratio(pos, mid)
    assert ratio is not None
    assert ratio < 0.20
