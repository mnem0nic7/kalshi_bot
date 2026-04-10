from decimal import Decimal

import pytest

from kalshi_bot.core.fixed_point import dollars_to_cents, make_client_order_id, quantize_count, quantize_price


def test_quantize_price_accepts_valid_probability() -> None:
    assert quantize_price("0.56789") == Decimal("0.5679")


def test_quantize_price_rejects_out_of_bounds() -> None:
    with pytest.raises(ValueError):
        quantize_price("1.1000")


def test_quantize_count_requires_positive_value() -> None:
    with pytest.raises(ValueError):
        quantize_count("0")


def test_dollars_to_cents_rounds_half_up() -> None:
    assert dollars_to_cents("0.125") == 13


def test_client_order_id_is_stable() -> None:
    assert make_client_order_id("room-a", "WX-1", "abc") == make_client_order_id("room-a", "WX-1", "abc")

