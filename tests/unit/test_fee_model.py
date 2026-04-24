from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars


def test_taker_fee_is_symmetric_between_yes_and_no_prices() -> None:
    fee_30 = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.30"),
        fee_rate=Decimal("0.07"),
    )
    fee_70 = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.70"),
        fee_rate=Decimal("0.07"),
    )

    assert fee_30 == Decimal("0.0147")
    assert fee_30 == fee_70


def test_taker_fee_boundaries_scaling_and_known_value() -> None:
    assert estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.00"),
        fee_rate=Decimal("0.07"),
    ) == Decimal("0.0000")
    assert estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("1.00"),
        fee_rate=Decimal("0.07"),
    ) == Decimal("0.0000")

    one_contract = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.50"),
        count=Decimal("1"),
        fee_rate=Decimal("0.07"),
    )
    ten_contracts = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.50"),
        count=Decimal("10"),
        fee_rate=Decimal("0.07"),
    )
    doubled_rate = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.50"),
        count=Decimal("1"),
        fee_rate=Decimal("0.14"),
    )

    assert one_contract == Decimal("0.0175")
    assert ten_contracts == Decimal("0.1750")
    assert doubled_rate == Decimal("0.0350")


def test_taker_fee_rejects_invalid_price_and_count() -> None:
    with pytest.raises(ValueError, match="price_dollars"):
        estimate_kalshi_taker_fee_dollars(
            price_dollars=Decimal("1.01"),
            fee_rate=Decimal("0.07"),
        )

    with pytest.raises(ValueError, match="count"):
        estimate_kalshi_taker_fee_dollars(
            price_dollars=Decimal("0.50"),
            count=Decimal("-1"),
            fee_rate=Decimal("0.07"),
        )
