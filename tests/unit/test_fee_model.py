from __future__ import annotations

from decimal import Decimal

import pytest

from kalshi_bot.services.fee_model import (
    estimate_kalshi_fee_for_role,
    estimate_kalshi_fill_fee,
    estimate_kalshi_maker_fee_dollars,
    estimate_kalshi_taker_fee_dollars,
    extract_kalshi_raw_fee_dollars,
)


def test_taker_fee_is_symmetric_between_yes_and_no_prices() -> None:
    fee_30 = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.30"),
        fee_rate=Decimal("0.07"),
    )
    fee_70 = estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.70"),
        fee_rate=Decimal("0.07"),
    )

    assert fee_30 == Decimal("0.02")
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

    assert one_contract == Decimal("0.02")
    assert ten_contracts == Decimal("0.18")
    assert doubled_rate == Decimal("0.04")


def test_taker_fee_can_return_raw_unrounded_estimate() -> None:
    assert estimate_kalshi_taker_fee_dollars(
        price_dollars=Decimal("0.30"),
        fee_rate=Decimal("0.07"),
        round_up_to_cent=False,
    ) == Decimal("0.0147")


def test_maker_fee_uses_role_rate_and_can_be_disabled() -> None:
    assert estimate_kalshi_maker_fee_dollars(
        price_dollars=Decimal("0.50"),
        count=Decimal("1"),
    ) == Decimal("0.01")
    assert estimate_kalshi_maker_fee_dollars(
        price_dollars=Decimal("0.50"),
        count=Decimal("1"),
        maker_fee_applies=False,
    ) == Decimal("0")
    assert estimate_kalshi_fee_for_role(
        role="maker",
        price_dollars=Decimal("0.50"),
        count=Decimal("10"),
        maker_fee_rate=Decimal("0.0175"),
    ) == Decimal("0.05")


def test_raw_fee_extraction_prefers_role_specific_keys_and_reports_missing() -> None:
    maker = extract_kalshi_raw_fee_dollars(
        {"maker_fees_dollars": "0.0300", "fee_cost": "0.9900"},
        role="maker",
    )
    assert maker.amount_dollars == Decimal("0.0300")
    assert maker.fee_source == "exchange_raw:maker_fees_dollars"
    assert maker.missing is False

    missing = extract_kalshi_raw_fee_dollars({}, role="taker")
    assert missing.amount_dollars == Decimal("0")
    assert missing.fee_source == "missing"
    assert missing.missing is True

    unknown_role = extract_kalshi_raw_fee_dollars({"maker_fee": "0.0200"})
    assert unknown_role.amount_dollars == Decimal("0.0200")
    assert unknown_role.fee_source == "exchange_raw:maker_fee"


def test_fill_fee_prefers_exchange_raw_and_falls_back_to_estimate() -> None:
    raw = estimate_kalshi_fill_fee(
        raw={"taker_fees_dollars": "0.04"},
        role="taker",
        price_dollars=Decimal("0.50"),
        count=Decimal("1"),
    )
    assert raw.amount_dollars == Decimal("0.0400")
    assert raw.estimated is False

    estimated = estimate_kalshi_fill_fee(
        raw={},
        role="taker",
        price_dollars=Decimal("0.50"),
        count=Decimal("1"),
    )
    assert estimated.amount_dollars == Decimal("0.02")
    assert estimated.fee_source == "estimated_taker"
    assert estimated.estimated is True


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
