from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from uuid import UUID


PRICE_TICK = Decimal("0.0001")
COUNT_TICK = Decimal("0.01")
LOWER_PRICE_BOUND = Decimal("0")
UPPER_PRICE_BOUND = Decimal("1")


def as_decimal(value: str | float | int | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value}") from exc


def quantize_price(value: str | float | int | Decimal) -> Decimal:
    price = as_decimal(value).quantize(PRICE_TICK, rounding=ROUND_HALF_UP)
    if price < LOWER_PRICE_BOUND or price > UPPER_PRICE_BOUND:
        raise ValueError("Price must be between 0 and 1 dollars inclusive")
    return price


def quantize_count(value: str | float | int | Decimal) -> Decimal:
    count = as_decimal(value).quantize(COUNT_TICK, rounding=ROUND_HALF_UP)
    if count <= 0:
        raise ValueError("Count must be positive")
    return count


def dollars_to_cents(value: str | float | int | Decimal) -> int:
    return int((as_decimal(value) * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def make_client_order_id(room_id: UUID | str, market_ticker: str, nonce: str) -> str:
    basis = f"{room_id}:{market_ticker}:{nonce}".encode("utf-8")
    digest = hashlib.blake2b(basis, digest_size=10).hexdigest()
    return f"room:{digest}"

