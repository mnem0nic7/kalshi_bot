"""Kalshi fee-model helpers."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from typing import Any, Literal

KALSHI_TAKER_FEE_V1 = "kalshi_taker_fee_v1"
KALSHI_TAKER_FEE_V2 = "kalshi_taker_fee_v2_cent_ceiling"
KALSHI_ROLE_AWARE_FEE_V1 = "kalshi_role_aware_fee_v1"

KALSHI_DEFAULT_TAKER_FEE_RATE = Decimal("0.07")
KALSHI_DEFAULT_MAKER_FEE_RATE = Decimal("0.0175")

FeeRole = Literal["taker", "maker"]


@dataclass(frozen=True, slots=True)
class FeeEstimate:
    amount_dollars: Decimal
    fee_source: str
    role: FeeRole | None
    fee_model_version: str
    missing: bool = False
    estimated: bool = False


def current_fee_model_version() -> str:
    return KALSHI_ROLE_AWARE_FEE_V1


def estimate_kalshi_fee_dollars(
    *,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal,
    round_up_to_cent: bool = True,
) -> Decimal:
    """Compute a Kalshi trade fee in dollars for a single-side trade.

    Formula: ``round up(fee_rate * count * price * (1 - price))``.

    The formula is symmetric: ``fee(0.30) == fee(0.70)``, so callers may pass
    either the YES price or the NO price. ``price_dollars`` must be in dollars
    on the ``[0, 1]`` interval, not cents.
    """
    price = Decimal(str(price_dollars))
    contracts = Decimal(str(count))
    rate = Decimal(str(fee_rate))
    if price < Decimal("0") or price > Decimal("1"):
        raise ValueError("price_dollars must be between 0 and 1")
    if contracts < Decimal("0"):
        raise ValueError("count must be non-negative")
    raw_fee = rate * contracts * price * (Decimal("1") - price)
    if not round_up_to_cent or raw_fee <= Decimal("0"):
        return raw_fee
    return (raw_fee * Decimal("100")).to_integral_value(rounding=ROUND_CEILING) / Decimal("100")


def estimate_kalshi_taker_fee_dollars(
    *,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal = KALSHI_DEFAULT_TAKER_FEE_RATE,
    round_up_to_cent: bool = True,
) -> Decimal:
    """Compute Kalshi taker fee in dollars for a single-side trade."""
    return estimate_kalshi_fee_dollars(
        price_dollars=price_dollars,
        count=count,
        fee_rate=fee_rate,
        round_up_to_cent=round_up_to_cent,
    )


def estimate_kalshi_maker_fee_dollars(
    *,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal = KALSHI_DEFAULT_MAKER_FEE_RATE,
    maker_fee_applies: bool = True,
    round_up_to_cent: bool = True,
) -> Decimal:
    """Compute Kalshi maker fee in dollars when maker fees apply."""
    if not maker_fee_applies:
        return Decimal("0")
    return estimate_kalshi_fee_dollars(
        price_dollars=price_dollars,
        count=count,
        fee_rate=fee_rate,
        round_up_to_cent=round_up_to_cent,
    )


def estimate_kalshi_fee_for_role(
    *,
    role: FeeRole,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    taker_fee_rate: Decimal = KALSHI_DEFAULT_TAKER_FEE_RATE,
    maker_fee_rate: Decimal = KALSHI_DEFAULT_MAKER_FEE_RATE,
    maker_fee_applies: bool = True,
    round_up_to_cent: bool = True,
) -> Decimal:
    normalized = str(role).strip().lower()
    if normalized == "taker":
        return estimate_kalshi_taker_fee_dollars(
            price_dollars=price_dollars,
            count=count,
            fee_rate=taker_fee_rate,
            round_up_to_cent=round_up_to_cent,
        )
    if normalized == "maker":
        return estimate_kalshi_maker_fee_dollars(
            price_dollars=price_dollars,
            count=count,
            fee_rate=maker_fee_rate,
            maker_fee_applies=maker_fee_applies,
            round_up_to_cent=round_up_to_cent,
        )
    raise ValueError("role must be taker or maker")


_RAW_FEE_KEYS_BY_ROLE: dict[FeeRole, tuple[str, ...]] = {
    "taker": ("taker_fees_dollars", "taker_fee_dollars", "taker_fee", "taker_fees"),
    "maker": ("maker_fees_dollars", "maker_fee_dollars", "maker_fee", "maker_fees"),
}
_RAW_FEE_KEYS_GENERIC: tuple[str, ...] = (
    "fee_cost",
    "fee_dollars",
    "fees_dollars",
    "fee",
    "fees",
)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def extract_kalshi_raw_fee_dollars(raw: Any, *, role: FeeRole | None = None) -> FeeEstimate:
    """Extract exchange-supplied fee dollars from a raw order/fill payload."""
    payload = raw if isinstance(raw, dict) else {}
    role_keys = (
        _RAW_FEE_KEYS_BY_ROLE.get(role, ())
        if role is not None
        else (*_RAW_FEE_KEYS_BY_ROLE["taker"], *_RAW_FEE_KEYS_BY_ROLE["maker"])
    )
    for key in (*role_keys, *_RAW_FEE_KEYS_GENERIC):
        value = _decimal_or_none(payload.get(key))
        if value is not None:
            return FeeEstimate(
                amount_dollars=value.quantize(Decimal("0.0001")),
                fee_source=f"exchange_raw:{key}",
                role=role,
                fee_model_version=current_fee_model_version(),
            )
    return FeeEstimate(
        amount_dollars=Decimal("0"),
        fee_source="missing",
        role=role,
        fee_model_version=current_fee_model_version(),
        missing=True,
    )


def estimate_kalshi_fill_fee(
    *,
    raw: Any,
    role: FeeRole,
    price_dollars: Decimal,
    count: Decimal,
    taker_fee_rate: Decimal = KALSHI_DEFAULT_TAKER_FEE_RATE,
    maker_fee_rate: Decimal = KALSHI_DEFAULT_MAKER_FEE_RATE,
    maker_fee_applies: bool = True,
    prefer_raw: bool = True,
) -> FeeEstimate:
    if prefer_raw:
        raw_fee = extract_kalshi_raw_fee_dollars(raw, role=role)
        if not raw_fee.missing:
            return raw_fee
    amount = estimate_kalshi_fee_for_role(
        role=role,
        price_dollars=price_dollars,
        count=count,
        taker_fee_rate=taker_fee_rate,
        maker_fee_rate=maker_fee_rate,
        maker_fee_applies=maker_fee_applies,
    )
    return FeeEstimate(
        amount_dollars=amount,
        fee_source=f"estimated_{role}",
        role=role,
        fee_model_version=current_fee_model_version(),
        missing=False,
        estimated=True,
    )
