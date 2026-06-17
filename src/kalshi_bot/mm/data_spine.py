"""Data spine: multi-venue spot consolidation + market-tick normalization (plan §4.1).

Pure transforms — the loop supplies live quotes (via existing integrations) and
persistence. A consolidated spot mid is volume/recency-weighted across venues,
with per-venue staleness guards and median-based outlier rejection, so the fair
value isn't driven by one stale or glitching feed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from statistics import median
from typing import Any


@dataclass(frozen=True)
class VenueQuote:
    venue: str
    price: Decimal
    volume: Decimal = Decimal("0")
    age_seconds: float = 0.0


@dataclass(frozen=True)
class ConsolidatedSpot:
    price: Decimal
    venues: list[str]
    max_age_seconds: float
    venue_count: int = field(default=0)


def consolidate_spot(
    quotes: list[VenueQuote],
    *,
    max_stale_seconds: float = 10.0,
    outlier_bps: float = 50.0,
) -> ConsolidatedSpot | None:
    """Volume/recency-weighted consolidated spot mid.

    Drops venues older than ``max_stale_seconds`` or with non-positive price,
    rejects quotes more than ``outlier_bps`` from the median, then weights the
    survivors by ``volume`` and recency (``1/(1+age)``). Returns ``None`` when no
    usable quote remains — the caller should stand down rather than trust a
    single stale feed.
    """
    fresh = [q for q in quotes if q.age_seconds <= max_stale_seconds and q.price > 0]
    if not fresh:
        return None
    med = median([q.price for q in fresh])
    if med <= 0:
        return None
    kept = [q for q in fresh if abs(q.price - med) / med * Decimal("10000") <= Decimal(str(outlier_bps))]
    if not kept:
        return None
    total_weight = Decimal("0")
    weighted_price = Decimal("0")
    for q in kept:
        recency = Decimal(str(1.0 / (1.0 + max(0.0, q.age_seconds))))
        weight = (max(q.volume, Decimal("0")) + Decimal("1")) * recency  # +1 so zero-volume venues still count
        weighted_price += q.price * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return ConsolidatedSpot(
        price=(weighted_price / total_weight),
        venues=[q.venue for q in kept],
        max_age_seconds=max(q.age_seconds for q in kept),
        venue_count=len(kept),
    )


def seconds_to_close(*, now_ts: float, close_ts: float) -> int:
    """Remaining seconds to settlement, clamped at zero."""
    return max(0, int(close_ts - now_ts))


def build_market_tick(
    *,
    market_ticker: str,
    asset_symbol: str,
    floor_strike: Decimal,
    spot: ConsolidatedSpot,
    yes_bid: Decimal | None,
    yes_ask: Decimal | None,
    bid_size: Decimal | None,
    ask_size: Decimal | None,
    seconds_to_close: int,
    observed_ts: float,
) -> dict[str, Any]:
    """Normalize one market observation into the logged spine row schema."""
    synthetic_mid = None
    if yes_bid is not None and yes_ask is not None:
        synthetic_mid = (yes_bid + yes_ask) / Decimal("2")
    return {
        "market_ticker": market_ticker,
        "asset_symbol": asset_symbol,
        "floor_strike": str(floor_strike),
        "spot_price": str(spot.price),
        "spot_venues": ",".join(spot.venues),
        "spot_max_age_seconds": spot.max_age_seconds,
        "yes_bid_dollars": None if yes_bid is None else str(yes_bid),
        "yes_ask_dollars": None if yes_ask is None else str(yes_ask),
        "yes_bid_size": None if bid_size is None else str(bid_size),
        "yes_ask_size": None if ask_size is None else str(ask_size),
        "synthetic_mid_dollars": None if synthetic_mid is None else str(synthetic_mid),
        "seconds_to_close": seconds_to_close,
        "observed_ts": observed_ts,
    }
