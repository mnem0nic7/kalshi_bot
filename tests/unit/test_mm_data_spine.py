"""Data spine: multi-venue spot consolidation + market-tick normalization.

Plan §4.1: a consolidated spot mid from ≥2 venues, volume/recency-weighted, with
outlier rejection and per-venue staleness guards; per market record floor_strike,
synthetic mid, best bid/ask + sizes, seconds-to-close. These are the pure
transforms; the loop supplies live quotes and persistence.
"""
from __future__ import annotations

from decimal import Decimal

from kalshi_bot.mm.data_spine import VenueQuote, consolidate_spot, seconds_to_close


def test_consolidate_uses_volume_recency_weighting():
    quotes = [
        VenueQuote(venue="coinbase", price=Decimal("100.00"), volume=Decimal("9"), age_seconds=1.0),
        VenueQuote(venue="kraken", price=Decimal("100.20"), volume=Decimal("1"), age_seconds=1.0),
    ]
    spot = consolidate_spot(quotes)
    # Both within the outlier band; coinbase dominates on volume → mid near 100.0,
    # below the simple average of 100.10.
    assert spot is not None
    assert Decimal("100.00") <= spot.price < Decimal("100.10")
    assert set(spot.venues) == {"coinbase", "kraken"}


def test_consolidate_drops_stale_venues():
    quotes = [
        VenueQuote(venue="coinbase", price=Decimal("100.0"), volume=Decimal("1"), age_seconds=1.0),
        VenueQuote(venue="kraken", price=Decimal("130.0"), volume=Decimal("1"), age_seconds=999.0),
    ]
    spot = consolidate_spot(quotes, max_stale_seconds=10)
    assert spot is not None
    assert spot.venues == ["coinbase"]
    assert spot.price == Decimal("100.0")


def test_consolidate_rejects_outliers():
    quotes = [
        VenueQuote(venue="coinbase", price=Decimal("100.0"), volume=Decimal("1"), age_seconds=1.0),
        VenueQuote(venue="kraken", price=Decimal("100.1"), volume=Decimal("1"), age_seconds=1.0),
        VenueQuote(venue="rogue", price=Decimal("180.0"), volume=Decimal("1"), age_seconds=1.0),
    ]
    spot = consolidate_spot(quotes, outlier_bps=50)
    assert spot is not None
    assert "rogue" not in spot.venues
    assert Decimal("100.0") <= spot.price <= Decimal("100.1")


def test_consolidate_none_when_all_stale_or_empty():
    assert consolidate_spot([]) is None
    stale = [VenueQuote(venue="c", price=Decimal("100"), volume=Decimal("1"), age_seconds=999)]
    assert consolidate_spot(stale, max_stale_seconds=10) is None


def test_seconds_to_close_clamps_non_negative():
    assert seconds_to_close(now_ts=1000.0, close_ts=1300.0) == 300
    assert seconds_to_close(now_ts=1400.0, close_ts=1300.0) == 0
