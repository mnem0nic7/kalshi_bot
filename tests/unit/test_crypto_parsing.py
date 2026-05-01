from __future__ import annotations

from decimal import Decimal

from kalshi_bot.crypto.parsing import (
    asset_symbol_from_series,
    normalize_candlestick,
    parse_crypto_market,
    parse_crypto_series,
)


def test_crypto_series_discovery_filters_crypto_fifteen_minute() -> None:
    rows = [
        {"ticker": "KXBTC15M", "title": "Bitcoin price up down", "category": "Crypto", "frequency": "fifteen_min"},
        {"ticker": "KXBTC1H", "title": "Bitcoin hourly", "category": "Crypto", "frequency": "hourly"},
        {"ticker": "KXHIGHNY", "title": "NY high temp", "category": "Weather", "frequency": "fifteen_min"},
    ]

    parsed = [parse_crypto_series(row) for row in rows]

    assert parsed[0] is not None
    assert parsed[0].series_ticker == "KXBTC15M"
    assert parsed[0].asset_symbol == "BTC"
    assert parsed[1] is None
    assert parsed[2] is None


def test_asset_symbol_extraction_handles_x_prefix_and_future_assets() -> None:
    assert asset_symbol_from_series("KXXRP15M") == "XRP"
    assert asset_symbol_from_series("KXHYPE15M") == "HYPE"
    assert asset_symbol_from_series("KXBCH15M") == "BCH"
    assert asset_symbol_from_series({"ticker": "KXADA15M", "tags": ["Crypto", "ADA"]}) == "ADA"


def test_crypto_market_parser_extracts_target_quotes_and_times() -> None:
    series = parse_crypto_series(
        {"ticker": "KXETH15M", "title": "ETH 15M", "category": "Crypto", "frequency": "fifteen_min"}
    )
    market = parse_crypto_market(
        {
            "ticker": "KXETH15M-26APR30-B2633",
            "series_ticker": "KXETH15M",
            "floor_strike": "2633.74",
            "close_time": "2026-04-30T17:15:00Z",
            "yes_bid": 47,
            "yes_ask": 50,
            "no_ask": 53,
            "last_price": 49,
            "volume": "5293",
            "open_interest": 77,
            "status": "open",
        },
        series=series,
    )

    assert market is not None
    assert market.asset_symbol == "ETH"
    assert market.frequency == "15m"
    assert market.target_price_dollars == Decimal("2633.74000000")
    assert market.yes_bid_dollars == Decimal("0.4700")
    assert market.yes_ask_dollars == Decimal("0.5000")
    assert market.no_ask_dollars == Decimal("0.5300")
    assert market.mid_yes_dollars == Decimal("0.4850")
    assert market.volume == 5293
    assert market.close_time is not None


def test_candlestick_normalization_accepts_official_nested_shape() -> None:
    candle = normalize_candlestick(
        {
            "end_period_ts": 1777568400,
            "period_interval": 1,
            "yes_ask": {"open": 49, "high": 52, "low": 48, "close": 51},
            "volume": "12",
        }
    )

    assert candle is not None
    assert candle["period_interval"] == 1
    assert candle["open_dollars"] == Decimal("0.4900")
    assert candle["close_dollars"] == Decimal("0.5100")
    assert candle["volume"] == 12
