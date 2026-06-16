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
    assert asset_symbol_from_series({"ticker": "KXSOLE", "title": "SOL Range", "category": "Crypto"}) == "SOL"
    assert asset_symbol_from_series({"ticker": "KXRIPPLE", "title": "Ripple Range", "category": "Crypto"}) == "XRP"


def test_crypto_series_discovery_accepts_hourly_crypto_frequency() -> None:
    series = parse_crypto_series(
        {"ticker": "KXBTC", "title": "Bitcoin range", "category": "Crypto", "frequency": "hourly"},
        frequency="1h",
    )

    assert series is not None
    assert series.series_ticker == "KXBTC"
    assert series.asset_symbol == "BTC"
    assert series.frequency == "1h"


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


def test_crypto_market_parser_infers_hourly_duration_and_ignores_title_dates() -> None:
    market = parse_crypto_market(
        {
            "ticker": "KXBTC-26MAY16-T0900-B105000",
            "series_ticker": "KXBTC",
            "title": "Bitcoin price range on May 16",
            "yes_sub_title": "Above $105,000 at 9am",
            "open_time": "2026-05-16T15:00:00Z",
            "close_time": "2026-05-16T16:00:00Z",
            "yes_bid": 48,
            "yes_ask": 52,
            "status": "open",
        }
    )

    assert market is not None
    assert market.asset_symbol == "BTC"
    assert market.frequency == "1h"
    assert market.target_price_dollars == Decimal("105000.00000000")


def test_crypto_market_parser_accepts_hourly_d_suffix_listing_window() -> None:
    series = parse_crypto_series(
        {"ticker": "KXBTCD", "title": "Bitcoin hourly", "category": "Crypto", "frequency": "hourly"},
        frequency="1h",
    )

    market = parse_crypto_market(
        {
            "ticker": "KXBTCD-26JUN1617-T75249.99",
            "event_ticker": "KXBTCD-26JUN1617",
            "series_ticker": "KXBTCD",
            "floor_strike": 75249.99,
            "open_time": "2026-06-15T20:00:00Z",
            "close_time": "2026-06-16T21:00:00Z",
            "expected_expiration_time": "2026-06-16T21:05:00Z",
            "yes_bid_dollars": "0.0000",
            "yes_ask_dollars": "0.0100",
            "no_bid_dollars": "0.9900",
            "no_ask_dollars": "1.0000",
            "status": "active",
        },
        series=series,
        frequency="1h",
    )

    assert market is not None
    assert market.asset_symbol == "BTC"
    assert market.frequency == "1h"
    assert market.status == "active"
    assert market.yes_ask_dollars == Decimal("0.0100")
    assert market.target_price_dollars == Decimal("75249.99000000")


def test_crypto_market_parser_rejects_weekly_range_for_requested_hourly_series() -> None:
    series = parse_crypto_series(
        {"ticker": "KXBTC", "title": "Bitcoin range", "category": "Crypto", "frequency": "hourly"},
        frequency="1h",
    )
    market = parse_crypto_market(
        {
            "ticker": "KXBTC-26JUN0517-B73050",
            "series_ticker": "KXBTC",
            "open_time": "2026-05-29T20:00:00Z",
            "close_time": "2026-06-05T21:00:00Z",
            "yes_bid": 48,
            "yes_ask": 52,
            "status": "open",
        },
        series=series,
        frequency="1h",
    )

    assert market is None


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


def test_btci_ticker_resolves_to_btc_via_alias() -> None:
    assert asset_symbol_from_series({"ticker": "BTCI", "category": "Crypto", "frequency": "hourly"}) == "BTC"


def test_kxbtcc_ticker_resolves_to_btc_via_alias() -> None:
    assert asset_symbol_from_series({"ticker": "KXBTCC", "category": "Crypto", "frequency": "hourly"}) == "BTC"


def test_crypto_market_parser_reads_fractional_fp_volume_fields() -> None:
    # Fractional (deci-cent) markets publish volume_fp/open_interest_fp and
    # omit the legacy integer fields entirely.
    market = parse_crypto_market(
        {
            "ticker": "KXETH15M-26JUN101500-00",
            "series_ticker": "KXETH15M",
            "floor_strike": "2633.74",
            "close_time": "2026-06-10T19:15:00Z",
            "yes_bid_dollars": "0.4725",
            "yes_ask_dollars": "0.5000",
            "volume_fp": "2826.68",
            "volume_24h_fp": "0.00",
            "open_interest_fp": "1385.07",
            "status": "open",
        },
    )

    assert market is not None
    assert market.volume == 2826
    assert market.open_interest == 1385
