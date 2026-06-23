"""Kraken/Gemini instantaneous ticker (fetch_current) for dense cross-venue basis.

The OHLC paths give 15-min candles; for a dense, time-aligned basis we also need each venue's
instantaneous current price at ~15s (like Coinbase fetch_current), stored as a tick (interval=0).
"""
from __future__ import annotations

from decimal import Decimal

from kalshi_bot.integrations.crypto_spot import (
    _parse_gemini_ticker_payload,
    _parse_kraken_ticker_payload,
)


def test_parse_kraken_ticker_close() -> None:
    payload = {
        "error": [],
        "result": {"XXBTZUSD": {"a": ["70010.0", "1", "1.0"], "b": ["70000.0", "2", "2.0"], "c": ["70005.5", "0.01"]}},
    }
    assert _parse_kraken_ticker_payload(payload, requested_pair="XXBTZUSD") == Decimal("70005.5")


def test_parse_kraken_ticker_handles_error_and_missing() -> None:
    assert _parse_kraken_ticker_payload({"error": ["EQuery:Unknown asset pair"], "result": {}}, requested_pair="X") is None
    assert _parse_kraken_ticker_payload({"result": {"XXBTZUSD": {}}}, requested_pair="XXBTZUSD") is None
    assert _parse_kraken_ticker_payload({}, requested_pair="X") is None


def test_parse_gemini_ticker_close() -> None:
    payload = {"symbol": "BTCUSD", "open": "69000", "high": "70500", "low": "68900", "close": "70005.25", "bid": "70000", "ask": "70010"}
    assert _parse_gemini_ticker_payload(payload) == Decimal("70005.25")


def test_parse_gemini_ticker_missing() -> None:
    assert _parse_gemini_ticker_payload({"symbol": "BTCUSD"}) is None
    assert _parse_gemini_ticker_payload({}) is None
    assert _parse_gemini_ticker_payload({"close": "abc"}) is None
