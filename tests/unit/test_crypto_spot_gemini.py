"""Gemini secondary spot venue — parser, pair mapping, time-frame, client.

Gemini is a US-accessible CF-Benchmarks constituent exchange. Adding it as an
additive 3rd venue (alongside Coinbase + Kraken) widens the multi-venue spot
coverage that the settlement-basis-alignment plan needs (see
docs/research/2026-06-22-multi-venue-settlement-basis-plan.md). Mirrors the
Kraken venue tests in test_crypto_orderbook_and_kraken.py.

Gemini v2 candles API returns a JSON array of
[time_ms, open, high, low, close, volume], most-recent-first.
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from kalshi_bot.integrations.crypto_spot import (
    GEMINI_PAIRS,
    GeminiSpotClient,
    _gemini_time_frame,
    _parse_gemini_ohlc_payload,
)

# Most-recent-first, millisecond timestamps (matches the live Gemini shape).
GEMINI_OHLC_FIXTURE = [
    [1_777_800_900_000, "108.0", "112.0", "107.0", "111.0", "3.25"],
    [1_777_800_000_000, "100.0", "110.0", "99.0", "108.0", "12.5"],
]


def test_parse_gemini_ohlc_payload_basic() -> None:
    rows = _parse_gemini_ohlc_payload(
        GEMINI_OHLC_FIXTURE,
        symbol="BTC",
        requested_symbol="btcusd",
        interval_seconds=900,
    )
    assert len(rows) == 2
    # Returned ascending by start_ts regardless of input ordering.
    first = rows[0]
    assert first.provider == "gemini"
    assert first.source_kind == "spot_ohlc"
    assert first.asset_symbol == "BTC"
    assert first.source_id == "btcusd"
    assert first.start_ts == datetime.fromtimestamp(1_777_800_000, UTC)
    assert first.end_ts == datetime.fromtimestamp(1_777_800_900, UTC)
    assert first.open_dollars == Decimal("100.0")
    assert first.high_dollars == Decimal("110.0")
    assert first.low_dollars == Decimal("99.0")
    assert first.close_dollars == Decimal("108.0")
    assert first.volume == Decimal("12.5")
    assert rows[1].end_ts == datetime.fromtimestamp(1_777_801_800, UTC)


def test_parse_gemini_ohlc_payload_window_filter() -> None:
    rows = _parse_gemini_ohlc_payload(
        GEMINI_OHLC_FIXTURE,
        symbol="BTC",
        requested_symbol="btcusd",
        interval_seconds=900,
        start=datetime.fromtimestamp(1_777_800_900, UTC),
        end=datetime.fromtimestamp(1_777_801_800, UTC),
    )
    assert [row.end_ts for row in rows] == [datetime.fromtimestamp(1_777_801_800, UTC)]


def test_parse_gemini_ohlc_payload_handles_garbage() -> None:
    for bad in ({}, [], ["nope"], [[1]], None):
        assert (
            _parse_gemini_ohlc_payload(bad, symbol="BTC", requested_symbol="btcusd", interval_seconds=900)
            == []
        )


def test_gemini_pair_mapping_skips_bnb_and_hype() -> None:
    assert GEMINI_PAIRS["BNB"] is None
    assert GEMINI_PAIRS["HYPE"] is None
    assert GEMINI_PAIRS["BTC"] == "btcusd"
    for asset in ("ETH", "SOL", "XRP", "DOGE"):
        assert GEMINI_PAIRS[asset] == f"{asset.lower()}usd"


def test_gemini_time_frame_mapping() -> None:
    assert _gemini_time_frame(60) == "1m"
    assert _gemini_time_frame(900) == "15m"
    assert _gemini_time_frame(3600) == "1hr"
    with pytest.raises(ValueError):
        _gemini_time_frame(7)


@pytest.mark.asyncio
async def test_gemini_spot_client_fetch_ohlc_no_network() -> None:
    captured: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url)
        return httpx.Response(200, json=GEMINI_OHLC_FIXTURE)

    client = GeminiSpotClient()
    await client.client.aclose()
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.base_url,
        headers={"Accept": "application/json"},
    )
    try:
        rows = await client.fetch_ohlc(
            "BTC",
            start=datetime.fromtimestamp(1_777_800_000, UTC),
            end=datetime.fromtimestamp(1_777_801_800, UTC),
            interval_seconds=900,
        )
        with pytest.raises(KeyError):
            await client.fetch_ohlc(
                "BNB",
                start=datetime.fromtimestamp(1_777_800_000, UTC),
                end=datetime.fromtimestamp(1_777_801_800, UTC),
                interval_seconds=900,
            )
    finally:
        await client.aclose()

    assert len(captured) == 1
    assert captured[0].path == "/v2/candles/btcusd/15m"
    assert [row.provider for row in rows] == ["gemini", "gemini"]
