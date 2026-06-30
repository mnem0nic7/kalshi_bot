from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import httpx
import pytest

import kalshi_bot.crypto.services as crypto_services
from kalshi_bot.config import Settings
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import CryptoSpotService, _crypto_decision_rows, _crypto_live_market_row, _crypto_trade_candidates
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.crypto_spot import (
    CoinbaseCdpCredentials,
    CoinbaseSpotClient,
    CoinGeckoSpotClient,
    SpotOHLC,
)


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto-spot.db",
        "web_auth_enabled": False,
        "crypto_min_training_samples": 2,
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.asyncio
async def test_coinbase_spot_client_parses_candles_with_end_time() -> None:
    request_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_paths.append(str(request.url))
        return httpx.Response(
            200,
            json=[
                [1_777_800_000, 100.0, 110.0, 101.0, 108.0, 12.5],
            ],
        )

    client = CoinbaseSpotClient()
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
            end=datetime.fromtimestamp(1_777_800_900, UTC),
            interval_seconds=900,
        )
    finally:
        await client.aclose()

    assert request_paths
    assert rows[0].provider == "coinbase"
    assert rows[0].source_kind == "spot_ohlc"
    assert rows[0].start_ts == datetime.fromtimestamp(1_777_800_000, UTC)
    assert rows[0].end_ts == datetime.fromtimestamp(1_777_800_900, UTC)
    assert rows[0].close_dollars == Decimal("108.0")


@pytest.mark.asyncio
async def test_coinbase_spot_client_parses_current_tick() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/products/BTC-USD/ticker"
        return httpx.Response(
            200,
            json={"price": "108.25", "volume": "12.5", "time": "2020-09-13T12:26:41Z"},
        )

    client = CoinbaseSpotClient()
    await client.client.aclose()
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.base_url,
        headers={"Accept": "application/json"},
    )
    try:
        row = await client.fetch_current("BTC")
    finally:
        await client.aclose()

    assert row is not None
    assert row.provider == "coinbase"
    assert row.source_kind == "spot_tick"
    assert row.end_ts == datetime(2020, 9, 13, 12, 26, 41, tzinfo=UTC)
    assert row.close_dollars == Decimal("108.25")


@pytest.mark.asyncio
async def test_coinbase_spot_client_prefers_authenticated_current_tick() -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    credentials = CoinbaseCdpCredentials(
        name="organizations/test-org/apiKeys/test-key",
        private_key=private_pem,
    )
    seen_authorization: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/brokerage/products/BTC-USD/ticker"
        seen_authorization.append(request.headers["Authorization"])
        return httpx.Response(
            200,
            json={"trades": [{"price": "109.25", "size": "1.5", "time": "2020-09-13T12:26:42Z"}]},
        )

    client = CoinbaseSpotClient(credentials=credentials)
    await client.authenticated_client.aclose()
    client.authenticated_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.authenticated_base_url,
        headers={"Accept": "application/json"},
    )
    try:
        row = await client.fetch_current("BTC")
    finally:
        await client.aclose()

    assert seen_authorization and seen_authorization[0].startswith("Bearer ")
    assert row is not None
    assert row.close_dollars == Decimal("109.25")
    assert row.volume == Decimal("1.5")
    assert row.payload["authenticated"] is True


@pytest.mark.asyncio
async def test_coinbase_spot_client_enriches_current_tick_with_microstructure() -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    credentials = CoinbaseCdpCredentials(
        name="organizations/test-org/apiKeys/test-key",
        private_key=private_pem,
    )
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path == "/api/v3/brokerage/products/BTC-USD/ticker":
            return httpx.Response(
                200,
                json={
                    "trades": [
                        {
                            "trade_id": "1",
                            "product_id": "BTC-USD",
                            "price": "109.25",
                            "size": "1.5",
                            "time": "2020-09-13T12:26:42Z",
                            "side": "BUY",
                            "bid": "109.20",
                            "ask": "109.30",
                        }
                    ],
                    "best_bid": "109.20",
                    "best_ask": "109.30",
                },
            )
        if request.url.path == "/api/v3/brokerage/best_bid_ask":
            return httpx.Response(
                200,
                json={
                    "pricebooks": [
                        {
                            "product_id": "BTC-USD",
                            "bids": [{"price": "109.19", "size": "2.0"}],
                            "asks": [{"price": "109.31", "size": "3.0"}],
                            "time": "2020-09-13T12:26:42Z",
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    client = CoinbaseSpotClient(credentials=credentials)
    await client.authenticated_client.aclose()
    client.authenticated_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.authenticated_base_url,
        headers={"Accept": "application/json"},
    )
    try:
        row = await client.fetch_current("BTC")
    finally:
        await client.aclose()

    assert row is not None
    assert seen_paths == ["/api/v3/brokerage/products/BTC-USD/ticker", "/api/v3/brokerage/best_bid_ask"]
    microstructure = row.payload["market_microstructure"]
    assert microstructure["recent_trade_count"] == 1
    assert microstructure["latest_trade"]["price_dollars"] == "109.25"
    assert microstructure["best_bid_ask"]["best_bid_dollars"] == "109.19"
    assert microstructure["best_bid_ask"]["best_ask_dollars"] == "109.31"
    assert microstructure["best_bid_ask"]["spread_bps"] == 11


@pytest.mark.asyncio
async def test_coinbase_spot_client_fetch_product_reports_support() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/products/BTC-USD"
        return httpx.Response(
            200,
            json={
                "id": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "status": "online",
            },
        )

    client = CoinbaseSpotClient()
    await client.client.aclose()
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.base_url,
        headers={"Accept": "application/json"},
    )
    try:
        product = await client.fetch_product("BTC-USD")
    finally:
        await client.aclose()

    assert product is not None
    assert product["id"] == "BTC-USD"
    assert product["base_currency"] == "BTC"


@pytest.mark.asyncio
async def test_coingecko_spot_client_buckets_price_history_as_proxy_ohlc() -> None:
    base_ms = int(datetime(2026, 5, 3, 9, 15, tzinfo=UTC).timestamp() * 1000)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={
                "prices": [
                    [base_ms, 100.0],
                    [base_ms + 300_000, 103.0],
                    [base_ms + 600_000, 101.0],
                ],
                "total_volumes": [
                    [base_ms, 5.0],
                    [base_ms + 300_000, 6.0],
                    [base_ms + 600_000, 7.0],
                ],
            },
        )

    client = CoinGeckoSpotClient()
    await client.client.aclose()
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.base_url,
        headers={"Accept": "application/json"},
    )
    try:
        rows = await client.fetch_ohlc(
            "HYPE",
            start=datetime.fromtimestamp(base_ms / 1000, UTC),
            end=datetime.fromtimestamp((base_ms + 900_000) / 1000, UTC),
            interval_seconds=900,
        )
    finally:
        await client.aclose()

    assert len(rows) == 1
    assert rows[0].provider == "coingecko"
    assert rows[0].source_kind == "spot_price_proxy"
    assert rows[0].open_dollars == Decimal("100.0")
    assert rows[0].high_dollars == Decimal("103.0")
    assert rows[0].low_dollars == Decimal("100.0")
    assert rows[0].close_dollars == Decimal("101.0")
    assert rows[0].volume == Decimal("18.0")


@pytest.mark.asyncio
async def test_coingecko_spot_client_parses_current_proxy_price() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/simple/price"
        return httpx.Response(
            200,
            json={"hyperliquid": {"usd": 42.5, "last_updated_at": 1_600_000_001}},
        )

    client = CoinGeckoSpotClient()
    await client.client.aclose()
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=client.base_url,
        headers={"Accept": "application/json"},
    )
    try:
        row = await client.fetch_current("HYPE")
    finally:
        await client.aclose()

    assert row is not None
    assert row.provider == "coingecko"
    assert row.source_kind == "spot_price_proxy"
    assert row.end_ts == datetime.fromtimestamp(1_600_000_001, UTC)
    assert row.close_dollars == Decimal("42.5")


def test_decision_rows_join_only_prior_spot_candles() -> None:
    decision_ts = datetime(2026, 5, 1, 12, 14, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-TEST",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": decision_ts,
            "close_time": datetime(2026, 5, 1, 12, 15, tzinfo=UTC),
            "expected_expiration_time": datetime(2026, 5, 1, 12, 15, tzinfo=UTC),
            "target_price_dollars": Decimal("100.00000000"),
            "yes_bid_dollars": Decimal("0.4500"),
            "yes_ask_dollars": Decimal("0.4700"),
            "no_ask_dollars": Decimal("0.5500"),
            "last_price_dollars": Decimal("0.4600"),
            "volume": 10,
            "open_interest": 5,
        },
    )()
    prior_spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=decision_ts - timedelta(minutes=15),
        end_ts=decision_ts,
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_ohlc",
        observed_at=decision_ts,
        payload={},
    )
    future_spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=decision_ts,
        end_ts=decision_ts + timedelta(minutes=1),
        close_dollars=Decimal("120.00000000"),
        source_kind="spot_ohlc",
        observed_at=decision_ts + timedelta(minutes=1),
        payload={},
    )

    rows = _crypto_decision_rows([snapshot], [], [future_spot, prior_spot])  # type: ignore[list-item]

    assert rows[0]["spot_feature_status"] == "available"
    assert rows[0]["spot_close_dollars"] == Decimal("99.00000000")
    assert rows[0]["spot_provider"] == "coinbase"
    assert rows[0]["strict_trade_eligible"] is True


def test_historical_spot_ohlc_uses_alignment_window_not_live_freshness() -> None:
    decision_ts = datetime(2026, 5, 12, 12, 11, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-HISTORICAL-SPOT",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": decision_ts,
            "close_time": decision_ts + timedelta(minutes=4),
            "expected_expiration_time": decision_ts + timedelta(minutes=4),
            "target_price_dollars": Decimal("100.00000000"),
            "yes_bid_dollars": Decimal("0.4500"),
            "yes_ask_dollars": Decimal("0.4700"),
            "no_ask_dollars": Decimal("0.5500"),
            "last_price_dollars": Decimal("0.4600"),
            "volume": 10,
            "open_interest": 5,
            "payload": {},
        },
    )()
    spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=datetime(2026, 5, 12, 11, 45, tzinfo=UTC),
        end_ts=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_ohlc",
        observed_at=decision_ts,
        payload={},
    )

    rows = _crypto_decision_rows([snapshot], [], [spot])  # type: ignore[list-item]

    assert rows[0]["spot_context_mode"] == "historical"
    assert rows[0]["spot_feature_status"] == "available"
    assert rows[0]["spot_stale_seconds"] == 660
    assert rows[0]["spot_max_stale_seconds"] == 905


def test_historical_spot_context_prefers_fresh_tick_over_stale_candle() -> None:
    # 2026-06-30 STALE-SPOT FIX: historical mode used to DROP spot_tick rows and
    # take the up-to-900s-stale 15m candle for moneyness, while LIVE used the fresh
    # ~20s ticks — a train/serve mismatch that left moneyness a coin flip on 15m
    # markets and collapsed the analytic vol fair value to Brier ~0.25. Validated
    # (scripts/diag_sigma_freshfix.py): fresh spot drops raw vol Brier 0.25 -> 0.15.
    # Historical mode now uses the freshest point-in-time spot (here the 12:10 tick,
    # close=101), matching live; the 12:00 candle (close=99) is older and ignored.
    decision_ts = datetime(2026, 5, 12, 12, 11, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-HISTORICAL-SPOT-TICK",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": decision_ts,
            "close_time": decision_ts + timedelta(minutes=4),
            "expected_expiration_time": decision_ts + timedelta(minutes=4),
            "target_price_dollars": Decimal("100.00000000"),
            "yes_bid_dollars": Decimal("0.4500"),
            "yes_ask_dollars": Decimal("0.4700"),
            "no_ask_dollars": Decimal("0.5500"),
            "last_price_dollars": Decimal("0.4600"),
            "volume": 10,
            "open_interest": 5,
            "payload": {},
        },
    )()
    ohlc = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=datetime(2026, 5, 12, 11, 45, tzinfo=UTC),
        end_ts=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_ohlc",
        observed_at=decision_ts,
        payload={},
    )
    tick = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=0,
        start_ts=None,
        end_ts=datetime(2026, 5, 12, 12, 10, tzinfo=UTC),
        close_dollars=Decimal("101.00000000"),
        source_kind="spot_tick",
        observed_at=decision_ts,
        payload={},
    )

    # settings → coinbase max_stale 180s, so the 60s-old tick is fresh & available
    # (production behaviour); without it the 5s fallback constant would mark it stale.
    rows = _crypto_decision_rows([snapshot], [], [ohlc, tick], settings=Settings())  # type: ignore[list-item]

    assert rows[0]["spot_context_mode"] == "historical"
    assert rows[0]["spot_feature_status"] == "available"
    assert rows[0]["spot_source_kind"] == "spot_tick"
    assert rows[0]["spot_close_dollars"] == Decimal("101.00000000")
    assert rows[0]["spot_stale_seconds"] == 60


def test_historical_spot_ohlc_stales_after_alignment_window() -> None:
    decision_ts = datetime(2026, 5, 12, 12, 15, 6, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXBTC15M-HISTORICAL-STALE-SPOT",
            "series_ticker": "KXBTC15M",
            "asset_symbol": "BTC",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": decision_ts,
            "close_time": decision_ts + timedelta(minutes=4),
            "expected_expiration_time": decision_ts + timedelta(minutes=4),
            "target_price_dollars": Decimal("100.00000000"),
            "yes_bid_dollars": Decimal("0.4500"),
            "yes_ask_dollars": Decimal("0.4700"),
            "no_ask_dollars": Decimal("0.5500"),
            "last_price_dollars": Decimal("0.4600"),
            "volume": 10,
            "open_interest": 5,
            "payload": {},
        },
    )()
    spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=datetime(2026, 5, 12, 11, 45, tzinfo=UTC),
        end_ts=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_ohlc",
        observed_at=decision_ts,
        payload={},
    )

    rows = _crypto_decision_rows([snapshot], [], [spot])  # type: ignore[list-item]

    assert rows[0]["spot_feature_status"] == "stale"
    assert rows[0]["spot_stale_seconds"] == 906
    assert rows[0]["spot_max_stale_seconds"] == 905


def test_historical_coingecko_spot_is_available_but_proxy_only(tmp_path) -> None:
    settings = _settings(tmp_path)
    decision_ts = datetime(2026, 5, 12, 12, 11, tzinfo=UTC)
    snapshot = type(
        "_Snapshot",
        (),
        {
            "market_ticker": "KXHYPE15M-HISTORICAL-PROXY-SPOT",
            "series_ticker": "KXHYPE15M",
            "asset_symbol": "HYPE",
            "frequency": "15m",
            "source_kind": "historical",
            "settlement_result": "yes",
            "observed_at": decision_ts,
            "close_time": decision_ts + timedelta(minutes=4),
            "expected_expiration_time": decision_ts + timedelta(minutes=4),
            "target_price_dollars": Decimal("40.00000000"),
            "yes_bid_dollars": Decimal("0.4500"),
            "yes_ask_dollars": Decimal("0.4700"),
            "no_ask_dollars": Decimal("0.5500"),
            "last_price_dollars": Decimal("0.4600"),
            "volume": 10,
            "open_interest": 5,
            "payload": {},
        },
    )()
    spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coingecko",
        asset_symbol="HYPE",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=datetime(2026, 5, 12, 11, 45, tzinfo=UTC),
        end_ts=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        close_dollars=Decimal("41.00000000"),
        source_kind="spot_price_proxy",
        observed_at=decision_ts,
        payload={},
    )

    row = _crypto_decision_rows([snapshot], [], [spot], settings=settings)[0]  # type: ignore[list-item]
    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)

    assert row["spot_feature_status"] == "available"
    assert row["spot_proxy_only"] is True
    assert {candidate["candidate_status"] for candidate in candidates} == {"prediction_only_proxy_quote"}
    assert {candidate["reason"] for candidate in candidates} == {"spot_source_proxy_only"}


def test_live_spot_tick_allows_live_quality_candidate(tmp_path) -> None:
    settings = _settings(tmp_path, risk_min_edge_bps=50)
    now = datetime.now(UTC)
    market = CryptoMarket(
        market_ticker="KXBTC15M-LIVE-TICK",
        series_ticker="KXBTC15M",
        asset_symbol="BTC",
        frequency="15m",
        target_price_dollars=Decimal("100.00000000"),
        yes_bid_dollars=Decimal("0.5000"),
        yes_ask_dollars=Decimal("0.5200"),
        no_ask_dollars=Decimal("0.5000"),
        last_price_dollars=Decimal("0.5100"),
        close_time=now + timedelta(minutes=5),
        status="open",
    )
    spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=None,
        end_ts=now - timedelta(seconds=1),
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_tick",
        observed_at=now,
        payload={
            "market_microstructure": {
                "best_bid_ask": {
                    "best_bid_dollars": "98.9900",
                    "best_ask_dollars": "99.0100",
                    "mid_dollars": "99.0000",
                    "spread_bps": 2,
                },
                "latest_trade": {"price_dollars": "99.0000", "size": "1.25"},
                "recent_trade_count": 7,
            }
        },
    )

    row = _crypto_live_market_row(market, spot_rows=[spot], settings=settings)
    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)

    assert row["spot_context_mode"] == "live"
    assert row["spot_feature_status"] == "available"
    assert row["spot_proxy_only"] is False
    assert row["spot_exchange_bid_dollars"] == Decimal("98.9900")
    assert row["spot_exchange_ask_dollars"] == Decimal("99.0100")
    assert row["spot_exchange_spread_bps"] == 2
    assert row["spot_exchange_recent_trade_count"] == 7
    assert any(candidate["candidate_status"] == "live_quality" for candidate in candidates)


def test_live_ohlc_alone_uses_strict_live_freshness() -> None:
    now = datetime.now(UTC)
    market = CryptoMarket(
        market_ticker="KXBTC15M-LIVE-OHLC",
        series_ticker="KXBTC15M",
        asset_symbol="BTC",
        frequency="15m",
        target_price_dollars=Decimal("100.00000000"),
        yes_bid_dollars=Decimal("0.4500"),
        yes_ask_dollars=Decimal("0.4700"),
        no_ask_dollars=Decimal("0.5500"),
        last_price_dollars=Decimal("0.4600"),
        close_time=now + timedelta(minutes=5),
        status="open",
    )
    spot = CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider="coinbase",
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=900,
        start_ts=now - timedelta(minutes=30),
        end_ts=now - timedelta(minutes=15),
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_ohlc",
        observed_at=now,
        payload={},
    )

    row = _crypto_live_market_row(market, spot_rows=[spot])

    assert row["spot_context_mode"] == "live"
    assert row["spot_feature_status"] == "stale"
    assert row["spot_max_stale_seconds"] == 5


@pytest.mark.asyncio
async def test_crypto_spot_service_status_reports_coverage(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    now = datetime.now(UTC)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.upsert_crypto_spot_ohlc(
            kalshi_env=settings.kalshi_env,
            provider="coinbase",
            asset_symbol="BTC",
            quote_currency="USD",
            frequency="15m",
            interval_seconds=900,
            start_ts=now - timedelta(minutes=15),
            end_ts=now,
            close_dollars=Decimal("100.00000000"),
            observed_at=now,
            source_kind="spot_ohlc",
            source_id="BTC-USD",
            payload={},
        )
        await session.commit()

    status = await CryptoSpotService(settings=settings, session_factory=session_factory).status(
        frequency="15m",
        days=1,
        asset_symbols=["BTC"],
    )

    assert status["spot_quality"]["status"] == "ready"
    assert status["spot_quality"]["coverage_pct"] == 1.0
    assert status["spot_quality"]["assets"]["BTC"]["provider_counts"] == {"coinbase": 1}
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_spot_service_does_not_use_proxy_fallback_by_default(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    class CoinbaseUnavailable:
        async def fetch_current(self, asset_symbol: str) -> None:
            del asset_symbol
            return None

        async def aclose(self) -> None:
            return None

    class ProxyShouldNotBeCreated:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs
            raise AssertionError("proxy fallback should be disabled by default")

    monkeypatch.setattr(crypto_services, "CoinbaseSpotClient", lambda **kwargs: CoinbaseUnavailable())
    monkeypatch.setattr(crypto_services, "CoinGeckoSpotClient", ProxyShouldNotBeCreated)

    try:
        result = await CryptoSpotService(settings=settings, session_factory=session_factory).collect_current(
            frequency="15m",
            asset_symbols=["HYPE"],
        )
    finally:
        await engine.dispose()

    assert result["proxy_fallback_enabled"] is False
    assert result["stored"] == 0
    assert "coingecko" not in result["providers"]
    assert result["providers"]["none"]["errors"][0]["attempted"] == ["coinbase"]


@pytest.mark.asyncio
async def test_crypto_spot_service_proxy_fallback_requires_explicit_opt_in(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path, crypto_spot_proxy_fallback_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    now = datetime.now(UTC)

    class CoinbaseUnavailable:
        async def fetch_current(self, asset_symbol: str) -> None:
            del asset_symbol
            return None

        async def aclose(self) -> None:
            return None

    class ExplicitProxyFallback:
        async def fetch_current(self, asset_symbol: str) -> SpotOHLC:
            return SpotOHLC(
                provider="coingecko",
                asset_symbol=asset_symbol,
                start_ts=None,
                end_ts=now - timedelta(seconds=1),
                open_dollars=None,
                high_dollars=None,
                low_dollars=None,
                close_dollars=Decimal("42.0"),
                source_kind="spot_price_proxy",
                source_id=asset_symbol,
                payload={},
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(crypto_services, "CoinbaseSpotClient", lambda **kwargs: CoinbaseUnavailable())
    monkeypatch.setattr(crypto_services, "CoinGeckoSpotClient", lambda **kwargs: ExplicitProxyFallback())

    try:
        result = await CryptoSpotService(settings=settings, session_factory=session_factory).collect_current(
            frequency="15m",
            asset_symbols=["HYPE"],
        )
    finally:
        await engine.dispose()

    assert result["proxy_fallback_enabled"] is True
    assert result["stored"] == 1
    assert result["providers"]["coingecko"]["stored"] == 1


def test_ada_and_bch_have_spot_feed_entries() -> None:
    from kalshi_bot.integrations.crypto_spot import COINBASE_PRODUCT_IDS, COINGECKO_IDS

    for asset in ("ADA", "BCH"):
        assert asset in COINBASE_PRODUCT_IDS, f"{asset} missing from COINBASE_PRODUCT_IDS"
        assert asset in COINGECKO_IDS, f"{asset} missing from COINGECKO_IDS"
