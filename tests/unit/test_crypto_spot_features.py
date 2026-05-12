from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import httpx
import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import CryptoSpotService, _crypto_decision_rows, _crypto_live_market_row, _crypto_trade_candidates
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.crypto_spot import CoinbaseCdpCredentials, CoinbaseSpotClient, CoinGeckoSpotClient


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
        start_ts=None,
        end_ts=now - timedelta(seconds=1),
        close_dollars=Decimal("99.00000000"),
        source_kind="spot_tick",
        observed_at=now,
        payload={},
    )

    row = _crypto_live_market_row(market, spot_rows=[spot], settings=settings)
    candidates = _crypto_trade_candidates(row, Decimal("0.9000"), settings=settings)

    assert row["spot_context_mode"] == "live"
    assert row["spot_feature_status"] == "available"
    assert row["spot_proxy_only"] is False
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
