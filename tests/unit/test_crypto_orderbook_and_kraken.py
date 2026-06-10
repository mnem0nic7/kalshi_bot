from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import (
    CryptoHistoryService,
    _crypto_order_book_depth_metrics,
    _crypto_spot_is_proxy,
    _dedup_spot_rows_by_provider_preference,
    _spot_context_for_decision,
)
from kalshi_bot.db.models import CryptoOrderBookSnapshotRecord, CryptoSpotOHLCRecord
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.crypto_spot import (
    KRAKEN_PAIRS,
    KrakenSpotClient,
    _parse_kraken_ohlc_payload,
)

KRAKEN_OHLC_FIXTURE = {
    "error": [],
    "result": {
        "XXBTZUSD": [
            [1_777_800_000, "100.0", "110.0", "99.0", "108.0", "104.5", "12.5", 42],
            [1_777_800_900, "108.0", "112.0", "107.0", "111.0", "109.8", "3.25", 17],
        ],
        "last": 1_777_800_900,
    },
}


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto-orderbook.db",
        "web_auth_enabled": False,
    }
    values.update(overrides)
    return Settings(**values)


def _spot_row(
    *,
    provider: str,
    end_ts: datetime,
    close: str,
    interval_seconds: int = 900,
    source_kind: str = "spot_ohlc",
) -> CryptoSpotOHLCRecord:
    return CryptoSpotOHLCRecord(
        kalshi_env="demo",
        provider=provider,
        asset_symbol="BTC",
        quote_currency="USD",
        frequency="15m",
        interval_seconds=interval_seconds,
        start_ts=end_ts - timedelta(seconds=interval_seconds),
        end_ts=end_ts,
        close_dollars=Decimal(close),
        source_kind=source_kind,
        payload={},
    )


# ---------------------------------------------------------------------------
# Kraken parsing + pair mapping
# ---------------------------------------------------------------------------


def test_parse_kraken_ohlc_payload_uses_canonical_pair_key() -> None:
    rows = _parse_kraken_ohlc_payload(
        KRAKEN_OHLC_FIXTURE,
        symbol="BTC",
        requested_pair="XBTUSD",
        interval_seconds=900,
    )
    assert len(rows) == 2
    first = rows[0]
    assert first.provider == "kraken"
    assert first.source_kind == "spot_ohlc"
    assert first.asset_symbol == "BTC"
    assert first.source_id == "XXBTZUSD"
    assert first.start_ts == datetime.fromtimestamp(1_777_800_000, UTC)
    assert first.end_ts == datetime.fromtimestamp(1_777_800_900, UTC)
    assert first.open_dollars == Decimal("100.0")
    assert first.high_dollars == Decimal("110.0")
    assert first.low_dollars == Decimal("99.0")
    assert first.close_dollars == Decimal("108.0")
    assert first.volume == Decimal("12.5")
    assert first.payload["pair"] == "XXBTZUSD"
    assert first.payload["requested_pair"] == "XBTUSD"
    assert first.payload["vwap"] == "104.5"
    assert first.payload["trade_count"] == 42
    assert rows[1].end_ts == datetime.fromtimestamp(1_777_801_800, UTC)


def test_parse_kraken_ohlc_payload_window_filter_and_errors() -> None:
    rows = _parse_kraken_ohlc_payload(
        KRAKEN_OHLC_FIXTURE,
        symbol="BTC",
        requested_pair="XBTUSD",
        interval_seconds=900,
        start=datetime.fromtimestamp(1_777_800_900, UTC),
        end=datetime.fromtimestamp(1_777_801_800, UTC),
    )
    assert [row.end_ts for row in rows] == [datetime.fromtimestamp(1_777_801_800, UTC)]

    with pytest.raises(ValueError):
        _parse_kraken_ohlc_payload(
            {"error": ["EQuery:Unknown asset pair"]},
            symbol="BTC",
            requested_pair="XBTUSD",
            interval_seconds=900,
        )
    assert (
        _parse_kraken_ohlc_payload({"error": [], "result": {}}, symbol="BTC", requested_pair="XBTUSD", interval_seconds=900)
        == []
    )


def test_kraken_pair_mapping_skips_bnb_and_hype() -> None:
    assert KRAKEN_PAIRS["BNB"] is None
    assert KRAKEN_PAIRS["HYPE"] is None
    assert KRAKEN_PAIRS["BTC"] == "XBTUSD"
    for asset in ("ETH", "SOL", "XRP", "DOGE"):
        assert KRAKEN_PAIRS[asset] == f"{asset}USD"


@pytest.mark.asyncio
async def test_kraken_spot_client_fetch_ohlc_no_network() -> None:
    captured: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url)
        return httpx.Response(200, json=KRAKEN_OHLC_FIXTURE)

    client = KrakenSpotClient()
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
    assert captured[0].path == "/0/public/OHLC"
    assert captured[0].params["pair"] == "XBTUSD"
    assert captured[0].params["interval"] == "15"
    assert [row.provider for row in rows] == ["kraken", "kraken"]


# ---------------------------------------------------------------------------
# Provider-preference dedup + proxy classification
# ---------------------------------------------------------------------------


def test_dedup_spot_rows_prefers_coinbase_and_preserves_order() -> None:
    base = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    coinbase_first = _spot_row(provider="coinbase", end_ts=base, close="100")
    kraken_first = _spot_row(provider="kraken", end_ts=base, close="101")
    kraken_second = _spot_row(provider="kraken", end_ts=base + timedelta(minutes=15), close="111")
    coinbase_second = _spot_row(provider="coinbase", end_ts=base + timedelta(minutes=15), close="110")
    deduped = _dedup_spot_rows_by_provider_preference(
        [coinbase_first, kraken_first, kraken_second, coinbase_second]
    )
    assert [(row.provider, row.close_dollars) for row in deduped] == [
        ("coinbase", Decimal("100")),
        ("coinbase", Decimal("110")),
    ]
    # Coinbase wins regardless of which provider appears first at a timestamp.
    deduped_reversed = _dedup_spot_rows_by_provider_preference([kraken_first, coinbase_first])
    assert [row.provider for row in deduped_reversed] == ["coinbase"]


def test_dedup_spot_rows_keeps_distinct_intervals_and_timestamps() -> None:
    base = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    rows = [
        _spot_row(provider="coinbase", end_ts=base, close="100", interval_seconds=900),
        _spot_row(provider="kraken", end_ts=base, close="101", interval_seconds=3600),
        _spot_row(provider="kraken", end_ts=base + timedelta(minutes=15), close="102"),
    ]
    assert _dedup_spot_rows_by_provider_preference(rows) == rows


def test_spot_context_momentum_uses_deduped_consecutive_rows() -> None:
    base = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    rows = [
        _spot_row(provider="coinbase", end_ts=base, close="100"),
        _spot_row(provider="kraken", end_ts=base, close="500"),
        _spot_row(provider="kraken", end_ts=base + timedelta(minutes=15), close="500"),
        _spot_row(provider="coinbase", end_ts=base + timedelta(minutes=15), close="110"),
    ]
    context = _spot_context_for_decision(
        rows,
        decision_ts=base + timedelta(minutes=16),
        target_price=Decimal("100"),
        mid_yes=Decimal("0.5"),
    )
    assert context["spot_provider"] == "coinbase"
    assert context["spot_close_dollars"] == Decimal("110")
    assert context["spot_return_1_pct"] == Decimal("0.1")


def test_crypto_spot_is_proxy_classification() -> None:
    assert _crypto_spot_is_proxy("kraken", "spot_ohlc") is False
    assert _crypto_spot_is_proxy("coinbase", "spot_ohlc") is False
    assert _crypto_spot_is_proxy("coinbase", "spot_tick") is False
    assert _crypto_spot_is_proxy("coingecko", "spot_price_proxy") is True
    assert _crypto_spot_is_proxy("coingecko", "spot_ohlc") is True


# ---------------------------------------------------------------------------
# Kalshi order-book depth metrics + collection
# ---------------------------------------------------------------------------


def test_order_book_depth_metrics_top_levels_and_imbalance() -> None:
    orderbook = {
        # YES bids (price cents, contracts); 6 levels so the worst is excluded.
        "yes": [[40, 10], [39, 20], [38, 30], [37, 40], [36, 50], [1, 999]],
        # NO bids; best NO bid 55 → best YES ask 45 cents.
        "no": [[55, 5], [54, 15], [50, 30]],
    }
    metrics = _crypto_order_book_depth_metrics(orderbook, top_levels=5)
    assert metrics["best_bid_dollars"] == Decimal("0.4")
    assert metrics["best_ask_dollars"] == Decimal("0.45")
    assert metrics["mid_dollars"] == Decimal("0.425")
    assert metrics["spread_bps"] == 500
    assert metrics["bid_depth"] == Decimal("150")
    assert metrics["ask_depth"] == Decimal("50")
    expected_imbalance = (Decimal("100") / Decimal("200")).quantize(Decimal("0.00000001"))
    assert metrics["depth_imbalance"] == expected_imbalance


def test_order_book_depth_metrics_zero_depth_guard() -> None:
    empty = _crypto_order_book_depth_metrics({"yes": [], "no": []})
    assert empty["bid_depth"] == Decimal("0")
    assert empty["ask_depth"] == Decimal("0")
    assert empty["depth_imbalance"] is None
    assert empty["best_bid_dollars"] is None
    assert empty["best_ask_dollars"] is None
    assert empty["mid_dollars"] is None
    assert empty["spread_bps"] is None

    malformed = _crypto_order_book_depth_metrics(None)
    assert malformed["depth_imbalance"] is None

    one_sided = _crypto_order_book_depth_metrics({"yes": [[30, 25]], "no": []})
    assert one_sided["best_bid_dollars"] == Decimal("0.3")
    assert one_sided["best_ask_dollars"] is None
    assert one_sided["spread_bps"] is None
    assert one_sided["depth_imbalance"] == Decimal("1.00000000")


class _StubKalshi:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[str] = []

    async def get_market_orderbook(self, ticker: str) -> dict:
        self.calls.append(ticker)
        return self.payload


@pytest.mark.asyncio
async def test_maybe_capture_order_book_writes_and_throttles(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    await init_models(engine)
    session_factory = create_session_factory(engine)
    kalshi = _StubKalshi({"orderbook": {"yes": [[40, 10], [39, 20]], "no": [[55, 5]]}})
    service = CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=kalshi,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )
    observed_at = datetime.now(UTC)
    market = CryptoMarket(
        market_ticker="KXBTCD-26JUN1012-T108000",
        series_ticker="KXBTCD",
        asset_symbol="BTC",
        frequency="15m",
    )
    try:
        async with session_factory() as session:
            from kalshi_bot.db.repositories import PlatformRepository

            repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
            first = await service._maybe_capture_order_book(repo, market, frequency="15m", observed_at=observed_at)
            second = await service._maybe_capture_order_book(repo, market, frequency="15m", observed_at=observed_at)
            await session.commit()
        assert first == "stored"
        assert second == "throttled"
        assert kalshi.calls == [market.market_ticker]
        async with session_factory() as session:
            records = list((await session.execute(select(CryptoOrderBookSnapshotRecord))).scalars())
        assert len(records) == 1
        record = records[0]
        assert record.provider == "kalshi"
        assert record.market_ticker == market.market_ticker
        assert Decimal(str(record.best_bid_dollars)) == Decimal("0.4")
        assert Decimal(str(record.best_ask_dollars)) == Decimal("0.45")
        assert Decimal(str(record.bid_depth)) == Decimal("30")
        assert Decimal(str(record.ask_depth)) == Decimal("5")
        assert Decimal(str(record.depth_imbalance)) == (Decimal("25") / Decimal("35")).quantize(Decimal("0.00000001"))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_maybe_capture_order_book_disabled(tmp_path) -> None:
    settings = _settings(tmp_path, crypto_orderbook_collect_enabled=False)
    engine = create_engine(settings)
    await init_models(engine)
    session_factory = create_session_factory(engine)
    kalshi = _StubKalshi({"orderbook": {"yes": [], "no": []}})
    service = CryptoHistoryService(
        settings=settings,
        session_factory=session_factory,
        kalshi=kalshi,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )
    market = CryptoMarket(market_ticker="KXBTCD-X", series_ticker="KXBTCD", asset_symbol="BTC")
    try:
        async with session_factory() as session:
            from kalshi_bot.db.repositories import PlatformRepository

            repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
            status = await service._maybe_capture_order_book(
                repo, market, frequency="15m", observed_at=datetime.now(UTC)
            )
        assert status == "disabled"
        assert kalshi.calls == []
    finally:
        await engine.dispose()
