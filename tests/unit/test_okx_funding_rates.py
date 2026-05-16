from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import CryptoSpotService
from kalshi_bot.db.models import CryptoFundingRateRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.okx_funding_rates import (
    FundingRate,
    OKX_ASSET_INST_IDS,
    OkxFundingRateClient,
    _parse_okx_record,
)


def _settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/funding.db",
        web_auth_enabled=False,
        kalshi_env="demo",
    )


async def _make_session_factory(tmp_path):
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


def _make_okx_raw(asset: str, settlement_ts: datetime, realized_rate: str = "0.0001") -> dict:
    return {
        "instId": OKX_ASSET_INST_IDS[asset],
        "instType": "SWAP",
        "fundingTime": str(int(settlement_ts.timestamp() * 1000)),
        "fundingRate": realized_rate,
        "realizedRate": realized_rate,
        "formulaType": "withRate",
        "method": "current_period",
    }


class TestParseOkxRecord:
    def test_parses_valid_record(self):
        ts = datetime(2026, 5, 16, 16, 0, tzinfo=UTC)
        raw = _make_okx_raw("BTC", ts, "0.0001234567")
        result = _parse_okx_record("BTC", raw)
        assert result is not None
        assert result.asset_symbol == "BTC"
        assert result.provider == "okx"
        assert result.quote_currency == "USDT"
        assert result.settlement_ts == ts
        assert result.realized_rate == Decimal("0.0001234567")
        assert result.funding_rate == Decimal("0.0001234567")

    def test_negative_funding_rate(self):
        ts = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
        raw = _make_okx_raw("ETH", ts, "-0.0000500000")
        result = _parse_okx_record("ETH", raw)
        assert result is not None
        assert result.realized_rate < 0

    def test_missing_key_returns_none(self):
        assert _parse_okx_record("BTC", {}) is None
        assert _parse_okx_record("BTC", {"fundingTime": "bad", "fundingRate": "0.0001", "realizedRate": "0.0001"}) is None

    def test_unknown_asset_still_parses(self):
        ts = datetime(2026, 5, 16, 0, 0, tzinfo=UTC)
        raw = {"fundingTime": str(int(ts.timestamp() * 1000)), "fundingRate": "0.0001", "realizedRate": "0.0001"}
        result = _parse_okx_record("NEWTOKEN", raw)
        assert result is not None
        assert result.asset_symbol == "NEWTOKEN"


class TestOkxFundingRateClientFetch:
    def _mock_response(self, data: list[dict]) -> dict:
        return {"code": "0", "msg": "", "data": data}

    def _make_httpx_mock(self, data: list[dict]):
        """Return a mock httpx response (synchronous json/raise_for_status)."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = self._mock_response(data)
        return mock_resp

    @pytest.mark.asyncio
    async def test_fetch_history_returns_parsed_records(self):
        ts1 = datetime(2026, 5, 16, 16, 0, tzinfo=UTC)
        ts2 = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
        raw_data = [_make_okx_raw("BTC", ts1, "0.0001"), _make_okx_raw("BTC", ts2, "0.0002")]

        client = OkxFundingRateClient()
        with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = self._make_httpx_mock(raw_data)
            records = await client.fetch_history("BTC", limit=2)

        assert len(records) == 2
        assert records[0].settlement_ts == ts1
        assert records[0].realized_rate == Decimal("0.0001")
        await client.aclose()

    @pytest.mark.asyncio
    async def test_fetch_history_unknown_asset_returns_empty(self):
        client = OkxFundingRateClient()
        records = await client.fetch_history("UNKNOWN_XYZ")
        assert records == []
        await client.aclose()

    @pytest.mark.asyncio
    async def test_fetch_history_api_error_returns_empty(self):
        client = OkxFundingRateClient()
        with patch.object(client._client, "get", new_callable=AsyncMock, side_effect=Exception("network error")):
            records = await client.fetch_history("BTC")
        assert records == []
        await client.aclose()

    @pytest.mark.asyncio
    async def test_fetch_latest_returns_most_recent(self):
        ts = datetime(2026, 5, 16, 16, 0, tzinfo=UTC)
        raw_data = [_make_okx_raw("ETH", ts, "0.0003")]

        client = OkxFundingRateClient()
        with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = self._make_httpx_mock(raw_data)
            result = await client.fetch_latest("ETH")

        assert result is not None
        assert result.asset_symbol == "ETH"
        assert result.settlement_ts == ts
        await client.aclose()

    @pytest.mark.asyncio
    async def test_fetch_latest_empty_returns_none(self):
        client = OkxFundingRateClient()
        with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = self._make_httpx_mock([])
            result = await client.fetch_latest("BTC")
        assert result is None
        await client.aclose()


class TestCollectFundingRates:
    def _make_service(self, session_factory, tmp_path) -> CryptoSpotService:
        settings = _settings(tmp_path)
        return CryptoSpotService(settings=settings, session_factory=session_factory)

    def _mock_okx_fetch(self, asset: str, settlement_ts: datetime, rate: str = "0.0001") -> list[FundingRate]:
        return [
            FundingRate(
                provider="okx",
                asset_symbol=asset,
                quote_currency="USDT",
                settlement_ts=settlement_ts,
                funding_rate=Decimal(rate),
                realized_rate=Decimal(rate),
                payload={},
            )
        ]

    @pytest.mark.asyncio
    async def test_collect_persists_funding_rates(self, tmp_path):
        session_factory = await _make_session_factory(tmp_path)
        service = self._make_service(session_factory, tmp_path)
        ts = datetime(2026, 5, 16, 16, 0, tzinfo=UTC)

        with patch("kalshi_bot.crypto.services.OkxFundingRateClient") as mock_cls:
            instance = AsyncMock()
            instance.fetch_history.return_value = self._mock_okx_fetch("BTC", ts, "0.0001")
            instance.aclose = AsyncMock()
            mock_cls.return_value = instance

            result = await service.collect_funding_rates(asset_symbols=["BTC"])

        assert result["stored"] >= 1
        assert not result["errors"]

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            rows = await repo.list_crypto_funding_rates("BTC", limit=5)
        assert len(rows) == 1
        assert rows[0].realized_rate == Decimal("0.0001")
        assert rows[0].settlement_ts.replace(tzinfo=UTC) == ts

    @pytest.mark.asyncio
    async def test_collect_upserts_without_duplicate(self, tmp_path):
        session_factory = await _make_session_factory(tmp_path)
        service = self._make_service(session_factory, tmp_path)
        ts = datetime(2026, 5, 16, 16, 0, tzinfo=UTC)

        for rate in ["0.0001", "0.0002"]:
            with patch("kalshi_bot.crypto.services.OkxFundingRateClient") as mock_cls:
                instance = AsyncMock()
                instance.fetch_history.return_value = self._mock_okx_fetch("BTC", ts, rate)
                instance.aclose = AsyncMock()
                mock_cls.return_value = instance
                await service.collect_funding_rates(asset_symbols=["BTC"])

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            rows = await repo.list_crypto_funding_rates("BTC", limit=10)
        # Should only have one row (upserted)
        assert len(rows) == 1
        assert rows[0].realized_rate == Decimal("0.0002")

    @pytest.mark.asyncio
    async def test_collect_funding_rates_error_isolated(self, tmp_path):
        session_factory = await _make_session_factory(tmp_path)
        service = self._make_service(session_factory, tmp_path)

        with patch("kalshi_bot.crypto.services.OkxFundingRateClient") as mock_cls:
            instance = AsyncMock()
            instance.fetch_history.side_effect = Exception("OKX down")
            instance.aclose = AsyncMock()
            mock_cls.return_value = instance

            result = await service.collect_funding_rates(asset_symbols=["BTC"])

        assert result["errors"]
        assert result["stored"] == 0


class TestListCryptoFundingRates:
    @pytest.mark.asyncio
    async def test_list_before_ts_filter(self, tmp_path):
        settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/fr.db", web_auth_enabled=False, kalshi_env="demo")
        engine = create_engine(settings)
        await init_models(engine)
        session_factory = create_session_factory(engine)

        t1 = datetime(2026, 5, 16, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 16, 8, 0, tzinfo=UTC)
        t3 = datetime(2026, 5, 16, 16, 0, tzinfo=UTC)

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            for ts, rate in [(t1, "0.0001"), (t2, "0.0002"), (t3, "0.0003")]:
                await repo.upsert_crypto_funding_rate(
                    provider="okx",
                    asset_symbol="BTC",
                    quote_currency="USDT",
                    settlement_ts=ts,
                    funding_rate=Decimal(rate),
                    realized_rate=Decimal(rate),
                    payload={},
                )
            await session.commit()

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            rows = await repo.list_crypto_funding_rates("BTC", before_ts=t3, limit=10)

        assert len(rows) == 2
        assert all(r.settlement_ts.replace(tzinfo=UTC) < t3 for r in rows)

    @pytest.mark.asyncio
    async def test_list_returns_newest_first(self, tmp_path):
        settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/fr2.db", web_auth_enabled=False, kalshi_env="demo")
        engine = create_engine(settings)
        await init_models(engine)
        session_factory = create_session_factory(engine)

        times = [datetime(2026, 5, 16, h, 0, tzinfo=UTC) for h in (0, 8, 16)]
        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            for ts in times:
                await repo.upsert_crypto_funding_rate(
                    provider="okx",
                    asset_symbol="ETH",
                    quote_currency="USDT",
                    settlement_ts=ts,
                    funding_rate=Decimal("0.0001"),
                    realized_rate=Decimal("0.0001"),
                    payload={},
                )
            await session.commit()

        async with session_factory() as session:
            repo = PlatformRepository(session, kalshi_env="demo")
            rows = await repo.list_crypto_funding_rates("ETH", limit=10)

        # SQLite strips tzinfo; compare as naive UTC datetimes
        assert rows[0].settlement_ts > rows[1].settlement_ts > rows[2].settlement_ts  # all naive from SQLite
