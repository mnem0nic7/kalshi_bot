from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import CryptoSpotService, _filter_snapshots_by_per_asset_funding_cutoff, _funding_rate_context_for_decision
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


def _make_rate_record(asset: str, settlement_ts: datetime, realized_rate: str) -> CryptoFundingRateRecord:
    record = MagicMock(spec=CryptoFundingRateRecord)
    record.provider = "okx"
    record.asset_symbol = asset
    record.quote_currency = "USDT"
    record.settlement_ts = settlement_ts
    record.funding_rate = Decimal(realized_rate)
    record.realized_rate = Decimal(realized_rate)
    record.payload = {}
    return record


class TestFundingRateContextForDecision:
    def _make_index(self, records: list[CryptoFundingRateRecord]) -> dict:
        from collections import defaultdict
        idx: dict = defaultdict(list)
        for r in records:
            idx[r.asset_symbol].append(r)
        for v in idx.values():
            v.sort(key=lambda r: r.settlement_ts)
        return idx

    def test_empty_rows_returns_zeros(self):
        ts = datetime(2026, 5, 17, 9, 0, tzinfo=UTC)
        result = _funding_rate_context_for_decision({}, "BTC", decision_ts=ts)
        assert result["funding_rate_current"] == Decimal("0")
        assert result["funding_rate_delta"] == Decimal("0")

    def test_single_row_returns_current_and_zero_delta(self):
        ts_settle = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
        ts_decision = datetime(2026, 5, 17, 9, 0, tzinfo=UTC)
        records = [_make_rate_record("BTC", ts_settle, "0.0001")]
        idx = self._make_index(records)
        result = _funding_rate_context_for_decision(idx, "BTC", decision_ts=ts_decision)
        assert result["funding_rate_current"] == Decimal("0.0001")
        assert result["funding_rate_delta"] == Decimal("0")

    def test_two_rows_computes_delta(self):
        t1 = datetime(2026, 5, 17, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
        t_decision = datetime(2026, 5, 17, 9, 0, tzinfo=UTC)
        records = [
            _make_rate_record("BTC", t1, "0.0001"),
            _make_rate_record("BTC", t2, "0.0003"),
        ]
        idx = self._make_index(records)
        result = _funding_rate_context_for_decision(idx, "BTC", decision_ts=t_decision)
        assert result["funding_rate_current"] == Decimal("0.0003")
        assert result["funding_rate_delta"] == Decimal("0.0002")

    def test_negative_delta(self):
        t1 = datetime(2026, 5, 17, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
        t_decision = datetime(2026, 5, 17, 10, 0, tzinfo=UTC)
        records = [
            _make_rate_record("BTC", t1, "0.0005"),
            _make_rate_record("BTC", t2, "-0.0001"),
        ]
        idx = self._make_index(records)
        result = _funding_rate_context_for_decision(idx, "BTC", decision_ts=t_decision)
        assert result["funding_rate_current"] == Decimal("-0.0001")
        assert result["funding_rate_delta"] == Decimal("-0.0006")

    def test_decision_before_all_settlements_returns_zeros(self):
        t_settle = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
        t_decision = datetime(2026, 5, 17, 7, 59, tzinfo=UTC)
        records = [_make_rate_record("BTC", t_settle, "0.0001")]
        idx = self._make_index(records)
        result = _funding_rate_context_for_decision(idx, "BTC", decision_ts=t_decision)
        assert result["funding_rate_current"] == Decimal("0")
        assert result["funding_rate_delta"] == Decimal("0")

    def test_unknown_asset_returns_zeros(self):
        t_settle = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
        t_decision = datetime(2026, 5, 17, 9, 0, tzinfo=UTC)
        records = [_make_rate_record("BTC", t_settle, "0.0001")]
        idx = self._make_index(records)
        result = _funding_rate_context_for_decision(idx, "ETH", decision_ts=t_decision)
        assert result["funding_rate_current"] == Decimal("0")
        assert result["funding_rate_delta"] == Decimal("0")

    def test_uses_only_rows_at_or_before_decision_ts(self):
        t1 = datetime(2026, 5, 17, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 17, 8, 0, tzinfo=UTC)
        t3 = datetime(2026, 5, 17, 16, 0, tzinfo=UTC)
        t_decision = datetime(2026, 5, 17, 9, 0, tzinfo=UTC)
        records = [
            _make_rate_record("BTC", t1, "0.0001"),
            _make_rate_record("BTC", t2, "0.0002"),
            _make_rate_record("BTC", t3, "0.9999"),  # future — must be excluded
        ]
        idx = self._make_index(records)
        result = _funding_rate_context_for_decision(idx, "BTC", decision_ts=t_decision)
        assert result["funding_rate_current"] == Decimal("0.0002")
        assert result["funding_rate_delta"] == Decimal("0.0001")


def _make_snapshot(asset: str, observed_at: datetime):
    s = MagicMock()
    s.asset_symbol = asset
    s.observed_at = observed_at
    return s


class TestPerAssetFundingRateCutoff:
    """Tests for the per-asset cutoff filter applied in train() and _build_report()."""

    def test_assets_without_funding_rates_are_excluded(self):
        btc_start = datetime(2026, 2, 12, 0, 0, tzinfo=UTC)
        rate_rows = [_make_rate_record("BTC", btc_start, "0.0001")]
        snapshots = [
            _make_snapshot("BTC", btc_start + timedelta(days=1)),
            _make_snapshot("ETH", btc_start + timedelta(days=1)),  # no rates for ETH
        ]
        result = _filter_snapshots_by_per_asset_funding_cutoff(rate_rows, snapshots)
        assert all(s.asset_symbol == "BTC" for s in result)
        assert len(result) == 1

    def test_per_asset_cutoff_does_not_bleed_across_assets(self):
        # BTC has earlier start than HYPE; BTC's cutoff must not trim HYPE rows
        btc_start = datetime(2026, 2, 12, 0, 0, tzinfo=UTC)
        hype_start = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
        rate_rows = [
            _make_rate_record("BTC", btc_start, "0.0001"),
            _make_rate_record("HYPE", hype_start, "0.0002"),
        ]
        # HYPE snapshot at 2026-02-20 — before HYPE start, but after BTC start
        hype_before_cutoff = _make_snapshot("HYPE", datetime(2026, 2, 20, 0, 0, tzinfo=UTC))
        hype_after_cutoff = _make_snapshot("HYPE", datetime(2026, 3, 5, 0, 0, tzinfo=UTC))
        btc_row = _make_snapshot("BTC", datetime(2026, 2, 15, 0, 0, tzinfo=UTC))
        snapshots = [hype_before_cutoff, hype_after_cutoff, btc_row]
        result = _filter_snapshots_by_per_asset_funding_cutoff(rate_rows, snapshots)
        assert hype_before_cutoff not in result
        assert hype_after_cutoff in result
        assert btc_row in result

    def test_rows_before_per_asset_cutoff_are_excluded(self):
        start = datetime(2026, 2, 15, 0, 0, tzinfo=UTC)
        rate_rows = [_make_rate_record("BTC", start, "0.0001")]
        before = _make_snapshot("BTC", start - timedelta(hours=1))
        at = _make_snapshot("BTC", start)
        after = _make_snapshot("BTC", start + timedelta(days=1))
        result = _filter_snapshots_by_per_asset_funding_cutoff(rate_rows, [before, at, after])
        assert before not in result
        assert at in result
        assert after in result

    def test_no_funding_rate_rows_returns_all_snapshots_unchanged(self):
        snapshots = [
            _make_snapshot("BTC", datetime(2026, 5, 1, 0, 0, tzinfo=UTC)),
            _make_snapshot("ETH", datetime(2026, 5, 1, 0, 0, tzinfo=UTC)),
        ]
        result = _filter_snapshots_by_per_asset_funding_cutoff([], snapshots)
        assert result == snapshots
