"""During materialize the spot rows are loaded once and then broadcast (pickled)
to every parallel build worker. Carrying every JSON payload bloats both the
parent and each worker copy; it OOMed the 32g trainer even at 2 workers.
``_list_crypto_spot_rows_with_cross_assets`` must be able to defer the payload,
and the optional payload-backed microstructure features must tolerate that.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import defer

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    CryptoTrainingBackfillService,
    _list_crypto_spot_rows_with_cross_assets,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


class _RecordingRepo:
    def __init__(self):
        self.calls: list[dict] = []

    async def list_crypto_spot_ohlc(self, **kwargs):
        self.calls.append(kwargs)
        return []


class _NoopSession:
    async def commit(self):
        raise AssertionError("no microstructure rows should be committed")

    async def rollback(self):
        raise AssertionError("no rollback should be needed")


class _NoopMicrostructureRepo:
    session = _NoopSession()

    async def upsert_crypto_order_book_snapshot(self, **kwargs):
        raise AssertionError("deferred payload should skip order book upserts")

    async def upsert_crypto_trade_tick(self, **kwargs):
        raise AssertionError("deferred payload should skip trade tick upserts")


@pytest.mark.asyncio
async def test_cross_asset_spot_loader_propagates_defer_payload():
    repo = _RecordingRepo()
    await _list_crypto_spot_rows_with_cross_assets(
        repo,
        frequency="15m",
        kalshi_env="production",
        requested_assets=["BTC"],
        since=None,
        limit=1000,
        defer_payload=True,
    )
    # Both the primary and cross-asset loads must defer the payload.
    assert repo.calls, "expected at least one spot load"
    assert all(call.get("defer_payload") is True for call in repo.calls)


@pytest.mark.asyncio
async def test_cross_asset_spot_loader_defaults_payload_loaded():
    repo = _RecordingRepo()
    await _list_crypto_spot_rows_with_cross_assets(
        repo,
        frequency="15m",
        kalshi_env="production",
        requested_assets=["BTC"],
        since=None,
        limit=1000,
    )
    assert all(call.get("defer_payload") is False for call in repo.calls)


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/spot_payload.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


@pytest.mark.asyncio
async def test_prepared_spot_context_tolerates_deferred_detached_payload(tmp_path):
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CryptoSpotOHLCRecord(
                kalshi_env="production",
                provider="coinbase",
                asset_symbol="BTC",
                quote_currency="USD",
                frequency="15m",
                interval_seconds=900,
                start_ts=NOW - timedelta(minutes=15),
                end_ts=NOW,
                open_dollars=Decimal("70000"),
                high_dollars=Decimal("70100"),
                low_dollars=Decimal("69900"),
                close_dollars=Decimal("70050"),
                volume=Decimal("1"),
                source_kind="spot_ohlc",
                observed_at=NOW,
                payload={
                    "market_microstructure": {
                        "best_bid_ask": {"best_bid_dollars": "70049.00"},
                        "recent_trade_count": 12,
                    }
                },
            )
        )
        await session.commit()

    async with session_factory() as session:
        stmt = select(CryptoSpotOHLCRecord).options(defer(CryptoSpotOHLCRecord.payload))
        rows = list((await session.execute(stmt)).scalars())

    prepared = _prepare_spot_context_series(rows)
    context = _spot_context_for_decision(
        rows,
        decision_ts=NOW + timedelta(minutes=1),
        target_price=Decimal("70000"),
        mid_yes=Decimal("0.50"),
        prepared=prepared,
    )

    assert context["spot_feature_status"] == "available"
    assert context["spot_close_dollars"] == Decimal("70050")
    assert context["spot_exchange_bid_dollars"] is None
    assert context["spot_exchange_recent_trade_count"] is None


@pytest.mark.asyncio
async def test_microstructure_materialize_tolerates_deferred_detached_payload(tmp_path):
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CryptoSpotOHLCRecord(
                kalshi_env="production",
                provider="coinbase",
                asset_symbol="BTC",
                quote_currency="USD",
                frequency="15m",
                interval_seconds=900,
                start_ts=NOW - timedelta(minutes=15),
                end_ts=NOW,
                open_dollars=Decimal("70000"),
                high_dollars=Decimal("70100"),
                low_dollars=Decimal("69900"),
                close_dollars=Decimal("70050"),
                volume=Decimal("1"),
                source_kind="spot_ohlc",
                observed_at=NOW,
                payload={
                    "market_microstructure": {
                        "best_bid_ask": {"best_bid_dollars": "70049.00"},
                        "recent_trades": [{"trade_id": "t1", "price": "70050"}],
                    }
                },
            )
        )
        await session.commit()

    async with session_factory() as session:
        stmt = select(CryptoSpotOHLCRecord).options(defer(CryptoSpotOHLCRecord.payload))
        rows = list((await session.execute(stmt)).scalars())

    service = CryptoTrainingBackfillService(
        settings=Settings(kalshi_env="production"),
        session_factory=object(),
        history_service=object(),
    )
    await service._materialize_spot_microstructure(
        _NoopMicrostructureRepo(),
        rows,
        frequency="15m",
    )
