"""Regression: _materialize_once loads snapshots with defer_payload=True and
builds decision rows in a compute phase *after* the DB session is closed. When a
structured price column (e.g. no_bid_dollars) is NULL, _snapshot_price falls back
to row.payload — but a deferred attribute on a detached instance raises
SQLAlchemy's DetachedInstanceError (not AttributeError, so getattr(..., None)
does not swallow it). That crashed the materialize on any NULL-price row.

The payload fallback must treat an unavailable (deferred + detached) payload as
"no payload" and return the structured value (or None) instead of raising.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.orm import defer

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import _snapshot_price
from kalshi_bot.db.models import CryptoMarketSnapshotRecord
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

NOW = datetime(2026, 6, 15, 0, 0, tzinfo=UTC)


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/detached.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


@pytest.mark.asyncio
async def test_snapshot_price_null_column_with_deferred_detached_payload(tmp_path) -> None:
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CryptoMarketSnapshotRecord(
                kalshi_env="production",
                series_ticker="KXBTC15M",
                market_ticker="KXBTC15M-NULLBID",
                asset_symbol="BTC",
                frequency="15m",
                settlement_result="yes",
                source_kind="live",
                yes_bid_dollars="0.40",
                yes_ask_dollars="0.45",
                no_bid_dollars=None,  # NULL -> triggers the payload fallback
                observed_at=NOW,
                payload={"market": {"no_bid": 55}},
            )
        )
        await session.commit()

    # Load with payload deferred, then let the session close so the row detaches.
    async with session_factory() as session:
        stmt = select(CryptoMarketSnapshotRecord).options(defer(CryptoMarketSnapshotRecord.payload))
        row = (await session.execute(stmt)).scalars().one()
    # row is now detached and payload is unloaded.

    # Must not raise DetachedInstanceError; NULL column -> no payload available -> None.
    price = _snapshot_price(
        row,
        attr="no_bid_dollars",
        dollar_keys=("no_bid_dollars",),
        cent_keys=("no_bid",),
    )
    assert price is None


@pytest.mark.asyncio
async def test_snapshot_price_uses_payload_when_loaded(tmp_path) -> None:
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CryptoMarketSnapshotRecord(
                kalshi_env="production",
                series_ticker="KXBTC15M",
                market_ticker="KXBTC15M-PAYLOAD",
                asset_symbol="BTC",
                frequency="15m",
                settlement_result="yes",
                source_kind="live",
                yes_bid_dollars="0.40",
                yes_ask_dollars="0.45",
                no_bid_dollars=None,
                observed_at=NOW,
                payload={"market": {"no_bid_dollars": "0.55"}},
            )
        )
        await session.commit()

    # Payload loaded (no defer) and still attached: fallback should read it.
    async with session_factory() as session:
        row = (await session.execute(select(CryptoMarketSnapshotRecord))).scalars().one()
        price = _snapshot_price(
            row,
            attr="no_bid_dollars",
            dollar_keys=("no_bid_dollars",),
            cent_keys=("no_bid",),
        )
        from decimal import Decimal

        assert price == Decimal("0.55")
