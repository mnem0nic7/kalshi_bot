"""Regression: the crypto nightly retrain crashed with asyncpg
``InterfaceError: the number of query arguments cannot exceed 32767`` because
``list_crypto_live_quote_snapshots`` loaded the matched snapshot rows with a
single ``id.in_(ids)`` clause (one bind param per id). After the spot-density
backfill grew the distinct-market count past 32,767, the second-stage lookup
exceeded asyncpg's parameter ceiling and the whole nightly ``train`` died, so
no new models were produced.

The id lookup must be chunked so each query stays under the bind-param limit
while still returning every matched record. SQLite (used in unit tests) does
not take the postgresql raw-SQL branch, so we test the dialect-agnostic
chunked id-loader directly.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

import kalshi_bot.db.repositories as repositories_module
from kalshi_bot.config import Settings
from kalshi_bot.db.models import CryptoMarketSnapshotRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

NOW = datetime(2026, 6, 15, 0, 0, tzinfo=UTC)


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/chunk.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


def _snapshot(idx: int) -> CryptoMarketSnapshotRecord:
    return CryptoMarketSnapshotRecord(
        kalshi_env="production",
        series_ticker="KXBTC15M",
        market_ticker=f"KXBTC15M-{idx}",
        asset_symbol="BTC",
        frequency="15m",
        settlement_result="yes",
        source_kind="live",
        yes_bid_dollars="0.40",
        yes_ask_dollars="0.45",
        observed_at=NOW,
    )


@pytest.mark.asyncio
async def test_load_crypto_snapshots_by_ids_returns_all_across_chunks(tmp_path, monkeypatch) -> None:
    # Force multiple chunks: 5 ids with a chunk size of 2 -> 3 queries.
    monkeypatch.setattr(repositories_module, "_CRYPTO_ID_IN_CHUNK_SIZE", 2)
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        records = [_snapshot(i) for i in range(5)]
        session.add_all(records)
        await session.flush()
        ids = [record.id for record in records]
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        loaded = await repo._load_crypto_snapshots_by_ids(ids)

    # All 5 returned despite spanning 3 chunks (a single-chunk loader returns <=2).
    assert {record.id for record in loaded} == set(ids)


@pytest.mark.asyncio
async def test_load_crypto_snapshots_by_ids_handles_empty(tmp_path) -> None:
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        assert await repo._load_crypto_snapshots_by_ids([]) == []
