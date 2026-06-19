"""Parity: a CHUNKED materialize (successive bounded passes) commits exactly the
same feature rows as a single unbounded pass over the same window.

A1 durability fix (2026-06-19): a wide materialize ran as one read->compute->one
upsert, so a kill lost the whole window. Chunking runs bounded passes (oldest
first); each commits, advancing the implicit watermark, so a kill only loses the
current chunk. This must not change WHAT gets materialized.

Byte-for-byte parity holds only when each chunk's read window contains the full
recency look-back for its rows (cross-market `asset_recent_*` features depend on
the window, per test_crypto_incremental_materialize). So, exactly like the
byte-for-byte parity test there, we set warmup >= lookback: every chunk reads
back to full_since, seeing the same priors as the single pass.

sqlite, no pgvector — reuses the incremental test's snapshot/spot/service harness.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

from tests.unit.test_crypto_incremental_materialize import (
    _make_snapshot,
    _service,
    _settled_market,
    _store_map,
)

# Anchor to "recent past" so the seeded markets fall inside materialize's real
# `datetime.now(UTC)` - lookback window (materialize uses the real wall clock).
NOW = datetime.now(UTC).replace(microsecond=0)
LOOKBACK_DAYS = 10
WARMUP_HOURS = 20 * 24  # >= lookback: each chunk reads back to full_since
STEP_HOURS = 96  # 4-day chunks over the 10-day window -> 3 chunks


def _spot_rows() -> list[CryptoSpotOHLCRecord]:
    rows: list[CryptoSpotOHLCRecord] = []
    start = NOW - timedelta(days=LOOKBACK_DAYS + 1)
    price = Decimal("70000")
    for i in range((LOOKBACK_DAYS + 1) * 24 + 1):
        ts = start + timedelta(hours=i)
        rows.append(
            CryptoSpotOHLCRecord(
                kalshi_env="production",
                provider="coinbase",
                asset_symbol="BTC",
                quote_currency="USD",
                frequency="15m",
                interval_seconds=3600,
                start_ts=ts - timedelta(hours=1),
                end_ts=ts,
                open_dollars=price,
                high_dollars=price + Decimal("100"),
                low_dollars=price - Decimal("100"),
                close_dollars=price,
                volume=Decimal("1"),
                source_kind="spot_ohlc",
                observed_at=ts,
                payload={},
            )
        )
    return rows


def _markets() -> list:
    # Settled markets spread across the window so chunk boundaries fall between them.
    records: list = []
    for days_ago, result in ((8, "yes"), (6, "no"), (4, "yes"), (2, "no")):
        records.extend(
            _settled_market(
                f"KXBTC15M-D{days_ago}",
                close_time=NOW - timedelta(days=days_ago),
                result=result,
            )
        )
    return records


def _settings(tmp_path, name: str, *, step_hours: float) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/{name}.db",
        kalshi_env="production",
        crypto_train_lookback_days=LOOKBACK_DAYS,
        crypto_train_incremental_materialize_enabled=True,
        crypto_train_incremental_warmup_hours=WARMUP_HOURS,
        crypto_train_incremental_max_gap_hours=24 * 365,
        crypto_train_materialize_max_step_hours=step_hours,
        crypto_min_training_samples=1,
    )


async def _factory(settings: Settings):
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


async def _seed(session_factory, records: list) -> None:
    async with session_factory() as session:
        for record in records:
            session.add(record)
        await session.commit()


async def _materialize(settings: Settings):
    session_factory = await _factory(settings)
    await _seed(session_factory, [*_spot_rows(), *_markets()])
    service = _service(settings, session_factory)
    result = await service.materialize(
        frequency="15m",
        asset_symbols=["BTC"],
        materialize_microstructure=False,
        materialize_settlement_windows=False,
    )
    return result, await _store_map(session_factory, settings)


@pytest.mark.asyncio
async def test_chunked_materialize_matches_single_pass(tmp_path) -> None:
    # Single unbounded pass (chunking disabled).
    single_result, single_map = await _materialize(
        _settings(tmp_path, "single", step_hours=0)
    )
    # Chunked pass (4-day steps over a 10-day window).
    chunked_result, chunked_map = await _materialize(
        _settings(tmp_path, "chunked", step_hours=STEP_HOURS)
    )

    assert single_map, "single pass committed no rows (seed/window mismatch)"
    # Same row_ids committed, and each row byte-for-byte identical (feature_hash)
    # with the same settled label — chunking changed durability, not content.
    assert set(chunked_map) == set(single_map)
    assert chunked_map == single_map
    assert single_result.get("status") == chunked_result.get("status")


@pytest.mark.asyncio
async def test_chunking_disabled_is_unbounded_single_pass(tmp_path) -> None:
    # step_hours=0 must behave exactly like the legacy single pass: all settled
    # markets in the lookback window are materialized.
    _result, store = await _materialize(_settings(tmp_path, "legacy", step_hours=0))
    assert len(store) >= 4  # one decision row per settled market (>=4 markets)
