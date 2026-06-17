"""Memory-safe DB loader for microstructure features.

Loading full spot payloads (~8.7KB/row) for a 30d window (~496k rows) into
Python OOMs the 32g trainer. ``list_crypto_spot_ohlc(microstructure_projection=
True)`` must stream rows and compact each payload to the microstructure scalars
as it arrives — so full payloads never all coexist — and must NOT write the
compacted payload back (the persisted row keeps its full payload).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


def _full_micro_payload(i: int) -> dict:
    return {
        "market_microstructure": {
            "product_id": "BTC-USD",
            "best_bid_ask": {
                "best_bid_dollars": "70049.00",
                "best_ask_dollars": "70051.00",
                "spread_bps": 3,
            },
            "recent_trade_count": 12,
            "recent_trades": [{"trade_id": f"t{i}-{j}", "price": "70050"} for j in range(40)],
        },
        "heavy_other": list(range(200)),
    }


async def _factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/micro_proj.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


async def _seed(session, n: int) -> None:
    for i in range(n):
        session.add(
            CryptoSpotOHLCRecord(
                kalshi_env="production",
                provider="coinbase",
                asset_symbol="BTC",
                quote_currency="USD",
                frequency="15m",
                interval_seconds=900,
                start_ts=NOW - timedelta(minutes=15 * (i + 1)),
                end_ts=NOW - timedelta(minutes=15 * i),
                open_dollars=Decimal("70000"),
                high_dollars=Decimal("70100"),
                low_dollars=Decimal("69900"),
                close_dollars=Decimal("70050"),
                volume=Decimal("1"),
                source_kind="spot_ohlc",
                observed_at=NOW - timedelta(minutes=15 * i),
                payload=_full_micro_payload(i),
            )
        )
    await session.commit()


@pytest.mark.asyncio
async def test_projection_returns_compacted_microstructure_payload(tmp_path):
    factory = await _factory(tmp_path)
    async with factory() as session:
        await _seed(session, 5)

    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        rows = await repo.list_crypto_spot_ohlc(
            frequency="15m", kalshi_env="production", asset_symbol="BTC",
            limit=100, microstructure_projection=True,
        )

    assert len(rows) == 5
    for row in rows:
        micro = row.payload["market_microstructure"]
        assert micro["best_bid_ask"]["spread_bps"] == 3
        assert micro["recent_trade_count"] == 12
        # Heavy content dropped from what we hold in memory.
        assert "recent_trades" not in micro
        assert "heavy_other" not in row.payload


@pytest.mark.asyncio
async def test_projection_does_not_mutate_persisted_payload(tmp_path):
    """The read path must never write the compacted payload back to the DB."""
    factory = await _factory(tmp_path)
    async with factory() as session:
        await _seed(session, 3)

    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.list_crypto_spot_ohlc(
            frequency="15m", kalshi_env="production", asset_symbol="BTC",
            limit=100, microstructure_projection=True,
        )

    # Re-read raw: the persisted payload must still be the FULL payload.
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        rows = await repo.list_crypto_spot_ohlc(
            frequency="15m", kalshi_env="production", asset_symbol="BTC", limit=100,
        )
    for row in rows:
        assert "recent_trades" in row.payload["market_microstructure"]
        assert "heavy_other" in row.payload
