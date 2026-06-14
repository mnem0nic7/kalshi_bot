from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

NOW = datetime(2026, 6, 13, 0, 0, tzinfo=UTC)


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/bulk.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


def _row(row_id: str, *, label: int | None = 1, qscore: float = 0.5) -> dict:
    return dict(
        kalshi_env="production",
        frequency="15m",
        market_ticker=f"KXBTC15M-{row_id}",
        asset_symbol="BTC",
        row_id=row_id,
        decision_time=NOW,
        settlement_time=None,
        label_yes=label,
        strict_trade_eligible=True,
        feature_schema_version="crypto-rich-v10",
        feature_hash=f"hash-{row_id}",
        source_build_id="build-1",
        quality_score=qscore,
        payload={"schema_version": "crypto-training-feature-row-v1", "decision_row": {"row_id": row_id}},
    )


@pytest.mark.asyncio
async def test_bulk_upsert_inserts_and_updates_on_conflict(tmp_path) -> None:
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        written = await repo.bulk_upsert_crypto_training_feature_rows([_row("a", label=None), _row("b", label=1)])
        await session.commit()
        assert written == 2

    # Re-upsert "a" with a settled label + new quality score → update, not duplicate.
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.bulk_upsert_crypto_training_feature_rows([_row("a", label=1, qscore=0.9)])
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        rows = await repo.list_crypto_training_feature_rows(frequency="15m", kalshi_env="production", limit=100)
        by_id = {r.row_id: r for r in rows}
        assert set(by_id) == {"a", "b"}              # no duplicate from re-upsert
        assert by_id["a"].label_yes == 1             # late label refreshed
        assert by_id["a"].quality_score == pytest.approx(0.9)


@pytest.mark.asyncio
async def test_bulk_upsert_empty_is_noop(tmp_path) -> None:
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        assert await repo.bulk_upsert_crypto_training_feature_rows([]) == 0


@pytest.mark.asyncio
async def test_bulk_upsert_multi_chunk_accumulation(tmp_path) -> None:
    """chunk_size=1 forces three separate chunks; all rows must be persisted and
    the return value must reflect the total written across all chunks."""
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        written = await repo.bulk_upsert_crypto_training_feature_rows(
            [_row("x"), _row("y"), _row("z")],
            chunk_size=1,
        )
        await session.commit()
        assert written == 3

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        rows = await repo.list_crypto_training_feature_rows(frequency="15m", kalshi_env="production", limit=100)
        assert {r.row_id for r in rows} == {"x", "y", "z"}


@pytest.mark.asyncio
async def test_watermark_is_max_decision_time_for_schema(tmp_path) -> None:
    from datetime import timedelta

    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        r_old = _row("old"); r_old["decision_time"] = NOW - timedelta(hours=3)
        r_new = _row("new"); r_new["decision_time"] = NOW
        r_v9 = _row("v9"); r_v9["decision_time"] = NOW + timedelta(hours=1); r_v9["feature_schema_version"] = "crypto-rich-v9"
        await repo.bulk_upsert_crypto_training_feature_rows([r_old, r_new, r_v9])
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        wm = await repo.get_crypto_training_feature_watermark(
            frequency="15m", kalshi_env="production", feature_schema_version="crypto-rich-v10"
        )
        assert wm == NOW  # ignores the newer v9 row (different schema)
        none_wm = await repo.get_crypto_training_feature_watermark(
            frequency="1h", kalshi_env="production", feature_schema_version="crypto-rich-v10"
        )
        assert none_wm is None


def test_incremental_materialize_defaults() -> None:
    from kalshi_bot.config import Settings

    s = Settings(database_url="sqlite+aiosqlite:///:memory:")
    assert s.crypto_train_incremental_materialize_enabled is True
    assert s.crypto_train_incremental_warmup_hours == 72
    assert s.crypto_train_incremental_max_gap_hours == 168
