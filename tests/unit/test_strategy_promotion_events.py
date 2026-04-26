"""Unit tests for the strategy_promotion_events audit log (P2-3)."""
from __future__ import annotations

from datetime import datetime

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


@pytest.fixture
async def repo_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/promotions.db")
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    await init_models(engine)

    async def _make():
        return factory()

    yield _make
    await engine.dispose()


@pytest.mark.asyncio
async def test_record_strategy_promotion_persists_row(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        event = await repo.record_strategy_promotion(
            strategy="C",
            from_state="shadow",
            to_state="live",
            actor="ops@example.com",
            evidence_ref="https://github.com/owner/repo/pull/123",
            notes="Cleanup fill rate held above 80% for 14 days.",
            kalshi_env="demo",
        )
        await session.commit()

        assert event.strategy == "C"
        assert event.from_state == "shadow"
        assert event.to_state == "live"
        assert event.actor == "ops@example.com"
        assert event.evidence_ref == "https://github.com/owner/repo/pull/123"
        assert event.notes is not None
        assert event.kalshi_env == "demo"
        assert isinstance(event.created_at, datetime)


@pytest.mark.asyncio
async def test_record_strategy_promotion_rejects_empty_strategy(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        with pytest.raises(ValueError, match="strategy"):
            await repo.record_strategy_promotion(
                strategy="",
                from_state="shadow",
                to_state="live",
                actor="ops",
            )


@pytest.mark.asyncio
async def test_record_strategy_promotion_rejects_empty_actor(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        with pytest.raises(ValueError, match="actor"):
            await repo.record_strategy_promotion(
                strategy="A",
                from_state="shadow",
                to_state="live",
                actor="   ",
            )


@pytest.mark.asyncio
async def test_record_strategy_promotion_rejects_no_op_transitions(repo_factory) -> None:
    """Recording shadow→shadow is usually a mistake and would pollute the
    audit log with no-op rows. Reject outright."""
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        with pytest.raises(ValueError, match="differ"):
            await repo.record_strategy_promotion(
                strategy="A",
                from_state="shadow",
                to_state="shadow",
                actor="ops",
            )


@pytest.mark.asyncio
async def test_list_strategy_promotions_orders_newest_first(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for i, (s, fs, ts) in enumerate([
            ("A", "shadow", "live"),
            ("C", "shadow", "live"),
            ("A", "live", "shadow"),
        ]):
            await repo.record_strategy_promotion(
                strategy=s,
                from_state=fs,
                to_state=ts,
                actor=f"ops-{i}",
                kalshi_env="demo",
            )
        await session.commit()

        events = await repo.list_strategy_promotions(kalshi_env="demo")
        # Three events, newest first.
        assert len(events) == 3
        assert [e.actor for e in events] == ["ops-2", "ops-1", "ops-0"]


@pytest.mark.asyncio
async def test_list_strategy_promotions_filters_by_strategy(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        await repo.record_strategy_promotion(
            strategy="A", from_state="shadow", to_state="live", actor="a1",
            kalshi_env="demo",
        )
        await repo.record_strategy_promotion(
            strategy="C", from_state="shadow", to_state="live", actor="c1",
            kalshi_env="demo",
        )
        await session.commit()

        only_a = await repo.list_strategy_promotions(strategy="A", kalshi_env="demo")
        assert [e.actor for e in only_a] == ["a1"]
        only_c = await repo.list_strategy_promotions(strategy="C", kalshi_env="demo")
        assert [e.actor for e in only_c] == ["c1"]


@pytest.mark.asyncio
async def test_list_strategy_promotions_respects_limit(repo_factory) -> None:
    session_ctx = await repo_factory()
    async with session_ctx as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for i in range(5):
            await repo.record_strategy_promotion(
                strategy="A", from_state="shadow", to_state="live",
                actor=f"ops-{i}", kalshi_env="demo",
            )
        await session.commit()

        events = await repo.list_strategy_promotions(kalshi_env="demo", limit=2)
        assert len(events) == 2
