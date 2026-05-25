from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import (
    CRYPTO_ASSET_MODE_LIVE,
    CRYPTO_ASSET_MODE_SHADOW,
    CRYPTO_ASSET_MODES_KEY,
    CryptoAssetControlService,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/winrate.db",
        "web_auth_enabled": False,
        "kalshi_env": "demo",
        "crypto_winrate_guard_enabled": True,
        "crypto_winrate_guard_window": 4,
        "crypto_winrate_guard_breakeven_pct": 0.62,
        "crypto_winrate_guard_consecutive_windows": 2,
    }
    values.update(overrides)
    return Settings(**values)


async def _build(tmp_path, **overrides):
    settings = _settings(tmp_path, **overrides)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    return settings, session_factory, service


async def _set_live(session_factory, service, symbol: str) -> None:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")

        def update(previous):
            modes = service.modes_from_notes({CRYPTO_ASSET_MODES_KEY: previous})
            modes[symbol] = CRYPTO_ASSET_MODE_LIVE
            return modes

        await repo.update_deployment_note_key(CRYPTO_ASSET_MODES_KEY, update, kalshi_env="demo")
        await session.commit()


async def _seed_outcomes(
    session_factory,
    *,
    symbol: str,
    pnls: list[float | None],
    frequency: str = "15m",
    fill_count: int = 1,
    settled: bool = True,
    tag: str = "a",
) -> None:
    # `tag` namespaces market_ticker/decision_time/input_hash so multiple seed
    # calls for the same symbol produce distinct rows instead of upsert-colliding
    # on (kalshi_env, market_ticker, decision_time, input_hash).
    base = datetime(2026, 5, 1, tzinfo=UTC) + timedelta(hours=ord(tag[0]))
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for idx, pnl in enumerate(pnls):
            await repo.upsert_crypto_decision_outcome(
                kalshi_env="demo",
                frequency=frequency,
                market_ticker=f"{symbol}-{tag}-{idx}",
                asset_symbol=symbol,
                decision_time=base + timedelta(minutes=idx),
                decision_kind="trade",
                input_hash=f"{symbol}-{tag}-hash-{idx}",
                settlement_result="yes" if settled else None,
                realized_pnl_dollars=None if pnl is None else Decimal(str(pnl)),
                fill_count=fill_count,
            )
        await session.commit()


async def _current_mode(session_factory, service, symbol: str) -> str:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        control = await repo.get_deployment_control(kalshi_env="demo")
        return service.modes_from_notes(control.notes).get(symbol, CRYPTO_ASSET_MODE_SHADOW)


async def _counter(session_factory, symbol: str) -> int:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        cp = await repo.get_checkpoint(f"crypto_winrate_guard:demo:{symbol}")
        return int((cp.payload or {}).get("consecutive_breaches", 0)) if cp else 0


# --- win-rate math -----------------------------------------------------------

def test_win_rate_math_ties_and_none():
    rows = [
        type("O", (), {"realized_pnl_dollars": Decimal("5")})(),
        type("O", (), {"realized_pnl_dollars": Decimal("0")})(),  # tie, not a win
        type("O", (), {"realized_pnl_dollars": None})(),  # None, not a win
        type("O", (), {"realized_pnl_dollars": Decimal("-3")})(),  # loss
    ]
    wins, total = CryptoAssetControlService._compute_win_rate(rows)
    assert wins == 1
    assert total == 4


# --- counter only increments when window full AND below threshold ------------

async def test_counter_unchanged_when_window_not_full(tmp_path):
    _, session_factory, service = await _build(tmp_path)
    await _set_live(session_factory, service, "BTC")
    # window=4, only 2 settled rows -> not full -> counter unchanged (0)
    await _seed_outcomes(session_factory, symbol="BTC", pnls=[-1.0, -1.0])
    result = await service.evaluate_winrate_guard(frequency="15m")
    assert result["assessed"]["BTC"]["window_full"] is False
    assert await _counter(session_factory, "BTC") == 0
    assert await _current_mode(session_factory, service, "BTC") == CRYPTO_ASSET_MODE_LIVE


async def test_counter_increments_only_on_full_breaching_window(tmp_path):
    _, session_factory, service = await _build(tmp_path)
    await _set_live(session_factory, service, "BTC")
    # 4 rows, 1 win -> 25% < 62% -> breach, counter 0 -> 1
    await _seed_outcomes(session_factory, symbol="BTC", pnls=[5.0, -1.0, -1.0, -1.0])
    result = await service.evaluate_winrate_guard(frequency="15m")
    assert result["assessed"]["BTC"]["breached"] is True
    assert await _counter(session_factory, "BTC") == 1
    assert await _current_mode(session_factory, service, "BTC") == CRYPTO_ASSET_MODE_LIVE


# --- flips to shadow only after N consecutive breaches -----------------------

async def test_flips_to_shadow_after_two_consecutive_breaches(tmp_path):
    _, session_factory, service = await _build(tmp_path)
    await _set_live(session_factory, service, "BTC")
    await _seed_outcomes(session_factory, symbol="BTC", pnls=[5.0, -1.0, -1.0, -1.0])

    # Call 1: breach -> counter 1, still LIVE
    await service.evaluate_winrate_guard(frequency="15m")
    assert await _counter(session_factory, "BTC") == 1
    assert await _current_mode(session_factory, service, "BTC") == CRYPTO_ASSET_MODE_LIVE

    # Call 2: breach -> counter reaches 2 -> flip to shadow, counter reset
    result = await service.evaluate_winrate_guard(frequency="15m")
    assert result["paused"] == ["BTC"]
    assert await _current_mode(session_factory, service, "BTC") == CRYPTO_ASSET_MODE_SHADOW
    assert await _counter(session_factory, "BTC") == 0

    # Call 3: asset now shadow -> not evaluated, stays shadow
    result3 = await service.evaluate_winrate_guard(frequency="15m")
    assert "BTC" not in result3["assessed"]
    assert await _current_mode(session_factory, service, "BTC") == CRYPTO_ASSET_MODE_SHADOW


# --- counter resets on a healthy window --------------------------------------

async def test_counter_resets_on_healthy_window(tmp_path):
    _, session_factory, service = await _build(tmp_path)
    await _set_live(session_factory, service, "BTC")
    # First breaching window -> counter 1
    await _seed_outcomes(session_factory, symbol="BTC", pnls=[5.0, -1.0, -1.0, -1.0])
    await service.evaluate_winrate_guard(frequency="15m")
    assert await _counter(session_factory, "BTC") == 1

    # Seed a healthy window with later timestamps so these rows dominate the
    # DESC-ordered, window=4 query (4/4 = 100% >= 62%).
    base = datetime(2026, 6, 1, tzinfo=UTC)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        for idx, pnl in enumerate([5.0, 5.0, 5.0, 5.0]):
            await repo.upsert_crypto_decision_outcome(
                kalshi_env="demo",
                frequency="15m",
                market_ticker=f"BTC-fresh-{idx}",
                asset_symbol="BTC",
                decision_time=base + timedelta(minutes=idx),
                decision_kind="trade",
                input_hash=f"BTC-fresh-{idx}",
                settlement_result="yes",
                realized_pnl_dollars=Decimal("5"),
                fill_count=1,
            )
        await session.commit()

    result = await service.evaluate_winrate_guard(frequency="15m")
    assert result["assessed"]["BTC"]["breached"] is False
    assert await _counter(session_factory, "BTC") == 0
    assert await _current_mode(session_factory, service, "BTC") == CRYPTO_ASSET_MODE_LIVE


# --- repo filter: only traded + settled rows are counted ---------------------

async def test_repo_excludes_unsettled_and_unfilled(tmp_path):
    _, session_factory, service = await _build(tmp_path)
    # settled+filled: counts; unsettled: excluded; fill_count=0: excluded.
    # Distinct tags keep these three sets from upsert-colliding on the conflict key.
    await _seed_outcomes(session_factory, symbol="ETH", pnls=[5.0, -1.0], tag="a")
    await _seed_outcomes(
        session_factory, symbol="ETH", pnls=[-1.0, -1.0], settled=False, fill_count=1, tag="b"
    )
    await _seed_outcomes(
        session_factory, symbol="ETH", pnls=[-1.0, -1.0], fill_count=0, tag="c"
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        rows = await repo.list_recent_settled_crypto_outcomes(
            frequency="15m", asset_symbol="ETH", limit=50
        )
    assert len(rows) == 2
    assert all(r.fill_count > 0 and r.settlement_result is not None for r in rows)


async def test_disabled_guard_via_per_asset_not_live_skips(tmp_path):
    # Asset never set live -> not in assessed, never paused.
    _, session_factory, service = await _build(tmp_path)
    await _seed_outcomes(session_factory, symbol="SOL", pnls=[-1.0, -1.0, -1.0, -1.0])
    result = await service.evaluate_winrate_guard(frequency="15m")
    assert "SOL" not in result["assessed"]
    assert result["paused"] == []
