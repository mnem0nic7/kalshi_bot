"""Regression: build_crypto_maker_markout_report must run past the fills query.

It crashed with `NameError: name 'defaultdict' is not defined` because the
repository module imported only `Counter` from collections. The early-return on
an empty fill set hid it; the bug only fires once at least one maker buy fill
exists (so the snapshot-grouping `defaultdict(list)` line is reached).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kalshi_bot.config import Settings
from kalshi_bot.db.models import FillRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


async def _repo_with_one_maker_fill(tmp_path):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/markout.db",
        web_auth_enabled=False,
        kalshi_env="demo",
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        session.add(
            FillRecord(
                kalshi_env="demo",
                market_ticker="KXBTC15M-26JUN181500-99000",
                side="yes",
                action="buy",
                yes_price_dollars=Decimal("0.4000"),
                count_fp=Decimal("1.00"),
                is_taker=False,  # maker fill — the path that reaches the bug
                strategy_code="CRYPTO_15M",
                created_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        await session.commit()
    return session_factory


async def test_maker_markout_report_runs_with_a_maker_fill(tmp_path):
    session_factory = await _repo_with_one_maker_fill(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        report = await repo.build_crypto_maker_markout_report(
            kalshi_env="demo", days=14, frequency="15m"
        )
    assert report["schema_version"] == "crypto-maker-markout-v1"
    assert report["totals"]["fills"] == 1
    assert isinstance(report["cells"], list)
