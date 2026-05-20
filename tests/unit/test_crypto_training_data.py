from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import CryptoForecastService
from kalshi_bot.db.models import CryptoMarketSnapshotRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


NOW = datetime(2026, 5, 20, 0, 0, tzinfo=UTC)


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/crypto-training.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


def _snapshot(
    market_ticker: str,
    *,
    observed_at: datetime,
    settlement_result: str | None,
    close_time: datetime,
) -> CryptoMarketSnapshotRecord:
    return CryptoMarketSnapshotRecord(
        kalshi_env="production",
        series_ticker="KXBTC15M",
        market_ticker=market_ticker,
        asset_symbol="BTC",
        frequency="15m",
        status="closed" if settlement_result else "active",
        close_time=close_time,
        expected_expiration_time=close_time,
        target_price_dollars=Decimal("70000"),
        yes_bid_dollars=Decimal("0.3000"),
        yes_ask_dollars=Decimal("0.3100"),
        no_ask_dollars=Decimal("0.7000"),
        settlement_result=settlement_result,
        observed_at=observed_at,
        source_kind="test",
    )


@pytest.mark.asyncio
async def test_list_settled_market_snapshots_excludes_open_markets(tmp_path) -> None:
    """Only snapshots belonging to markets that actually settled should be returned.

    A market is 'settled' if any of its snapshots carries settlement_result in
    {yes, no}. Decision-time snapshots of that market (settlement_result NULL,
    observed before close) must still be returned, while snapshots for markets
    that never settled are excluded entirely.
    """
    close_ts = NOW - timedelta(hours=1)
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        # Settled market: a pre-close decision snapshot + the settlement snapshot.
        session.add(
            _snapshot(
                "KXBTC15M-SETTLED",
                observed_at=close_ts - timedelta(minutes=10),
                settlement_result=None,
                close_time=close_ts,
            )
        )
        session.add(
            _snapshot(
                "KXBTC15M-SETTLED",
                observed_at=close_ts,
                settlement_result="yes",
                close_time=close_ts,
            )
        )
        # Open market: never settled, must be excluded.
        session.add(
            _snapshot(
                "KXBTC15M-OPEN",
                observed_at=NOW,
                settlement_result=None,
                close_time=NOW + timedelta(minutes=15),
            )
        )
        await session.commit()

        repo = PlatformRepository(session)
        rows = await repo.list_crypto_settled_market_snapshots(
            frequency="15m",
            kalshi_env="production",
            limit=1000,
        )

    returned_markets = {row.market_ticker for row in rows}
    assert returned_markets == {"KXBTC15M-SETTLED"}
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_train_includes_settled_markets_beyond_recent_snapshot_cap(tmp_path) -> None:
    """train() must not let recent open-market snapshots crowd out settled ones.

    With a small snapshot cap, a sea of recent open-market snapshots would, under
    the old most-recent-N-raw-snapshots loader, push the only settled market out
    of the training set (sample_count -> 0). Sourcing from settled markets first
    means the settled market still yields its decision row.
    """
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/train.db",
        kalshi_env="production",
        crypto_train_max_snapshots=5,
        crypto_min_training_samples=1,
    )
    engine = create_engine(settings)
    await init_models(engine)
    session_factory = create_session_factory(engine)

    settled_close = NOW - timedelta(days=2)
    async with session_factory() as session:
        # One older settled market (pre-close decision snapshot + settlement snapshot).
        session.add(
            _snapshot(
                "KXBTC15M-SETTLED",
                observed_at=settled_close - timedelta(minutes=10),
                settlement_result=None,
                close_time=settled_close,
            )
        )
        session.add(
            _snapshot(
                "KXBTC15M-SETTLED",
                observed_at=settled_close,
                settlement_result="yes",
                close_time=settled_close,
            )
        )
        # Many more-recent open-market snapshots (never settled) — exceed the cap of 5.
        for i in range(8):
            session.add(
                _snapshot(
                    f"KXBTC15M-OPEN-{i}",
                    observed_at=NOW - timedelta(minutes=i),
                    settlement_result=None,
                    close_time=NOW + timedelta(minutes=15),
                )
            )
        await session.commit()

    # The cap must be a real, configurable field (not silently dropped by pydantic),
    # otherwise a 100k default would let the old loader sweep in everything and the
    # test would pass without exercising the settled-market scoping.
    assert settings.crypto_train_max_snapshots == 5

    service = CryptoForecastService(settings=settings, session_factory=session_factory)
    result = await service.train(frequency="15m")

    # Old behavior: most-recent-5 raw snapshots are all open markets -> 0 samples.
    # New behavior: settled market is scoped in regardless -> its decision row survives.
    assert result["sample_count"] >= 1
