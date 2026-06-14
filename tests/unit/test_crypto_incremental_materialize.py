"""Integration test for incremental ``_materialize_once`` (narrower variant).

This is the narrower Part C variant described in the task: rather than asserting
full byte-for-byte parity with a from-scratch rebuild (which is entangled with
cross-market recency features such as ``asset_recent_*`` that legitimately depend
on the whole lookback window and therefore differ when the read window is
narrowed), it calls ``_materialize_once`` twice on the SAME db:

  1. a cold full build (no watermark yet -> reads the full lookback window), then
  2. an incremental build after a previously-open market has settled and a new
     market has appeared.

It asserts the incremental run (a) narrows the READ window to the watermark-warmup
tail, (b) refreshes the late-settled market's label that flipped from None to a
real settlement between builds, and (c) preserves the earlier rows it chose not
to recompute. The PURE helper tests in ``test_incremental_materialize_since.py``
are the primary correctness guarantee for the window math; this test is
defense-in-depth that the wiring behaves end-to-end.

Uses sqlite (no pgvector) like the rest of ``tests/unit``. We reuse the fully
populated ``_snapshot`` builder from ``test_crypto_training_data`` and set the
remaining quote columns so the ``defer_payload=True`` read path never falls back
to the deferred ``.payload`` on a detached instance (DetachedInstanceError).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto import services as crypto_services
from kalshi_bot.crypto.services import CryptoTrainingBackfillService
from kalshi_bot.db.models import CryptoSpotOHLCRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

from tests.unit.test_crypto_training_data import _snapshot


NOW = datetime(2026, 6, 13, 0, 0, tzinfo=UTC)
WARMUP_HOURS = 24
LOOKBACK_DAYS = 30


def _make_snapshot(ticker: str, *, observed_at, settlement_result, close_time):
    """Build a fully-priced snapshot. ``_snapshot`` omits ``no_bid_dollars``,
    which would force ``_crypto_decision_rows`` to fall back to the deferred
    ``.payload`` on a detached instance (DetachedInstanceError). Set every quote
    column the decision-row path reads, and an explicit empty payload."""
    record = _snapshot(
        ticker,
        observed_at=observed_at,
        settlement_result=settlement_result,
        close_time=close_time,
    )
    record.no_bid_dollars = Decimal("0.6900")
    record.payload = {}
    return record


def _settled_market(ticker: str, *, close_time: datetime, result: str) -> list:
    """A settled market = a pre-close decision snapshot + the settlement snapshot."""
    return [
        _make_snapshot(
            ticker,
            observed_at=close_time - timedelta(minutes=10),
            settlement_result=None,
            close_time=close_time,
        ),
        _make_snapshot(
            ticker,
            observed_at=close_time,
            settlement_result=result,
            close_time=close_time,
        ),
    ]


def _spot_rows() -> list[CryptoSpotOHLCRecord]:
    """Dense hourly BTC spot history across the whole lookback window so decision
    rows can compute spot features regardless of the read window narrowing."""
    rows: list[CryptoSpotOHLCRecord] = []
    start = NOW - timedelta(days=LOOKBACK_DAYS)
    hours = LOOKBACK_DAYS * 24 + 1
    price = Decimal("70000")
    for i in range(hours):
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


def _settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/incremental.db",
        kalshi_env="production",
        crypto_train_lookback_days=LOOKBACK_DAYS,
        crypto_train_incremental_materialize_enabled=True,
        crypto_train_incremental_warmup_hours=WARMUP_HOURS,
        crypto_train_incremental_max_gap_hours=24 * 365,
        crypto_min_training_samples=1,
    )


async def _factory(settings: Settings):
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


def _service(settings: Settings, session_factory) -> CryptoTrainingBackfillService:
    return CryptoTrainingBackfillService(
        settings=settings,
        session_factory=session_factory,
        history_service=object(),
        spot_service=None,
    )


async def _store_map(session_factory, settings: Settings) -> dict[str, tuple[str, int | None]]:
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        rows = await repo.list_crypto_training_feature_rows(
            frequency="15m",
            kalshi_env=settings.kalshi_env,
            limit=10000,
        )
    return {row.row_id: (row.feature_hash, row.label_yes) for row in rows}


async def _seed(session_factory, records: list) -> None:
    async with session_factory() as session:
        for record in records:
            session.add(record)
        await session.commit()


@pytest.mark.asyncio
async def test_incremental_materialize_narrows_window_and_refreshes_late_label(tmp_path, monkeypatch) -> None:
    # Markets spaced so OLD/MID fall OUTSIDE the incremental tail (watermark -
    # warmup) but inside the full lookback window; LATE sits in the warmup
    # overlap (open at build A, settled at build B); NEW only appears at build B.
    old_close = NOW - timedelta(days=10)
    mid_close = NOW - timedelta(days=5)
    late_close = NOW - timedelta(hours=6)
    new_close = NOW - timedelta(hours=2)

    old_market = _settled_market("KXBTC15M-OLD", close_time=old_close, result="yes")
    mid_market = _settled_market("KXBTC15M-MID", close_time=mid_close, result="no")
    late_open = [
        _make_snapshot(
            "KXBTC15M-LATE",
            observed_at=late_close - timedelta(minutes=10),
            settlement_result=None,
            close_time=late_close,
        )
    ]
    late_settle = _make_snapshot(
        "KXBTC15M-LATE",
        observed_at=late_close,
        settlement_result="yes",
        close_time=late_close,
    )
    new_market = _settled_market("KXBTC15M-NEW", close_time=new_close, result="no")

    settings = _settings(tmp_path)
    session_factory = await _factory(settings)
    await _seed(session_factory, _spot_rows())

    # Capture the effective `since` the resolver hands the READ phase on each call.
    real_resolve = crypto_services._resolve_incremental_materialize_since
    resolved: list[tuple[datetime, str]] = []

    def _spy(**kwargs):
        result = real_resolve(**kwargs)
        resolved.append(result)
        return result

    monkeypatch.setattr(crypto_services, "_resolve_incremental_materialize_since", _spy)

    service = _service(settings, session_factory)

    # ---- Build A: cold full build (no watermark yet) ----
    await _seed(session_factory, [*old_market, *mid_market, *late_open])
    await service._materialize_once(
        frequency="15m",
        asset_symbols=["BTC"],
        materialize_microstructure=False,
        materialize_settlement_windows=False,
    )
    store_a = await _store_map(session_factory, settings)
    # Cold cache -> full window read. OLD + MID settled (LATE still open -> no row).
    assert resolved[-1][1] == "full_cold_cache"
    assert "KXBTC15M-OLD:2026-06-02T23:50:00" in store_a
    assert "KXBTC15M-MID:2026-06-07T23:50:00" in store_a
    late_key = "KXBTC15M-LATE:2026-06-12T17:50:00"
    # LATE is open at build A: its decision row exists but has no settled label yet.
    assert store_a.get(late_key, (None, "missing"))[1] is None or late_key not in store_a

    # ---- Build B: incremental (LATE now settled + brand-new market) ----
    await _seed(session_factory, [late_settle, *new_market])
    await service._materialize_once(
        frequency="15m",
        asset_symbols=["BTC"],
        materialize_microstructure=False,
        materialize_settlement_windows=False,
    )
    store_b = await _store_map(session_factory, settings)

    # (a) The incremental run narrowed the READ window to watermark - warmup, which
    # is strictly newer than the full lookback window start.
    effective_since, reason = resolved[-1]
    assert reason == "incremental"
    full_since_floor = NOW - timedelta(days=LOOKBACK_DAYS + 1)
    assert effective_since > full_since_floor
    # The tail must start well after the OLD market (10 days back) and after the
    # MID market minus warmup, proving genuine narrowing.
    assert effective_since > mid_close - timedelta(hours=WARMUP_HOURS + 1)

    # (b) The late-settling market's label was refreshed from None -> 1 (yes).
    assert store_b[late_key][1] == 1
    # And the brand-new market appeared with its settled label (no -> 0).
    assert store_b["KXBTC15M-NEW:2026-06-12T21:50:00"][1] == 0

    # (c) Rows the incremental run did NOT recompute (OLD, outside the tail) are
    # preserved untouched from build A.
    assert "KXBTC15M-OLD:2026-06-02T23:50:00" in store_b
    assert store_b["KXBTC15M-OLD:2026-06-02T23:50:00"] == store_a["KXBTC15M-OLD:2026-06-02T23:50:00"]
