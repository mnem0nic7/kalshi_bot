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
    resolved: list[tuple[datetime, datetime | None, str]] = []

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
    assert resolved[-1][2] == "full_cold_cache"
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
    effective_since, upsert_floor, reason = resolved[-1]
    assert reason == "incremental"
    assert upsert_floor is not None
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


# --- Real byte-for-byte parity test for the label-refresh zone --------------
#
# warmup_hours is set large enough that the incremental read window contains the
# FULL recency look-back (<=20 prior settled markets) for every re-upserted row,
# and label_refresh_hours is small so only the newest few rows are re-upserted.
# Under those conditions every re-upserted row MUST recompute byte-for-byte
# identically to a from-scratch full rebuild.
PARITY_WARMUP_HOURS = 20 * 24  # 20 days: contains all priors for re-upserted rows
PARITY_LABEL_REFRESH_HOURS = 2  # only rows within 2h of the watermark are re-upserted


def _parity_settings(tmp_path, name: str) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/{name}.db",
        kalshi_env="production",
        crypto_train_lookback_days=LOOKBACK_DAYS,
        crypto_train_incremental_materialize_enabled=True,
        crypto_train_incremental_warmup_hours=PARITY_WARMUP_HOURS,
        crypto_train_incremental_max_gap_hours=24 * 365,
        crypto_train_incremental_label_refresh_hours=PARITY_LABEL_REFRESH_HOURS,
        crypto_min_training_samples=1,
    )


def _parity_market_set() -> dict[str, datetime]:
    """A run of settled BTC markets spaced 1h apart ending near NOW. Spacing keeps
    the whole set inside the 20-day warmup so re-upserted rows' look-back is fully
    loaded in the narrow read window."""
    return {
        "KXBTC15M-M01": NOW - timedelta(hours=12),
        "KXBTC15M-M02": NOW - timedelta(hours=11),
        "KXBTC15M-M03": NOW - timedelta(hours=10),
        "KXBTC15M-M04": NOW - timedelta(hours=9),
        "KXBTC15M-M05": NOW - timedelta(hours=8),
        "KXBTC15M-M06": NOW - timedelta(hours=7),
        "KXBTC15M-M07": NOW - timedelta(hours=6),
        "KXBTC15M-M08": NOW - timedelta(hours=5),
        # M09 is OPEN at build A, SETTLES at build B (label None -> settled).
        "KXBTC15M-M09": NOW - timedelta(hours=2),
        # M10 is brand-new at build B.
        "KXBTC15M-M10": NOW - timedelta(hours=1),
    }


def _result_for(idx: int) -> str:
    return "yes" if idx % 2 == 0 else "no"


@pytest.mark.asyncio
async def test_incremental_refresh_zone_byte_for_byte_parity(tmp_path) -> None:
    markets = _parity_market_set()
    closes = list(markets.items())

    # ---- Reference FULL rebuild (incremental disabled) on its own DB ----
    full_settings = _parity_settings(tmp_path, "parity_full")
    full_settings = full_settings.model_copy(
        update={"crypto_train_incremental_materialize_enabled": False}
    )
    full_factory = await _factory(full_settings)
    await _seed(full_factory, _spot_rows())
    full_records: list = []
    for idx, (ticker, close) in enumerate(closes):
        full_records.extend(_settled_market(ticker, close_time=close, result=_result_for(idx)))
    await _seed(full_factory, full_records)
    full_service = _service(full_settings, full_factory)
    await full_service._materialize_once(
        frequency="15m",
        asset_symbols=["BTC"],
        materialize_microstructure=False,
        materialize_settlement_windows=False,
    )
    FULL = await _store_map(full_factory, full_settings)

    # ---- Incremental path: build A (cold full) then build B (incremental) ----
    inc_settings = _parity_settings(tmp_path, "parity_inc")
    inc_factory = await _factory(inc_settings)
    await _seed(inc_factory, _spot_rows())

    # Build A: everything EXCEPT M09's settlement and M10 entirely. M09 is open.
    build_a_records: list = []
    open_idx = list(markets).index("KXBTC15M-M09")
    new_idx = list(markets).index("KXBTC15M-M10")
    for idx, (ticker, close) in enumerate(closes):
        if idx == new_idx:
            continue  # M10 not present yet
        if idx == open_idx:
            # M09 only has its pre-close (open) snapshot at build A.
            build_a_records.append(
                _make_snapshot(
                    ticker,
                    observed_at=close - timedelta(minutes=10),
                    settlement_result=None,
                    close_time=close,
                )
            )
            continue
        build_a_records.extend(_settled_market(ticker, close_time=close, result=_result_for(idx)))
    await _seed(inc_factory, build_a_records)
    inc_service = _service(inc_settings, inc_factory)
    await inc_service._materialize_once(
        frequency="15m",
        asset_symbols=["BTC"],
        materialize_microstructure=False,
        materialize_settlement_windows=False,
    )
    store_a = await _store_map(inc_factory, inc_settings)

    # Watermark after build A = max decision_ts persisted (M09's open decision row,
    # observed 10 min before its close at NOW-2h, is the newest).
    async with inc_factory() as session:
        repo = PlatformRepository(session, kalshi_env=inc_settings.kalshi_env)
        watermark_b = await repo.get_crypto_training_feature_watermark(
            frequency="15m",
            kalshi_env=inc_settings.kalshi_env,
            feature_schema_version=crypto_services.CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
        )
    assert watermark_b is not None
    if watermark_b.tzinfo is None:
        watermark_b = watermark_b.replace(tzinfo=UTC)
    upsert_floor_b = watermark_b - timedelta(hours=PARITY_LABEL_REFRESH_HOURS)

    # Build B: settle M09 + add brand-new M10 (incremental ON).
    m09_ticker, m09_close = closes[open_idx]
    m10_ticker, m10_close = closes[new_idx]
    build_b_records: list = [
        _make_snapshot(
            m09_ticker,
            observed_at=m09_close,
            settlement_result=_result_for(open_idx),
            close_time=m09_close,
        ),
        *_settled_market(m10_ticker, close_time=m10_close, result=_result_for(new_idx)),
    ]
    await _seed(inc_factory, build_b_records)
    await inc_service._materialize_once(
        frequency="15m",
        asset_symbols=["BTC"],
        materialize_microstructure=False,
        materialize_settlement_windows=False,
    )
    store_b = await _store_map(inc_factory, inc_settings)

    # Identify which rows build B re-upserted: decision_ts > upsert_floor.
    def _decision_ts(row_id: str) -> datetime:
        return datetime.fromisoformat(row_id.split(":", 1)[1]).replace(tzinfo=UTC)

    refreshed_ids = [rid for rid in store_b if _decision_ts(rid) > upsert_floor_b]
    assert refreshed_ids, "expected at least one re-upserted row in the refresh zone"

    # PARITY: every re-upserted row equals the from-scratch full rebuild
    # byte-for-byte on both feature_hash AND label_yes.
    for rid in refreshed_ids:
        assert rid in FULL, f"re-upserted row {rid} missing from full rebuild"
        assert store_b[rid] == FULL[rid], (
            f"refresh-zone parity violated for {rid}: "
            f"incremental={store_b[rid]} full={FULL[rid]}"
        )

    # At least one re-upserted row had its label refreshed None -> settled (M09).
    m09_key = next(rid for rid in refreshed_ids if rid.startswith(f"{m09_ticker}:"))
    assert store_a.get(m09_key, (None, None))[1] is None
    assert store_b[m09_key][1] in {0, 1}

    # Rows OLDER than the upsert floor were NOT modified by build B (preserved
    # exactly from build A).
    preserved_ids = [rid for rid in store_a if _decision_ts(rid) <= upsert_floor_b]
    assert preserved_ids, "expected at least one preserved row below the upsert floor"
    for rid in preserved_ids:
        assert store_b[rid] == store_a[rid], (
            f"row {rid} below upsert floor was modified by incremental build B"
        )
