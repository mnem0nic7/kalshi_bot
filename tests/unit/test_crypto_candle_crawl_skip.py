"""Candlestick crawl skip: settled markets with complete stored candles are not
re-fetched from Kalshi on every history run (the nightly used to re-crawl the
whole lookback window, starving the live API budget for hours)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from kalshi_bot.crypto.services import CryptoHistoryService


def _service(*, skip_enabled: bool = True, concurrency: int = 1) -> CryptoHistoryService:
    settings = SimpleNamespace(
        kalshi_env="production",
        crypto_history_skip_existing_candles=skip_enabled,
        crypto_history_candle_concurrency=concurrency,
    )
    return CryptoHistoryService(
        settings=settings,  # type: ignore[arg-type]
        session_factory=None,  # type: ignore[arg-type]
        kalshi=None,  # type: ignore[arg-type]
        market_service=None,  # type: ignore[arg-type]
    )


def _market(ticker: str, *, close_offset_minutes: int | None) -> SimpleNamespace:
    close_time = (
        datetime.now(UTC) + timedelta(minutes=close_offset_minutes)
        if close_offset_minutes is not None
        else None
    )
    return SimpleNamespace(market_ticker=ticker, close_time=close_time)


def test_partition_skips_settled_markets_with_complete_coverage() -> None:
    service = _service()
    settled = _market("SETTLED-FULL", close_offset_minutes=-120)
    captures: list = []

    pending = service._partition_covered_markets(
        [settled],
        captures,
        coverage={"SETTLED-FULL": settled.close_time},
    )

    assert pending == []
    assert captures[0][1]["status"] == "skipped_existing"


def test_partition_refetches_partial_coverage() -> None:
    service = _service()
    settled = _market("SETTLED-PARTIAL", close_offset_minutes=-120)
    # Stored candles stop 10 minutes before close: the tail is missing.
    coverage = {"SETTLED-PARTIAL": settled.close_time - timedelta(minutes=10)}
    captures: list = []

    pending = service._partition_covered_markets([settled], captures, coverage=coverage)

    assert pending == [settled]
    assert captures == []


def test_partition_never_skips_open_or_closeless_markets() -> None:
    service = _service()
    open_market = _market("OPEN", close_offset_minutes=30)
    closeless = _market("NO-CLOSE", close_offset_minutes=None)
    far_future = datetime.now(UTC) + timedelta(days=1)
    captures: list = []

    pending = service._partition_covered_markets(
        [open_market, closeless],
        captures,
        coverage={"OPEN": far_future, "NO-CLOSE": far_future},
    )

    assert pending == [open_market, closeless]
    assert captures == []


def test_partition_handles_naive_coverage_timestamps() -> None:
    """SQLite returns naive datetimes for DateTime(timezone=True) columns."""
    service = _service()
    settled = _market("NAIVE", close_offset_minutes=-60)
    naive_coverage = settled.close_time.replace(tzinfo=None)
    captures: list = []

    pending = service._partition_covered_markets([settled], captures, coverage={"NAIVE": naive_coverage})

    assert pending == []
    assert captures[0][1]["status"] == "skipped_existing"


class _FakeRepo:
    def __init__(self, coverage: dict) -> None:
        self.coverage = coverage
        self.calls: list[dict] = []

    async def map_crypto_candlestick_coverage(self, **kwargs):
        self.calls.append(kwargs)
        return self.coverage


class _FakeSession:
    async def commit(self) -> None:
        pass


async def test_capture_skips_covered_and_fetches_rest(monkeypatch) -> None:
    service = _service()
    settled = _market("COVERED", close_offset_minutes=-90)
    fresh = _market("NEEDS-FETCH", close_offset_minutes=-5)
    repo = _FakeRepo({"COVERED": settled.close_time})
    fetched: list[str] = []

    async def fake_fetch(market, *, cutoff):
        fetched.append(market.market_ticker)
        return {"status": "ok", "stored": 0, "candles": [], "source": "live", "attempted_sources": ["live"]}

    async def fake_store(repo_arg, market, capture):
        return {**{k: v for k, v in capture.items() if k != "candles"}, "stored": 0}

    monkeypatch.setattr(service, "_fetch_candle_rows", fake_fetch)
    monkeypatch.setattr(service, "_store_candle_rows", fake_store)

    captures = await service._capture_candles_for_markets(
        _FakeSession(),  # type: ignore[arg-type]
        repo,  # type: ignore[arg-type]
        [settled, fresh],
        cutoff=datetime.now(UTC) - timedelta(days=2),
        commit_batch_size=250,
        frequency="1h",
    )

    assert fetched == ["NEEDS-FETCH"]
    statuses = {market.market_ticker: capture["status"] for market, capture in captures}
    assert statuses == {"COVERED": "skipped_existing", "NEEDS-FETCH": "ok"}
    assert repo.calls[0]["frequency"] == "1h"
    assert repo.calls[0]["kalshi_env"] == "production"


async def test_capture_skip_disabled_fetches_everything(monkeypatch) -> None:
    service = _service(skip_enabled=False)
    settled = _market("COVERED", close_offset_minutes=-90)
    repo = _FakeRepo({"COVERED": settled.close_time})
    fetched: list[str] = []

    async def fake_fetch(market, *, cutoff):
        fetched.append(market.market_ticker)
        return {"status": "ok", "stored": 0, "candles": [], "source": "live", "attempted_sources": ["live"]}

    async def fake_store(repo_arg, market, capture):
        return {**{k: v for k, v in capture.items() if k != "candles"}, "stored": 0}

    monkeypatch.setattr(service, "_fetch_candle_rows", fake_fetch)
    monkeypatch.setattr(service, "_store_candle_rows", fake_store)

    captures = await service._capture_candles_for_markets(
        _FakeSession(),  # type: ignore[arg-type]
        repo,  # type: ignore[arg-type]
        [settled],
        cutoff=datetime.now(UTC) - timedelta(days=2),
        commit_batch_size=250,
        frequency="1h",
    )

    assert fetched == ["COVERED"]
    assert repo.calls == []
    assert captures[0][1]["status"] == "ok"
