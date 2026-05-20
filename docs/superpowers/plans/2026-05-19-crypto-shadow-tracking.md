# Crypto Shadow Trading Performance Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist every would-have-gone-live crypto shadow decision, score it against market settlement on the daemon heartbeat, and surface rolling P&L via a `crypto-shadow-pnl` CLI command.

**Architecture:** A new `CryptoShadowService` (record / score / report) backed by a new `crypto_shadow_decisions` table. Capture happens in `CryptoExecutionService.execute()` when a live-quality candidate is suppressed solely by `app_shadow_mode`/kill-switch; scoring is an idempotent active-color heartbeat sweep that joins decisions to `crypto_market_snapshots.settlement_result`. Read-only with respect to trading.

**Tech Stack:** Python 3.12, SQLAlchemy async, Alembic, pytest (SQLite for unit tests), argparse CLI.

**Spec:** `docs/superpowers/specs/2026-05-19-crypto-shadow-tracking-design.md`

**Conventions to follow:**
- Run tests with `.venv/bin/python -m pytest <path> -q` (the venv has pytest 9.0.3).
- Unit tests build a session via `Settings(database_url="sqlite+aiosqlite:///{tmp_path}/x.db")` → `create_engine` → `await init_models(engine)` → `create_session_factory(engine)` (see `tests/unit/test_crypto_forecast_replay_execution.py`).
- New crypto code lives in `src/kalshi_bot/crypto/services.py` alongside the other crypto services.
- `init_models` on SQLite is slow (~60-90s/file on a loaded host) — expect minute-plus test runs.

---

### Task 1: Config flag

**Files:**
- Modify: `src/kalshi_bot/config.py` (near `crypto_min_training_samples`, ~line 187)
- Test: `tests/unit/test_crypto_shadow_tracking.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_crypto_shadow_tracking.py
from __future__ import annotations

from kalshi_bot.config import Settings


def test_shadow_tracking_enabled_defaults_true() -> None:
    assert Settings().crypto_shadow_tracking_enabled is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_shadow_tracking_enabled_defaults_true -q`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'crypto_shadow_tracking_enabled'`

- [ ] **Step 3: Add the field**

In `src/kalshi_bot/config.py`, immediately after `crypto_min_training_samples: int = 250`:

```python
    crypto_shadow_tracking_enabled: bool = True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_shadow_tracking_enabled_defaults_true -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/config.py tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): add crypto_shadow_tracking_enabled flag"
```

---

### Task 2: Database model + migration

**Files:**
- Modify: `src/kalshi_bot/db/models.py` (add class after `CryptoModelArtifactRecord`, ~line 228)
- Create: `alembic/versions/<rev>_crypto_shadow_decisions.py`
- Test: `tests/unit/test_crypto_shadow_tracking.py`

- [ ] **Step 1: Write the failing test**

```python
# add to tests/unit/test_crypto_shadow_tracking.py
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from kalshi_bot.db.models import CryptoShadowDecisionRecord
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/shadow.db")
    engine = create_engine(settings)
    await init_models(engine)
    return create_session_factory(engine)


@pytest.mark.asyncio
async def test_shadow_decision_row_round_trips(tmp_path) -> None:
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CryptoShadowDecisionRecord(
                kalshi_env="production",
                frequency="15m",
                asset_symbol="BTC",
                series_ticker="KXBTC15M",
                market_ticker="KXBTC15M-T",
                decided_at=datetime(2026, 5, 20, tzinfo=UTC),
                side="yes",
                entry_price_dollars=Decimal("0.4200"),
                count_fp=Decimal("3.00"),
                candidate_status="live_quality",
                suppressed_by=["app_shadow_mode"],
                decision_context={"edge_bps": 600},
            )
        )
        await session.commit()
        rows = list((await session.execute(__import__("sqlalchemy").select(CryptoShadowDecisionRecord))).scalars())
    assert len(rows) == 1
    assert rows[0].settled_at is None
    assert rows[0].realized_pl_dollars is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_shadow_decision_row_round_trips -q`
Expected: FAIL — `ImportError: cannot import name 'CryptoShadowDecisionRecord'`

- [ ] **Step 3: Add the model**

In `src/kalshi_bot/db/models.py`, after the `CryptoModelArtifactRecord` class (before `class SignalRecord`):

```python
class CryptoShadowDecisionRecord(Base, IdMixin, TimestampMixin):
    __tablename__ = "crypto_shadow_decisions"
    __table_args__ = (
        UniqueConstraint(
            "kalshi_env", "market_ticker", "decided_at", "side",
            name="uq_crypto_shadow_decision_identity",
        ),
        Index("ix_crypto_shadow_decisions_open", "settled_at"),
        Index("ix_crypto_shadow_decisions_report", "kalshi_env", "frequency", "decided_at"),
    )

    kalshi_env: Mapped[str] = mapped_column(String(16), nullable=False, default="demo", index=True)
    frequency: Mapped[str] = mapped_column(String(32), nullable=False, default="15m", index=True)
    asset_symbol: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    entry_price_dollars: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    count_fp: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    candidate_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    agent_pack_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    app_color: Mapped[str | None] = mapped_column(String(16), nullable=True)
    suppressed_by: Mapped[list] = mapped_column(JSON, default=list)
    decision_context: Mapped[dict] = mapped_column(JSON, default=dict)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    settlement_result: Mapped[str | None] = mapped_column(String(16), nullable=True)
    won: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    realized_pl_dollars: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
```

Verify `UniqueConstraint`, `Index`, `Boolean`, `Numeric`, `JSON`, `String`, `DateTime`, `Mapped`, `mapped_column`, `Decimal`, `datetime` are already imported at the top of `models.py` (they are used by neighboring models — confirm before running).

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_shadow_decision_row_round_trips -q`
Expected: PASS

- [ ] **Step 5: Generate the Alembic migration**

Run: `.venv/bin/python -m alembic revision --autogenerate -m "crypto_shadow_decisions"`
Then open the new file under `alembic/versions/` and confirm `op.create_table("crypto_shadow_decisions", ...)` includes all columns above plus the unique constraint and two indexes. If autogenerate missed the indexes, add them explicitly with `op.create_index(...)`. Confirm `downgrade()` drops the table.

- [ ] **Step 6: Commit**

```bash
git add src/kalshi_bot/db/models.py alembic/versions/ tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): add crypto_shadow_decisions table + migration"
```

---

### Task 3: Repository methods

**Files:**
- Modify: `src/kalshi_bot/db/repositories.py` (add after `list_crypto_settled_market_snapshots`, ~line 975)
- Test: `tests/unit/test_crypto_shadow_tracking.py`

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_repo_insert_and_list_open_shadow_decisions(tmp_path) -> None:
    from kalshi_bot.db.repositories import PlatformRepository
    session_factory = await _session_factory(tmp_path)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.insert_crypto_shadow_decision(
            kalshi_env="production", frequency="15m", asset_symbol="BTC",
            series_ticker="KXBTC15M", market_ticker="KXBTC15M-T",
            decided_at=datetime(2026, 5, 20, tzinfo=UTC), side="yes",
            entry_price_dollars=Decimal("0.4200"), count_fp=Decimal("3.00"),
            candidate_status="live_quality", agent_pack_version=None, app_color="blue",
            suppressed_by=["app_shadow_mode"], decision_context={"edge_bps": 600},
        )
        await session.commit()
        open_rows = await repo.list_open_crypto_shadow_decisions(kalshi_env="production")
    assert len(open_rows) == 1
    assert open_rows[0].market_ticker == "KXBTC15M-T"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_repo_insert_and_list_open_shadow_decisions -q`
Expected: FAIL — `AttributeError: ... has no attribute 'insert_crypto_shadow_decision'`

- [ ] **Step 3: Add the repo methods**

In `src/kalshi_bot/db/repositories.py`, after `list_crypto_settled_market_snapshots`:

```python
    async def insert_crypto_shadow_decision(
        self,
        *,
        kalshi_env: str,
        frequency: str,
        asset_symbol: str,
        series_ticker: str | None,
        market_ticker: str,
        decided_at: datetime,
        side: str,
        entry_price_dollars: Decimal,
        count_fp: Decimal,
        candidate_status: str | None,
        agent_pack_version: str | None,
        app_color: str | None,
        suppressed_by: list[str],
        decision_context: dict[str, Any],
    ) -> CryptoShadowDecisionRecord:
        record = CryptoShadowDecisionRecord(
            kalshi_env=kalshi_env,
            frequency=frequency,
            asset_symbol=asset_symbol,
            series_ticker=series_ticker,
            market_ticker=market_ticker,
            decided_at=decided_at,
            side=side,
            entry_price_dollars=entry_price_dollars,
            count_fp=count_fp,
            candidate_status=candidate_status,
            agent_pack_version=agent_pack_version,
            app_color=app_color,
            suppressed_by=list(suppressed_by),
            decision_context=dict(decision_context),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_open_crypto_shadow_decisions(
        self,
        *,
        kalshi_env: str | None = None,
        limit: int = 5000,
    ) -> list[CryptoShadowDecisionRecord]:
        stmt = select(CryptoShadowDecisionRecord).where(
            CryptoShadowDecisionRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoShadowDecisionRecord.settled_at.is_(None),
        ).order_by(CryptoShadowDecisionRecord.decided_at.asc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def get_crypto_settlement_result(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> str | None:
        stmt = select(CryptoMarketSnapshotRecord.settlement_result).where(
            CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoMarketSnapshotRecord.market_ticker == market_ticker,
            CryptoMarketSnapshotRecord.settlement_result.in_(["yes", "no"]),
        ).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_crypto_shadow_decisions(
        self,
        *,
        kalshi_env: str | None = None,
        frequency: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        scored_only: bool = False,
        limit: int = 100000,
    ) -> list[CryptoShadowDecisionRecord]:
        stmt = select(CryptoShadowDecisionRecord).where(
            CryptoShadowDecisionRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoShadowDecisionRecord.frequency == frequency)
        symbols = [s for s in (asset_symbols or []) if str(s or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoShadowDecisionRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoShadowDecisionRecord.decided_at >= since)
        if scored_only:
            stmt = stmt.where(CryptoShadowDecisionRecord.settled_at.is_not(None))
        stmt = stmt.order_by(CryptoShadowDecisionRecord.decided_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())
```

Add `CryptoShadowDecisionRecord` to the model imports at the top of `repositories.py` (where `CryptoMarketSnapshotRecord` is imported). `Any`, `select`, `datetime`, `Decimal` are already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_repo_insert_and_list_open_shadow_decisions -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/db/repositories.py tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): repository CRUD + settlement lookup"
```

---

### Task 4: `CryptoShadowService.record_decision`

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py` (add new `CryptoShadowService` class after `CryptoForecastService`)
- Test: `tests/unit/test_crypto_shadow_tracking.py`

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_shadow_service_record_decision_persists_intended_order(tmp_path) -> None:
    from kalshi_bot.crypto.services import CryptoShadowService
    from kalshi_bot.db.repositories import PlatformRepository
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/svc.db", kalshi_env="production")
    session_factory = await _session_factory(tmp_path)
    service = CryptoShadowService(settings=settings, session_factory=session_factory)
    await service.record_decision(
        frequency="15m", asset_symbol="BTC", series_ticker="KXBTC15M",
        market_ticker="KXBTC15M-T", decided_at=datetime(2026, 5, 20, tzinfo=UTC),
        side="yes", entry_price_dollars=Decimal("0.4200"), count_fp=Decimal("3.00"),
        candidate_status="live_quality", agent_pack_version=None, app_color="blue",
        suppressed_by=["app_shadow_mode"], decision_context={"edge_bps": 600},
    )
    async with session_factory() as session:
        rows = await PlatformRepository(session).list_open_crypto_shadow_decisions(kalshi_env="production")
    assert len(rows) == 1
    assert rows[0].side == "yes"
    assert rows[0].count_fp == Decimal("3.00")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_shadow_service_record_decision_persists_intended_order -q`
Expected: FAIL — `ImportError: cannot import name 'CryptoShadowService'`

- [ ] **Step 3: Add the service skeleton + `record_decision`**

In `src/kalshi_bot/crypto/services.py`, after the `CryptoForecastService` class:

```python
class CryptoShadowService:
    """Records would-have-gone-live shadow decisions and scores them against settlement.

    Read-only with respect to trading: it observes decisions and computes hypothetical
    P&L. It never touches execution, the kill switch, or shadow mode.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def record_decision(
        self,
        *,
        frequency: str,
        asset_symbol: str,
        series_ticker: str | None,
        market_ticker: str,
        decided_at: datetime,
        side: str,
        entry_price_dollars: Decimal,
        count_fp: Decimal,
        candidate_status: str | None,
        agent_pack_version: str | None,
        app_color: str | None,
        suppressed_by: list[str],
        decision_context: dict[str, Any],
    ) -> None:
        if not self.settings.crypto_shadow_tracking_enabled:
            return
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await repo.insert_crypto_shadow_decision(
                    kalshi_env=self.settings.kalshi_env,
                    frequency=frequency,
                    asset_symbol=asset_symbol,
                    series_ticker=series_ticker,
                    market_ticker=market_ticker,
                    decided_at=decided_at,
                    side=side,
                    entry_price_dollars=entry_price_dollars,
                    count_fp=count_fp,
                    candidate_status=candidate_status,
                    agent_pack_version=agent_pack_version,
                    app_color=app_color,
                    suppressed_by=suppressed_by,
                    decision_context=decision_context,
                )
                await session.commit()
        except Exception:  # best-effort: tracking must never break the decision loop
            logger.exception("failed to record crypto shadow decision for %s", market_ticker)
```

Confirm `logger` exists at module scope in `services.py` (it does — the module logs elsewhere). `Settings`, `async_sessionmaker`, `AsyncSession`, `PlatformRepository`, `datetime`, `Decimal`, `Any` are already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_shadow_service_record_decision_persists_intended_order -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/crypto/services.py tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): CryptoShadowService.record_decision"
```

---

### Task 5: `CryptoShadowService.score_open_decisions`

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py` (`CryptoShadowService`)
- Test: `tests/unit/test_crypto_shadow_tracking.py`

- [ ] **Step 1: Write the failing tests**

```python
async def _seed_decision_and_settlement(session_factory, *, side, entry, settlement):
    from kalshi_bot.db.repositories import PlatformRepository
    from kalshi_bot.db.models import CryptoMarketSnapshotRecord
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.insert_crypto_shadow_decision(
            kalshi_env="production", frequency="15m", asset_symbol="BTC",
            series_ticker="KXBTC15M", market_ticker="KXBTC15M-T",
            decided_at=datetime(2026, 5, 20, tzinfo=UTC), side=side,
            entry_price_dollars=Decimal(entry), count_fp=Decimal("2.00"),
            candidate_status="live_quality", agent_pack_version=None, app_color="blue",
            suppressed_by=["app_shadow_mode"], decision_context={},
        )
        session.add(CryptoMarketSnapshotRecord(
            kalshi_env="production", series_ticker="KXBTC15M", market_ticker="KXBTC15M-T",
            asset_symbol="BTC", frequency="15m", status="closed",
            settlement_result=settlement, observed_at=datetime(2026, 5, 20, 1, tzinfo=UTC),
            source_kind="test",
        ))
        await session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "side,entry,settlement,won,pl",
    [
        ("yes", "0.40", "yes", True, Decimal("1.20")),   # 2 * (1 - 0.40)
        ("yes", "0.40", "no", False, Decimal("-0.80")),  # 2 * (0 - 0.40)
        ("no", "0.30", "no", True, Decimal("1.40")),     # 2 * (1 - 0.30)
        ("no", "0.30", "yes", False, Decimal("-0.60")),  # 2 * (0 - 0.30)
    ],
)
async def test_score_open_decisions_pnl(tmp_path, side, entry, settlement, won, pl) -> None:
    from kalshi_bot.crypto.services import CryptoShadowService
    from kalshi_bot.db.repositories import PlatformRepository
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/score.db", kalshi_env="production")
    session_factory = await _session_factory(tmp_path)
    await _seed_decision_and_settlement(session_factory, side=side, entry=entry, settlement=settlement)
    service = CryptoShadowService(settings=settings, session_factory=session_factory)

    scored = await service.score_open_decisions()
    assert scored == 1
    async with session_factory() as session:
        rows = await PlatformRepository(session).list_crypto_shadow_decisions(kalshi_env="production", scored_only=True)
    assert rows[0].won is won
    assert rows[0].realized_pl_dollars == pl
    assert rows[0].settlement_result == settlement

    # idempotent: a second sweep scores nothing new
    assert await service.score_open_decisions() == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest "tests/unit/test_crypto_shadow_tracking.py::test_score_open_decisions_pnl" -q`
Expected: FAIL — `AttributeError: 'CryptoShadowService' object has no attribute 'score_open_decisions'`

- [ ] **Step 3: Implement `score_open_decisions`**

Add to `CryptoShadowService`:

```python
    async def score_open_decisions(self, *, limit: int = 5000) -> int:
        if not self.settings.crypto_shadow_tracking_enabled:
            return 0
        scored = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            open_rows = await repo.list_open_crypto_shadow_decisions(
                kalshi_env=self.settings.kalshi_env, limit=limit
            )
            settlement_cache: dict[str, str | None] = {}
            for row in open_rows:
                if row.market_ticker not in settlement_cache:
                    settlement_cache[row.market_ticker] = await repo.get_crypto_settlement_result(
                        row.market_ticker, kalshi_env=self.settings.kalshi_env
                    )
                result = settlement_cache[row.market_ticker]
                if result not in {"yes", "no"}:
                    continue  # not settled yet; leave open
                won = (row.side == result)
                payout = Decimal("1") if won else Decimal("0")
                row.realized_pl_dollars = (row.count_fp * (payout - row.entry_price_dollars)).quantize(Decimal("0.0001"))
                row.won = won
                row.settlement_result = result
                row.settled_at = datetime.now(UTC)
                scored += 1
            await session.commit()
        return scored
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest "tests/unit/test_crypto_shadow_tracking.py::test_score_open_decisions_pnl" -q`
Expected: PASS (all 4 parametrized cases + idempotency)

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/crypto/services.py tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): score_open_decisions settlement sweep"
```

---

### Task 6: `CryptoShadowService.report`

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py` (`CryptoShadowService`)
- Test: `tests/unit/test_crypto_shadow_tracking.py`

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_report_aggregates_by_asset(tmp_path) -> None:
    from kalshi_bot.crypto.services import CryptoShadowService
    from kalshi_bot.db.repositories import PlatformRepository
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/rep.db", kalshi_env="production")
    session_factory = await _session_factory(tmp_path)
    # two scored BTC decisions (one win +1.20, one loss -0.80) + one open
    await _seed_decision_and_settlement(session_factory, side="yes", entry="0.40", settlement="yes")
    service = CryptoShadowService(settings=settings, session_factory=session_factory)
    await service.score_open_decisions()
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.insert_crypto_shadow_decision(
            kalshi_env="production", frequency="15m", asset_symbol="BTC",
            series_ticker="KXBTC15M", market_ticker="KXBTC15M-OPEN",
            decided_at=datetime(2026, 5, 20, tzinfo=UTC), side="yes",
            entry_price_dollars=Decimal("0.50"), count_fp=Decimal("1.00"),
            candidate_status="live_quality", agent_pack_version=None, app_color="blue",
            suppressed_by=["app_shadow_mode"], decision_context={},
        )
        await session.commit()

    report = await service.report(frequency="15m", days=7)
    btc = next(r for r in report["by_asset"] if r["asset_symbol"] == "BTC")
    assert btc["scored_trades"] == 1
    assert btc["wins"] == 1
    assert btc["open_trades"] == 1
    assert Decimal(str(btc["net_pl_dollars"])) == Decimal("1.2000")
    assert report["totals"]["scored_trades"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_report_aggregates_by_asset -q`
Expected: FAIL — `AttributeError: ... has no attribute 'report'`

- [ ] **Step 3: Implement `report`**

Add to `CryptoShadowService`:

```python
    async def report(
        self,
        *,
        frequency: str | None = None,
        days: int = 7,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        since = datetime.now(UTC) - timedelta(days=max(1, int(days)))
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            rows = await repo.list_crypto_shadow_decisions(
                kalshi_env=self.settings.kalshi_env,
                frequency=frequency,
                asset_symbols=asset_symbols,
                since=since,
            )
        buckets: dict[str, dict[str, Any]] = {}
        for row in rows:
            b = buckets.setdefault(
                row.asset_symbol,
                {"asset_symbol": row.asset_symbol, "scored_trades": 0, "open_trades": 0,
                 "wins": 0, "net_pl_dollars": Decimal("0")},
            )
            if row.settled_at is None:
                b["open_trades"] += 1
                continue
            b["scored_trades"] += 1
            if row.won:
                b["wins"] += 1
            b["net_pl_dollars"] += row.realized_pl_dollars or Decimal("0")
        by_asset = []
        totals = {"scored_trades": 0, "open_trades": 0, "wins": 0, "net_pl_dollars": Decimal("0")}
        for b in sorted(buckets.values(), key=lambda x: x["asset_symbol"]):
            b["win_rate"] = round(b["wins"] / b["scored_trades"], 4) if b["scored_trades"] else None
            b["net_pl_dollars"] = str(b["net_pl_dollars"].quantize(Decimal("0.0001")))
            by_asset.append(b)
            totals["scored_trades"] += b["scored_trades"]
            totals["open_trades"] += b["open_trades"]
            totals["wins"] += b["wins"]
            totals["net_pl_dollars"] += Decimal(b["net_pl_dollars"])
        totals["win_rate"] = round(totals["wins"] / totals["scored_trades"], 4) if totals["scored_trades"] else None
        totals["net_pl_dollars"] = str(totals["net_pl_dollars"].quantize(Decimal("0.0001")))
        return {
            "kalshi_env": self.settings.kalshi_env,
            "frequency": frequency,
            "days": days,
            "by_asset": by_asset,
            "totals": totals,
        }
```

`timedelta` is already imported in `services.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_report_aggregates_by_asset -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/crypto/services.py tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): report aggregation"
```

---

### Task 7: Wire `CryptoShadowService` into `AppContainer`

**Files:**
- Modify: `src/kalshi_bot/services/container.py` (import ~line 17; attribute ~line 135; construction ~line 220; constructor kwarg ~line 484)
- Test: `tests/integration/test_container_wiring.py` (or extend an existing container test — check `tests/integration/` for the container build test; if none asserts attributes, add a minimal one)

- [ ] **Step 1: Write the failing test**

Find the existing container build test (search: `grep -rn "AppContainer.build" tests/`). Add:

```python
@pytest.mark.asyncio
async def test_container_exposes_crypto_shadow_service(tmp_path) -> None:
    from kalshi_bot.config import Settings
    from kalshi_bot.services.container import AppContainer
    from kalshi_bot.crypto.services import CryptoShadowService
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/c.db")
    container = await AppContainer.build(settings)
    assert isinstance(container.crypto_shadow_service, CryptoShadowService)
```

(Match the actual `AppContainer.build(...)` signature found in the grep — adjust the call if it takes different args.)

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/integration/test_container_wiring.py::test_container_exposes_crypto_shadow_service -q`
Expected: FAIL — `AttributeError: 'AppContainer' object has no attribute 'crypto_shadow_service'`

- [ ] **Step 3: Wire it in**

In `src/kalshi_bot/services/container.py`:

1. Import (with the other crypto imports near line 15-17):
```python
    CryptoShadowService,
```
2. Dataclass attribute (near `crypto_forecast_service: CryptoForecastService`, ~line 135):
```python
    crypto_shadow_service: CryptoShadowService
```
3. Construction (near where `crypto_forecast_service = CryptoForecastService(...)` is built, ~line 220):
```python
        crypto_shadow_service = CryptoShadowService(
            settings=settings,
            session_factory=session_factory,
        )
```
(Use the same `session_factory` variable name the surrounding code uses.)
4. Pass into the `AppContainer(...)` / `cls(...)` constructor call (near `crypto_forecast_service=crypto_forecast_service,` ~line 484):
```python
            crypto_shadow_service=crypto_shadow_service,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/integration/test_container_wiring.py::test_container_exposes_crypto_shadow_service -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/services/container.py tests/integration/
git commit -m "feat(crypto-shadow): register CryptoShadowService in AppContainer"
```

---

### Task 8: Capture hook in `CryptoExecutionService.execute()`

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py` — `CryptoExecutionService.execute` (~line 3029) and its `__init__` (to receive the shadow service); `src/kalshi_bot/services/container.py` (pass `crypto_shadow_service` into `CryptoExecutionService`)
- Test: `tests/unit/test_crypto_shadow_tracking.py`

**Capture rule:** record exactly when the candidate is `CRYPTO_LIVE_QUALITY`, every deterministic gate would pass (trading enabled, replay gate passed, market open, asset not *explicitly* shadowed), and the only suppressor is `app_shadow_mode` and/or the kill switch. `entry_price_dollars` is the price for the side taken: `ticket.yes_price_dollars` for a `yes` ticket, `Decimal("1") - ticket.yes_price_dollars` for a `no` ticket.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_execute_records_shadow_decision_when_only_blocker_is_shadow_mode(tmp_path) -> None:
    """A live-quality candidate suppressed solely by app_shadow_mode is recorded."""
    # Build a CryptoExecutionService with app_shadow_mode=True, a live-quality candidate,
    # passing replay gate, open market, asset not explicitly shadowed. Call execute(...)
    # and assert: (a) receipt.status == "shadow_skipped", (b) exactly one row exists in
    # crypto_shadow_decisions with side/entry/count matching the ticket.
    #
    # Use the construction + seeding helpers already in
    # tests/unit/test_crypto_forecast_replay_execution.py for building a CryptoExecutionService
    # (see test_crypto_replay_gate_* and the execution tests there for the exact wiring of
    # room/control/ticket/market/signal). Reuse those fixtures rather than re-deriving them.
```

> Implementer note: the exact `CryptoExecutionService` construction is involved. Mirror the
> nearest existing execution test in `test_crypto_forecast_replay_execution.py`. Write the test
> body following that pattern, assert the row is created, and watch it fail before implementing.

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_execute_records_shadow_decision_when_only_blocker_is_shadow_mode -q`
Expected: FAIL — no row recorded (capture not implemented).

- [ ] **Step 3: Add `crypto_shadow_service` to `CryptoExecutionService.__init__`**

Add an optional dependency so existing constructions keep working:

```python
        shadow_service: "CryptoShadowService | None" = None,
```
and in the body: `self.shadow_service = shadow_service`.

- [ ] **Step 4: Implement the capture in `execute()`**

Just before the shadow short-circuit returns `shadow_skipped` (~line 3061-3069), after `asset_mode`/`gate` are computed, add a helper call that records when the rule holds. Insert this block right before `if asset_mode != CRYPTO_ASSET_MODE_LIVE:`:

```python
        await self._maybe_record_shadow_decision(
            room=room, ticket=ticket, market=market, signal=signal,
            crypto_policy=crypto_policy, gate=gate,
            explicit_asset_mode=explicit_asset_mode, control=fresh_control,
        )
```

And add the method to `CryptoExecutionService`:

```python
    async def _maybe_record_shadow_decision(
        self, *, room, ticket, market, signal, crypto_policy, gate,
        explicit_asset_mode, control,
    ) -> None:
        if self.shadow_service is None or not self.settings.crypto_shadow_tracking_enabled:
            return
        candidate_status = (signal.candidate_trace or {}).get("candidate_status")
        if candidate_status != CRYPTO_LIVE_QUALITY:
            return
        trading_enabled = self.settings.crypto_trading_enabled or bool(
            crypto_policy.trading_enabled if crypto_policy is not None else False
        )
        gates_pass = (
            trading_enabled
            and _runtime_replay_gate_passed(gate, crypto_policy)
            and not _crypto_market_closed_for_execution(market)
            and explicit_asset_mode != CRYPTO_ASSET_MODE_SHADOW
        )
        if not gates_pass:
            return
        suppressed_by: list[str] = []
        if self.settings.app_shadow_mode or room.shadow_mode:
            suppressed_by.append("app_shadow_mode")
        if getattr(control, "kill_switch_enabled", False):
            suppressed_by.append("kill_switch")
        if not suppressed_by:
            return  # would actually have gone live; nothing to shadow-record
        side = str(ticket.side).lower()
        entry = ticket.yes_price_dollars if side == "yes" else (Decimal("1") - ticket.yes_price_dollars)
        await self.shadow_service.record_decision(
            frequency=market.frequency,
            asset_symbol=market.asset_symbol,
            series_ticker=market.series_ticker,
            market_ticker=market.market_ticker,
            decided_at=datetime.now(UTC),
            side=side,
            entry_price_dollars=entry,
            count_fp=ticket.count_fp,
            candidate_status=candidate_status,
            agent_pack_version=getattr(control, "agent_pack_version", None),
            app_color=self.settings.app_color,
            suppressed_by=suppressed_by,
            decision_context={
                "fair_yes_dollars": str(getattr(signal, "fair_yes_dollars", "")),
                "candidate_status": candidate_status,
            },
        )
```

- [ ] **Step 5: Pass the service in `container.py`**

In the `CryptoExecutionService(...)` construction (~line 231) add:
```python
            shadow_service=crypto_shadow_service,
```
(Ensure `crypto_shadow_service` is constructed *before* `crypto_execution_service` in the file; move its construction up if needed.)

- [ ] **Step 6: Run the test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py::test_execute_records_shadow_decision_when_only_blocker_is_shadow_mode -q`
Expected: PASS

- [ ] **Step 7: Add the negative test (does NOT record when not live-quality)**

```python
@pytest.mark.asyncio
async def test_execute_does_not_record_exploratory_shadow(tmp_path) -> None:
    # Same setup but candidate_status = CRYPTO_EXPLORATORY_SHADOW.
    # Assert: zero rows in crypto_shadow_decisions after execute(...).
```
Run it; expect PASS (capture rule already excludes non-live-quality). If it fails, fix the predicate, not the test.

- [ ] **Step 8: Commit**

```bash
git add src/kalshi_bot/crypto/services.py src/kalshi_bot/services/container.py tests/unit/test_crypto_shadow_tracking.py
git commit -m "feat(crypto-shadow): capture would-have-gone-live decisions in execute()"
```

---

### Task 9: Score on the active-color daemon heartbeat

**Files:**
- Modify: `src/kalshi_bot/services/daemon.py` — constructor (~line 76) to accept `crypto_shadow_service`; `reconcile_once` (~line 171) to call the sweep after gate tuning, gated by active color + flag
- Modify: `src/kalshi_bot/services/container.py` — pass `crypto_shadow_service` into the daemon service construction
- Test: `tests/integration/test_daemon_service.py`

- [ ] **Step 1: Write the failing test**

Extend `tests/integration/test_daemon_service.py` (it already builds a daemon service — follow its existing fixture). Add a test that seeds one shadow decision + its settlement, runs `reconcile_once()` while active color, and asserts the decision is scored (`settled_at` not None). Use a fake/stub `crypto_shadow_service` if the existing test uses stubs, or the real one with a SQLite session factory if it uses a real container.

```python
@pytest.mark.asyncio
async def test_reconcile_scores_open_shadow_decisions(tmp_path) -> None:
    # build daemon with crypto_shadow_service + active color True
    # seed one open shadow decision whose market has settlement_result
    # await daemon.reconcile_once()
    # assert the decision now has settled_at set / realized_pl_dollars set
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/integration/test_daemon_service.py::test_reconcile_scores_open_shadow_decisions -q`
Expected: FAIL — decision still open (sweep not wired).

- [ ] **Step 3: Wire the sweep into `reconcile_once`**

In `daemon.py` constructor, add parameter `crypto_shadow_service: "CryptoShadowService | None" = None` and `self.crypto_shadow_service = crypto_shadow_service`.

In `reconcile_once`, after the autonomous-gate-tuning block (after ~line 202), add:

```python
            if (
                self.settings.crypto_shadow_tracking_enabled
                and self.crypto_shadow_service is not None
                and await self._is_active_color()
            ):
                scored = await self.crypto_shadow_service.score_open_decisions()
                result["crypto_shadow_scored"] = scored
```

(Use the exact `result` dict variable name already in `reconcile_once`.)

- [ ] **Step 4: Pass the service into the daemon in `container.py`**

In the daemon-service construction add:
```python
            crypto_shadow_service=crypto_shadow_service,
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/integration/test_daemon_service.py::test_reconcile_scores_open_shadow_decisions -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/kalshi_bot/services/daemon.py src/kalshi_bot/services/container.py tests/integration/test_daemon_service.py
git commit -m "feat(crypto-shadow): score open decisions on active-color heartbeat"
```

---

### Task 10: `crypto-shadow-pnl` CLI command

**Files:**
- Modify: `src/kalshi_bot/cli.py` — subparser (near the other `crypto-*` subparsers, ~line 4150+), command dispatch (near `if args.command == "crypto-history": ...` ~line 2679), and a handler function (near `_run_crypto_history_command`, ~line 668)
- Test: `tests/integration/test_cli_module_entrypoint.py`

- [ ] **Step 1: Write the failing test**

Follow the existing CLI test pattern in `tests/integration/test_cli_module_entrypoint.py`. Add a test that invokes the parser with `["crypto-shadow-pnl", "--kalshi-env", "production", "--frequency", "15m", "--days", "7"]` and asserts it parses to `command == "crypto-shadow-pnl"` with the expected args. If that file runs commands end-to-end against a session, assert the handler prints JSON containing `"by_asset"` and `"totals"`.

```python
def test_crypto_shadow_pnl_parses() -> None:
    from kalshi_bot.cli import build_parser  # use the actual parser factory name in cli.py
    args = build_parser().parse_args(
        ["crypto-shadow-pnl", "--kalshi-env", "production", "--frequency", "15m", "--days", "7"]
    )
    assert args.command == "crypto-shadow-pnl"
    assert args.frequency == "15m"
    assert args.days == 7
```

(Check `cli.py` for the real parser-factory name — search `def build_parser` / `argparse.ArgumentParser(`. Match it.)

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/integration/test_cli_module_entrypoint.py::test_crypto_shadow_pnl_parses -q`
Expected: FAIL — `invalid choice: 'crypto-shadow-pnl'`

- [ ] **Step 3: Add the subparser**

Near the other crypto subparsers (~line 4150), add:

```python
    crypto_shadow_pnl = subparsers.add_parser("crypto-shadow-pnl")
    add_kalshi_env_argument(crypto_shadow_pnl)
    crypto_shadow_pnl.add_argument("--frequency", default="15m")
    crypto_shadow_pnl.add_argument("--days", type=int, default=7)
    crypto_shadow_pnl.add_argument("--assets", nargs="*", default=None)
```

- [ ] **Step 4: Add the handler + dispatch**

Add a handler near `_run_crypto_history_command`:

```python
async def _run_crypto_shadow_pnl_command(args: argparse.Namespace, container: AppContainer) -> int:
    result = await container.crypto_shadow_service.report(
        frequency=args.frequency,
        days=args.days,
        asset_symbols=getattr(args, "assets", None),
    )
    print(json.dumps(result, indent=2, default=str))
    return 0
```

In the command dispatch block (near line 2679), add:

```python
        if args.command == "crypto-shadow-pnl":
            return await _run_crypto_shadow_pnl_command(args, container)
```

If `crypto-shadow-pnl` should be in the `_KNOWN_COMMANDS`/choices list (~line 122), add `"crypto-shadow-pnl"` there too.

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/integration/test_cli_module_entrypoint.py::test_crypto_shadow_pnl_parses -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/kalshi_bot/cli.py tests/integration/test_cli_module_entrypoint.py
git commit -m "feat(crypto-shadow): crypto-shadow-pnl CLI command"
```

---

### Task 11: Full-suite regression + docs

- [ ] **Step 1: Run the new test file end to end**

Run: `.venv/bin/python -m pytest tests/unit/test_crypto_shadow_tracking.py -q`
Expected: all PASS.

- [ ] **Step 2: Run the touched integration tests**

Run: `.venv/bin/python -m pytest tests/integration/test_daemon_service.py tests/integration/test_cli_module_entrypoint.py -q`
Expected: all PASS.

- [ ] **Step 3: Update CLAUDE.md crypto subsystem note**

Add one line under the Crypto subsystem section of `CLAUDE.md` documenting `CryptoShadowService` (records would-have-gone-live shadow decisions, scores them on the active-color heartbeat, surfaced by `crypto-shadow-pnl`).

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(crypto-shadow): note CryptoShadowService in CLAUDE.md"
```

---

## Self-Review Notes

- **Spec coverage:** per-decision log (Task 2/4), mirror-live capture with side/price/count_fp (Task 8), would-have-gone-live-only population (Task 8 predicate + negative test), heartbeat scoring sweep (Task 5/9), rolling P&L (Task 6), CLI surfacing (Task 10), config flag + read-only safety (Task 1, best-effort capture in Task 4). All spec sections mapped.
- **Deployment note:** none of this changes runtime behavior until the `kalshi-bot:local` image is rebuilt and containers recreated. The migration (`alembic upgrade head`) must run against `postgres_production`/`postgres_demo` before the new code reads/writes the table — the existing `migrate_*` compose services handle this on deploy. Do NOT rebuild/deploy as part of this plan unless explicitly asked.
- **Known soft spots for the implementer:** Tasks 8 and 9 reference exact line numbers/variable names that may have shifted — confirm against the current file and follow the nearest existing test fixture for `CryptoExecutionService` and the daemon. The capture predicate (Task 8) is the subtlest piece; let the positive + negative tests drive it.
