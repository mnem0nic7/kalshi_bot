# Trainer Foundation Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one trainer model run complete in minutes instead of 7.5h by (F3) bulk-writing training rows and (F1) making `_materialize_once` rebuild only the new tail instead of the full 180-day window — without changing any feature semantics. F2 (asset-parallel full rebuild) is a fast-follow.

**Architecture:** The nightly pooled path runs preflight → `CryptoTrainingBackfillService._materialize_once` (full-window `_crypto_decision_rows` rebuild + per-row upsert into `crypto_training_feature_rows`) → `train(use_feature_store=True)` (already fast, reads the store back). We make the materialize incremental: read raw data only from `watermark - warmup`, recompute, and bulk-upsert the recomputed window (idempotent on `row_id`, so late-settled labels refresh). A schema-version guard and a staleness guard fall back to a full rebuild. All changes are gated by config defaulting to today's behavior where risk warrants.

**Tech Stack:** Python 3, SQLAlchemy async (postgres in prod, sqlite in unit tests), pydantic `Settings`, pytest (`asyncio_mode=auto`).

**Key files:**
- `src/kalshi_bot/db/repositories.py` — `PlatformRepository` (bulk upsert + watermark query)
- `src/kalshi_bot/crypto/services.py` — `CryptoTrainingBackfillService._materialize_once` (`:3231`), constant `CRYPTO_RICH_FEATURE_SCHEMA_VERSION` (`:99`)
- `src/kalshi_bot/config.py` — `Settings` (`:215-222` neighborhood)
- `infra/docker-compose.yml` — env mappings (production base `:~182`, trainer `:529`)
- Tests: `tests/unit/test_crypto_training_data.py` (fixture pattern: `_session_factory`, `_snapshot`, `CryptoSpotOHLCRecord`)

---

## Task 1: F3 — `bulk_upsert_crypto_training_feature_rows` repository method

**Files:**
- Modify: `src/kalshi_bot/db/repositories.py` (add method to `PlatformRepository`, immediately after `upsert_crypto_training_feature_row` at `:2790`)
- Test: `tests/unit/test_crypto_training_bulk_upsert.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_crypto_training_bulk_upsert.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_crypto_training_bulk_upsert.py -v`
Expected: FAIL with `AttributeError: 'PlatformRepository' object has no attribute 'bulk_upsert_crypto_training_feature_rows'`

- [ ] **Step 3: Write minimal implementation**

Add after `upsert_crypto_training_feature_row` (after `:2790`). The existing per-row method and `_upsert_stmt_for` (`:2696`) already import `pg_insert` / `sqlite_insert` at the top of the file — reuse them.

```python
    async def bulk_upsert_crypto_training_feature_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        chunk_size: int = 2000,
    ) -> int:
        """Upsert many training feature rows per statement (idempotent on
        (kalshi_env, frequency, row_id)). Re-upserting an existing row_id
        refreshes its columns (e.g. a label that settled since first build)."""
        if not rows:
            return 0
        prepared: list[dict[str, Any]] = []
        for values in rows:
            payload = dict(values)
            payload["kalshi_env"] = self._resolved_kalshi_env(payload.get("kalshi_env"))
            payload.pop("id", None)
            payload.pop("created_at", None)
            prepared.append(payload)

        dialect = self.session.bind.dialect.name if self.session.bind is not None else ""
        written = 0
        conflict_keys = {"kalshi_env", "frequency", "row_id"}
        for start in range(0, len(prepared), chunk_size):
            chunk = prepared[start : start + chunk_size]
            if dialect == "postgresql":
                stmt = pg_insert(CryptoTrainingFeatureRowRecord).values(chunk)
            elif dialect == "sqlite":
                stmt = sqlite_insert(CryptoTrainingFeatureRowRecord).values(chunk)
            else:
                for payload in chunk:
                    self.session.add(CryptoTrainingFeatureRowRecord(**payload))
                await self.session.flush()
                written += len(chunk)
                continue
            update_cols = {
                col: getattr(stmt.excluded, col)
                for col in chunk[0].keys()
                if col not in conflict_keys
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    CryptoTrainingFeatureRowRecord.kalshi_env,
                    CryptoTrainingFeatureRowRecord.frequency,
                    CryptoTrainingFeatureRowRecord.row_id,
                ],
                set_=update_cols,
            )
            await self.session.execute(stmt)
            await self.session.flush()
            written += len(chunk)
        return written
```

Note: every dict in a chunk must carry the same keys (the materialize WRITE phase builds them uniformly — Task 2 preserves this).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_crypto_training_bulk_upsert.py -v`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_crypto_training_bulk_upsert.py src/kalshi_bot/db/repositories.py
git commit -m "feat: bulk_upsert_crypto_training_feature_rows (F3 repository method)"
```

---

## Task 2: F3 — use bulk upsert in the materialize WRITE phase

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py:3315-3336` (the per-row upsert loop in `_materialize_once`)
- Test: `tests/unit/test_crypto_training_data.py` (existing materialize coverage exercises this path; no new test — Task 5's parity test is the strong check)

- [ ] **Step 1: Replace the per-row loop with a single bulk call**

Current (`:3315-3336`):
```python
            for row in decision_rows:
                payload = _crypto_training_json_ready(row)
                feature_hash = _crypto_training_build_id(payload)
                await repo.upsert_crypto_training_feature_row(
                    kalshi_env=self.settings.kalshi_env,
                    frequency=freq,
                    market_ticker=str(row.get("market_ticker") or ""),
                    asset_symbol=normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")),
                    row_id=str(row.get("row_id") or feature_hash),
                    decision_time=_as_utc_datetime(row.get("decision_ts")),
                    settlement_time=_as_utc_datetime(row.get("settlement_ts")) if row.get("settlement_ts") else None,
                    label_yes=int(row["label_yes"]) if row.get("label_yes") in {0, 1} else None,
                    strict_trade_eligible=bool(row.get("strict_trade_eligible")),
                    feature_schema_version=CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
                    feature_hash=feature_hash,
                    source_build_id=build_id,
                    quality_score=_crypto_training_row_quality_score(row),
                    payload={
                        "schema_version": "crypto-training-feature-row-v1",
                        "decision_row": payload,
                    },
                )
```

Replace with:
```python
            feature_row_values: list[dict[str, Any]] = []
            for row in decision_rows:
                payload = _crypto_training_json_ready(row)
                feature_hash = _crypto_training_build_id(payload)
                feature_row_values.append(
                    dict(
                        kalshi_env=self.settings.kalshi_env,
                        frequency=freq,
                        market_ticker=str(row.get("market_ticker") or ""),
                        asset_symbol=normalize_asset_symbol(str(row.get("asset_symbol") or "UNKNOWN")),
                        row_id=str(row.get("row_id") or feature_hash),
                        decision_time=_as_utc_datetime(row.get("decision_ts")),
                        settlement_time=_as_utc_datetime(row.get("settlement_ts")) if row.get("settlement_ts") else None,
                        label_yes=int(row["label_yes"]) if row.get("label_yes") in {0, 1} else None,
                        strict_trade_eligible=bool(row.get("strict_trade_eligible")),
                        feature_schema_version=CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
                        feature_hash=feature_hash,
                        source_build_id=build_id,
                        quality_score=_crypto_training_row_quality_score(row),
                        payload={
                            "schema_version": "crypto-training-feature-row-v1",
                            "decision_row": payload,
                        },
                    )
                )
            await repo.bulk_upsert_crypto_training_feature_rows(feature_row_values)
```

- [ ] **Step 2: Run existing materialize tests to verify no regression**

Run: `pytest tests/unit/test_crypto_training_data.py -v`
Expected: PASS (all existing tests unchanged)

- [ ] **Step 3: Commit**

```bash
git add src/kalshi_bot/crypto/services.py
git commit -m "perf: bulk-write training feature rows in _materialize_once (F3)"
```

---

## Task 3: F1 — `get_crypto_training_feature_watermark` repository method

**Files:**
- Modify: `src/kalshi_bot/db/repositories.py` (add after `list_crypto_training_feature_rows` at `:2814`)
- Test: `tests/unit/test_crypto_training_bulk_upsert.py` (append — same fixture file)

- [ ] **Step 1: Write the failing test (append to the file from Task 1)**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_crypto_training_bulk_upsert.py::test_watermark_is_max_decision_time_for_schema -v`
Expected: FAIL with `AttributeError: ... 'get_crypto_training_feature_watermark'`

- [ ] **Step 3: Write minimal implementation**

`func` and `select` are already imported in repositories.py (used by `count_crypto_decision_outcomes` at `:2845`).

```python
    async def get_crypto_training_feature_watermark(
        self,
        *,
        frequency: str,
        kalshi_env: str | None = None,
        feature_schema_version: str,
    ) -> datetime | None:
        """Max persisted decision_time for (env, frequency) at a given schema
        version, or None if no schema-matched rows exist (cold cache)."""
        stmt = select(func.max(CryptoTrainingFeatureRowRecord.decision_time)).where(
            CryptoTrainingFeatureRowRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoTrainingFeatureRowRecord.frequency == frequency,
            CryptoTrainingFeatureRowRecord.feature_schema_version == feature_schema_version,
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_crypto_training_bulk_upsert.py -v`
Expected: PASS (all three tests in the file)

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_crypto_training_bulk_upsert.py src/kalshi_bot/db/repositories.py
git commit -m "feat: get_crypto_training_feature_watermark (F1 watermark query)"
```

---

## Task 4: F1 — config knobs for incremental materialize

**Files:**
- Modify: `src/kalshi_bot/config.py` (add next to `crypto_train_lookback_days` at `:215`)
- Test: `tests/unit/test_crypto_training_bulk_upsert.py` (append a trivial default-value assertion)

- [ ] **Step 1: Add the settings fields**

After `crypto_train_max_spot_rows: int = 600_000` (`:218`) add:
```python
    crypto_train_incremental_materialize_enabled: bool = True
    crypto_train_incremental_warmup_hours: int = 72
    crypto_train_incremental_max_gap_hours: int = 168
```

Rationale for defaults: `warmup_hours=72` comfortably exceeds both the longest feature look-back and the 15m/1h settlement delay (verify in Task 5). `max_gap_hours=168` (7d) → if the store is staler than a week, do a full rebuild rather than computing a giant tail.

- [ ] **Step 2: Write the default-value test (append)**

```python
def test_incremental_materialize_defaults() -> None:
    from kalshi_bot.config import Settings

    s = Settings(database_url="sqlite+aiosqlite:///:memory:")
    assert s.crypto_train_incremental_materialize_enabled is True
    assert s.crypto_train_incremental_warmup_hours == 72
    assert s.crypto_train_incremental_max_gap_hours == 168
```

- [ ] **Step 3: Run test to verify it passes**

Run: `pytest tests/unit/test_crypto_training_bulk_upsert.py::test_incremental_materialize_defaults -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/kalshi_bot/config.py tests/unit/test_crypto_training_bulk_upsert.py
git commit -m "feat: incremental-materialize config knobs (F1)"
```

---

## Task 5: F1 — incremental `effective_since` in `_materialize_once`

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py:3239-3300` (the `since`/READ phase of `_materialize_once`)
- Test: `tests/unit/test_crypto_incremental_materialize.py` (create — parity test)

This is the core correctness task. The change: compute a `watermark`-based `effective_since` and use it for all READ-phase queries; recompute and upsert the whole recomputed window (the WRITE phase already does this after Task 2 — it just operates over fewer rows). Full rebuild when: incremental disabled, no schema-matched watermark, or gap > `max_gap_hours`.

- [ ] **Step 1: Write the failing parity test**

```python
# tests/unit/test_crypto_incremental_materialize.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.crypto.services import CryptoTrainingBackfillService
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models

# Reuse the snapshot/spot fixture builders from the existing training test module
from tests.unit.test_crypto_training_data import _snapshot  # type: ignore


async def _factory(tmp_path, **overrides):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/inc.db",
        kalshi_env="production",
        **overrides,
    )
    engine = create_engine(settings)
    await init_models(engine)
    return settings, create_session_factory(engine)


async def _store_rows(session_factory, settings):
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        rows = await repo.list_crypto_training_feature_rows(
            frequency="15m", kalshi_env=settings.kalshi_env, limit=10_000
        )
        return {r.row_id: (r.feature_hash, r.label_yes) for r in rows}


@pytest.mark.asyncio
async def test_incremental_equals_full_rebuild_including_late_labels(tmp_path, monkeypatch) -> None:
    """Two builds with incremental ON must produce the same store as one full
    rebuild over the same data — including a row that settles between builds."""
    # Build A: settled snapshots up to T-2h (one market still open).
    # Build B (incremental): the open market has now settled (late label) + a new market.
    # Compare against a FULL rebuild over the union dataset.
    # NOTE: implementer fills in snapshot/spot fixtures using _snapshot(...) and
    # CryptoSpotOHLCRecord covering a window > warmup so look-back is exercised.
    pytest.skip("fixture authored during implementation; asserts incremental store == full store")
```

The implementer replaces the `pytest.skip` body with concrete fixtures: seed snapshots/spot via the session, run `_materialize_once` twice with `crypto_train_incremental_materialize_enabled=True` and a small `crypto_train_incremental_warmup_hours`, capture `_store_rows`; then on a fresh DB seed the union dataset and run one full rebuild (`crypto_train_incremental_materialize_enabled=False`); assert the two `_store_rows` dicts are equal (same row_ids, same feature_hash, same label_yes — proving look-back parity and late-label refresh).

- [ ] **Step 2: Run test to verify it currently skips (placeholder) — then author fixtures and watch it fail**

Run: `pytest tests/unit/test_crypto_incremental_materialize.py -v`
Expected: SKIP initially; after authoring fixtures, FAIL (incremental path not implemented → stores differ or method ignores config).

- [ ] **Step 3: Implement incremental `effective_since`**

In `_materialize_once`, replace the `since` computation (`:3241-3242`) and add a watermark lookup inside the READ-phase session, before the reads (`:3254`).

Current:
```python
        lookback_days = max(1, int(self.settings.crypto_train_lookback_days))
        since = datetime.now(UTC) - timedelta(days=lookback_days)
```

Replace with:
```python
        lookback_days = max(1, int(self.settings.crypto_train_lookback_days))
        now_utc = datetime.now(UTC)
        full_since = now_utc - timedelta(days=lookback_days)
        since = full_since
```

Then, inside the READ-phase `async with self.session_factory() as session:` block (at `:3254`, immediately after `repo = PlatformRepository(...)` and before `list_crypto_settled_market_snapshots`), insert:
```python
            if self.settings.crypto_train_incremental_materialize_enabled:
                watermark = await repo.get_crypto_training_feature_watermark(
                    frequency=freq,
                    kalshi_env=self.settings.kalshi_env,
                    feature_schema_version=CRYPTO_RICH_FEATURE_SCHEMA_VERSION,
                )
                warmup = timedelta(hours=max(1, int(self.settings.crypto_train_incremental_warmup_hours)))
                max_gap = timedelta(hours=max(1, int(self.settings.crypto_train_incremental_max_gap_hours)))
                if watermark is not None:
                    if watermark.tzinfo is None:
                        watermark = watermark.replace(tzinfo=UTC)
                    gap = now_utc - watermark
                    if gap <= max_gap:
                        candidate = watermark - warmup
                        # Never read MORE than the full window; never less than warmup of context.
                        since = max(full_since, candidate)
                        logger.info(
                            "crypto_materialize incremental freq=%s watermark=%s warmup_h=%s effective_since=%s",
                            freq, watermark.isoformat(), self.settings.crypto_train_incremental_warmup_hours, since.isoformat(),
                        )
                    else:
                        logger.info(
                            "crypto_materialize full_rebuild freq=%s reason=gap_exceeds_max gap_h=%.1f",
                            freq, gap.total_seconds() / 3600.0,
                        )
                else:
                    logger.info("crypto_materialize full_rebuild freq=%s reason=cold_cache_or_schema_bump", freq)
```

All existing READ queries already use `since=since`, so they automatically narrow. The WRITE phase (Task 2) upserts the whole recomputed window (idempotent), refreshing late labels. No other change needed.

**Guard rails the implementer must preserve:**
- `since = max(full_since, watermark - warmup)` — incremental never reads beyond the lookback window, and a cold/staleness/schema-bump case keeps `since = full_since` (full rebuild).
- The watermark query filters on `CRYPTO_RICH_FEATURE_SCHEMA_VERSION`, so a v9→v10 bump yields `watermark=None` → full rebuild (no mixed-schema rows).

- [ ] **Step 4: Run the parity test (and the broader training suite)**

Run: `pytest tests/unit/test_crypto_incremental_materialize.py tests/unit/test_crypto_training_data.py -v`
Expected: PASS — incremental store == full-rebuild store (incl. late label); existing tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_crypto_incremental_materialize.py src/kalshi_bot/crypto/services.py
git commit -m "perf: incremental materialize window (watermark + warmup overlap) (F1)"
```

---

## Task 6: F1/F3 — wire config into compose env + deploy trainer

**Files:**
- Modify: `infra/docker-compose.yml` (production env block near `:182`; confirm trainer inherits)

- [ ] **Step 1: Add env mappings in the production app/daemon base env block (near `CRYPTO_TRAINING_PREFLIGHT_ENABLED` at `:182`)**

```yaml
  CRYPTO_TRAIN_INCREMENTAL_MATERIALIZE_ENABLED: ${PRODUCTION_CRYPTO_TRAIN_INCREMENTAL_MATERIALIZE_ENABLED:-true}
  CRYPTO_TRAIN_INCREMENTAL_WARMUP_HOURS: ${PRODUCTION_CRYPTO_TRAIN_INCREMENTAL_WARMUP_HOURS:-72}
  CRYPTO_TRAIN_INCREMENTAL_MAX_GAP_HOURS: ${PRODUCTION_CRYPTO_TRAIN_INCREMENTAL_MAX_GAP_HOURS:-168}
```

- [ ] **Step 2: Verify the trainer service inherits the env block**

Run: `grep -n "production-daemon-base\|production-app-base\|trainer_production" infra/docker-compose.yml | head`
Expected: `trainer_production` merges the production daemon base that carries this env block (the new vars apply without per-service duplication).

- [ ] **Step 3: Run the full unit suite before deploy**

Run: `pytest tests/unit/test_crypto_training_bulk_upsert.py tests/unit/test_crypto_incremental_materialize.py tests/unit/test_crypto_training_data.py tests/unit/test_crypto_feature_schema_v10.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add infra/docker-compose.yml
git commit -m "chore: wire incremental-materialize env for production trainer (F1/F3)"
```

- [ ] **Step 5: Rebuild + recreate ONLY the trainer (live daemons untouched)**

After the in-flight 7.5h run finishes (so the store is seeded at v10 — the warm cache the incremental path reads):
```bash
docker compose --env-file .env -f infra/docker-compose.yml build trainer_production
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps trainer_production
```

- [ ] **Step 6: Verify the warm incremental run reaches the GPU fit in minutes**

After the trainer's next nightly tick:
```bash
docker logs infra-trainer_production-1 2>&1 | grep -E "crypto_materialize incremental|crypto_materialize full_rebuild|model_nightly|champion"
nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader
```
Expected: a `crypto_materialize incremental` log line, materialize completes in minutes, GPU utilization > 0 during the fit.

---

## Task 7 (FAST-FOLLOW): F2 — asset-parallel full rebuild

Only the *cold/schema-bump/staleness* full rebuild still runs `_crypto_decision_rows` single-threaded. Parallelize it across the 4 dedicated cores with a process pool (the GIL makes the current `run_in_executor(None, ...)` thread useless for CPU parallelism).

**Files:**
- Modify: `src/kalshi_bot/crypto/services.py` (`_materialize_once` COMPUTE phase `:3294-3300`; `_crypto_decision_rows` `:9128` is already a module-level function → picklable for `ProcessPoolExecutor`)
- Modify: `src/kalshi_bot/config.py` (`crypto_train_build_workers: int = 1`)
- Test: `tests/unit/test_crypto_parallel_build.py` (create)

- [ ] **Step 1: Add config**

In `config.py` after the incremental knobs:
```python
    crypto_train_build_workers: int = 1  # 1 = today's single-thread path; >1 = ProcessPool over assets
```

- [ ] **Step 2: Write the failing parity test**

```python
# tests/unit/test_crypto_parallel_build.py
import pytest
from kalshi_bot.crypto.services import _crypto_decision_rows_parallel, _crypto_decision_rows

@pytest.mark.asyncio
async def test_parallel_build_matches_serial(tmp_path):
    # Implementer seeds in-memory snapshots/candles/spot for >=2 assets, then asserts
    # _crypto_decision_rows_parallel(..., workers=2) returns the SAME rows (by row_id/feature_hash)
    # as _crypto_decision_rows(...). Cross-asset spot must be broadcast to each worker.
    pytest.skip("fixture authored during implementation; asserts parallel == serial row set")
```

- [ ] **Step 3: Implement a parallel wrapper that partitions by asset and broadcasts spot**

```python
def _crypto_decision_rows_parallel(snapshots, candles, spot_rows, *, settings, workers, funding_rate_rows=None):
    """Asset-partitioned parallel build. Each worker receives ALL spot_rows
    (cross-asset features) but only its asset subset of snapshots/candles, then
    results are concatenated. workers<=1 delegates to the serial path."""
    assets = sorted({str(s.asset_symbol) for s in snapshots})
    if workers <= 1 or len(assets) <= 1:
        return _crypto_decision_rows(snapshots, candles, spot_rows, settings=settings, funding_rate_rows=funding_rate_rows)
    import concurrent.futures
    def _subset(rows, asset):
        return [r for r in rows if str(getattr(r, "asset_symbol", "")) == asset]
    out = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                _crypto_decision_rows,
                _subset(snapshots, a), _subset(candles, a), spot_rows,
                settings=settings, funding_rate_rows=funding_rate_rows,
            ): a for a in assets
        }
        for fut in concurrent.futures.as_completed(futs):
            out.extend(fut.result())
    return out
```

Then in `_materialize_once` COMPUTE (`:3296-3300`), replace the `run_in_executor` call so it routes through `_crypto_decision_rows_parallel(..., workers=self.settings.crypto_train_build_workers)` when `workers > 1` (still inside `run_in_executor` so the event loop is free).

**Caveats the implementer must verify:** (a) snapshot/candle/spot row objects must be picklable across processes — if they are ORM instances, convert to plain dicts/namedtuples before submitting; (b) cross-asset feature parity — broadcasting all spot_rows to each worker must reproduce serial output exactly (the test asserts this); (c) confine workers to the trainer cpuset (process pool inherits the container's cgroup cpuset automatically — verify live daemons stay healthy).

- [ ] **Step 4: Run the parity test**

Run: `pytest tests/unit/test_crypto_parallel_build.py -v`
Expected: PASS — parallel row set == serial row set.

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/config.py src/kalshi_bot/crypto/services.py tests/unit/test_crypto_parallel_build.py
git commit -m "perf: asset-parallel decision-row build for cold rebuild (F2)"
```

- [ ] **Step 6: Enable on the trainer only + verify isolation**

Set `PRODUCTION_CRYPTO_TRAIN_BUILD_WORKERS=4` for the trainer, add the env mapping (mirroring Task 6), recreate the trainer, and confirm during a full rebuild that the trainer uses ~4 cores while live daemons stay `healthy` and keep producing decisions.

---

## Self-Review

**Spec coverage:**
- F1 incremental materialize → Tasks 3 (watermark), 4 (config), 5 (effective_since + parity incl. late labels). ✓
- F1 schema-version guard → Task 5 Step 3 (watermark filters schema → None → full rebuild). ✓
- F1 bounded-staleness guard → Task 5 Step 3 (`gap > max_gap` → full rebuild). ✓
- F3 bulk write → Tasks 1 (repo method) + 2 (use in materialize). ✓
- F2 asset-parallel → Task 7 (fast-follow, config-gated `workers=1` default = today). ✓
- Deploy trainer-only, live daemons untouched → Tasks 6/7 Step "recreate ONLY trainer". ✓
- Bootstrap note (let 7.5h run seed v10 store) → Task 6 Step 5 precondition. ✓

**Placeholder scan:** Tasks 5 and 7 use `pytest.skip` placeholders for *fixture authoring* (snapshot/spot seeding is bulky and codebase-specific); the assertion shape and the implementation code are fully specified. Acceptable per the "author fixtures during implementation" note — but the implementer MUST replace the skip with a real failing test before implementing (TDD).

**Type consistency:** `bulk_upsert_crypto_training_feature_rows(rows: list[dict]) -> int` used identically in Tasks 1 and 2. `get_crypto_training_feature_watermark(*, frequency, kalshi_env, feature_schema_version) -> datetime | None` used identically in Tasks 3 and 5. Config names `crypto_train_incremental_materialize_enabled` / `_warmup_hours` / `_max_gap_hours` consistent across Tasks 4, 5, 6. ✓

**Out of scope (unchanged):** feature semantics / schema version, gate thresholds, trader guards, weather, sub-projects A & B (separate plans).
