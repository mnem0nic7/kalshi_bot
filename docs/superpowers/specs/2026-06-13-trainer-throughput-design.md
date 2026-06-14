# Dedicated Trainer Throughput: Foundation → Cadence → Breadth

**Date:** 2026-06-13
**Status:** Approved (decomposition + Foundation approach). Foundation is the active sub-project; A and B are sequenced roadmap.
**Context:** A dedicated `trainer_production` container (cores 4-7,12-15 + GPU, training-node mode, triple-guarded against trading) now owns all model training, off the live trading daemons. With training isolated, the operator asked to "speed training, have it run continuously."

## Problem

The trainer is **data-bound, not compute-starved**, but a *single* pooled-15m run is pathologically slow: the in-flight first run has pegged **one core at ~100% for 7.5h** with the **GPU at 0%** (never reached the fit phase). Root cause: the 60s spot densification multiplied `spot_rows` ~15× (900s→60s granularity), and the corpus build is single-threaded.

### Where the time actually goes (verified against the running trainer)

The nightly's pooled path (`daemon.py:_run_crypto_model_nightly_pooled_frequency`, `:2090`) runs **preflight-then-feature-store**, and that path is **active** on the trainer (`CRYPTO_TRAINING_PREFLIGHT_ENABLED=true`, backfill service wired):

1. `preflight = CryptoTrainingBackfillService.prepare(run_source_backfill=True)` (`crypto/services.py:3098`) → source backfills → **`materialize()` → `_materialize_once()` (`:3231`)**.
2. `crypto_forecast_service.train(use_feature_store=True, feature_store_only=True)` (`daemon.py:2159-2163`) — reads the materialized rows back from `crypto_training_feature_row` (`crypto/services.py:3663-3677`). **This step is already fast** (no rebuild).

So the 7.5h is spent **inside `_materialize_once`**, which always rebuilds the **full `crypto_train_lookback_days` (180d) window** (`since = now - lookback_days`, `:3242`). Two single-threaded chokepoints there:

1. **COMPUTE** (`:3294-3300`) — `_crypto_decision_rows(snapshots, candles, spot_rows, ...)` runs in one `run_in_executor` thread (one core; comment literally says "multi-hour CPU pass"). The fit phase (`:10750`, XGBoost `n_jobs=-1` + GPU) is already parallel but is reached only after materialize finishes — hence GPU at 0%.
2. **WRITE** (`:3315-3336`) — one `await repo.upsert_crypto_training_feature_row(...)` *per row*, sequentially.

**The feature-store/cache infrastructure already exists** (`crypto_training_feature_row` + the `use_feature_store` read path). What's missing is making **materialize incremental** so it doesn't rebuild 180 days every run.

The operator's goals ("continuous", "all breadth") are **physically blocked** until one run is fast:
- Cannot retrain every ~2-3h (cadence) when a run takes 7h.
- Cannot add 1h-pooled + calibration + per-asset models (breadth) on top of a 7h base.

## Why this is the binding constraint, not retrain frequency

A trained model is a deterministic function of its corpus; new labeled rows arrive only as 15m markets settle (~28 settled rows/hr across 7 assets). Retraining faster than data arrives reproduces near-identical models. So "continuous" delivers value only up to data-arrival cadence (~every 2-3h), and even that is impossible until a run is minutes, not hours. **Speed first, then cadence-matched, then breadth.**

## Decomposition (strict dependency order)

| # | Sub-project | Depends on | Rationale |
|---|---|---|---|
| **F** | Make one run fast | — | At >7.5h/run, A and B are impossible. **Active.** |
| **A** | Matched cadence (retrain ~every 2-3h on a new-rows gate) | F | Can't retrain every 2-3h if a run takes 7h. Roadmap. |
| **B** | Breadth: 1h-pooled + calibration/source-health + per-asset 15m | F (+A's loop) | Each adds *more* runs per cycle. Roadmap. |

Each sub-project gets its own implementation plan. This spec fully designs **F** and records **A**/**B** as committed roadmap so sequencing/constraints are not lost.

---

## Sub-project F — Make one run fast (ACTIVE)

Three composable levers. Ship **F1 + F3** first (make the common case minutes); **F2** is a fast-follow for the cold/schema-bump full rebuild.

### F1 — Incremental materialize (primary lever)

Make `_materialize_once` compute only the **new tail** instead of the full 180d window. The rows are **already persisted** to `crypto_training_feature_row` (`:3318`), tagged with `feature_schema_version` and `decision_time`, and `train(use_feature_store=True)` already reads them back — so F1 only needs to change *materialize*, not *train*.

- **Watermark:** max `decision_time` already persisted for `(kalshi_env, frequency)` **at the current `CRYPTO_RICH_FEATURE_SCHEMA_VERSION`**.
- **Windowed READ with warmup overlap (correctness-critical):** `_crypto_decision_rows` computes features with look-back (rolling windows, cross-asset returns, settlement windows). To compute tail rows *identically* to a full rebuild, the READ phase must load raw snapshots/candles/spot from `watermark - warmup_window` (not just `watermark`), COMPUTE decision rows over that, then **keep only rows with `decision_ts > watermark`** for upsert. `warmup_window` must be ≥ the longest feature look-back; pick a safe margin (e.g. a few hours/days) and verify via the parity test below.
- **Schema-version guard:** if no rows exist at the current schema version (v9→v10 bump, or cold cache), fall back to a **full rebuild** — different-schema rows must never be mixed.
- **Bounded-staleness guard:** if the watermark is older than a configurable cap, full rebuild rather than computing a huge tail.
- `train(use_feature_store=True)` then reads the lookback window from the store (cached older rows + freshly upserted tail) — unchanged.

New config: `crypto_train_incremental_materialize_enabled: bool = True`, `crypto_train_incremental_warmup_hours: int`, `crypto_train_incremental_max_gap_hours: int` (full-rebuild trigger). `enabled=false` preserves today's full-rebuild path exactly.

With matched cadence (A) this rebuilds ~2-3h of rows per cycle instead of 180 days — the single biggest win, and the reason A becomes feasible.

### F3 — Bulk training-row write

Replace the per-row `await repo.upsert_crypto_training_feature_row(...)` loop (`:3315-3336`) with a single batched/`executemany` upsert (chunked, e.g. 1-5k rows). Add a `bulk_upsert_crypto_training_feature_rows(...)` method to the appropriate repository mixin (`StrategyRepositoryMixin` / wherever the single-row upsert lives); keep the single-row method for other callers. With incremental builds the write set is small, but bulk write also slashes the cold full-rebuild write time.

### F2 — Asset-parallel full rebuild (fast-follow)

For the rare full rebuild (schema bump / cold cache / staleness fallback), parallelize the COMPUTE phase across the 4 dedicated cores with a **`ProcessPoolExecutor`** (the current `run_in_executor(None, ...)` thread can't bypass the GIL). `_crypto_decision_rows` partitions naturally by asset; cross-asset spot features must be **broadcast** to each worker (each worker gets all spot rows but computes decision rows for its asset subset), then results concatenated. Gated by a config (`crypto_train_build_workers: int`, default = min(cores, n_assets)); workers=1 preserves today's path exactly.

### F — Out of scope

- No change to feature semantics / `_crypto_raw_feature_vector` / schema version (outputs must be byte-identical; existing schema tests must pass unchanged).
- No vectorization rewrite of `_crypto_decision_rows` (a possible future deeper optimization; not needed once F1 makes the hot path incremental).
- Trainer must never trade (already triple-guarded; unchanged).

### F — Verification

1. **Parity (correctness-critical):** for a fixed dataset, incremental materialize (windowed read with warmup overlap → keep `decision_ts > watermark`) produces **byte-identical persisted rows** (same `row_id`/`feature_hash` set) as a full rebuild for the overlapping window. Add a test asserting incremental == full on a small fixture, exercising a row whose features look back across the watermark (proves `warmup_window` is sufficient).
2. **Schema-bump safety:** with a simulated schema-version change, F1 falls back to full rebuild (no mixed-version rows). Unit test.
3. **Speed:** measure wall-time of (a) cold full rebuild with F2 vs the current single-thread baseline (the in-flight 7.5h run gives the baseline number), (b) a warm incremental run. Target: warm incremental run completes in minutes and **reaches the GPU fit phase** (GPU util > 0).
4. **Isolation preserved:** during F2 multiprocessing, live daemons stay healthy and responsive (process pool confined to the trainer's cpuset).
5. `pytest tests/unit/test_crypto_feature_schema_v10.py` and existing crypto training tests pass unchanged.

### F — Bootstrap note

The in-flight 7.5h run is **not wasted**: it populates `crypto_training_feature_row` for the full 180d window at schema v10, which is exactly the cache F1 reads from. Let it finish to (a) get the cold-rebuild baseline number and (b) seed the cache. First post-F1 run will be a warm incremental.

---

## Sub-project A — Matched cadence (ROADMAP)

Retrain when fresh data warrants, not once per calendar day.

- Replace the **per-local-date idempotency** in `_maybe_run_crypto_model_nightly` (`:1769-1810`, the `local_ran.date() == night_state["local_date"]` short-circuit) with a **min-interval + new-rows gate**, *for the training node only* (live daemons keep the nightly fully disabled — `CRYPTO_MODEL_NIGHTLY_AUTO_ENABLED=false`).
- Trigger: retrain when `(now - last_run) ≥ crypto_train_min_interval_hours` (e.g. 2-3h) **AND** the existing refresh-gate fires (`_crypto_model_refresh_reason`, `≥ crypto_model_nightly_min_new_strict_rows` new strict rows). Reuse the existing gate logic; the loop already ticks every ≥300s.
- Keep the `_crypto_model_nightly_lock` (no overlapping runs) and the per-color checkpoint mechanism (repurposed to store `last_run` rather than `local_date`).

### A — Key correctness constraint: decouple model regen from gate-threshold derivation

Every model regen currently re-derives the bucket_matrix/gates that feed `AutonomousGateTuningService`'s canary. Retraining every 2-3h would **thrash the canary**, and `autonomous_gate_tuning` is the **sole threshold authority** (we never hand-tune thresholds). Therefore: the cadence design must let the **model artifact** regenerate frequently while **gate-threshold derivation runs on its own slower cadence** (unchanged from today). Concretely — separate "regenerate model artifact" from "re-derive/stage gate thresholds" so frequent retrains do not feed the canary more often than today. Exact mechanism to be settled in A's own design/plan.

---

## Sub-project B — Breadth (ROADMAP; operator chose all three)

Use the now-fast, now-frequent trainer to build everything of value each cycle:

- **B1 — 1h pooled every cycle (toward promotion).** Drop the 15m/1h date-ordinal *alternation* (`_crypto_model_nightly_rotation`, `:1859-1884`) for the training node — alternation existed to fit one model/night on the shared daemon; a dedicated fast box can do both. Accumulates the OOS track record CLAUDE.md notes is missing for 1h promotion. (1h stays **disabled on the live path** until promoted — this only *builds* it.)
- **B2 — Calibration + source-health refresh each cycle (live-relevant today).** Run `online_calibrator` + `source_health` refresh in the trainer loop; these feed the live 15m path now.
- **B3 — Per-asset 15m models alongside pooled.** Train each asset separately and persist as distinct artifacts. **Explicitly "build now, wire later":** per-asset models are **not consumed by the live path** until a follow-up adds A/B selection vs the pooled model. B3 produces the artifacts and the comparison data; the consumption wiring is a separate, later change.

---

## Rollout / safety (all sub-projects)

- Trainer **never** trades (triple-guarded: `CRYPTO_AUTONOMY_ENABLED=false`, `CRYPTO_TRADING_ENABLED=false`, `APP_SHADOW_MODE=true`, kill switch on). Unchanged.
- **Do not** touch gate thresholds or `.env`/`config.py` threshold defaults at runtime; `autonomous_gate_tuning` remains the sole authority (see A's constraint).
- Weather stays fully disabled.
- Deploy via image rebuild + recreate of `trainer_production` only (`docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps trainer_production`); live daemons untouched. Commits land direct on `main`.
- Phase-2 live-daemon recreate (nightly-off / GPU-removed / cpuset 0-3,8-11) proceeds independently once the trainer proves out — tracked separately, not part of this throughput work.
