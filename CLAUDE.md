# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation policy (MANDATORY)

**Every commit that changes behavior must update the documentation in the same commit.** Before you `git commit`, check whether the change affects any of: this `CLAUDE.md` (architecture, commands, config, safety/status notes), `README.md` (command list), `docs/crypto-trading-dials-and-knobs.md` / `docs/training-dials-and-knobs.md` (new/changed settings), the relevant `docs/operations/*` or `docs/research/*` (status, runbooks, findings). If it does, update those files and stage them together with the code. A new CLI command, config setting, model candidate, gate, or status change is never "done" until the docs reflect it. When a documented status becomes stale (e.g. a per-asset model status), correct it rather than leaving the old claim. If a commit genuinely needs no doc change, that is fine — but it should be a conscious check, not an omission.

## Commands

```bash
# Install (local dev)
python3 -m venv .venv && source .venv/bin/activate
python3 -m pip install -e ".[dev]"

# Run tests (all)
pytest

# Run a single test file
pytest tests/unit/test_risk_engine.py
pytest tests/integration/test_supervisor_workflow.py

# Run a single test by name
pytest -k "test_risk_blocks_oversized_order"

# Browser regression tests (requires Playwright)
python -m playwright install chromium
pytest tests/browser/

# Run migrations
alembic upgrade head

# Start the app locally
python3 -m kalshi_bot.main

# CLI entry point
kalshi-bot-cli <subcommand>   # see README for the full list

# Evaluate crypto trading (run inside a container / where the DB is reachable)
kalshi-bot-cli crypto-report --frequency 15m --days 7        # decision funnel (blocks→eligible→fills) + live champion per asset
kalshi-bot-cli crypto-pnl-report --days 14                   # fee-accurate FILL economics (gross/net/fees, by market)
kalshi-bot-cli crypto-maker-markout-report --days 14         # maker fill quality / adverse selection
kalshi-bot-cli crypto-vol-eval --frequency 15m              # light, training-free OOS eval of the analytic vol fair-value strategy vs mid (no GPU/tree fits)
kalshi-bot-cli crypto-mm run                                 # statistical market-making RESEARCH loop (NON-TRADING): data spine + fair value + backtest (runs as crypto_mm_production)
kalshi-bot-cli crypto-mm collect-once                        # one data-spine collection pass (debug)
kalshi-bot-cli crypto-mm eval-once                           # one vol fair-value OOS eval pass (debug)
```

No linter/formatter is configured in `pyproject.toml`. Tests use `pytest-asyncio` with `asyncio_mode = "auto"`. The global `conftest.py` sets `WEB_AUTH_ENABLED=false` as an autouse fixture, so integration tests skip HTTP basic-auth without extra setup.

Test tiers: `tests/unit/` is fast and runs on SQLite (no `pgvector`); `tests/integration/` exercises the supervisor, services, and `AppContainer` wiring; `tests/browser/` is Playwright-driven layout regression on the control room. Place new tests in the lowest tier that can exercise the behavior.

### Docker workflow
The compose file lives at `infra/docker-compose.yml`. Copy `.env.example` to `.env` before first run. Postgres runs as two separate services (`postgres_demo` / `postgres_production`); migrations are separate `migrate_demo` / `migrate_production` services. App and daemon containers follow the pattern `{app|daemon}_{demo|production}_{blue|green}`.

When force-recreating an app or daemon container, pass `--env-file .env` explicitly (e.g. `docker compose --env-file .env -f infra/docker-compose.yml up -d --force-recreate --no-deps app_production_blue`). Without it, the container boots with stale env from the original `up` invocation.

## Architecture

The platform is one async Python service (`src/kalshi_bot/`) with the following layers:

### Dependency injection via `AppContainer`
`services/container.py` constructs and wires every service at startup. Almost every service receives a `Settings` object, `async_sessionmaker`, and collaborating services through this container. When adding a new service, register it here.

### Core primitives (`core/`)
Shared types used across every layer:
- `enums.py` — `AgentRole`, `RoomStage`, `RiskStatus`, `StrategyCode`, etc.
- `schemas.py` — Pydantic models for inter-service payloads (`TradeTicket`, `RiskVerdictPayload`, `RoomMessageCreate`, …)
- `fixed_point.py` — Kalshi price/count quantization helpers (`quantize_price`, `quantize_count`, `make_client_order_id`)
- `metrics.py` — Prometheus counters (`ACTIVE_ROOMS`, `ORDERS_TOTAL`, `ROOM_RUNS_TOTAL`)
- `signal_payload.py` — capital-bucket derivation from signal payloads

### Agent room (`agents/`)
`room_agents.py` defines `AgentSuite` — eight roles that run in sequence inside each trading room:
1. **Researcher** — posts evidence-backed observation using dossier + weather signal + memories
2. **President** — advisory posture memo
3. **Trader** — emits a `TradeTicket` (structured) or stand-down message
4. **Risk officer** — explains deterministic verdict
5. **Execution clerk** — places order or records skip reason
6. **Auditor** — ties rationale chain
7. **Ops monitor** — operational health check
8. **Memory librarian** — distills room into semantic memory notes

Each role calls `providers.rewrite_with_metadata()` which routes to Gemini (primary) or an OpenAI-compatible local/hosted endpoint (fallback) via `agents/providers.py` (`ProviderRouter`). Role-specific models are configured per `gemini_model_*` settings.

### Orchestration (`orchestration/supervisor.py`)
`WorkflowSupervisor` runs the fixed 12-step workflow per room: trigger → market snapshot → weather bundle → deterministic signal → agent role sequence. LLM output is **never** used to sign requests or bypass risk rules.

### Deterministic engines (`services/`)
- `signal.py` — `WeatherSignalEngine`: fair-value estimation from NWS weather
- `risk.py` — `DeterministicRiskEngine`: enforces order size, position, and daily-loss limits; result is authoritative regardless of LLM opinion
- `execution.py` — `ExecutionService`: the only path that hits Kalshi write endpoints; requires active deployment color + cleared kill switch

### Risk sub-package (`risk/`)
Structured sub-models used by `DeterministicRiskEngine`:
- `hard_caps.py`, `parameter_pack.py` — position and daily-loss cap definitions
- `exit_score.py`, `survival.py`, `sizing.py`, `uncertainty.py` — probabilistic position-management helpers

### Forecast sub-package (`forecast/`)
Ensemble probability engine for signal generation:
- `probability_engine.py` / `ensemble_fuser.py` — combine multiple model outputs into a calibrated probability
- `online_calibrator.py` — recalibrates forecasts against recent settlements
- `learned_head.py` — XGBoost/LightGBM learned residual head
- `source_health.py` — tracks per-source reliability for fuser weighting

### Crypto subsystem (`crypto/`)
A self-contained parallel trading stack for crypto prediction markets, mirroring the weather pipeline:
- `services.py` — `CryptoWorkflowService`, `CryptoExecutionService`, `CryptoForecastService`, `CryptoHistoryService`, `CryptoReplayService`, `CryptoSpotService`, `CryptoAssetControlService`, `CryptoAutonomyService`, `CryptoMarketService`
- `models.py` / `parsing.py` — `CryptoMarket`, `CryptoSeries`, candlestick normalization
All crypto services are wired through `AppContainer` alongside weather services.

`CryptoExecutionService` is the only path to Kalshi crypto write endpoints (parallel to weather's `ExecutionService`); it shares the same kill-switch + deployment-color checks. `CryptoAutonomyService` runs as a continuous loop on the active-color daemon (no sleep between iterations on the active path; `CRYPTO_AUTONOMY_IDLE_INTERVAL_SECONDS` only applies when the service is disabled or this container is the inactive color). Crypto model regeneration runs nightly inside the daemon (`CRYPTO_MODEL_NIGHTLY_AUTO_ENABLED`, fires at `CRYPTO_MODEL_NIGHTLY_HOUR_LOCAL` in `CRYPTO_MODEL_NIGHTLY_TIMEZONE`) and only retrains when at least `CRYPTO_MODEL_NIGHTLY_MIN_NEW_STRICT_ROWS` new strict-as-of rows are available.

Active crypto assets (as of 2026-05-20): **BTC, ETH, SOL, XRP, BNB, DOGE, HYPE**. ADA and BCH were removed from all active asset lists (`crypto_model_nightly_assets`, the CLI live-path default, passive-bid, overnight-readiness prefixes) because they have no backfilled spot history yet — configured-but-untrainable. Their inert lookup/parsing tables (Coinbase/CoinGecko product-id maps, ticker-recognition lists) are intentionally kept, so re-adding them after a backfill is a config-only change.

**2026-06-19 → SUPERSEDED 2026-06-20 — "SIGMA ONLY" live-trading scope.** (Original directive: only `vol_normal_fair_value` could deploy live via `CRYPTO_LIVE_CHAMPION_ALLOWLIST=vol_normal_fair_value`.) **Reversed 2026-06-20** (operator: "use whatever strategy you can model, sigma or anything else") because a full sigma-only sweep deployed **0/7** — and the diagnosis showed sigma is a dead end in-pipeline: the trainer-pipeline `vol_normal_fair_value` collapses to Brier ≈0.25 on **every** asset (the offline spine-based sigma that beats mid on BTC/ETH/HYPE is a *different* code path that never reaches the trainer/live pipeline). Worse, the **brier-vs-mid deploy ceiling** (`crypto_model_max_brier_regression_vs_mid`, default **0.07**) was vetoing **every profitable candidate, ML included** — at 15m the market mid is a strong forecast and every model that earns positive shrinkage-adjusted sim P&L is 12–27% worse than mid on Brier (XRP sklearn +12%, BNB +14%, DOGE +15%, ETH +20%, SOL +27%). Two operator config changes (`.env`, fallbacks in compose):
- `CRYPTO_LIVE_CHAMPION_ALLOWLIST=` (empty → no restriction; best gate-passing model wins per asset). In-pipeline sigma still never deploys (brier-broken), so this just unblocks ML.
- `CRYPTO_MODEL_MAX_BRIER_REGRESSION_VS_MID` (was 0.07 → **0.16** → **0.30** 2026-06-20 "get the rest of the market live ready"): at **0.30** ALL profitable ML candidates compete (XRP +12% / BNB +14% / DOGE +15% / ETH +20% / SOL +27%) and the **replay gate** (independent OOS profit+coverage) is the final arbiter; still excludes the broken in-pipeline sigma (+78–90%). (0.16 had admitted only the ≤15% cluster.) **The replay gate + decision-time fee/edge gates remain** as the downstream safety net. Neither field is tuner-managed (`crypto_model_max_brier_regression_vs_mid` is not in `TUNABLE_GATE_FIELDS`), so setting them via `.env` is allowed. **Revert** = set allowlist back to `vol_normal_fair_value` and ceiling back to `0.07`. Takes effect as the trainer re-selects each asset; verify champions flip away from `market_mid_baseline` via `crypto_model_artifacts`. **Hard limit:** BTC + HYPE have NO profitable candidate (best is baseline) — no ceiling deploys them; they need real edge. The per-asset `model_type` statuses below predate all of this.

All 7 assets are *configured* on the **CRYPTO_15M model path**. The champion set is re-selected continuously by the trainer loop, so verify live `model_type` per asset via `crypto_model_artifacts` before asserting. As of the **2026-06-17 artifacts**: **SOL (`sklearn_logistic`), BNB (`lightgbm_classifier`), DOGE (`sklearn_logistic`) run a trained model AND passed the replay gate** (live-eligible); **XRP, HYPE, ETH select a trained candidate but the replay gate is BLOCKED → they run in shadow** (`live_eligible=False`, "Asset X is shadow because the replay gate has not passed" — mode/gate are separate controls, gate blocks live orders on top of mode); **BTC = `model_type=market_mid_baseline`** (echoes the mid, ≈zero edge → stands down). This is the model-selection safety gate working as designed: a trained candidate only goes live if it shows positive OOS net simulated P&L *and* beats market-mid (`_crypto_select_champion` / `_crypto_candidate_is_profit_deployable` in `crypto/services.py`); BTC's candidate didn't clear it. Earlier "only HYPE+DOGE trained (2026-06-13)" and "all 7 trained (2026-06-05)" statuses are stale. Diagnosis + forward plan: `docs/research/2026-06-14-model-selection-diagnosis-and-plan.md`.

**2026-06-17 updates (sim/live parity + new candidate + reporting):**
- **Sim/live edge-shrinkage parity (commit `bd9b0f5`):** champion selection now applies the live edge-shrinkage fit (β floored at `crypto_edge_shrinkage_beta_floor`, raw ~0.125 — realized live edge ≈12.5% of predicted) inside the trainer candidate simulation, gated by `crypto_model_selection_apply_edge_shrinkage` (default true). Before this, selection optimized an edge ~5× larger than what reaches the book and promoted models that traded $0 live (BTC 15m lightgbm showed "+$1.99/11 trades" in sim but placed **0 live fills** — all decisions blocked at the $0.45 entry cap / fee-edge floor). Expect most assets to honestly select `market_mid_baseline` until a candidate has edge that survives the brake. The brier-vs-mid deploy ceiling (`crypto_model_max_brier_regression_vs_mid`) is the companion guard.
- **Analytic vol fair-value candidate `vol_normal_fair_value` (commit `5c59782`):** mechanism-based `Φ(ln(S/K)/(σ√τ))` from existing spot features + isotonic calibration; competes in the champion pool against the curve-fit heads (from `docs/research/kalshi_15m_market_making_plan.md` §4.2).
- **Trading evaluation:** use `kalshi-bot-cli crypto-report` (decision funnel + live champion) alongside `crypto-pnl-report` (fee-accurate fill economics). **Always compute fill economics from the `fills` table, not `crypto_decision_outcomes` — `fill_count` stays 0 there even when real fills exist (attribution gap), so the decision-funnel reports UNDERSTATE fills.** As of 2026-06-17, model BUY entries have collapsed to ~0/day (06-09=166 → 06-17=0 executed buys) since the 06-12 gate tightening (`CRYPTO_MAX_ENTRY_PRICE_DOLLARS=0.45`, `RISK_MIN_EDGE_BPS=750`, `RISK_MAX_CREDIBLE_EDGE_BPS=1500`, enforced edge-shrinkage). Two blocking layers, neither bankroll: (1) ~99.5% die at `blocked_fee_edge`/`blocked_max_entry_price`/`blocked_shrunk_edge`; (2) the few eligible place a passive bid that isn't hit and the taker cross is refused by `crypto_max_fee_to_edge_ratio` → order status `passive_unfilled_taker_blocked`. Sizing works (avg 1.49 contracts when entries fire). The squeeze: realized edge (≈0.2×predicted, predicted capped 1500 bps → ≤300 bps) can't clear the ~347 bps fee-ratio requirement at p≈0.45 — only at p≲0.305 or if β≥~0.233. Unlock = more real edge (sigma/calibration), not loosening gates. Touch20 (`btc15m_touch20`, `1h_touch20`) is a separate, fully disabled strategy — `RULES_ENABLED=false`, `RULES_TRADING_ENABLED=false`, containers disabled. Do not conflate touch20 with the model path. **1h training:** the continuous trainer loop (`_continuous_crypto_train_loop`) now uses an **asset-major interleave** (each asset's 15m then 1h, via `crypto/train_loop.py`) plus a **DB-persisted resume cursor** (`deployment_control.notes.crypto_continuous_train_cursor`) so a restart picks up at the next (asset, frequency) item instead of restarting the 15m pass — fixing the prior starvation where 1h was never reached and the first 1h `full_cold_cache` materialize never survived to completion (1h artifacts had been stale since 06-09). **2026-06-19 durability follow-up:** the cursor was necessary but insufficient — the long pole is the per-asset materialize (a stale per-asset watermark makes each "incremental" pass an ~11-day rebuild; >2h) which committed in ONE upsert at the end, so a kill (restarts were mostly redeploy/session churn; trainer runs under uvicorn → SIGTERM = exit-0 graceful) lost the whole pass. Two fixes: (A1) **chunked/resumable materialize** — `crypto_train_materialize_max_step_hours` (>0) runs the window as successive bounded passes (oldest first, `materialize_window_bounds` in `crypto/train_loop.py`; `_materialize_stepped`→`_materialize_once(window_end=…)` with `until=` bounds on the 4 reads), each committing so the watermark advances per chunk and a kill only loses one chunk; byte-for-byte parity with a single pass when warmup≥lookback (`tests/unit/test_crypto_chunked_materialize.py`). **ENABLED on the trainer 2026-06-19** with `CRYPTO_TRAIN_MATERIALIZE_MAX_STEP_HOURS=48`, paired with a **warmup cut from 264h→72h and label-refresh 240h→24h** (the old values re-read+re-committed ~10 days every pass — a near-full rebuild that was the >2h long pole; 72h is ample recency context for 15m/1h and 24h catches same-day settlements, so steady-state passes are now cheap and the initial catch-up chunks survive interruption). (A2) the model fit (`_fit_crypto_calibration`) now runs in `run_in_executor` so it no longer blocks the event loop during the multi-minute fit. **2026-06-19 — 1h TRAINING TEMPORARILY DISABLED (`CRYPTO_CONTINUOUS_TRAIN_FREQUENCIES=15m`).** Unblocking materialize (warmup cut + chunking) let the loop finally reach the **1h candidate report**, which fits ALL models ×4 walk-forward folds on the large 1h sample set (+GPU xgboost) and **OOMs the 32g cgroup** — the trainer hung at 99.5% mem, went unhealthy, and wedged the whole loop at BTC 1h (no 15m re-selection could proceed behind it). 15m trains fine (~17g peak). So the trainer runs **15m-only** until the 1h-fit memory blowup is fixed (smaller 1h sample cap / fewer folds for 1h / chunked fit); restore `15m,1h` after. Live 1h *trading* was already disabled/shadow regardless. Promotion process and per-asset gate/model artifact history: `docs/operations/crypto-live-asset-promotion.md`. **2026-06-30 — crypto_1h DAEMON leak containment (distinct from the trainer 1h-fit OOM above):** the shadow `daemon_production_crypto_1h_{blue,green}` containers leaked from their ~0.5GB normal footprint to **35GB RSS over ~15h**, thrashing the host into swap (load avg 109) and wedging the trainer (its async loop timed out mid XRP fit). Root cause: these daemons inherited the `*oom-protect` `oom_score_adj: -900` from `production-daemon-base` but **no `mem_limit`** (unlike trainer 32g / crypto_mm 8g) — so an uncapped leak was actively *defended* by the kernel while it reaped healthier containers. Fix: **`mem_limit: 8g` on both crypto_1h daemons** (compose), converting a host-wide thrash into a contained cgroup self-restart of the expendable shadow daemon. The underlying 1h-path leak itself is still an open follow-up; the cap is the blast-radius guardrail. The live 15m daemon/app stay intentionally uncapped (no leak, protected by -900).

### Market-making research stack (`mm/`)
A self-contained, **NON-TRADING** research subsystem implementing `docs/research/kalshi_15m_market_making_plan.md` as a continuous loop, isolated in its own container (`crypto_mm_production`, CPU-only, `mem_limit 8g`, `oom_score_adj 500`). It **never places orders** — execution stays in `ExecutionService`; the container is triple-guarded (`CRYPTO_TRADING_ENABLED=false`, shadow on, kill switch on).
- `data_spine.py` — multi-venue spot consolidation (volume/recency weighted, staleness + outlier guards), market-tick normalization anchored to `floor_strike`.
- `fair_value.py` — analytic `Φ(ln(S/K)/(σ√τ))` + a configurable σ̂ estimator: `ewma_vol` (default, RiskMetrics-style stable σ) or `realized_vol` (equal-weight), via `mm_vol_estimator`/`mm_vol_window`/`mm_vol_ewma_lambda`. **σ is the lever** — `crypto-vol-eval` showed the analytic edge stands or falls on it; iterate σ here. The spine logs both σ̂'s per tick for offline OOS comparison.
- `backtest.py` — maker entry rule (avoid the 45–55¢ band) + realistic-fill / settlement P&L (no maker rebate).
- `storage.py` — append-only per-day JSONL on the `mm_data` volume (+ optional Parquet compaction).
- `loop.py` / `service.py` — the continuous loop (log every tick; OOS-evaluate every `MM_EVAL_INTERVAL_SECONDS`), assembled with real collaborators. Run via `crypto-mm run`.
Staged per plan §6: the data spine is v1 (reuses already-collected spot, so it's verifiable without new WS infra); live multi-venue WS book reconstruction, the §4.3 order-flow gate, and live execution are deliberate later stages. Config: `mm_*` settings in `config.py`.

### Persistence (`db/`)
Postgres + SQLAlchemy async + `pgvector` for semantic memory embeddings. In tests, SQLite is used via a JSON-compatible type wrapper (no pgvector). Alembic migrations live in `alembic/`.

`PlatformRepository` (in `db/repositories.py`) is the single repository surface passed to services. It is assembled from four mixin classes — `DeploymentControlRepositoryMixin`, `LearningRepositoryMixin`, `StrategyRepositoryMixin`, `WebAuthRepositoryMixin` — each in their own file under `db/`. Add new query methods to the appropriate mixin, not directly to `PlatformRepository`. The mixin chain order in `repositories.py` matters for MRO when mixins define overlapping helpers — keep the existing order unless you have a specific reason to change it, and add a test that exercises both methods if you do.

### Integrations (`integrations/`)
- `kalshi.py` — REST (RSA-signed) + WebSocket client
- `weather.py` — NWS/NOAA ingestion
- `forecast_archive.py` — Open-Meteo historical weather recovery
- `crypto_spot.py` — Coinbase OHLC/current spot feeds; proxy fallback is opt-in only

### Learning sub-package (`learning/`)
Drift watcher and parameter-search utilities used by the self-improve and strategy-evolution pipelines.

### Repo memory: deterministic autonomous gate tuning
LLM calls are hard-disabled by default (`LLM_CALLS_ENABLED=false`), and the built-in runtime pack is `builtin-deterministic-v1` with provider `none` roles. Gate thresholds learned from backtests/modeling are runtime data, not code rewrites. `AutonomousGateTuningService` runs after settlement reconciliation and on the active-color periodic heartbeat, builds a gate-learning recommendation from historical + forward-shadow bundles, validates the candidate through bundle-backed backtesting/modeling, stages threshold changes in `AgentPackThresholds`, and promotes only after newly labeled live decision-corpus rows pass the canary. Do not update `.env` or `config.py` at runtime for these thresholds; code defaults remain fallback values only. Legacy Strategy Codex, Strategy Auto-Evolve, Self-Improve, and strategy-eval paths must not mutate tunable gate thresholds; `autonomous_gate_tuning` is the sole threshold authority.

**Crypto gate tuning is PER-ASSET (2026-06-23).** The trainer writes per-asset model/replay-gate artifacts only, so a pooled (all-asset) crypto tuning run can never pass the artifact-check (`model_status=missing` because no pooled `model` artifact exists; the pooled `replay_gate` is `blocked` whenever any one asset fails) — which is why crypto tuning historically sat at `not_started`. `_run_crypto_assets` now fans the crypto domain out **per asset**, each evaluated against its own `model:{ASSET}` / `replay_gate:{ASSET}` artifacts and staged under a per-asset checkpoint (`_checkpoint_name(env, domain="crypto:{ASSET}")` → `autonomous_gate_tuning:crypto:{env}:{ASSET}`). Staging is **serialized**: the agent pack holds one candidate at a time, so once any asset is staged (this pass or a prior one) the dispatcher only advances that asset's canary and skips building new candidates for the rest until it resolves (`reason=another_crypto_candidate_in_flight`). `run(domain="crypto"|"all")` returns the per-asset map under `result["assets"]` with a rolled-up `status`; `autonomous-gates status` surfaces `crypto.assets`. NOTE: even per-asset, tuning only promotes when a model has fee-clearing edge — today every asset returns `no_candidate` (`selected_count=0`), so this is plumbing that activates when the multi-venue settlement-basis edge lands, not a profitability change by itself.

### Control room (`web/`)
FastAPI app with server-rendered Jinja2 templates, SSE transcript stream, and REST endpoints. The top-level summary strip (`/api/control-room/summary`) is designed to be fast — it avoids live market discovery and uses lightweight room snapshots. The `Research` view also exposes an 180d-only assignment review queue (`ready_for_approval`, `drifted_assignment`, `evidence_weakened`, `aligned`, `waiting_for_evidence`), and city detail includes the latest approval note plus next-action copy. The operator win-rate card uses `PlatformRepository.get_fill_win_rate_30d()` and treats wins as realized-P&L-positive exits first, falling back to settlement results only when no sell fill exists for that ticker and side.

### Blue/green deployment
A DB-backed single-writer lock enforces that only the active color (`app_color` setting) can acquire the execution lock. The kill switch (`app_enable_kill_switch`) clears the execution lock and blocks new live orders. Self-improve staging is checkpoint-based: promotions write `pending_pack_promotion:{kalshi_env}:{color}`, and the target color's daemon applies it at startup so watchdog restarts or failovers do not strand an old pack assignment. Canary state has a max lifetime and becomes `stalled` after `SELF_IMPROVE_CANARY_MAX_SECONDS`.

The active color lives in the `deployment_control` DB row, set via `kalshi-bot-cli promote <blue|green>` (a pure metadata update). For **zero-downtime redeploys**, recreate only the inactive color, wait for health, then `promote` to hand off the lock — use `scripts/blue_green_redeploy.sh` and `docs/operations/blue-green-redeploy.md`. A plain `docker compose up -d --force-recreate <all colors>` bounces the active color too (brief trading gap + ~74s daemon warmup) and is only for an intentional full bounce.

### Historical data layers (four separate concerns)
1. `source_replay_coverage` — strict-as-of replay sources
2. `checkpoint_archive_coverage` — canonical checkpoint-weather records
3. `external_archive_coverage` — Open-Meteo-assisted recovery
4. `replay_corpus` — materialized `historical_replay` rooms

## Key configuration
`config.py` (`Settings`) reads from `.env` (copy from `.env.example`). Key env vars:
- `KALSHI_ENV` — `demo` or `live`
- `LIVE_KALSHI_API_KEY` / `DEMO_KALSHI_API_KEY` — API key IDs
- `LIVE_KALSHI_READ_PRIVATE_KEY_PATH` / `DEMO_*` — RSA PEM paths
- `LLM_CALLS_ENABLED=false` — disables Gemini, OpenAI, Codex, and local OpenAI-compatible LLM paths
- `GEMINI_KEY` or `GEMINI_API_KEY` — optional LLM provider only when `LLM_CALLS_ENABLED=true`
- `APP_SHADOW_MODE=true` — prevents live order submission (default on)
- `APP_COLOR` — `blue` or `green` for blue/green deployment
- `SELF_IMPROVE_CANARY_MAX_SECONDS` — max staged-canary lifetime before status becomes `stalled`
- `AUTONOMOUS_GATE_TUNING_ENABLED=true` — after settlement reconciliation, lets backtesting/modeling stage and canary agent-pack threshold updates
- `WEATHER_MARKET_MAP_PATH` — path to market config YAML (default: `docs/examples/weather_markets.example.yaml`); the YAML uses `series_templates` so the app auto-discovers current daily temperature contracts per configured city
- `RISK_MIN_EDGE_BPS` — minimum required edge in basis points for any order to clear `DeterministicRiskEngine` (currently `500`; bumped from earlier defaults)
- `CRYPTO_AUTONOMY_ENABLED` + `CRYPTO_TRADING_ENABLED` — both must be true for live crypto trading; `CRYPTO_AUTONOMY_ENABLED` alone only runs the decision loop in shadow

## Safety rules
- The app starts in shadow mode (`APP_SHADOW_MODE=true`) and with the kill switch enabled by default. Do not disable either until mappings, reconciliation, and restart recovery are validated.
- LLM responses are inputs to human-readable transcripts only. Deterministic engines are authoritative for all trading decisions.
- Kalshi write endpoints are only reachable through `ExecutionService`, which checks the kill switch and deployment lock before every order.
- **Exit management (stop-loss + take-profit) is DISABLED as of 2026-06-18** (`PRODUCTION_STOP_LOSS_ENABLED=false`, `PRODUCTION_CRYPTO_TAKE_PROFIT_ENABLED=false`; both daemon colors recreated). Reason: the operator trades manually in the same Kalshi account and does not want the bot stop-lossing/taking-profit on manual positions. The system cannot distinguish bot from manual trades in a single account — `reconcile.py` ingests the whole account (`get_positions`/`get_orders`/`get_fills`) and attributes every crypto buy's `strategy_code` by ticker pattern (`_crypto_strategy_code_for_ticker`), so the exit managers (`stop_loss.py`, `crypto_take_profit.py`) iterate ALL account positions. Do NOT re-enable the flags without first adding a bot-origin filter (only manage positions with a bot-generated `client_order_id`) or moving manual trading to a separate subaccount — otherwise it manages manual trades again. Low cost while disabled: bot entries are ~0/day.
- **Weather: SHADOW DATA COLLECTION re-enabled 2026-06-22 (operator "proceed weather"); still NO live weather trading.** Was fully disabled 2026-05-20 (crypto-only); operator authorized the weather rework → shadow → gate → live path. Now: `WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS=300` (re-enables the daemon market-history snapshot loop + research refresh → collects intraday KXHIGH quotes — the substrate for the market-edge gate). **Order safety:** `*_TRIGGER_ENABLE_AUTO_ROOMS` stays **false**, which is the ONLY thing preventing live weather orders (production is NOT in shadow — `PRODUCTION_APP_SHADOW_MODE=false`, kill switch off — so crypto trades live; weather has no order path solely because auto-rooms are off and the market-history loop's one trading call is gated behind `_auto_trigger_enabled_for_run`). DO NOT enable `*_TRIGGER_ENABLE_AUTO_ROOMS` for weather until the market-edge gate passes (see `docs/research/2026-06-22-weather-strategy-rework.md`: oriented model validated forecast-quality, Brier 0.075 vs 0.139 baseline, but market-edge-after-fees unproven). Still off: `*_WEATHER_PREDICTION_ENABLED`, intraday/residual models, `WEATHER_REJECTED_OPPORTUNITY_SCORER_ENABLED`. See `docs/operations/weather-disabled-2026-05-20.md` (kill-set + restore).
