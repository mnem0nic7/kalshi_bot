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

All 7 assets are *configured* on the **CRYPTO_15M model path**. The champion set is re-selected continuously by the trainer loop, so verify live `model_type` per asset via `crypto_model_artifacts` before asserting. As of the **2026-06-17 artifacts**: **SOL (`sklearn_logistic`), BNB (`lightgbm_classifier`), DOGE (`sklearn_logistic`) run a trained model AND passed the replay gate** (live-eligible); **XRP, HYPE, ETH select a trained candidate but the replay gate is BLOCKED → they run in shadow** (`live_eligible=False`, "Asset X is shadow because the replay gate has not passed" — mode/gate are separate controls, gate blocks live orders on top of mode); **BTC = `model_type=market_mid_baseline`** (echoes the mid, ≈zero edge → stands down). This is the model-selection safety gate working as designed: a trained candidate only goes live if it shows positive OOS net simulated P&L *and* beats market-mid (`_crypto_select_champion` / `_crypto_candidate_is_profit_deployable` in `crypto/services.py`); BTC's candidate didn't clear it. Earlier "only HYPE+DOGE trained (2026-06-13)" and "all 7 trained (2026-06-05)" statuses are stale. Diagnosis + forward plan: `docs/research/2026-06-14-model-selection-diagnosis-and-plan.md`.

**2026-06-17 updates (sim/live parity + new candidate + reporting):**
- **Sim/live edge-shrinkage parity (commit `bd9b0f5`):** champion selection now applies the live edge-shrinkage fit (β floored at `crypto_edge_shrinkage_beta_floor`, raw ~0.125 — realized live edge ≈12.5% of predicted) inside the trainer candidate simulation, gated by `crypto_model_selection_apply_edge_shrinkage` (default true). Before this, selection optimized an edge ~5× larger than what reaches the book and promoted models that traded $0 live (BTC 15m lightgbm showed "+$1.99/11 trades" in sim but placed **0 live fills** — all decisions blocked at the $0.45 entry cap / fee-edge floor). Expect most assets to honestly select `market_mid_baseline` until a candidate has edge that survives the brake. The brier-vs-mid deploy ceiling (`crypto_model_max_brier_regression_vs_mid`) is the companion guard.
- **Analytic vol fair-value candidate `vol_normal_fair_value` (commit `5c59782`):** mechanism-based `Φ(ln(S/K)/(σ√τ))` from existing spot features + isotonic calibration; competes in the champion pool against the curve-fit heads (from `docs/research/kalshi_15m_market_making_plan.md` §4.2).
- **Trading evaluation:** use `kalshi-bot-cli crypto-report` (decision funnel + live champion) alongside `crypto-pnl-report` (fee-accurate fill economics). **Always compute fill economics from the `fills` table, not `crypto_decision_outcomes` — `fill_count` stays 0 there even when real fills exist (attribution gap), so the decision-funnel reports UNDERSTATE fills.** As of 2026-06-17, model BUY entries have collapsed to ~0/day (06-09=166 → 06-17=0 executed buys) since the 06-12 gate tightening (`CRYPTO_MAX_ENTRY_PRICE_DOLLARS=0.45`, `RISK_MIN_EDGE_BPS=750`, `RISK_MAX_CREDIBLE_EDGE_BPS=1500`, enforced edge-shrinkage). Two blocking layers, neither bankroll: (1) ~99.5% die at `blocked_fee_edge`/`blocked_max_entry_price`/`blocked_shrunk_edge`; (2) the few eligible place a passive bid that isn't hit and the taker cross is refused by `crypto_max_fee_to_edge_ratio` → order status `passive_unfilled_taker_blocked`. Sizing works (avg 1.49 contracts when entries fire). The squeeze: realized edge (≈0.2×predicted, predicted capped 1500 bps → ≤300 bps) can't clear the ~347 bps fee-ratio requirement at p≈0.45 — only at p≲0.305 or if β≥~0.233. Unlock = more real edge (sigma/calibration), not loosening gates. Touch20 (`btc15m_touch20`, `1h_touch20`) is a separate, fully disabled strategy — `RULES_ENABLED=false`, `RULES_TRADING_ENABLED=false`, containers disabled. Do not conflate touch20 with the model path. The 1h model path is also currently disabled (insufficient OOS data). Promotion process and per-asset gate/model artifact history: `docs/operations/crypto-live-asset-promotion.md`.

### Market-making research stack (`mm/`)
A self-contained, **NON-TRADING** research subsystem implementing `docs/research/kalshi_15m_market_making_plan.md` as a continuous loop, isolated in its own container (`crypto_mm_production`, CPU-only, `mem_limit 8g`, `oom_score_adj 500`). It **never places orders** — execution stays in `ExecutionService`; the container is triple-guarded (`CRYPTO_TRADING_ENABLED=false`, shadow on, kill switch on).
- `data_spine.py` — multi-venue spot consolidation (volume/recency weighted, staleness + outlier guards), market-tick normalization anchored to `floor_strike`.
- `fair_value.py` — analytic `Φ(ln(S/K)/(σ√τ))` + the σ̂ estimator (`realized_vol`). **σ is the lever** — `crypto-vol-eval` showed the analytic edge stands or falls on it; iterate σ here.
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
- **Weather is fully disabled (crypto-only operation, as of 2026-05-20)** — escalated from the earlier auto-rooms pause. In `.env`: `*_TRIGGER_ENABLE_AUTO_ROOMS=false`, `*_WEATHER_PREDICTION_ENABLED=false`, `*_WEATHER_INTRADAY_MODEL_ENABLED=false`, residual models off, `WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS=0` (stops the daemon periodic weather loop + nested daily rejected-opportunity scorer), `WEATHER_REJECTED_OPPORTUNITY_SCORER_ENABLED=false`. Weather services stay constructed but inert; there is no weather nightly-training auto-toggle, so nothing reschedules it. Do not re-enable any weather flag without operator instruction. See `docs/operations/weather-disabled-2026-05-20.md` (full kill-set + restore) and the historical `docs/operations/weather-pause-2026-05-16.md`.
