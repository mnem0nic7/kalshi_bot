# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
