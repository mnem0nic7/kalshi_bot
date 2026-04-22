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

# Run migrations
alembic upgrade head

# Start the app locally
python3 -m kalshi_bot.main

# CLI entry point
kalshi-bot-cli <subcommand>   # see README for the full list
```

No linter/formatter is configured in `pyproject.toml`. Tests use `pytest-asyncio` with `asyncio_mode = "auto"`.

## Architecture

The platform is one async Python service (`src/kalshi_bot/`) with the following layers:

### Dependency injection via `AppContainer`
`services/container.py` constructs and wires every service at startup. Almost every service receives a `Settings` object, `async_sessionmaker`, and collaborating services through this container. When adding a new service, register it here.

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

### Integrations (`integrations/`)
- `kalshi.py` — REST (RSA-signed) + WebSocket client
- `weather.py` — NWS/NOAA ingestion
- `forecast_archive.py` — Open-Meteo historical weather recovery

### Persistence
Postgres + SQLAlchemy async + `pgvector` for semantic memory embeddings. In tests, SQLite is used via a JSON-compatible type wrapper (no pgvector). Alembic migrations live in `alembic/`.

### Control room (`web/`)
FastAPI app with server-rendered Jinja2 templates, SSE transcript stream, and REST endpoints. The top-level summary strip (`/api/control-room/summary`) is designed to be fast — it avoids live market discovery and uses lightweight room snapshots. The `Research` view also exposes an 180d-only assignment review queue (`ready_for_approval`, `drifted_assignment`, `evidence_weakened`, `aligned`, `waiting_for_evidence`), and city detail includes the latest approval note plus next-action copy. The operator win-rate card uses `PlatformRepository.get_fill_win_rate_30d()` and treats wins as realized-P&L-positive exits first, falling back to settlement results only when no sell fill exists for that ticker and side.

### Blue/green deployment
A DB-backed single-writer lock enforces that only the active color (`app_color` setting) can acquire the execution lock. The kill switch (`app_enable_kill_switch`) clears the execution lock and blocks new live orders. Self-improve staging is checkpoint-based: promotions write `pending_pack_promotion:{kalshi_env}:{color}`, and the target color's daemon applies it at startup so watchdog restarts or failovers do not strand an old pack assignment. Canary state has a max lifetime and becomes `stalled` after `SELF_IMPROVE_CANARY_MAX_SECONDS`.

### Historical data layers (four separate concerns)
1. `source_replay_coverage` — strict-as-of replay sources
2. `checkpoint_archive_coverage` — canonical checkpoint-weather records
3. `external_archive_coverage` — Open-Meteo-assisted recovery
4. `replay_corpus` — materialized `historical_replay` rooms

## Key configuration
`config.py` (`Settings`) reads from `.env`. Key env vars:
- `KALSHI_ENV` — `demo` or `live`
- `LIVE_KALSHI_API_KEY` / `DEMO_KALSHI_API_KEY` — API key IDs
- `LIVE_KALSHI_READ_PRIVATE_KEY_PATH` / `DEMO_*` — RSA PEM paths
- `GEMINI_KEY` or `GEMINI_API_KEY` — primary LLM provider
- `APP_SHADOW_MODE=true` — prevents live order submission (default on)
- `APP_COLOR` — `blue` or `green` for blue/green deployment
- `SELF_IMPROVE_CANARY_MAX_SECONDS` — max staged-canary lifetime before status becomes `stalled`
- `WEATHER_MARKET_MAP_PATH` — path to market config YAML (default: `docs/examples/weather_markets.example.yaml`)

## Safety rules
- The app starts in shadow mode (`APP_SHADOW_MODE=true`) and with the kill switch enabled by default. Do not disable either until mappings, reconciliation, and restart recovery are validated.
- LLM responses are inputs to human-readable transcripts only. Deterministic engines are authoritative for all trading decisions.
- Kalshi write endpoints are only reachable through `ExecutionService`, which checks the kill switch and deployment lock before every order.
