# Kalshi Bot

Multi-agent Kalshi weather trading platform with a visible control room, deterministic risk and execution gates, Postgres-backed memory, and blue/green Docker deployment support.

## What’s here

- `src/kalshi_bot`: application code for agents, orchestration, Kalshi and NOAA integrations, FastAPI UI, and persistence.
- `infra`: Docker, reverse proxy, scripts, and systemd assets for VPS deployment.
- `docs`: architecture, agent protocol, strategy, security, database, and operations guides.

For training prep and dataset exports, use [docs/training.md](docs/training.md).

## Quick start

1. Copy `.env.example` to `.env`.
2. Set a local `POSTGRES_PASSWORD` in `.env`, or replace the full `DATABASE_URL` if you are using an existing database. The Compose stack reads the password from your local `.env` instead of checking one into `docker-compose.yml`.
3. Set `LIVE_KALSHI_API_KEY` and `DEMO_KALSHI_API_KEY` in `.env`.
4. Set the live/demo PEM host paths for Docker with `LIVE_KALSHI_KEY_PATH_HOST` and `DEMO_KALSHI_KEY_PATH_HOST`, or use the default local filenames.
5. If you run the app outside Docker, point `LIVE_KALSHI_READ_PRIVATE_KEY_PATH` / `DEMO_KALSHI_READ_PRIVATE_KEY_PATH` at the local PEM files, or keep the default local key file path fallback.
6. Review `docs/examples/weather_markets.example.yaml`. It now uses `series_templates`, so the app can discover current daily temperature contracts automatically for the configured locations.
7. Start Postgres:

```bash
docker compose -f infra/docker-compose.yml up --build -d postgres
```

8. Run migrations:

```bash
docker compose -f infra/docker-compose.yml run --rm --no-deps migrate
```

9. Start the app stack:

```bash
docker compose -f infra/docker-compose.yml up --build -d app_blue app_green daemon_blue daemon_green nginx
```

10. Open `http://localhost:8080`.

## Local Python workflow

If you prefer local Python instead of Docker:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e ".[dev]"
alembic upgrade head
python3 -m kalshi_bot.main
```

## Operator commands

After activating the virtualenv:

```bash
kalshi-bot-cli init-db
kalshi-bot-cli discover --json
kalshi-bot-cli stream --max-messages 25
kalshi-bot-cli stream --auto-trigger
kalshi-bot-cli daemon --auto-trigger
kalshi-bot-cli research-refresh KXHIGHNY-26APR11-T68
kalshi-bot-cli research-show KXHIGHNY-26APR11-T68
kalshi-bot-cli research-failures
kalshi-bot-cli training-export --mode bundles --output data/training/room_bundles.jsonl
kalshi-bot-cli training-export --mode role-sft --roles researcher trader --output data/training/role_sft.jsonl
kalshi-bot-cli create-room --name "NYC weather" --market-ticker KXHIGHNY-26APR11-T68
kalshi-bot-cli run-room <room-id>
kalshi-bot-cli reconcile
kalshi-bot-cli status
kalshi-bot-cli kill-switch on
kalshi-bot-cli promote green
```

`discover --json` now expands any configured `series_templates` into the currently active greater/less daily temperature markets, and the control room uses the same live discovery path.

## GitHub Actions smoke workflows

The repo includes manual read-only smoke workflows for both Kalshi environments:

- `.github/workflows/demo-smoke.yml`
- `.github/workflows/live-smoke.yml`

Add these GitHub Secrets before running them:

- `DEMO_KALSHI_API_KEY`
- `DEMO_KALSHI_PRIVATE_KEY_PEM`
- `LIVE_KALSHI_API_KEY`
- `LIVE_KALSHI_PRIVATE_KEY_PEM`

Each workflow writes the PEM to a temporary file at runtime, runs REST plus WebSocket auth checks, and removes the file before exit. Neither workflow places orders.

`Live Smoke` now runs in the GitHub Actions environment named `live`. For the safer setup:

- Go to `Settings` -> `Environments` -> `live`
- Add required reviewers before the workflow can run
- Move `LIVE_KALSHI_API_KEY` and `LIVE_KALSHI_PRIVATE_KEY_PEM` into that environment if you want the live credentials isolated from repo-wide secrets

## Current defaults

- Weather threshold markets only
- Human-visible multi-agent room
- Shared market research dossiers plus room-local research deltas
- Advisory `president`
- `trader` emits structured `TradeTicket`s
- Trader is gated on fresh research dossier coverage
- Deterministic risk engine and execution clerk remain authoritative
- Blue/green deployment with a DB-backed single-writer lock
- Optional checkpointed Kalshi websocket ingestion via `kalshi-bot-cli stream`
- Optional auto-room launching from streamed books via `kalshi-bot-cli stream --auto-trigger`
- Long-running daemon mode via `kalshi-bot-cli daemon`

## Important safety note

This repo is wired for live-capable trading, but it should begin in demo or shadow mode. Keep the kill switch enabled until mappings, reconciliation, and restart recovery are validated.
