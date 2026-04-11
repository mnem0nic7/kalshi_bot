# Kalshi Bot

Multi-agent Kalshi weather trading platform with a visible control room, deterministic risk and execution gates, Postgres-backed memory, and blue/green Docker deployment support.

## What’s here

- `src/kalshi_bot`: application code for agents, orchestration, Kalshi and NOAA integrations, FastAPI UI, and persistence.
- `infra`: Docker, reverse proxy, scripts, and systemd assets for VPS deployment.
- `docs`: architecture, agent protocol, strategy, security, database, and operations guides.

For training prep and dataset exports, use [docs/training.md](docs/training.md).
For Gemini runtime routing, agent packs, and daily self-improvement operations, use [docs/self_improve.md](docs/self_improve.md).

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
kalshi-bot-cli shadow-run KXHIGHNY-26APR11-T68
kalshi-bot-cli shadow-sweep --limit 3
kalshi-bot-cli research-refresh KXHIGHNY-26APR11-T68
kalshi-bot-cli research-show KXHIGHNY-26APR11-T68
kalshi-bot-cli research-failures
kalshi-bot-cli training-export --mode bundles --output data/training/room_bundles.jsonl
kalshi-bot-cli training-export --mode role-sft --roles researcher trader --output data/training/role_sft.jsonl
kalshi-bot-cli self-improve status
kalshi-bot-cli self-improve critique --days 14 --limit 200
kalshi-bot-cli self-improve eval --candidate-version <VERSION> --days 14 --limit 200
kalshi-bot-cli self-improve promote --evaluation-run-id <EVALUATION_RUN_ID>
kalshi-bot-cli self-improve rollback --reason manual_rollback
kalshi-bot-cli health-check app --color blue
kalshi-bot-cli health-check daemon --color blue
kalshi-bot-cli watchdog status
kalshi-bot-cli create-room --name "NYC weather" --market-ticker KXHIGHNY-26APR11-T68
kalshi-bot-cli run-room <room-id>
kalshi-bot-cli reconcile
kalshi-bot-cli status
kalshi-bot-cli kill-switch on
kalshi-bot-cli promote green
```

`discover --json` now expands any configured `series_templates` into the currently active greater/less daily temperature markets, and the control room uses the same live discovery path.

The control room also supports one-click `Run Shadow Room` actions from the market cards. That path creates a room and immediately runs the workflow in shadow mode so you can build training transcripts quickly without placing orders.

## GitHub Actions smoke workflows

The repo includes manual read-only smoke workflows for both Kalshi environments:

- `.github/workflows/demo-smoke.yml`
- `.github/workflows/live-smoke.yml`
- `.github/workflows/compose-shadow-smoke.yml`
- `.github/workflows/self-improve.yml`
- `.github/workflows/rollback-agent-pack.yml`

Add these GitHub Secrets before running them:

- `DEMO_KALSHI_API_KEY`
- `DEMO_KALSHI_PRIVATE_KEY_PEM`
- `LIVE_KALSHI_API_KEY`
- `LIVE_KALSHI_PRIVATE_KEY_PEM`
- `POSTGRES_PASSWORD`
- `GEMINI_API_KEY`
- `DEPLOY_HOST`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY`
- `DEPLOY_APP_DIR`

Each workflow writes the PEM to a temporary file at runtime, runs REST plus WebSocket auth checks, and removes the file before exit. Neither workflow places orders.

`Compose Shadow Smoke` uses `POSTGRES_PASSWORD` to build a temporary `.env`, starts Postgres, runs Alembic migrations, boots the nginx plus FastAPI web stack in shadow mode, and hits `/healthz`, `/readyz`, and `/api/status`. It uses temporary dummy PEM files because this workflow validates deploy mechanics rather than authenticated Kalshi access.

`Live Smoke` now runs in the GitHub Actions environment named `live`. For the safer setup:

- Go to `Settings` -> `Environments` -> `live`
- Add required reviewers before the workflow can run
- Move `LIVE_KALSHI_API_KEY` and `LIVE_KALSHI_PRIVATE_KEY_PEM` into that environment if you want the live credentials isolated from repo-wide secrets

`Self Improve` runs a local offline guard test slice, then critiques and evaluates the last 14 days of shadow or demo rooms on the VPS. If the candidate pack passes the holdout gates, it stages the pack on the inactive color and restarts only that color so canary shadow rooms can begin. `Rollback Agent Pack` is manual-only and is designed to live behind the GitHub Actions `live` environment.

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
- Gemini-first runtime routing for LLM-backed roles with local fallback
- Versioned agent packs and GitHub Actions self-improvement loop
- Host-native watchdog recovery with compose healthchecks and color failover

## Important safety note

This repo is wired for live-capable trading, but it should begin in demo or shadow mode. Keep the kill switch enabled until mappings, reconciliation, and restart recovery are validated.

## Boot and watchdog

The canonical live path is `/workspace/kalshi_bot`.

Boot and recovery assets:

- `infra/scripts/start-stack.sh`
- `infra/scripts/watchdog-run-once.sh`
- `infra/systemd/kalshi-bot-compose.service`
- `infra/systemd/kalshi-bot-watchdog.service`
- `infra/systemd/kalshi-bot-watchdog.timer`

Recommended host setup:

```bash
sudo cp infra/systemd/kalshi-bot-compose.service /etc/systemd/system/
sudo cp infra/systemd/kalshi-bot-watchdog.service /etc/systemd/system/
sudo cp infra/systemd/kalshi-bot-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-bot-compose.service
sudo systemctl enable --now kalshi-bot-watchdog.timer
```

If you are upgrading an already-running stack, run migrations before using the new watchdog CLI or timer:

```bash
docker compose -f infra/docker-compose.yml run --rm --no-deps migrate
```
