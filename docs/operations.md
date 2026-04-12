# Operations

## Blue/green model

- `app_blue` and `app_green` run simultaneously.
- Both can observe rooms and render the UI.
- Only the DB’s `active_color` may take the execution lock and place orders.

## Promotion flow

1. Deploy the inactive color.
2. Confirm it starts, reconnects to Postgres, and can run room workflows in shadow mode.
3. Enable the kill switch if you want a quiet handoff.
4. Run `infra/scripts/promote.sh green` or `blue`.
5. Verify the new color acquires the execution lock on its next trade attempt.
6. Disable the kill switch when satisfied.

## Migrations

For the Docker deployment flow:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
docker compose -f infra/docker-compose.yml build migrate
docker compose -f infra/docker-compose.yml run --rm --no-deps migrate
```

For local Python:

```bash
alembic upgrade head
```

Always migrate before live promotion.
Always migrate before enabling the watchdog timer on an already-running deployment, because the runtime now depends on the newer agent-pack tables and checkpoints.

## CLI workflow

Typical operational loop:

```bash
kalshi-bot-cli init-db
kalshi-bot-cli discover
kalshi-bot-cli shadow-campaign run --limit 3
kalshi-bot-cli training-status
kalshi-bot-cli stream --max-messages 100
kalshi-bot-cli reconcile
kalshi-bot-cli status
```

By default, `discover` and `stream` now expand any configured `series_templates` from `docs/examples/weather_markets.example.yaml` into the currently active greater/less daily temperature contracts for those locations.

To let live market updates launch rooms automatically:

```bash
kalshi-bot-cli stream --auto-trigger
```

To run the full long-lived production worker:

```bash
kalshi-bot-cli daemon --auto-trigger
```

This mode:

- opens the Kalshi websocket stream
- runs periodic reconciliation
- emits heartbeat ops events
- auto-launches rooms when enabled and when this deployment color is active
- writes durable per-color daemon heartbeats and last-reconcile timestamps for watchdog recovery

The behavior is controlled by:

- `TRIGGER_ENABLE_AUTO_ROOMS`
- `TRIGGER_COOLDOWN_SECONDS`
- `TRIGGER_MAX_SPREAD_BPS`
- `TRIGGER_MAX_CONCURRENT_ROOMS`
- `DAEMON_RECONCILE_INTERVAL_SECONDS`
- `DAEMON_HEARTBEAT_INTERVAL_SECONDS`
- `DAEMON_START_WITH_RECONCILE`

The daemon also monitors staged agent-pack canaries and live-monitor windows. When a candidate pack is staged on the inactive color, that color's daemon automatically produces shadow rooms during heartbeats until the canary either passes or rolls back.

For manual room execution:

```bash
room_id="$(kalshi-bot-cli create-room --name 'manual room' --market-ticker KXHIGHNY-26APR11-T68)"
kalshi-bot-cli run-room "$room_id"
```

For quick shadow transcript collection without creating the room separately:

```bash
kalshi-bot-cli shadow-run KXHIGHNY-26APR11-T68
kalshi-bot-cli shadow-sweep --limit 3
kalshi-bot-cli shadow-campaign run --limit 3
```

The control room index page offers the same behavior with `Run Shadow Room`, plus a `Training And Research` panel that shows corpus readiness, recent dataset builds, research audit issues, and a one-click shadow campaign action.

## Training corpus

Use the training-first commands to keep the weather corpus clean and reproducible:

```bash
kalshi-bot-cli training-status
kalshi-bot-cli research-audit --limit 20
kalshi-bot-cli training-build --mode room-bundles --good-research-only
kalshi-bot-cli training-build --mode role-sft --good-research-only
kalshi-bot-cli training-build --mode evaluation-holdout --settled-only
kalshi-bot-cli training-build-list
```

Historical replay and import loop:

```bash
kalshi-bot-cli historical-status --verbose
kalshi-bot-cli historical-import weather --date-from 2026-03-01 --date-to 2026-03-31 --series KXHIGHNY KXHIGHCHI
kalshi-bot-cli historical-backfill market --date-from 2026-03-01 --date-to 2026-03-31 --series KXHIGHNY KXHIGHCHI
kalshi-bot-cli historical-backfill weather-archive --date-from 2026-03-01 --date-to 2026-03-31 --series KXHIGHNY KXHIGHCHI
kalshi-bot-cli historical-archive capture --once --series KXHIGHNY KXHIGHCHI
kalshi-bot-cli historical-replay weather --date-from 2026-03-01 --date-to 2026-03-31 --series KXHIGHNY KXHIGHCHI
kalshi-bot-cli training-build historical --mode bundles --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli training-build historical --mode gemini-finetune --date-from 2026-03-01 --date-to 2026-03-31 --output data/training/gemini_weather
```

Historical replay rooms are stored with `room_origin = historical_replay`. They reuse the room/export machinery, but the main control-room lists and live/shadow learning surfaces keep filtering to `shadow` and `live` by default so operator views stay focused.

Historical status now reports replayable market-days, full-vs-late checkpoint coverage, missing checkpoint reasons, and whether the Gemini export is only draft-ready or truly split-ready for training.

The self-improvement loop now respects corpus readiness gates. If the corpus is too small, too concentrated, or too weakly labeled, critique and evaluation commands will stop early instead of generating noisy candidates.

## Boot and self-healing

The self-healing runtime assumes the canonical deploy path is `/workspace/kalshi_bot`.

Boot assets:

- `infra/scripts/start-stack.sh`
- `infra/systemd/kalshi-bot-compose.service`

Watchdog assets:

- `infra/scripts/watchdog-run-once.sh`
- `infra/systemd/kalshi-bot-watchdog.service`
- `infra/systemd/kalshi-bot-watchdog.timer`

Recommended host enablement:

```bash
sudo cp infra/systemd/kalshi-bot-compose.service /etc/systemd/system/
sudo cp infra/systemd/kalshi-bot-watchdog.service /etc/systemd/system/
sudo cp infra/systemd/kalshi-bot-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-bot-compose.service
sudo systemctl enable --now kalshi-bot-watchdog.timer
```

Manual health and watchdog commands:

```bash
kalshi-bot-cli health-check app --color blue
kalshi-bot-cli health-check daemon --color blue
kalshi-bot-cli watchdog status
```

Recovery behavior is fixed:

- restart the inactive color if only that color is unhealthy
- restart the active color first if it is unhealthy
- fail over to the healthy inactive color if the active color remains unhealthy after the restart wait
- restart the full stack if both colors are unhealthy

Recovery actions are recorded in ops events and surfaced in `/api/status` plus the control room `Runtime Health` panel.

## Self-improvement loop

The Gemini-first runtime and versioned agent-pack system are documented in [self_improve.md](self_improve.md).

Typical operator flow:

```bash
kalshi-bot-cli self-improve status
kalshi-bot-cli self-improve critique --days 14 --limit 200
kalshi-bot-cli self-improve eval --candidate-version <VERSION> --days 14 --limit 200
kalshi-bot-cli self-improve promote --evaluation-run-id <EVALUATION_RUN_ID>
```

For Docker blue or green deployments, the helper scripts mirror the same flow:

```bash
infra/scripts/run-self-improve.sh status
infra/scripts/run-self-improve.sh critique --days 14 --limit 200
infra/scripts/restart-color.sh green
```

The GitHub Actions control-plane workflows are:

- `.github/workflows/self-improve.yml`
- `.github/workflows/rollback-agent-pack.yml`

`self-improve.yml` stages only the inactive color after a passing evaluation. The live color changes only after the canary finishes and the DB-backed rollout monitor promotes it.

For Docker deployments that need both environments available, `infra/docker-compose.yml` now mounts separate live and demo PEMs into the containers and relies on:

- `LIVE_KALSHI_KEY_PATH_HOST`
- `DEMO_KALSHI_KEY_PATH_HOST`
- `LIVE_KALSHI_API_KEY`
- `DEMO_KALSHI_API_KEY`

## Backups

- `infra/scripts/backup.sh` creates a compressed `pg_dump`.
- `infra/scripts/restore.sh` restores from a chosen dump file.
