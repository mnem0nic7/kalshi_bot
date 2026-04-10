# Operations

For the first post-merge rollout, use [First Shadow Rollout](first_shadow_rollout.md) as the primary runbook.

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
docker compose -f infra/docker-compose.yml run --rm --no-deps migrate
```

For local Python:

```bash
alembic upgrade head
```

Always migrate before live promotion.

## CLI workflow

Typical operational loop:

```bash
kalshi-bot-cli init-db
kalshi-bot-cli discover
kalshi-bot-cli stream --markets WEATHER-NYC-HIGH-80F --max-messages 100
kalshi-bot-cli reconcile
kalshi-bot-cli status
```

To let live market updates launch rooms automatically:

```bash
kalshi-bot-cli stream --markets WEATHER-NYC-HIGH-80F --auto-trigger
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

The behavior is controlled by:

- `TRIGGER_ENABLE_AUTO_ROOMS`
- `TRIGGER_COOLDOWN_SECONDS`
- `TRIGGER_MAX_SPREAD_BPS`
- `TRIGGER_MAX_CONCURRENT_ROOMS`
- `DAEMON_RECONCILE_INTERVAL_SECONDS`
- `DAEMON_HEARTBEAT_INTERVAL_SECONDS`
- `DAEMON_START_WITH_RECONCILE`

For manual room execution:

```bash
room_id="$(kalshi-bot-cli create-room --name 'manual room' --market-ticker WEATHER-NYC-HIGH-80F)"
kalshi-bot-cli run-room "$room_id"
```

## Backups

- `infra/scripts/backup.sh` creates a compressed `pg_dump`.
- `infra/scripts/restore.sh` restores from a chosen dump file.
