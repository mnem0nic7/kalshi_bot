# First Shadow Rollout

This checklist is the safest first deploy path after merging to `main`.

Use it when you want the platform online, observable, and reconciling against Kalshi without allowing live order placement.

## Preconditions

- `Demo Smoke` succeeded in GitHub Actions.
- `Live Smoke` succeeded in GitHub Actions if you plan to point the VPS at production credentials.
- Your VPS has Docker and Docker Compose installed.
- Your VPS `.env` has valid Kalshi keys, PEM paths, and a local `POSTGRES_PASSWORD`.
- `APP_SHADOW_MODE=true`
- `APP_ENABLE_KILL_SWITCH=true`
- `TRIGGER_ENABLE_AUTO_ROOMS=false` for the first burn-in
- `docs/examples/weather_markets.example.yaml` has been replaced with real supported market mappings before any autonomous workflow testing

## 1. Pull The Merged Main Branch

```bash
cd /path/to/kalshi_bot
git fetch origin
git checkout main
git pull --ff-only origin main
```

## 2. Confirm Safety Settings

```bash
grep -E '^(APP_SHADOW_MODE|APP_ENABLE_KILL_SWITCH|TRIGGER_ENABLE_AUTO_ROOMS|KALSHI_ENV|POSTGRES_PASSWORD)=' .env
```

Expected for first rollout:

- `APP_SHADOW_MODE=true`
- `APP_ENABLE_KILL_SWITCH=true`
- `TRIGGER_ENABLE_AUTO_ROOMS=false`

## 3. Validate Compose Resolution

```bash
docker compose -f infra/docker-compose.yml config >/tmp/kalshi-compose.rendered.yaml
tail -n 20 /tmp/kalshi-compose.rendered.yaml
```

This catches missing env values before anything starts.

## 4. Start Postgres And Run Migrations

```bash
docker compose -f infra/docker-compose.yml up -d postgres
docker compose -f infra/docker-compose.yml run --rm --no-deps migrate
```

## 5. Start The Shadow Stack

```bash
docker compose -f infra/docker-compose.yml up -d --build app_blue app_green daemon_blue daemon_green nginx
docker compose -f infra/docker-compose.yml ps
```

Expected:

- `postgres` healthy
- `app_blue`, `app_green`, `daemon_blue`, `daemon_green`, and `nginx` up

## 6. Verify HTTP Health

```bash
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/api/status | jq
```

Check that:

- `/healthz` returns `{"status":"ok"}`
- `/readyz` reports an `active_color`
- `/api/status` shows the kill switch enabled

## 7. Check Logs For Startup Errors

```bash
docker compose -f infra/docker-compose.yml logs --tail=100 app_blue
docker compose -f infra/docker-compose.yml logs --tail=100 app_green
docker compose -f infra/docker-compose.yml logs --tail=100 daemon_blue
docker compose -f infra/docker-compose.yml logs --tail=100 daemon_green
```

Look for:

- migration failures
- websocket auth failures
- repeated DB connection retries
- missing weather mapping errors

## 8. Run Read-Only Operator Checks

Inside the app container:

```bash
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli status
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli reconcile
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli discover --json
```

These should succeed without placing orders.

## 9. Validate Research And Manual Room Flow

Pick one configured market ticker and run:

```bash
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli research-refresh <MARKET_TICKER>
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli research-show <MARKET_TICKER>
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli create-room --name "shadow smoke" --market-ticker <MARKET_TICKER>
```

Then use the returned room id:

```bash
docker compose -f infra/docker-compose.yml exec app_blue python -m kalshi_bot.cli run-room <ROOM_ID>
```

Expected:

- research artifacts persist
- room messages appear in the UI
- no live order should be submitted because shadow mode is still enabled

## 10. Leave It In Burn-In Mode

For the first burn-in window:

- keep `APP_SHADOW_MODE=true`
- keep the kill switch enabled
- keep auto-triggering off
- monitor logs, reconciliation, and room behavior for several market cycles

## 11. Only After Burn-In

When the shadow run is clean:

1. Replace any remaining sample mappings with production-ready ones.
2. Optionally enable `TRIGGER_ENABLE_AUTO_ROOMS=true` while still in shadow mode.
3. Promote the intended active color:

```bash
infra/scripts/promote.sh green
```

4. Verify the new color in `/readyz` and `/api/status`.
5. Only then consider disabling the kill switch and eventually shadow mode for tiny live limits.

## Rollback

If startup or reconciliation looks wrong:

```bash
docker compose -f infra/docker-compose.yml logs --tail=200
docker compose -f infra/docker-compose.yml down
```

If the issue is color-specific:

```bash
infra/scripts/promote.sh blue
```

If the database needs restore:

```bash
infra/scripts/restore.sh <dump-file>
```
