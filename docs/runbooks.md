# Runbooks

## Stale weather feed

1. Leave the kill switch enabled.
2. Confirm NWS API availability.
3. Check whether the issue is point lookup, forecast fetch, or observation fetch.
4. Resume only after fresh weather data lands and the next room run shows healthy timestamps.

## Kalshi auth or signing failure

1. Confirm the key ID and PEM path.
2. Confirm the signed path excludes query parameters.
3. Validate the container-mounted key file permissions.
4. Rotate the key if the signature still fails.

## Duplicate-order suspicion

1. Enable the kill switch.
2. Query `orders` by `client_order_id`.
3. Compare against Kalshi `GET /portfolio/orders`.
4. Resume only after reconciliation is clean.

## Blue/green rollback

1. Enable the kill switch.
2. Promote the previously healthy color.
3. Confirm the new active color reacquires the execution lock.
4. Run `infra/scripts/sync-web-color.sh <demo|production|all>` if the rollback was manual.
5. Disable the kill switch when stable.

## Crypto current collector stale

1. Leave live trading posture unchanged; do not promote assets to live while current evidence is stale.
2. Check the relevant singleton container: `crypto_current_production` for 15m or `crypto_current_1h_production` for 1h.
3. Inspect recent logs for Kalshi pagination, Coinbase spot, settlement-label, or replay-gate errors.
4. Recreate only the affected singleton with `docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --force-recreate <service>`.
5. Confirm `crypto-live-path status --kalshi-env production --frequency <15m|1h> --assets all --json` shows fresh quote and spot evidence before changing any asset mode.

## Training node stalled

1. Confirm live app and daemon colors are healthy before touching training.
2. Inspect `trainer_production` logs and the latest `crypto_data_quality_runs` / `crypto_model_artifacts`.
3. Recreate only the trainer when needed:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --build trainer_production
```

4. Do not restart the live production daemons to fix trainer-only failures unless the model artifact or replay-gate reader path is also failing.

## Web route mismatch

1. Check the active colors with `kalshi-bot-cli status` for demo and production.
2. Run `infra/scripts/sync-web-color.sh all` to recreate `web_demo`, `web_production`, and `web_strategies` with the DB active colors.
3. Recreate Caddy only if the Caddyfile, hostnames, or ports changed:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --force-recreate caddy
```

4. The checked-in Caddyfile routes localhost and `home.kb-trade.trade` to `web_production`; custom demo or strategy host routing requires a Caddyfile change.
