# Crypto Live Asset Promotion

Last updated: 2026-06-05

This runbook promotes one model-trained crypto asset/frequency at a time. It is
for the `CRYPTO_15M` and `CRYPTO_1H` model path, not the non-model Touch20 path.
Keep Touch20 containers and fuses disabled unless an operator explicitly chooses
that separate strategy.

## Current BTC 15m State

BTC 15m was recovered to live-order-ready on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["BTC"]` and `live_order_ready_assets=["BTC"]`.
- BTC asset mode was `live`.
- Strict eligible real-quote rows: `1963`.
- Current-model live-quality candidates: `117`.
- OOS trade candidates: `109`.
- Replay net simulated P/L: `$17.82`.
- Replay gate: `crypto-15m-gate-20260605160801-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605160706-9f5261f436d4`, `trained`.
- Backtest: `crypto-15m-backtest-20260605160316-8228958d7f96`, `pass`.

## Process

1. Confirm production safety posture.

   ```bash
   docker compose --env-file .env -f infra/docker-compose.yml ps
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli status
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-asset-mode list \
     --kalshi-env production \
     --frequency 15m
   ```

2. Check the target asset before changing anything.

   ```bash
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-live-path status \
     --kalshi-env production \
     --frequency 15m \
     --assets BTC \
     --json
   ```

3. Refresh evidence and artifacts for exactly one asset.

   ```bash
   scripts/crypto_live_path_refresh.sh \
     --kalshi-env production \
     --frequency 15m \
     --settled-days 2 \
     --history-days 2 \
     --spot-days 2 \
     --replay-days 30 \
     --assets BTC \
     --docker-container infra-app_production_green-1
   ```

   The underlying `crypto-live-path refresh` command collects open/settled
   Kalshi evidence, refreshes Coinbase spot, runs training preflight, trains the
   per-asset model, runs replay, and gates the asset. If refresh creates enough
   strict rows after an initial preflight block, it performs one post-refresh
   train/replay/gate retry before returning.

4. Verify readiness. This must exit `0`.

   ```bash
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-live-path status \
     --kalshi-env production \
     --frequency 15m \
     --assets BTC \
     --require-ready \
     --json
   ```

5. Promote only after readiness passes.

   ```bash
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-asset-mode set \
     --kalshi-env production \
     BTC live
   ```

6. Recheck runtime.

   ```bash
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli status
   docker ps --filter 'name=touch20' --format 'table {{.Names}}\t{{.Status}}'
   ```

## Future Assets

Repeat the same process with one asset substituted for `BTC`. Keep other assets
in `shadow` until their own `--require-ready` check passes. Do not use a BTC
model, BTC replay gate, pooled fallback artifact, or Touch20 gate to promote a
different asset.
