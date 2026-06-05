# Crypto Live Asset Promotion

Last updated: 2026-06-05

This runbook promotes one model-trained crypto asset/frequency at a time. It is
for the `CRYPTO_15M` and `CRYPTO_1H` model path, not the non-model Touch20 path.
Keep Touch20 containers and fuses disabled unless an operator explicitly chooses
that separate strategy.

## Current 15m Live Model State

BTC 15m was recovered to live-order-ready on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["BTC"]` and `live_order_ready_assets=["BTC"]`.
- BTC asset mode was `live`.
- Strict eligible real-quote rows: `2629`.
- Current-model live-quality candidates: `117`.
- OOS trade candidates: `109`.
- Replay net simulated P/L: `$17.82`.
- Replay gate: `crypto-15m-gate-20260605160801-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605160706-9f5261f436d4`, `trained`.
- Backtest: `crypto-15m-backtest-20260605160316-8228958d7f96`, `pass`.

XRP 15m became live-order-ready and was promoted live on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["XRP"]` and `live_order_ready_assets=["XRP"]`.
- XRP asset mode was `live`.
- Strict eligible real-quote rows: `2975`.
- Current-model live-quality candidates: `158`.
- OOS trade candidates: `114`.
- Replay net simulated P/L: `$19.67`.
- Replay gate: `crypto-15m-gate-20260605162646-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605162545-162f5fcc347f`, `trained`.
- Backtest: `crypto-15m-backtest-20260605162646-099927b2236d`, `pass`.

BNB 15m became live-order-ready and was promoted live on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["BNB"]` and `live_order_ready_assets=["BNB"]`.
- BNB asset mode was `live`.
- Strict eligible real-quote rows: `3895`.
- Current-model live-quality candidates: `380`.
- OOS trade candidates: `361`.
- Replay net simulated P/L: `$67.89`.
- Replay gate: `crypto-15m-gate-20260605170613-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605170515-24a698005c6a`, `trained`.
- Backtest: `crypto-15m-backtest-20260605170606-29f14b53c0fe`, `pass`.

SOL 15m became live-order-ready and was promoted live on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["SOL"]` and `live_order_ready_assets=["SOL"]`.
- SOL asset mode was `live`.
- Strict eligible real-quote rows: `3903`.
- Current-model live-quality candidates: `340`.
- OOS trade candidates: `327`.
- Replay net simulated P/L: `$66.55`.
- Replay gate: `crypto-15m-gate-20260605171353-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605171305-0471d37016ea`, `trained`.
- Backtest: `crypto-15m-backtest-20260605171352-c962d1305ac9`, `pass`.

ETH 15m became live-order-ready and was promoted live on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["ETH"]` and `live_order_ready_assets=["ETH"]`.
- ETH asset mode was `live`.
- Strict eligible real-quote rows: `4195`.
- Current-model live-quality candidates: `605`.
- OOS trade candidates: `464`.
- Replay net simulated P/L: `$147.79`.
- Replay gate: `crypto-15m-gate-20260605172654-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605172512-776c4638f0fe`, `trained`.
- Backtest: `crypto-15m-backtest-20260605172649-edd3d591cc57`, `pass`.

DOGE 15m became live-order-ready and was promoted live on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["DOGE"]` and `live_order_ready_assets=["DOGE"]`.
- DOGE asset mode was `live`.
- Strict eligible real-quote rows: `4452`.
- Current-model live-quality candidates: `405`.
- OOS trade candidates: `355`.
- Replay net simulated P/L: `$67.45`.
- Replay gate: `crypto-15m-gate-20260605173717-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605173602-9546f1c4d0a9`, `trained`.
- Backtest: `crypto-15m-backtest-20260605173707-92f42be92c7e`, `pass`.

HYPE 15m became live-order-ready and was promoted live on 2026-06-05:

- `crypto-live-path status --require-ready` returned `status=ready`.
- `ready_assets=["HYPE"]` and `live_order_ready_assets=["HYPE"]`.
- HYPE asset mode was `live`.
- Strict eligible real-quote rows: `4685`.
- Current-model live-quality candidates: `104`.
- OOS trade candidates: `380`.
- Replay net simulated P/L: `$46.09`.
- Replay gate: `crypto-15m-gate-20260605174605-b0ed8233c062`, `passed`.
- Model: `crypto-15m-model-20260605174501-2fe69fa1e911`, `trained`.
- Backtest: `crypto-15m-backtest-20260605174601-99fe5d9b027c`, `pass`.

## Process

Set the target asset once and substitute only that asset through the whole run:

```bash
ASSET=BTC
```

1. Confirm production safety posture and the intended live surface.

   ```bash
   docker compose --env-file .env -f infra/docker-compose.yml ps
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli status
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-asset-mode list \
     --kalshi-env production \
     --frequency 15m
   docker exec infra-app_production_green-1 sh -lc 'printf "AUTO=%s\n1H_TOUCH_CONTAINER=%s\n1H_TOUCH_RULES=%s\n1H_TOUCH_TRADING=%s\n15M_TOUCH_CONTAINER=%s\n" "$CRYPTO_AUTO_FREQUENCIES" "$ENABLE_CRYPTO_1H_TOUCH20_CONTAINER" "$PRODUCTION_CRYPTO_1H_TOUCH20_RULES_ENABLED" "$PRODUCTION_CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED" "$ENABLE_BTC15M_TOUCH20_CONTAINER"'
   ```

   For a 15m-only promotion, `CRYPTO_AUTO_FREQUENCIES` must include `15m` and
   must not include `1h`. Touch20 container/rule/trading flags must stay
   disabled unless that separate strategy is intentionally being deployed.

2. Check the target asset before changing anything.

   ```bash
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-live-path status \
     --kalshi-env production \
     --frequency 15m \
     --assets "$ASSET" \
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
     --assets "$ASSET" \
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
     --assets "$ASSET" \
     --require-ready \
     --json
   ```

5. Promote only after readiness passes.

   ```bash
   docker exec infra-app_production_green-1 python -m kalshi_bot.cli crypto-asset-mode set \
     --kalshi-env production \
     "$ASSET" live
   ```

   The asset-mode command is asset-scoped. Before promoting a 15m-only asset,
   verify the active runtime is constrained to 15m and that 1h strategy services
   are not running.

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
