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

## Current BTC 1h Model State

BTC 1h was checked on 2026-06-06 and is not live-order-ready yet:

- Kalshi discovery found 10 open BTC 1h markets.
- BTC asset mode is already `live`, but live orders are still blocked by the
  per-asset replay gate.
- Latest BTC 1h replay gate: `crypto-1h-gate-20260605185128-eea5982844bb`,
  `blocked`.
- Latest BTC 1h backtest: `crypto-1h-backtest-20260605184905-63fb5a780683`,
  `warn`.
- Latest BTC 1h model: `crypto-1h-model-20260605203830-018765f6af9e`,
  `trained`, still with `0` live-quality candidates.
- Strict eligible real-quote rows: `198585` from the latest replay artifacts;
  the fast status command deliberately reports only bounded support probes and
  uses artifact metrics for strict-row volume.
- Strict settled real-quote BTC 1h evidence currently covers only one close
  day, 2026-06-05 (`1930` markets, `400282` rows). Older settled days exist
  as settled backfill, but not as strict point-in-time bid/ask quote rows, so
  the walk-forward OOS gate cannot honestly pass yet.
- Spot feature coverage: `99.5847%`.
- Current-model live-quality candidates: `0`.
- Replay net simulated P/L: `$0.00`.
- Gate reasons: OOS replay unavailable, live-quality candidates below 50, and
  net simulated P/L not positive.

Do not start the 1h live daemon or enable 1h production trading until a fresh
BTC 1h refresh has strict real-quote evidence across multiple close days,
produces a passed per-asset replay gate, and the status wrapper exits `0` with
`ready=true`.

## Process

Set the target asset and frequency once, then substitute only those values
through the whole run:

```bash
ASSET=BTC
FREQ=1h
ACTIVE_COLOR="$(docker exec infra-app_production_green-1 python -m kalshi_bot.cli status | python3 -c 'import json,sys; print(json.load(sys.stdin)["active_color"])')"
ACTIVE_CONTAINER="infra-app_production_${ACTIVE_COLOR}-1"
if [[ "$ACTIVE_COLOR" == "blue" ]]; then
  INACTIVE_CONTAINER=infra-app_production_green-1
else
  INACTIVE_CONTAINER=infra-app_production_blue-1
fi
```

1. Confirm production safety posture and the intended live surface.

   ```bash
   docker compose --env-file .env -f infra/docker-compose.yml ps
   docker exec "$ACTIVE_CONTAINER" python -m kalshi_bot.cli status
   docker exec "$INACTIVE_CONTAINER" python -m kalshi_bot.cli crypto-asset-mode list \
     --kalshi-env production \
     --frequency "$FREQ"
   docker exec "$INACTIVE_CONTAINER" sh -lc 'printf "AUTO=%s\n1H_DAEMON=%s\n1H_AUTONOMY=%s\n1H_PROD_AUTONOMY=%s\n1H_TRADING=%s\n1H_TOUCH_CONTAINER=%s\n1H_TOUCH_RULES=%s\n1H_TOUCH_TRADING=%s\n15M_TOUCH_CONTAINER=%s\n" "$CRYPTO_AUTO_FREQUENCIES" "$ENABLE_CRYPTO_1H_DAEMON" "$PRODUCTION_CRYPTO_1H_AUTONOMY_ENABLED" "$PRODUCTION_CRYPTO_1H_PRODUCTION_AUTONOMY_ENABLED" "$PRODUCTION_CRYPTO_1H_TRADING_ENABLED" "$ENABLE_CRYPTO_1H_TOUCH20_CONTAINER" "$PRODUCTION_CRYPTO_1H_TOUCH20_RULES_ENABLED" "$PRODUCTION_CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED" "$ENABLE_BTC15M_TOUCH20_CONTAINER"'
   ```

   For a 15m-only promotion, `CRYPTO_AUTO_FREQUENCIES` must include `15m` and
   must not include `1h`. Touch20 container/rule/trading flags must stay
   disabled unless that separate strategy is intentionally being deployed. For
   a 1h model-path promotion, use the dedicated
   `daemon_production_crypto_1h_<active_color>` service; do not add `1h` to the
   main production daemon until explicitly choosing a combined loop.
   The 1h current collector should have settlement propagation enabled
   (`CRYPTO_1H_CURRENT_SETTLED_EVERY_CYCLES=20` and
   `CRYPTO_1H_CURRENT_SETTLED_LABEL_PROPAGATION_ENABLED=true`) so collected quote
   evidence becomes replay-eligible as markets settle.
   `ENABLE_CRYPTO_1H_DAEMON` defaults to `false`; keep it false until this
   runbook's readiness step passes.

2. Check the target asset before changing anything.

   ```bash
   scripts/crypto_live_path_status.sh \
     --kalshi-env production \
     --frequency "$FREQ" \
     --asset "$ASSET" \
     --docker-container "$INACTIVE_CONTAINER"
   ```

   The wrapper stores the full JSON under
   `reports/crypto_live_path/status/` and prints the readiness fields that
   matter: strict quote days, strict rows, replay gate, live-quality candidate
   count, P/L, active/app color, warnings, and blockers. It always uses
   `crypto-live-path status --skip-growth`, whose fast path uses bounded SQL
   probes and a local statement timeout for operator status checks.

3. Refresh evidence and artifacts for exactly one asset.

   ```bash
   scripts/crypto_live_path_refresh.sh \
     --kalshi-env production \
     --frequency "$FREQ" \
     --settled-days 2 \
     --history-days 2 \
     --spot-days 2 \
     --replay-days 30 \
     --assets "$ASSET" \
     --docker-env CRYPTO_COLLECT_SETTLED_CANDLES_ENABLED=false \
     --docker-env CRYPTO_HISTORY_CANDLE_CONCURRENCY=1 \
     --docker-container "$INACTIVE_CONTAINER"
   ```

   The underlying `crypto-live-path refresh` command collects open/settled
   Kalshi evidence, refreshes Coinbase spot, runs training preflight, trains the
   per-asset model, runs replay, and gates the asset. If refresh creates enough
   strict rows after an initial preflight block, it performs one post-refresh
   train/replay/gate retry before returning.
   For 1h, keep settled candle capture disabled during the refresh because the
   following history bootstrap captures candles; also keep candle concurrency at
   `1` unless the Kalshi candlestick endpoint is clearly tolerating more.
   For 1h assets that have been collecting live quotes, a manual label catch-up
   can be run before refresh without changing trading state:

   ```bash
   docker exec "$INACTIVE_CONTAINER" python -m kalshi_bot.cli crypto-history collect-settled \
     --kalshi-env production \
     --frequency "$FREQ" \
     --assets "$ASSET" \
     --days 2 \
     --skip-candles \
     --skip-quality \
     --json
   ```

4. Verify readiness. This must exit `0`.

   ```bash
   scripts/crypto_live_path_status.sh \
     --kalshi-env production \
     --frequency "$FREQ" \
     --asset "$ASSET" \
     --require-ready \
     --docker-container "$INACTIVE_CONTAINER"
   ```

   A successful `--require-ready` exit proves the asset/model gate for the
   requested asset and frequency: strict rows, multiple strict quote close days
   when OOS is not otherwise usable, current-model live-quality candidate
   count, passed replay gate, positive P/L that beats market-mid, spot coverage,
   non-proxy spot, trained model, and replay artifact. It does not by itself
   prove the inactive container can place live orders. The JSON field
   `live_order_ready_assets` becomes non-empty only from the active color with
   live order switches and credentials also clear.

5. Promote only after readiness passes.

   ```bash
   docker exec "$INACTIVE_CONTAINER" python -m kalshi_bot.cli crypto-asset-mode set \
     --kalshi-env production \
     "$ASSET" live
   ```

   The asset-mode command is asset-scoped. Before promoting a 15m-only asset,
   verify the active runtime is constrained to 15m and that 1h strategy services
   are not running. Before promoting a 1h asset, verify the 15m assets are
   already in their intended modes because the asset-mode table is shared across
   crypto frequencies.

6. For 1h only, enable and start the dedicated active-color daemon after
   readiness passes.

   ```bash
   ENABLE_CRYPTO_1H_DAEMON=true \
   PRODUCTION_CRYPTO_1H_AUTONOMY_ENABLED=true \
   PRODUCTION_CRYPTO_1H_PRODUCTION_AUTONOMY_ENABLED=true \
   PRODUCTION_CRYPTO_1H_TRADING_ENABLED=true \
   docker compose --env-file .env -f infra/docker-compose.yml up -d --no-build --no-deps \
     "daemon_production_crypto_1h_${ACTIVE_COLOR}"
   ```

7. Recheck runtime.

   ```bash
   docker exec "$ACTIVE_CONTAINER" python -m kalshi_bot.cli status
   docker ps --filter 'name=daemon_production_crypto_1h' --format 'table {{.Names}}\t{{.Status}}'
   docker ps --filter 'name=touch20' --format 'table {{.Names}}\t{{.Status}}'
   scripts/crypto_live_path_status.sh \
     --kalshi-env production \
     --frequency "$FREQ" \
     --asset "$ASSET" \
     --require-ready \
     --docker-container "$ACTIVE_CONTAINER"
   ```

## Future Assets

Repeat the same process with one asset substituted for `BTC`. Keep other assets
in `shadow` until their own `--require-ready` check passes. Do not use a BTC
model, BTC replay gate, pooled fallback artifact, or Touch20 gate to promote a
different asset.
