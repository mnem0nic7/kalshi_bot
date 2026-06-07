# Crypto Live Asset Promotion

Last updated: 2026-06-07

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

BTC 1h passed the model-path replay gate and was started live on the active
blue production runtime on 2026-06-07:

- Kalshi discovery found 10 open BTC 1h markets.
- BTC asset mode is `live`.
- Refresh report:
  `reports/crypto_live_path/production_1h_BTC_refresh_20260606T174203Z.json`.
- Status report:
  `reports/crypto_live_path/status/production_1h_BTC_status_20260607T154744Z.json`.
- Latest BTC 1h model: `crypto-1h-model-20260606175714-4dbbe1629820`,
  `trained`, `50000` samples, `143` current-model live-quality candidates.
- Final BTC 1h replay proof:
  `reports/crypto_live_path/production_1h_BTC_replay_featurestore50k_20260606T190016Z.json`.
- Latest BTC 1h backtest: `crypto-1h-backtest-20260606190559-169e307c2419`,
  `pass`, `50000` feature-store samples.
- Latest BTC 1h replay gate: `crypto-1h-gate-20260606190628-b0ed8233c062`,
  `passed`.
- Fast status support: `3` strict quote close days and `45985` strict eligible
  real-quote rows from the latest artifact metrics.
- Training preflight materialized `66439` BTC 1h rows, including `54592` strict
  trade-eligible rows, with `100%` spot coverage.
- Final replay quality: `75` live-quality candidates, `57` OOS trade
  candidates, `1` OOS fold, `100%` spot feature coverage, and `$12.84` net
  simulated P/L.
- Runtime proof: `daemon_heartbeat:production:blue:crypto_1h` updated at
  `2026-06-07T15:53:41Z`, and the active blue `crypto_1h` daemon log showed
  successful Kalshi HTTPS market discovery after startup warmup.
- Compose left `infra-daemon_production_crypto_1h_blue-1` stuck in `Created`
  during the 2026-06-07 turn-up. The live 1h loop is currently the equivalent
  `python -m kalshi_bot.cli daemon --crypto-only --heartbeat-role crypto_1h`
  process started inside `infra-daemon_production_blue-1` with
  `CRYPTO_AUTO_FREQUENCIES=1h`, `CRYPTO_AUTONOMY_ENABLED=true`,
  `CRYPTO_PRODUCTION_AUTONOMY_ENABLED=true`, and `CRYPTO_TRADING_ENABLED=true`.
  Clean up the stuck created compose container before the next redeploy so the
  dedicated service can be managed normally.

## Process

Set the target asset and frequency once, then substitute only those values
through the whole run:

```bash
ASSET=BTC
FREQ=1h
TRAIN_MAX_SNAPSHOTS=50000
REPLAY_LIMIT=50000
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
     --replay-limit "$REPLAY_LIMIT" \
     --assets "$ASSET" \
     --docker-env CRYPTO_COLLECT_SETTLED_CANDLES_ENABLED=false \
     --docker-env CRYPTO_HISTORY_CANDLE_CONCURRENCY=1 \
     --docker-env CRYPTO_TRAIN_MAX_SNAPSHOTS="$TRAIN_MAX_SNAPSHOTS" \
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
   Start 1h promotions with `TRAIN_MAX_SNAPSHOTS=50000` and
   `REPLAY_LIMIT=50000`; the full uncapped fit can run for a long time and does
   not add value until the bounded pass has live-quality candidates and a
   plausible replay gate. Increase those caps or omit them only when a broader
   proof is needed after the bounded pass is promising.
   If the refresh/replay path uses rebuilt snapshots and fails OOS candidate
   support, run the final proof against persisted feature rows before deciding
   the asset is blocked:

   ```bash
   docker exec -e CRYPTO_TRAINING_FEATURE_STORE_ENABLED=true "$INACTIVE_CONTAINER" \
     python -m kalshi_bot.cli crypto-replay run \
       --kalshi-env production \
       --frequency "$FREQ" \
       --days 30 \
       --limit "$REPLAY_LIMIT" \
       --assets "$ASSET" \
       --json

   docker exec -e CRYPTO_TRAINING_FEATURE_STORE_ENABLED=true "$INACTIVE_CONTAINER" \
     python -m kalshi_bot.cli crypto-replay gate \
       --kalshi-env production \
       --frequency "$FREQ" \
       --assets "$ASSET"
   ```

   For BTC 1h on 2026-06-06, `REPLAY_LIMIT=75000` found enough OOS candidates
   but failed spot coverage because the older slice included missing spot rows.
   `REPLAY_LIMIT=50000` preserved the prior/latest strict decision-day split
   while keeping spot coverage at `100%`; use the smaller cap first for new 1h
   assets unless their feature-store coverage distribution says otherwise.
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
