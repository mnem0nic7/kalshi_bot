# Crypto Touch Strategy Rollout

Status: deprecated for production trading.

The current production posture is model-trained replay only. The deterministic
30% touch strategy can still be studied offline, but it must not be the live
authority path. `CRYPTO_MODEL_TRAINED_REPLAY_ONLY=true` is the default runtime
guard; when it is enabled, `CRYPTO_TOUCH_STRATEGY_ENABLED=true` is ignored by
forecast/recommendation logic, and live execution blocks final-minute passive
traces.

## Production Runtime Mode

Production should keep trained-model and replay infrastructure on, and touch
strategy off:

```dotenv
CRYPTO_MODEL_TRAINED_REPLAY_ONLY=true
PRODUCTION_CRYPTO_TOUCH_STRATEGY_ENABLED=false
PRODUCTION_CRYPTO_TRAINING_PREFLIGHT_ENABLED=true
PRODUCTION_CRYPTO_TRAINING_FEATURE_STORE_ENABLED=true
```

`daemon_production_<color>` handles the 15m runtime. The color-scoped
`daemon_production_crypto_1h_<color>` handles the 1h runtime when explicitly
enabled. The old singleton `crypto_1h_production` live-path refresh container
remains opt-in.

## Minimal Blue/Green Deploy

Deploy the inactive production color, wait for health, then promote:

```bash
scripts/blue_green_redeploy.sh --env production --yes
```

For a dry run:

```bash
scripts/blue_green_redeploy.sh --env production --dry-run
```

The script recreates only the incoming production app, 15m daemon, and 1h daemon
for that color before promotion. The active color keeps the execution lock until
the new color is healthy.

## Offline Research Only

If you need to reproduce old touch analytics, do it in a local/offline research
environment and explicitly opt out of the production guard there:

```dotenv
CRYPTO_MODEL_TRAINED_REPLAY_ONLY=false
CRYPTO_TOUCH_STRATEGY_ENABLED=true
CRYPTO_TOUCH_STRATEGY_TAKE_PROFIT_PCT=0.30
CRYPTO_TOUCH_STRATEGY_STOP_LOSS_PCT=0.30
CRYPTO_TOUCH_STRATEGY_MIN_TOUCH_PROBABILITY=0.60
```

Do not carry that override into production. Promote a crypto asset only after a
trained model, replay backtest, and replay gate all pass with strict real-quote
candidate evidence.
