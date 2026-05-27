# Crypto Touch Strategy Rollout

Goal: trade only when the current quote/spot state predicts that a YES or NO
contract is likely to be exitably up 30% before market close. This path is
deterministic and does not require nightly model training.

## Runtime Mode

Use the touch strategy for both 15m and 1h crypto markets:

```dotenv
PRODUCTION_CRYPTO_TOUCH_STRATEGY_ENABLED=true
CRYPTO_TOUCH_STRATEGY_TAKE_PROFIT_PCT=0.30
CRYPTO_TOUCH_STRATEGY_STOP_LOSS_PCT=0.30
CRYPTO_TOUCH_STRATEGY_MIN_TOUCH_PROBABILITY=0.60
CRYPTO_TOUCH_STRATEGY_REQUIRE_EMPIRICAL_BUCKET=false
ENABLE_CRYPTO_1H_DAEMON=true
```

Keep the legacy training loop off:

```dotenv
PRODUCTION_CRYPTO_MODEL_NIGHTLY_AUTO_ENABLED=false
PRODUCTION_CRYPTO_TRAINING_PREFLIGHT_ENABLED=false
PRODUCTION_CRYPTO_TRAINING_FEATURE_STORE_ENABLED=false
CRYPTO_LIVE_PATH_REFRESH_ENABLED=false
ENABLE_CRYPTO_1H_CONTAINER=false
CRYPTO_LEGACY_TRAINING_ENABLED=false
```

`daemon_production_<color>` handles the 15m runtime. The color-scoped
`daemon_production_crypto_1h_<color>` handles the 1h runtime. The old singleton
`crypto_1h_production` live-path refresh container is opt-in only and should stay
off for this strategy.

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

## Re-Enabling Legacy Training

Only opt back in deliberately:

```dotenv
PRODUCTION_CRYPTO_MODEL_NIGHTLY_AUTO_ENABLED=true
CRYPTO_LIVE_PATH_REFRESH_ENABLED=true
```

GitHub Actions `model-quality.yml` also defaults `run_training=false`; set it to
`true` manually if you intentionally want the old train/replay/gate loop.
The weekly `scripts/crypto_model_train_weekly.sh` cron helper also exits unless
`CRYPTO_LEGACY_TRAINING_ENABLED=true`.
