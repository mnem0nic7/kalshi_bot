# Weather Fully Disabled Runbook

Date: 2026-05-20

Supersedes the auto-rooms-only pause (`weather-pause-2026-05-16.md`) by disabling
**all** weather runtime functionality, not just room creation. Crypto is the only
active trading path.

## Reason

Operator decision: crypto-only operation. The earlier pause stopped weather *room
creation* but left the weather prediction signals, intraday/residual model inference,
the daemon's periodic weather-research-refresh loop, and the daily rejected-opportunity
scorer all running as pure overhead. This runbook turns the entire weather subsystem off.

## Scope

Both `demo` and `production`.

## Knobs changed (in `.env`)

```
# Auto-rooms (already off from the 2026-05-16 pause)
DEMO_TRIGGER_ENABLE_AUTO_ROOMS=false
PRODUCTION_TRIGGER_ENABLE_AUTO_ROOMS=false

# Prediction signal (was DEMO=true)
DEMO_WEATHER_PREDICTION_ENABLED=false
PRODUCTION_WEATHER_PREDICTION_ENABLED=false

# Intraday model inference (was PRODUCTION=true)
DEMO_WEATHER_INTRADAY_MODEL_ENABLED=false
PRODUCTION_WEATHER_INTRADAY_MODEL_ENABLED=false

# Residual models (already off)
DEMO_WEATHER_RESIDUAL_MODEL_ENABLED=false
PRODUCTION_WEATHER_RESIDUAL_MODEL_ENABLED=false

# Daemon periodic weather loop + nested daily rejected-opportunity scorer (NEW)
WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS=0
WEATHER_REJECTED_OPPORTUNITY_SCORER_ENABLED=false
```

The two base-named vars reach containers via `env_file: ../.env` passthrough; the
`DEMO_/PRODUCTION_`-prefixed vars are mapped to base names by the compose `environment:`
anchors per container. `WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS` was not previously in
`.env`, so it had been inheriting the code default of `300` (loop running every 5 min).

## What this kills

- Weather signal scoring, intraday + residual model inference.
- The daemon `_periodic_weather_research_refresh_loop` (gated on `interval > 0`).
- The daily rejected-weather-opportunity scorer (nested in that loop).

There is **no** weather nightly-training auto-toggle (unlike crypto's
`CRYPTO_MODEL_NIGHTLY_AUTO_ENABLED`), so once the loop is at `0` and prediction is off,
nothing reschedules weather. Weather services remain constructed but inert — fully
reversible by flipping the flags back.

## What stays untouched

- Crypto autonomy + trading (`CRYPTO_AUTONOMY_ENABLED`, `CRYPTO_TRADING_ENABLED`, and the
  `CRYPTO_PRODUCTION_AUTONOMY_ENABLED` production path) — structurally independent.
- Settlement reconciliation, crypto model nightly regen, historical archival.
- `WEATHER_MARKET_MAP_PATH` is left populated on purpose; emptying it risks a startup
  error in `WeatherMarketDirectory.from_file()`, and with prediction off + the loop at `0`
  the loaded directory is already inert.

## Deploy

Rebuild was required this time because the ADA/BCH asset removal lives in image-baked
defaults (shipped in the same change); the weather-off values are `.env`-driven and apply
on recreate alone. Production was redeployed via a both-colors `--force-recreate` on
2026-05-20. Future deploys should prefer the zero-downtime path
(`scripts/blue_green_redeploy.sh`, `docs/operations/blue-green-redeploy.md`).

## Verification

Confirm inside the active daemon container:

```bash
docker exec infra-daemon_production_blue-1 python -c "
from kalshi_bot.config import Settings
s = Settings()
assert s.weather_prediction_enabled is False
assert s.weather_intraday_model_enabled is False
assert s.weather_research_refresh_interval_seconds == 0
print('weather disabled OK; nightly assets:', s.crypto_model_nightly_assets)
"
```

No new weather rooms should appear (see the SQL in `weather-pause-2026-05-16.md`), and
crypto rooms should continue.

## Restore steps

When the operator says to re-enable weather, flip the relevant flags back in `.env`
(at minimum `*_WEATHER_PREDICTION_ENABLED=true` and
`WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS=300`), then recreate the app + daemon
containers with `--env-file .env` (per CLAUDE.md, otherwise crypto env vars boot stale).
Re-enabling auto-rooms additionally requires `*_TRIGGER_ENABLE_AUTO_ROOMS=true` — see
`weather-pause-2026-05-16.md`.
