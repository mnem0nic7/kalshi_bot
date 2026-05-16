# Weather Trading Pause Runbook

Date: 2026-05-16

## Reason

Operator-initiated pause. A 6-hour rooms-output evaluation on 2026-05-16 showed
610 weather rooms completing with 0 trade tickets — the deterministic signal was
generating stand-downs across every market (forecasts far from strike prices, no
taker trade clears the configured edge threshold, or remaining payout below
minimum). Weather room creation is pure overhead until market conditions improve.

## Scope

Both `demo` and `production` environments.

## Knob Changed

In `.env`:

```
DEMO_TRIGGER_ENABLE_AUTO_ROOMS=false        # was true
PRODUCTION_TRIGGER_ENABLE_AUTO_ROOMS=false  # was true
```

Eight containers restarted to pick up the new env (all app + daemon, demo and
production, blue and green). Postgres containers were not touched.

## What Remains Active

- Crypto 15m autonomy (`CRYPTO_AUTONOMY_ENABLED`, `CRYPTO_TRADING_ENABLED`) —
  unchanged and confirmed trading normally.
- Settlement reconciliation — runs on daemon heartbeat, not gated on this flag.
- Historical archival and weather data ingestion — continues; only room creation
  is paused.
- Overnight readiness checks — gated on this flag, so weather readiness checks
  will report disabled.

## Verification

No new weather rooms should appear after the restart timestamp:

```sql
-- prod
SELECT COUNT(*) FROM rooms
WHERE kalshi_env = 'production'
  AND created_at > NOW() - INTERVAL '15 minutes'
  AND (market_ticker LIKE 'KXHIGH%' OR market_ticker LIKE 'KXLOW%');
-- expect 0

-- demo
SELECT COUNT(*) FROM rooms
WHERE kalshi_env = 'demo'
  AND created_at > NOW() - INTERVAL '15 minutes'
  AND (market_ticker LIKE 'KXHIGH%' OR market_ticker LIKE 'KXLOW%');
-- expect 0
```

Crypto rooms should still appear:

```sql
SELECT COUNT(*) FROM rooms
WHERE kalshi_env = 'production'
  AND created_at > NOW() - INTERVAL '15 minutes'
  AND market_ticker LIKE 'KX%15M%';
-- expect > 0
```

## Restore Steps

When the operator says to re-enable weather:

1. In `.env`, flip both flags back:
   ```
   DEMO_TRIGGER_ENABLE_AUTO_ROOMS=true
   PRODUCTION_TRIGGER_ENABLE_AUTO_ROOMS=true
   ```

2. Restart the eight containers:
   ```
   docker compose -f infra/docker-compose.yml restart \
     app_demo_blue app_demo_green \
     app_production_blue app_production_green \
     daemon_demo_blue daemon_demo_green \
     daemon_production_blue daemon_production_green
   ```

3. Verify weather rooms reappear (run the verification queries above; expect
   non-zero counts within a few minutes).

4. Archive or delete this file.
