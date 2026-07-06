#!/usr/bin/env bash
# Weekly cross-asset stale-quote edge recheck (2026-07-06 edge-decay follow-up,
# see docs/research/2026-07-02-stale-quote-taker-edge.md "Edge decay watch").
# READ-ONLY: scripts/xrp_stale_backtest.py only queries settled feature rows +
# spot ticks via PlatformRepository — no orders, no ExecutionService import.
# Detects per-asset edge decay (DOGE flipped +3.70c -> -0.23c/ct window-over-
# window; BTC softened the same week and was independently kill-ruled live)
# before the next asset quietly does the same.
set -euo pipefail

OUT_DIR=/home/user1/kalshi_stale_pilot/edge_recheck
SCRIPT=/home/user1/workspace/kalshi_bot/scripts/xrp_stale_backtest.py
LOG="$OUT_DIR/$(date -u +%F).log"
mkdir -p "$OUT_DIR"

ASSETS="BNB HYPE DOGE ETH XRP"

# active color from the DB via any running app container — cloned from the
# resolution block in scripts/stale_quote_pilot_watchdog.sh (keep in sync).
ACTIVE=""
for c in infra-app_production_blue-1 infra-app_production_green-1; do
  docker ps --format '{{.Names}}' | grep -qx "$c" || continue
  ACTIVE=$(docker exec "$c" python -c "
import asyncio
from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
async def m():
    s=get_settings(); e=create_engine(s); f=create_session_factory(e)
    async with f() as ss:
        c=await PlatformRepository(ss, kalshi_env=s.kalshi_env).get_deployment_control(kalshi_env=s.kalshi_env)
    print(c.active_color); await e.dispose()
asyncio.run(m())" 2>/dev/null | tail -1) && break
done
CONTAINER="infra-app_production_${ACTIVE:-blue}-1"
docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" || { echo "$(date -u +%FT%TZ) active container $CONTAINER not running" >> "$LOG"; exit 1; }

echo "$(date -u +%FT%TZ) weekly stale-edge recheck starting via $CONTAINER" >> "$LOG"
docker cp "$SCRIPT" "$CONTAINER":/tmp/xrp_stale_backtest.py >/dev/null 2>&1

for asset in $ASSETS; do
  echo "--- $asset $(date -u +%FT%TZ) ---" >> "$LOG"
  docker exec "$CONTAINER" python /tmp/xrp_stale_backtest.py "$asset" 14 0 >> "$LOG" 2>&1 \
    || echo "$(date -u +%FT%TZ) $asset recheck FAILED (see above)" >> "$LOG"
done

echo "$(date -u +%FT%TZ) weekly stale-edge recheck complete" >> "$LOG"
