#!/usr/bin/env bash
# Keep the stale-quote micro-PILOT running on the ACTIVE-color app container.
# OPERATOR-AUTHORIZED live orders (2026-07-02 "enable pilot"), hard caps below.
# Orders route through ExecutionService (kill switch / color / creds rails); if
# the color flips, orders auto-stop (inactive_color_skipped) — safe by design.
set -euo pipefail

OUT_DIR=/home/user1/kalshi_stale_pilot
OUT="$OUT_DIR/pilot.jsonl"
SCRIPT=/home/user1/workspace/kalshi_bot/scripts/stale_quote_pilot.py
SRC=/home/user1/workspace/kalshi_bot/src/kalshi_bot/crypto/stale_quote_pilot.py
PIDFILE="$OUT_DIR/runner.pid"
mkdir -p "$OUT_DIR"

if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then exit 0; fi

# Kill any orphaned IN-CONTAINER pilots first — killing the host `docker exec`
# wrapper does NOT kill the container process (we leaked 3 concurrent pilots
# this way on 2026-07-02; multiple pilots multiply the caps).
for c in infra-app_production_blue-1 infra-app_production_green-1; do
  docker ps --format '{{.Names}}' | grep -qx "$c" || continue
  docker exec "$c" sh -c 'ls /proc | grep -E "^[0-9]+$" | while read p; do cmd=$(cat /proc/$p/cmdline 2>/dev/null | tr "\0" " "); case "$cmd" in python*stale_quote_pilot.py*) kill -9 $p;; esac; done' 2>/dev/null || true
done

# active color from the DB via any running app container
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
docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" || { echo "$(date -u +%FT%TZ) active container $CONTAINER not running" >> "$OUT_DIR/watchdog.log"; exit 1; }

docker cp "$SCRIPT" "$CONTAINER":/app/stale_quote_pilot.py >/dev/null 2>&1 || true
docker cp "$SRC" "$CONTAINER":/app/src/kalshi_bot/crypto/stale_quote_pilot.py >/dev/null 2>&1 || true

nohup docker exec "$CONTAINER" env STALE_PILOT_STDOUT=1 \
  STALE_PILOT_ENABLED=1 \
  STALE_PILOT_ASSETS=BTC,BNB,HYPE,DOGE,ETH \
  STALE_PILOT_MAX_TRADES_PER_DAY=10 \
  STALE_PILOT_MAX_OPEN=1 \
  STALE_PILOT_DAILY_LOSS_STOP=3.0 \
  STALE_PILOT_MAX_ENTRY=0.75 \
  python /app/stale_quote_pilot.py >> "$OUT" 2>> "$OUT_DIR/err.log" &
echo $! > "$PIDFILE"
echo "$(date -u +%FT%TZ) started PILOT via $CONTAINER (pid $(cat "$PIDFILE"))" >> "$OUT_DIR/watchdog.log"
