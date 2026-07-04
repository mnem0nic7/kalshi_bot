#!/usr/bin/env bash
# Keep the 1h stale-quote SHADOW scanner running on the ACTIVE-color app
# container. GET-only signal logger, zero orders (scripts/stale_quote_shadow_1h.py);
# no enable env needed. Cloned from scripts/stale_quote_pilot_watchdog.sh's
# active-color resolution + in-container process sweep pattern.
set -euo pipefail

OUT_DIR=/home/user1/kalshi_stale_pilot
SCRIPT=/home/user1/workspace/kalshi_bot/scripts/stale_quote_shadow_1h.py
SRC=/home/user1/workspace/kalshi_bot/src/kalshi_bot/crypto/stale_quote_shadow_1h.py
PIDFILE="$OUT_DIR/shadow_1h_runner.pid"
LOG="$OUT_DIR/shadow_1h_watchdog.log"
mkdir -p "$OUT_DIR"

if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then exit 0; fi

# Kill any orphaned IN-CONTAINER 1h shadow scanners first — killing the host
# `docker exec` wrapper does NOT kill the container process. Match ONLY the
# 1h script name so this never touches the 15m pilot or 15m shadow processes.
for c in infra-app_production_blue-1 infra-app_production_green-1; do
  docker ps --format '{{.Names}}' | grep -qx "$c" || continue
  docker exec "$c" sh -c 'ls /proc | grep -E "^[0-9]+$" | while read p; do cmd=$(cat /proc/$p/cmdline 2>/dev/null | tr "\0" " "); case "$cmd" in python*stale_quote_shadow_1h.py*) kill -9 $p;; esac; done' 2>/dev/null || true
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
docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" || { echo "$(date -u +%FT%TZ) active container $CONTAINER not running" >> "$LOG"; exit 1; }

docker cp "$SCRIPT" "$CONTAINER":/app/stale_quote_shadow_1h.py >/dev/null 2>&1 || true
docker exec "$CONTAINER" mkdir -p /app/src/kalshi_bot/crypto >/dev/null 2>&1 || true
docker cp "$SRC" "$CONTAINER":/app/src/kalshi_bot/crypto/stale_quote_shadow_1h.py >/dev/null 2>&1 || true

nohup docker exec "$CONTAINER" python /app/stale_quote_shadow_1h.py >> "$OUT_DIR/shadow_1h_stdout.log" 2>> "$OUT_DIR/shadow_1h_err.log" &
echo $! > "$PIDFILE"
echo "$(date -u +%FT%TZ) started SHADOW_1H via $CONTAINER (pid $(cat "$PIDFILE"))" >> "$LOG"
