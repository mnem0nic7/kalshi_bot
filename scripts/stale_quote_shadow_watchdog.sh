#!/usr/bin/env bash
# Keep the stale-quote SHADOW logger (GET-only, zero orders) running; append to host disk.
# Cron-safe: exits silently if already running. See scripts/stale_quote_shadow.py.
set -euo pipefail

OUT_DIR=/home/user1/kalshi_stale_shadow
OUT="$OUT_DIR/signals.jsonl"
SCRIPT=/home/user1/workspace/kalshi_bot/scripts/stale_quote_shadow.py
PIDFILE="$OUT_DIR/runner.pid"
mkdir -p "$OUT_DIR"

# already running? (pidfile check — pgrep would self-match the invoking shell)
if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then exit 0; fi

CONTAINER=""
for c in infra-trainer_production-1 infra-app_production_blue-1 infra-app_production_green-1; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then CONTAINER="$c"; break; fi
done
[ -z "$CONTAINER" ] && { echo "$(date -u +%FT%TZ) no container" >> "$OUT_DIR/watchdog.log"; exit 1; }

docker cp "$SCRIPT" "$CONTAINER":/app/stale_quote_shadow.py >/dev/null 2>&1 || true
nohup docker exec "$CONTAINER" env STALE_SHADOW_STDOUT=1 \
  python /app/stale_quote_shadow.py BTC,BNB,HYPE,DOGE >> "$OUT" 2>> "$OUT_DIR/err.log" &
echo $! > "$PIDFILE"
echo "$(date -u +%FT%TZ) started via $CONTAINER (pid $(cat "$PIDFILE"))" >> "$OUT_DIR/watchdog.log"
