#!/usr/bin/env bash
# Daily CPI nowcast-vs-market shadow snapshot (read-only, GET). Appends one JSON
# line to persistent HOST disk so it survives container recreates. Targets whichever
# production app container (blue/green) is currently running. See scripts/collect_cpi_shadow.py.
set -euo pipefail

OUT=/home/user1/kalshi_cpi_shadow/snapshots.jsonl
SCRIPT=/home/user1/workspace/kalshi_bot/scripts/collect_cpi_shadow.py
mkdir -p "$(dirname "$OUT")"

CONTAINER=""
for c in infra-app_production_blue-1 infra-app_production_green-1; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then CONTAINER="$c"; break; fi
done
if [ -z "$CONTAINER" ]; then echo "$(date -u +%FT%TZ) no running production app container" >&2; exit 1; fi

docker cp "$SCRIPT" "$CONTAINER":/app/collect_cpi_shadow.py >/dev/null 2>&1 || true
docker exec "$CONTAINER" env CPI_SHADOW_STDOUT=1 python /app/collect_cpi_shadow.py >> "$OUT"
echo "$(date -u +%FT%TZ) appended snapshot via $CONTAINER (host lines: $(wc -l < "$OUT"))"
