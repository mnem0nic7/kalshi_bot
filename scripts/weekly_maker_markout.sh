#!/usr/bin/env bash
# Weekly maker-fill adverse-selection (markout) snapshot.
#
# Re-runs crypto-maker-markout-report against the *active* color's app container
# and writes a timestamped JSON to ~/maker_markout_logs so we can track whether
# the BTC >=0.50 "clean maker fill" pocket holds up over time (see the
# 2026-06-18 adverse-selection finding). Resolves the active blue/green color at
# run time, so it keeps working across deploys/flips.
#
# Intended for a weekly host crontab entry, e.g.:
#   37 9 * * 1  /home/user1/workspace/kalshi_bot/scripts/weekly_maker_markout.sh
set -euo pipefail

OUT_DIR="${MARKOUT_OUT_DIR:-$HOME/maker_markout_logs}"
DAYS="${MARKOUT_DAYS:-14}"
PG="infra-postgres_production-1"
mkdir -p "$OUT_DIR"
ts="$(date +%Y-%m-%d)"

# deployment_control has multiple rows (id='production', id='live', ...); the
# production stack reads the id='production' row, so filter explicitly — a bare
# LIMIT 1 can return the wrong row and point us at the inactive color.
color="$(docker exec "$PG" bash -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT active_color FROM deployment_control WHERE id='\''production'\'' LIMIT 1"' \
  2>/dev/null | tr -d '[:space:]')"
if [[ -z "$color" ]]; then
  echo "$(date -Is) ERROR: could not resolve active color" >>"$OUT_DIR/run.log"
  exit 1
fi

app="infra-app_production_${color}-1"
out="$OUT_DIR/markout_${ts}.json"
docker exec "$app" kalshi-bot-cli crypto-maker-markout-report \
  --kalshi-env production --days "$DAYS" --json >"$out" 2>"$OUT_DIR/markout_${ts}.err"

bytes="$(wc -c <"$out" 2>/dev/null || echo 0)"
echo "$(date -Is) color=$color days=$DAYS out=$out bytes=$bytes" >>"$OUT_DIR/run.log"
