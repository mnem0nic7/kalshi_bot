#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: run-parameter-pack.sh <default|hard-caps|status|validate|gate|drift|grid|select|record-starvation|stage|canary|promote-staged|rollback-staged> [args...]" >&2
  exit 1
fi

compose_file="${COMPOSE_FILE:-infra/docker-compose.yml}"
service="${APP_SERVICE:-app_demo_blue}"

docker compose -f "${compose_file}" exec -T "${service}" \
  python -m kalshi_bot.cli parameter-pack "$@"
