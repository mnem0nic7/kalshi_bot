#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: run-self-improve.sh <status|critique|eval|promote|rollback> [args...]" >&2
  exit 1
fi

compose_file="${COMPOSE_FILE:-infra/docker-compose.yml}"
service="${APP_SERVICE:-app_blue}"

extra_env=()
if [[ -n "${GEMINI_API_KEY:-}" ]]; then
  extra_env+=(-e "GEMINI_API_KEY=${GEMINI_API_KEY}")
fi

docker compose -f "${compose_file}" exec -T "${extra_env[@]}" "${service}" \
  python -m kalshi_bot.cli self-improve "$@"
