#!/usr/bin/env bash
set -euo pipefail

color="${1:?usage: promote.sh <blue|green>}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_db="${POSTGRES_DB:-kalshi_bot}"

if [[ "${color}" != "blue" && "${color}" != "green" ]]; then
  echo "color must be blue or green" >&2
  exit 1
fi

docker compose -f infra/docker-compose.yml exec -T postgres \
  psql -U "${postgres_user}" -d "${postgres_db}" \
  -c "UPDATE deployment_control SET active_color='${color}', execution_lock_holder=NULL, updated_at=NOW() WHERE id='default';"

echo "Promoted ${color}"
