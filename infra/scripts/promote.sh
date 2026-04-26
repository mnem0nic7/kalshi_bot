#!/usr/bin/env bash
set -euo pipefail

env_name="${1:?usage: promote.sh <demo|production> <blue|green>}"
color="${2:?usage: promote.sh <demo|production> <blue|green>}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_db="${POSTGRES_DB:-kalshi_bot}"

if [[ "${env_name}" != "demo" && "${env_name}" != "production" ]]; then
  echo "env must be demo or production" >&2
  exit 1
fi

if [[ "${color}" != "blue" && "${color}" != "green" ]]; then
  echo "color must be blue or green" >&2
  exit 1
fi

postgres_service="postgres_${env_name}"

docker compose -f infra/docker-compose.yml exec -T "${postgres_service}" \
  psql -U "${postgres_user}" -d "${postgres_db}" \
  -c "INSERT INTO deployment_control (id, active_color, shadow_color, kill_switch_enabled, execution_lock_holder, notes, updated_at)
      VALUES ('${env_name}', '${color}', NULL, TRUE, NULL, '{}', NOW())
      ON CONFLICT (id) DO UPDATE
      SET active_color=EXCLUDED.active_color,
          execution_lock_holder=NULL,
          updated_at=NOW();"

echo "Promoted ${env_name} ${color}"
