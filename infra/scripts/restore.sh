#!/usr/bin/env bash
set -euo pipefail

env_name="${1:?usage: restore.sh <demo|production> <dump.sql.gz>}"
infile="${2:?usage: restore.sh <demo|production> <dump.sql.gz>}"
if [[ "${env_name}" != "demo" && "${env_name}" != "production" ]]; then
  echo "env_name must be demo or production" >&2
  exit 1
fi

postgres_user="${POSTGRES_USER:-postgres}"
postgres_db="${POSTGRES_DB:-kalshi_bot}"

gunzip -c "${infile}" | docker compose -f infra/docker-compose.yml exec -T "postgres_${env_name}" \
  psql -U "${postgres_user}" -d "${postgres_db}"

echo "Restore completed from ${infile}"
