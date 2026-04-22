#!/usr/bin/env bash
set -euo pipefail

env_name="${1:?usage: backup.sh <demo|production> [outfile]}"
if [[ "${env_name}" != "demo" && "${env_name}" != "production" ]]; then
  echo "env_name must be demo or production" >&2
  exit 1
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
outfile="${2:-backup-${env_name}-${timestamp}.sql.gz}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_db="${POSTGRES_DB:-kalshi_bot}"

docker compose -f infra/docker-compose.yml exec -T "postgres_${env_name}" \
  pg_dump -U "${postgres_user}" -d "${postgres_db}" | gzip > "${outfile}"

echo "Backup written to ${outfile}"
