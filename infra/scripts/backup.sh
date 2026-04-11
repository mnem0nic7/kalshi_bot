#!/usr/bin/env bash
set -euo pipefail

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
outfile="${1:-backup-${timestamp}.sql.gz}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_db="${POSTGRES_DB:-kalshi_bot}"

docker compose -f infra/docker-compose.yml exec -T postgres \
  pg_dump -U "${postgres_user}" -d "${postgres_db}" | gzip > "${outfile}"

echo "Backup written to ${outfile}"
