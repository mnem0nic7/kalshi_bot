#!/usr/bin/env bash
set -euo pipefail

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
outfile="${1:-backup-${timestamp}.sql.gz}"

docker compose -f infra/docker-compose.yml exec -T postgres \
  pg_dump -U postgres -d kalshi_bot | gzip > "${outfile}"

echo "Backup written to ${outfile}"

