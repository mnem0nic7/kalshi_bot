#!/usr/bin/env bash
set -euo pipefail

infile="${1:?usage: restore.sh <dump.sql.gz>}"
postgres_user="${POSTGRES_USER:-postgres}"
postgres_db="${POSTGRES_DB:-kalshi_bot}"

gunzip -c "${infile}" | docker compose -f infra/docker-compose.yml exec -T postgres \
  psql -U "${postgres_user}" -d "${postgres_db}"

echo "Restore completed from ${infile}"
