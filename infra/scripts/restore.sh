#!/usr/bin/env bash
set -euo pipefail

infile="${1:?usage: restore.sh <dump.sql.gz>}"

gunzip -c "${infile}" | docker compose -f infra/docker-compose.yml exec -T postgres \
  psql -U postgres -d kalshi_bot

echo "Restore completed from ${infile}"

