#!/usr/bin/env bash
set -euo pipefail

color="${1:?usage: restart-color.sh <blue|green>}"

if [[ "${color}" != "blue" && "${color}" != "green" ]]; then
  echo "color must be blue or green" >&2
  exit 1
fi

docker compose -f infra/docker-compose.yml up -d --no-deps --force-recreate "app_${color}" "daemon_${color}"

echo "Recreated ${color}"
