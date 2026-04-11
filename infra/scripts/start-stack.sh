#!/usr/bin/env bash
set -euo pipefail

reason="${1:-systemd_boot}"
compose_file="infra/docker-compose.yml"

build_migrate_image() {
  docker compose -f "${compose_file}" build migrate >/dev/null
}

run_migrate() {
  build_migrate_image
  docker compose -f "${compose_file}" run --rm --no-deps migrate "$@"
}

run_control() {
  local -a cmd=("$@")
  if [[ -n "$(docker compose -f "${compose_file}" ps --status running -q app_blue 2>/dev/null || true)" ]]; then
    docker compose -f "${compose_file}" exec -T app_blue "${cmd[@]}"
    return
  fi
  if [[ -n "$(docker compose -f "${compose_file}" ps --status running -q app_green 2>/dev/null || true)" ]]; then
    docker compose -f "${compose_file}" exec -T app_green "${cmd[@]}"
    return
  fi
  run_migrate "${cmd[@]}"
}

docker compose -f "${compose_file}" config >/dev/null
docker compose -f "${compose_file}" up -d postgres
run_migrate
docker compose -f "${compose_file}" up -d --build app_blue app_green daemon_blue daemon_green nginx

run_control python -m kalshi_bot.cli watchdog mark-boot --status success --reason "${reason}"

echo "Started Kalshi Bot stack (${reason})"
