#!/usr/bin/env bash
set -euo pipefail

reason="${1:-systemd_boot}"
compose_file="infra/docker-compose.yml"
compose_env_file="--env-file .env"

build_migrate_image() {
  docker compose -f "${compose_file}" ${compose_env_file} build migrate >/dev/null
}

service_health() {
  local service="$1"
  local container_id
  container_id="$(docker compose -f "${compose_file}" ${compose_env_file} ps -q "${service}" 2>/dev/null || true)"
  if [[ -z "${container_id}" ]]; then
    printf '%s\n' "missing"
    return
  fi
  docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}" 2>/dev/null || printf '%s\n' "unknown"
}

wait_for_service_health() {
  local service="$1"
  local timeout_seconds="${2:-180}"
  local waited=0
  local status
  while (( waited < timeout_seconds )); do
    status="$(service_health "${service}")"
    if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  echo "Timed out waiting for ${service} to become healthy" >&2
  docker compose -f "${compose_file}" ${compose_env_file} ps "${service}" >&2 || true
  return 1
}

run_migrate() {
  build_migrate_image
  docker compose -f "${compose_file}" ${compose_env_file} run --rm --no-deps migrate "$@"
}

run_control() {
  local -a cmd=("$@")
  if [[ -n "$(docker compose -f "${compose_file}" ${compose_env_file} ps --status running -q app_blue 2>/dev/null || true)" ]]; then
    docker compose -f "${compose_file}" ${compose_env_file} exec -T app_blue "${cmd[@]}"
    return
  fi
  if [[ -n "$(docker compose -f "${compose_file}" ${compose_env_file} ps --status running -q app_green 2>/dev/null || true)" ]]; then
    docker compose -f "${compose_file}" ${compose_env_file} exec -T app_green "${cmd[@]}"
    return
  fi
  run_migrate "${cmd[@]}"
}

docker compose -f "${compose_file}" ${compose_env_file} config >/dev/null
docker compose -f "${compose_file}" ${compose_env_file} up -d postgres
run_migrate
docker compose -f "${compose_file}" ${compose_env_file} up -d --build app_blue app_green daemon_blue daemon_green
wait_for_service_health app_blue 180
wait_for_service_health app_green 180
docker compose -f "${compose_file}" ${compose_env_file} up -d --force-recreate nginx
wait_for_service_health nginx 90

run_control python -m kalshi_bot.cli watchdog mark-boot --status success --reason "${reason}"

echo "Started Kalshi Bot stack (${reason})"
