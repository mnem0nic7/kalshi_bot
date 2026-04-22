#!/usr/bin/env bash
set -euo pipefail

reason="${1:-systemd_boot}"
compose_file="infra/docker-compose.yml"
compose_env_file="--env-file .env"

build_migrate_image() {
  local env_name="$1"
  docker compose -f "${compose_file}" ${compose_env_file} build "migrate_${env_name}" >/dev/null
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
  local env_name="$1"
  shift
  build_migrate_image "${env_name}"
  docker compose -f "${compose_file}" ${compose_env_file} run --rm --no-deps "migrate_${env_name}" "$@"
}

run_control() {
  local env_name="$1"
  shift
  local -a cmd=("$@")
  local primary_service="app_${env_name}_blue"
  local secondary_service="app_${env_name}_green"
  if [[ -n "$(docker compose -f "${compose_file}" ${compose_env_file} ps --status running -q "${primary_service}" 2>/dev/null || true)" ]]; then
    docker compose -f "${compose_file}" ${compose_env_file} exec -T "${primary_service}" "${cmd[@]}"
    return
  fi
  if [[ -n "$(docker compose -f "${compose_file}" ${compose_env_file} ps --status running -q "${secondary_service}" 2>/dev/null || true)" ]]; then
    docker compose -f "${compose_file}" ${compose_env_file} exec -T "${secondary_service}" "${cmd[@]}"
    return
  fi
  run_migrate "${env_name}" "${cmd[@]}"
}

docker compose -f "${compose_file}" ${compose_env_file} config >/dev/null
docker compose -f "${compose_file}" ${compose_env_file} up -d postgres_demo postgres_production
wait_for_service_health postgres_demo 60
wait_for_service_health postgres_production 60
run_migrate demo
run_migrate production
docker compose -f "${compose_file}" ${compose_env_file} up -d --build \
  app_demo_blue app_demo_green daemon_demo_blue daemon_demo_green \
  app_production_blue app_production_green daemon_production_blue daemon_production_green \
  web_demo web_production web_strategies
wait_for_service_health app_demo_blue 180
wait_for_service_health app_demo_green 180
wait_for_service_health app_production_blue 180
wait_for_service_health app_production_green 180
wait_for_service_health web_demo 180
wait_for_service_health web_production 180
wait_for_service_health web_strategies 180
docker compose -f "${compose_file}" ${compose_env_file} up -d --force-recreate caddy
wait_for_service_health caddy 90

run_control demo python -m kalshi_bot.cli watchdog mark-boot --status success --reason "${reason}"
run_control production python -m kalshi_bot.cli watchdog mark-boot --status success --reason "${reason}"

echo "Started Kalshi Bot stack (${reason})"
