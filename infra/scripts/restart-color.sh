#!/usr/bin/env bash
set -euo pipefail

refresh_caddy=false
if [[ "${1:-}" == "--refresh-caddy" ]]; then
  refresh_caddy=true
  shift
fi

if [[ $# -eq 1 ]]; then
  target_env="all"
  color="${1:?usage: restart-color.sh [--refresh-caddy] [demo|production|all] <blue|green>}"
elif [[ $# -eq 2 ]]; then
  target_env="${1:?usage: restart-color.sh [--refresh-caddy] [demo|production|all] <blue|green>}"
  color="${2:?usage: restart-color.sh [--refresh-caddy] [demo|production|all] <blue|green>}"
else
  echo "usage: restart-color.sh [--refresh-caddy] [demo|production|all] <blue|green>" >&2
  exit 1
fi
compose_file="infra/docker-compose.yml"
# Docker Compose v2 uses the compose-file directory (infra/) as the project dir
# and looks for infra/.env for variable interpolation — not the root .env.
# Pass --env-file explicitly so root .env is used for ${VAR:-default} substitution.
compose_env_file="--env-file .env"

if [[ "${color}" != "blue" && "${color}" != "green" ]]; then
  echo "color must be blue or green" >&2
  exit 1
fi
if [[ "${target_env}" != "all" && "${target_env}" != "demo" && "${target_env}" != "production" ]]; then
  echo "env must be demo, production, or all" >&2
  exit 1
fi

docker compose -f "${compose_file}" ${compose_env_file} config >/dev/null

build_app_image() {
  docker compose -f "${compose_file}" ${compose_env_file} build migrate_demo >/dev/null
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

wait_for_services_health() {
  local timeout_seconds="$1"
  shift
  local service
  local -a pids=()
  local status=0
  for service in "$@"; do
    wait_for_service_health "${service}" "${timeout_seconds}" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    if ! wait "${pid}"; then
      status=1
    fi
  done
  return "${status}"
}

envs=("demo" "production")
if [[ "${target_env}" != "all" ]]; then
  envs=("${target_env}")
fi

build_app_image

for env_name in "${envs[@]}"; do
  app_service="app_${env_name}_${color}"
  daemon_service="daemon_${env_name}_${color}"
  runtime_services=("${app_service}" "${daemon_service}")
  if [[ "${env_name}" == "production" && "${ENABLE_CRYPTO_1H_DAEMON:-true}" == "true" ]]; then
    runtime_services+=("daemon_production_crypto_1h_${color}")
  fi
  docker compose -f "${compose_file}" ${compose_env_file} stop "${runtime_services[@]}" 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} rm -f "${runtime_services[@]}" 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} up -d --no-build --no-deps \
    "${runtime_services[@]}"
  wait_for_service_health "${app_service}" 180
done

infra/scripts/sync-web-color.sh "${target_env}"

if [[ "${refresh_caddy}" == "true" ]]; then
  # Stop and remove caddy explicitly before recreating to avoid removal-in-progress errors.
  docker compose -f "${compose_file}" ${compose_env_file} stop caddy 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} rm -f caddy 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} up -d --no-deps --force-recreate caddy
  wait_for_service_health caddy 90
fi

if [[ "${refresh_caddy}" == "true" ]]; then
  echo "Recreated ${target_env}/${color} runtime services, targeted site containers, and caddy"
else
  echo "Recreated ${target_env}/${color} runtime services and targeted site containers"
fi
