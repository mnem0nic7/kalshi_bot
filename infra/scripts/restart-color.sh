#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 1 ]]; then
  target_env="all"
  color="${1:?usage: restart-color.sh [demo|production] <blue|green>}"
elif [[ $# -eq 2 ]]; then
  target_env="${1:?usage: restart-color.sh [demo|production] <blue|green>}"
  color="${2:?usage: restart-color.sh [demo|production] <blue|green>}"
else
  echo "usage: restart-color.sh [demo|production] <blue|green>" >&2
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
  echo "env must be demo or production" >&2
  exit 1
fi

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

envs=("demo" "production")
if [[ "${target_env}" != "all" ]]; then
  envs=("${target_env}")
fi

for env_name in "${envs[@]}"; do
  app_service="app_${env_name}_${color}"
  daemon_service="daemon_${env_name}_${color}"
  docker compose -f "${compose_file}" ${compose_env_file} stop "${app_service}" "${daemon_service}" 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} rm -f "${app_service}" "${daemon_service}" 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} up -d --build --no-deps \
    "${app_service}" "${daemon_service}"
  wait_for_service_health "${app_service}" 180
done

docker compose -f "${compose_file}" ${compose_env_file} up -d --build --no-deps \
  web_demo web_production web_strategies
wait_for_service_health web_demo 180
wait_for_service_health web_production 180
wait_for_service_health web_strategies 180

# Stop and remove caddy explicitly before recreating to avoid removal-in-progress errors
docker compose -f "${compose_file}" ${compose_env_file} stop caddy 2>/dev/null || true
docker compose -f "${compose_file}" ${compose_env_file} rm -f caddy 2>/dev/null || true
docker compose -f "${compose_file}" ${compose_env_file} up -d --no-deps --force-recreate caddy
wait_for_service_health caddy 90

echo "Recreated ${target_env}/${color} runtime services and refreshed caddy plus site containers"
