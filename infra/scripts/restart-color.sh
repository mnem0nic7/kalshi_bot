#!/usr/bin/env bash
set -euo pipefail

color="${1:?usage: restart-color.sh <blue|green>}"
compose_file="infra/docker-compose.yml"

if [[ "${color}" != "blue" && "${color}" != "green" ]]; then
  echo "color must be blue or green" >&2
  exit 1
fi

service_health() {
  local service="$1"
  local container_id
  container_id="$(docker compose -f "${compose_file}" ps -q "${service}" 2>/dev/null || true)"
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
  docker compose -f "${compose_file}" ps "${service}" >&2 || true
  return 1
}

docker compose -f "${compose_file}" up -d --no-deps --force-recreate "app_${color}" "daemon_${color}"
wait_for_service_health "app_${color}" 180
docker compose -f "${compose_file}" up -d --no-deps --force-recreate nginx
wait_for_service_health nginx 90

echo "Recreated ${color} and refreshed nginx"
