#!/usr/bin/env bash
set -euo pipefail

target_env="${1:-all}"
compose_file="infra/docker-compose.yml"
compose_env_file="--env-file .env"

if [[ "${target_env}" != "all" && "${target_env}" != "demo" && "${target_env}" != "production" ]]; then
  echo "env must be demo, production, or all" >&2
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
  docker compose -f "${compose_file}" ${compose_env_file} run --rm --no-deps "migrate_${env_name}" "${cmd[@]}"
}

active_color_for_env() {
  local env_name="$1"
  run_control "${env_name}" python -m kalshi_bot.cli status \
    | python3 -c 'import json, sys
payload = json.load(sys.stdin)
color = payload.get("active_color")
if color not in {"blue", "green"}:
    raise SystemExit(f"unexpected active_color: {color!r}")
print(color)'
}

docker compose -f "${compose_file}" ${compose_env_file} config >/dev/null

demo_color=""
production_color=""
if [[ "${target_env}" == "all" || "${target_env}" == "demo" ]]; then
  demo_color="$(active_color_for_env demo)"
fi
if [[ "${target_env}" == "all" || "${target_env}" == "production" ]]; then
  production_color="$(active_color_for_env production)"
fi
if [[ -z "${demo_color}" ]]; then
  demo_color="$(active_color_for_env demo 2>/dev/null || true)"
fi
if [[ -z "${demo_color}" ]]; then
  demo_color="${production_color:-${WEB_APP_COLOR:-blue}}"
fi
if [[ -z "${production_color}" ]]; then
  production_color="${demo_color:-${WEB_APP_COLOR:-blue}}"
fi

export WEB_DEMO_APP_COLOR="${demo_color}"
export WEB_STRATEGIES_APP_COLOR="${demo_color}"
export WEB_PRODUCTION_APP_COLOR="${production_color}"

web_services=()
if [[ "${target_env}" == "all" || "${target_env}" == "demo" ]]; then
  web_services+=("web_demo")
fi
if [[ "${target_env}" == "all" || "${target_env}" == "production" ]]; then
  web_services+=("web_production")
fi
if [[ "${ENABLE_WEB_STRATEGIES_CONTAINER:-true}" == "true" ]]; then
  web_services+=("web_strategies")
else
  docker compose -f "${compose_file}" ${compose_env_file} stop web_strategies 2>/dev/null || true
  docker compose -f "${compose_file}" ${compose_env_file} rm -f web_strategies 2>/dev/null || true
fi

docker compose -f "${compose_file}" ${compose_env_file} up -d --no-build --no-deps --force-recreate \
  "${web_services[@]}"
wait_for_services_health 180 "${web_services[@]}"

echo "Synced web app colors: demo=${WEB_DEMO_APP_COLOR}, production=${WEB_PRODUCTION_APP_COLOR}, strategies=${WEB_STRATEGIES_APP_COLOR}"
