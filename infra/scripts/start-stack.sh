#!/usr/bin/env bash
set -euo pipefail

reason="${1:-systemd_boot}"
compose_file="infra/docker-compose.yml"
compose_env_file="--env-file .env"

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

run_migrate() {
  local env_name="$1"
  shift
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
build_app_image
docker compose -f "${compose_file}" ${compose_env_file} up -d postgres_demo postgres_production
wait_for_services_health 60 postgres_demo postgres_production
run_migrate demo &
demo_migrate_pid="$!"
run_migrate production &
production_migrate_pid="$!"
migrate_status=0
if ! wait "${demo_migrate_pid}"; then
  migrate_status=1
fi
if ! wait "${production_migrate_pid}"; then
  migrate_status=1
fi
if [[ "${migrate_status}" -ne 0 ]]; then
  exit "${migrate_status}"
fi
production_active_color="$(active_color_for_env production 2>/dev/null || true)"
if [[ "${production_active_color}" != "blue" && "${production_active_color}" != "green" ]]; then
  production_active_color="blue"
fi
export CRYPTO_CURRENT_APP_COLOR="${production_active_color}"
export CRYPTO_1H_CURRENT_APP_COLOR="${production_active_color}"
export CRYPTO_BTC15M_TOUCH20_APP_COLOR="${production_active_color}"
export CRYPTO_1H_TOUCH20_APP_COLOR="${production_active_color}"
runtime_services=(
  app_demo_blue app_demo_green
  app_production_blue app_production_green
)
if [[ "${ENABLE_DEMO_DAEMON:-true}" == "true" ]]; then
  runtime_services+=(daemon_demo_blue daemon_demo_green)
fi
if [[ "${ENABLE_PRODUCTION_DAEMON:-true}" == "true" ]]; then
  runtime_services+=(daemon_production_blue daemon_production_green)
fi
if [[ "${ENABLE_CRYPTO_1H_CONTAINER:-false}" == "true" ]]; then
  runtime_services+=(crypto_1h_production)
fi
if [[ "${ENABLE_CRYPTO_1H_DAEMON:-false}" == "true" ]]; then
  runtime_services+=(daemon_production_crypto_1h_blue daemon_production_crypto_1h_green)
fi
if [[ "${ENABLE_CRYPTO_CURRENT_CONTAINER:-true}" == "true" ]]; then
  runtime_services+=(crypto_current_production)
fi
if [[ "${ENABLE_CRYPTO_CURRENT_1H_CONTAINER:-false}" == "true" ]]; then
  runtime_services+=(crypto_current_1h_production)
fi
if [[ "${ENABLE_BTC15M_TOUCH20_CONTAINER:-true}" == "true" ]]; then
  runtime_services+=(crypto_non_model_btc15m_touch20_production)
fi
if [[ "${ENABLE_CRYPTO_1H_TOUCH20_CONTAINER:-false}" == "true" ]]; then
  runtime_services+=(crypto_non_model_1h_touch20_production)
fi
docker compose -f "${compose_file}" ${compose_env_file} up -d --no-build \
  "${runtime_services[@]}"
wait_for_services_health 180 \
  app_demo_blue app_demo_green app_production_blue app_production_green
if [[ "${ENABLE_CRYPTO_CURRENT_CONTAINER:-true}" == "true" ]]; then
  wait_for_service_health crypto_current_production 60
fi
if [[ "${ENABLE_CRYPTO_CURRENT_1H_CONTAINER:-false}" == "true" ]]; then
  wait_for_service_health crypto_current_1h_production 60
fi
if [[ "${ENABLE_BTC15M_TOUCH20_CONTAINER:-true}" == "true" ]]; then
  wait_for_service_health crypto_non_model_btc15m_touch20_production 60
fi
if [[ "${ENABLE_CRYPTO_1H_TOUCH20_CONTAINER:-false}" == "true" ]]; then
  wait_for_service_health crypto_non_model_1h_touch20_production 60
fi
infra/scripts/sync-web-color.sh all
# Stop and remove caddy explicitly before recreating to avoid Docker compose
# removal-in-progress races during full machine recovery.
docker compose -f "${compose_file}" ${compose_env_file} stop caddy 2>/dev/null || true
docker compose -f "${compose_file}" ${compose_env_file} rm -f caddy 2>/dev/null || true
docker compose -f "${compose_file}" ${compose_env_file} up -d --no-deps --force-recreate caddy
wait_for_service_health caddy 90

run_control demo python -m kalshi_bot.cli watchdog mark-boot --status success --reason "${reason}"
run_control production python -m kalshi_bot.cli watchdog mark-boot --status success --reason "${reason}"

echo "Started Kalshi Bot stack (${reason})"
