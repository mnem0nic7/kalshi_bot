#!/usr/bin/env bash
set -euo pipefail

compose_file="infra/docker-compose.yml"

container_status() {
  local service="$1"
  local container_id
  container_id="$(docker compose -f "${compose_file}" ps -q "${service}" 2>/dev/null || true)"
  if [[ -z "${container_id}" ]]; then
    printf '%s\n' "missing"
    return
  fi
  docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}" 2>/dev/null || printf '%s\n' "unknown"
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
  docker compose -f "${compose_file}" run --rm --no-deps migrate "${cmd[@]}"
}

record_action() {
  local action="$1"
  local outcome="$2"
  local reason="$3"
  local target="${4:-}"
  local failed="${5:-}"
  local -a cmd=(python -m kalshi_bot.cli watchdog record-action --action "${action}" --outcome "${outcome}" --reason "${reason}")
  if [[ -n "${target}" ]]; then
    cmd+=(--target-color "${target}")
  fi
  if [[ -n "${failed}" ]]; then
    cmd+=(--failed-color "${failed}")
  fi
  run_control "${cmd[@]}" >/dev/null
}

execute_plan() {
  local json="$1"
  local action target failed reason wait_seconds
  action="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["action"])' <<<"${json}")"
  target="$(python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("target_color") or "")' <<<"${json}")"
  failed="$(python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("failed_color") or "")' <<<"${json}")"
  reason="$(python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("reason") or "")' <<<"${json}")"
  wait_seconds="$(python3 -c 'import json,sys; data=json.load(sys.stdin); print(int(data.get("wait_seconds") or 0))' <<<"${json}")"

  case "${action}" in
    none)
      printf '%s\n' "${json}"
      return 0
      ;;
    restart_color)
      if docker compose -f "${compose_file}" restart "app_${target}" "daemon_${target}"; then
        record_action "restart_color" "succeeded" "${reason}" "${target}" "${failed}"
      else
        record_action "restart_color" "failed" "${reason}" "${target}" "${failed}"
        return 1
      fi
      if [[ "${wait_seconds}" -gt 0 ]]; then
        sleep "${wait_seconds}"
        local app_blue_status app_green_status followup
        app_blue_status="$(container_status app_blue)"
        app_green_status="$(container_status app_green)"
        followup="$(run_control python -m kalshi_bot.cli watchdog run-once --app-blue-status "${app_blue_status}" --app-green-status "${app_green_status}" --source watchdog_recheck)"
        execute_plan "${followup}"
        return $?
      fi
      ;;
    failover)
      if docker compose -f "${compose_file}" restart "app_${failed}" "daemon_${failed}"; then
        record_action "failover" "succeeded" "${reason}" "${target}" "${failed}"
      else
        record_action "failover" "failed" "${reason}" "${target}" "${failed}"
        return 1
      fi
      ;;
    restart_stack)
      if ./infra/scripts/start-stack.sh watchdog_restart_stack; then
        record_action "restart_stack" "succeeded" "${reason}" "${target}" "${failed}"
      else
        record_action "restart_stack" "failed" "${reason}" "${target}" "${failed}"
        return 1
      fi
      ;;
    *)
      echo "Unknown watchdog action: ${action}" >&2
      return 1
      ;;
  esac

  printf '%s\n' "${json}"
}

app_blue_status="$(container_status app_blue)"
app_green_status="$(container_status app_green)"
plan_json="$(run_control python -m kalshi_bot.cli watchdog run-once --app-blue-status "${app_blue_status}" --app-green-status "${app_green_status}" --source watchdog_timer)"
execute_plan "${plan_json}"
