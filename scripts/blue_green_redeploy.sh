#!/usr/bin/env bash
# Zero-downtime blue/green redeploy for the kalshi_bot stack.
#
# Strategy: build the new image, recreate ONLY the inactive color's containers
# (the active color keeps trading untouched), wait for them to report healthy,
# then hand off the DB-backed execution lock with `kalshi-bot-cli promote`.
#
# The active color is the single source of truth in the deployment_control row;
# `promote <color>` is just a DB update, so the handoff is instant and the old
# color can be left running (or recreated afterward) without touching live trading.
#
# Usage:
#   scripts/blue_green_redeploy.sh [--env production|demo] [--target blue|green]
#                                  [--no-build] [--yes] [--recreate-old] [--dry-run]
#
#   --env           Kalshi env to deploy (default: production)
#   --target        Force the target (incoming) color; default = opposite of active
#   --no-build      Skip the image rebuild (reuse the current kalshi-bot:local)
#   --yes           Don't prompt before the promote (lock handoff)
#   --recreate-old  After promoting, also recreate the now-inactive old color
#   --dry-run       Print what would happen; make no changes
#
# Requires: docker compose, and the .env at repo root (passed via --env-file).
set -euo pipefail

# --- locate repo + compose -------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/infra/docker-compose.yml"
ENV_FILE="${REPO_ROOT}/.env"
PROJECT="infra"   # compose project name (containers are ${PROJECT}-<service>-1)

ENV="production"
FORCE_TARGET=""
DO_BUILD=1
ASSUME_YES=0
RECREATE_OLD=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)          ENV="$2"; shift 2 ;;
    --target)       FORCE_TARGET="$2"; shift 2 ;;
    --no-build)     DO_BUILD=0; shift ;;
    --yes)          ASSUME_YES=1; shift ;;
    --recreate-old) RECREATE_OLD=1; shift ;;
    --dry-run)      DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

compose() { docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"; }
container() { echo "${PROJECT}-$1-1"; }
log() { echo -e "\033[1;36m[blue-green]\033[0m $*"; }
run() { if [[ "${DRY_RUN}" == 1 ]]; then echo "  DRY-RUN> $*"; else eval "$*"; fi; }

# services that participate in the color flip (per-color)
color_services() {
  local color="$1"
  local services=("app_${ENV}_${color}")
  if [[ "${ENV}" == "production" ]]; then
    if [[ "${ENABLE_PRODUCTION_DAEMON:-true}" == "true" ]]; then
      services+=("daemon_${ENV}_${color}")
    fi
    if [[ "${ENABLE_CRYPTO_1H_DAEMON:-false}" == "true" ]]; then
      services+=("daemon_${ENV}_crypto_1h_${color}")
    fi
  else
    if [[ "${ENABLE_DEMO_DAEMON:-true}" == "true" ]]; then
      services+=("daemon_${ENV}_${color}")
    fi
  fi
  echo "${services[*]}"
}

# --- discover the active color from the deployment_control row -------------
# status reads the DB, so any running app container of this env can answer.
read_active_color() {
  local c
  for color in blue green; do
    c="$(container "app_${ENV}_${color}")"
    if docker inspect "${c}" >/dev/null 2>&1; then
      docker exec "${c}" kalshi-bot-cli status 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['active_color'])" 2>/dev/null && return 0
    fi
  done
  return 1
}

log "env=${ENV}  compose=${COMPOSE_FILE}"
ACTIVE="$(read_active_color || true)"
if [[ -z "${ACTIVE}" ]]; then
  echo "ERROR: could not read active color (is an app_${ENV}_* container running?)" >&2
  exit 1
fi

if [[ -n "${FORCE_TARGET}" ]]; then
  TARGET="${FORCE_TARGET}"
else
  TARGET=$([[ "${ACTIVE}" == "blue" ]] && echo green || echo blue)
fi

if [[ "${TARGET}" == "${ACTIVE}" ]]; then
  echo "ERROR: target color (${TARGET}) is already the active color; nothing to flip." >&2
  exit 1
fi
log "active=${ACTIVE}  ->  deploying onto inactive target=${TARGET}"

# --- 1. build --------------------------------------------------------------
if [[ "${DO_BUILD}" == 1 ]]; then
  log "building image (kalshi-bot:local)"
  run "COMPOSE_BAKE=false compose build app_${ENV}_${TARGET}"
else
  log "skipping build (--no-build)"
fi

# --- 2. recreate the inactive color only -----------------------------------
TARGET_SVCS="$(color_services "${TARGET}")"
log "recreating ${TARGET} color: ${TARGET_SVCS}"
run "compose up -d --no-deps --force-recreate ${TARGET_SVCS}"

# --- 3. wait for the inactive color to report healthy ----------------------
log "waiting for ${TARGET} containers to become healthy (timeout 180s)"
if [[ "${DRY_RUN}" != 1 ]]; then
  deadline=$(( $(date +%s) + 180 ))
  for svc in ${TARGET_SVCS}; do
    c="$(container "${svc}")"
    while :; do
      h="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${c}" 2>/dev/null || echo missing)"
      [[ "${h}" == "healthy" || "${h}" == "none" ]] && { log "  ${svc}: ${h}"; break; }
      if [[ $(date +%s) -ge ${deadline} ]]; then
        echo "ERROR: ${svc} did not become healthy (last=${h}). Aborting BEFORE promote." >&2
        echo "       Active color (${ACTIVE}) is untouched and still serving." >&2
        exit 1
      fi
      sleep 3
    done
  done
fi

# --- 4. confirm + flip the execution lock ----------------------------------
if [[ "${ASSUME_YES}" != 1 && "${DRY_RUN}" != 1 ]]; then
  read -r -p "Promote ${TARGET} to active (hand off execution lock from ${ACTIVE})? [y/N] " ans
  [[ "${ans}" == "y" || "${ans}" == "Y" ]] || { echo "Aborted before promote; ${ACTIVE} still active."; exit 0; }
fi
log "promoting ${TARGET} -> active"
run "docker exec $(container "app_${ENV}_${TARGET}") kalshi-bot-cli promote ${TARGET}"
log "verifying"
run "docker exec $(container "app_${ENV}_${TARGET}") kalshi-bot-cli status | python3 -c \"import sys,json; d=json.load(sys.stdin); print('active_color=%s kill_switch=%s lock=%s' % (d['active_color'], d['kill_switch_enabled'], d['execution_lock_holder']))\""

if [[ "${ENV}" == "production" ]]; then
  export CRYPTO_CURRENT_APP_COLOR="${TARGET}"
  export CRYPTO_1H_CURRENT_APP_COLOR="${TARGET}"
  export CRYPTO_BTC15M_TOUCH20_APP_COLOR="${TARGET}"
  export CRYPTO_1H_TOUCH20_APP_COLOR="${TARGET}"
fi

if [[ "${ENV}" == "production" && "${ENABLE_CRYPTO_CURRENT_CONTAINER:-true}" == "true" ]]; then
  log "recreating production crypto current collector"
  run "compose up -d --no-deps --force-recreate crypto_current_production"
fi
if [[ "${ENV}" == "production" && "${ENABLE_CRYPTO_CURRENT_1H_CONTAINER:-false}" == "true" ]]; then
  log "recreating production 1h crypto current collector"
  run "compose up -d --no-deps --force-recreate crypto_current_1h_production"
fi
if [[ "${ENV}" == "production" && "${ENABLE_CRYPTO_1H_TOUCH20_CONTAINER:-false}" == "true" ]]; then
  log "recreating production 1h non-model touch20 worker"
  run "compose up -d --no-deps --force-recreate crypto_non_model_1h_touch20_production"
fi

# --- 5. optionally recreate the now-idle old color -------------------------
if [[ "${RECREATE_OLD}" == 1 ]]; then
  OLD_SVCS="$(color_services "${ACTIVE}")"
  log "recreating now-inactive old color (${ACTIVE}): ${OLD_SVCS}"
  run "compose up -d --no-deps --force-recreate ${OLD_SVCS}"
fi

log "done. active color is now ${TARGET}."
log "NOTE: singleton services (crypto_1h_${ENV}, web_${ENV}) are NOT color-scoped;"
log "      recreate them separately if a deploy changed their behavior."
