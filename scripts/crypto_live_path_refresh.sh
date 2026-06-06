#!/usr/bin/env bash
set -euo pipefail

kalshi_env="production"
frequency="15m"
history_days="2"
settled_days="2"
spot_days="2"
replay_days="30"
replay_limit=""
out_dir="reports/crypto_live_path"
docker_container="${DOCKER_CONTAINER:-}"
docker_env=()
static_assets=(BTC ETH SOL XRP BNB DOGE HYPE)
assets=()
discover_assets=true

usage() {
  cat <<'USAGE'
usage: scripts/crypto_live_path_refresh.sh [options]

Options:
  --kalshi-env <demo|production>       Target Kalshi environment. Default: production.
  --frequency <frequency>              Crypto market frequency. Default: 15m.
  --settled-days <days>                Settled market label backfill window. Default: 2.
  --history-days <days>                Kalshi history bootstrap window. Default: 2.
  --spot-days <days>                   Spot backfill window. Default: 2.
  --replay-days <days>                 Replay window. Default: 30.
  --replay-limit <rows>                Limit replay to the most recent rows. Default: unbounded.
  --assets <ASSET...>                  Assets to refresh. Omit or use "all" to discover current assets.
  --out-dir <path>                     Report output directory. Default: reports/crypto_live_path.
  --docker-container <name>            Run the CLI inside this Docker container.
  --docker-env <KEY=VALUE>             Add an environment variable to docker exec. Repeatable.
  -h, --help                           Show this help.

Set CLI="..." to override the local command when --docker-container is not used.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kalshi-env)
      kalshi_env="${2:?missing value for --kalshi-env}"
      shift 2
      ;;
    --frequency)
      frequency="${2:?missing value for --frequency}"
      shift 2
      ;;
    --history-days)
      history_days="${2:?missing value for --history-days}"
      shift 2
      ;;
    --settled-days)
      settled_days="${2:?missing value for --settled-days}"
      shift 2
      ;;
    --spot-days)
      spot_days="${2:?missing value for --spot-days}"
      shift 2
      ;;
    --replay-days)
      replay_days="${2:?missing value for --replay-days}"
      shift 2
      ;;
    --replay-limit)
      replay_limit="${2:?missing value for --replay-limit}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --docker-container)
      docker_container="${2:?missing value for --docker-container}"
      shift 2
      ;;
    --docker-env)
      docker_env+=("${2:?missing value for --docker-env}")
      shift 2
      ;;
    --assets)
      shift
      assets=()
      while [[ $# -gt 0 && "${1:0:1}" != "-" ]]; do
        assets+=("$1")
        shift
      done
      if [[ "${#assets[@]}" -eq 0 ]]; then
        echo "--assets requires at least one asset" >&2
        exit 2
      fi
      discover_assets=false
      for asset in "${assets[@]}"; do
        normalized="$(printf '%s' "${asset}" | tr '[:lower:]' '[:upper:]')"
        if [[ "${normalized}" == "ALL" || "${normalized}" == "*" ]]; then
          assets=()
          discover_assets=true
          break
        fi
      done
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "${out_dir}"

if [[ -n "${docker_container}" ]]; then
  docker_exec_env=()
  for env_pair in "${docker_env[@]}"; do
    docker_exec_env+=(-e "${env_pair}")
  done
  cli=(docker exec -i "${docker_exec_env[@]}" "${docker_container}" python -m kalshi_bot.cli)
else
  read -r -a cli <<< "${CLI:-kalshi-bot-cli}"
fi

discover_current_assets() {
  "${PYTHON:-python3}" -c '
import json
import sys

text = sys.stdin.read()
decoder = json.JSONDecoder()
idx = 0
found = None
while idx < len(text):
    while idx < len(text) and text[idx].isspace():
        idx += 1
    if idx >= len(text):
        break
    try:
        obj, end = decoder.raw_decode(text, idx)
    except json.JSONDecodeError:
        idx += 1
        continue
    if isinstance(obj, dict) and isinstance(obj.get("modes"), dict):
        found = obj
    idx = end
if found is None:
    raise SystemExit("asset discovery did not return modes")
print(" ".join(sorted(str(asset).upper() for asset in found["modes"].keys() if str(asset).strip())))
'
}

if [[ "${discover_assets}" == "true" ]]; then
  discovery_log="${out_dir}/${kalshi_env}_${frequency}_asset_discovery.log"
  if discovery_output="$("${cli[@]}" crypto-asset-mode list --kalshi-env "${kalshi_env}" --frequency "${frequency}" 2>"${discovery_log}")" \
    && discovered_text="$(printf '%s' "${discovery_output}" | discover_current_assets)" \
    && [[ -n "${discovered_text}" ]]; then
    read -r -a assets <<< "${discovered_text}"
    echo "discovered ${#assets[@]} ${frequency} asset(s): ${assets[*]}" >&2
  else
    assets=("${static_assets[@]}")
    echo "asset discovery failed or returned no assets; falling back to: ${assets[*]}" >&2
  fi
fi

status=0
for asset in "${assets[@]}"; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  report="${out_dir}/${kalshi_env}_${frequency}_${asset}_refresh_${ts}.json"
  log="${out_dir}/${kalshi_env}_${frequency}_${asset}_refresh_${ts}.log"
  echo "refreshing ${asset}; report=${report} log=${log}" >&2
  replay_args=()
  if [[ -n "${replay_limit}" ]]; then
    replay_args+=(--replay-limit "${replay_limit}")
  fi
  if "${cli[@]}" crypto-live-path refresh \
    --kalshi-env "${kalshi_env}" \
    --frequency "${frequency}" \
    --settled-days "${settled_days}" \
    --history-days "${history_days}" \
    --spot-days "${spot_days}" \
    --replay-days "${replay_days}" \
    "${replay_args[@]}" \
    --assets "${asset}" \
    --skip-growth \
    --json >"${report}" 2>"${log}"; then
    echo "completed ${asset}" >&2
  else
    rc=$?
    echo "failed ${asset} rc=${rc}" >&2
    status=1
  fi
done

exit "${status}"
