#!/usr/bin/env bash
set -euo pipefail

kalshi_env="production"
frequency="15m"
history_days="2"
spot_days="2"
replay_days="30"
out_dir="reports/crypto_live_path"
docker_container="${DOCKER_CONTAINER:-}"
assets=(BTC ETH SOL XRP BNB DOGE HYPE)

usage() {
  cat <<'USAGE'
usage: scripts/crypto_live_path_refresh.sh [options]

Options:
  --kalshi-env <demo|production>       Target Kalshi environment. Default: production.
  --frequency <frequency>              Crypto market frequency. Default: 15m.
  --history-days <days>                Kalshi history bootstrap window. Default: 2.
  --spot-days <days>                   Spot backfill window. Default: 2.
  --replay-days <days>                 Replay window. Default: 30.
  --assets <ASSET...>                  Assets to refresh. Default: BTC ETH SOL XRP BNB DOGE HYPE.
  --out-dir <path>                     Report output directory. Default: reports/crypto_live_path.
  --docker-container <name>            Run the CLI inside this Docker container.
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
    --spot-days)
      spot_days="${2:?missing value for --spot-days}"
      shift 2
      ;;
    --replay-days)
      replay_days="${2:?missing value for --replay-days}"
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
  cli=(docker exec -i "${docker_container}" python -m kalshi_bot.cli)
else
  read -r -a cli <<< "${CLI:-kalshi-bot-cli}"
fi

status=0
for asset in "${assets[@]}"; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  report="${out_dir}/${kalshi_env}_${frequency}_${asset}_refresh_${ts}.json"
  log="${out_dir}/${kalshi_env}_${frequency}_${asset}_refresh_${ts}.log"
  echo "refreshing ${asset}; report=${report} log=${log}" >&2
  if "${cli[@]}" crypto-live-path refresh \
    --kalshi-env "${kalshi_env}" \
    --frequency "${frequency}" \
    --history-days "${history_days}" \
    --spot-days "${spot_days}" \
    --replay-days "${replay_days}" \
    --assets "${asset}" \
    --json >"${report}" 2>"${log}"; then
    echo "completed ${asset}" >&2
  else
    rc=$?
    echo "failed ${asset} rc=${rc}" >&2
    status=1
  fi
done

exit "${status}"
