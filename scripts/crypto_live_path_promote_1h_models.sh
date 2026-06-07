#!/usr/bin/env bash
set -euo pipefail

kalshi_env="production"
frequency="1h"
assets=(XRP BNB SOL ETH DOGE HYPE)
settled_days="2"
history_days="2"
spot_days="2"
replay_days="30"
replay_limit="${REPLAY_LIMIT:-50000}"
train_max_snapshots="${TRAIN_MAX_SNAPSHOTS:-50000}"
out_dir="reports/crypto_live_path"
docker_container="${DOCKER_CONTAINER:-}"
dry_run=false
skip_refresh=false
skip_promote=false

usage() {
  cat <<'USAGE'
usage: scripts/crypto_live_path_promote_1h_models.sh [options]

Promote the model-based 1h crypto path for the remaining assets after BTC.
By default this runs XRP BNB SOL ETH DOGE HYPE one asset at a time using the
same bounded feature-store proof used for BTC 1h.

Options:
  --kalshi-env <demo|production>       Target Kalshi environment. Default: production.
  --assets <ASSET...>                  Assets to promote. Default: XRP BNB SOL ETH DOGE HYPE.
  --settled-days <days>                Settled label window. Default: 2.
  --history-days <days>                History bootstrap window. Default: 2.
  --spot-days <days>                   Spot backfill window. Default: 2.
  --replay-days <days>                 Replay window. Default: 30.
  --replay-limit <rows>                Replay row cap. Default: 50000.
  --train-max-snapshots <rows>         Training snapshot cap. Default: 50000.
  --out-dir <path>                     Report output directory. Default: reports/crypto_live_path.
  --docker-container <name>            Run the CLI inside this Docker container.
  --skip-refresh                       Verify/promote from existing artifacts only.
  --skip-promote                       Do not set asset mode live.
  --dry-run                            Print commands without executing them.
  -h, --help                           Show this help.

Set CLI="..." to override the local command used by the wrappers when
--docker-container is not used.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kalshi-env)
      kalshi_env="${2:?missing value for --kalshi-env}"
      shift 2
      ;;
    --assets)
      shift
      assets=()
      while [[ $# -gt 0 && "${1:0:1}" != "-" ]]; do
        assets+=("$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')")
        shift
      done
      if [[ "${#assets[@]}" -eq 0 ]]; then
        echo "--assets requires at least one asset" >&2
        exit 2
      fi
      ;;
    --settled-days)
      settled_days="${2:?missing value for --settled-days}"
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
    --replay-limit)
      replay_limit="${2:?missing value for --replay-limit}"
      shift 2
      ;;
    --train-max-snapshots)
      train_max_snapshots="${2:?missing value for --train-max-snapshots}"
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
    --skip-refresh)
      skip_refresh=true
      shift
      ;;
    --skip-promote)
      skip_promote=true
      shift
      ;;
    --dry-run)
      dry_run=true
      shift
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

if [[ "${frequency}" != "1h" ]]; then
  echo "this helper is intentionally limited to 1h model promotions" >&2
  exit 2
fi

mkdir -p "${out_dir}/status"

run_cmd() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  if [[ "${dry_run}" == "true" ]]; then
    return 0
  fi
  "$@"
}

wrapper_container_args=()
cli_container_args=()
if [[ -n "${docker_container}" ]]; then
  wrapper_container_args=(--docker-container "${docker_container}")
  cli_container_args=(docker exec -i "${docker_container}" python -m kalshi_bot.cli)
else
  read -r -a cli_container_args <<< "${CLI:-kalshi-bot-cli}"
fi

status=0
for asset in "${assets[@]}"; do
  echo "=== ${asset} ${frequency} model promotion started at $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" >&2

  if [[ "${skip_refresh}" != "true" ]]; then
    if ! run_cmd scripts/crypto_live_path_refresh.sh \
      --kalshi-env "${kalshi_env}" \
      --frequency "${frequency}" \
      --settled-days "${settled_days}" \
      --history-days "${history_days}" \
      --spot-days "${spot_days}" \
      --replay-days "${replay_days}" \
      --replay-limit "${replay_limit}" \
      --assets "${asset}" \
      --out-dir "${out_dir}" \
      --docker-env CRYPTO_COLLECT_SETTLED_CANDLES_ENABLED=false \
      --docker-env CRYPTO_HISTORY_CANDLE_CONCURRENCY=1 \
      --docker-env CRYPTO_TRAIN_MAX_SNAPSHOTS="${train_max_snapshots}" \
      --docker-env CRYPTO_TRAINING_FEATURE_STORE_ENABLED=true \
      "${wrapper_container_args[@]}"; then
      echo "${asset}: refresh failed" >&2
      status=1
      continue
    fi

    if ! run_cmd "${cli_container_args[@]}" crypto-replay run \
      --kalshi-env "${kalshi_env}" \
      --frequency "${frequency}" \
      --days "${replay_days}" \
      --limit "${replay_limit}" \
      --assets "${asset}" \
      --json; then
      echo "${asset}: feature-store replay proof failed" >&2
      status=1
      continue
    fi

    if ! run_cmd "${cli_container_args[@]}" crypto-replay gate \
      --kalshi-env "${kalshi_env}" \
      --frequency "${frequency}" \
      --assets "${asset}"; then
      echo "${asset}: replay gate failed" >&2
      status=1
      continue
    fi
  fi

  if ! run_cmd scripts/crypto_live_path_status.sh \
    --kalshi-env "${kalshi_env}" \
    --frequency "${frequency}" \
    --asset "${asset}" \
    --require-ready \
    --out-dir "${out_dir}/status" \
    "${wrapper_container_args[@]}"; then
    echo "${asset}: readiness check failed" >&2
    status=1
    continue
  fi

  if [[ "${skip_promote}" != "true" ]]; then
    if ! run_cmd "${cli_container_args[@]}" crypto-asset-mode set \
      --kalshi-env "${kalshi_env}" \
      "${asset}" live; then
      echo "${asset}: setting asset mode live failed" >&2
      status=1
      continue
    fi
  fi

  if ! run_cmd scripts/crypto_live_path_status.sh \
    --kalshi-env "${kalshi_env}" \
    --frequency "${frequency}" \
    --asset "${asset}" \
    --require-ready \
    --out-dir "${out_dir}/status" \
    "${wrapper_container_args[@]}"; then
    echo "${asset}: post-promotion readiness check failed" >&2
    status=1
    continue
  fi

  echo "=== ${asset} ${frequency} model promotion completed ===" >&2
done

exit "${status}"
