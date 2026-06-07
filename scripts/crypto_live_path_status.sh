#!/usr/bin/env bash
set -euo pipefail

kalshi_env="production"
frequency="15m"
asset=""
status_days="14"
out_dir="reports/crypto_live_path/status"
docker_container="${DOCKER_CONTAINER:-}"
require_ready=false

usage() {
  cat <<'USAGE'
usage: scripts/crypto_live_path_status.sh --asset <ASSET> [options]

Options:
  --kalshi-env <demo|production>       Target Kalshi environment. Default: production.
  --frequency <frequency>              Crypto market frequency. Default: 15m.
  --asset <ASSET>                      Asset to check. Required.
  --status-days <days>                 Status lookback window. Default: 14.
  --require-ready                      Return non-zero unless the asset is ready.
  --out-dir <path>                     Report output directory. Default: reports/crypto_live_path/status.
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
    --asset|--assets)
      asset="${2:?missing value for --asset}"
      shift 2
      ;;
    --status-days)
      status_days="${2:?missing value for --status-days}"
      shift 2
      ;;
    --require-ready)
      require_ready=true
      shift
      ;;
    --out-dir)
      out_dir="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --docker-container)
      docker_container="${2:?missing value for --docker-container}"
      shift 2
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

if [[ -z "${asset}" ]]; then
  echo "--asset is required" >&2
  usage >&2
  exit 2
fi

asset="$(printf '%s' "${asset}" | tr '[:lower:]' '[:upper:]')"
mkdir -p "${out_dir}"

if [[ -n "${docker_container}" ]]; then
  cli=(docker exec -i "${docker_container}" python -m kalshi_bot.cli)
else
  read -r -a cli <<< "${CLI:-kalshi-bot-cli}"
fi

status_args=()
if [[ "${require_ready}" == "true" ]]; then
  status_args+=(--require-ready)
fi

ts="$(date -u +%Y%m%dT%H%M%SZ)"
report="${out_dir}/${kalshi_env}_${frequency}_${asset}_status_${ts}.json"
log="${out_dir}/${kalshi_env}_${frequency}_${asset}_status_${ts}.log"

set +e
"${cli[@]}" crypto-live-path status \
  --kalshi-env "${kalshi_env}" \
  --frequency "${frequency}" \
  --assets "${asset}" \
  --status-days "${status_days}" \
  --skip-growth \
  --json \
  "${status_args[@]}" >"${report}" 2>"${log}"
rc=$?
set -e

"${PYTHON:-python3}" - "${report}" "${log}" <<'PY'
import json
import sys
from pathlib import Path

report = Path(sys.argv[1])
log = Path(sys.argv[2])
try:
    payload = json.loads(report.read_text(encoding="utf-8"))
except Exception as exc:
    print(f"failed to parse {report}: {exc}", file=sys.stderr)
    if log.exists():
        print(log.read_text(encoding="utf-8"), file=sys.stderr)
    raise SystemExit(1)

asset_reports = payload.get("asset_reports") or []
asset_report = asset_reports[0] if asset_reports else {}
quote = asset_report.get("quote_evidence") or {}
replay = asset_report.get("replay") or {}
deployment = payload.get("deployment") or {}
blockers = asset_report.get("blockers") or []
warnings = asset_report.get("warnings") or []

print(f"report={report}")
print(f"status={payload.get('status')}")
print(f"asset={asset_report.get('asset')} frequency={payload.get('frequency')}")
print(f"ready={asset_report.get('ready_for_live_mode')} replay_gate={replay.get('gate_status')}")
print(
    "strict_days="
    f"{quote.get('strict_quote_market_day_count')} "
    "strict_rows="
    f"{quote.get('strict_trade_eligible_count')}"
)
print(
    "live_quality_candidates="
    f"{replay.get('current_model_live_quality_candidate_count')} "
    "oos_candidates="
    f"{replay.get('oos_trade_candidate_count')} "
    "oos_folds="
    f"{replay.get('oos_fold_count')} "
    "net_pl="
    f"{replay.get('net_simulated_pl_dollars')}"
)
print(f"active_color={deployment.get('active_color')} app_color={deployment.get('app_color')}")
if replay.get("gate_reasons"):
    print("replay_gate_reasons:")
    for item in replay.get("gate_reasons") or []:
        print(f"- {item}")
if deployment.get("live_order_blockers"):
    print("live_order_blockers:")
    for item in deployment.get("live_order_blockers") or []:
        print(f"- {item}")
if blockers:
    print("blockers:")
    for item in blockers:
        print(f"- {item}")
if warnings:
    print("warnings:")
    for item in warnings:
        print(f"- {item}")
PY

exit "${rc}"
