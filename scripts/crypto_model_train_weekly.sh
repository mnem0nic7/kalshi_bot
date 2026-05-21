#!/usr/bin/env bash
# Weekly per-asset crypto model training + gate check.
# Called by cron with the asset symbol as $1.
# Usage: crypto_model_train_weekly.sh BTC
#
# After each frequency's training, runs the replay gate.
# If the gate passes and the asset is in shadow mode, promotes it to live.
#
# Schedule (PDT / UTC):
#   Mon 2am PDT / 9am UTC  — BTC
#   Tue 2am PDT / 9am UTC  — ETH
#   Wed 2am PDT / 9am UTC  — SOL
#   Thu 2am PDT / 9am UTC  — XRP
#   Fri 2am PDT / 9am UTC  — BNB
#   Sat 2am PDT / 9am UTC  — DOGE
#   Sun 6am PDT / 1pm UTC  — HYPE (delayed to clear Sunday VACUUM FULL window)
set -euo pipefail

ASSET="${1:?Usage: $0 ASSET}"
LOG="/tmp/crypto_train_${ASSET}_$(date +%Y%m%d).log"

exec > >(tee -a "$LOG") 2>&1

echo "=== crypto-model train ${ASSET} started at $(date) ==="

# Resolve the active daemon container (prefer green, fall back to blue).
active_container() {
    for color in green blue; do
        local name="infra-daemon_production_${color}-1"
        if docker inspect --format "{{.State.Running}}" "$name" 2>/dev/null | grep -q "true"; then
            echo "$name"
            return 0
        fi
    done
    echo "ERROR: no running daemon_production container found" >&2
    exit 1
}

CONTAINER="$(active_container)"
echo "--- using container: ${CONTAINER} ---"

# Train and gate-check one frequency.
# Promotes the asset from shadow → live if the gate passes.
train_and_gate() {
    local freq="$1"

    echo "--- ${ASSET}/${freq} training started at $(date) ---"
    docker exec "$CONTAINER" kalshi-bot-cli crypto-model train \
        --assets "$ASSET" --frequency "$freq"
    echo "--- ${ASSET}/${freq} training done at $(date) ---"

    echo "--- ${ASSET}/${freq} gate check started at $(date) ---"
    if docker exec "$CONTAINER" kalshi-bot-cli crypto-replay gate \
            --kalshi-env production --frequency "$freq" --assets "$ASSET"; then
        echo "--- ${ASSET}/${freq} gate PASSED ---"

        # Check current mode; promote shadow → live.
        # Filter out structured-log lines (contain "timestamp") before parsing JSON.
        current_mode=$(docker exec "$CONTAINER" kalshi-bot-cli crypto-asset-mode list \
            --kalshi-env production 2>/dev/null \
            | grep -v '"timestamp"' \
            | jq -r --arg a "$ASSET" '.modes[$a] // "unknown"' || true)

        if [[ "$current_mode" == "shadow" ]]; then
            echo "--- promoting ${ASSET} to live (was shadow) ---"
            docker exec "$CONTAINER" kalshi-bot-cli crypto-asset-mode set \
                --kalshi-env production "$ASSET" live
            echo "--- ${ASSET} is now live ---"
        else
            echo "--- ${ASSET} mode is '${current_mode}' — no promotion needed ---"
        fi
    else
        echo "--- ${ASSET}/${freq} gate BLOCKED — asset stays in current mode ---"
    fi
}

train_and_gate 15m
train_and_gate 1h

echo "=== crypto-model train ${ASSET} complete at $(date) ==="
