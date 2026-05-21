#!/usr/bin/env bash
# Weekly per-asset crypto model training.
# Called by cron with the asset symbol as $1.
# Usage: crypto_model_train_weekly.sh BTC
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
CONTAINER="infra-daemon_production_blue-1"
LOG="/tmp/crypto_train_${ASSET}_$(date +%Y%m%d).log"

exec > >(tee -a "$LOG") 2>&1

echo "=== crypto-model train ${ASSET} started at $(date) ==="

if ! docker inspect --format "{{.State.Running}}" "$CONTAINER" 2>/dev/null | grep -q "true"; then
    echo "ERROR: $CONTAINER is not running — aborting ${ASSET} training"
    exit 1
fi

docker exec "$CONTAINER" kalshi-bot-cli crypto-model train --assets "$ASSET" --frequency 15m
echo "--- ${ASSET}/15m done at $(date) ---"

docker exec "$CONTAINER" kalshi-bot-cli crypto-model train --assets "$ASSET" --frequency 1h
echo "--- ${ASSET}/1h done at $(date) ---"

echo "=== crypto-model train ${ASSET} complete at $(date) ==="
