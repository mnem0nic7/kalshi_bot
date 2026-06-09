#!/usr/bin/env bash
# Regenerate per-asset 15m backtest + replay_gate artifacts with the fixed
# bucket-matrix code (9f746ea). Single-asset runs write backtest:<ASSET>,
# which gate() then copies into replay_gate:<ASSET> — the artifact the
# empirical bucket gate reads at decision time.
set -uo pipefail

CONTAINER="${1:-infra-daemon_production_blue-1}"
ASSETS=(BTC ETH SOL XRP BNB DOGE HYPE)

for asset in "${ASSETS[@]}"; do
  echo "=== ${asset}: backtest run $(date -u +%H:%M:%S) ==="
  docker exec "${CONTAINER}" kalshi-bot-cli crypto-replay run \
    --kalshi-env production --frequency 15m --assets "${asset}" --json 2>/dev/null \
    | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('version'), r.get('status'))" \
    || { echo "${asset}: run FAILED"; continue; }

  echo "=== ${asset}: gate $(date -u +%H:%M:%S) ==="
  docker exec "${CONTAINER}" kalshi-bot-cli crypto-replay gate \
    --kalshi-env production --frequency 15m --assets "${asset}" --json 2>/dev/null \
    | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('version'), r.get('status'), r.get('reasons'))" \
    || echo "${asset}: gate FAILED"
done
echo "=== all done $(date -u +%H:%M:%S) ==="
