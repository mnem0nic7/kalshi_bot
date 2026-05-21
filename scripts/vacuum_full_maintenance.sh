#!/usr/bin/env bash
# Scheduled VACUUM FULL on bloated crypto training tables.
# Requires exclusive lock — stop daemons first to avoid blocking active ingestion.
# Expected runtime: 15-45 min depending on table size.
set -euo pipefail

LOG="/tmp/vacuum_full_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee "$LOG") 2>&1

echo "=== VACUUM FULL maintenance started at $(date) ==="

cd /home/user1/workspace/kalshi_bot

# Stop daemon containers to release table locks
docker compose --env-file .env -f infra/docker-compose.yml stop \
  daemon_production_blue daemon_production_green \
  daemon_production_crypto_1h_blue daemon_production_crypto_1h_green \
  app_production_blue app_production_green

echo "Daemons stopped at $(date)"

# VACUUM FULL reclaims physical space (requires exclusive lock, hence the stop above)
docker exec infra-postgres_production-1 \
  bash -c "PGOPTIONS='-c max_parallel_maintenance_workers=0' psql -U postgres -d kalshi_bot -c 'VACUUM FULL ANALYZE crypto_market_candlesticks;'"

echo "crypto_market_candlesticks done at $(date)"

docker exec infra-postgres_production-1 \
  bash -c "PGOPTIONS='-c max_parallel_maintenance_workers=0' psql -U postgres -d kalshi_bot -c 'VACUUM FULL ANALYZE crypto_market_snapshots;'"

echo "crypto_market_snapshots done at $(date)"

# Restart all containers
docker compose --env-file .env -f infra/docker-compose.yml up -d \
  daemon_production_blue daemon_production_green \
  daemon_production_crypto_1h_blue daemon_production_crypto_1h_green \
  app_production_blue app_production_green

echo "=== Maintenance complete at $(date) ==="
echo "Log saved to: $LOG"
