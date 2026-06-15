# Crypto Trainer Research Workflows

All heavy crypto research jobs run in the dedicated `trainer_production`
container. Do not run feature materialization, model retraining, replay gates,
calibration studies, candidate diagnostics, or adverse-selection backfills on
the host shell or normal app/daemon containers.

## Container Boundary

- `trainer_production`: model retrains, materialization, replay/gate runs,
  calibration studies, candidate diagnostics, maker markout reports.
- app/daemon containers: live decisions, reconciliation, execution, lightweight
  status/report reads.
- host shell: unit tests, `docker compose config`, and small non-DB code checks.

The trainer is triple-guarded against trading in compose (`APP_SHADOW_MODE=true`,
kill switch enabled, crypto trading disabled) and owns the trainer CPU/GPU set.

## Commands

Recreate only the trainer after code/config changes:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --build trainer_production
```

Candidate diagnostics:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec trainer_production \
  python -m kalshi_bot.cli crypto-model candidates --kalshi-env production --frequency 15m --days 30 --json
```

Model train from the trainer:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec trainer_production \
  python -m kalshi_bot.cli crypto-model train --kalshi-env production --frequency 15m
```

Replay validation and gate persistence:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec trainer_production \
  python -m kalshi_bot.cli crypto-replay validate --kalshi-env production --frequency 15m --days 30 --json

docker compose --env-file .env -f infra/docker-compose.yml exec trainer_production \
  python -m kalshi_bot.cli crypto-replay gate --kalshi-env production --frequency 15m
```

Fee/net-P&L attribution:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec trainer_production \
  python -m kalshi_bot.cli crypto-pnl-report --kalshi-env production --days 30 --frequency 15m --json
```

Shadow-only maker adverse-selection markouts:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec trainer_production \
  python -m kalshi_bot.cli crypto-maker-markout-report --kalshi-env production --days 30 --frequency 15m --json
```

## Promotion Rule

Maker research is evidence-only. Do not enable live maker expansion or loosen
crypto gates from these reports unless a separate reviewed change proves
positive net expectancy after maker fees and adverse-selection markouts.
