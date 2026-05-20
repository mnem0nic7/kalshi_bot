# Blue/green redeploy runbook

Zero-downtime deploys for the kalshi_bot stack. The **active color** keeps trading
while the **inactive color** is rebuilt and recreated; the cutover is a single
DB-backed lock handoff (`promote`), so there is no trading gap.

> Contrast: a plain `docker compose up -d --force-recreate <all colors>` bounces the
> active color too, causing a brief interruption (and a ~74s daemon warmup before the
> restarted color resumes work). Use that only when you intentionally want a full bounce.

## Mental model

| Concept | Where | Meaning |
|---------|-------|---------|
| `APP_COLOR` (per container) | `config.py` (`app_color`, default `blue`) | a container's fixed identity |
| `active_color` (DB row) | `deployment_control`, set via `repo.set_active_color` | which color may hold the execution lock |
| execution lock | `execution_lock_holder` in `status` | only the active color can place live orders |
| kill switch | `kill-switch on/off` | when **on**, the lock is cleared and live orders are blocked |
| pack handoff | `pending_pack_promotion:{env}:{color}` checkpoint | applied at target-color startup so a flip never strands the agent pack |

Key properties:
- `promote <color>` is **just a DB update** — instant, reversible, no container op.
- A flip while the **kill switch is on** is purely metadata; the lock is only contested
  once the kill switch is turned off. (`execution_lock_holder` is `null` while killed.)
- It is safe to `promote` a color that is still in its startup warmup; it begins live
  work after warmup. The script gates on container *health* (process up) before flipping.

## Colored vs singleton services

Per-color (participate in the flip): `app_{env}_{color}`, `daemon_{env}_{color}`,
`daemon_{env}_crypto_1h_{color}`.

Singletons (NOT color-scoped — recreate separately if a deploy changed them):
`crypto_1h_{env}`, `web_{env}`, `caddy`, `web_strategies`.

## Automated path (preferred)

```bash
# dry-run first to see the plan and confirm active/target detection
scripts/blue_green_redeploy.sh --env production --dry-run

# real run: builds, recreates inactive color, waits for health, prompts, then promotes
scripts/blue_green_redeploy.sh --env production

# fully unattended + recreate the old color afterward
scripts/blue_green_redeploy.sh --env production --yes --recreate-old
```

Flags: `--target blue|green` (force incoming color), `--no-build` (reuse current image),
`--yes` (skip the promote confirmation), `--recreate-old`, `--dry-run`.

The script aborts **before** the promote if the inactive color fails to go healthy, so a
bad build never takes down live trading — the active color is untouched until cutover.

## Manual path (what the script automates)

Assume `blue` is active; deploying onto `green`. Always pass `--env-file .env` and
`--no-deps` (per CLAUDE.md: otherwise force-recreate cascades into Postgres and/or boots
with stale env).

```bash
cd /home/user1/workspace/kalshi_bot

# 0. confirm current state
docker exec infra-app_production_blue-1 kalshi-bot-cli status   # active_color, kill_switch, lock

# 1. build the new image
COMPOSE_BAKE=false docker compose --env-file .env -f infra/docker-compose.yml build app_production_green

# 2. recreate the INACTIVE color only (blue keeps trading)
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --force-recreate \
  app_production_green daemon_production_green daemon_production_crypto_1h_green

# 3. verify green is healthy
docker compose -f infra/docker-compose.yml ps | grep green

# 4. hand off the execution lock (the cutover)
docker exec infra-app_production_green-1 kalshi-bot-cli promote green
docker exec infra-app_production_green-1 kalshi-bot-cli status   # expect active_color=green

# 5. (optional) recreate the now-idle old color
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --force-recreate \
  app_production_blue daemon_production_blue daemon_production_crypto_1h_blue
```

## Rollback

Re-promote the previous color — instant, no rebuild needed:

```bash
docker exec infra-app_production_blue-1 kalshi-bot-cli promote blue
```

If the new image itself is bad, roll the lock back first (above), then rebuild/fix on the
inactive color and retry the flip.

## Current state (as of 2026-05-20)

`active_color=blue`, `kill_switch_enabled=true`, `execution_lock_holder=null`,
`APP_SHADOW_MODE=true`. Live order submission is gated by the kill switch + shadow mode;
crypto autonomy/decision loops run, weather is fully disabled.
