# Self Improve

This system can run Gemini-first agent packs, critique recent shadow or demo rooms, evaluate a candidate pack on a holdout slice, stage the candidate on the inactive blue or green color through a pending-promotion checkpoint, and roll back if canary or live guardrails fail.

## Runtime Model Routing

- `researcher`, `president`, `trader`, `risk_officer`, `ops_monitor`, and `memory_librarian` default to Gemini-first routing.
- The provider router still falls back to the existing local model when Gemini is unavailable.
- Deterministic authority does not move:
  - structured weather pricing stays code-owned
  - research gating stays code-owned
  - risk approval stays code-owned
  - execution, signing, and deployment locking stay code-owned

## Agent Packs

An agent pack is the mutable runtime unit for self-improvement. Each version stores:

- per-role prompt, provider, model, and temperature
- research search and synthesis settings
- memory summarization settings
- bounded mutable thresholds:
  - `risk_min_edge_bps`
  - `risk_max_order_notional_dollars`
  - `risk_max_position_notional_dollars`
  - `trigger_max_spread_bps`
  - `trigger_cooldown_seconds`

The following safety controls are never auto-modified:

- `risk_daily_loss_limit_dollars`
- stale-data thresholds
- kill-switch semantics
- execution lock behavior
- credential handling
- order schema and submission invariants

Rooms now snapshot:

- `agent_pack_version`
- `kalshi_env`
- provider and model usage per role
- optional evaluation provenance

## CLI

Use the self-improve commands from the host or from inside the running app container:

```bash
kalshi-bot-cli self-improve status
kalshi-bot-cli self-improve critique --days 14 --limit 200
kalshi-bot-cli self-improve eval --candidate-version <VERSION> --days 14 --limit 200
kalshi-bot-cli self-improve promote --evaluation-run-id <EVALUATION_RUN_ID>
kalshi-bot-cli self-improve rollback --reason manual_rollback
```

The helper scripts wrap the same flow for Docker blue or green deployments:

```bash
infra/scripts/run-self-improve.sh status
infra/scripts/run-self-improve.sh critique --days 14 --limit 200
infra/scripts/restart-color.sh green
```

## GitHub Actions

Two workflows support the control plane:

- `.github/workflows/self-improve.yml`
- `.github/workflows/rollback-agent-pack.yml`

`Self Improve` does three things:

1. Runs a local offline test slice for Gemini, agent-pack, and training export logic.
2. SSHes to the VPS, syncs the configured Gemini key into the remote `.env`, then runs:
   - `self-improve critique`
   - `self-improve eval`
   - `self-improve promote` when the holdout summary passes
3. Restarts only the inactive color after staging so its daemon can apply the pending pack checkpoint on startup and begin canary shadow rooms.

`Rollback Agent Pack` is manual-only and restores the previous pack version on the server.

Required GitHub Secrets:

- `GEMINI_KEY` or `GEMINI_API_KEY`
- `DEPLOY_HOST`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY`
- `DEPLOY_SSH_PORT` (optional, defaults to `22`)
- `DEPLOY_APP_DIR`

## Canary and Live Monitoring

After a candidate is staged:

- a `pending_pack_promotion:<kalshi_env>:<color>` checkpoint is written instead of immediately mutating the inactive color assignment
- the inactive `daemon_<color>` applies that checkpoint on startup before entering the main loop
- the inactive `daemon_<color>` begins generating canary shadow rooms during heartbeats
- the canary remains `running` until both thresholds are satisfied:
  - `SELF_IMPROVE_CANARY_MIN_ROOMS`
  - `SELF_IMPROVE_CANARY_MIN_SECONDS`
- if the staged canary sits longer than `SELF_IMPROVE_CANARY_MAX_SECONDS`, `self-improve status` marks it `stalled` so operators do not mistake an abandoned rollout for an active one
- the system flips the active color and starts a live-monitor window

Guardrails that trigger automatic rollback:

- research-gate regression spike
- abnormal blocked-order spike
- stale-data spike
- drawdown beyond the daily-loss threshold

## Environment Variables

Important runtime envs:

- `GEMINI_KEY` or `GEMINI_API_KEY`
- `GEMINI_BASE_URL`
- `GEMINI_MODEL_RESEARCHER`
- `GEMINI_MODEL_PRESIDENT`
- `GEMINI_MODEL_TRADER`
- `GEMINI_MODEL_RISK_OFFICER`
- `GEMINI_MODEL_OPS_MONITOR`
- `GEMINI_MODEL_MEMORY_LIBRARIAN`
- `ACTIVE_AGENT_PACK_VERSION`
- `SELF_IMPROVE_WINDOW_DAYS`
- `SELF_IMPROVE_HOLDOUT_RATIO`
- `SELF_IMPROVE_MIN_IMPROVEMENT`
- `SELF_IMPROVE_MAX_CRITICAL_REGRESSION`
- `SELF_IMPROVE_CANARY_MIN_ROOMS`
- `SELF_IMPROVE_CANARY_MIN_SECONDS`
- `SELF_IMPROVE_CANARY_MAX_SECONDS`
- `SELF_IMPROVE_LIVE_MONITOR_SECONDS`
- `SELF_IMPROVE_RESEARCH_GATE_FAILURE_THRESHOLD`
- `SELF_IMPROVE_BLOCKED_ORDER_THRESHOLD`
