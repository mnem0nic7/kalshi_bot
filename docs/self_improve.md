# Self Improve

This system currently runs Gemini-first agent-pack self-improvement for the non-canonical agent room. The trading decision path remains deterministic by default, and Phase 0 deterministic autonomy adds durable `decision_traces` so future self-improvement can graduate from prompt packs to replay-tested parameter packs.

Long-term target: promote bounded parameter packs, not LLM trade logic. A candidate parameter pack must be produced offline from strict-as-of replay data, evaluated on a holdout window, staged on the inactive blue/green color, and rolled back automatically if canary or live guardrails fail.

## Runtime Model Routing

- `researcher`, `president`, `trader`, `risk_officer`, `ops_monitor`, and `memory_librarian` default to Gemini-first routing.
- The provider router still falls back to the existing local model when Gemini is unavailable.
- `LLM_TRADING_ENABLED=false` keeps these roles out of the default trade path.
- Deterministic authority does not move:
  - structured weather pricing stays code-owned
  - research gating stays code-owned
  - risk approval stays code-owned
  - execution, signing, and deployment locking stay code-owned
  - decision trace replay stays code-owned

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

## Parameter-Pack Roadmap

Parameter packs are the planned replacement mutable unit for autonomous trading improvements. Unlike agent packs, they will contain only bounded numeric or categorical parameters such as probability blending weights, uncertainty buffers, Kelly fractions, source weights, and climatology pseudo-counts.

Hard caps remain outside the pack and operator-only:

- max position notional and percentage caps
- total exposure caps
- daily loss and drawdown caps
- kill-switch and rollback semantics
- source addition/removal
- first demo-to-live promotion

Promotion requires complete `decision_traces`, strict-as-of replay, holdout gates, and canary shadow evidence. The existing agent-pack workflow remains available until the parameter-pack promotion service replaces it.

Phase 4 scaffolding is present now: bounded `parameter_packs` records, deterministic pack hashes, hard-cap exclusion during sanitization, holdout promotion gates, DB-audited parameter-pack staging, and calibration drift pause criteria. These pieces are not yet wired to autonomous nightly promotion; they exist so replay search can be added without mixing mutable trading parameters into LLM agent-pack state.

The operator-owned caps live in `infra/config/hard_caps.yaml`. Parameter-pack validation can print a hash of that sealed config and can run in `--strict` mode to fail candidates that attempt to include hard-cap fields. The parameter-pack promotion gate loads the same sealed config and applies its `max_drawdown_pct` as the hard replay drawdown ceiling.

## CLI

Use the self-improve commands from the host or from inside the running app container:

```bash
kalshi-bot-cli self-improve status
kalshi-bot-cli self-improve critique --days 14 --limit 200
kalshi-bot-cli self-improve eval --candidate-version <VERSION> --days 14 --limit 200
kalshi-bot-cli self-improve promote --evaluation-run-id <EVALUATION_RUN_ID>
kalshi-bot-cli self-improve rollback --reason manual_rollback
kalshi-bot-cli decision-trace show <DECISION_TRACE_ID>
kalshi-bot-cli decision-trace replay <DECISION_TRACE_ID>
kalshi-bot-cli parameter-pack default
kalshi-bot-cli parameter-pack hard-caps
kalshi-bot-cli parameter-pack status
kalshi-bot-cli parameter-pack validate candidate-pack.json
kalshi-bot-cli parameter-pack validate candidate-pack.json --strict
kalshi-bot-cli parameter-pack gate --candidate-report candidate-holdout.json --current-report current-holdout.json --hard-caps infra/config/hard_caps.yaml
kalshi-bot-cli parameter-pack drift --window drift-window.json
kalshi-bot-cli parameter-pack grid --grid search-grid.json
kalshi-bot-cli parameter-pack select --candidates search-candidates.json --current-report current-holdout.json --hard-caps infra/config/hard_caps.yaml --starvation-tolerance 10
kalshi-bot-cli parameter-pack record-starvation --selection selection.json --escalation-threshold 3
kalshi-bot-cli parameter-pack learned-gate --closed-form-report closed-form-holdout.json --learned-report learned-holdout.json --requested-weight 0.25
kalshi-bot-cli parameter-pack nws-parser-gate --window parser-window.json --requested-feature-weight 0.25
kalshi-bot-cli parameter-pack stage --candidate-pack candidate-pack.json --candidate-report candidate-holdout.json --current-report current-holdout.json --hard-caps infra/config/hard_caps.yaml
kalshi-bot-cli parameter-pack canary --report canary-report.json
kalshi-bot-cli parameter-pack promote-staged --reason manual_parameter_pack_promote
kalshi-bot-cli parameter-pack rollback-staged --reason manual_parameter_pack_rollback
```

`parameter-pack status` shows the current staged rollout notes, champion pack, recent parameter packs, recent parameter-pack promotion events, and the current `promotion_starvation` checkpoint state. It also marks stale staged or canary-pending candidates `stalled` after `SELF_IMPROVE_CANARY_MAX_SECONDS` so abandoned rollouts are visible. `parameter-pack drift` evaluates calibration drift from a JSON window and reports whether new entries should pause and a pack search should trigger; it does not mutate runtime state. `parameter-pack grid` emits deterministic bounded candidates for offline replay without evaluating or staging them. `parameter-pack select` reads offline replay search candidates, stamps deterministic pack hashes into holdout reports when absent, applies the same sealed hard-cap promotion gates, prints the first passing candidate artifact, and reports `promotion_starvation` after K failed candidates; it does not mutate runtime state. `parameter-pack record-starvation` turns that read-only `promotion_starvation` result into an ops event and checkpoint, retaining the current pack and escalating from warning to error after repeated starvations. `parameter-pack learned-gate` keeps optional learned-head blend weight at zero unless learned holdout Brier, ECE, and Sharpe beat closed-form. `parameter-pack nws-parser-gate` keeps optional parser feature weight at zero unless shadow availability and schema-validity windows pass. `parameter-pack stage` is intentionally not an activator. It stores a sanitized candidate pack, records a `promotion_events` row with holdout-gate evidence and sealed hard-cap hash, and writes `deployment_control.notes.parameter_packs` for operator visibility. `parameter-pack canary` evaluates shadow-canary evidence: Brier must stay within 20% of holdout Brier, and risk-engine bypasses, data-source kill events, and hard-cap touches must be zero. `parameter-pack promote-staged` is operator-only and marks a canary-passed pack as the champion metadata record. `parameter-pack rollback-staged` marks that staged candidate rolled back and rejected. None of these commands change active color, live execution, hard caps, or runtime thresholds.

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
- `DEPLOY_SSH_ADDRESS_FAMILY` (optional, defaults to IPv4 with `inet`)
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
