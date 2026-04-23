# Strategy Page — Reference & Progress Tracker

> **Living document.** Updated when features ship or the architecture changes.  
> Scope: everything under the Strategies tab of the control room dashboard.

---

## What It Is

The Strategies page is the operator interface for monitoring automated strategy evolution across weather market cities. Auto-Evolve runs the nightly 180d evidence refresh, asks the AI for evaluation and suggestion runs, accepts valid suggested presets, activates them, and applies eligible city assignments automatically. Manual approval, activation, and lab controls remain available as operator override tools.

---

## Architecture Overview

```
Browser (dashboard.js)
  └── GET /api/dashboard/strategies?window_days=&series_ticker=&strategy_name=
        └── control_room.py → build_strategies_dashboard_core()
              ├── StrategyRegressionService     (historical backtesting)
              ├── StrategyEvaluationService     (live edge-adjustment after settlements)
              ├── StrategyCodexService          (Evaluation Lab runs)
              └── StrategyAutoEvolveService     (nightly automation orchestration)
```

**Key files:**

| Layer | File |
|-------|------|
| Routes | `src/kalshi_bot/web/app.py` lines 1292–1481 |
| Dashboard builder | `src/kalshi_bot/web/control_room.py` — `build_strategies_dashboard_core()` |
| Regression engine | `src/kalshi_bot/services/strategy_regression.py` |
| Edge adjustment | `src/kalshi_bot/services/strategy_eval.py` |
| Evaluation Lab | `src/kalshi_bot/services/strategy_codex.py` |
| Auto-Evolve service | `src/kalshi_bot/services/strategy_auto_evolve.py` |
| Codex CLI provider | `src/kalshi_bot/agents/codex_cli.py` |
| Provider router | `src/kalshi_bot/agents/providers.py` — `build_codex_provider()` |
| Dashboard service | `src/kalshi_bot/services/strategy_dashboard.py` — bridges `control_room.py` to the three sub-services |
| Frontend | `src/kalshi_bot/web/static/dashboard.js` — `renderStrategies()` and friends |
| Template | `src/kalshi_bot/web/templates/index.html` lines 209–351 |

---

## Focus Modes

### Review
Assignment review queue. Shows **all** cities grouped by their current review status (not just those that need action). Groups appear in priority order:

| Group (display order) | Status | Meaning |
|-----------------------|--------|---------|
| Needs review: drifted assignment | `drifted_assignment` | Assigned, but regression now recommends a different strategy |
| Ready for approval | `ready_for_approval` | Unassigned city; evidence meets the approval threshold |
| Needs review: weakened evidence | `evidence_weakened` | Assigned, but the 180d snapshot has no scored outcome evidence |
| Aligned assignments | `aligned` | Assignment matches current recommendation — no action needed |
| Waiting for evidence | `waiting_for_evidence` | Unassigned city; evidence not yet strong enough |

Only `strong_recommendation` and `lean_recommendation` cities are eligible for assignment (`STRATEGY_APPROVAL_ELIGIBLE_STATUSES`). Auto-Evolve applies those eligible 180d recommendations automatically when enabled. The review workflow requires the latest 180d regression snapshot to be available (`summary.review_available`); if not, the queue is hidden.

**Manual override flow:**
Click a city → detail panel shows recommendation rationale and threshold comparison → "Approve" button → `POST /api/strategies/assignments/{series_ticker}/approve` → recorded as `strategy_review` / `assignment_approval` event.

---

### Cities
City matrix. Rows = weather market cities (series tickers). Columns = strategy presets. Each cell shows the backtest win rate for that city × strategy combination over the selected window (30d / 90d / 180d, default 180d).

The recommendation column shows which strategy the regression engine currently favors for that city, and whether it matches the active assignment.

Clicking a city row loads the city detail panel:
- Threshold comparison (current assignment vs. recommended strategy parameters)
- Approval eligibility and next-action copy
- Latest approval note

---

### Strategies (Leaderboard + Evaluation Lab)
Two panels:

**Leaderboard** — all strategy presets ranked by aggregate win rate across cities for the selected window. Clicking a strategy card loads the strategy detail panel showing per-city breakdown.

**Evaluation Lab** — operator interface for the AI strategy suggestion engine (see below).

---

## Evaluation Lab

The Evaluation Lab lets the operator or Auto-Evolve ask the AI to evaluate an existing city's strategy fit or propose an entirely new strategy preset. Results are stored as "runs" and can be accepted manually or automatically.

### How It Works

1. Operator clicks "Open in Evaluation Lab" in context of a city or strategy.
2. Browser calls `POST /api/strategies/codex/runs` with a `mode` (`"evaluate"` or `"suggest"`), `window_days`, and optional `series_ticker` / `strategy_name`.
   - `evaluate` — scores how well existing strategies fit a city; no new strategy is created.
   - `suggest` — proposes a new strategy preset with different thresholds; accepted runs create a new strategy.
3. `StrategyCodexService.execute_run()` calls the selected AI provider with a structured prompt.
4. Browser polls `GET /api/strategies/codex/runs/{run_id}` until status is `completed` or `failed`.
5. For manual runs, the operator reviews output and clicks "Accept" → `POST /api/strategies/codex/runs/{run_id}/accept` → strategy saved (only valid for `suggest` mode runs). For Auto-Evolve runs, valid suggestions with deterministic backtest status `ok` are accepted by the service.

**Trigger sources:** `"manual"` (operator-initiated from the UI) and `"nightly"` (daemon-scheduled).

### Providers

The Evaluation Lab supports two providers: **Gemini** (primary) and **Codex CLI** (secondary). No other paths are wired.

| Provider | How it works |
|----------|-------------|
| Gemini | `NativeGeminiProvider` via `GEMINI_KEY`. Used by default when available. |
| Codex CLI | `CodexCLIProvider` shells out to the `codex` binary: `codex exec -c 'approval_policy="never"' - < prompt`. Auth is managed by the CLI from `~/.codex/auth.json`. Available when the `codex` binary is on PATH. |

Config:
```
GEMINI_KEY=...          # enables Gemini provider
CODEX_MODEL=gpt-4o          # model passed to the CLI binary (default)
```

If neither provider is available, the Evaluation Lab is disabled (`is_available()` returns false).

## Auto-Evolve Lifecycle

Auto-Evolve is the production-on automation path for the Strategies page. It has one service entrypoint:

```
StrategyAutoEvolveService.run_once(trigger_source="nightly" | "manual")
```

Daily cadence:

1. The daemon reaches the local nightly target from `STRATEGY_CODEX_NIGHTLY_TIMEZONE` and `STRATEGY_CODEX_NIGHTLY_HOUR_LOCAL`.
2. `_maybe_run_strategy_codex_nightly()` delegates to `StrategyAutoEvolveService` when `STRATEGY_AUTO_EVOLVE_ENABLED=true`.
3. The service refreshes or requires a fresh strategy regression checkpoint for the configured window, default 180d.
4. It builds the dashboard snapshot and runs both AI modes: `evaluate` and `suggest`.
5. If the suggestion run completes and its deterministic backtest has `status="ok"`, the existing accept logic saves the preset into `strategies`.
6. If activation is enabled, the saved preset is immediately activated.
7. The service rebuilds the 180d dashboard and assigns every `approval_eligible=true` city to the current recommendation, including drifted and unassigned cities.
8. It records a single `ops_events` row and updates the checkpoint.

Checkpoint names:

| Checkpoint | Purpose |
|------------|---------|
| `daemon_strategy_auto_evolve:{kalshi_env}:{app_color}` | Auto-Evolve run status, run IDs, accepted/activated strategy, provider/model, assignment changes, skips, and errors |
| `strategy_regression` | Latest regression refresh used as the 180d evidence base |
| `daemon_strategy_codex_nightly:{kalshi_env}:{app_color}` | Legacy nightly Codex checkpoint when Auto-Evolve is disabled |

Default settings:

```
STRATEGY_AUTO_EVOLVE_ENABLED=true
STRATEGY_AUTO_EVOLVE_WINDOW_DAYS=180
STRATEGY_AUTO_EVOLVE_ASSIGN_ELIGIBLE=true
STRATEGY_AUTO_EVOLVE_ACCEPT_SUGGESTIONS=true
STRATEGY_AUTO_EVOLVE_ACTIVATE_SUGGESTIONS=true
```

Provider precedence is Gemini first, then Codex CLI when available. Gemini requires `GEMINI_KEY`; Codex requires the `codex` binary and CLI auth. If no provider is available, Auto-Evolve writes a skipped checkpoint and does not mutate strategies or assignments.

Automatic mutations:

| Storage | Mutation |
|---------|----------|
| `strategy_codex_runs` | Stores nightly `evaluate` and `suggest` runs |
| `strategies` | Stores accepted AI-suggested presets and active status |
| `city_strategy_assignments` | Writes canonical city assignments with `assigned_by="auto_evolve"` |
| `ops_events` | Summarizes each Auto-Evolve run |

The flow is idempotent by local date: rerunning after a completed same-day run refreshes the Auto-Evolve checkpoint but does not duplicate accepted strategies or rewrite already matching assignments.

Rollback / stop procedure:

1. Set `STRATEGY_AUTO_EVOLVE_ENABLED=false`.
2. Redeploy.
3. Use the Strategies page manual override controls to restore city assignments or deactivate AI-suggested strategy presets as needed.

Failure modes:

| Failure | Behavior |
|---------|----------|
| Provider unavailable | Skipped checkpoint, no strategy or assignment mutation |
| Invalid suggestion schema | Suggestion cannot be accepted, activation and assignment are skipped |
| Backtest failure | Suggestion is not accepted, activation and assignment are skipped |
| Stale regression unavailable | Skipped checkpoint, no mutation |
| Partial assignment failure | Run records `completed_with_failures` with assignment errors |

### Nightly Evaluation

When `STRATEGY_CODEX_NIGHTLY_ENABLED=true`, the daemon enters the nightly strategy window defined by `STRATEGY_CODEX_NIGHTLY_TIMEZONE` and `STRATEGY_CODEX_NIGHTLY_HOUR_LOCAL`. With Auto-Evolve enabled, that nightly path delegates to `StrategyAutoEvolveService` and writes `daemon_strategy_auto_evolve:{kalshi_env}:{app_color}`. With Auto-Evolve disabled, it falls back to the legacy Codex-only checkpoint `daemon_strategy_codex_nightly:{kalshi_env}:{app_color}`.

---

## Nightly Regression

Strategy presets are seeded at startup from `STRATEGY_PRESETS` in `src/kalshi_bot/services/strategy_regression.py`. Threshold values are intentionally not duplicated here — read the source for current values.

`StrategyRegressionService.run_regression()` runs at most once every `STRATEGY_REGRESSION_DAILY_RUN_SECONDS` (minimum 3600s). It:
1. Fetches all historical replay rooms with scored outcomes.
2. For each city × strategy combination, computes win rate, trade count, and a recommendation label.
3. Stores the result snapshot; the dashboard reads the latest snapshot (or runs live if stale).

---

## Edge Adjustment

`StrategyEvaluationService.maybe_adjust()` runs after every reconciliation cycle that contains settlements. It is an autonomous feedback loop that adjusts the minimum edge threshold (`risk_min_edge_bps`) in the active agent pack based on realized win rate.

**How it works:**

| Condition | Action |
|-----------|--------|
| 30d win rate ≥ 60% | Loosen — decrease `risk_min_edge_bps` by 10 bps (trade more freely) |
| 30d win rate ≤ 35% | Tighten — increase `risk_min_edge_bps` by 10 bps (require stronger signal) |
| Between 35%–60% | No change |

**Constraints:**
- Requires at least **50 settled contracts** before any adjustment fires.
- Adjustments are rate-limited to once every **24 hours**; after a tighten, another tighten is blocked for **48 hours**.
- `risk_min_edge_bps` is clamped to the range **20–150 bps**.
- Each adjustment creates a new agent pack version (named `auto-{direction}-{timestamp}`) and immediately promotes it as champion for the active color.

**Operator implication:** This is an autonomous pack promotion. Adjustments appear in the ops event log (`source="strategy_eval"`) and can be reviewed there. If the win rate data is skewed (e.g., due to bulk-import timestamp issues), adjustments may fire incorrectly — watch the ops log after any fill data migration.

---

## API Reference

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/dashboard/strategies` | Full dashboard payload; params: `window_days`, `series_ticker`, `strategy_name` |
| `POST` | `/api/strategies/auto-evolve/run` | Authenticated manual Auto-Evolve smoke run using the same service path |
| `POST` | `/api/strategies/codex/runs` | Create an Evaluation Lab run |
| `GET` | `/api/strategies/codex/runs/{run_id}` | Poll run status |
| `POST` | `/api/strategies/codex/runs/{run_id}/accept` | Accept Evaluation Lab suggestion |
| `POST` | `/api/strategies/{strategy_name}/activate` | Activate a strategy preset |
| `POST` | `/api/strategies/assignments/{series_ticker}/approve` | Approve city assignment |
| `GET` | `/api/strategy-audit/rooms/{room_id}` | Strategy audit for one room |
| `GET` | `/api/strategy-audit/summary` | Aggregate strategy audit |

`GET /api/dashboard/strategies` includes an `automation` object with enabled state, mode, checkpoint name, last status, provider/model, accepted and activated strategies, assignment change count, and recent assignment changes.

---

## Progress Tracker

### Done

- [x] Strategy regression engine — historical win-rate backtesting per city × strategy
- [x] City matrix view — recommendation grid with window filter (30d / 90d / 180d)
- [x] Strategy leaderboard — aggregate ranking across cities
- [x] Manual approval workflow — Review queue with status labels and approval POST
- [x] Auto-Evolve workflow — nightly 180d evaluation, suggestion acceptance, activation, and eligible assignment application
- [x] City detail panel — threshold comparison, evidence status, next-action copy
- [x] Strategy detail panel — per-city breakdown for selected preset
- [x] Evaluation Lab UI — run creation, polling, accept/discard flow
- [x] `CodexCLIProvider` — shells out to `codex exec` via subprocess; CLI manages its own auth
- [x] `build_codex_provider()` — provider resolution: Gemini first, Codex CLI second
- [x] Nightly strategy automation — daemon-scheduled, checkpoint-guarded
- [x] Edge adjustment after settlements
- [x] Recent promotion / approval history panel
- [x] Strategy audit per room and aggregate summary endpoints

### In Progress / Pending

- [ ] **Codex CLI in production** — `@openai/codex` npm package must be installed in the production container (`npm install -g @openai/codex`). Without it, only Gemini is available for the Evaluation Lab. Run `codex login` on the server after install to authenticate.
- [ ] **Evaluation Lab end-to-end smoke test on live** — verify provider execution works after deploy; check `codexLabPayload` and `automation.provider` in dashboard response show the selected provider.
- [ ] **Evaluation Lab provider smoke test on live** — verify the active provider path works after deploy; the dashboard `automation.provider` field should show `gemini` first when `GEMINI_KEY` is present.

### Known Gaps / Future Work

- Strategy presets are currently defined at seed time; there is no UI for creating a new preset from scratch without going through Evaluation Lab.
- The 180d window is the only one used for approval eligibility; shorter windows (30d / 90d) are display-only.
- No external alerting when a city drifts from `aligned` to `drifted_assignment` between regression runs; Auto-Evolve records the correction in the dashboard and ops log.
- **Strategies read source is configurable.** By default the page reads from the deployment's primary DB (demo deployments read `postgres_demo`, production reads `postgres_production`). Set `STRATEGY_REGRESSION_READ_SOURCE=secondary` together with `POSTGRES_SECONDARY_HOST` on the demo deployment to make the strategies page and the regression pipeline pull from the production DB instead — regression snapshots and city assignments still write locally so each deployment keeps its own history. If `secondary` is requested without a secondary DB configured, the app logs an error and falls back to primary. The active source appears as `regression_read_source` in `/api/control-room/summary`.
