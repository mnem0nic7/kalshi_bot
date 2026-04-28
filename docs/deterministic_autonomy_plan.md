# Deterministic Autonomy Plan

## Quality Evaluation

Score: **8.2/10**.

The plan is pointed in the right direction: remove nondeterminism from trade decisions, make every decision replayable, promote only bounded parameters, and keep hard risk caps outside the autonomy envelope. The strongest parts are the explicit rollback triggers, the strict-as-of replay dependency, and the insistence that profitability is evidence rather than a code assertion.

Repo-specific correction: `kalshi_bot` already defaults to the deterministic fast path (`LLM_TRADING_ENABLED=false`). Phase 0 is therefore not a risky removal of live LLM trading. It is an auditability phase: persist complete traces, prove replay equivalence, and update documentation so the deterministic path is the canonical architecture.

## Phase 0: Audit And Replay Foundation

Phase 0 is implemented as the safe first slice:

- Keep current live risk thresholds, source set, and execution behavior unchanged.
- Persist a durable `decision_traces` row for every deterministic supervisor decision.
- Trace accepted entries, risk-blocked proposals, and stand-downs.
- Store source snapshot references, threshold inputs, signal state, candidate trace, sizing context, risk verdict, execution receipt, final outcome, and stable hashes.
- Add `kalshi-bot-cli decision-trace show <id>` for inspection.
- Add `kalshi-bot-cli decision-trace replay <id>` for hash-based replay verification.
- Make CLI `shadow-run` surface `decision_trace_id` and fail if no deterministic trace was produced.

The trace table is intentionally separate from `trade_tickets`. No-ticket decisions are first-class training and calibration data.

## Roadmap

### Phase 1: Probability Engine

Add the closed-form Gumbel + climatology shrinkage engine and run it in shadow against the current NWS-driven signal. Multi-source weather data should be introduced adapter-first, with each provider normalized behind a source adapter before ensemble fusion is allowed to affect decisions.

Phase 1 foundation now exists as shadow-first primitives:

- `kalshi_bot.forecast.probability_engine` implements Gumbel fitting, bucket probability, KDE integration, climatology shrinkage, boundary mass, disagreement, and mapping-to-bucket conversion.
- `kalshi_bot.forecast.ensemble_fuser` defines adapter-facing ensemble member types and deterministic source fusion with QC outlier rejection.
- `forecast_snapshots` and `climatology_priors` provide storage for future strict-as-of replay and probability audits.
- None of these primitives are wired into live order selection yet.

Default source posture:

- Launch adapters for GFS, ECMWF, AIFS, and NWS-point.
- Keep AIFS at reduced weight until replay evidence validates parity.
- Do not trade live from the ensemble engine until holdout Brier, ECE, and coverage gates pass.

### Phase 2: Risk Math

Wire uncertainty, fee-aware Kelly sizing, survival mode, and exit risk scoring only after Phase 1 has replay evidence. The existing deterministic risk engine remains the hard authority and must continue clipping or blocking any request that violates caps.

Phase 2 foundation now exists as shadow-first primitives:

- `kalshi_bot.risk.uncertainty` computes uncertainty score, dynamic minimum EV, and size taper.
- `kalshi_bot.risk.sizing` computes fee-aware binary Kelly and hard-cap-aware sizing breakdowns.
- `kalshi_bot.risk.survival` switches Kelly fraction and minimum EV when bankroll falls below the survival threshold.
- `kalshi_bot.risk.exit_score` computes exit risk and deterministic exit-rule precedence.
- None of these primitives replace the live `services.risk` or `services.sizing` path yet.

### Phase 3: Source Health

Score source success, freshness, completeness, and consistency each cycle. Broken sources are excluded from fusion; degraded aggregate health reduces size; broken aggregate health pauses entries or engages kill-switch behavior according to the operator-approved hard caps.

Phase 3 foundation now exists as a guarded degradation layer:

- `kalshi_bot.forecast.source_health` scores each source with the planned 0.45/0.25/0.20/0.10 success, freshness, completeness, and consistency weights.
- `source_health_logs` stores per-source and aggregate labels/components for replay and incident review.
- Deterministic sizing primitives can consume `health_size_mult` values of 1.0, 0.5, or 0.0 for HEALTHY, DEGRADED, and BROKEN aggregate health.
- The watchdog records a `deployment_control.notes.source_health.pause_new_entries` state after consecutive BROKEN aggregate cycles.
- The live risk engine honors that explicit pause note for new entries, while existing position management and kill-switch semantics remain separate.

### Phase 4: Parameter-Pack Autonomy

Repurpose self-improve from agent-pack promotion into parameter-pack promotion. Candidate packs are tuned offline on strict-as-of replay data and promoted only after coverage, Brier, ECE, Sharpe, drawdown, city consistency, idempotency, and canary gates pass.

Phase 4 foundation now exists without activating autonomous promotion:

- `kalshi_bot.learning.parameter_pack` defines bounded tunable parameters, stable pack hashing, sanitization, and hard-cap exclusion.
- `parameter_packs` stores candidate/champion parameter payloads and holdout reports separately from legacy `agent_packs`.
- `kalshi_bot.learning.promotion_gates` evaluates the holdout gates for coverage, Brier, ECE, Sharpe, drawdown, city win-rate consistency, hard-cap touches, and idempotent hashes.
- `kalshi_bot.learning.drift_watcher` provides the calibration pause/search trigger for Brier, ECE, and realized-vs-predicted win-rate drift.
- `infra/config/hard_caps.yaml` is the operator-owned sealed hard-cap artifact, and `kalshi_bot.learning.hard_caps` loads, validates, and hashes it.
- `parameter-pack gate` loads the sealed hard-cap artifact and uses `max_drawdown_pct` as the promotion drawdown ceiling.
- `parameter-pack status` exposes staged rollout notes, recent packs, and parameter-pack promotion events; `parameter-pack stage` records a gated candidate into `parameter_packs`, `promotion_events`, and `deployment_control.notes.parameter_packs` without changing live color or risk; `parameter-pack canary` evaluates shadow-canary Brier/risk/source evidence; `parameter-pack promote-staged` lets an operator mark a canary-passed pack as champion metadata; `parameter-pack rollback-staged` clears that staged candidate as rejected.
- `infra/config/parameter_pack_default.yaml` mirrors the built-in default parameter pack for audit and operator review.
- The existing self-improve workflow still promotes agent packs until replay-backed parameter search is wired explicitly.

Default promotion settings:

- 30-year climatology normal with 14-day rolling smoother.
- `pseudo_count` search range `[2, 32]`.
- Nightly search budget `N=20`.
- Starvation tolerance `K=10`.
- 30-day holdout.
- Survival mode threshold: 25% of starting bankroll.
- Live autonomous promotion remains operator-only for the first 90 days after Phase 4.

### Phase 5 And 6: Optional Learned Features

CatBoost/River probability heads and NWS discussion parsing are optional. They must be deterministic given seed and data, must be operable at zero weight, and must beat the closed-form engine on holdout Brier, ECE, and realized trading metrics before receiving weight.

Phase 5/6 scaffolding now exists without adding live dependencies or changing behavior:

- `kalshi_bot.forecast.learned_head` defines the structured feature contract, CatBoost manifest validation, stable feature hashes, and a learned-probability blend capped at 0.5.
- `kalshi_bot.forecast.online_calibrator` provides a deterministic pure-Python logistic calibration state that can be replaced by River later without changing saved feature semantics.
- `kalshi_bot.forecast.nws_discussion_parser` validates strict NWS discussion JSON and returns neutral features for malformed or over-broad output.
- None of these modules are wired into the trading decision path; default learned weight remains zero.

## Autonomy Boundaries

The autonomy loop may tune only declared parameter-pack fields inside configured ranges. It may down-size, skip, pause entries, stage a candidate on the inactive color, or roll back a candidate pack after failed gates.

Operator-only actions remain:

- changing hard caps
- adding new cities or market families
- introducing new weather sources
- production schema migrations
- clearing kill-switch events
- first demo-to-live promotion
- disabling rollback or auto-kill conditions

## Acceptance Criteria

The system is not ready to graduate beyond Phase 0 until deterministic shadow decisions have complete traces and `decision-trace replay` verifies the normalized intent for saved regression decisions. Later phases must use those traces and the decision corpus as evidence; threshold tuning toward more trades is not acceptable without positive net expectancy after fees, slippage, fills, and settlement.
