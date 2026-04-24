# Decision Corpus PR 1 Design

## Problem

The current modeling and strategy analysis flow is too room-shaped and too win-rate-shaped for policy evaluation.

Rooms are useful narrative and audit containers, but they are not the statistical unit. A policy should be evaluated on the full as-of decision space: trades, stand-downs, missing fills, and risk-gated opportunities. Filled trades only are a policy-conditioned sample and therefore biased.

Win rate is also the wrong ranking target for Kalshi binary contracts. In a calibrated binary market, win rate is mechanically tied to entry price. A 95 cent favorite can have a high win rate and zero or negative expected value after fees and losses. PR 1 creates the substrate needed to move analysis toward supported, as-of, market-day-clustered PnL evaluation without changing bot behavior.

## Scope

PR 1 adds an append-only decision corpus for historical replay decisions.

It includes:

- migrations for `decision_corpus_builds` and `decision_corpus_rows`
- a corpus build service populated from historical replay rooms only
- current-build pointer mechanics through checkpoints
- CLI commands for build, dry-run, list, inspect, validate, promote, and current
- a narrow Kalshi taker fee helper used only by the corpus builder
- simple per-build support labels and provenance metadata
- tests for lifecycle, immutability, determinism, PnL semantics, cutoff, CLI, and pointer behavior

PR 1 is mergeable even if no corpus build has been promoted yet. Until a later consumer reads the corpus, it is idle infrastructure.

## Non-Goals

PR 1 does not change:

- signal generation
- risk rules
- execution
- supervisor workflow
- stop-loss behavior
- auto-trigger behavior
- strategy regression ranking
- strategy promotion
- live or shadow room capture
- control-room UI
- LLM prompts or agent-room behavior

PR 1 does not migrate existing hardcoded fee calculations in live trading or arb code. The new fee helper is introduced for corpus PnL computation only.

## Zero Behavioral Change Invariant

PR 1 must be verifiably additive.

Before merge:

- the existing test suite should pass without changed expectations
- the new fee helper should not be imported outside the corpus builder and its tests
- the migration downgrade should restore the pre-PR-1 schema cleanly
- a staging bot run should show no intentional changes to ops events, generated signals, risk verdicts, or orders

Reviewer checklist:

- no changes to `src/kalshi_bot/services/signal.py`
- no changes to `src/kalshi_bot/services/risk.py`
- no changes to `src/kalshi_bot/services/execution.py`
- no changes to `src/kalshi_bot/orchestration/supervisor.py`
- no changes to `src/kalshi_bot/services/strategy_regression.py`
- no new daemon loop or scheduled corpus build

If one of these files must change for mechanical wiring, the PR description must explain why the change is not behavior-facing.

## Data Model

### `decision_corpus_builds`

Build metadata is first-class and stored separately from rows.

Suggested fields:

- `id`
- `version`
- `created_at`
- `finished_at`
- `status`: `in_progress`, `successful`, or `failed`
- `git_sha`
- `source`: JSON
- `filters`: JSON
- `date_from`
- `date_to`
- `row_count`
- `parent_build_id`
- `failure_reason`
- `notes`

Build completion and build promotion are separate operations. A successful build is not current until the pointer is updated.

### `decision_corpus_rows`

Each row represents one as-of decision checkpoint from a historical replay room.

Minimum row fields:

- identity: `id`, `corpus_build_id`, `room_id`, `market_ticker`, `series_ticker`, `station_id`, `local_market_day`, `checkpoint_ts`
- environment and versions: `kalshi_env`, `deployment_color`, `model_version`, `policy_version`
- time features: `source_asof_ts`, `quote_observed_at`, `quote_captured_at`, `time_to_settlement_at_checkpoint_minutes`
- model output: `fair_yes_dollars`, `confidence`, `edge_bps`, `recommended_side`, `target_yes_price_dollars`, `eligibility_status`, `stand_down_reason`
- regimes: `trade_regime`, `liquidity_regime`
- support: `support_status`, `support_level`, `support_n`, `support_market_days`, `support_recency_days`, `backoff_path`
- outcomes: `settlement_result`, `settlement_value_dollars`
- PnL: `pnl_counterfactual_target_frictionless`, `pnl_counterfactual_target_with_fees`, `pnl_model_fair_frictionless`, `pnl_executed_realized`, `fee_counterfactual_dollars`
- quantities: `counterfactual_count`, `executed_count`
- fee metadata: `fee_model_version`
- provenance: `source_provenance`, `source_details`
- payloads: `signal_payload`, `quote_snapshot`, `settlement_payload`, `diagnostics`

Rows are immutable during normal operation. Rebuilds create a new build and new rows.

Recommended uniqueness within a build:

```text
corpus_build_id
room_id
market_ticker
checkpoint_ts
policy_version
model_version
```

Initial indexes:

- `(local_market_day, kalshi_env, policy_version)`
- `(series_ticker, local_market_day)`
- `(corpus_build_id)`

The corpus schema should not include `win_rate`, `win_count`, or `loss_count` columns. Those are aggregate report fields, not decision-row facts.

## Current Build Pointer

The current build is selected by checkpoint pointer, not by an `is_current` flag on build rows.

Checkpoint key:

```text
current_decision_corpus_build:{kalshi_env}
```

Consumers should either use a repository helper or a `current_decision_corpus_rows` view that dereferences the pointer once and returns only rows for the current successful build.

Expected behavior:

- no pointer set: current rows are empty
- pointer references a missing build: current rows are empty
- pointer references a failed or in-progress build: current rows are empty
- promoting an older successful build is rollback

Promotion is a single pointer update and should emit an ops event.

## Population Source

PR 1 builds from historical replay only:

- `historical_replay_runs`
- `rooms`
- `signals`
- trade tickets and fills where available
- `historical_settlement_labels`

Live and shadow rooms are intentionally deferred. Live rooms carry operator intervention, kill-switch, reconciliation, stop-loss, and streaming artifacts that need a separate capture design. Shadow rooms are closer to historical replay but still require hot-path integration decisions, so they also wait.

The build should be a full rebuild over its date range. Incremental builds are deferred.

Rows without settlement labels are excluded. The default cutoff should also avoid very recent unsettled market days, for example `local_market_day <= today - 2 days`, with the actual filter recorded in build metadata.

Builds must be deterministic and idempotent for the same inputs and code version. Iteration order must be stable. No sampling or wall-clock-dependent row content should enter the row hashable payload, except build metadata timestamps.

## Source Provenance

Each row must have non-null `source_provenance` plus flexible `source_details` JSON.

Initial enum:

- `historical_replay_full_checkpoint`
- `historical_replay_partial_checkpoint`
- `historical_replay_late_only`
- `historical_replay_external_forecast_repair`
- `historical_replay_unknown`

If consumers do not need to filter `late_only` separately, it may be collapsed into `historical_replay_partial_checkpoint` before implementation.

Mixed provenance uses the most conservative applicable label. External forecast repair wins over cleaner labels. Unknown is treated as less clean than known partial coverage.

`historical_replay_external_forecast_repair` means replay weather evidence included forecast-archive recovery. It must not be treated as observed intraday truth. Open-Meteo forecast archives are forecast evidence, not observation evidence.

`source_details` should include, where available:

- `checkpoints_present`
- `checkpoints_missing`
- `forecast_repair_fields`
- `repair_sources`
- `repair_confidence`
- `asof_nominal`
- `notes`

Enum evolution should be additive. Do not rename or delete existing values.

## PnL Semantics

All PnL fields are per-contract.

Policy-ranking PnL uses target or quote entry, not fair-value entry:

- `pnl_counterfactual_target_frictionless`: target entry, no fees
- `pnl_counterfactual_target_with_fees`: target entry minus estimated taker fee
- `fee_counterfactual_dollars`: the fee subtracted for the target-with-fees field

Model-diagnostic PnL uses fair-value entry:

- `pnl_model_fair_frictionless`: fair entry, no fees

Executed PnL uses actual fills:

- `pnl_executed_realized`: actual realized per-contract PnL net of known execution fees, null when no execution outcome exists

For a settled row:

```text
outcome_yes = 1 if settlement_result == "yes" else 0

YES target frictionless = outcome_yes - target_yes_price_dollars
NO target frictionless = (1 - outcome_yes) - (1 - target_yes_price_dollars)

YES fair frictionless = outcome_yes - fair_yes_dollars
NO fair frictionless = (1 - outcome_yes) - (1 - fair_yes_dollars)
```

Rows without a recommended side, without a target price, or without settlement result should have null counterfactual PnL fields. Stand-down rows are not zero-PnL rows. They are excluded from PnL ranking cohorts and remain useful for calibration and missed-opportunity analysis.

## Fee Helper

PR 1 adds a narrow taker-fee helper in `src/kalshi_bot/services/fee_model.py`.

Suggested API:

```python
def estimate_kalshi_taker_fee_dollars(
    *,
    price_dollars: Decimal,
    count: Decimal = Decimal("1"),
    fee_rate: Decimal,
) -> Decimal:
    ...
```

Formula:

```text
fee_rate * count * price_dollars * (1 - price_dollars)
```

The formula is symmetric, so either YES price `p` or NO price `1 - p` gives the same fee.

Config:

- `kalshi_taker_fee_rate: float = 0.07`

Version:

- `kalshi_taker_fee_v1`

The helper is taker-only. It does not model maker fees, rebates, volume tiers, or multi-leg orders.

Known hardcoded fee call sites should be documented in the helper module and migrated opportunistically when those modules are touched. PR 1 should not sweep live trading code.

## Support Labels

Support labels are descriptive per-build metadata. They are not shrinkage estimates.

Statuses:

- `supported`
- `exploratory`
- `insufficient`

Initial floors:

```text
supported:
  support_n >= 100
  support_market_days >= 20
  support_recency_days <= 365

exploratory:
  support_n >= 30
  support_market_days >= 10

otherwise:
  insufficient
```

Initial backoff levels:

1. `station_id + season_bucket + lead_bucket + trade_regime`
2. `station_id + season_bucket + lead_bucket`
3. `station_id + season_bucket`
4. `season_bucket + lead_bucket`
5. `global`

This ordering is an empirical starting choice. Document whether the implementation chooses to preserve station consistency or lead-time consistency first when backing off.

Bucket defaults:

- season bucket: meteorological seasons
- lead bucket: `imminent` 0-2h, `near` 2-6h, `mid` 6-12h, `far` 12-24h, `multi_day` 24h+

`support_level` should be enum-backed, for example:

- `L1_station_season_lead_regime`
- `L2_station_season_lead`
- `L3_station_season`
- `L4_season_lead`
- `L5_global`

`backoff_path` JSON entries should include:

- `level`
- `n`
- `market_days`
- `recency_days`
- `status`
- `failed_on`

All-levels-insufficient is a valid state. In that case `support_level` is `L5_global`, `support_status` is `insufficient`, and `backoff_path` shows every failed floor.

Support labels are computed from the build's own row set. The same logical decision may receive different support labels across different builds as the corpus grows.

## Immutability

The threat model is normal-operation immutability, not cryptographic immutability.

Primary enforcement:

- expose insert and read repository methods for corpus rows
- do not expose generic update, upsert, or delete methods for corpus rows

Defensive guard:

- row insertion is allowed only while the parent build is `in_progress`
- inserting into `successful` or `failed` builds is rejected

No DB triggers in PR 1. Cross-table triggers add dialect friction and can be introduced later if the threat model changes.

Raw SQL mutation is reserved for migrations or explicit ops intervention. Normal corrections should create a new build.

## Build Lifecycle

1. Create build with `status = in_progress`.
2. Populate rows under the build id.
3. Validate row content and support labels.
4. Mark build `successful`, set `finished_at`, and set `row_count`.
5. Inspect the build.
6. Promote by updating `current_decision_corpus_build:{env}`.

Failure path:

1. Mark build `failed`.
2. Set `finished_at` and `failure_reason`.
3. Do not update the current pointer.
4. Keep failed rows for debugging.

Completion is not promotion.

## CLI Surface

PR 1 commands:

```bash
kalshi-bot-cli decision-corpus build \
  --date-from 2026-01-01 \
  --date-to 2026-04-22 \
  --source historical-replay

kalshi-bot-cli decision-corpus build ... --dry-run
kalshi-bot-cli decision-corpus list-builds
kalshi-bot-cli decision-corpus inspect-build <build-id>
kalshi-bot-cli decision-corpus validate <build-id>
kalshi-bot-cli decision-corpus promote <build-id> --env demo
kalshi-bot-cli decision-corpus current --env demo
```

Useful flags:

- `--notes` on build
- `--json` on structured outputs
- `--status`, `--date-from`, `--date-to`, and `--limit` on list-builds

`promote` can target any successful build, including older builds for rollback. There is no separate demote command.

Build completion, build failure, and promotion should emit ops events.

## Tests

Hard blockers:

- deterministic rebuild with identical fixture inputs produces identical row content except build ids and timestamps
- PnL formulas are sign-correct for YES and NO and distinguish all PnL fields
- failed and in-progress builds cannot be promoted
- unsettled or settlement-missing rows are excluded
- current pointer is env-scoped and rollback works through promote-to-old
- completed-build rows are immutable through repository paths

Mandatory coverage:

- migrations create build and row tables with required constraints
- build lifecycle transitions
- current view or helper returns only pointed successful build rows
- concurrent promotes end in one consistent pointer state
- historical replay fixture becomes exactly one decision row
- support labels and all-levels-insufficient behavior
- source provenance is non-null and valid
- duplicate row identity within a build is rejected
- CLI dry-run writes no rows
- CLI JSON output is parseable
- validate catches required-field and PnL consistency failures
- repository does not expose row update, upsert, or delete methods
- fee helper symmetry, boundaries, scaling, precision, and known-value regression

Tests should use synthetic fixtures, not real DB dumps.

Fixture cases should cover:

- standard eligible YES decision
- eligible NO decision
- near-threshold stand-down
- longshot stand-down
- missing settlement
- low-support corpus

Browser/UI tests are out of scope for PR 1.

## Rollout

After PR 1 merges:

```bash
kalshi-bot-cli decision-corpus build \
  --date-from <start> \
  --date-to <settled-cutoff> \
  --source historical-replay

kalshi-bot-cli decision-corpus inspect-build <build-id>
kalshi-bot-cli decision-corpus validate <build-id>
kalshi-bot-cli decision-corpus promote <build-id> --env demo
kalshi-bot-cli decision-corpus current --env demo
```

PR 1 does not make regression read the corpus. PR 2 will be the first ranking consumer.

## Future Work

- PR 1.5: calibration report from a selected corpus build
- PR 2: strategy regression reads current corpus and ranks with `clustered_sortino_v1`
- PR 2: remove win rate from all ranking logic and keep it display-only
- live-shadow corpus capture
- live-trade corpus capture
- persisted station observation stream
- intraday observed-max settlement model
- orderbook event corpus with depth and sequence
- execution fill/slippage model
- direct profitability model as diagnostic research only
- corpus retention cleanup
- control-room corpus status tile

## Open Questions

- Should `historical_replay_late_only` remain a separate provenance enum or collapse into partial checkpoint?
- What exact season and lead bucket boundaries should be configurable in PR 1 versus hardcoded defaults?
- Should `liquidity_regime` stay null/unknown in PR 1 or be computed from top-of-book spread immediately?
- Should `pnl_executed_realized` be populated in PR 1 when historical replay has no real fills, or left null until live sources are added?
- What exact row hash or diff format should be used later for build comparison?
- Should retention cleanup be manual CLI, daemon task, or both when storage growth warrants it?
