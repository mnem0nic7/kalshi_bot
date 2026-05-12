# Weather Trading Strategy

Last updated: 2026-05-12

This document is the operator-facing summary of the weather trading system: what
we trade, which strategies are active, which gates must pass before an order can
go live, and what evidence is required before the system should be trusted at a
larger scale.

## Executive Summary

The active weather strategy is **Strategy A**: deterministic directional taker
trading on unresolved structured daily-high temperature markets.

The core rule is simple: trade only when the bot has fresh weather research,
fresh market quotes, an unresolved contract, a marketable taker side, enough
forecast separation, enough fee-adjusted edge, and enough remaining payout. All
orders pass through deterministic pre-risk eligibility, empirical behavior
checks, portfolio risk checks, execution locks, and reconciliation.

The live path is not LLM-driven. LLM roles may exist for research, critique, or
future agent-pack work, but production trading uses the deterministic fast path
with `LLM_TRADING_ENABLED=false`.

## Strategy Inventory

| Strategy | Status | Description | Live eligibility |
|---|---:|---|---|
| Strategy A | Active | Directional trading on unresolved structured weather contracts. | Live-capable after all gates pass. |
| Strategy B | Roadmap | Strategy A plus stricter microstructure filtering. | Not active; extend only after Strategy A evidence is stable. |
| Strategy C | Inactive | Resolution-lag cleanup trading after observations have effectively locked a result but market prices lag. | Shadow-only until separately approved with its own activation record. |
| Monotonicity arb | Inactive | Taker-only pair trading across inconsistent thresholds in the same station/day. | Shadow-only until separately approved. |

Strategy A must stay separate from Strategy C and monotonicity arb. Cleanup or
arbitrage logic should not be folded into the base directional path.

## Market Scope

The weather strategy only trades mapped structured Kalshi weather markets, mainly
daily high-temperature contracts such as `KXHIGH*` / `KXHIGHT*`.

In scope:

- structured daily high-temperature markets with a configured weather mapping
- live Kalshi market snapshots from stream/reconcile state
- NWS/Open-Meteo/weather archive inputs that are valid at decision time
- taker entries only

Out of scope:

- market making
- web-only or unmapped markets
- unsupported settlement sources
- social/news-driven trades
- resolved-contract cleanup under Strategy A
- manual bypass of risk, kill switch, active color, or credential gates

## Decision Flow

The production flow is:

1. Discover or stream active mapped weather markets.
2. Auto-trigger a room only if the market is actionable enough to inspect.
3. Build a weather bundle and market snapshot.
4. Score the market into fair value, confidence, edge, and resolution state.
5. Apply pre-risk trade eligibility gates.
6. Build a `TradeTicket` only for eligible unresolved setups.
7. Apply empirical behavior and deterministic risk gates.
8. Execute only if live mode, active color, write credentials, kill switch, and
   risk all permit it.
9. Reconcile positions/orders/fills and persist a decision trace.
10. Feed settled results back into audits, empirical gates, and promotion logic.

Every Strategy A decision should persist a `decision_traces` row so the decision
can be replayed and audited.

## Signal Model

The signal engine first classifies the weather contract resolution state:

| State | Meaning | Strategy A behavior |
|---|---|---|
| `unresolved` | The observed weather has not locked the contract. | Eligible for directional scoring. |
| `locked_yes` | Current observation already makes YES the final result. | No new Strategy A entry. |
| `locked_no` | Current observation already makes NO the final result. | No new Strategy A entry. |

For unresolved contracts, the model:

1. Extracts the relevant forecast high.
2. Compares forecast high to the market threshold.
3. Converts forecast separation into a fair YES probability.
4. Applies calibration, source disagreement, sigma, and optional model overlays.
5. Chooses the best taker side if the expected value clears quality gates.

Current supporting model features:

- source ensemble and source-disagreement widening
- nowcast high-so-far awareness
- sigma calibration, with station/YAML/global fallback
- optional residual and intraday models, gated by holdout quality and freshness
- deterministic trade-regime penalties for near-threshold and longshot setups

Trade regimes:

| Regime | Trigger | Live treatment |
|---|---|---|
| `standard` | Normal unresolved setup. | Allowed if all gates pass. |
| `near_threshold` | Forecast is within about 2 F of threshold. | Blocked for live Strategy A entries. |
| `longshot_yes` | Fair YES is at or below about 8%. | Blocked for live Strategy A entries. |
| `longshot_no` | Fair NO is at or below about 8%. | Blocked for live Strategy A entries. |

## Gate Stack

### 1. Auto-Trigger Gates

Auto-trigger decides whether a live market update should create a room.

Default/fallback controls:

| Gate | Default |
|---|---:|
| Auto rooms enabled | `trigger_enable_auto_rooms` |
| Cooldown per market | 300 seconds |
| Price-move bypass | 1500 bps |
| Max trigger spread | 250 bps |
| Max concurrent rooms | 12 |
| Active-room stale window | 1800 seconds |
| Marketability recheck interval | 60 seconds |
| Marketability waitlist TTL | 1800 seconds |

Auto-trigger blocks or waitlists markets when:

- market state is missing
- neither YES nor NO has a taker quote
- the book is one-sided
- spread is non-positive
- spread exceeds the active threshold
- max room concurrency is already reached
- cooldown has not elapsed and price movement does not justify bypass

Runtime agent packs may override `trigger_max_spread_bps` and
`trigger_cooldown_seconds`.

### 2. Source And Freshness Gates

Before a ticket can be generated:

- market quote age must be within `risk_stale_market_seconds` (default 60s)
- weather/research context must be within `research_stale_seconds` (default 900s)
- fair-value source must not be fallback, unavailable, dark, or none
- source-health pause must not be active
- entry pause must not be active

If source health degrades, `DeploymentControl.notes.source_health.pause_new_entries`
can pause new entries without stopping reconciliation or risk-reducing exits.

### 3. Pre-Risk Eligibility Gates

The strategy stands down before risk when any of these are true:

- research is stale
- market quotes are stale
- contract is already resolved by observed weather
- selected side has no taker quote
- book is effectively broken
- spread is too wide
- remaining payout is too small
- no actionable edge remains after the quality buffer
- forecast separation is below the configured minimum
- confidence is below the configured minimum
- setup is longshot or near-threshold
- historical heuristic policy forces stand-down

Important fallback thresholds:

| Gate | Fallback |
|---|---:|
| Minimum edge | `risk_min_edge_bps`, default 750 bps |
| Quality edge buffer | 25 bps |
| Maximum credible edge | 10000 bps |
| Minimum confidence | 0.80 |
| Minimum contract price | $0.25 |
| Probability midband exclusion | 25 percentage points around 50% |
| Minimum forecast separation | 8 F |
| Minimum remaining payout | 2000 bps ($0.20) |
| Max spread | `trigger_max_spread_bps`, default 250 bps |

These are fallback settings. The live source of truth is the active agent pack
plus deployment notes. Do not assume `.env` alone describes production behavior.

### 4. Empirical Behavior Gates

Empirical behavior gates compare a candidate entry bucket against actual settled
or closed fills in the recent lookback window.

Default controls:

| Gate | Default |
|---|---:|
| Empirical gate enabled | true |
| Lookback | 180 days |
| Minimum settled fills per bucket | 20 |
| Minimum net P/L per bucket | greater than $0.00 |
| Production entry freeze default | true |
| Freeze edge floor | 500 bps |

The active empirical identity is coarse bucket v2: strategy, series, station,
side, forecast-delta band, and coarse trade-quality band. Legacy keys with entry
price, confidence, and spread bands remain in traces for audit, but spread is no
longer part of the gate/bootstrap identity because it is already gated upstream.

Gate outcomes:

- `allowed`: enough settled evidence and positive net P/L
- `shadow_only`: insufficient or negative evidence outside production live entry
- `blocked`: insufficient or negative evidence for production live entry
- `production_frozen`: production entry freeze is active
- `disabled`: empirical gate disabled
- `not_applicable`: not a new buy entry

This gate prevents a visually attractive setup from going live when the same
bucket has not yet shown enough positive settled evidence.

### 5. Deterministic Risk Gates

Risk is the final authority before execution. It blocks:

- non-risk-reducing entries while kill switch is enabled
- source-health or entry-pause state
- missing recommended action/side/price
- non-unresolved contracts
- upstream eligibility failures
- edge below active minimum
- edge above active credibility ceiling, unless validated by the extreme-edge diagnostic
- confidence below minimum
- contract price below minimum
- remaining payout below minimum
- probability too close to 50% unless extra edge clears the midband requirement
- stale market or research data
- order count above cap
- add-ons when pyramiding is disabled
- opposite-side entry against an existing position
- per-ticker position count cap
- max concurrent open tickers
- non-standard trade regimes
- order notional cap
- projected position notional cap
- per-strategy daily realized-loss cap, if configured
- capital bucket exhaustion
- fee-adjusted net edge failure

Portfolio defaults:

| Control | Default |
|---|---:|
| Max order notional | 5% of live balance |
| Max position notional | 10% of live balance |
| Max concurrent tickers | 10 |
| Max order count | 200 contracts |
| Max position count per ticker | 200 contracts |
| Daily loss percent | 2% |
| Safe capital reserve | 0% |
| Risky capital max | 0% |

The safe/risky bucket ratios are effectively disabled today because risky
regimes are blocked upstream.

### 6. Execution Gates

Execution requires:

- `APP_SHADOW_MODE=false`
- target environment is `production` for live production trading
- production write credentials are present
- the app color matches `deployment_control.active_color`
- kill switch is off, except for explicitly risk-reducing exits
- post-clear reconcile gate has passed after any kill-switch clear
- execution lock can be acquired
- risk verdict is approved

Live execution submits to Kalshi through `POST /portfolio/orders`. Shadow mode
records the ticket and skips the API call.

Reconciliation runs every 60 seconds and records balances, positions, orders,
fills, settlements, and live tickers. The watchdog can auto-enable the kill
switch if active-color reconciliation is stale for more than 300 seconds.

## Readiness And Promotion Gates

There are two different questions:

1. Can the system technically place a live weather order?
2. Should the system promote new thresholds or scale trust?

Technical live readiness requires no hard blockers from:

- kill switch
- app shadow mode
- active color mismatch
- missing write credentials
- pending post-clear reconcile
- stale daemon heartbeat/reconcile
- source-health pause
- risk caps

Evidence-backed promotion requires a stricter corpus:

- enough complete rooms
- enough settled rooms
- enough trade-positive rooms
- enough market diversity
- enough full-checkpoint historical replay coverage
- enough holdout rows
- positive out-of-sample P/L
- drawdown not worse than champion
- acceptable calibration and source quality
- no hard-cap touches or source kill events

Historical baseline gate from operations docs:

| Support requirement | Minimum |
|---|---:|
| Execution-usable market-days | 60 |
| Full-checkpoint directional market-days | 30 |
| Full-checkpoint holdout days | 7 |

Autonomous gate tuning may stage weather policies globally or by scope
(`city`, `month`, `side`, `lane`). It must report either a passing dry run,
`no_candidate`, or an explicit safety failure. Operators should not hand-edit
`.env` to apply learned thresholds.

## Current Production Snapshot

As of 2026-05-12 checks from the production database:

- production shadow mode was off
- production entry freeze was not active
- kill switch was off
- write credentials were present
- active color was `blue`
- active agent pack was `auto-tightened-20260424T113407Z`
- active weather readiness fast check had no hard blockers
- weather had real executed Strategy A order/fill history
- current reconciled live position count was zero
- autonomous gate tuning was `no_candidate`, with 22 labeled rows versus a
  minimum support target of 30 and only 2 holdout rows
- decision-corpus auto-promotion was blocked by source provenance: 813 allowed
  full-checkpoint rows and 816 disallowed rows
- the latest successful decision corpus build had 337 rows across 3 market-days

Important caveat: this is a point-in-time snapshot. Always check current status
before changing live exposure.

Current active pack override observed on 2026-05-12:

| Runtime field | Active value |
|---|---:|
| `risk_min_edge_bps` | 150 |
| `trigger_max_spread_bps` | 1200 |
| `trigger_cooldown_seconds` | 300 |
| `risk_max_order_notional_dollars` | null |
| `risk_max_position_notional_dollars` | null |

Missing fields fall back to settings/defaults.

## Operator Commands

Read live readiness:

```bash
kalshi-bot-cli overnight-readiness report \
  --kalshi-env production \
  --domains weather \
  --json
```

Check deployment state:

```bash
kalshi-bot-cli status
kalshi-bot-cli reconcile
kalshi-bot-cli kill-switch on
kalshi-bot-cli kill-switch off
```

Check weather evidence and promotion readiness:

```bash
kalshi-bot-cli historical-status --verbose
kalshi-bot-cli training-status
kalshi-bot-cli trading-audit report --kalshi-env production --days 30 --json
kalshi-bot-cli autonomous-gates status --kalshi-env production --domain weather --format json
```

Run read-only autonomous gate validation:

```bash
kalshi-bot-cli autonomous-gates run \
  --kalshi-env production \
  --domain weather \
  --source combined \
  --days 3650 \
  --dry-run \
  --format json
```

Run a shadow room:

```bash
kalshi-bot-cli shadow-run <MARKET_TICKER>
kalshi-bot-cli shadow-campaign run --limit 3 --reason baseline_shadow_collection
```

Replay a decision trace:

```bash
kalshi-bot-cli decision-trace replay <TRACE_ID>
```

## Review Checklist Before Increasing Live Exposure

Before increasing weather exposure or promoting a new policy:

- active color is healthy
- daemon heartbeat and reconcile are fresh
- kill switch is off by operator intent
- post-clear reconcile checkpoint is newer than any kill-switch clear
- production write credentials are present
- no source-health pause is active
- latest trading audit has no money-safety blockers
- latest decision corpus promotion has no provenance failures
- autonomous gate dry run is pass or `no_candidate`, not an unexamined failure
- empirical buckets have enough settled evidence and positive net P/L
- historical replay has enough full-checkpoint directional support
- operator accepts the current caps and rollback path

## Related Docs

- [Weather Temperature Taker Strategy](weather-temp-taker.md)
- [Weather + Microstructure Roadmap](weather-microstructure-roadmap.md)
- [Strategy Page](strategy_page.md)
- [Kalshi Build Spec](kalshi_build_spec.md)
- [Operations](../operations.md)
- [Training](../training.md)
