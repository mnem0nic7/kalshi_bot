# Weather Temperature Taker Strategy

## Current Strategy

The current production weather strategy is `Strategy A`: directional trading on unresolved daily-high temperature contracts only.

It is intentionally conservative:

- structured weather markets only
- taker orders only
- no market making
- no web-only markets
- no resolved-contract cleanup trading
- deterministic fast path by default (`LLM_TRADING_ENABLED=false`)

## Layer 1: Weather State

The structured weather path computes a first-class `resolution_state`:

- `unresolved`
- `locked_yes`
- `locked_no`

For daily-high contracts:

- `>` / `>=` markets lock `yes` once the observed temperature has already met or exceeded the threshold
- `<` / `<=` markets lock `no` once the observed temperature has already exceeded the threshold

If a market is locked by observation, the strategy snaps fair value to `0.0000` or `1.0000` with confidence `1.0`.

## Layer 2: Fair Value

Only unresolved contracts use the forecast-driven model:

1. extract the daytime high forecast
2. compare it to the threshold
3. convert the gap to a probability with a logistic curve

Resolved contracts do not use the logistic model.

## Layer 3: Marketability

Before the trader can propose an order, the strategy runs deterministic eligibility checks:

- research freshness at decision time
- market quote freshness at decision time
- resolved-contract rejection
- maximum spread
- minimum payout left
- edge after a quality buffer

Explicit stand-down reasons are:

- `research_stale`
- `market_stale`
- `resolved_contract`
- `insufficient_edge_quality`
- `spread_too_wide`
- `insufficient_remaining_payout`

## Layer 4: Execution Policy

Only eligible unresolved setups may produce a live `TradeTicket`.

Current strategy modes:

- `directional_unresolved`
- `late_day_avoid`
- `resolved_cleanup_candidate`

The last mode is informational only in this slice. The platform does not trade resolved contracts under the current base strategy.

## Layer 5: Decision Trace

Every deterministic Strategy A decision persists a `decision_traces` row. The trace is written whether the result is an entry, a risk block, or a stand-down.

The trace includes:

- market and weather source references
- effective thresholds
- signal and eligibility state
- candidate trace and selected side
- sizing context
- risk verdict
- execution receipt when present
- final outcome plus stable input and intent hashes

Use `kalshi-bot-cli decision-trace replay <id>` to verify that the saved normalized intent still hashes to the recorded value.

## No-Trade Policy

The strategy stands down early when:

- research is stale
- market data is stale
- the contract is already resolved
- remaining payout is too small
- spread quality is poor
- the raw edge survives pricing math but fails quality gating

Risk remains the final authority, but the intent is for bad setups to stop at eligibility, not to drift all the way to downstream risk blocks.

## Exclusions

- market making
- news or social-data-driven trading
- unsupported settlement sources
- unmapped weather markets
- resolved-contract cleanup trading

## Canonical Regression

Room `a29a4ded-e1c0-4d16-ba48-1d432b415476` is the anchor regression for this strategy-hardening slice.

Interpretation:

- directional thesis: correct
- trade quality: weak
- preferred current behavior: explicit stand-down before ticketing
