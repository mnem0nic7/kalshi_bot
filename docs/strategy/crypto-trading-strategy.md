# Crypto Trading Strategy

Last updated: 2026-06-01

This document is the operator-facing summary of the crypto trading system: what
we trade, which strategy is active, which gates must pass before an order can go
live, and what evidence is required before any asset is promoted out of shadow
mode.

## Executive Summary

The active model-trained crypto strategies are **CRYPTO_15M** and **CRYPTO_1H**:
deterministic trading on Kalshi crypto markets using live Kalshi quotes,
spot/candle evidence, a per-asset prediction model, and a fee-aware candidate
selector.

There is also a separate non-model 15-minute Touch20 rules path. BTC keeps the
legacy strategy code `btc15m_touch20_rules`; ETH, SOL, XRP, BNB, DOGE, and HYPE
use separate asset lanes such as `eth15m_touch20_rules`. These lanes do not load
trained predictive model artifacts and do not call the crypto probability model.
Each lane can trade only after its own quote-path replay gate and matching
operator approval pass.

Crypto is not a single global live switch. It is promoted per asset. BTC can be
approved without approving ETH, and deployment-note asset mode `off` wins over
any agent-pack live mode.

The current production posture is BTC-only live eligibility with conservative
entry gates. Other crypto assets remain shadow evidence collection until their
own replay, P/L, and asset-mode gates pass.

## Strategy Inventory

| Strategy | Status | Description | Live eligibility |
|---|---:|---|---|
| `CRYPTO_15M` live-quality | Active shadow | Predict fair YES for 15-minute crypto markets and select the best fee-adjusted YES/NO buy candidate. | Per-asset live only after replay, asset-mode, risk, and execution gates pass. |
| `CRYPTO_1H` live-quality | Active shadow | Predict fair YES for 1-hour crypto markets and select the best fee-adjusted YES/NO buy candidate. | Same per-asset live gates as `CRYPTO_15M`; ongoing collection runs in the crypto-only 1h daemon with `CRYPTO_AUTO_FREQUENCIES=1h`. |
| `*_15m_touch20_rules` | Disabled by default except explicitly enabled lanes | Independent 15-minute, non-model Touch20 path for BTC, ETH, SOL, XRP, BNB, DOGE, and HYPE. It enters on a standalone rules score plus replay bucket evidence, exits at +20% net executable profit, and cuts at -20% net executable loss. | Requires the asset lane enabled, a passed asset-owned gate such as `btc15m_touch20_rules_gate:15m:BTC` or `eth15m_touch20_rules_gate:15m:ETH`, gate-version-matched operator approval, active color, kill switch off, fresh real quotes and spot, strategy cap room, and asset-lane `trading_enabled=true` before live order submission. |
| Crypto exploratory shadow | Active shadow | Collect candidate and quote evidence even when live edge is not present. | Never live eligible; evidence only. |
| Per-asset policy promotion | Active control path | Stage asset-specific crypto policy in the active agent pack. | One asset at a time after live-path readiness passes. |
| Crypto market making | Out of scope | Posting two-sided liquidity. | Not supported. |
| Spot/perp trading | Out of scope | Trading on external crypto venues. | Not supported. |

The live path must treat `live_quality` and `exploratory_shadow` differently.
Only `live_quality` candidates can reach live execution.

## Market Scope

Crypto trading applies to Kalshi 15-minute and 1-hour crypto markets such as:

- `KXBTC15M*`
- `KXETH15M*`
- `KXSOL15M*`
- `KXXRP15M*`
- `KXDOGE15M*`
- `KXBNB15M*`
- `KXHYPE15M*`

Current tracked assets:

| Asset | Primary treatment |
|---|---|
| BTC | Standard tracked asset. |
| ETH | Standard tracked asset. |
| SOL | Standard tracked asset. |
| XRP | Standard tracked asset. |
| DOGE | Tracked, but requires enough labeled real-quote evidence. |
| BNB | Coinbase-supported tracked asset; keep shadow until its own data/replay gates pass. |
| HYPE | Coinbase-supported tracked asset; keep shadow until its own data/replay gates pass. |

In scope:

- Kalshi 15-minute crypto markets
- live Kalshi bid/ask quote snapshots
- market candlesticks from Kalshi history
- spot OHLC/price evidence from Coinbase; proxy fallback is disabled by default
- Coinbase current tick, best-bid/ask, and recent trade microstructure for supported assets
- per-asset model, backtest, replay-gate, and live-path artifacts
- per-asset live/shadow/off controls

Out of scope:

- external exchange execution
- manual bypass of replay gates
- market making
- proxy-only spot assets going live
- global promotion of all assets at once
- live execution while app shadow, kill switch, inactive color, or missing
  credentials are present

## Decision Flow

The production flow is:

1. Discover open 15-minute crypto markets.
2. Filter to eligible markets with enough time before close.
3. Cap room creation by total rooms and per-asset rooms.
4. Resolve per-asset mode from deployment notes and active crypto policy.
5. Create a crypto room in shadow or live-eligible mode.
6. Persist Kalshi quote, candle, and spot evidence.
7. Load the latest per-asset model, backtest, and replay gate.
8. Predict fair YES probability and generate YES/NO buy candidates.
9. Classify the selected candidate as `live_quality`, `exploratory_shadow`, or
   blocked.
10. Build a 1-contract `TradeTicket` only for a tradeable signal.
11. Apply deterministic risk with runtime crypto thresholds.
12. Execute only if asset mode, replay gate, live switches, active color, kill
   switch, credentials, risk, and candidate status all permit it.
13. Reconcile orders/fills/positions and feed settled results back into replay,
   model quality, and promotion gates.

Every crypto decision should persist enough candidate trace data to recover the
model version, backtest version, replay-gate status, runtime crypto policy,
asset mode, selected side, expected fee, expected net edge, and live eligibility.

## Crypto 15m Non-Model Touch20 Rules Path

The non-model 15m Touch20 rules process is additive. It runs beside the existing
crypto daemons and must not disable, retune, or gate the model-trained crypto
paths. It is scoped to supported 15-minute crypto assets only: BTC, ETH, SOL,
XRP, BNB, DOGE, and HYPE. BTC uses `btc15m_touch20_rules`; non-BTC lanes use
their own strategy codes, such as `eth15m_touch20_rules`.

### Objective

The objective is `touch_20pct_before_close`: buy a YES or NO contract early
enough in the 15-minute market that the contract price can fluctuate upward, then
sell when the executable exit price clears +20% net profit after estimated entry
and exit taker fees.

This is not a settlement-edge strategy. A position can be profitable even if the
contract later settles out of the money, provided the contract touches the exit
target before close and the dedicated exit loop fills.

### Non-Model Boundary

The path may use:

- live Kalshi bid/ask quote snapshots
- fresh non-proxy asset spot features
- standalone deterministic rules scoring
- replay-derived bucket evidence from settled quote paths
- deployment control, active color, kill switch, credentials, and execution
  safety already used by production

The path must not use:

- trained crypto prediction calls
- trained model probability artifacts as entry authority
- model feature predictions
- settlement replay gates from `CRYPTO_15M`
- BTC 1-hour Touch20 gates
- global crypto take-profit exits for attribution

The candidate payload uses standalone `rule_score` and `score_components`
fields. It does not populate trained-model probability fields as entry
authority.

### Runtime Process

The Docker service is `crypto_non_model_btc15m_touch20_production`. Each loop
runs over `CRYPTO_15M_TOUCH20_RULES_ASSETS`, default `BTC`:

1. `crypto-non-model-touch20 exit-once` for each configured asset
2. `crypto-non-model-touch20 run-once` for each configured asset
3. sleep for `CRYPTO_BTC15M_TOUCH20_LOOP_INTERVAL_SECONDS`, default 15 seconds

The exit pass runs first so existing strategy-owned positions get a chance to
take profit before the process considers a new entry.

### Entry Gate Order

The entry loop returns without submitting an order unless every required gate
passes:

1. CLI scope is a supported `--frequency 15m` asset.
2. The asset lane has `rules_enabled=true`; BTC uses
   `CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true`.
3. The running app color equals deployment control `active_color`.
4. Deployment kill switch is off.
5. The latest asset-owned gate artifact exists and is passed.
6. The operator approval checkpoint exists and references that exact gate
   version.
7. Strategy-only realized P/L for the current UTC day is above the daily loss
   limit. Default loss limit is `$10`.
8. At least one latest asset 15m snapshot is fresh under
   `CRYPTO_BTC15M_TOUCH20_QUOTE_FRESH_SECONDS`.
9. Market status is open or active.
10. YES bid, YES ask, NO bid, and NO ask are all present.
11. Asset spot data is fresh under `CRYPTO_BTC15M_TOUCH20_SPOT_FRESH_SECONDS`
    and the spot source is not proxy-only.
12. Candidate status is `live_quality` under the standalone rules engine.
13. Candidate replay bucket is present in the gate artifact's
   `allowed_bucket_keys`.
14. Candidate `rule_score` is at least `CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE`.
15. Strategy-owned open plus pending notional is below
   `CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS`, default `$10`.
16. The asset lane has `trading_enabled=true`; BTC uses
    `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true`.

If the first rules flag is true but the trading flag is false, the path can
select and log a candidate but returns `trading_disabled` and submits no order.
That is the intended tiny-live staging mode before actual execution.

### Candidate Construction

The strategy evaluates both sides of each candidate market:

| Side | Entry cost | Exit quote used later |
|---|---:|---:|
| YES | executable YES ask | executable YES bid |
| NO | executable NO ask | executable NO bid, represented as `1 - YES ask` in the exit loop |

For each side the entry selector computes:

- executable entry cost
- fee-aware exit price required for +20% net profit
- price-band, spread-band, and time-to-close bucket
- standalone rules score
- expected return using +20% touch upside and the configured risk settings
- bucket key used to match replay evidence

The standalone rules score combines:

- replay bucket touch rate
- replay bucket P/L per candidate
- short-term asset spot momentum in the candidate side's direction
- recent asset spot volatility
- remaining time
- target-gap closeness
- spread quality

The default minimum score is `0.60`.

### Entry Candidate Blocks

A candidate is blocked when any of these are true:

- strict real quote evidence is missing
- spot data is stale, missing, or proxy-only
- executable quote is missing
- entry cost is below the minimum contract price
- the +20% fee-aware target price is impossible
- the target exit price is at or above `$1.00`
- spread is above the tier limit
- market age is below 60 seconds
- time to close is below 300 seconds
- standalone rule score is below the configured minimum
- replay bucket is not allowed by the separate rules gate
- strategy cap or daily loss cap is exhausted

The current entry window intentionally preserves the final 5-minute entry block:
`time_to_close_seconds < 300` is too late for new entries.

### Spread And Price Limits

Default spread limits are strategy-local hard limits:

| Contract price | Max spread |
|---:|---:|
| under `$0.20` | 1 cent |
| `$0.20` and above | 2 cents |

The entry ask must be at least `$0.10` by default through
`CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS`. The calculated fee-aware
target exit side price must be below `$1.00`.

### Candidate Ranking

After filtering, candidates are ranked by:

1. replay bucket P/L per candidate
2. replay bucket touch rate
3. standalone rule score
4. tighter spread
5. more remaining time

Only the top ranked candidate is submitted in a single `run-once` cycle.

### Order And Ledger Attribution

Entry orders use strategy prefix `b15t20r:e:` and strategy code
`btc15m_touch20_rules` for BTC. Non-BTC lanes use their own prefixes and
strategy codes, such as `eth15t20r:e:` and `eth15m_touch20_rules`. Exit orders
use the same lane prefix with `:x:`.

The strategy maintains its own checkpoint ledger:

```text
btc15m_touch20_rules:<kalshi_env>:BTC:15m
```

Non-BTC lanes maintain separate ledgers, for example:

```text
eth15m_touch20_rules:<kalshi_env>:ETH:15m
```

That ledger is the source of truth for strategy-owned open notional, pending
notional, daily realized P/L, and dedicated exits. Exchange positions are
aggregated by market, so this path must not infer ownership from aggregate
exchange position alone. Manual trades and current model-bot trades may overlap
the same market, but they should not count against this path's `$10` cap unless
they are recorded under the `b15t20r:` prefix in the strategy ledger.
For non-BTC lanes, the equivalent asset prefix, such as `eth15t20r:`, is the
ownership boundary.

### Sizing And Risk

The default maximum open plus pending notional is `$10`, controlled by
`CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS`. The entry size is the largest
count that fits inside the remaining strategy cap at the selected entry cost.

This cap is strategy-local. It does not block existing bot exposure and it does
not use existing bot exposure to reduce the strategy cap. Global execution
safety, credentials, active color, kill switch, and exchange errors still apply.

### Exit Logic

The dedicated exit loop only evaluates ledger entries owned by this strategy.
It does not globally enable 15-minute take-profit behavior for all crypto
positions.

For each open strategy entry, the exit loop:

1. loads the latest quote snapshot for the market
2. computes the executable sell price for the owned side
3. estimates entry and exit taker fees
4. computes net executable profit percentage
5. updates profit-protection state
6. submits a risk-reducing close only when an exit trigger is present

Exit triggers:

| Trigger | Rule |
|---|---|
| `take_profit` | net executable profit is at least `CRYPTO_BTC15M_TOUCH20_TAKE_PROFIT_PCT`, default `0.20` |
| `stop_loss` | net executable profit is at or below negative `CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT`; default setting `0.20` triggers at `-0.20` |
| `profit_protection_floor` | profit protection is armed and profit falls to or below `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_FLOOR_PCT`, default `0.05` |
| `profit_protection_adverse_momentum` | profit protection is armed, quote profit declines from the prior observation, and asset spot momentum is adverse across short windows |

Profit protection arms only after net executable profit first reaches
`CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_THRESHOLD_PCT`, default `0.10`.
The stop loss is evaluated by the dedicated exit loop from current executable
quotes; it is not a resting exchange-side stop order.

Exit submissions use the existing close-position path with
`allow_risk_reducing_exit=True`. If an exit order is cancelled, expired, or
unfilled-cancelled, the ledger keeps the entry open and waits 60 seconds before
retrying. If an exit is submitted but not immediately closed, the status becomes
`exit_submitted` and is rechecked after the cooldown.

### Replay Gate

Replay, gate, and approval are owned by `crypto-non-model-touch20`, not the
generic `crypto-replay` command.

Replay uses settled real quote-path rows only. It does not use proxy rows,
trained model features, or trained model predictions. For each historical
candidate row it scans future same-market quote snapshots before close:

1. If the candidate side first touches the fee-aware +20% target, replay
   simulates a take-profit exit.
2. If no touch occurs before close, replay simulates holding to settlement.
3. Results are grouped into replay buckets used by live selection.

Each lane's gate artifact is separate from model and 1-hour Touch20 gates:

```text
btc15m_touch20_rules_gate:15m:BTC
```

Non-BTC example:

```text
eth15m_touch20_rules_gate:15m:ETH
```

Live entry also requires the lane's own approval checkpoint:

```text
btc15m_touch20_rules_approval:<kalshi_env>:BTC:15m
```

Non-BTC example:

```text
eth15m_touch20_rules_approval:<kalshi_env>:ETH:15m
```

The approval checkpoint must reference the latest passed gate version. A new
gate version invalidates old approval.

Gate pass requirements:

| Gate | Requirement |
|---|---:|
| Strategy | asset-specific, for example `btc15m_touch20_rules` or `eth15m_touch20_rules` |
| Uses trained model | false |
| Real quote-path evidence | present |
| Minimum candidates | 50 |
| Net simulated P/L | greater than `$0.00` |
| P/L per candidate | at least `$0.01` |
| Touch rate | at least 25% |
| Hard-cap breaches | 0 |
| Allowed buckets | at least one |

Missing, negative, undersampled, proxy-only, trained-model-tainted, or
no-allowed-bucket artifacts block live entry.

## Signal Model

The signal engine loads the latest per-asset model and converts the live market
row into a fair YES probability. The current model path may use a trained
market-mid baseline, heuristic adjustment, calibration metadata, and rich crypto
features from Kalshi quotes, candles, and spot inputs.

For each market it evaluates both sides:

| Candidate side | Cost input | Probability input |
|---|---:|---:|
| YES buy | YES ask | predicted YES |
| NO buy | NO ask | `1 - predicted YES` |

For each side the selector computes:

- raw edge: predicted payoff probability minus execution cost
- estimated Kalshi taker fee
- expected net edge after fee
- remaining payout
- spread bucket and price bucket
- candidate status

Candidate statuses:

| Status | Meaning | Live treatment |
|---|---|---|
| `live_quality` | Fee-adjusted edge clears the active live threshold and all entry gates pass. | May proceed to risk and live execution if all other gates pass. |
| `exploratory_shadow` | Useful for evidence collection, but live edge or policy support is insufficient. | Shadow only. |
| `prediction_only_proxy_quote` | Row lacks strict real bid/ask quote evidence. | Shadow or blocked; never live. |
| `blocked_fee_edge` | Fee-adjusted edge is below live minimum. | Blocked for live. |
| `unfillable` | Required quote is missing. | Blocked. |

The candidate selector prefers `live_quality` over `exploratory_shadow`, then
ranks by expected net edge.

## Gate Stack

### 1. Autonomy And Discovery Gates

Crypto autonomy decides whether to create crypto rooms from open markets.

Default/fallback controls:

| Gate | Default |
|---|---:|
| Crypto enabled | true |
| 15-minute crypto enabled | true |
| Crypto trading enabled | false |
| Crypto autonomy enabled | false |
| Production crypto autonomy enabled | false |
| Quote evidence enabled | true |
| Minimum seconds to close | 0 seconds |
| Max rooms per run | 7 |
| Max rooms per asset per run | 1 |
| Autonomy interval | 30 seconds |

In production, quote evidence mode can still collect shadow evidence when
production autonomy is disabled, provided quote evidence is enabled. That mode
must not be mistaken for live order readiness.

Autonomy blocks or skips when:

- crypto or 15-minute crypto is disabled
- production autonomy is not enabled and shadow evidence mode is not applicable
- app color is not the active deployment color
- asset mode is `off`
- the market is too close to close or already has a room
- room caps are reached
- the asset is not live-eligible and shadow evidence is not allowed

### 2. Data And Source Gates

Before a candidate can be trusted for live mode:

- Kalshi market quote data must be present
- strict real bid/ask quote rows must exist
- proxy-only quote rows cannot qualify for live trade quality
- crypto candlesticks must exist
- spot OHLC/price evidence must be fresh
- spot feature coverage must be at least 80%
- replay rows must be point-in-time, with zero leakage rows
- per-asset model/backtest/replay artifacts must exist
- the replay gate must be evaluated against the active runtime crypto policy

BNB and HYPE previously required special caution because spot support was
proxy-only. As of May 12, 2026, the Coinbase CDP-backed product check
returns online `BNB-USD` and `HYPE-USD`, so they can collect non-proxy Coinbase
spot ticks. They still remain shadow until their own strict-row, replay, P/L,
and asset-mode gates pass.

### 3. Entry Candidate Gates

The live-quality candidate path blocks when:

- the row is not strict trade eligible
- selected side has no fillable ask quote
- spread exceeds the active max spread
- contract price is below the minimum
- raw edge exceeds the maximum credible edge
- fee-adjusted expected net edge is below the active minimum
- market age is below 180 seconds or the market has already closed

Current runtime entry thresholds observed in production on 2026-05-14:

| Gate | Active value |
|---|---:|
| Minimum fee-adjusted edge | 500 bps |
| Max spread | 1000 bps |
| Minimum confidence | 0.80 |
| Minimum contract price | $0.50 |
| Minimum remaining payout | disabled (0 bps) |
| Maximum credible edge | 10000 bps |
| Minimum market age | 180 seconds |
| Minimum seconds to close | 0 seconds |

Shadow exploration controls:

| Gate | Default |
|---|---:|
| Max shadow candidates per run | 12 |
| Max shadow candidates per asset per run | 2 |
| Minimum shadow expected net edge | -$0.03 |
| Max shadow spread | 500 bps |

Shadow exploration is for evidence collection only. It must not be used as a
live entry rationale.

### 4. Replay And Promotion Gates

Replay is the hard evidence gate for live crypto. A per-asset replay gate must
pass before live execution is allowed.

Runtime replay requirements:

| Gate | Requirement |
|---|---:|
| Minimum resolved markets | 500 |
| Minimum out-of-sample trade candidates | 50 |
| Minimum current-model live-quality candidates | 50 |
| Minimum net simulated P/L | greater than $0.00 |
| P/L versus market-mid | model selected-candidate P/L must beat market-mid P/L |
| Maximum hard-cap breaches | 0 |
| Minimum spot coverage | 80% |
| Calibration versus market-mid | diagnostic only: Brier, log-loss, and ECE are reported |
| Candles | required |
| Point-in-time rows | required |
| Strict real-quote trade quality | required |

The `crypto-live-path` readiness command uses an additional operator target of
at least 60 strict labeled real-quote rows per asset before promotion.

Replay blocks live mode when any of these are true:

- model artifact is missing
- backtest artifact is missing
- candlestick coverage is missing
- leakage rows are present
- spot coverage is below 80%
- strict real-quote row count is below the trade-candidate target
- resolved sample count is below 500
- out-of-sample trade candidate count is below 50
- current-model live-quality candidate count is below 50
- simulated net P/L is not positive
- simulated net P/L does not beat market-mid P/L
- hard-cap breaches are present
- calibration diagnostics are missing from the replay report

### Settlement Backfill

Recent 15-minute crypto settlements are collected from the regular Kalshi
markets endpoint with `status=settled`, not only from the historical markets
endpoint. The historical endpoint can lag recent 15-minute markets; the settled
markets endpoint supplies current finalized rows with `result=yes|no`.

`crypto-history collect-settled` appends terminal label snapshots with
`source_kind=settled_backfill`. It does not mutate pre-close
`live_quote_evidence` rows. Replay joins the terminal label snapshot to earlier
quote snapshots by `market_ticker`, preserving point-in-time quote evidence.

Live decision traces record Kalshi crypto settlement as the CFB RTI 60-second
average benchmark. Coinbase-derived rows are marked as a proxy for that
benchmark, so readiness and replay reports can separate model input quality from
the actual settlement source.

Settled backfill also attempts candle capture for each settled market. It tries
the regular market candlestick endpoint first and falls back to the historical
candlestick endpoint. Candle failures are reported, but they do not block label
storage.

### 5. Asset Mode And Live Switch Gates

Per-asset mode can be:

| Mode | Meaning |
|---|---|
| `off` | Hard disabled. Deployment-note `off` wins over policy. |
| `shadow` | Rooms and evidence may be created, but no live order should be placed. |
| `live` | Asset may proceed to live execution only if every other gate passes. |

Live order readiness also requires:

- `APP_SHADOW_MODE=false`
- `CRYPTO_TRADING_ENABLED=true` or active runtime crypto policy trading enabled
- production crypto autonomy enabled for autonomous live production entry
- target app color equals `deployment_control.active_color`
- kill switch is off
- write credentials are present
- per-asset replay gate passes
- selected candidate status is `live_quality`
- deterministic risk approves the ticket

Runtime crypto flags may satisfy crypto trading and production-autonomy checks,
but they do not override disabled crypto settings, app shadow mode, inactive
color, missing credentials, stale data, kill switch, replay failure, or risk
caps.

### 6. Deterministic Risk Gates

Risk is the final authority before execution. It evaluates the crypto ticket
with runtime crypto thresholds, not just raw settings.

Risk blocks:

- missing recommended action/side/price
- upstream eligibility failure
- candidate edge below active minimum
- candidate edge above active credibility ceiling
- confidence below minimum
- contract price below minimum
- spread above active maximum
- stale market data
- kill switch for non-risk-reducing entries
- order count above cap
- position add-ons when disabled
- opposite-side entry against an existing position
- per-ticker position count cap
- max concurrent tickers
- order notional cap
- projected position notional cap
- daily realized-loss cap, if configured
- capital bucket exhaustion
- fee-adjusted net edge failure

Portfolio defaults:

| Control | Default |
|---|---:|
| Default crypto order size | 1 contract |
| Max order notional | 5% of live balance |
| Max position notional | 10% of live balance |
| Max concurrent tickers | 10 |
| Max order count | 500 contracts |
| Max position count per ticker | 200 contracts |
| Daily loss percent | 20% |
| Safe capital reserve | 0% |
| Risky capital max | 0% |

### 7. Execution Gates

Crypto execution supports `passive_only` and `passive_then_taker`. Production
crypto uses passive-first execution with taker fallback disabled for normal
candidates:

1. Submit a passive maker-style order one tick inside the quoted spread.
2. If the passive order fills or receives a terminal non-retry status, stop.
3. In `passive_only`, if the passive order is unfilled/cancelled or loses edge
   on requote, emit `passive_unfilled_no_taker`.
4. In `passive_then_taker`, normal taker fallback is controlled by
   `crypto_taker_fallback_close_seconds`. Production sets this to `0`, so normal
   edge trades do not cross the spread after an unfilled passive attempt.
5. Late high-confidence directional entries may still fall back to taker inside
   `crypto_late_sure_thing_max_seconds_to_close` when the candidate remains
   live-quality. The standard late window is 180 seconds; entries from 180-300
   seconds before close must have model probability at least 0.90. Inside the
   standard window, near-strike entries with recent spot momentum against the
   selected side also require at least 0.90 model probability. In the extended
   late window, recent adverse spot momentum blocks the late path, and available
   target-distance features must show at least 3 volatility units of directional
   cushion.
6. Last-minute passive market-confidence entries are a separate final-60s path:
   they use the Kalshi market-implied side probability, choose a passive bid
   from the learned final-minute price matrix when a mature profitable row is
   available, otherwise fall back to the configured asset threshold, do not
   requote, do not use taker fallback, and cancel after close if still open.

Execution blocks live orders when:

- asset mode is not `live`
- candidate status is not `live_quality`
- crypto trading is disabled
- replay gate is missing or blocked
- base execution rejects the ticket after requote or risk checks

Shadow mode records the room, signal, ticket, risk verdict, and skipped receipt
without submitting to Kalshi.

## Readiness And Promotion Gates

There are three different questions:

1. Can the bot collect crypto evidence?
2. Can a specific asset be promoted to live mode?
3. Should production live switches be enabled?

Evidence collection readiness requires:

- crypto and 15-minute crypto enabled
- quote evidence enabled
- active color healthy enough to run the workflow
- spot and Kalshi data collection jobs fresh
- room creation in shadow mode working

Per-asset live readiness requires:

- `crypto-live-path status` reports the asset ready
- asset mode intentionally set to `live`
- at least 60 strict labeled real-quote rows
- replay gate passed
- at least 50 replay trade candidates
- positive simulated net P/L
- simulated net P/L beats market-mid P/L
- calibration diagnostics are present for Brier, log-loss, and ECE
- spot coverage at least 80% and fresh
- model status trained
- no proxy-only spot dependency for live assets
- no hard-cap breaches

Production live switch readiness requires:

- at least one asset is explicitly promoted to `live`
- `CRYPTO_TRADING_ENABLED=true`
- `CRYPTO_AUTONOMY_ENABLED=true`
- `CRYPTO_PRODUCTION_AUTONOMY_ENABLED=true`
- app shadow mode remains off
- active color, kill switch, credentials, and reconcile checks are clean
- operator accepts current caps and rollback path

Do not enable global production crypto switches before at least one asset has
passed per-asset readiness and has been intentionally promoted.

## Current Production Snapshot

As of 2026-05-12 checks from the production runtime and database:

- crypto live-path status was `collecting`
- no requested assets were ready for live mode
- no requested assets were live-order ready
- production app shadow mode was off
- kill switch was off
- write credentials were present
- active color was `blue`
- crypto and 15-minute crypto were enabled
- `CRYPTO_TRADING_ENABLED=false`
- `CRYPTO_AUTONOMY_ENABLED=false`
- `CRYPTO_PRODUCTION_AUTONOMY_ENABLED=false`
- runtime crypto trading and production autonomy were false
- all tracked asset modes resolved to shadow
- `CRYPTO_15M` had 40 blocked trade tickets and no `CRYPTO_15M` live orders
- crypto shadow evidence had 183 complete rooms and 1 failed room

Latest live-path blockers by asset:

| Asset | Strict labeled real-quote rows | Resolved sample | Trade candidates | Replay gate | Model status | Notes |
|---|---:|---:|---:|---|---|---|
| BNB | 3 | 3 | 0 | blocked | insufficient data | Coinbase spot now supported; needs fresh strict rows and replay candidates. |
| BTC | 16 | 16 | 0 | blocked | trained | P/L and candidate support not ready. |
| DOGE | 20 | 24 | 0 | blocked | insufficient data | Candidate support not ready. |
| ETH | 19 | 23 | 0 | blocked | trained | P/L and candidate support not ready. |
| HYPE | 27 | 37 | 0 | blocked | insufficient data | Coinbase spot now supported; needs fresh strict rows and replay candidates. |
| SOL | 27 | 37 | 0 | blocked | trained | P/L and candidate support not ready. |
| XRP | 27 | 37 | 0 | blocked | trained | P/L and candidate support not ready. |

Spot coverage was fresh and at 100% in the latest live-path report. BNB and HYPE
were previously proxy-only, but Coinbase CDP now exposes online `BNB-USD` and
`HYPE-USD` products and current non-proxy ticks. The dominant blockers remain
strict real-quote support, resolved-sample support, zero replay trade candidates,
non-positive simulated P/L, and model P/L not beating market-mid P/L.

Important caveat: this is a point-in-time snapshot. Always rerun
`crypto-live-path status` and check deployment health before changing live
exposure.

## Operator Commands

Read crypto live-path status:

```bash
kalshi-bot-cli crypto-live-path status \
  --kalshi-env production \
  --frequency 15m \
  --assets all \
  --baselines \
  --json
```

Refresh production evidence:

```bash
scripts/crypto_live_path_refresh.sh \
  --kalshi-env production \
  --frequency 15m \
  --settled-days 2 \
  --history-days 2 \
  --spot-days 2 \
  --replay-days 30 \
  --assets all \
  --docker-container infra-app_production_blue-1
```

Omit `--assets` or use `--assets all` to discover open Kalshi crypto assets for
the requested frequency. Explicit asset lists remain exact.

Repair recent settlement labels directly:

```bash
kalshi-bot-cli crypto-history collect-settled \
  --kalshi-env production \
  --frequency 15m \
  --days 2 \
  --assets BTC \
  --json
```

Set a single asset mode:

```bash
kalshi-bot-cli crypto-asset-mode list --kalshi-env production
kalshi-bot-cli crypto-asset-mode set --kalshi-env production BTC live
kalshi-bot-cli crypto-asset-mode set --kalshi-env production BTC shadow
kalshi-bot-cli crypto-asset-mode set --kalshi-env production BTC off
```

Run collection/model/replay checks:

```bash
kalshi-bot-cli crypto-history status --kalshi-env production --frequency 15m
kalshi-bot-cli crypto-spot status --kalshi-env production --frequency 15m
kalshi-bot-cli crypto-model train --kalshi-env production --frequency 15m --assets BTC
kalshi-bot-cli crypto-replay run --kalshi-env production --frequency 15m --assets BTC --days 30
kalshi-bot-cli crypto-replay validate --kalshi-env production --frequency 15m --assets BTC --days 30
kalshi-bot-cli crypto-replay gate --kalshi-env production --frequency 15m --assets BTC
```

Run autonomy manually:

```bash
kalshi-bot-cli crypto-autonomy run-once \
  --kalshi-env production \
  --frequency 15m \
  --assets BTC ETH SOL XRP
```

Check autonomous gate state:

```bash
kalshi-bot-cli autonomous-gates status \
  --kalshi-env production \
  --domain crypto \
  --format json
```

Run read-only autonomous gate validation:

```bash
kalshi-bot-cli autonomous-gates run \
  --kalshi-env production \
  --domain crypto \
  --source combined \
  --days 3650 \
  --dry-run \
  --format json
```

## Review Checklist Before Enabling Live Crypto

Before enabling live crypto for any asset:

- active color is healthy and matches the running app
- daemon heartbeat and reconciliation are fresh
- kill switch is off by operator intent
- app shadow mode is off
- production write credentials are present
- global crypto live switches are intentionally still off until asset readiness
  passes
- target asset passes `crypto-live-path status`
- target asset replay gate is passed
- target asset model is trained
- target asset has at least 60 strict labeled real-quote rows
- target asset has at least 50 out-of-sample replay trade candidates
- target asset has at least 50 current-model live-quality candidates
- replay simulated net P/L is positive
- replay simulated net P/L beats market-mid P/L
- calibration diagnostics are present for Brier, log-loss, and ECE
- spot coverage is at least 80%, fresh, and not proxy-only
- BNB/HYPE remain shadow until their own strict-row, replay, P/L, and asset-mode gates pass
- target asset mode is promoted one asset at a time
- no `exploratory_shadow` candidate can reach live execution
- order size, position caps, daily loss caps, and rollback path are accepted

## Related Docs

- [Operations](../operations.md)
- [Crypto 15m Touch20 Rules Runbook](../operations/btc15m-touch20-rules.md)
- [Self Improve](../self_improve.md)
- [Weather Trading Strategy](weather-trading-strategy.md)
- [Strategy Page](strategy_page.md)
- [Kalshi Build Spec](kalshi_build_spec.md)
