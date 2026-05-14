# Crypto Trading Strategy

Last updated: 2026-05-12

This document is the operator-facing summary of the crypto trading system: what
we trade, which strategy is active, which gates must pass before an order can go
live, and what evidence is required before any asset is promoted out of shadow
mode.

## Executive Summary

The active crypto strategy is **CRYPTO_15M**: deterministic trading on Kalshi
15-minute crypto markets using live Kalshi quotes, spot/candle evidence, a
per-asset prediction model, and a fee-aware candidate selector.

Crypto is not a single global live switch. It is promoted per asset. BTC can be
approved without approving ETH, and deployment-note asset mode `off` wins over
any agent-pack live mode.

The current production posture is shadow evidence collection, not live trading.
As of the 2026-05-12 production check, no requested crypto asset was ready for
live mode, all replay gates were blocked, and the live switches
`CRYPTO_TRADING_ENABLED`, `CRYPTO_AUTONOMY_ENABLED`, and
`CRYPTO_PRODUCTION_AUTONOMY_ENABLED` were off.

## Strategy Inventory

| Strategy | Status | Description | Live eligibility |
|---|---:|---|---|
| `CRYPTO_15M` live-quality | Active shadow | Predict fair YES for 15-minute crypto markets and select the best fee-adjusted YES/NO buy candidate. | Per-asset live only after replay, asset-mode, risk, and execution gates pass. |
| `CRYPTO_15M` exploratory shadow | Active shadow | Collect candidate and quote evidence even when live edge is not present. | Never live eligible; evidence only. |
| Per-asset policy promotion | Active control path | Stage asset-specific crypto policy in the active agent pack. | One asset at a time after live-path readiness passes. |
| Crypto market making | Out of scope | Posting two-sided liquidity. | Not supported. |
| Spot/perp trading | Out of scope | Trading on external crypto venues. | Not supported. |

The live path must treat `live_quality` and `exploratory_shadow` differently.
Only `live_quality` candidates can reach live execution.

## Market Scope

Crypto trading only applies to Kalshi 15-minute crypto markets such as:

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
| Minimum seconds to close | 120 seconds |
| Max rooms per run | 7 |
| Max rooms per asset per run | 1 |
| Autonomy interval | 60 seconds |

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
- remaining payout is below the minimum
- raw edge exceeds the maximum credible edge
- fee-adjusted expected net edge is below the active minimum

Current runtime entry thresholds observed in production on 2026-05-12:

| Gate | Active value |
|---|---:|
| Minimum fee-adjusted edge | 500 bps |
| Max spread | 250 bps |
| Minimum confidence | 0.80 |
| Minimum contract price | $0.50 |
| Minimum remaining payout | 2000 bps ($0.20) |
| Maximum credible edge | 10000 bps |

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
- remaining payout below minimum
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

Crypto execution is `passive_then_taker` by default:

1. Submit a passive maker-style order one tick inside the quoted spread.
2. If the passive order fills or receives a terminal non-retry status, stop.
3. If the passive order is unfilled/cancelled or loses edge on requote, consider
   taker fallback.
4. Taker fallback is allowed only when the market is within 90 seconds of close
   and the signal edge still clears the active minimum.

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
  --assets BTC ETH SOL XRP BNB DOGE HYPE \
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
  --assets BTC ETH SOL XRP BNB DOGE HYPE \
  --docker-container infra-app_production_blue-1
```

Repair recent settlement labels directly:

```bash
kalshi-bot-cli crypto-history collect-settled \
  --kalshi-env production \
  --frequency 15m \
  --days 2 \
  --assets BTC ETH SOL XRP BNB DOGE HYPE \
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
- [Self Improve](../self_improve.md)
- [Weather Trading Strategy](weather-trading-strategy.md)
- [Strategy Page](strategy_page.md)
- [Kalshi Build Spec](kalshi_build_spec.md)
