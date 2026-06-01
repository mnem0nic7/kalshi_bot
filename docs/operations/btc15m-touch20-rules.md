# BTC 15m Touch20 Rules Runbook

Last updated: 2026-06-01

This runbook covers the independent non-model BTC 15-minute Touch20 strategy,
`btc15m_touch20_rules`. The path is additive. It must not disable or retune the
model-trained crypto bot.

## Core Trading Logic

The strategy buys a BTC 15-minute YES or NO contract only when deterministic
rules and replay-bucket evidence indicate the contract has a good chance to
touch a +20% net executable profit before close.

It does not try to predict settlement as its primary objective. The preferred
outcome is:

1. enter from the executable ask
2. wait for contract-price fluctuation
3. sell from the executable bid when net profit is at least +20% after estimated
   entry and exit taker fees

If the +20% touch never occurs, replay assumes settlement hold. Live v1 uses
profit protection after the position first moves in our favor, but it does not
use an initial hard stop.

## Scope

Allowed:

- asset: BTC only
- frequency: 15m only
- market type: Kalshi crypto contracts
- sides: YES buy or NO buy
- entry: executable ask
- exit: executable bid
- process: `crypto_non_model_btc15m_touch20_production`
- strategy code: `btc15m_touch20_rules`
- order ID prefix: `b15t20r`

Not allowed:

- ETH, SOL, XRP, BNB, DOGE, HYPE, or other assets
- 1h BTC markets
- trained crypto model calls
- trained model artifact loading as entry authority
- model settlement replay gates
- global crypto 15m take-profit exits
- entries inside the final 5 minutes
- proxy-only spot evidence

## Environment Flags

All live flags are disabled by default.

| Variable | Default | Meaning |
|---|---:|---|
| `CRYPTO_BTC15M_TOUCH20_RULES_ENABLED` | `false` | Enables the rules path to evaluate candidates. |
| `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED` | `false` | Allows the rules path to submit entry orders. |
| `CRYPTO_BTC15M_TOUCH20_TAKE_PROFIT_PCT` | `0.20` | Net executable take-profit target. |
| `CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS` | `10` | Strategy-local open plus pending notional cap. |
| `CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS` | `10` | Strategy-local daily realized loss stop. |
| `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_THRESHOLD_PCT` | `0.10` | Profit level that arms profit protection. |
| `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_FLOOR_PCT` | `0.05` | Armed profit-protection floor. |
| `CRYPTO_BTC15M_TOUCH20_LOOP_INTERVAL_SECONDS` | `15` | Docker process loop sleep. |

Production uses the `PRODUCTION_` prefixed versions in `.env`, mapped into the
container as the runtime names above.

## Entry Checklist

An entry can be submitted only when all of the following are true:

1. The command scope is BTC 15m.
2. `CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true`.
3. The running container color is the active deployment color.
4. The kill switch is off.
5. The separate rules replay gate has status `passed`.
6. Strategy daily realized P/L is not below the daily loss limit.
7. The latest market quote snapshot is fresh.
8. Market status is open or active.
9. YES bid, YES ask, NO bid, and NO ask are present.
10. BTC spot features are fresh and non-proxy.
11. Market age is at least 60 seconds.
12. Time to close is at least 300 seconds.
13. Entry ask is at least the configured minimum contract price, default `$0.10`.
14. The +20% fee-aware target exit price is below `$1.00`.
15. Spread is within tier limits: 1 cent under 20c, otherwise 2 cents.
16. Deterministic touch probability clears the configured minimum.
17. Expected return clears the active runtime edge threshold.
18. Candidate replay bucket is allowed by `replay_gate_touch20_rules:15m:BTC`.
19. Strategy-owned open plus pending notional remains within the `$10` cap.
20. `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true`.

If the final trading flag is false, the process can still produce
`trading_disabled` telemetry with the selected candidate and no order.

## Candidate Ranking

When multiple candidates pass filters, the strategy chooses one candidate per
entry cycle using this ranking:

1. higher replay bucket P/L per candidate
2. higher replay bucket touch rate
3. higher deterministic touch probability
4. tighter spread
5. more remaining time

The deterministic touch probability is a heuristic score based on target gap,
remaining time, realized spot volatility, short-term spot momentum, moneyness,
and spread penalty. It is not a trained model prediction.

## Replay And Gate

Build the non-model replay artifact:

```bash
kalshi-bot-cli crypto-replay run \
  --kalshi-env production \
  --frequency 15m \
  --assets BTC \
  --objective touch20_rules \
  --days 30 \
  --json
```

Validate without persisting a gate:

```bash
kalshi-bot-cli crypto-replay validate \
  --kalshi-env production \
  --frequency 15m \
  --assets BTC \
  --objective touch20_rules \
  --days 30 \
  --json
```

Persist the separate live gate:

```bash
kalshi-bot-cli crypto-replay gate \
  --kalshi-env production \
  --frequency 15m \
  --assets BTC \
  --objective touch20_rules \
  --json
```

Gate artifact:

```text
replay_gate_touch20_rules:15m:BTC
```

Gate requirements:

- at least 50 candidates
- real settled quote-path evidence present
- no trained model usage
- net simulated P/L above `$0.00`
- P/L per candidate at least `$0.01`
- touch rate at least 25%
- hard-cap breaches equal 0
- at least one allowed bucket

## Dry Run

Enable evaluation but keep trading disabled:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=false
```

Run one entry evaluation:

```bash
kalshi-bot-cli crypto-non-model-touch20 run-once \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --json
```

Expected safe statuses include:

- `disabled`
- `inactive_color`
- `kill_switch_enabled`
- `gate_blocked`
- `daily_loss_limit_blocked`
- `no_candidate`
- `strategy_cap_blocked`
- `trading_disabled`

Only `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true` allows entry order
submission.

## Tiny-Live

Before tiny-live:

1. Confirm active color and kill switch.
2. Confirm production write credentials are present.
3. Confirm BTC 15m quote collection is current.
4. Confirm BTC spot rows are fresh and non-proxy.
5. Confirm `replay_gate_touch20_rules:15m:BTC` is passed.
6. Confirm the selected dry-run candidate is in an allowed replay bucket.
7. Confirm the strategy ledger has no stale pending notional.
8. Confirm max strategy notional remains `$10`.
9. Confirm the existing model-trained crypto bot remains unchanged.

Then enable:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS=10
PRODUCTION_CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS=10
```

Start or recreate the process:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d \
  crypto_non_model_btc15m_touch20_production
```

## Exit Behavior

Run one exit pass:

```bash
kalshi-bot-cli crypto-non-model-touch20 exit-once \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --json
```

The exit loop evaluates only strategy-ledger positions with the `b15t20r:`
prefix. It exits on:

- `take_profit`: net executable profit is at least +20%
- `profit_protection_floor`: armed profit falls to 5% or lower
- `profit_protection_adverse_momentum`: armed profit is declining and spot
  momentum is adverse

Profit protection arms only after net executable profit first reaches +10%.
There is no initial hard stop in v1.

## Ledger

Checkpoint stream:

```text
btc15m_touch20_rules:<kalshi_env>:BTC:15m
```

The ledger records:

- entry client order ID
- Kalshi order ID when available
- side
- count
- entry price and notional
- target exit price
- replay bucket
- gate version
- profit-protection state
- exit client order ID
- realized P/L when closed

Manual trades and model-bot trades can overlap the same Kalshi market, but they
must not be counted as strategy-owned unless they are in this ledger under the
`b15t20r:` prefix.

## Monitoring

Watch these first:

- candidate funnel: market seen, quote valid, spot fresh, spread pass,
  entry-window pass, replay-bucket pass, selected, submitted, filled
- gate health: candidate count, touch rate, net P/L, P/L per candidate,
  allowed bucket count, blocked bucket count
- trading quality: entry spread, exit spread, slippage, fill latency, partial
  fills, rejected orders, stale quote skips
- P/L attribution: strategy-only realized/unrealized P/L, take-profit exits,
  profit-protection exits, settlement holds
- risk: strategy open notional, pending notional, daily loss, cap blocks,
  overlap with model-bot positions
- market regime: BTC spot volatility, short-term momentum, distance to target,
  liquidity by price band, time-to-close bucket

Ops events are logged with source:

```text
crypto_non_model_btc15m_touch20
```

## Rollback

The fastest safe rollback is to disable entries:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=false
```

Keep the process running if it owns open positions, because the exit loop is the
strategy-specific take-profit and profit-protection path. If the process itself
must be stopped, manually inspect and manage any open `b15t20r:` positions first.

For a hard stop of new evaluation:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=false
```

Global kill switch also blocks entries and still allows risk-reducing exits.

## Important Caveats

- This is a tiny-live path, not a broad BTC 15m replacement.
- The replay gate is necessary, not sufficient. Live spread and fill quality can
  differ from historical quote-path snapshots.
- The ledger is strategy-local. If an exchange fill happens after initial order
  submission, reconciliation must keep the strategy ledger accurate before
  sizing up.
- No initial hard stop means losses can still settle to full contract loss if
  neither take-profit nor profit protection exits fill.
