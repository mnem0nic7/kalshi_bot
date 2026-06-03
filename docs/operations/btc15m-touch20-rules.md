# Crypto 15m Touch20 Rules Runbook

Last updated: 2026-06-03

This runbook covers the independent non-model 15-minute Touch20 rules strategy.
BTC keeps the legacy strategy code `btc15m_touch20_rules`; other supported
assets use their own lanes, settings, gates, approvals, ledgers, and order
prefixes. The path is additive. It must not disable or retune the model-trained
crypto bot.

## Core Trading Logic

The strategy buys a supported 15-minute contract only when deterministic rules
and replay-bucket evidence indicate the contract has a good chance to touch a
+20% net executable profit before close. BTC defaults to a stricter YES-only
setup based on current live-faithful replay evidence.

It does not try to predict settlement as its primary objective. The preferred
outcome is:

1. enter from the executable ask
2. wait for contract-price fluctuation
3. sell from the executable bid when net profit is at least +20% after estimated
   entry and exit taker fees

Replay now mirrors live exit mechanics. It scans the future quote path for
take-profit, stop-loss, and profit-protection exits, then terminal-closes at
market close only when no executable exit occurs first. Live exits are
strategy-owned and trigger on +20% net executable take-profit, -30% net
executable stop loss for the current BTC live profile, or armed profit
protection.

## Scope

Allowed:

- assets: BTC, ETH, SOL, XRP, BNB, DOGE, HYPE
- frequency: 15m only
- market type: Kalshi crypto contracts
- sides: configured YES/NO buys; BTC default is YES only
- entry: executable ask
- exit: executable bid
- process: `crypto_non_model_btc15m_touch20_production`
- strategy code: `btc15m_touch20_rules` for BTC; `<asset>15m_touch20_rules` for
  non-BTC assets, such as `eth15m_touch20_rules`
- order ID prefix: `b15t20r` for BTC; `<asset>15t20r` for non-BTC assets, such
  as `eth15t20r`

Not allowed:

- assets outside BTC, ETH, SOL, XRP, BNB, DOGE, HYPE
- 1h crypto markets
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
| `CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES` | `yes` | Comma-separated allowed entry sides. BTC defaults to YES-only. |
| `CRYPTO_BTC15M_TOUCH20_TAKE_PROFIT_PCT` | `0.20` | Net executable take-profit target. |
| `CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT` | `0.30` | Net executable stop-loss trigger for strategy-owned positions. |
| `CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE` | `600` | Minimum time to close for new entries. |
| `CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS` | `10` | Strategy-local open plus pending notional cap. |
| `CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS` | `10` | Strategy-local daily realized loss stop. |
| `CRYPTO_BTC15M_TOUCH20_MIN_ORDER_NOTIONAL_DOLLARS` | `5` | Minimum strategy entry notional after sizing. |
| `CRYPTO_BTC15M_TOUCH20_MAX_BUCKET_LIVE_LOSS_DOLLARS` | `1` | Live bucket loss threshold that blocks more entries in that bucket. |
| `CRYPTO_BTC15M_TOUCH20_MAX_BUCKET_CONSECUTIVE_LOSSES` | `2` | Consecutive stop/terminal losses that block a live bucket. |
| `CRYPTO_BTC15M_TOUCH20_MAX_REPLAY_STOP_LOSS_RATE` | `0.35` | Max replay stop-loss rate for the gate and bucket allowance. |
| `CRYPTO_BTC15M_TOUCH20_MAX_REPLAY_TERMINAL_LOSS_RATE` | `0.15` | Max replay terminal-loss rate for the gate and bucket allowance. |
| `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_THRESHOLD_PCT` | `0.10` | Profit level that arms profit protection. |
| `CRYPTO_BTC15M_TOUCH20_PROFIT_PROTECTION_FLOOR_PCT` | `0.05` | Armed profit-protection floor. |
| `CRYPTO_BTC15M_TOUCH20_LOOP_INTERVAL_SECONDS` | `15` | Docker process loop sleep. |
| `CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS` | `0.20` | Strategy-owned minimum entry ask. |
| `CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS` | `0.50` | Strategy-owned maximum entry ask. |
| `CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM` | `0.0005` | Minimum side-aligned spot momentum. |
| `CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE` | `0.458` | Minimum standalone rules score for entry. |
| `CRYPTO_BTC15M_TOUCH20_QUOTE_FRESH_SECONDS` | `30` | Maximum age for live Kalshi quote snapshots. |
| `CRYPTO_BTC15M_TOUCH20_SPOT_FRESH_SECONDS` | `180` | Maximum age for live asset spot rows. |
| `CRYPTO_15M_TOUCH20_RULES_ASSETS` | `BTC` | Comma-separated assets for the Docker loop to evaluate. |
| `CRYPTO_15M_TOUCH20_ASSET_SETTINGS` | `{}` | JSON object with per-asset overrides for non-BTC lanes. |

Production uses the `PRODUCTION_` prefixed versions in `.env`, mapped into the
container as the runtime names above.

BTC uses the legacy `CRYPTO_BTC15M_TOUCH20_*` flags. Non-BTC lanes are disabled
by default and must be enabled through `CRYPTO_15M_TOUCH20_ASSET_SETTINGS`.
Example:

```json
{
  "ETH": {
    "rules_enabled": true,
    "trading_enabled": false,
    "max_open_notional_dollars": 10,
    "daily_loss_limit_dollars": 10,
    "take_profit_pct": 0.20,
    "stop_loss_pct": 0.20,
    "min_rule_score": 0.48
  }
}
```

## Entry Checklist

An entry can be submitted only when all of the following are true:

1. The command scope is a supported 15m asset.
2. The asset lane has `rules_enabled=true`; BTC uses
   `CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true`.
3. The running container color is the active deployment color.
4. The kill switch is off.
5. The separate rules replay gate has status `passed`.
6. Strategy daily realized P/L is not below the daily loss limit.
7. The latest market quote snapshot is fresh.
8. Market status is open or active.
9. YES bid, YES ask, NO bid, and NO ask are present.
10. Asset spot features are fresh and non-proxy.
11. Market age is at least 60 seconds.
12. Time to close is at least 600 seconds by default.
13. Candidate side is allowed by `CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES`; BTC
    defaults to YES-only.
14. Entry ask is at least the configured minimum contract price, default `$0.20`.
15. Entry ask is below the configured maximum contract price, default `$0.50`.
16. Side-aligned spot momentum is at least `0.0005` by default.
17. The +20% fee-aware target exit price is below `$1.00`.
18. Spread is within tier limits: 1 cent under 20c, otherwise 2 cents.
19. Standalone rule score clears the configured minimum.
20. Candidate replay bucket is allowed by the asset-owned gate, such as
    `btc15m_touch20_rules_gate:15m:BTC` or `eth15m_touch20_rules_gate:15m:ETH`.
21. Candidate bucket is not blocked by live bucket controls.
22. This strategy has no open or pending entry on the same Kalshi market.
23. The market is not in the one-cycle cooldown after a strategy stop/terminal
    loss.
24. Strategy-owned open plus pending notional remains within the `$10` cap.
25. Sized order notional is at least the configured minimum, default `$5`.
26. Operator approval checkpoint exists and references the latest passed gate
    version and replay simulator version.
27. The asset lane has `trading_enabled=true`; BTC uses
    `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true`.

If the final trading flag is false, the process can still produce
`trading_disabled` telemetry with the selected candidate and no order.

## Candidate Ranking

When multiple candidates pass filters, the strategy chooses one candidate per
entry cycle using this ranking:

1. higher replay bucket P/L per candidate
2. higher replay bucket touch rate
3. higher standalone rule score
4. tighter spread
5. more remaining time

The standalone rule score is a deterministic weighted score based on replay
touch rate, replay P/L per candidate, target gap, remaining time, realized spot
volatility, short-term spot momentum, and spread quality. It is not a trained
model prediction.

## Replay And Gate

Build the non-model replay artifact:

```bash
kalshi-bot-cli crypto-non-model-touch20 replay \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --days 30 \
  --json
```

Persist the separate live gate:

```bash
kalshi-bot-cli crypto-non-model-touch20 gate \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --json
```

Approve the exact passed gate version:

```bash
kalshi-bot-cli crypto-non-model-touch20 approve \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --approved-by <operator> \
  --json
```

Gate artifact:

```text
btc15m_touch20_rules_gate:15m:BTC
```

Non-BTC lanes use their own artifact type, for example:

```text
eth15m_touch20_rules_gate:15m:ETH
```

Gate requirements:

- at least 50 candidates in allowed replay buckets
- real settled quote-path evidence present
- no trained model usage
- replay simulator version `live_exit_v2`
- entry replay mode `first_eligible_per_market`
- net simulated P/L above `$0.00` after live-faithful exits
- P/L per candidate at least `$0.01` after fees
- touch rate at least 25%
- stop-loss rate at or below 35%
- terminal-loss rate at or below 15%
- hard-cap breaches equal 0
- at least one allowed bucket
- no allowed replay bucket with negative P/L, excessive stop losses, or
  excessive terminal losses

Approval checkpoint:

```text
btc15m_touch20_rules_approval:<kalshi_env>:BTC:15m
```

Non-BTC approvals are separate, for example:

```text
eth15m_touch20_rules_approval:<kalshi_env>:ETH:15m
```

A new gate version or simulator version invalidates old approval until the
operator approves again. The old Grantv approval for the touch-only simulator is
expected to fail closed after this remediation.

## BTC Live Activation Record - 2026-06-03

This is the condensed record of how BTC 15m Touch20 reached live-approved
status.

### Starting Problem

The first replay-passing profile was profitable but depended on the live-blocked
bucket:

```text
BTC|yes|50_60c|le_1c|10_15m
```

That bucket had prior live losses and was blocked by live bucket controls. A
gate that passed by leaning on that bucket would look good in replay while the
live entry loop skipped part of the candidate universe.

### Code And Gate Fixes

Two commits made the evidence and runtime behavior line up:

- `e12b07f Clarify BTC touch gate executable evidence`
- `8bc906f Tune BTC touch profile away from live-blocked bucket`

The first commit added separate reporting for:

- total replay candidates
- candidates in allowed replay buckets
- candidates still executable after subtracting live-blocked buckets
- allowed replay buckets that are currently live-blocked

The second commit moved the BTC profile away from the blocked 50-60c band and
made bucket-level gate failures apply only to buckets that the gate allows live.
Unallowed buckets remain visible in `blocked_bucket_keys`, but they do not block
the gate simply because live would never trade them.

### Final BTC Profile

The final BTC production profile is:

```text
CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true
CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES=yes
CRYPTO_BTC15M_TOUCH20_TAKE_PROFIT_PCT=0.20
CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT=0.30
CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE=600
CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.20
CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.50
CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0005
CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE=0.458
CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS=10
CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS=10
CRYPTO_BTC15M_TOUCH20_MIN_ORDER_NOTIONAL_DOLLARS=5
```

### Replay Evidence

The non-persisting production replay audit for this profile returned:

```text
status pass
gate_reasons []
trade_candidate_count 54
allowed_trade_candidate_count 51
net_simulated_pl_dollars 2.657
pnl_per_candidate_dollars 0.0492037037037037
touch_rate 0.6111111111111112
stop_loss_rate 0.3333333333333333
terminal_loss_rate 0.0
allowed_bucket_keys [
  BTC|yes|20_30c|le_1c|10_15m,
  BTC|yes|30_40c|le_1c|10_15m,
  BTC|yes|40_50c|le_1c|10_15m
]
```

This proved the profile had at least 50 live-executable replay candidates and
positive net P/L without relying on the blocked `50_60c` bucket.

### Deployment Shape

The focused production redeploy kept unrelated strategy containers down:

```bash
ENABLE_DEMO_DAEMON=false \
ENABLE_PRODUCTION_DAEMON=false \
ENABLE_CRYPTO_1H_DAEMON=false \
ENABLE_CRYPTO_1H_CONTAINER=false \
ENABLE_CRYPTO_CURRENT_CONTAINER=true \
ENABLE_BTC15M_TOUCH20_CONTAINER=true \
ENABLE_WEB_STRATEGIES_CONTAINER=false \
scripts/blue_green_redeploy.sh --env production --yes
```

After the final redeploy, the running container set was:

```text
infra-crypto_non_model_btc15m_touch20_production-1
infra-crypto_current_production-1
infra-app_production_blue-1
infra-web_production-1
infra-postgres_production-1
infra-caddy-1
```

No demo daemon, generic production daemon, 1h crypto container/daemon, web
strategies container, or inactive app color was left running.

### Final Gate And Approval

The live gate became:

```text
btc15m-touch20-rules-gate-15m-BTC-20260603161709-146487a96acc
```

Status before approval was correctly fail-closed:

```text
approval_valid false
approval_reason operator_approval_simulator_version_mismatch
```

The user then explicitly approved:

```text
approve BTC 15m touch with max notional 10
```

Approval was recorded with:

```bash
kalshi-bot-cli crypto-non-model-touch20 approve \
  --kalshi-env production \
  --frequency 15m \
  --asset BTC \
  --approved-by Grantv \
  --max-notional-dollars 10 \
  --note "User approved via Codex: approve BTC 15m touch with max notional 10" \
  --json
```

The approved checkpoint referenced:

```text
gate_version btc15m-touch20-rules-gate-15m-BTC-20260603161709-146487a96acc
simulator_version live_exit_v2
max_notional_dollars 10.0000
```

Final status after approval:

```text
enabled true
trading_enabled true
approval_valid true
approval_reason operator_approval_valid
gate status passed
gate sample_count 51
live_executable_candidate_count 51
live_blocked_allowed_bucket_keys []
open_strategy_positions 0
open_pending_notional_dollars 0.0000
daily_realized_pnl_dollars 0.0000
```

The first approved `run-once` returned `no_candidate`, not an order, because the
fresh market was too early or too late and older snapshots were stale. That is
expected: the strategy is live-approved and armed, but it remains selective.
Based on the 30-day replay evidence, expect roughly one to two qualifying
opportunities per day rather than trades in every 15-minute market.

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
- `approval_blocked`
- `daily_loss_limit_blocked`
- `no_candidate`
- `strategy_cap_blocked`
- `min_order_notional_blocked`
- `trading_disabled`

Only `CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true` allows entry order
submission.

## Tiny-Live

Before tiny-live:

1. Confirm active color and kill switch.
2. Confirm production write credentials are present.
3. Confirm the asset's 15m quote collection is current.
4. Confirm the asset spot rows are fresh and non-proxy.
5. Confirm the asset-owned gate, such as `btc15m_touch20_rules_gate:15m:BTC`, is
   passed with simulator version `live_exit_v2`.
6. Confirm the selected dry-run candidate is in an allowed replay bucket.
7. Confirm `live_bucket_controls.blocked_bucket_keys` does not include the
   selected candidate bucket.
8. Confirm the strategy ledger has no stale pending notional and no duplicate
   strategy entry for the selected market.
9. Confirm max strategy notional remains `$10` and daily loss limit remains
   `$10`; do not override the daily-loss block as part of this remediation.
10. Confirm the existing model-trained crypto bot remains unchanged.
11. Approve the latest gate with `crypto-non-model-touch20 approve`.

Then enable the current BTC live profile:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_TRADING_ENABLED=true
PRODUCTION_CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT=0.30
PRODUCTION_CRYPTO_BTC15M_TOUCH20_ALLOWED_SIDES=yes
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE=600
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.20
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.50
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0005
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE=0.458
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MAX_OPEN_NOTIONAL_DOLLARS=10
PRODUCTION_CRYPTO_BTC15M_TOUCH20_DAILY_LOSS_LIMIT_DOLLARS=10
PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_ORDER_NOTIONAL_DOLLARS=5
```

For non-BTC lanes, run the same replay, gate, approve, status, run-once, and
exit-once commands with that asset symbol. Do not set a non-BTC lane's
`trading_enabled` override to `true` until its own gate and approval are valid.

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
prefix for BTC, or the asset lane's own prefix for non-BTC. It exits on:

- `take_profit`: net executable profit is at least +20%
- `stop_loss`: net executable profit is at or below -30% for the current BTC
  live profile
- `profit_protection_floor`: armed profit falls to 5% or lower
- `profit_protection_adverse_momentum`: armed profit is declining and spot
  momentum is adverse

Profit protection arms only after net executable profit first reaches +10%.
The stop loss is not a resting exchange order; it is evaluated by the dedicated
exit loop from current executable quotes.

## Ledger

Checkpoint stream:

```text
btc15m_touch20_rules:<kalshi_env>:BTC:15m
```

Non-BTC lanes use separate streams, for example:

```text
eth15m_touch20_rules:<kalshi_env>:ETH:15m
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
must not be counted as strategy-owned unless they are in that asset lane's
ledger under that lane's order prefix.

## Monitoring

Watch these first:

- candidate funnel: market seen, quote valid, spot fresh, spread pass,
  entry-window pass, replay-bucket pass, selected, submitted, filled
- gate health: candidate count, touch rate, net P/L, P/L per candidate,
  allowed bucket count, blocked bucket count, exit reason counts, stop-loss
  rate, terminal-loss rate, simulator version
- trading quality: entry spread, exit spread, slippage, fill latency, partial
  fills, rejected orders, stale quote skips
- P/L attribution: strategy-only realized/unrealized P/L, take-profit exits,
  profit-protection exits, settlement holds
- risk: strategy open notional, pending notional, daily loss, cap blocks, live
  bucket blocks, duplicate-market skips, cooldown skips, overlap with model-bot
  positions
- market regime: asset spot volatility, short-term momentum, distance to target,
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

For non-BTC lanes, set that asset's `trading_enabled` override to `false`.

Keep the process running if it owns open positions, because the exit loop is the
strategy-specific take-profit, stop-loss, and profit-protection path. If the
process itself must be stopped, manually inspect and manage any open
strategy-prefixed positions first, such as `b15t20r:` for BTC.

For a hard stop of new evaluation:

```bash
PRODUCTION_CRYPTO_BTC15M_TOUCH20_RULES_ENABLED=false
```

Global kill switch also blocks entries and still allows risk-reducing exits.

## Important Caveats

- This is a tiny-live path, not a broad 15m crypto replacement.
- The replay gate is necessary, not sufficient. Live spread and fill quality can
  differ from historical quote-path snapshots.
- The ledger is strategy-local. If an exchange fill happens after initial order
  submission, reconciliation must keep the strategy ledger accurate before
  sizing up.
- The stop loss only works when the process has a fresh quote and can submit a
  risk-reducing close; it is not a guaranteed exchange-side stop.
