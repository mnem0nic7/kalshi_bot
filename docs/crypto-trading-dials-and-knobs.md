# Crypto Trading Dials and Knobs

This document is the crypto-only operator reference for the controls that affect
crypto model training, replay qualification, live candidate selection, autonomy,
and execution.

The source of truth is the code. This document summarizes the current behavior
from:

- `src/kalshi_bot/config.py`
- `src/kalshi_bot/crypto/services.py`
- `src/kalshi_bot/services/agent_packs.py`
- `src/kalshi_bot/services/risk.py`
- `src/kalshi_bot/cli.py`
- `docs/strategy/crypto-trading-strategy.md`

## Scope

The active crypto strategy is `CRYPTO_15M`.

It trades Kalshi 15-minute crypto markets for these asset families:

- `KXBTC15M*` for BTC
- `KXETH15M*` for ETH
- `KXSOL15M*` for SOL
- `KXXRP15M*` for XRP
- `KXDOGE15M*` for DOGE
- `KXBNB15M*` for BNB
- `KXHYPE15M*` for HYPE

The main runtime modes are:

- `off`: ignore the asset.
- `shadow`: collect evidence and create non-trading workflow records.
- `live`: allow real entries if all global, asset, replay, candidate, risk, and
  execution gates also pass.

## Control Layers

Crypto trading is controlled by several independent layers. A live order needs
all relevant layers to agree.

| Layer | Where it lives | What it controls |
| --- | --- | --- |
| Static settings | `Settings` in `config.py`, usually via environment | Defaults, hard runtime switches, collection cadence, gate thresholds, risk caps. |
| Agent pack crypto policy | `AgentPackCryptoPolicy` | Runtime entry thresholds, replay thresholds, trading enablement, production autonomy, per-asset modes, per-asset entry overrides. |
| Deployment control notes | Database deployment control notes | Asset mode override and production live approval. In production, explicit control-mode `live` is required for live entries. |
| Replay artifacts | Crypto model and replay gate artifacts | Whether an asset/frequency/env is qualified for live trading. |
| Candidate trace | Crypto forecast and candidate payload | Why a candidate was or was not considered live quality. |
| Risk engine | `services/risk.py` | Position, notional, confidence, edge, spread, staleness, and daily-loss checks. |
| Execution service | `services/execution.py` and crypto execution path | Passive order, taker fallback, kill switch, shadow mode, active color, write credentials. |

## Live Order Checklist

A production crypto fill requires all of the following:

1. The runtime app is not in `APP_SHADOW_MODE`.
2. The deployment kill switch is off.
3. The runtime `app_color` is the active deployment color.
4. Kalshi write credentials are present.
5. `crypto_enabled` is true.
6. `crypto_15m_enabled` is true.
7. Crypto live trading is enabled by settings or the active agent pack.
8. The asset mode is `live`.
9. In production, the deployment-control asset mode is explicitly `live`.
10. The latest replay gate for the asset/frequency/env passes.
11. The current candidate status is `live_quality`.
12. Risk approves the ticket.
13. Execution can place the order without losing fee-adjusted edge.

Shadow and evidence collection can run with fewer permissions, depending on the
command and environment.

## Global Crypto Switches

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_enabled` | `True` | Master crypto feature switch. Turning this off blocks crypto workflow behavior. |
| `crypto_15m_enabled` | `True` | Enables the 15-minute crypto strategy family. |
| `crypto_trading_enabled` | `False` | Global live-trading switch. The active agent pack can also set `crypto_policy.live.trading_enabled`. |
| `crypto_autonomy_enabled` | `False` | Allows crypto autonomy outside production-specific live enablement. Useful for scheduled room creation and evidence generation. |
| `crypto_production_autonomy_enabled` | `False` | Allows production crypto autonomy. The active agent pack can also set `crypto_policy.live.production_autonomy_enabled`. |
| `app_shadow_mode` | `True` | Blocks real orders globally even if crypto gates pass. |
| `app_enable_kill_switch` | `True` | Initial kill-switch state when deployment control is created. Runtime kill switch lives in deployment control. |
| `app_color` | `blue` | Runtime color. Must match active deployment color for live execution. |

## Asset Mode Knobs

Asset modes are per asset, not global. This lets BTC be live while ETH remains
shadow, for example.

CLI:

```bash
python -m kalshi_bot.cli crypto-asset-mode list --kalshi-env production --frequency 15m
python -m kalshi_bot.cli crypto-asset-mode set --kalshi-env production BTC live
python -m kalshi_bot.cli crypto-asset-mode set --kalshi-env production ETH shadow
python -m kalshi_bot.cli crypto-asset-mode set --kalshi-env production SOL off
```

Precedence:

1. Deployment-control mode `off` wins.
2. In demo or production, explicit deployment-control mode `off`, `shadow`, or
   `live` wins over the agent-pack asset mode.
3. If there is no explicit deployment-control mode, the active agent-pack
   `crypto_policy.live.asset_modes` can apply.
4. If nothing is configured, the default mode is `shadow`.

Production-specific behavior:

- An agent pack can request `live`, but production entries still require the
  deployment-control note to set that asset to `live`.
- `off` is the strongest asset-level block.

## Data Collection Knobs

Crypto has two data streams:

- Kalshi crypto market history and settled outcomes.
- Underlying spot data from Coinbase or fallback sources.

### Kalshi Market History

| Setting or CLI flag | Current default | Effect |
| --- | ---: | --- |
| `crypto_history_lookback_days` | `180` | Default historical horizon for bootstrap/training workflows. |
| `crypto_history_auto_enabled` | `True` | Enables scheduled market-history collection. |
| `crypto_history_auto_interval_seconds` | `3600` | Scheduled market-history collection cadence. |
| `crypto_history_auto_lookback_days` | `2` | Lookback window for scheduled incremental collection. |
| `crypto-history bootstrap --days` | `180` | Backfills market history for the requested horizon. |
| `crypto-history collect-settled --days` | `2` | Collects recently settled crypto markets. |
| `crypto-history status --days` | `0` | Reports history status, optionally limited to recent days. |

Useful commands:

```bash
python -m kalshi_bot.cli crypto-history bootstrap --kalshi-env production --days 180 --frequency 15m
python -m kalshi_bot.cli crypto-history collect-open --kalshi-env production --frequency 15m --json
python -m kalshi_bot.cli crypto-history collect-settled --kalshi-env production --days 2 --frequency 15m --json
python -m kalshi_bot.cli crypto-history status --kalshi-env production --frequency 15m --json
```

### Spot Data

| Setting or CLI flag | Current default | Effect |
| --- | ---: | --- |
| `crypto_spot_request_timeout_seconds` | `30.0` | Timeout for spot-source requests. |
| `coinbase_cdp_api_key_file` | `cdp_api_key.json` | File used for Coinbase CDP credentials. |
| `coinbase_cdp_key_name` | `None` | Explicit Coinbase CDP key name override. |
| `coinbase_cdp_private_key` | `None` | Explicit Coinbase CDP private key override. |
| `coinbase_advanced_trade_authenticated_enabled` | `True` | Enables authenticated Coinbase Advanced Trade spot requests. |
| `crypto_spot_proxy_fallback_enabled` | `False` | Allows proxy/fallback spot source. Live-quality candidates reject proxy-only spot evidence. |
| `crypto_spot_coinbase_max_stale_seconds` | `180` | Max age for Coinbase spot data. |
| `crypto_spot_coingecko_max_stale_seconds` | `90` | Max age for CoinGecko spot fallback data. |
| `crypto_spot_current_auto_enabled` | `True` | Enables scheduled current-spot collection. |
| `crypto_spot_current_interval_seconds` | `30` | Current-spot collection cadence. |
| `crypto_spot_history_auto_enabled` | `True` | Enables scheduled spot-history backfill. |
| `crypto_spot_history_auto_lookback_days` | `2` | Scheduled spot-history lookback. |
| `crypto-spot backfill --days` | `180` | Backfills spot history for training and replay. |
| `crypto-spot status --days` | `0` | Reports spot coverage. |

Useful commands:

```bash
python -m kalshi_bot.cli crypto-spot collect-current --kalshi-env production --frequency 15m --json
python -m kalshi_bot.cli crypto-spot backfill --kalshi-env production --days 180 --frequency 15m --json
python -m kalshi_bot.cli crypto-spot status --kalshi-env production --frequency 15m --json
python -m kalshi_bot.cli crypto-spot coinbase-products --kalshi-env production --json
```

### Quote Evidence

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_quote_evidence_enabled` | `True` | Enables strict quote-evidence collection. Production can collect shadow evidence even when production autonomy is not live-enabled. |
| `crypto_quote_evidence_interval_seconds` | `60` | Quote-evidence collection cadence. |

Quote evidence matters because replay promotion requires strict real-quote trade
quality. Proxy-only or stale spot evidence can still be useful diagnostically,
but it does not make a `live_quality` candidate.

## Training and Model Knobs

| Setting or CLI flag | Current default | Effect |
| --- | ---: | --- |
| `crypto_min_training_samples` | `250` | Minimum training sample count expected by crypto model training. |
| `crypto-model train --assets` | `None` | Limits training to selected assets. If omitted, trains configured/default assets. |
| `crypto-model candidates --days` | `30` | Candidate-analysis lookback. |
| `crypto-model candidates --assets` | `None` | Limits candidate analysis to selected assets. |

Commands:

```bash
python -m kalshi_bot.cli crypto-model train --kalshi-env production --frequency 15m
python -m kalshi_bot.cli crypto-model train --kalshi-env production --frequency 15m --assets BTC ETH
python -m kalshi_bot.cli crypto-model candidates --kalshi-env production --frequency 15m --days 30 --json
```

Training depends on both market history and spot history. A model can be trained
but still fail replay if the replay window does not produce enough strict
real-quote candidates or does not beat the market-mid baseline.

## Replay Gate Knobs

Replay is the promotion gate between trained models and live asset eligibility.

| Setting or policy field | Current default | Effect |
| --- | ---: | --- |
| `crypto_replay_min_resolved_markets` | `500` | Minimum resolved markets required in replay evidence. |
| `crypto_replay_min_trade_candidates` | `50` | Minimum OOS and current-model live-quality candidate count. |
| `crypto_replay_min_net_pl_dollars` | `0.0` | Minimum replay net P/L. With the current gate logic, default means replay must be positive, not merely equal to zero. |
| `crypto_replay_max_hard_cap_breaches` | `0` | Maximum allowed hard-cap breaches in replay. Default allows none. |
| `crypto_replay_require_calibration_better_than_mid` | `False` | If true, calibration metrics must beat market mid. |
| `crypto_replay_require_pnl_beats_market_mid` | `True` | Requires replay P/L advantage versus market-mid baseline. |
| `crypto_replay_min_pnl_advantage_dollars` | `0.0` | Minimum dollar advantage over market mid. Default requires positive advantage. |
| `crypto_replay_min_spot_coverage_pct` | `0.80` | Minimum spot-data coverage for replay. |
| `crypto-replay run --days` | `30` | Backtest/replay lookback window. |
| `crypto-replay run --limit` | `0` | Optional cap on replay rows. `0` means no explicit limit. |

Agent-pack overrides live in `crypto_policy.replay`:

- `min_resolved_markets`
- `min_trade_candidates`
- `min_net_pl_dollars`
- `max_hard_cap_breaches`
- `min_spot_coverage_pct`
- `require_calibration_better_than_mid`
- `require_pnl_beats_market_mid`
- `min_pnl_advantage_dollars`

Candidate packs are sanitized to these replay ranges:

| Field | Sanitized range |
| --- | ---: |
| `min_resolved_markets` | `10` to `5000` |
| `min_trade_candidates` | `1` to `1000` |
| `min_net_pl_dollars` | `0.0` to `1000.0` |
| `max_hard_cap_breaches` | `0` to `100` |
| `min_spot_coverage_pct` | `0.50` to `1.0` |
| `min_pnl_advantage_dollars` | `0.0` to `1000.0` |

Commands:

```bash
python -m kalshi_bot.cli crypto-replay run --kalshi-env production --frequency 15m --days 30 --json
python -m kalshi_bot.cli crypto-replay validate --kalshi-env production --frequency 15m --days 30 --json
python -m kalshi_bot.cli crypto-replay gate --kalshi-env production --frequency 15m
```

Replay blockers include missing model artifacts, missing backtest artifacts,
missing candles, missing OOS markers, leakage rows, low spot coverage, too few
strict real-quote rows, too few resolved markets, too few OOS candidates, too
few current-model live-quality candidates, non-positive net P/L, insufficient
market-mid advantage, and excessive hard-cap breaches.

## Runtime Entry Policy Knobs

The runtime crypto policy merges active agent-pack settings with static settings.

| Entry field | Settings fallback | Current default behavior |
| --- | --- | --- |
| `min_fee_adjusted_edge_bps` | `risk_min_edge_bps` | Default `500`. Agent-pack values are floored at `risk_min_edge_bps`. Lower pack values do not undercut the settings floor. |
| `max_spread_bps` | `crypto_live_max_spread_bps` | Settings fallback `1000`. The built-in default pack seeds this from `crypto_live_max_spread_bps`. |
| `min_confidence` | `risk_min_confidence` | Default `0.80`. |
| `min_contract_price_dollars` | `risk_min_contract_price_dollars` | Default `0.50`. Agent-pack values are floored at the settings minimum. |
| `min_remaining_payout_bps` | code constant | Currently fixed at `0` for crypto. |
| `max_credible_edge_bps` | `risk_max_credible_edge_bps` | Default `10000`. Blocks implausibly large edge. |

Agent-pack overrides live in `crypto_policy.entry` and per-asset overrides live
in `crypto_policy.asset_entry_overrides`.

Per-asset overrides can set:

- `min_fee_adjusted_edge_bps`
- `max_spread_bps`
- `min_confidence`
- `min_contract_price_dollars`
- `max_credible_edge_bps`

`min_remaining_payout_bps` is still fixed to the crypto constant.

Candidate packs are sanitized to these entry ranges:

| Field | Sanitized range |
| --- | ---: |
| `min_fee_adjusted_edge_bps` | `250` to `5000`, then runtime-floored by `risk_min_edge_bps` |
| `max_spread_bps` | `50` to `2500` |
| `min_confidence` | `0.50` to `0.99` |
| `min_contract_price_dollars` | `risk_min_contract_price_dollars` to `0.99` |
| `max_credible_edge_bps` | `2500` to `10000` |

## Candidate Statuses

Crypto candidate selection records why an opportunity did or did not qualify.

| Status | Meaning |
| --- | --- |
| `live_quality` | Strict real-quote candidate eligible for live trading if every downstream gate passes. |
| `exploratory_shadow` | Shadow candidate used for evidence and learning, not live entry. |
| `prediction_only_proxy_quote` | Prediction exists, but quote/spot evidence is proxy-only, stale, or otherwise not strict enough for live quality. |
| `blocked_fee_edge` | Gross edge exists but fee-adjusted edge is too small. |
| `unfillable` | Market conditions make the candidate unfillable under current gates. |

Main live candidate gates:

- Strict spot data is available.
- Spot evidence is not proxy-only.
- Spot data is not stale.
- Spread is at or below the runtime policy maximum.
- Contract price is at or above `min_contract_price_dollars`.
- Remaining payout is at or above the crypto constant, currently `0` bps.
- Edge is not above `max_credible_edge_bps`.
- Fee-adjusted edge is at or above `min_fee_adjusted_edge_bps`.
- Market age is at least `crypto_live_min_market_age_seconds`.
- Time-to-close satisfies the live entry window.
- Late sure-thing rules pass when that path is used.

## Fee and Market-Price Knobs

| Setting | Current default | Effect |
| --- | ---: | --- |
| `kalshi_taker_fee_rate` | `0.07` | Used to convert gross edge to fee-adjusted edge. |
| `risk_fee_aware_edge_enabled` | `True` | Keeps risk checks fee-aware. |
| `crypto_market_price_anchor_enabled` | `True` | Blends model probability toward market mid before selection. |
| `crypto_market_price_anchor_weight` | `0.75` | Maximum market-mid anchor weight. Effective weight can be lower depending on market-mid extremity. |

Increasing the market-price anchor generally makes predictions more conservative
relative to the traded market. Decreasing it gives the model more independence,
but can create more high-edge candidates that later fail credibility, replay, or
execution checks.

## Late Sure-Thing Knobs

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_late_sure_thing_enabled` | `True` | Enables the late sure-thing candidate path. |
| `crypto_late_sure_thing_max_seconds_to_close` | `300` | Candidate must be within this many seconds of close. |
| `crypto_late_sure_thing_min_probability` | `0.85` | Minimum model probability for the winning side. |
| `crypto_late_sure_thing_min_market_probability` | `0.75` | Minimum market-implied probability for the same side. |

Late sure-thing also affects taker fallback. If the candidate trace is marked as
late sure thing, taker fallback is allowed only inside
`crypto_late_sure_thing_max_seconds_to_close`.

## Autonomy and Room Creation Knobs

| Setting or CLI flag | Current default | Effect |
| --- | ---: | --- |
| `crypto_autonomy_interval_seconds` | `30` | Scheduled autonomy cadence. |
| `crypto_autonomy_min_seconds_to_close` | `0` | Minimum seconds-to-close filter for autonomy. `0` keeps evaluating until close. |
| `crypto_live_min_market_age_seconds` | `180` | Minimum market age before live-quality entry. |
| `crypto_autonomy_max_rooms_per_run` | `7` | Max crypto rooms per autonomy run. |
| `crypto_autonomy_max_per_asset_per_run` | `1` | Max autonomy rooms per asset per run. |
| `crypto-autonomy run-once --assets` | `None` | Limits one autonomy pass to selected assets. |

Command:

```bash
python -m kalshi_bot.cli crypto-autonomy run-once --kalshi-env production --frequency 15m --json
```

Autonomy skips assets in `off` mode. In production, live room creation requires
production autonomy or the shadow quote-evidence mode. Existing rooms are
re-evaluated only when the workflow is eligible for the relevant live/shadow
path.

## Shadow Exploration Knobs

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_shadow_exploration_max_candidates_per_run` | `12` | Total shadow candidates retained per run. |
| `crypto_shadow_exploration_max_per_asset_per_run` | `2` | Shadow candidates retained per asset per run. |
| `crypto_shadow_exploration_min_expected_net_edge_dollars` | `-0.03` | Allows slightly negative expected net edge in shadow exploration. |
| `crypto_shadow_exploration_max_spread_bps` | `500` | Max spread for shadow exploration candidates. |

Shadow exploration is intentionally looser than live trading so the system can
learn from rejected or marginal cases without taking risk.

## Empirical Bucket Gate Knobs

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_empirical_bucket_gate_enabled` | `True` | Requires empirical bucket evidence for selected assets/modes. |
| `crypto_empirical_bucket_gate_assets` | `live` | Applies bucket gate to live assets by default. |
| `crypto_empirical_bucket_min_samples` | `20` | Minimum settled sample count in the relevant bucket. |
| `crypto_empirical_bucket_min_net_pnl_dollars` | `0.0` | Minimum bucket net P/L. |
| `crypto_empirical_bucket_min_win_rate` | `0.55` | Minimum bucket win rate. |

The risk engine also requires empirical bucket allowance for the special crypto
same-side add-on path.

## Execution Knobs

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_order_mode` | `passive_then_taker` | `passive_only` places only passive orders. `passive_then_taker` can fall back to taker under strict conditions. |
| `crypto_passive_timeout_seconds` | `5` | Configured timeout for passive handling where used by the crypto path. |
| `crypto_taker_fallback_close_seconds` | `90` | Non-late-sure-thing taker fallback only inside this close window. |
| `crypto_default_order_count_fp` | `1.0` | Fallback crypto ticket size before risk resizing. Non-live-quality candidates keep this size. |

Execution behavior:

- Crypto creates a buy ticket with `time_in_force="immediate_or_cancel"`.
- Passive-first execution tries the passive price before taker fallback.
- `passive_only` stops after passive execution fails or loses edge.
- `passive_then_taker` can fall back to taker if the candidate remains eligible.
- Base limit execution uses code constants for requotes and fill polling, not
  configurable settings.
- App shadow mode, room shadow mode, kill switch, inactive color, and missing
  write credentials all block real orders.

## Risk and Position Knobs

General risk settings affect crypto live entries.

| Setting | Current default | Effect |
| --- | ---: | --- |
| `risk_order_pct` | `0.05` | Order-size cap as a fraction of capital, unless dollar overrides are active. |
| `risk_position_pct` | `0.10` | Position cap as a fraction of capital. Crypto entries require capital to enforce this. |
| `risk_daily_loss_pct` | `0.20` | Portfolio daily-loss envelope. |
| `risk_daily_loss_sensitivity_pct` | `0.10` | Lower daily-loss threshold where sensitivity multipliers can apply. |
| `risk_daily_loss_sensitivity_edge_multiplier` | `2.0` | Edge multiplier used under daily-loss sensitivity behavior. |
| `risk_daily_loss_sensitivity_size_multiplier` | `0.50` | Size multiplier used under daily-loss sensitivity behavior. |
| `risk_max_concurrent_tickers` | `10` | Max open tickers. |
| `risk_max_order_notional_dollars` | `None` | Optional hard dollar cap per order. |
| `risk_max_position_notional_dollars` | `None` | Optional hard dollar cap per position. |
| `risk_daily_loss_limit_dollars` | `None` | Optional hard dollar daily-loss cap. |
| `risk_daily_loss_dollars_by_strategy` | `{}` | Optional per-strategy daily-loss cap. Use key `CRYPTO_15M` for crypto. |
| `risk_edge_scaled_sizing_enabled` | `False` | Enables edge-scaled sizing. Still bounded by existing caps. |
| `risk_edge_scaled_kelly_multiplier` | `0.25` | Fractional-Kelly multiplier when edge-scaled sizing is enabled. |
| `crypto_dynamic_order_sizing_enabled` | `True` | Enables crypto-specific dynamic initial ticket sizing. |
| `crypto_dynamic_order_sizing_scope` | `live_quality` | Candidate scope for crypto dynamic sizing. Current behavior sizes only `live_quality` candidates dynamically. |
| `crypto_dynamic_order_target_position_pct` | `0.10` | Target crypto position allocation as a fraction of capital before risk applies hard caps. |
| `risk_max_order_count_fp` | `500.0` | Max ticket count. |
| `risk_max_position_count_fp_per_ticker` | `200.0` | Max same-ticker position plus in-flight count. |
| `risk_allow_position_add_ons` | `False` | Global same-side add-on switch. If false, only the crypto-specific add-on exception can allow approved add-ons. |
| `risk_safe_capital_reserve_ratio` | `0.0` | Reserve ratio used in capital-bucket thresholds. |
| `risk_risky_capital_max_ratio` | `0.0` | Risky-capital max ratio used in capital-bucket thresholds. |
| `risk_stale_market_seconds` | `60` | Blocks stale Kalshi market data. |
| `research_stale_seconds` | `900` | Blocks stale research/signal inputs. |
| `risk_min_edge_bps` | `500` | Global minimum fee-adjusted edge floor. |
| `risk_max_credible_edge_bps` | `10000` | Blocks implausibly large edge. |
| `risk_min_confidence` | `0.80` | Minimum signal confidence. |
| `risk_min_contract_price_dollars` | `0.50` | Minimum contract price. Also floors crypto policy overrides. |
| `risk_min_probability_extremity_pct` | `25.0` | Mid-band probability guard input. |
| `risk_probability_midband_max_extra_edge_bps` | `500` | Extra edge requirement cap from mid-band probability guard. |

Crypto dynamic sizing:

- Applies to accepted `CRYPTO_15M` candidates only when candidate status is
  `live_quality`.
- Unit cost is YES price for YES tickets and `1 - yes_price` for NO tickets.
- Target notional is `total_capital * min(crypto_dynamic_order_target_position_pct, risk_position_pct)`.
- Available notional subtracts current position notional and pending same-side
  order notional.
- Requested count is floored to `0.01` contract increments and capped by
  `risk_max_order_count_fp`, remaining `risk_max_position_count_fp_per_ticker`,
  and the target notional budget.
- If the computed count is below `crypto_default_order_count_fp`, the workflow
  keeps the default count and lets risk resize or block it.
- Risk still independently enforces the 10% position-notional cap, missing
  capital blocks, and all order/count/notional limits.

Crypto-specific add-on settings:

| Setting | Current default | Effect |
| --- | ---: | --- |
| `crypto_position_add_ons_enabled` | `True` | Enables the crypto-specific same-side add-on exception. |
| `crypto_position_add_on_assets` | `live` | Assets allowed to use the crypto add-on exception. `live`, `all`, `any`, or `*` allow any live-quality crypto asset; an explicit CSV list still narrows the scope. |
| `crypto_position_add_on_max_position_count_fp` | `200.0` | Max projected count for approved crypto add-ons. The 10% notional cap is the primary limiter. |
| `crypto_position_add_on_max_ticket_count_fp` | `500.0` | Max ticket count for approved crypto add-ons. The 10% notional cap can still downsize the final order. |

Crypto add-ons require all of these:

- Strategy is `CRYPTO_15M`.
- Asset is allowed by `crypto_position_add_on_assets`.
- There is an existing same-side position.
- Candidate is `live_quality`.
- Empirical bucket gate allows it.
- Ticket count and projected position count fit the crypto add-on count caps.
- If the recommendation would exceed the 10% position-notional cap but some
  budget remains, risk downsizes the approved count to the remaining budget
  instead of blocking only because the recommendation was oversized.

## Live Path and Readiness Knobs

The live-path command is the fastest way to see what is still blocking an asset.

| CLI flag | Current default | Effect |
| --- | ---: | --- |
| `crypto-live-path status --status-days` | `14` | Status lookback window. |
| `crypto-live-path status --strict-rows-target` | `60` | Target strict labeled real-quote rows per asset. |
| `crypto-live-path status --candidate-target` | `50` | Target trade candidates per asset. |
| `crypto-live-path status --require-ready` | `False` | Exits non-zero if readiness is not met. |
| `crypto-live-path status --baselines` | `False` | Includes baseline details. |
| `crypto-live-path refresh --settled-days` | `2` | Settled-market collection window. |
| `crypto-live-path refresh --history-days` | `2` | History refresh window. |
| `crypto-live-path refresh --spot-days` | `2` | Spot refresh window. |
| `crypto-live-path refresh --replay-days` | `30` | Replay refresh window. |
| `crypto-live-path refresh --until-ready` | `False` | Repeats refresh until ready or iteration cap. |
| `crypto-live-path refresh --max-iterations` | `1` | Max refresh loops. |
| `crypto-live-path refresh --sleep-seconds` | `0.0` | Sleep between refresh loops. |

Commands:

```bash
python -m kalshi_bot.cli crypto-live-path status --kalshi-env production --frequency 15m --json
python -m kalshi_bot.cli crypto-live-path status --kalshi-env production --frequency 15m --assets BTC --require-ready --json
python -m kalshi_bot.cli crypto-live-path refresh --kalshi-env production --frequency 15m --assets BTC --json
```

Other useful diagnostics:

```bash
python -m kalshi_bot.cli crypto-status --kalshi-env production --frequency 15m
python -m kalshi_bot.cli funnel-report --kalshi-env production --domain crypto --days 7 --frequency 15m --json
python -m kalshi_bot.cli model-quality status --kalshi-env production --domain crypto --days 180 --frequency 15m --json
python -m kalshi_bot.cli overnight-readiness report --kalshi-env production --domains crypto --frequency 15m --json
```

## Crypto Policy Optimization

`crypto-policy optimize` analyzes recent crypto evidence and can propose policy
changes.

```bash
python -m kalshi_bot.cli crypto-policy optimize --kalshi-env production --frequency 15m --days 30 --json
python -m kalshi_bot.cli crypto-policy optimize --kalshi-env production --frequency 15m --days 30 --assets BTC ETH --json
```

This affects policy-level candidates, not raw data collection. Policy changes
still need to survive agent-pack sanitization, replay requirements, and live
deployment controls.

## Autonomous Gate Tuning

General autonomous gate tuning can include crypto when invoked for the crypto
domain.

Relevant settings:

| Setting | Current default | Effect |
| --- | ---: | --- |
| `autonomous_gate_tuning_enabled` | `True` | Enables autonomous tuning framework. |
| `autonomous_gate_tuning_source` | `combined` | Evidence source selector. |
| `autonomous_gate_tuning_days` | `3650` | Lookback window for tuning evidence. |
| `autonomous_gate_tuning_min_support` | `30` | Minimum support required for tuning. |
| `autonomous_gate_tuning_canary_min_settled_rows` | `10` | Minimum canary rows for promotion decisions. |
| `autonomous_gate_tuning_canary_max_wait_hours` | `72` | Max wait for canary evidence. |
| `autonomous_gate_tuning_periodic_interval_seconds` | `3600` | Periodic tuning cadence. |

Use the crypto domain and crypto asset flags when tuning crypto gates.

## Change Protocol

Use this order when changing crypto trading behavior:

1. Check readiness and blockers.

   ```bash
   python -m kalshi_bot.cli crypto-live-path status --kalshi-env production --frequency 15m --json
   ```

2. Refresh market and spot evidence.

   ```bash
   python -m kalshi_bot.cli crypto-live-path refresh --kalshi-env production --frequency 15m --json
   ```

3. Train or retrain the model if the data changed materially.

   ```bash
   python -m kalshi_bot.cli crypto-model train --kalshi-env production --frequency 15m
   ```

4. Run and validate replay.

   ```bash
   python -m kalshi_bot.cli crypto-replay run --kalshi-env production --frequency 15m --days 30 --json
   python -m kalshi_bot.cli crypto-replay gate --kalshi-env production --frequency 15m
   ```

5. Review funnel and model quality.

   ```bash
   python -m kalshi_bot.cli funnel-report --kalshi-env production --domain crypto --days 7 --frequency 15m --json
   python -m kalshi_bot.cli model-quality status --kalshi-env production --domain crypto --frequency 15m --json
   ```

6. Move one asset at a time from `shadow` to `live`.

   ```bash
   python -m kalshi_bot.cli crypto-asset-mode set --kalshi-env production BTC live
   ```

7. Run one autonomy pass and inspect candidate traces before allowing broader
   scheduling.

   ```bash
   python -m kalshi_bot.cli crypto-autonomy run-once --kalshi-env production --frequency 15m --assets BTC --json
   ```

## Safer vs More Aggressive Changes

Safer changes:

- Increase `min_fee_adjusted_edge_bps`.
- Decrease `max_spread_bps`.
- Increase `min_confidence`.
- Increase `min_contract_price_dollars`.
- Increase replay sample and candidate requirements.
- Keep asset modes in `shadow` until strict real-quote evidence improves.
- Use `passive_only`.
- Keep `crypto_trading_enabled` false while collecting evidence.

More aggressive changes:

- Decrease entry edge requirements.
- Increase max spread.
- Lower replay sample requirements.
- Lower replay spot coverage requirements.
- Disable market-mid P/L advantage requirements.
- Enable production autonomy.
- Set assets to `live`.
- Use `passive_then_taker`.
- Increase order count, notional caps, or add-on allowances.

## Code-Only Constants and Behaviors

These are important but are not first-class settings today:

| Item | Current behavior |
| --- | --- |
| `CRYPTO_MIN_REMAINING_PAYOUT_BPS` | Fixed at `0`. |
| Base execution requotes | Code constant in execution path. |
| Base execution fill timeout | Code constant in execution path. |
| Base execution poll interval | Code constant in execution path. |
| Candidate statuses | Defined by crypto candidate selection logic. |
| Default live-path asset list | BTC, ETH, SOL, XRP, BNB, DOGE, HYPE. |
| Default live-path strict rows target | `60`. |
| Default live-path candidate target | `50`. |

If any of these need to become adjustable at runtime, promote them to settings
or explicit agent-pack policy fields rather than changing call-site constants.
