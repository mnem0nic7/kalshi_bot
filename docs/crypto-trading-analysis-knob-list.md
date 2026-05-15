# Crypto Trading Analysis Knob List

This is the short list of crypto knobs worth testing with data analysis. It
intentionally excludes deployment switches, credentials, kill switches, and
other operational controls.

Use this as the candidate grid/search space for `CRYPTO_15M`.

## Entry Quality

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `min_fee_adjusted_edge_bps` | `500` | bps | Minimum post-fee edge required to enter. Sweep higher/lower by asset and time-to-close bucket. |
| `max_spread_bps` | `1000` | bps | Max bid/ask spread accepted for live crypto entries. Built-in pack and runtime fallback use `crypto_live_max_spread_bps`. |
| `min_confidence` | `0.80` | probability | Minimum model confidence. Test against realized win rate and candidate starvation. |
| `min_contract_price_dollars` | `0.50` | dollars | Minimum entry contract price. Test whether lower-priced entries have enough realized edge after fees. |
| `max_credible_edge_bps` | `10000` | bps | Upper guardrail for implausibly large edge. Mostly a data-quality/outlier filter. |
| `min_remaining_payout_bps` | `0` | bps | Fixed crypto constant today. Include only if promoted to a setting. |
| `risk_min_probability_extremity_pct` | `25.0` | percent | Mid-band probability guard input. Test whether avoiding uncertain mid-band markets improves P/L. |
| `risk_probability_midband_max_extra_edge_bps` | `500` | bps | Max extra edge demanded by the mid-band guard. |

## Entry Timing

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_live_min_market_age_seconds` | `180` | seconds | Earliest allowed live entry after market opens. Sweep by asset and volatility regime. |
| `crypto_autonomy_min_seconds_to_close` | `0` | seconds | Minimum time remaining for autonomy to consider a market. Test stricter entry cutoffs such as 60, 120, 180, 300. |
| `crypto_taker_fallback_close_seconds` | `0` | seconds | Taker fallback window for normal candidates. `0` keeps normal edge trades passive-only. Test fill rate vs edge decay near close. |
| `crypto_late_sure_thing_max_seconds_to_close` | `300` | seconds | Max time-to-close for late sure-thing path. |

## Late Sure-Thing Path

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_late_sure_thing_enabled` | `True` | bool | Compare normal-only vs late-sure-thing candidates. |
| `crypto_late_sure_thing_min_probability` | `0.85` | probability | Minimum model probability for late sure thing. |
| `crypto_late_sure_thing_min_market_probability` | `0.75` | probability | Minimum market-implied probability for late sure thing. |

## Market Anchor

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_market_price_anchor_enabled` | `True` | bool | Compare anchored model probabilities vs raw model probabilities. |
| `crypto_market_price_anchor_weight` | `0.75` | weight | Strength of market-mid anchoring. Sweep from weak to strong anchoring. |

## Replay Promotion Gates

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_replay_min_resolved_markets` | `500` | count | Minimum resolved markets before live eligibility. |
| `crypto_replay_min_trade_candidates` | `50` | count | Minimum strict candidate count. Test starvation vs reliability. |
| `crypto_replay_min_net_pl_dollars` | `0.0` | dollars | Replay P/L threshold. Current logic requires P/L greater than this. |
| `crypto_replay_min_pnl_advantage_dollars` | `0.0` | dollars | Required advantage over market-mid baseline. |
| `crypto_replay_require_pnl_beats_market_mid` | `True` | bool | Whether replay must beat market mid. |
| `crypto_replay_require_calibration_better_than_mid` | `False` | bool | Whether calibration must beat market mid. |
| `crypto_replay_min_spot_coverage_pct` | `0.80` | ratio | Minimum spot coverage in replay window. |
| `crypto_replay_max_hard_cap_breaches` | `0` | count | Max allowed hard-cap breaches. Usually keep at zero. |

## Empirical Bucket Gate

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_empirical_bucket_gate_enabled` | `True` | bool | Compare bucket-gated vs ungated decisions in replay only. |
| `crypto_empirical_bucket_gate_assets` | `live` | selector | Which assets/modes require bucket evidence. |
| `crypto_empirical_bucket_min_samples` | `20` | count | Minimum settled rows per empirical bucket. |
| `crypto_empirical_bucket_min_net_pnl_dollars` | `0.0` | dollars | Minimum bucket net P/L. |
| `crypto_empirical_bucket_min_win_rate` | `0.55` | probability | Minimum bucket win rate. |

## Shadow Exploration

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_shadow_exploration_min_expected_net_edge_dollars` | `-0.03` | dollars | How far below zero shadow exploration may sample. |
| `crypto_shadow_exploration_max_spread_bps` | `500` | bps | Max spread for shadow candidates. |
| `crypto_shadow_exploration_max_candidates_per_run` | `12` | count | Total shadow candidates retained per run. Mostly affects evidence volume. |
| `crypto_shadow_exploration_max_per_asset_per_run` | `2` | count | Per-asset shadow candidate cap. Mostly affects evidence balance. |

## Order Mode and Fill Behavior

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_order_mode` | `passive_then_taker` | enum | Compare `passive_only` vs `passive_then_taker` in replay/simulation using realistic fill assumptions. |
| `crypto_passive_timeout_seconds` | `5` | seconds | Passive wait window where used. Test fill probability vs stale edge. |
| `crypto_default_order_count_fp` | `1.0` | contracts | Fallback order size before risk resizing. Non-live-quality candidates keep this size. |

## Risk and Sizing

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `risk_order_pct` | `0.05` | fraction of capital | Per-order capital allocation. |
| `risk_position_pct` | `0.10` | fraction of capital | Per-position capital cap. |
| `risk_max_order_notional_dollars` | `None` | dollars | Optional hard order cap. Useful for fixed-size experiments. |
| `risk_max_position_notional_dollars` | `None` | dollars | Optional hard position cap. |
| `risk_edge_scaled_sizing_enabled` | `False` | bool | Whether to size by edge instead of flat sizing. |
| `risk_edge_scaled_kelly_multiplier` | `0.25` | multiplier | Fractional-Kelly multiplier when edge-scaled sizing is enabled. |
| `crypto_dynamic_order_sizing_enabled` | `True` | bool | Whether live-quality crypto candidates request dynamic initial ticket size. |
| `crypto_dynamic_order_sizing_scope` | `live_quality` | selector | Which candidate scope dynamic sizing applies to. |
| `crypto_dynamic_order_target_position_pct` | `0.10` | fraction of capital | Target crypto position allocation for dynamic sizing, capped by `risk_position_pct`. |
| `risk_max_order_count_fp` | `500.0` | contracts | Max contracts per ticket. |
| `risk_max_position_count_fp_per_ticker` | `200.0` | contracts | Max contracts per ticker including in-flight orders. |
| `risk_daily_loss_dollars_by_strategy["CRYPTO_15M"]` | unset | dollars | Per-strategy daily stop. Test drawdown containment. |

## Crypto Add-On Path

| Knob | Current default | Units | What to test |
| --- | ---: | --- | --- |
| `crypto_position_add_ons_enabled` | `True` | bool | Whether crypto-specific same-side add-ons are allowed. |
| `crypto_position_add_on_assets` | `live` | asset scope/list | Assets eligible for add-ons. `live`, `all`, `any`, or `*` allow any live-quality crypto asset; CSV assets still narrow scope. |
| `crypto_position_add_on_max_position_count_fp` | `200.0` | contracts | Max projected count for add-on exception. The 10% notional cap remains the primary limiter. |
| `crypto_position_add_on_max_ticket_count_fp` | `500.0` | contracts | Max ticket size for add-on exception. Oversized notional can still be resized down to the 10% cap. |

## Analysis Dimensions

Slice every knob test by these dimensions:

- Asset: BTC, ETH, SOL, XRP, DOGE, BNB, HYPE.
- Side: YES vs NO.
- Time-to-close bucket.
- Market age bucket.
- Spread bucket.
- Contract-price bucket.
- Model probability bucket.
- Market-implied probability bucket.
- Raw edge bucket.
- Fee-adjusted edge bucket.
- Volatility or spot-move bucket.
- Coinbase strict spot vs fallback/proxy evidence.
- Candidate status.
- Replay fold or out-of-sample marker.

## Primary Objective Metrics

Use these as the optimization targets:

- Net P/L after fees.
- P/L advantage vs market-mid baseline.
- Expected value per contract.
- Fill-adjusted expected value.
- Win rate.
- Brier score and calibration error.
- Candidate count and live-quality candidate count.
- Candidate starvation rate.
- Max drawdown.
- Hard-cap breach count.
- Spot coverage.
- Quote-evidence strict-row count.

## Suggested First Sweep

Start with a small grid before doing broader optimization:

| Knob | Values to try |
| --- | --- |
| `min_fee_adjusted_edge_bps` | `250`, `500`, `750`, `1000`, `1500`, `2000` |
| `max_spread_bps` | `100`, `250`, `500`, `750`, `1000` |
| `crypto_live_min_market_age_seconds` | `0`, `60`, `180`, `300`, `600` |
| `crypto_autonomy_min_seconds_to_close` | `0`, `60`, `120`, `180`, `300` |
| `crypto_taker_fallback_close_seconds` | `0`, `30`, `60`, `90`, `180` |
| `min_confidence` | `0.60`, `0.70`, `0.80`, `0.90` |
| `min_contract_price_dollars` | `0.05`, `0.10`, `0.25`, `0.50`, `0.75` |
| `crypto_market_price_anchor_weight` | `0.00`, `0.25`, `0.50`, `0.75`, `1.00` |
| `crypto_late_sure_thing_min_probability` | `0.80`, `0.85`, `0.90`, `0.95` |
| `crypto_late_sure_thing_min_market_probability` | `0.60`, `0.70`, `0.75`, `0.80`, `0.90` |

Keep replay sample gates fixed during the first sweep. Once entry/timing behavior
is understood, sweep replay thresholds separately so the analysis does not mix
entry quality with promotion conservatism.
