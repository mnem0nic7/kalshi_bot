# Prediction-scaled take-profit — design (2026-07-04)

Operator ask: "adjust take profit as our model predictions update." Approved
mechanic: **confidence-scaled threshold** (option B of three presented; edge-based
exit and prediction-flip exit were considered and rejected as too large a
behavior change / too coarse respectively).

## Mechanic

`CryptoTakeProfitService._evaluate` currently exits when
`profit_ratio >= threshold` with a static per-frequency/per-asset threshold
(base: 50% on 15m, 30% on 1h). Change: the threshold becomes dynamic:

```
edge_remaining = shrink_beta * (model_fair_side_value - current_mid_side_value)   # dollars, position side
frac           = clamp(edge_remaining / edge_ref, 0.0, 1.0)
effective      = base * (min_mult + (max_mult - min_mult) * frac)
```

- `edge_remaining <= 0` (model sees the move as done) → `min_mult * base`
  (default 0.5× → exit at 25% on 15m).
- `edge_remaining >= edge_ref` (default $0.05 shrunk) → `max_mult * base`
  (default 2.0× → hold while the model still sees material edge).
- Linear in between. Shrinkage uses the live edge-shrinkage β so units match
  realized edge.

## Prediction source

Inline `CryptoForecastService.forecast(market)` from the TP service, throttled
to at most one call per market per `crypto_take_profit_prediction_refresh_seconds`
(default 30) with an in-process cache, because the position-exit hot loop can
run sub-second and forecast() loads model artifacts. Reading
`crypto_decision_outcomes` was rejected: decision rows may stop updating for a
market once a position is open, which would silently disable the feature.

## Fallback (operator choice: "keep today's static threshold")

Any of the following → use the static base threshold, exactly today's
behavior:

- no champion model / champion is `market_mid_baseline`
- forecast older than `crypto_take_profit_prediction_max_age_seconds` (default 120)
- forecast raises / returns no usable fair value
- scaling disabled via `crypto_take_profit_prediction_scaling_enabled`

The exit ops record includes `threshold_mode: scaled|static` plus the inputs
(`edge_remaining`, `effective_threshold`) so the feature can be evaluated from
history.

## Config (Settings, `crypto_take_profit_*`)

| Setting | Default |
|---|---|
| `crypto_take_profit_prediction_scaling_enabled` | `true` |
| `crypto_take_profit_edge_ref_cents` | `5` |
| `crypto_take_profit_min_multiplier` | `0.5` |
| `crypto_take_profit_max_multiplier` | `2.0` |
| `crypto_take_profit_prediction_refresh_seconds` | `30` |
| `crypto_take_profit_prediction_max_age_seconds` | `120` |

Not tuner-managed (same status as the existing TP thresholds).

## Blast radius

- Stop-loss untouched.
- Manual bracket/daily positions have no model → static path, unchanged.
- In practice scaling activates today only for HYPE 15m (only gate-passed
  trained champion) and extends automatically as champions pass gates.
- The pilot's positions are 15m BTC/BNB → champions are baseline → static
  path; the pilot's hold-to-settlement behavior is not affected by scaling
  today.

## Testing

Unit (`tests/unit/test_crypto_take_profit_scaling.py`):
- scaling math: floor at ≤0 edge, cap at ≥ref, linearity, side sign
  conventions (yes and no positions)
- throttle: second lookup within refresh window hits cache, not forecast()
- `_evaluate` end-to-end with stubbed forecast: early exit on edge collapse
  (profit ≥ scaled-down threshold but < base), hold when edge persists
  (profit ≥ base but < scaled-up threshold), static fallback on baseline /
  stale / error / disabled.

Existing `crypto_take_profit` tests must pass unchanged with scaling disabled.
