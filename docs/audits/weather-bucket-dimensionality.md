# Weather Bucket Dimensionality Audit

Date: 2026-05-12

## Finding

The legacy weather empirical bucket was too granular for cold start. It keyed
evidence by series, station, side, strategy, entry-price band, forecast-delta
band, confidence band, and spread band. With a 20 settled-fill minimum, this
made many production buckets impossible to mature organically.

The implementation now uses `empirical_bucket_v2` as the gate/bootstrap
identity:

- series ticker
- station
- side
- strategy code
- forecast-delta band
- coarse trade-quality band

The legacy key is still persisted in traces as `legacy_bucket_key`, while
`bucket_key` and `coarse_bucket_key` carry the v2 identity.

## Recommendation Applied

Drop spread from the identity because spread is already gated upstream. Replace
entry-price plus confidence bands with a coarse `quality:{low|medium|high}`
dimension. Preserve legacy keys for audit and historical re-keying.

## Operator Check

Use the trading audit daily funnel and bootstrap traces to confirm that:

- new rows include `bucket_key_version=empirical_bucket_v2`
- legacy bucket keys remain visible
- under-sampled empirical buckets decline over time
- mature buckets still require positive weighted P/L

