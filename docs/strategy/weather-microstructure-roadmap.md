# Weather + Microstructure Roadmap

## Strategy A

Directional trading on unresolved structured weather contracts.

This is the active strategy today.

Core rules:

- structured weather only
- deterministic weather state recognition
- forecast pricing only while unresolved
- early stand-down on stale, resolved, low-upside, or wide-spread setups

## Strategy B

Directional weather trading with deeper microstructure filtering on unresolved contracts.

This is the next extension path after the current hardening slice. It keeps the same weather thesis but gets stricter about:

- quote quality
- payout left
- spread regime
- late-day actionability

## Strategy C

Resolution-lag cleanup trading.

This strategy is explicitly not active in the current base system.

If pursued later, it should be:

- shadow-only first
- separately documented
- separately evaluated
- governed by its own risk and labeling policy

The key point is separation: a future cleanup strategy should not be mixed into the base directional weather logic.

## Audit Lens

Completed rooms should be reviewable under five questions:

1. Was the thesis directionally correct?
2. Was the trade actually good, or merely mathematically positive?
3. If blocked, was the block correct?
4. Should the room have stood down earlier?
5. Did freshness disagree across research, trader, and risk?

That audit lens is what turns rooms like the Chicago regression into useful training labels instead of just anecdotes.
