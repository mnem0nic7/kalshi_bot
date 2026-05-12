# Weather Bootstrap Activation Runbook

Date: 2026-05-12

## Preconditions

- Active deployment color is healthy.
- Kill switch is clear and post-clear reconcile is newer than the clear time.
- Source-health pause is not active.
- The active weather policy has not expired.
- `weather_empirical_bootstrap` traces show non-zero cold shadow matches.
- No matched bootstrap trace uses fallback, unavailable, dark, or none fair
  value.
- Trading audit daily funnel shows bootstrap gates are not allowing every
  decision.

## Initial Live Configuration

- Cold tier only.
- One concurrent bootstrap position.
- Daily bootstrap notional cap: $100.
- Size factor: 10% of policy size.
- Warming and maturing tiers remain shadow-only.

## Monitoring

Check daily for the first 14 days:

- bootstrap decisions by tier and outcome
- daily bootstrap notional used
- concurrent bootstrap positions
- bootstrap win/loss streak
- kill-switch state and reason
- realized P/L for settled bootstrap trades
- trading audit funnel top blockers

## Rollback Triggers

- bootstrap kill switch trips
- three consecutive bootstrap losses
- two consecutive losing calendar days
- fallback fair-value row reaches bootstrap allow path
- operator decision

Rollback by disabling cold-tier `live_enabled` or setting rollout state back to
`shadow`, then run trading audit and reconciliation.

## Graduation

Enable warming only after at least 10 resolved cold-tier trades, win rate of at
least 60%, and total bootstrap P/L greater than $0.

