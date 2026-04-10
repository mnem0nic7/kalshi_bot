# Database

## Primary tables

- `rooms`
- `room_messages`
- `artifacts`
- `raw_exchange_events`
- `raw_weather_events`
- `market_state`
- `signals`
- `trade_tickets`
- `risk_verdicts`
- `orders`
- `fills`
- `positions`
- `ops_events`
- `memory_notes`
- `memory_embeddings`
- `checkpoints`
- `deployment_control`

## Notable rules

- `room_messages` are append-only and sequence-ordered per room.
- `trade_tickets.client_order_id` is unique.
- `orders.client_order_id` is unique for idempotent execution tracing.
- `deployment_control` is a singleton row controlling active color and kill switch status.

## Restore checks

After a restore:

1. Verify `deployment_control.active_color`.
2. Re-enable the kill switch before resuming trading.
3. Reconcile positions, fills, and open orders against Kalshi.

