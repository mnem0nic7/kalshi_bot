# Architecture

## Runtime shape

The platform is one async Python service with four main layers:

1. Integrations
   - Kalshi REST signing and order submission
   - Kalshi WebSocket streaming for market and account events
   - NOAA/NWS weather ingestion
2. Deterministic engines
   - Weather fair-value estimation
   - Risk validation
   - Execution and reconciliation
3. Agent room
   - Researcher
   - President
   - Trader
   - Risk officer
   - Execution clerk
   - Ops monitor
   - Auditor
   - Memory librarian
4. Control room
   - FastAPI
   - server-rendered UI
   - SSE transcript stream
   - kill-switch and promotion controls

## Workflow

The supervisor runs a fixed workflow:

1. Trigger room
2. Load market mapping
3. Fetch Kalshi market snapshot
4. Fetch weather bundle from NWS
5. Compute deterministic signal
6. Researcher posts evidence-backed observation
7. President posts posture memo
8. Trader emits either a `TradeTicket` or a stand-down message
9. Risk officer explains deterministic verdict
10. Execution clerk either places the order or records why execution was skipped
11. Auditor ties the rationale chain together
12. Memory librarian distills the room into durable semantic memory

## Safety boundaries

- LLM output never signs requests.
- LLM output never bypasses risk rules.
- Only the execution clerk path can hit Kalshi write endpoints.
- Only the active deployment color can acquire the execution lock.
- The kill switch clears the execution lock and blocks new live orders.

## Storage model

Postgres stores:

- room and transcript state
- raw exchange and weather events
- signals, tickets, verdicts, orders, fills, positions
- ops events and checkpoints
- memory notes and embeddings
- deployment control and writer lock

`pgvector` is used for semantic memory. The app falls back cleanly in SQLite-backed tests via a JSON-compatible type wrapper.

## Live stream path

The websocket ingest path is separate from room execution:

1. Connect to authenticated Kalshi WS
2. Subscribe to `market_lifecycle_v2`, `orderbook_delta`, `user_orders`, and `fill`
3. Persist every raw event
4. Maintain in-memory orderbook state per market
5. Upsert derived best bid/ask into `market_state`
6. Upsert user order and fill records
7. Store per-subscription checkpoints for restart diagnostics

## Auto-trigger path

When auto-triggering is enabled:

1. Streamed orderbook updates refresh `market_state`
2. The auto-trigger coordinator checks whether the market is configured and actionable
3. It enforces cooldown and single-active-room rules per market
4. It creates a new room and launches the existing supervisor workflow
5. Trigger metadata is stored in checkpoints under `auto_trigger:<market>`
