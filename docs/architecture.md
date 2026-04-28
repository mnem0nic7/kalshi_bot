# Architecture

## Runtime shape

The platform is one async Python service with four main layers:

1. Integrations
   - Kalshi REST signing and order submission
   - Kalshi WebSocket streaming for market and account events
   - NOAA/NWS weather ingestion
2. Deterministic engines
   - Weather fair-value estimation
   - Shadow-first Gumbel/KDE/climatology probability primitives
   - Adapter-first ensemble fusion primitives
   - Shadow-first uncertainty, fee-aware Kelly, survival, and exit-risk primitives
   - Risk validation
   - Execution and reconciliation
3. Decision trace and replay
   - normalized deterministic intent
   - source snapshot references
   - input and intent hashes
4. Agent room (legacy/experimental for trade decisions)
   - Researcher
   - President
   - Trader
   - Risk officer
   - Execution clerk
   - Ops monitor
   - Auditor
   - Memory librarian
5. Control room
   - FastAPI
   - server-rendered UI
   - SSE transcript stream
   - kill-switch and promotion controls

## Workflow

The canonical supervisor path is deterministic when `LLM_TRADING_ENABLED=false` (the default):

1. Trigger room
2. Load market mapping
3. Fetch Kalshi market snapshot
4. Fetch weather bundle from NWS
5. Compute deterministic signal
6. Run deterministic eligibility and marketability gates
7. Build a `TradeTicket` only when the signal is eligible
8. Run the deterministic risk engine
9. Execute only approved tickets through the execution clerk and active-color lock
10. Persist a `decision_traces` row with source references, thresholds, signal state, sizing context, risk verdict, receipt, final outcome, and stable hashes
11. Persist strategy audit material for training and replay

The LLM agent-room path still exists behind `LLM_TRADING_ENABLED=true`. It is treated as legacy/experimental scaffolding for analysis and prompt-pack work, not the production decision authority.

Optional learned-head and NWS-discussion-parser modules exist only as shadow-first interfaces. They define deterministic feature contracts, stable hashes, zero-weight fallback behavior, and strict JSON validation; they do not affect live probability, sizing, or execution.

## Safety boundaries

- LLM output never signs requests.
- LLM output never bypasses risk rules.
- LLM output is not part of the default trading decision path.
- Only the execution clerk path can hit Kalshi write endpoints.
- Only the active deployment color can acquire the execution lock.
- The kill switch clears the execution lock and blocks new live orders.

## Storage model

Postgres stores:

- room and transcript state
- raw exchange and weather events
- signals, tickets, verdicts, orders, fills, positions
- deterministic decision traces and replay hashes
- forecast snapshots and climatology priors for future replay-gated probability promotion
- source health logs for per-provider success, freshness, completeness, consistency, and aggregate pause audits
- deterministic parameter packs and holdout reports for future replay-gated promotion
- staged parameter-pack promotion evidence in `promotion_events` and `deployment_control.notes.parameter_packs`
- shadow-canary parameter-pack evidence before any operator promotion
- sealed hard-cap config hashes so operator-owned risk caps remain outside parameter packs
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
