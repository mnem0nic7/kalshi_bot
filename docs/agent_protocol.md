# Agent Protocol

## Roles

- `researcher`: turns exchange plus weather data into evidence-backed observations
- `president`: sets capital posture and session tone
- `trader`: emits structured `TradeTicket` proposals
- `risk_officer`: narrates deterministic risk verdicts
- `execution_clerk`: submits or skips orders and records receipts
- `ops_monitor`: reports health, stale feeds, and workflow failures
- `auditor`: links message-level rationale to execution outcomes
- `memory_librarian`: turns sessions into retrievable memory notes

## Message kinds

- `Observation`
- `EvidenceArtifact`
- `PolicyMemo`
- `TradeIdea`
- `TradeTicket`
- `RiskVerdict`
- `ExecReceipt`
- `OpsAlert`
- `MemoryNote`
- `IncidentAction`

## Contract rules

- Every room message is append-only and gets a per-room sequence number.
- The researcher consumes a persisted market dossier plus a room-local delta rather than improvising from raw inputs alone.
- `TradeTicket` messages carry the machine-readable order payload that downstream code validates.
- The trader may not emit a live proposal unless the latest research gate passes.
- Risk and execution messages must reference the ticket they evaluate.
- Memory notes should link back to the last few messages in the room.

## LLM usage

- Hosted models are routed to `trader` and `president` when configured.
- Cheaper local or local-compatible models are routed to `researcher`, `ops_monitor`, and `memory_librarian`.
- Fallback behavior is deterministic if no provider is available.
