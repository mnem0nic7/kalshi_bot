# BTC Strict-Quote Ingestion Audit

Date: 2026-05-12

## Audit Path

The strict-quote audit is exposed in crypto quote evidence as
`strict_quote_ingestion_audit_by_asset`. For BTC, DOGE, and SOL it reports:

- snapshot rows present
- rows with real bid/ask quotes
- rows with settled labels joined
- point-in-time rows
- rows with fresh spot joined
- strict-trade-eligible rows
- rows that reached candidate generation

Recent settlements are repaired by:

```bash
kalshi-bot-cli crypto-history collect-settled --kalshi-env production --frequency 15m --days 2 --assets BTC --json
```

The command writes immutable `settled_backfill` snapshots. It should increase
`settled_label_joined` once matching pre-close `live_quote_evidence` snapshots
exist.

## Root-Cause Lens

BTC should have the highest strict real-quote capture. If it does not, compare
BTC pass-through against DOGE and SOL at each step. The likely root causes are:

- real bid/ask snapshots not being persisted
- recent settled labels not being collected from `markets?status=settled`
- spot rows stale under the Coinbase freshness gate
- settlement labels not joining by market ticker
- strict-trade rows blocked before candidate generation by missing fresh spot

## Operator Command

Run:

```bash
kalshi-bot-cli crypto-history status --kalshi-env production --frequency 15m --days 14 --json
```

Then inspect:

```json
quote_evidence.strict_quote_ingestion_audit_by_asset.BTC
```
