# Training

This project supports two export formats for training preparation:

- room bundles: full decision records for analysis, curation, and custom dataset generation
- role SFT examples: role-specific prompt/target records for `researcher`, `president`, `trader`, and `memory_librarian`

The exports are JSONL so they can feed notebooks, curation scripts, or direct fine-tuning pipelines.

## What Gets Captured

Room bundle exports include:

- room metadata
- full room transcript
- latest signal
- research dossier snapshot and room-local delta
- market snapshot and weather bundle artifacts when present
- research source cards
- trade ticket and risk verdict
- orders and fills
- memory note
- optional settlement label when reconciliation has already seen that market settle
- derived outcome summary such as final status, research gate result, risk status, and order/fill counts

Role SFT exports derive training rows from those bundles and currently support:

- `researcher`
- `president`
- `trader`
- `memory_librarian`

## Recommended Data Collection Loop

1. Run in demo or shadow mode first.
2. Keep reconciliation running so settlements can be attached later when available.
3. Use real configured market mappings, not the example file.
4. Let the system accumulate rooms across multiple market conditions.
5. Export bundles for analysis before curating role-specific SFT examples.

## Export Commands

Export complete room bundles:

```bash
kalshi-bot-cli training-export \
  --mode bundles \
  --output data/training/room_bundles.jsonl \
  --limit 500
```

Export only one market:

```bash
kalshi-bot-cli training-export \
  --mode bundles \
  --market-ticker WEATHER-NYC-HIGH-80F \
  --output data/training/nyc_room_bundles.jsonl
```

Export role-specific SFT examples:

```bash
kalshi-bot-cli training-export \
  --mode role-sft \
  --roles researcher trader president memory_librarian \
  --output data/training/role_sft.jsonl \
  --limit 500
```

Export one room by id:

```bash
kalshi-bot-cli training-export \
  --room-id <ROOM_ID> \
  --mode bundles \
  --output data/training/single_room.jsonl
```

Include non-complete rooms if you want failure or partial-workflow examples:

```bash
kalshi-bot-cli training-export \
  --mode role-sft \
  --include-non-complete \
  --output data/training/mixed_examples.jsonl
```

## When You Are Ready To Train

Use `bundles` first when:

- you want human review
- you want to filter for only good decision chains
- you want to derive custom labels
- you want to separate shadow-mode and live-mode behavior

Use `role-sft` when:

- you already trust the stored transcripts
- you want direct supervised examples for agent tone and structure
- you want a fast path into prompt tuning or fine-tuning

## Current Limitation

Outcome labels are best-effort today:

- behavioral and execution labels are always present
- settlement labels appear only after reconciliation has seen that market settle

That means you can start behavioral training now, while outcome-labeled policy training becomes stronger as more reconciled settlements accumulate.
