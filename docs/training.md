# Training

This project supports two export formats for training preparation:

- room bundles: full decision records for analysis, curation, and custom dataset generation
- role SFT examples: role-specific prompt/target records for `researcher`, `president`, `trader`, and `memory_librarian`
- evaluation holdouts: reproducible room slices for offline pack comparisons

The exports are JSONL so they can feed notebooks, curation scripts, or direct fine-tuning pipelines. Role SFT exports include both structured `input_context` / `target` fields and a model-ready `messages` array for chat-style fine-tuning.

Those same exported bundles are also the substrate for the self-improvement loop described in [self_improve.md](self_improve.md). The daily critique and holdout evaluation path works only on stored shadow or demo rooms, never on raw live market access.

## What Gets Captured

Room bundle exports include:

- room metadata
- room campaign metadata for why the room was created
- room-level research health summary
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
- agent-pack version, runtime environment, and per-role provider or model provenance

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

The fastest shadow-mode collection loop is now:

```bash
kalshi-bot-cli shadow-campaign run --limit 3
kalshi-bot-cli training-status
kalshi-bot-cli training-build --mode room-bundles --good-research-only
```

You can also launch one room at a time from the control room homepage with `Run Shadow Room`.

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
  --market-ticker <MARKET_TICKER> \
  --output data/training/market_room_bundles.jsonl
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

Create and run one shadow room directly from the CLI:

```bash
kalshi-bot-cli shadow-run KXHIGHNY-26APR11-T68
```

Run the structured-weather corpus scheduler:

```bash
kalshi-bot-cli shadow-campaign run --limit 3
```

Check corpus readiness and research health:

```bash
kalshi-bot-cli training-status
kalshi-bot-cli research-audit --limit 20
```

Create a reproducible dataset build and persist its metadata:

```bash
kalshi-bot-cli training-build \
  --mode room-bundles \
  --good-research-only \
  --output data/training/builds/weather_room_bundles.jsonl
```

List recent dataset builds:

```bash
kalshi-bot-cli training-build-list
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
