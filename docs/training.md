# Training

This project supports two export formats for training preparation:

- room bundles: full decision records for analysis, curation, and custom dataset generation
- role SFT examples: role-specific prompt/target records for `researcher`, `president`, `trader`, and `memory_librarian`
- evaluation holdouts: reproducible room slices for offline pack comparisons
- historical replay bundles and Gemini-first fine-tune exports built from strict as-of settled weather days

The exports are JSONL so they can feed notebooks, curation scripts, or direct fine-tuning pipelines. Role SFT exports include both structured `input_context` / `target` fields and a model-ready `messages` array for chat-style fine-tuning.

Those same exported bundles are also the substrate for the self-improvement loop described in [self_improve.md](self_improve.md). The daily critique and holdout evaluation path works only on stored shadow or demo rooms, never on raw live market access.

Historical replay uses the same canonical room bundle shape, but marks rows with `room_origin = historical_replay` and stores provenance, replay checkpoint time, settlement label, and counterfactual PnL alongside the standard room artifacts.

## Historical Replay

Historical replay is strict as-of, weather-only, and structured-only.

For each settled market-day we:

- import Kalshi market metadata and settlement labels
- import locally captured market snapshots and weather bundles when available
- optionally ingest file-backed archived weather bundles from `HISTORICAL_WEATHER_ARCHIVE_PATH`
- select fixed replay checkpoints at `09:00`, `13:00`, and `17:00` local market time, or `1 hour before close` if earlier
- choose only sources with `source_ts <= checkpoint_ts`
- rebuild dossier, signal, eligibility, strategy audit, and dry-run risk at that checkpoint

If a checkpoint is missing a captured market snapshot or weather bundle, it is skipped rather than fabricated. That keeps the historical corpus traceable and leakage-resistant.
Going forward, the daemon also captures dedicated checkpoint weather bundles on schedule so future settled days can become full-coverage historical replay days without depending on room traffic.

Deploy findings from April 12, 2026:

- the new checkpoint archive path is healthy, but a manual `historical-archive checkpoint-capture --once` may correctly return zero captures when no checkpoint slot is currently due
- settlement backfill is already proving useful on the live shadow corpus and should be treated as a normal maturity-repair tool, not an emergency-only action
- the current blocker for historical Gemini fine-tuning is still missing full-checkpoint weather coverage, so exports should remain `draft_only` until distinct full-coverage market-days accumulate naturally

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
- historical provenance, replay checkpoint time, and counterfactual realized PnL when the room came from historical replay
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

Import settled historical weather market-days plus captured sources:

```bash
kalshi-bot-cli historical-import weather \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA
```

Replay historical checkpoints into room-shaped records:

```bash
kalshi-bot-cli historical-replay weather \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA
```

Inspect historical corpus status:

```bash
kalshi-bot-cli historical-status --verbose
```

Backfill missing checkpoint coverage before replay:

```bash
kalshi-bot-cli historical-backfill market \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA

kalshi-bot-cli historical-backfill weather-archive \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA

kalshi-bot-cli historical-archive capture --once \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA

kalshi-bot-cli historical-archive checkpoint-capture --once \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA

kalshi-bot-cli historical-archive checkpoint-status \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA \
  --verbose

kalshi-bot-cli historical-backfill settlements \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI KXHIGHMIA
```

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

Build historical replay datasets:

```bash
kalshi-bot-cli training-build historical \
  --mode bundles \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --output data/training/historical_bundles.jsonl
```

Build Gemini-first fine-tune artifacts:

```bash
kalshi-bot-cli training-build historical \
  --mode gemini-finetune \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --output data/training/gemini_weather
```

The Gemini export writes:

- `train.jsonl`
- `validation.jsonl`
- `holdout.jsonl`
- `manifest.json`

The split is chronological by local settlement day:

- oldest `70%` train
- next `15%` validation
- newest `15%` holdout

All checkpoints from the same market-day stay together in one split.

Gemini exports are historical-first by default and require full checkpoint coverage. If there are fewer than `3` distinct local market-days or no viable validation/holdout day, the build still succeeds but is marked `draft_only` in the manifest instead of pretending to be training-ready.

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

For historical replay, settlement labels are expected up front, but NOAA cross-check mismatches are quarantined by default and excluded from quality-cleaned historical builds.
