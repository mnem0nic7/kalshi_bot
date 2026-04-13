# Training

This project supports two export formats for training preparation:

- room bundles: full decision records for analysis, curation, and custom dataset generation
- role SFT examples: role-specific prompt/target records for `researcher`, `president`, `trader`, and `memory_librarian`
- evaluation holdouts: reproducible room slices for offline pack comparisons
- historical replay bundles and Gemini-first fine-tune exports built from strict as-of settled weather days

The exports are JSONL so they can feed notebooks, curation scripts, or direct fine-tuning pipelines. Role SFT exports include both structured `input_context` / `target` fields and a model-ready `messages` array for chat-style fine-tuning.

Those same exported bundles are also the substrate for the self-improvement loop described in [self_improve.md](self_improve.md). The daily critique and holdout evaluation path works only on stored shadow or demo rooms, never on raw live market access.

Historical replay uses the same canonical room bundle shape, but marks rows with `room_origin = historical_replay` and stores provenance, replay checkpoint time, settlement label, and counterfactual PnL alongside the standard room artifacts.

Historical intelligence builds on top of those replayed rows. It consumes quality-cleaned historical replay bundles, computes execution and directional intelligence, and can stage versioned runtime heuristic packs when the support thresholds are honestly met.

The historical program is now meant to run as a rolling one-year loop, not as an occasional manual export. The default window is the past `365` settled days ending yesterday, and the pipeline keeps strict tier boundaries:

- `full_checkpoint_coverage`: eligible for directional replay, directional heuristics, decision-eval, and Gemini readiness
- `late_only_coverage` and `partial_checkpoint_coverage`: eligible for execution-quality and outcome analysis only
- `outcome_only_coverage`: settlement and PnL reference only, never directional replay
- `no_replayable_coverage`: not usable for replay-driven learning

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

Historical replay now uses checkpoint-time decision logic instead of wall-clock time. That matters because historical staleness and eligibility should be evaluated at the replay checkpoint, not at the moment the replay job happens to run.

The rolling one-year bootstrap is now chunked and resumable by default. The standard shape is `--chunk-days 14`, which lets us advance a long backfill in recoverable slices. If a bootstrap is interrupted, `historical-pipeline resume` continues from the persisted chunk cursor instead of repeating completed chunks.

The historical status surfaces now separate three different truths:

- `source_replay_coverage`: what the current strict-asof source tables could replay
- `checkpoint_archive_coverage`: what dedicated scheduled checkpoint captures alone could replay
- `replay_corpus`: what has actually been rebuilt into `historical_replay` rooms and is safe to use for readiness

Historical training readiness should be read from the replay corpus, not from source potential alone.

The confidence loop is separate from raw readiness:

- `insufficient_support`: not enough rolling-year support yet to trust either execution or directional heuristic changes
- `execution_confident_only`: enough replayable market-days to tune execution-quality heuristics, but not enough full-checkpoint support to trust directional rewrites
- `directional_confident`: enough execution support plus enough full-checkpoint market-days and holdout days to judge directional changes honestly

Historical status now also reports progress-to-threshold metrics directly:

- execution support progress toward `60` distinct execution-usable market-days
- directional support progress toward `30` distinct full-checkpoint market-days
- holdout support progress toward `7` full-checkpoint holdout market-days
- explicit blocker labels such as `lack_of_execution_support`, `lack_of_full_coverage_support`, and `lack_of_holdout_support`

Coverage backlog is now split into what is recoverable versus what is currently outcome-only:

- `promotable_to_full_checkpoint_coverage`: the day is already `late_only` or `partial`, so more checkpoint sources can lift it into full directional coverage
- `promotable_to_partial_or_late_only`: the day is currently `outcome_only`, but at least one side of the checkpoint evidence already exists
- `permanently_outcome_only_with_current_sources`: the day still has settlement and PnL value, but there is no current replay-source coverage to lift it without new archives

Historical settlement crosschecks now respect strict market operators. That means `>` and `<` no longer behave like inclusive comparisons at the exact threshold, which removes the false mismatch cluster we saw on exact-threshold days. Historical status now breaks crosschecks out as:

- `threshold_edge_strictness`: a strict threshold edge case that should not be treated like a random data disagreement
- `daily_summary_disagreement`: a real Kalshi versus NOAA/NCEI disagreement that should stay quarantined
- `crosscheck_missing`: no usable NOAA/NCEI crosscheck was available

Historical weather backfill now also promotes recoverable as-of weather evidence into checkpoint-archive records when the source bundle is already valid for that checkpoint. That does not fabricate missing history; it just upgrades already-valid weather evidence into the dedicated checkpoint path so replay support and checkpoint-archive coverage stay aligned.

Deploy findings from April 12, 2026:

- the new checkpoint archive path is healthy, but a manual `historical-archive checkpoint-capture --once` may correctly return zero captures when no checkpoint slot is currently due
- settlement backfill is already proving useful on the live shadow corpus and should be treated as a normal maturity-repair tool, not an emergency-only action
- the current blocker for historical Gemini fine-tuning is still missing full-checkpoint weather coverage, so exports should remain `draft_only` until distinct full-coverage market-days accumulate naturally
- historical replay repair is now part of the normal maintenance path when replay logic changes; stale derived replay rooms should be refreshed rather than trusted
- historical intelligence indicators are only trustworthy after replay refresh, because stale replay rooms can otherwise collapse into misleading blanket stand-down reasons like `market_stale`
- settlement refresh is now also part of replay maintenance; when crosscheck semantics or mismatch classification change, the rolling replay corpus and overlapping historical builds should be refreshed so readiness stays tied to current truth

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

Run the rolling one-year historical pipeline:

```bash
kalshi-bot-cli historical-pipeline bootstrap --days 365 --chunk-days 14 --series KXHIGHNY KXHIGHCHI KXHIGHMIA
kalshi-bot-cli historical-pipeline resume --series KXHIGHNY KXHIGHCHI KXHIGHMIA
kalshi-bot-cli historical-pipeline daily --series KXHIGHNY KXHIGHCHI KXHIGHMIA
kalshi-bot-cli historical-pipeline status --verbose
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

Repair stale historical replay rows when source selection or replay logic changes:

```bash
kalshi-bot-cli historical-repair audit \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI \
  --verbose

kalshi-bot-cli historical-repair refresh \
  --date-from 2026-03-01 \
  --date-to 2026-03-31 \
  --series KXHIGHNY KXHIGHCHI
```

Run historical intelligence and inspect the active heuristic state:

```bash
kalshi-bot-cli historical-intelligence status
kalshi-bot-cli historical-intelligence run --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-intelligence explain --series KXHIGHNY
kalshi-bot-cli heuristic-pack status
```

Current interpretation guidance:

- if `source_replay_coverage` is ahead of `replay_corpus`, a refresh is still needed
- if `replay_corpus.refresh_needed = false` but `directional_candidate_allowed = false`, the indicators are healthy but support is still too small for heuristic promotion
- if historical intelligence shows every row as `market_stale`, treat that as a replay-health bug and refresh problem, not as a real market-quality insight

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
