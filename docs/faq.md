# FAQ

This is the operator-first explainer for the control room, training corpus, and safety rails. It is meant to answer “how does this all work?” without requiring a code dive first.

## What is this platform doing?

It runs a visible, room-based weather trading workflow for Kalshi. Every room captures research, agent messages, signal generation, risk checks, execution decisions, and audit context.

The current execution and training scope is structured weather markets only. Shadow and demo collection are the default path for improving the corpus before trusting self-improvement or live execution.

Read more: `docs/architecture.md`, `docs/operations.md`

## What does shadow mode mean?

Shadow mode lets the bot run the full room workflow without placing live orders. It still creates dossiers, scores markets, proposes trade tickets when allowed, and records why risk or eligibility would have blocked or allowed an action.

That gives us training data and operational confidence without turning on live risk.

Read more: `docs/operations.md`, `docs/training.md`

## What is a room?

A room is one decision trace for one market. It has a transcript, research snapshot, signal, trade ticket, risk verdict, execution state, memory note, and strategy audit.

Rooms are the main unit for operator review and for training exports.

Read more: `docs/agent_protocol.md`, `docs/training.md`

## What are the agent roles?

The researcher summarizes evidence, the president sets posture, the trader can propose a structured ticket, the risk officer explains deterministic gates, the execution clerk records exchange actions, the auditor links rationale, and the memory librarian distills the room.

Even with AI involved, deterministic code still owns risk limits, credential use, and final order submission.

Read more: `docs/agent_protocol.md`, `docs/strategy/weather-temp-taker.md`

## What is a research dossier?

A dossier is the shared market research record: structured weather facts, settlement coverage, source cards, claims, freshness, confidence, and a trader-facing fair-value context.

Rooms take a dossier snapshot and add a room-local delta so every decision is reproducible even if the shared dossier changes later.

Read more: `docs/training.md`, `docs/strategy/weather-temp-taker.md`

## What is a strategy audit?

A strategy audit is a post-hoc quality label for a room. It scores thesis correctness, trade quality, block correctness, stale-data mismatch, missed stand-downs, and whether the room should be trainable by default.

Historical rooms stay immutable; audits are supplemental labels used to clean the corpus.

Read more: `docs/training.md`, `docs/strategy/weather-microstructure-roadmap.md`

## Why do some rooms stand down early now?

The base weather strategy now treats resolved contracts, stale research, stale market data, wide spreads, and tiny remaining payout as non-actionable. Those rooms should stand down at eligibility instead of proposing a low-quality ticket and relying on later risk blocks.

That keeps the live strategy tighter and the training corpus cleaner.

Read more: `docs/strategy/weather-temp-taker.md`, `docs/strategy/weather-microstructure-roadmap.md`

## What makes a room trainable?

By default, rooms need to be complete, quality-cleaned, and free of stale-data mismatch or weak resolved-contract proposal labels. Good research helps, but the cleaned strategy audit is the final default filter.

You can still build raw legacy datasets when you want analysis instead of clean training slices.

Read more: `docs/training.md`

## How does historical replay training work?

Historical replay imports settled weather market-days, captured market snapshots, reconstructed checkpoint snapshots from Kalshi candlesticks, and point-in-time weather bundles from the bot’s archive plus captured raw events. The daemon now also writes dedicated checkpoint weather captures on schedule so future settled days can become fully replayable without depending on room traffic.

Those replayed rooms are marked `historical_replay`, kept out of live operator views by default, and exported into bundles, eval slices, or Gemini-first fine-tune files. If we do not have enough distinct full-coverage market-days yet, the Gemini export is marked draft-only instead of pretending it is ready to tune on.

The default operational mode is now a rolling one-year historical pipeline. `historical-pipeline bootstrap` builds the last `365` settled days ending yesterday, and `historical-pipeline daily` only processes newly settled or newly replayable days before rerunning rolling-year intelligence and confidence refresh.

## How do I tell whether the historical checks are healthy?

Read the historical status in three layers:

- `source_replay_coverage`: what the current strict-asof source tables could replay right now
- `checkpoint_archive_coverage`: what the scheduled checkpoint weather archive alone could support
- `replay_corpus`: what has actually been rebuilt into `historical_replay` rooms and is safe to use for readiness or intelligence

If source coverage is ahead of replay corpus materialization, run the historical repair refresh path before trusting the dashboard or the intelligence output.

Healthy indicators should also look plausible. After the replay-time staleness fix, the historical intelligence output should mostly surface real trade-quality reasons like `spread_too_wide`, `resolved_contract`, `book_effectively_broken`, or `insufficient_remaining_payout` instead of collapsing into blanket `market_stale`.

The confidence state should also make sense:

- `insufficient_support` means the rolling-year corpus is still too small for trustworthy heuristic changes
- `execution_confident_only` means execution-quality tuning has enough support, but directional rewrites do not
- `directional_confident` means both execution support and full-checkpoint directional support are finally strong enough to compare directional changes honestly

## What is settlement backfill?

Settlement backfill is the direct repair path for closed shadow/live room markets that still have no settlement label. Instead of waiting only for passive reconcile events, the bot can fetch the final market outcome directly from Kalshi, persist it as a labeled settlement, and clear rooms out of the `possible_ingestion_gap` backlog.

That makes the maturity dashboard more honest and helps move old rooms into outcome-aware training slices faster.

Read more: `docs/training.md`

## What did we learn from the April 12, 2026 deploy?

Three things mattered:

- New schema changes require a rebuilt `migrate` image. Rebuilding only the app or daemon containers can leave Alembic stuck on the previous head even though the new code is already live.
- `historical-archive checkpoint-capture --once` returning zero captures is normal when no checkpoint slot is currently due. That is a scheduling outcome, not a job failure.
- Settlement backfill is now a normal repair tool for maturity. The first live sweep immediately reduced the likely-ingestion-gap backlog, while historical Gemini readiness correctly stayed blocked because full checkpoint coverage still is not there yet.
- Historical replay repair is now part of normal maintenance after replay-logic changes. Derived replay rooms are safe to purge and rebuild; the raw historical sources stay immutable.
- Historical intelligence got much more honest once replay checks started using checkpoint-time staleness and refreshed replay rows. That is why the dashboard should now be read as real indicator quality, not just raw counts.

Read more: `docs/operations.md`, `docs/training.md`

## Why might self-improvement still be blocked?

Because readiness is gated by corpus volume and label quality, not just by plumbing. We need enough complete rooms, enough diversity, enough trade-positive examples, and enough settled rooms before critique, evaluation, or promotion should be trusted.

If settled coverage is low, the right move is usually to keep shadow collection and reconciliation running rather than lowering the threshold.

Read more: `docs/self_improve.md`, `docs/training.md`

## What should I watch on the dashboard?

The training panel shows room counts, cleaned-trainable share, recent exclusion memory, quality debt, unsettled backlog, settled-label velocity, and readiness blockers.

If the next bottleneck is not obvious from that panel, something is missing in the status surface and we should improve it.

Read more: `docs/training.md`, `docs/operations.md`

## How do blue/green deploys and the watchdog work?

Both colors run on the same host, but only the active color can own the execution path. The watchdog monitors app and daemon health, restarts unhealthy colors, and can fail over the active color if needed.

Boot, restart, and recovery stay host-native through systemd plus Docker Compose.

Read more: `docs/operations.md`, `docs/runbooks.md`

## What do kill switch and active color mean?

The kill switch blocks new live execution even if rooms continue to run. Active color is the stack currently allowed to own execution-related responsibilities.

Keeping the kill switch on is the safest default while we are still collecting and cleaning data.

Read more: `docs/operations.md`, `docs/security.md`

## Where do I inspect the system quickly?

Use the Control Room for runtime health, room creation, training status, audits, and research status. Use room pages for the detailed decision trace. Use the CLI for exact JSON and operational scripts when you need machine-readable state.

Useful commands include `training-status`, `research-audit`, `strategy-audit summary`, `shadow-campaign run`, `reconcile`, and `self-improve status`.

Read more: `README.md`, `docs/operations.md`
