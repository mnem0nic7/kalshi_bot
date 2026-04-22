# Operations

## Blue/green model

- `app_blue` and `app_green` run simultaneously.
- Both can observe rooms and render the UI.
- Only the DB’s `active_color` may take the execution lock and place orders.

## Promotion flow

1. Deploy the inactive color.
2. Confirm it starts, reconnects to Postgres, and can run room workflows in shadow mode.
3. Enable the kill switch if you want a quiet handoff.
4. Run `infra/scripts/promote.sh green` or `blue`.
5. Verify the new color acquires the execution lock on its next trade attempt.
6. Disable the kill switch when satisfied.

## Migrations

For the Docker deployment flow:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
docker compose -f infra/docker-compose.yml build migrate
docker compose -f infra/docker-compose.yml run --rm --build --no-deps migrate
```

For local Python:

```bash
alembic upgrade head
```

Always migrate before live promotion.
Always migrate before enabling the watchdog timer on an already-running deployment, because the runtime now depends on the newer agent-pack tables and checkpoints.

Deploy finding from April 12, 2026:

- rebuilding `app_*` or `daemon_*` alone is not enough when a new Alembic revision has been added
- if the `migrate` image is stale, `run --rm --no-deps migrate` can report success while still stopping at the old head
- after any migration change, rebuild or run with `--build` on the `migrate` service before trusting the DB version

## CLI workflow

Typical operational loop:

```bash
kalshi-bot-cli init-db
kalshi-bot-cli discover
kalshi-bot-cli shadow-campaign run --limit 3
kalshi-bot-cli training-status
kalshi-bot-cli stream --max-messages 100
kalshi-bot-cli reconcile
kalshi-bot-cli status
```

By default, `discover` and `stream` now expand any configured `series_templates` from `docs/examples/weather_markets.example.yaml` into the currently active greater/less daily temperature contracts for those locations.

To let live market updates launch rooms automatically:

```bash
kalshi-bot-cli stream --auto-trigger
```

To run the full long-lived production worker:

```bash
kalshi-bot-cli daemon --auto-trigger
```

This mode:

- opens the Kalshi websocket stream
- runs periodic reconciliation
- emits heartbeat ops events
- auto-launches rooms when enabled and when this deployment color is active
- writes durable per-color daemon heartbeats and last-reconcile timestamps for watchdog recovery

The behavior is controlled by:

- `TRIGGER_ENABLE_AUTO_ROOMS`
- `TRIGGER_COOLDOWN_SECONDS`
- `TRIGGER_MAX_SPREAD_BPS`
- `TRIGGER_MAX_CONCURRENT_ROOMS`
- `DAEMON_RECONCILE_INTERVAL_SECONDS`
- `DAEMON_HEARTBEAT_INTERVAL_SECONDS`
- `DAEMON_START_WITH_RECONCILE`

The daemon also monitors staged agent-pack canaries and live-monitor windows. When a candidate pack is staged on the inactive color, the rollout is recorded first as `pending_pack_promotion:<env>:<color>`, then that color's daemon applies the new pack on startup before producing canary shadow rooms during heartbeats. If the canary sits longer than `SELF_IMPROVE_CANARY_MAX_SECONDS`, `self-improve status` and the control room mark it `stalled` instead of pretending rollout is still in progress.

For manual room execution:

```bash
room_id="$(kalshi-bot-cli create-room --name 'manual room' --market-ticker KXHIGHNY-26APR11-T68)"
kalshi-bot-cli run-room "$room_id"
```

For quick shadow transcript collection without creating the room separately:

```bash
kalshi-bot-cli shadow-run KXHIGHNY-26APR11-T68
kalshi-bot-cli shadow-sweep --limit 3
kalshi-bot-cli shadow-campaign run --limit 3
```

The control room index page now uses a top-tabbed dashboard layout with a summary strip at the top and lazy-loaded `Overview`, `Training & Historical`, `Research`, `Rooms`, and `Operations` tabs.
It still offers the same operator actions like `Run Shadow Room`, grouped dataset builds, kill-switch and color promotion controls, but the heavy historical and training views now stay out of the initial page load until their tab is opened.
The summary strip and bootstrap path now compute `Research Confidence` from cached research dossiers instead of live all-city discovery, and the `Room Outcomes` card uses lightweight room outcome snapshots instead of full room-bundle exports. `/` and `/api/control-room/summary` should stay fast even after expanding to every configured city and a larger 24-hour room history.
The `Research` tab now includes an `Assignment Review Queue` driven only by the latest stored 180d strategy snapshot. It groups cities into `ready_for_approval`, `drifted_assignment`, `evidence_weakened`, `aligned`, and `waiting_for_evidence`, and the city detail drilldown shows the current canonical assignment, the latest recommendation, and the latest approval note.
The top summary strip should be read as operator truth, not raw internals:

- `System Status` shows the actual operator state like `KILL SWITCH ON`, `HEALTHY`, or `DEGRADED`; active color is supporting context, not the headline
- `Active Deployment` foregrounds the active color, while watchdog freshness is shown as relative time
- the 30d win-rate stat uses realized contract outcomes by contract count: profitable exits count as wins immediately, and settlement labels are used only when no sell fill exists
- `Room Outcomes` uses resolved rooms only in the `succeeded/total` headline; running rooms are shown separately so in-flight work does not make the denominator misleading
- `Quality Debt` detail now breaks out `stale`, `missed`, and `weak` counts so the visible breakdown reconciles to the total

## Training corpus

Use the training-first commands to keep the weather corpus clean and reproducible:

```bash
kalshi-bot-cli training-status
kalshi-bot-cli research-audit --limit 20
kalshi-bot-cli training-build --mode room-bundles --good-research-only
kalshi-bot-cli training-build --mode role-sft --good-research-only
kalshi-bot-cli training-build --mode evaluation-holdout --settled-only
kalshi-bot-cli training-build-list
```

Historical replay and import loop:

Leave `--series` unset here to process every configured city template.

```bash
kalshi-bot-cli historical-status --verbose
kalshi-bot-cli historical-import weather --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-backfill market --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-backfill weather-archive --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-backfill forecast-archive --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-archive capture --once
kalshi-bot-cli historical-archive checkpoint-capture --once
kalshi-bot-cli historical-archive checkpoint-status --date-from 2026-03-01 --date-to 2026-03-31 --verbose
kalshi-bot-cli historical-backfill settlements --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-replay weather --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli training-build historical --mode bundles --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli training-build historical --mode gemini-finetune --date-from 2026-03-01 --date-to 2026-03-31 --output data/training/gemini_weather
```

Historical replay rooms are stored with `room_origin = historical_replay`. They reuse the room/export machinery, but the main control-room lists and live/shadow learning surfaces keep filtering to `shadow` and `live` by default so operator views stay focused.

Historical status now reports replayable market-days, exact weather checkpoint-archive coverage, exact market checkpoint-capture coverage, missing checkpoint reasons, settlement-backfill progress, and whether the Gemini export is only draft-ready or truly split-ready for training.
Historical status should be interpreted in layers:

- `source_replay_coverage` tells you what the current strict-asof sources can support
- `checkpoint_archive_coverage` tells you how much coverage came from scheduled checkpoint captures specifically
- `external_archive_coverage` tells you how much missing native checkpoint weather is recoverable or already recovered through archived Open-Meteo forecast runs
- `replay_corpus` tells you what has actually been materialized and is safe to use for readiness or intelligence

If `source_replay_coverage` is ahead of `replay_corpus`, run the historical repair refresh path before trusting the dashboard or the intelligence outputs.

Historical market snapshots with missing bid/ask are still stored when they are the best as-of evidence available. Treat those rows as expired-book audit coverage: replay rooms built from them should stand down rather than fabricate executable quotes.

Settlement status also has to be read more carefully now:

- `settlement_mismatch_breakdown.threshold_edge_strictness` means an exact-threshold strictness case and should usually disappear after the corrected crosscheck refresh
- `settlement_mismatch_breakdown.daily_summary_disagreement` means a real Kalshi vs NOAA/NCEI disagreement and should stay quarantined
- `settlement_mismatch_breakdown.crosscheck_missing` means no usable daily-summary crosscheck was available

Weather-archive backfill now has a second repair job besides writing raw archives: it promotes already-valid as-of weather bundles into checkpoint-archive records for the exact checkpoint slot when that evidence is recoverable without using future data.
Forecast-archive backfill adds a third strict-fidelity repair lane: it fetches archived Open-Meteo forecast runs, stores them as `external_forecast_archive_weather_bundle`, and promotes them into canonical checkpoint archives only when the run timestamp still satisfies checkpoint-time validity.
The Open-Meteo client now uses checkpoint-local cycle runs plus `forecast_days`; it does not send `start_date` or `end_date` when a specific archived run is requested.

Deploy findings from April 12, 2026:

- `historical-archive checkpoint-capture --once` now captures weather checkpoint archives and exact market checkpoint snapshots; `captured_checkpoint_count = 0` is still expected outside the due checkpoint windows and means nothing was due, not that the job failed
- `historical-backfill settlements` is now part of the operational repair path for closed markets with missing labels; the first live sweep backfilled labels immediately and materially reduced the `possible_ingestion_gap` backlog
- historical replay readiness is still constrained by missing checkpoint-weather coverage, so the right next action is continued checkpoint capture, not lowering training-readiness thresholds
- historical replay repair is also now a normal maintenance tool after replay-logic changes; source tables can be ahead of the materialized replay corpus until `historical-repair refresh` is run
- after the replay staleness fix, useful historical indicators should show real reasons like `spread_too_wide`, `resolved_contract`, or `book_effectively_broken` instead of collapsing into blanket `market_stale`
- settlement crosscheck semantics now honor strict `>` and `<` operators, so exact-threshold false mismatches should be refreshed away instead of treated as real data disagreements

Deploy findings from April 14, 2026:

- the historical repair lane now includes `historical-backfill forecast-archive`, which is the first strict external source for recovering checkpoint-time weather on older settled days
- `checkpoint_archive_coverage` remains the canonical readiness source, but it can now be native-capture-backed or external-archive-assisted; check `checkpoint_archive_coverage.source_counts` and `external_archive_recovery_summary` before assuming which path improved readiness
- `external_archive_coverage` is the right operator view for deciding whether a missing-weather backlog is recoverable this week or still genuinely unrecoverable
- `historical-backfill forecast-archive` now emits `reason_counts` and `failure_samples`, and `historical-status` mirrors the latest run under `external_archive_last_backfill` plus `external_archive_backfill_reason_counts`

Historical intelligence and heuristic-pack workflow:

```bash
kalshi-bot-cli historical-intelligence status
kalshi-bot-cli historical-intelligence run --date-from 2026-03-01 --date-to 2026-03-31
kalshi-bot-cli historical-intelligence explain --series KXHIGHNY
kalshi-bot-cli heuristic-pack status
```

Repair stale replay rows before trusting those indicators:

```bash
kalshi-bot-cli historical-repair audit --date-from 2026-03-01 --date-to 2026-03-31 --verbose
kalshi-bot-cli historical-repair refresh --date-from 2026-03-01 --date-to 2026-03-31
```

The self-improvement loop now respects corpus readiness gates. If the corpus is too small, too concentrated, or too weakly labeled, critique and evaluation commands will stop early instead of generating noisy candidates.

## Boot and self-healing

The self-healing runtime assumes the canonical deploy path is `/workspace/kalshi_bot`.

Boot assets:

- `infra/scripts/start-stack.sh`
- `infra/systemd/kalshi-bot-compose.service`

Watchdog assets:

- `infra/scripts/watchdog-run-once.sh`
- `infra/systemd/kalshi-bot-watchdog.service`
- `infra/systemd/kalshi-bot-watchdog.timer`

Recommended host enablement:

```bash
sudo cp infra/systemd/kalshi-bot-compose.service /etc/systemd/system/
sudo cp infra/systemd/kalshi-bot-watchdog.service /etc/systemd/system/
sudo cp infra/systemd/kalshi-bot-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-bot-compose.service
sudo systemctl enable --now kalshi-bot-watchdog.timer
```

Manual health and watchdog commands:

```bash
kalshi-bot-cli health-check app --color blue
kalshi-bot-cli health-check daemon --color blue
kalshi-bot-cli watchdog status
```

Recovery behavior is fixed:

- restart the inactive color if only that color is unhealthy
- restart the active color first if it is unhealthy
- fail over to the healthy inactive color if the active color remains unhealthy after the restart wait
- restart the full stack if both colors are unhealthy

Recovery actions are recorded in ops events and surfaced in `/api/status` plus the control room `Runtime Health` panel.

## Self-improvement loop

The Gemini-first runtime and versioned agent-pack system are documented in [self_improve.md](self_improve.md).

Typical operator flow:

```bash
kalshi-bot-cli self-improve status
kalshi-bot-cli self-improve critique --days 14 --limit 200
kalshi-bot-cli self-improve eval --candidate-version <VERSION> --days 14 --limit 200
kalshi-bot-cli self-improve promote --evaluation-run-id <EVALUATION_RUN_ID>
```

For Docker blue or green deployments, the helper scripts mirror the same flow:

```bash
infra/scripts/run-self-improve.sh status
infra/scripts/run-self-improve.sh critique --days 14 --limit 200
infra/scripts/restart-color.sh green
```

The GitHub Actions control-plane workflows are:

- `.github/workflows/self-improve.yml`
- `.github/workflows/rollback-agent-pack.yml`

`self-improve.yml` stages only the inactive color after a passing evaluation. The live color changes only after the canary finishes and the DB-backed rollout monitor promotes it.
The staged pack is now applied through a pending checkpoint that the restarted inactive daemon consumes on startup, which keeps rollout safe across watchdog restarts and blue/green failovers.
If the staged canary does not progress within `SELF_IMPROVE_CANARY_MAX_SECONDS`, the status surface marks it `stalled` and operators should inspect or roll it back instead of assuming rollout is still advancing.

For Docker deployments that need both environments available, `infra/docker-compose.yml` now mounts separate live and demo PEMs into the containers and relies on:

- `LIVE_KALSHI_KEY_PATH_HOST`
- `DEMO_KALSHI_KEY_PATH_HOST`
- `LIVE_KALSHI_API_KEY`
- `DEMO_KALSHI_API_KEY`

## Backups

- `infra/scripts/backup.sh` creates a compressed `pg_dump`.
- `infra/scripts/restore.sh` restores from a chosen dump file.
