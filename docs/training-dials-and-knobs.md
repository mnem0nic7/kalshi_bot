# Training Dials and Knobs

This is an operator map for the settings, CLI flags, YAML files, and runtime controls that change what the system learns from or how a trained/tuned artifact is accepted. In this repo, "training" covers four related surfaces:

- dataset creation for room bundles, role SFT, evaluation, and Gemini fine-tune exports
- historical replay and strict as-of weather corpus generation
- deterministic policy, gate, parameter-pack, heuristic, and strategy tuning
- weather, crypto, momentum, and trade-behavior model calibration

Most settings below are Pydantic settings from `src/kalshi_bot/config.py`. They can be set in `.env` using the uppercase field name unless a specific CLI flag or YAML path is called out.

## Change Surfaces

| Surface | Where | What it changes |
| --- | --- | --- |
| CLI flags | `src/kalshi_bot/cli.py` | One-off build windows, export modes, filters, dry-runs, scope, and output files. |
| Runtime settings | `.env`, `src/kalshi_bot/config.py` | Default windows, readiness thresholds, promotion gates, model enable flags, data collection cadence, and safety thresholds. |
| Parameter packs | `infra/config/parameter_pack_default.yaml`, DB champion/staged packs | Deterministic probabilistic and sizing parameters. |
| Hard caps | `infra/config/hard_caps.yaml` | Operator-owned safety ceilings. Autonomous tuning must not write these. |
| Agent packs | DB agent-pack records, active color | Runtime role policies, thresholds, and optional crypto/weather policy overrides. |
| Data files | `data/training/*.jsonl` | Offline bundle sources consumed by gate learning, modeling, parameter search, and fine-tuning. |
| Weather market map | `WEATHER_MARKET_MAP_PATH`, default `docs/examples/weather_markets.example.yaml` | Which city/series templates are discoverable and trainable. |

## Forward Room Dataset Knobs

These affect shadow/live room bundles and role SFT exports.

### `training-export`

| Knob | Default | Effect |
| --- | --- | --- |
| `--mode bundles|role-sft` | `bundles` | Chooses full decision records or role-specific supervised rows. |
| `--output` | required | JSONL destination. |
| `--room-id` | unset | Exports exactly one room when set. |
| `--market-ticker` | unset | Restricts rows to one market. |
| `--limit` | `100` | Caps exported rooms before role expansion. |
| `--include-non-complete` | `false` | Includes partial or failed workflows; useful for failure examples, risky for clean SFT. |
| `--roles` | all roles | Restricts role SFT rows to `researcher`, `president`, `trader`, and/or `memory_librarian`. |

### `training-build`

| Knob | Default | Effect |
| --- | --- | --- |
| `--mode room-bundles|role-sft|evaluation-holdout` | `room-bundles` | Chooses forward build format. |
| `--limit` | `200` | Maximum selected bundles. The loader initially fetches up to `max(limit * 5, limit, 50)` before filters. |
| `--days` | `30` | Lookback window for candidate rooms. |
| `--settled-only` | `false` | Requires settlement labels. Better for outcome learning, smaller corpus. |
| `--include-non-complete` | `false` | Keeps partial workflows. |
| `--good-research-only` | `false` | Requires `room_research_health.good_for_training`. |
| `--quality-cleaned-only`, `--no-quality-cleaned-only` | `true` | Excludes rooms whose strategy audit marks `trainable_default=false`. |
| `--market-ticker` | unset | Restricts to one market. |
| `--origins` | unset | Restricts room origins, commonly `shadow`, `live`, or `historical_replay`. |
| `--output` | unset | Writes JSONL and persists build metadata. |

Quality cleaning excludes rows with `stale_data_mismatch`, `missed_stand_down`, and weak resolved contract proposals. Turning it off is useful for pathology analysis, not for clean training.

### Forward Readiness Settings

| Setting | Default | Effect |
| --- | --- | --- |
| `TRAINING_WINDOW_DAYS` | `30` | Status window for readiness and quality-debt dashboards. |
| `TRAINING_STATUS_ROOM_LIMIT` | `500` | Max rooms read by `training-status`. |
| `TRAINING_MIN_COMPLETE_ROOMS` | `25` | Minimum cleaned complete rooms for SFT readiness. |
| `TRAINING_MIN_MARKET_DIVERSITY` | `4` | Minimum distinct markets for SFT readiness. |
| `TRAINING_MIN_SETTLED_ROOMS` | `10` | Minimum settled rooms for evaluation/promotion readiness. |
| `TRAINING_MIN_TRADE_POSITIVE_ROOMS` | `8` | Minimum rooms that generated a trade ticket for critique/evaluation readiness. |
| `TRAINING_GOOD_RESEARCH_THRESHOLD` | `0.7` | Research health scoring threshold used by training health paths. |
| `SELF_IMPROVE_HOLDOUT_RATIO` | `0.2` | Holdout share for self-improvement/evaluation slices. |

Readiness also has hard-coded distribution warnings: too many no-trade examples, too many rooms from one market, or too many rooms from one city/strike regime.

## Data Collection Knobs

These controls affect what future training rows exist.

| Knob | Default | Effect |
| --- | --- | --- |
| `kalshi-bot-cli shadow-campaign run --limit` | `3` | Number of shadow rooms to create in a campaign. |
| `shadow-campaign run --domain` | `weather` | Domain for campaign rooms. |
| `shadow-sweep --markets` | unset | Explicit market list for shadow room generation. |
| `shadow-sweep --limit` | unset | Caps sweep-generated rooms. |
| `run-room ROOM_ID --reason` | `cli_run` | Re-runs a specific room and tags provenance. |
| `TRAINING_CAMPAIGN_ENABLED` | `false` | Enables automated training campaign collection. |
| `TRAINING_CAMPAIGN_ROOMS_PER_RUN` | `3` | Campaign room count per run. |
| `TRAINING_CAMPAIGN_LOOKBACK_HOURS` | `24` | Recent-room memory window used to avoid over-collecting the same market. |
| `TRAINING_CAMPAIGN_COOLDOWN_SECONDS` | `600` | Campaign cooldown. |
| `TRAINING_CAMPAIGN_MAX_RECENT_PER_MARKET` | `5` | Recent per-market cap. |
| `TRIGGER_ENABLE_AUTO_ROOMS` | `false` | Enables automatic rooms from trigger logic. |
| `TRIGGER_COOLDOWN_SECONDS` | `300` | Trigger cooldown. |
| `TRIGGER_PRICE_MOVE_BYPASS_BPS` | `1500` | Price move that bypasses normal trigger cooldown. |
| `TRIGGER_MAX_SPREAD_BPS` | `250` | Marketability gate for triggered rooms. |
| `TRIGGER_MAX_CONCURRENT_ROOMS` | `12` | Active room concurrency cap. |
| `TRIGGER_ACTIVE_ROOM_STALE_SECONDS` | `1800` | Stale active room age. |
| `TRIGGER_MARKETABILITY_RECHECK_SECONDS` | `60` | Waitlist recheck cadence. |
| `TRIGGER_MARKETABILITY_WAITLIST_TTL_SECONDS` | `1800` | Waitlist TTL. |
| `TRIGGER_MARKETABILITY_RECHECK_LIMIT` | `50` | Max rechecks per pass. |

## Agent and LLM Knobs

These change generated transcripts and therefore future SFT rows. Keep pack/model changes visible in build metadata.

| Setting | Default | Effect |
| --- | --- | --- |
| `LLM_CALLS_ENABLED` | `false` | Enables LLM agents instead of deterministic fallbacks. |
| `ACTIVE_AGENT_PACK_VERSION` | `builtin-deterministic-v1` | Default active agent-pack version. |
| `LLM_TRADING_ENABLED` | `false` | Allows LLM-backed trading paths. Leave off unless explicitly validating that path. |
| `LLM_HOSTED_BASE_URL` | `https://api.openai.com/v1` | Hosted OpenAI-compatible endpoint. |
| `LLM_HOSTED_MODEL` | `gpt-4o` | Hosted fallback model. |
| `LLM_LOCAL_BASE_URL` | `http://localhost:11434/v1` | Local OpenAI-compatible endpoint. |
| `LLM_LOCAL_MODEL` | `llama3.1:8b` | Local model. |
| `GEMINI_BASE_URL` | `https://generativelanguage.googleapis.com/v1beta` | Gemini endpoint. |
| `GEMINI_MODEL_RESEARCHER` | `gemini-2.5-flash` | Researcher role model. |
| `GEMINI_MODEL_PRESIDENT` | `gemini-2.5-pro` | President role model. |
| `GEMINI_MODEL_TRADER` | `gemini-2.5-pro` | Trader role model. |
| `GEMINI_MODEL_RISK_OFFICER` | `gemini-2.5-flash` | Risk officer role model. |
| `GEMINI_MODEL_OPS_MONITOR` | `gemini-2.5-flash` | Ops monitor role model. |
| `GEMINI_MODEL_MEMORY_LIBRARIAN` | `gemini-2.5-flash` | Memory librarian role model. |
| `LLM_REQUEST_TIMEOUT_SECONDS` | `30.0` | LLM request timeout. |

Provider runtime currently uses role temperature `0.2` in code. Changing that requires code or agent-provider changes, not a setting.

## Historical Replay and Fine-Tune Knobs

Historical replay builds strict as-of weather rows. It is the preferred source for outcome and policy learning because it avoids final-outcome leakage.

### Historical Source Commands

| Command knob | Effect |
| --- | --- |
| `historical-import weather --date-from --date-to --series` | Imports market definitions, settlement labels, captured market snapshots, captured weather snapshots, and file-backed weather archives. |
| `historical-replay weather --date-from --date-to --series` | Replays fixed checkpoints into room-shaped records. |
| `historical-backfill market --date-from --date-to --series` | Backfills market checkpoints. |
| `historical-backfill weather-archive --date-from --date-to --series --import-only` | Backfills archived weather bundles. |
| `historical-backfill forecast-archive --date-from --date-to --series` | Uses Open-Meteo point-in-time runs to recover checkpoint weather. |
| `historical-backfill settlements --date-from --date-to --series` | Refreshes settlement labels. |
| `historical-archive capture --once --series` | Captures raw weather archive once. |
| `historical-archive checkpoint-capture --once --series` | Captures checkpoint-quality archive once. |
| `historical-archive checkpoint-status --date-from --date-to --series --verbose` | Inspects checkpoint archive coverage. |
| `historical-repair audit --date-from --date-to --series --verbose` | Finds stale replay rows after replay logic/source changes. |
| `historical-repair refresh --date-from --date-to --series` | Rebuilds stale historical replay rooms. |
| `historical-pipeline bootstrap --days --chunk-days --series` | Runs rolling bootstrap in resumable chunks. |
| `historical-pipeline resume --series` | Continues persisted bootstrap cursor. |
| `historical-pipeline daily --series` | Daily incremental import/backfill/replay/build loop. |

Replay checkpoints are fixed in code at `09:00`, `13:00`, and `17:00` local market time, plus `1 hour before close` if earlier. Changing checkpoint times requires code.

### Historical Build Knobs

| Knob | Default | Effect |
| --- | --- | --- |
| `training-build historical --mode` | must pass `bundles`, `role-sft`, `decision-eval`, `outcome-eval`, or `gemini-finetune` | Chooses historical output format. |
| `--date-from`, `--date-to` | required | Historical market-day selection window. |
| `--series` | all configured series | Restricts cities/series. |
| `--limit` | `200` from parser | Max replay rooms selected. |
| `--quality-cleaned-only`, `--no-quality-cleaned-only` | `true` | Applies strategy-audit trainability filter. |
| `--include-pathology-examples` | `false` | Keeps settlement crosscheck mismatches and other pathology rows. |
| `--require-full-checkpoints`, `--no-require-full-checkpoints` | `true` | For `role-sft`, `decision-eval`, and `gemini-finetune`, requires full checkpoint coverage. |
| `--late-only-ok` | `false` | Allows late-only coverage for `outcome-eval`. |
| `--origins` | `historical_replay` | Restricts source room origins. |
| `--output` | unset | JSONL path, or directory for `gemini-finetune`. |

Historical Gemini exports split by local market day, never by checkpoint: one day is all train, two days are train plus holdout, three days are train/validation/holdout, and four or more days use about 70 percent train, 15 percent validation, and 15 percent holdout. Fewer than three local market days, no validation day, or no holdout day marks the manifest `draft_only`.

### Historical Settings

| Setting | Default | Effect |
| --- | --- | --- |
| `HISTORICAL_IMPORT_PAGE_SIZE` | `500` | Kalshi import page size. |
| `HISTORICAL_IMPORT_MAX_PAGES` | `25` | Max import pages. |
| `HISTORICAL_REPLAY_MARKET_SNAPSHOT_LOOKBACK_HOURS` | `36` | Search window for replay market snapshots. |
| `HISTORICAL_REPLAY_MARKET_STALE_SECONDS` | `900` | Staleness threshold for replay market evidence. |
| `HISTORICAL_WEATHER_ARCHIVE_PATH` | `data/historical_weather` | File-backed weather archive root. |
| `HISTORICAL_FORECAST_ARCHIVE_PROVIDER_ENABLED` | `true` | Enables external forecast archive recovery. |
| `HISTORICAL_FORECAST_ARCHIVE_BASE_URL` | `https://single-runs-api.open-meteo.com/v1/forecast` | External forecast archive endpoint. |
| `HISTORICAL_FORECAST_ARCHIVE_API_KEY` | unset | External archive API key. |
| `HISTORICAL_FORECAST_ARCHIVE_MODEL_PREFERENCE` | `gfs_seamless` | Preferred Open-Meteo historical forecast model. |
| `HISTORICAL_FORECAST_ARCHIVE_TIMEOUT_SECONDS` | `30.0` | Archive request timeout. |
| `HISTORICAL_FORECAST_ARCHIVE_MAX_RETRIES` | `2` | Archive retry count. |
| `HISTORICAL_CHECKPOINT_CAPTURE_LEAD_SECONDS` | `300` | How early checkpoint capture can fire. |
| `HISTORICAL_CHECKPOINT_CAPTURE_GRACE_SECONDS` | `900` | How late checkpoint capture can still count. |
| `HISTORICAL_PIPELINE_BOOTSTRAP_DAYS` | `365` | Default rolling bootstrap horizon. |
| `HISTORICAL_PIPELINE_CHUNK_DAYS` | `14` | Bootstrap chunk size. |
| `HISTORICAL_PIPELINE_DAILY_RUN_SECONDS` | `86400` | Daily historical pipeline cadence. |
| `HISTORICAL_PIPELINE_INCREMENTAL_DAYS` | `7` | Incremental daily lookback. |

### Historical Confidence Knobs

| Setting | Default | Effect |
| --- | --- | --- |
| `HISTORICAL_EXECUTION_CONFIDENCE_MIN_MARKET_DAYS` | `60` | Distinct execution-usable days needed for execution-confidence. |
| `HISTORICAL_DIRECTIONAL_CONFIDENCE_MIN_FULL_MARKET_DAYS` | `30` | Full-checkpoint days needed for directional confidence. |
| `HISTORICAL_DIRECTIONAL_CONFIDENCE_MIN_HOLDOUT_MARKET_DAYS` | `7` | Full-checkpoint holdout days needed for directional confidence. |
| `HISTORICAL_INTELLIGENCE_WINDOW_DAYS` | `365` | Historical intelligence analysis window. |
| `HISTORICAL_INTELLIGENCE_MIN_FULL_MARKET_DAYS` | `3` | Minimum full days for heuristic intelligence. |
| `HISTORICAL_INTELLIGENCE_MIN_SEGMENT_SUPPORT` | `5` | Minimum segment support. |
| `HISTORICAL_INTELLIGENCE_MIN_COMPOSITE_IMPROVEMENT` | `0.02` | Required improvement for heuristic candidate. |
| `HISTORICAL_INTELLIGENCE_AUTO_PROMOTE` | `true` | Allows automatic heuristic pack promotion when gates pass. |
| `ACTIVE_HEURISTIC_PACK_VERSION` | `historical-baseline-v1` | Runtime heuristic pack fallback. |

## Weather Model and Calibration Knobs

### Weather Prediction and Source Fusion

| Setting | Default | Effect |
| --- | --- | --- |
| `WEATHER_MARKET_MAP_PATH` | `docs/examples/weather_markets.example.yaml` | Determines supported weather templates/cities. |
| `WEATHER_PREDICTION_ENABLED` | `false` | Enables weather prediction service consumers. |
| `WEATHER_SOURCE_ENSEMBLE_ENABLED` | `true` | Uses ensemble/fuser logic. |
| `WEATHER_SOURCE_DISAGREEMENT_WIDEN_F` | `3.0` | Disagreement level that widens uncertainty. |
| `WEATHER_SOURCE_DISAGREEMENT_STAND_DOWN_F` | `8.0` | Disagreement level that stands down. |
| `WEATHER_SOURCE_DISAGREEMENT_SIGMA_MULTIPLIER_MAX` | `2.0` | Max sigma widening multiplier. |
| `WEATHER_NOWCAST_HIGH_SO_FAR_ENABLED` | `true` | Includes observed high-so-far feature. |
| `WEATHER_REQUEST_TIMEOUT_SECONDS` | `30.0` | Weather request timeout. |
| `WEATHER_RETRY_ATTEMPTS` | `3` | Weather request retries. |
| `WEATHER_RETRY_BASE_DELAY_SECONDS` | `0.25` | Weather retry delay base. |

Commands:

- `weather-prediction evaluate --series`
- `weather-prediction station-diagnostics --min-days`

### Sigma Calibration

| Knob | Default | Effect |
| --- | --- | --- |
| `weather-sigma refit --version` | generated | Version label for refit output. |
| `weather-sigma refit --dry-run` | `false` | Evaluate without writing. |
| `SIGMA_CALIBRATION_ENABLED` | `true` | Enables station sigma use. |
| `SIGMA_MIN_SAMPLES_BEATS_GLOBAL` | `100` | Minimum samples to beat global fallback. |
| `SIGMA_MIN_SAMPLES_BEATS_YAML` | `200` | Minimum samples to beat YAML/static fallback. |
| `SIGMA_MIN_CRPS_IMPROVEMENT` | `0.0` | Required CRPS improvement. |
| `SIGMA_LEAD_CORRECTION_ENABLED` | `true` | Enables lead-time correction factors. |

### Residual Model

| Knob | Default | Effect |
| --- | --- | --- |
| `weather-residual train --kalshi-env --dry-run` | `demo`, false | Fits residual model or evaluates only. |
| `weather-residual evaluate --kalshi-env` | `demo` | Dry-run evaluation path. |
| `WEATHER_RESIDUAL_MODEL_ENABLED` | `false` | Enables residual model use at runtime. |
| `WEATHER_RESIDUAL_MIN_MAE_IMPROVEMENT_PCT` | `0.02` | Required MAE improvement. |
| `WEATHER_RESIDUAL_MIN_CRPS_IMPROVEMENT_PCT` | `0.0` | Required CRPS improvement. |
| `WEATHER_RESIDUAL_MIN_BRIER_IMPROVEMENT_PCT` | `0.0` | Required Brier improvement. |
| `WEATHER_RESIDUAL_MODEL_MAX_AGE_HOURS` | `168` | Runtime artifact staleness limit. |

### Intraday Model

| Knob | Default | Effect |
| --- | --- | --- |
| `weather-intraday train --kalshi-env --series` | `demo`, all series | Fits intraday logistic model and probability calibration (Platt or isotonic, by sample size). |
| `weather-intraday evaluate --kalshi-env --series` | `demo`, all series | Dry-run evaluation. |
| `WEATHER_INTRADAY_ISOTONIC_MIN_ROWS` | `1000` | Below this many calibration rows, fit Platt (sigmoid) instead of isotonic — isotonic overfits on scarce data (Niculescu-Mizil & Caruana, ICML'05). At/above it, isotonic. Measured A/B on real data (480 calib rows): Platt holdout Brier 0.0832 / +20.0% vs isotonic 0.0883 / +15.2%. |
| `WEATHER_INTRADAY_CALIBRATION_METHOD` | `auto` | `auto` (size-based per the row above) or force `platt` \| `isotonic` \| `venn_abers`. Venn-Abers (arXiv 2502.05676) gives distribution-free finite-sample calibration. Real-data A/B/C (same splits): Platt Brier **0.0832** / MCB 0.0062; Venn-Abers Brier 0.0842 / **MCB 0.0047** (best-calibrated); isotonic 0.0883 / MCB 0.0080. **Default stays `auto`/Platt — best Brier; Venn-Abers is the lowest-miscalibration option when calibration matters more than sharpness.** |
| `intraday metrics → score_decomposition` | — | Each intraday eval now reports the CORP Brier decomposition `S = MCB − DSC + UNC` (miscalibration / discrimination / uncertainty) alongside Brier, separating "fixable by calibration" from "no signal". |
| `WEATHER_INTRADAY_MODEL_ENABLED` | `false` | Enables intraday model at runtime. |
| `WEATHER_INTRADAY_MODEL_MAX_AGE_HOURS` | `168` | Runtime artifact staleness limit. |
| `WEATHER_INTRADAY_MIN_TRAIN_ROWS` | `500` | Minimum rows before fitting. |
| `WEATHER_INTRADAY_MIN_HOLDOUT_ROWS` | `100` | Minimum holdout rows. |
| `WEATHER_INTRADAY_MIN_BRIER_IMPROVEMENT_PCT` | `0.01` | Required holdout Brier improvement. |
| `WEATHER_INTRADAY_MAX_CALIBRATION_ERROR` | `0.20` | Max checked calibration bucket error. |
| `WEATHER_INTRADAY_MIN_CALIBRATION_BUCKET_ROWS` | `50` | Buckets below this are ignored for calibration-error gate. |
| `WEATHER_INTRADAY_MIN_SERIES_HOLDOUT_ROWS` | `30` | Per-series holdout support threshold. |
| `WEATHER_INTRADAY_MAX_SERIES_BRIER_REGRESSION` | `0.05` | Max allowed per-series Brier regression. |

The train/holdout split is time ordered by local market day. Fit/calibration rows are also split chronologically inside the training period.

## Momentum Calibration Knobs

Momentum calibration writes an active checkpoint named `momentum_calibration:{kalshi_env}` and falls back per field to settings when no checkpoint exists.

| Knob | Default | Effect |
| --- | --- | --- |
| `calibrate-momentum backfill-slopes --date-from --date-to` | required | Fetches 60-minute candlestick slopes into signal payloads. |
| `calibrate-momentum preview --date-from --date-to --output` | required dates | Read-only fit and bucket analysis. |
| `calibrate-momentum stage --date-from --date-to --min-observations --staged-by --force --output` | `1000` min observations | Writes pending calibration after sanity checks. |
| `calibrate-momentum promote --activated-by` | current user | Promotes pending calibration to active. |
| `calibrate-momentum reject` | n/a | Clears pending calibration. |
| `MOMENTUM_WEIGHT_SCALE_CENTS_PER_MIN` | `1.0` | Slope scale for momentum weighting. |
| `MOMENTUM_SLOPE_VETO_CENTS_PER_MIN` | unset | Optional slope veto threshold. |
| `MOMENTUM_WEIGHT_FLOOR` | `0.3` | Minimum momentum weight. |
| `MOMENTUM_VETO_STALENESS_GATE` | `0.5` | Veto staleness gate. |
| `MOMENTUM_WEIGHT_SHADOW_MODE` | `true` | Keeps momentum weight changes shadowed. |
| `MOMENTUM_CALIBRATION_AUTO_ENABLED` | `false` | Enables nightly auto calibration. |
| `MOMENTUM_CALIBRATION_NIGHTLY_HOUR_LOCAL` | `2` | Nightly run hour. |
| `MOMENTUM_CALIBRATION_NIGHTLY_TIMEZONE` | `America/Los_Angeles` | Nightly run timezone. |
| `MOMENTUM_CALIBRATION_NIGHTLY_LOOKBACK_DAYS` | `90` | Auto calibration lookback. |
| `MOMENTUM_CALIBRATION_MIN_SLOPE_COVERAGE` | `0.80` | Required slope coverage. |
| `MOMENTUM_CALIBRATION_RECENT_COVERAGE_DAYS` | `7` | Recent coverage window. |
| `MOMENTUM_CALIBRATION_MIN_OBSERVATIONS` | `1000` | Minimum usable observations. |
| `MOMENTUM_CALIBRATION_TIER1_MAX_DELTA_FRACTION` | `0.10` | Tier 1 max parameter delta. |
| `MOMENTUM_CALIBRATION_TIER2_MAX_DELTA_FRACTION` | `0.20` | Tier 2 max parameter delta before tier 3. |
| `MOMENTUM_CALIBRATION_TIER1_MAX_CI_WIDTH_FRACTION` | `0.30` | Tier 1 CI-width gate. |
| `MOMENTUM_CALIBRATION_SANITY_MAX_CI_WIDTH_FRACTION` | `0.50` | Stage sanity CI-width gate. |
| `MOMENTUM_CALIBRATION_TIER1_AUTO_PROMOTE_ENABLED` | `false` | Allows tier 1 auto promotion. |
| `MOMENTUM_CALIBRATION_SKIP_CRITICAL_THRESHOLD` | `4` | Critical skip threshold for ops events. |

Stage also enforces code-level sanity bounds on scale: `0.1 <= scale <= 10.0`.

## Gate Learning and Autonomous Gate Tuning

Gate learning reads historical and forward-shadow bundles, labels blocked/pass decisions, and recommends threshold changes. Autonomous gate tuning can stage those recommendations into runtime policy.

### Gate Learning

| Knob | Default | Effect |
| --- | --- | --- |
| `gate-learning report|recommend --kalshi-env` | `production` | Environment scope. |
| `--days` | `180` | Decision window. |
| `--source historical|forward-shadow|combined` | `combined` | Bundle source. |
| `--min-support` | `GATE_LEARNING_MIN_SUPPORT` | Minimum support per recommendation. |
| `--policy-scope global|cohort|city` | `global` | Recommendation scope. |
| `--series-ticker` | unset | Required for city-specific focus. |
| `--side yes|no` | unset | Side-specific focus. |
| `--month` | unset | Month/cohort focus. |
| `--lane` | `entry_gate` | Policy lane key. |
| `--episode-level` | `false` | Aggregates at episode level. |
| `GATE_LEARNING_MIN_SUPPORT` | `30` | Default minimum support. |

Default file inputs are `data/training/historical_outcome_eval_latest.jsonl`, `data/training/historical_decision_eval_latest.jsonl`, `data/training/forward_shadow_uncurated_bundles.jsonl`, and `data/training/forward_shadow_bundles.jsonl`.

### Autonomous Gate Tuning

| Knob | Default | Effect |
| --- | --- | --- |
| `autonomous-gates run --kalshi-env` | `production` | Target environment. |
| `--source historical|forward-shadow|combined` | setting default | Evidence source. |
| `--days` | setting default | Evidence window. |
| `--min-support` | setting default | Required support. |
| `--dry-run` | `false` | Produces recommendation without staging. |
| `--domain weather|crypto|all` | `all` | Domain scope. |
| `--scope global|cohort|city` | `global` | Weather policy scope. |
| `--series-ticker`, `--side`, `--month`, `--lane` | unset / `entry_gate` | Weather policy key dimensions. |
| `--crypto-assets` | unset | Crypto asset scope. |
| `--bootstrap-promote-from-historical` | `false` | Allows historical bootstrap promotion path. |
| `AUTONOMOUS_GATE_TUNING_ENABLED` | `true` | Enables daemon/reconcile tuning runs. |
| `AUTONOMOUS_GATE_TUNING_SOURCE` | `combined` | Default evidence source. |
| `AUTONOMOUS_GATE_TUNING_DAYS` | `3650` | Default evidence window. |
| `AUTONOMOUS_GATE_TUNING_MIN_SUPPORT` | `30` | Default support threshold. |
| `AUTONOMOUS_GATE_TUNING_CANARY_MIN_SETTLED_ROWS` | `10` | Canary support threshold. |
| `AUTONOMOUS_GATE_TUNING_CANARY_MAX_WAIT_HOURS` | `72` | Canary expiry window. |
| `AUTONOMOUS_GATE_TUNING_PERIODIC_INTERVAL_SECONDS` | `3600` | Daemon cadence. |

The weather threshold fields autonomous tuning is allowed to adjust are:

- `risk_min_contract_price_dollars`
- `strategy_min_remaining_payout_bps`
- `trigger_max_spread_bps`
- `risk_min_confidence`
- `risk_min_edge_bps`
- `strategy_min_abs_delta_f`
- `risk_max_credible_edge_bps`

## Parameter Pack Knobs

Parameter packs are deterministic, bounded, hashable bundles of model and sizing parameters. The default file is `infra/config/parameter_pack_default.yaml`.

| Parameter | Default | Bounds | Effect |
| --- | --- | --- | --- |
| `pseudo_count` | `8` | `2..32` | Climatology shrinkage pseudo-count. |
| `gumbel_weight` | `0.5` | `0.0..1.0` | Closed-form Gumbel blend weight. |
| `kde_weight` | `0.5` | `0.0..1.0` | Ensemble KDE blend weight. |
| `boundary_threshold` | `0.25` | `0.05..0.75` | Boundary mass threshold. |
| `disagreement_threshold` | `0.85` | `0.10..1.0` | Source disagreement threshold. |
| `base_min_ev` | `0.02` | `0.0..0.20` | Base minimum EV dollars per contract. |
| `uncertainty_min_ev_buffer` | `0.02` | `0.0..0.20` | Additional EV buffer at maximum uncertainty. |
| `uncertainty_size_taper` | `0.60` | `0.0..1.0` | Size taper under uncertainty. |
| `kelly_fraction` | `0.25` | `0.01..0.50` | Fractional Kelly multiplier. |
| `survival_kelly` | `0.10` | `0.01..0.25` | Survival-mode Kelly multiplier. |
| `survival_ev_buffer` | `0.03` | `0.0..0.20` | Survival-mode EV buffer. |
| `health_degraded_size_mult` | `0.5` | `0.0..1.0` | Size multiplier when source health is degraded. |
| `catboost_blend_weight` | `0.0` | `0.0..0.5` | Optional learned-head probability blend weight. |

Commands:

| Command | Important knobs |
| --- | --- |
| `parameter-pack default --path` | Reads default or custom pack. |
| `parameter-pack hard-caps --path` | Reads sealed hard caps. |
| `parameter-pack validate PATH --strict` | Sanitizes candidate; strict fails if hard-cap fields are present. |
| `parameter-pack grid --grid --limit` | Generates bounded candidates from a grid JSON. |
| `parameter-pack gate --candidate-report --current-report --hard-caps` | Applies promotion gates. |
| `parameter-pack select --candidates --current-report --hard-caps --starvation-tolerance` | Picks first passing replay-gated candidate. |
| `parameter-pack drift --window` | Evaluates calibration drift and can pause new entries. |
| `parameter-pack learned-gate --closed-form-report --learned-report --requested-weight` | Allows nonzero learned-head blend only if holdout improves. |
| `parameter-pack nws-parser-gate --window --requested-feature-weight` | Allows NWS parser feature weight only with parser health evidence. |
| `parameter-pack stage --candidate-pack --candidate-report --current-report --hard-caps --target-color --reason` | Stages a gated pack on inactive color. |
| `parameter-pack canary --report --min-shadow-rooms --min-elapsed-seconds --max-brier-ratio` | Evaluates staged-pack canary evidence. |
| `parameter-pack promote-staged --reason` | Marks canary-passed staged pack champion. |
| `parameter-pack rollback-staged --reason` | Rolls staged candidate back. |

Promotion gates in code default to:

- coverage at least `0.95`
- resolved trades at least `30`
- candidate Brier no more than `1.02x` current Brier
- ECE no more than `0.06`
- Sharpe at least `0.95x` positive current Sharpe
- max drawdown no more than `1.10x` current drawdown and no more than hard-cap `max_drawdown_pct`
- city win-rate drop no more than `0.10`
- zero hard-cap touches
- deterministic pack hash must match rerun hash

Learned-head gate requires lower Brier, lower ECE, Sharpe improvement of at least `0.05`, no invalid probabilities, and clamps requested blend weight to `0.0..0.5`.

NWS parser gate requires parser availability at least `0.95`, schema failure rate no more than `0.01`, and at least one parser attempt.

## Hard-Cap Knobs

Hard caps are operator-owned and should not be tuned by autonomous parameter packs. Default file: `infra/config/hard_caps.yaml`.

| Hard cap | Default | Effect |
| --- | --- | --- |
| `max_position_pct` | `0.10` | Max position as account fraction. |
| `max_total_exposure_pct` | `0.25` | Max total exposure fraction. |
| `daily_max_loss_pct` | `0.02` | Daily max loss fraction for hard-cap evaluation. |
| `max_drawdown_pct` | `0.05` | Hard max drawdown used by promotion gates. |
| `max_position_usd` | `null` | Optional absolute position cap. |
| `max_order_count_fp` | `200.0` | Max fixed-point order count. |
| `max_position_count_fp_per_ticker` | `200.0` | Max fixed-point position count per ticker. |
| `strategy_c_max_order_notional_usd` | `50.0` | Strategy C order notional cap. |
| `strategy_c_max_position_notional_usd` | `50.0` | Strategy C position notional cap. |

## Risk and Selection Thresholds That Shape Training Labels

These are runtime thresholds, not training-only settings. Changing them changes which future rooms produce tickets, blocks, and PnL labels.

| Setting | Default | Effect |
| --- | --- | --- |
| `RISK_MIN_EDGE_BPS` | `500` | Minimum edge for entries. |
| `RISK_FEE_AWARE_EDGE_ENABLED` | `true` | Uses fee-aware edge gates. |
| `RISK_MAX_CREDIBLE_EDGE_BPS` | `10000` | Blocks implausibly large edge. |
| `RISK_MIN_CONFIDENCE` | `0.80` | Minimum signal confidence. |
| `RISK_MIN_CONTRACT_PRICE_DOLLARS` | `0.50` | Minimum entry price. |
| `RISK_MIN_PROBABILITY_EXTREMITY_PCT` | `25.0` | Probability extremity gate. |
| `RISK_PROBABILITY_MIDBAND_MAX_EXTRA_EDGE_BPS` | `500` | Extra edge requirement near midband. |
| `RISK_STALE_MARKET_SECONDS` | `60` | Market data staleness gate. |
| `RISK_STALE_WEATHER_SECONDS` | `900` | Weather data staleness gate. |
| `STRATEGY_MIN_ABS_DELTA_F` | `8.0` | Minimum forecast/threshold separation. |
| `STRATEGY_MIN_REMAINING_PAYOUT_BPS` | `2000` | Minimum remaining payout. |
| `STRATEGY_QUALITY_EDGE_BUFFER_BPS` | `25` | Edge buffer subtracted for quality adjustment. |
| `KALSHI_TAKER_FEE_RATE` | `0.07` | Fee estimate used in simulations and gates. |
| `RESEARCH_STALE_SECONDS` | `900` | Dossier staleness threshold. |
| `RESEARCH_STALE_GRACE_FACTOR` | `2.0` | Grace multiplier with reduced size. |
| `RESEARCH_STALE_TOLERANCE_NOTIONAL_FACTOR` | `0.5` | Size multiplier in stale-tolerance mode. |

If you change these, build metadata and decision traces should be treated as a new policy regime.

## Modeling, Backtesting, and Trade-Behavior Knobs

### Modeling

| Knob | Default | Effect |
| --- | --- | --- |
| `modeling status|backtest|validate|train-shadow --kalshi-env` | `demo` | Environment. |
| `--days` | `180` | Lookback window. |
| `--full-history` | `false` | Uses `3650` days. |
| `--limit` | `20` | Diagnostics/bucket cap; `0` means unbounded dataset pass for most sources. |
| `--source trade-analysis|gate-learning-bundles` | `trade-analysis` | Dataset source. |

Code-level modeling constants are `MIN_MODELING_ROWS=20` and `TRAIN_FRACTION=0.7`. Changing those requires code.

### Backtesting

| Knob | Default | Effect |
| --- | --- | --- |
| `backtesting status|run|validate --kalshi-env` | `demo` | Environment. |
| `--days` | `180` | Lookback window. |
| `--full-history` | `false` | Uses `3650` days. |
| `--limit` | `20` | Diagnostics/bucket cap; `0` means unbounded dataset pass for most sources. |
| `--dataset-source auto|trade-analysis|decision-corpus|gate-learning-bundles` | `auto` | Backtest dataset source. |
| `--output` | unset | Writes report JSON. |

Backtesting walk-forward constants are `DEFAULT_MIN_TRAIN_ROWS=20` and `DEFAULT_MIN_TEST_ROWS=1`. Changing those requires code.

### Trade Behavior

| Knob | Default | Effect |
| --- | --- | --- |
| `trade-analysis dataset|report|model-eval --kalshi-env --days --full-history --limit --output --dataset` | varies | Builds trade-analysis datasets and model cards. |
| `trade-behavior validate --kalshi-env --days --since-hours --mode fast|detailed` | `production`, `7`, `24`, `detailed` | Validates recent behavior gates. |
| `trade-behavior quality --kalshi-env --days --min-samples --limit` | `production`, `180`, unset, `20` | Bucket quality report. |
| `TRADE_BEHAVIOR_PRODUCTION_ENTRY_FREEZE_ENABLED` | `true` | Freezes production entries while behavior training/readiness is unsafe. |
| `TRADE_BEHAVIOR_ENTRY_FREEZE_REASON` | `trade_behavior_retraining_freeze` | Reason emitted by freeze. |
| `TRADE_BEHAVIOR_FREEZE_MIN_EDGE_BPS` | `500` | Minimum edge considered by freeze logic. |
| `TRADE_BEHAVIOR_EMPIRICAL_GATE_ENABLED` | `true` | Enables empirical trade-behavior gate. |
| `TRADE_BEHAVIOR_EMPIRICAL_GATE_MIN_SETTLED_FILLS` | `20` | Minimum settled fills per bucket. |
| `TRADE_BEHAVIOR_EMPIRICAL_GATE_MIN_NET_PNL_DOLLARS` | `0.0` | Minimum net PnL per bucket. |
| `TRADE_BEHAVIOR_EMPIRICAL_GATE_LOOKBACK_DAYS` | `180` | Empirical gate lookback. |
| `TRADE_BEHAVIOR_SNAPSHOT_SCOREABILITY_SINCE` | unset | Optional lower bound for scoreable snapshots. |

## Decision Corpus, Strategy Regression, and Auto-Evolve Knobs

| Knob | Default | Effect |
| --- | --- | --- |
| `decision-corpus build --date-from --date-to --source --dry-run --notes --parent-build-id` | source `historical-replay` | Builds point-in-time decision corpus. |
| `decision-corpus calibration-report --env|--build-id --output` | required selector | Writes calibration report for a corpus. |
| `DECISION_CORPUS_AUTO_PROMOTE_INTERVAL_SECONDS` | `86400` | Daemon cadence for corpus promotion checks. |
| `strategy-regression rank --env|--build-id --output` | required selector | Ranks strategy regression clusters. |
| `STRATEGY_REGRESSION_READ_SOURCE` | `primary` | DB read source for strategy regression. |
| `STRATEGY_REGRESSION_DAILY_RUN_SECONDS` | `86400` | Daily run cadence. |
| `STRATEGY_REGRESSION_PROMOTE_FLOOR_CLUSTERS` | `30` | Promotion floor support. |
| `STRATEGY_REGRESSION_MIN_CLUSTERS_FOR_RANKING` | `3` | Minimum clusters for ranking. |
| `STRATEGY_REGRESSION_MIN_SORTINO_FOR_PROMOTION` | `0.5` | Sortino promotion threshold. |
| `STRATEGY_REGRESSION_SORTINO_DOWNSIDE_EPSILON_DOLLARS` | `1.0` | Sortino downside epsilon. |
| `STRATEGY_CORPUS_EXCLUDED_DATE_RANGES` | empty | Comma-separated `YYYY-MM-DD/YYYY-MM-DD` ranges excluded from strategy corpus. |

### Strategy Auto-Evolve

| Setting | Default | Effect |
| --- | --- | --- |
| `STRATEGY_AUTO_EVOLVE_ENABLED` | `false` | Enables auto-evolve service. |
| `STRATEGY_AUTO_EVOLVE_WINDOW_DAYS` | `180` | Corpus window. |
| `STRATEGY_AUTO_EVOLVE_ACCEPT_SUGGESTIONS` | `true` | Allows accepting suggestions. |
| `STRATEGY_AUTO_EVOLVE_ACTIVATE_SUGGESTIONS` | `false` | Allows activation after acceptance. |
| `STRATEGY_AUTO_EVOLVE_ASSIGN_ELIGIBLE` | `false` | Allows assignment; requires activation and acceptance. |
| `STRATEGY_AUTO_EVOLVE_MAX_THRESHOLD_DELTA_PCT` | `0.30` | Max threshold change. |
| `STRATEGY_AUTO_EVOLVE_MIN_IMPROVEMENT_BPS` | `100` | Global improvement threshold. |
| `STRATEGY_AUTO_EVOLVE_MIN_CITY_IMPROVEMENT_BPS` | `100` | City improvement threshold. |
| `STRATEGY_AUTO_EVOLVE_MAX_REGRESSION_BPS` | `50` | Max tolerated regression. |
| `STRATEGY_AUTO_EVOLVE_MAX_RUN_AGE_SECONDS` | `172800` | Max eligible run age. |
| `STRATEGY_AUTO_EVOLVE_MIN_CORPUS_ROWS` | `500` | Minimum corpus rows. |
| `STRATEGY_AUTO_EVOLVE_MIN_CORPUS_CITIES` | `3` | Minimum corpus cities. |
| `STRATEGY_AUTO_EVOLVE_MIN_CITY_ROWS` | `25` | Minimum city rows. |
| `STRATEGY_AUTO_EVOLVE_COOLDOWN_SECONDS` | `86400` | Suggestion cadence. |
| `STRATEGY_AUTO_EVOLVE_GREENFIELD_ENABLED` | `false` | Allows greenfield strategy suggestions. |
| `STRATEGY_AUTO_EVOLVE_REFERENCE_STRATEGY_NAME` | unset | Optional reference strategy. |
| `STRATEGY_AUTO_EVOLVE_REFERENCE_RUN_ID` | unset | Optional reference run. |
| `STRATEGY_AUTO_EVOLVE_MAX_CITIES_PER_CYCLE` | `3` | City assignment cap per cycle. |
| `STRATEGY_AUTO_EVOLVE_ACCEPT_MAX_RUN_AGE_SECONDS` | `3600` | Max age for acceptance. |
| `STRATEGY_AUTO_EVOLVE_CITY_ASSIGNMENT_COOLDOWN_DAYS` | `14` | City assignment cooldown. |
| `STRATEGY_AUTO_EVOLVE_MIN_CITY_CORPUS_DAYS` | `14` | Minimum city corpus days. |
| `STRATEGY_AUTO_EVOLVE_MIN_RECENT_LIVE_RESOLVED_FILLS` | `5` | Recent live support for assignments. |
| `STRATEGY_AUTO_EVOLVE_BACKTEST_MIN_RESOLVED_REGRESSION_ROOMS` | `30` | Regression-room support. |
| `STRATEGY_AUTO_EVOLVE_BACKTEST_MIN_CANDIDATE_TRADES` | `10` | Candidate trade support. |
| `STRATEGY_AUTO_EVOLVE_ASSIGNMENT_MIN_IMPROVEMENT_BPS` | `200` | Assignment improvement threshold. |
| `STRATEGY_AUTO_EVOLVE_PER_CITY_MAX_NEGATIVE_DELTA_BPS` | `100` | Per-city max negative delta. |
| `STRATEGY_AUTO_EVOLVE_GREENFIELD_MIN_WIN_RATE_BPS` | `5500` | Greenfield win-rate threshold. |
| `STRATEGY_AUTO_EVOLVE_GREENFIELD_MIN_RESOLVED_TRADES` | `10` | Greenfield support. |
| `STRATEGY_AUTO_EVOLVE_GREENFIELD_REFERENCE_WIN_RATE` | `0.50` | Greenfield baseline win rate. |
| `STRATEGY_AUTO_EVOLVE_INCUMBENT_HEALTH_WIN_RATE_FLOOR_BPS` | `4500` | Incumbent health floor. |
| `STRATEGY_AUTO_EVOLVE_WATCHDOG_WIN_RATE_DEGRADATION_BPS` | `1000` | Watchdog degradation threshold. |
| `STRATEGY_AUTO_EVOLVE_WATCHDOG_MIN_RESOLVED_LIVE_FILLS` | `5` | Watchdog support. |

Related commands:

- `strategy-promotion-watchdog evaluate --promotion-id --source`
- `strategy-promotion-watchdog resolve --promotion-id --action approve|rollback --resolved-by --note`
- `strategy-promotion-secondary-sync sweep --source --limit`
- `record-strategy-promotion --strategy --from-state --to-state --actor --evidence-ref --notes`
- `list-strategy-promotions --strategy --limit`

## Self-Improve Knobs

The self-improvement loop consumes stored bundles, critiques, holdouts, and canaries.

| Setting | Default | Effect |
| --- | --- | --- |
| `SELF_IMPROVE_WINDOW_DAYS` | `14` | Critique/evaluation window. |
| `SELF_IMPROVE_HOLDOUT_RATIO` | `0.2` | Holdout ratio. |
| `SELF_IMPROVE_MIN_IMPROVEMENT` | `0.02` | Required improvement. |
| `SELF_IMPROVE_MAX_CRITICAL_REGRESSION` | `0.01` | Max critical regression. |
| `SELF_IMPROVE_CANARY_MIN_ROOMS` | `25` | Canary room support. |
| `SELF_IMPROVE_CANARY_MIN_SECONDS` | `7200` | Minimum canary runtime. |
| `SELF_IMPROVE_CANARY_MAX_SECONDS` | `21600` | Canary stale window. |
| `SELF_IMPROVE_LIVE_MONITOR_SECONDS` | `86400` | Live monitor window. |
| `SELF_IMPROVE_RESEARCH_GATE_FAILURE_THRESHOLD` | `0.6` | Research-gate failure threshold. |
| `SELF_IMPROVE_BLOCKED_ORDER_THRESHOLD` | `0.8` | Blocked-order threshold. |

## Crypto Training and Replay Knobs

Crypto has its own history, spot, model, replay, gate, and autonomy loops.

### Crypto Commands

| Command knob | Effect |
| --- | --- |
| `crypto-history bootstrap --kalshi-env --days --frequency --assets` | Backfills market history; default days `180`, frequency `15m`. |
| `crypto-history daily --kalshi-env --frequency` | Daily history update. |
| `crypto-history collect-open --kalshi-env --frequency --assets` | Captures open markets. |
| `crypto-history collect-settled --kalshi-env --days --frequency --assets` | Captures settled markets; default days `2`. |
| `crypto-history status --kalshi-env --days --frequency` | History coverage status. |
| `crypto-spot collect-current --kalshi-env --frequency --assets` | Captures current spot OHLC. |
| `crypto-spot backfill --kalshi-env --days --frequency --assets` | Backfills spot OHLC; default days `180`. |
| `crypto-spot status --kalshi-env --days --frequency --assets` | Spot coverage status. |
| `crypto-spot coinbase-products --kalshi-env --assets` | Verifies Coinbase product coverage. |
| `crypto-model train --kalshi-env --frequency --assets` | Runs pre-training backfill/materialization, then trains crypto forecast artifacts from feature-store rows. |
| `crypto-model train --skip-preflight` | Operator recovery flag to bypass pre-training backfill/materialization. |
| `crypto-model train --feature-store-only` | Trains from existing materialized feature rows only. |
| `crypto-model candidates --kalshi-env --days --frequency --assets` | Lists candidate rows; default days `30`. |
| `crypto-replay run --kalshi-env --days --limit --frequency --assets` | Runs strict replay/backtest; default days `30`, limit `0` means unbounded. |
| `crypto-replay gate --kalshi-env --frequency --assets` | Persists replay gate result. |
| `crypto-replay validate --kalshi-env --days --limit --frequency --assets` | Validates replay readiness. |
| `crypto-maker-markout-report --kalshi-env --days --frequency --assets` | Read-only maker adverse-selection markout report; run from `trainer_production` for historical windows. |
| `crypto-status --kalshi-env --frequency --assets` | Current crypto domain status. |
| `crypto-autonomy run-once --kalshi-env --frequency --assets` | Runs one crypto autonomy pass. |
| `crypto-asset-mode list --kalshi-env --frequency` | Lists per-asset modes. |
| `crypto-asset-mode set --kalshi-env SYMBOL off|shadow|live` | Overrides one asset mode. |
| `crypto-policy optimize --kalshi-env --frequency --days --assets` | Optimizes crypto runtime policy from replay evidence. |
| `crypto-live-path status --kalshi-env --frequency --assets --status-days --strict-rows-target --candidate-target --require-ready --baselines` | Readiness report for explicit assets, or discovered frequency-scoped assets when `--assets` is omitted or `all`; defaults are `14` status days, `60` strict rows, and `50` trade candidates. |
| `crypto-live-path refresh --kalshi-env --frequency --assets --settled-days --history-days --spot-days --replay-days --until-ready --max-iterations --sleep-seconds` | Runs history, spot, model training, replay, and gate refresh loop toward live readiness; omitted assets and `--assets all` use discovery. If refresh creates enough strict rows after an initial training-preflight block, it performs one post-refresh train/replay/gate retry. |

### Crypto Settings

| Setting | Default | Effect |
| --- | --- | --- |
| `CRYPTO_ENABLED` | `true` | Enables crypto domain. |
| `CRYPTO_15M_ENABLED` | `true` | Enables 15-minute crypto markets. |
| `CRYPTO_1H_ENABLED` | `true` | Enables 1-hour crypto markets. |
| `CRYPTO_AUTO_FREQUENCIES` | `15m` | Frequencies collected by daemon crypto loops; the dedicated 1h daemon sets this to `1h`. |
| `CRYPTO_TRADING_ENABLED` | `false` | Allows crypto trading path. |
| `KALSHI_REST_RATE_LIMIT_PER_SECOND` | `8.0` | Local Kalshi REST read/write token-bucket refill rate. Raise only for bounded catch-up jobs and watch for `429` responses. |
| `KALSHI_REST_RATE_LIMIT_BURST` | `16` | Local Kalshi REST token-bucket burst size. |
| `CRYPTO_HISTORY_LOOKBACK_DAYS` | `180` | Default history lookback. |
| `CRYPTO_COLLECT_SETTLED_CANDLES_ENABLED` | `true` | Captures Kalshi candles during settled-label collection. Turn off for full `crypto-live-path refresh` catch-ups when the following history bootstrap will capture candles. |
| `CRYPTO_SETTLED_PAGINATION_STOP_AT_CUTOFF` | `false` | Stops settled-market pagination once a whole page is older than the requested cutoff. Keep off for maximum conservatism; enable for bounded catch-ups after confirming Kalshi pages are newest-first. |
| `CRYPTO_HISTORICAL_PAGINATION_STOP_AT_CUTOFF` | `false` | Stops historical-market pagination once a whole page is older than the requested cutoff. Keep off for maximum conservatism; enable for bounded catch-ups after confirming Kalshi pages are newest-first. |
| `CRYPTO_HISTORY_CANDLE_CONCURRENCY` | `1` | Max concurrent Kalshi candlestick fetches during crypto history backfills. |
| `CRYPTO_MIN_TRAINING_SAMPLES` | `250` | Minimum rows for trained crypto model status. |
| `CRYPTO_TRAIN_MAX_FIT_ROWS_1H` | `150000` | 1h-only fit-row cap applied inside `CryptoForecastService.train()` (newest rows win); bounds the 1h candidate fit's memory footprint independent of `CRYPTO_TRAIN_MAX_SNAPSHOTS`. `None` disables the cap (falls back to `CRYPTO_TRAIN_MAX_SNAPSHOTS`); 15m is never capped by this. |
| `CRYPTO_REPLAY_MIN_RESOLVED_MARKETS` | `500` | Replay gate resolved-market support. |
| `CRYPTO_REPLAY_MIN_TRADE_CANDIDATES` | `50` | Replay gate candidate support. |
| `CRYPTO_REPLAY_MIN_NET_PL_DOLLARS` | `0.0` | Replay gate net P/L floor. |
| `CRYPTO_REPLAY_MAX_HARD_CAP_BREACHES` | `0` | Replay gate hard-cap breach ceiling. |
| `CRYPTO_REPLAY_REQUIRE_CALIBRATION_BETTER_THAN_MID` | `false` | Requires calibration to beat market mid. |
| `CRYPTO_REPLAY_REQUIRE_PNL_BEATS_MARKET_MID` | `true` | Requires P/L to beat market mid. |
| `CRYPTO_REPLAY_MIN_PNL_ADVANTAGE_DOLLARS` | `0.0` | Required P/L advantage. |
| `CRYPTO_REPLAY_MIN_SPOT_COVERAGE_PCT` | `0.80` | Minimum spot coverage. |
| `CRYPTO_DYNAMIC_ORDER_SIZING_ENABLED` | `true` | Enables dynamic initial ticket sizing for live-quality crypto candidates. |
| `CRYPTO_DYNAMIC_ORDER_SIZING_SCOPE` | `live_quality` | Candidate scope for crypto dynamic sizing. |
| `CRYPTO_DYNAMIC_ORDER_TARGET_POSITION_PCT` | `0.10` | Target crypto position allocation as a fraction of capital, capped by `RISK_POSITION_PCT`. |
| `CRYPTO_HISTORY_AUTO_ENABLED` | `true` | Enables automatic history collection. |
| `CRYPTO_HISTORY_AUTO_INTERVAL_SECONDS` | `900` | History collection cadence. |
| `CRYPTO_HISTORY_AUTO_LOOKBACK_DAYS` | `2` | Auto history lookback. |
| `CRYPTO_QUOTE_EVIDENCE_ENABLED` | `true` | Enables quote evidence collection. |
| `CRYPTO_QUOTE_EVIDENCE_INTERVAL_SECONDS` | `15` | Quote evidence cadence. |
| `CRYPTO_SPOT_CURRENT_AUTO_ENABLED` | `true` | Enables current spot auto collection. |
| `CRYPTO_SPOT_CURRENT_INTERVAL_SECONDS` | `15` | Current spot cadence. |
| `CRYPTO_SPOT_HISTORY_AUTO_ENABLED` | `true` | Enables spot history auto collection. |
| `CRYPTO_SPOT_HISTORY_AUTO_LOOKBACK_DAYS` | `2` | Spot history auto lookback. |
| `CRYPTO_SPOT_PROXY_FALLBACK_ENABLED` | `false` | Allows proxy spot fallback. Proxy-only data is not live-quality. |
| `CRYPTO_SPOT_COINBASE_MAX_STALE_SECONDS` | `180` | Coinbase spot staleness limit. |
| `CRYPTO_SPOT_COINGECKO_MAX_STALE_SECONDS` | `90` | CoinGecko spot staleness limit. |
| `CRYPTO_AUTONOMY_ENABLED` | `false` | Enables crypto autonomy loop. |
| `CRYPTO_PRODUCTION_AUTONOMY_ENABLED` | `false` | Enables production autonomy. |
| `CRYPTO_AUTONOMY_INTERVAL_SECONDS` | `30` | Idle retry interval when autonomy cannot run because the service is missing or the color is inactive. Active autonomy runs continuously: the next pass starts as soon as the prior pass finishes. |
| `CRYPTO_AUTONOMY_MIN_SECONDS_TO_CLOSE` | `0` | Minimum seconds to close; `0` keeps evaluating until close. |
| `CRYPTO_1H_AUTONOMY_MIN_SECONDS_TO_CLOSE` | `300` | Per-frequency no-entry buffer for 1h crypto. Keeps BTC 1h out of the final 0-5m settlement-reversal bucket. |
| `CRYPTO_LATE_SURE_THING_ENABLED` | `true` | Allows the late high-confidence market-confirmed bypass path. |
| `CRYPTO_LATE_SURE_THING_MAX_SECONDS_TO_CLOSE` | `300` | Outer cap for late high-confidence entries. |
| `CRYPTO_LATE_SURE_THING_STANDARD_MAX_SECONDS_TO_CLOSE` | `180` | Standard late window; entries inside it use the normal probability floor. |
| `CRYPTO_LATE_SURE_THING_MIN_PROBABILITY` | `0.85` | Minimum model-side probability inside the standard late window. |
| `CRYPTO_LATE_SURE_THING_EXTENDED_MIN_PROBABILITY` | `0.90` | Minimum model-side probability from 180-300 seconds before close. |
| `CRYPTO_LATE_SURE_THING_MIN_MARKET_PROBABILITY` | `0.75` | Minimum market-implied side probability for late high-confidence entries. |
| `CRYPTO_LATE_SURE_THING_NEAR_STRIKE_MOMENTUM_GUARD_ENABLED` | `true` | Enables the near-strike adverse-momentum guard inside the standard late window. |
| `CRYPTO_LATE_SURE_THING_NEAR_STRIKE_MAX_MONEYNESS_PCT` | `0.0001` | Near-strike band as percent distance to target. |
| `CRYPTO_LATE_SURE_THING_NEAR_STRIKE_MIN_ADVERSE_RETURN_PCT` | `0.0001` | Minimum recent spot return that counts as adverse momentum. |
| `CRYPTO_LATE_SURE_THING_NEAR_STRIKE_MIN_ADVERSE_RETURNS` | `2` | Required count of adverse recent return windows. |
| `CRYPTO_LATE_SURE_THING_NEAR_STRIKE_MIN_PROBABILITY` | `0.90` | Required model-side probability when the near-strike guard applies. |
| `CRYPTO_LATE_SURE_THING_REVERSAL_GUARD_ENABLED` | `true` | Blocks extended-window late entries when spot momentum points against the selected side. |
| `CRYPTO_LATE_SURE_THING_REVERSAL_GUARD_MIN_SECONDS_TO_CLOSE` | `181` | Start of the adverse-momentum block inside the extended late window. |
| `CRYPTO_LATE_SURE_THING_REVERSAL_GUARD_MIN_ADVERSE_RETURN_PCT` | `0.0001` | Minimum recent return that counts as adverse for the selected side. |
| `CRYPTO_LATE_SURE_THING_TARGET_DISTANCE_GUARD_ENABLED` | `true` | Requires directional target-distance cushion when the feature is available. |
| `CRYPTO_LATE_SURE_THING_MIN_TARGET_DISTANCE_VOLATILITY` | `3.0` | Required target distance in volatility units. |
| `CRYPTO_AUTONOMY_MAX_ROOMS_PER_RUN` | `7` | Per-run room cap. |
| `CRYPTO_AUTONOMY_MAX_PER_ASSET_PER_RUN` | `1` | Per-asset room cap. |
| `CRYPTO_SHADOW_EXPLORATION_MAX_CANDIDATES_PER_RUN` | `12` | Shadow candidate cap. |
| `CRYPTO_SHADOW_EXPLORATION_MAX_PER_ASSET_PER_RUN` | `2` | Shadow per-asset cap. |
| `CRYPTO_SHADOW_EXPLORATION_MIN_EXPECTED_NET_EDGE_DOLLARS` | `-0.03` | Shadow exploration net-edge floor. |
| `CRYPTO_SHADOW_EXPLORATION_MAX_SPREAD_BPS` | `500` | Shadow spread ceiling. |
| `CRYPTO_LIVE_MAX_SPREAD_BPS` | `1000` | Live spread ceiling. |
| `CRYPTO_EMPIRICAL_BUCKET_GATE_ENABLED` | `true` | Enables empirical bucket gate. |
| `CRYPTO_EMPIRICAL_BUCKET_GATE_ASSETS` | `live` | Assets subject to empirical bucket gate. |
| `CRYPTO_EMPIRICAL_BUCKET_MIN_SAMPLES` | `20` | Bucket support threshold. |
| `CRYPTO_EMPIRICAL_BUCKET_MIN_NET_PNL_DOLLARS` | `0.0` | Bucket net P/L floor. |
| `CRYPTO_EMPIRICAL_BUCKET_MIN_WIN_RATE` | `0.55` | Bucket win-rate floor. |

### Nightly Model Regeneration Settings

The daemon runs a nightly per-asset model+backtest+gate refresh at a configured local hour. See `docs/crypto-trading-dials-and-knobs.md` for the full reference including staleness logic, checkpoint schema, and rollback steps.

| Setting | Default | Effect |
| --- | --- | --- |
| `CRYPTO_MODEL_NIGHTLY_AUTO_ENABLED` | `false` | Master switch for the legacy crypto train/replay/gate loop. |
| `CRYPTO_MODEL_NIGHTLY_TIMEZONE` | `America/Los_Angeles` | IANA timezone for local-date and hour checks. |
| `CRYPTO_MODEL_NIGHTLY_HOUR_LOCAL` | `3` | Local clock hour (0–23) at which the job becomes eligible. |
| `CRYPTO_MODEL_NIGHTLY_MIN_NEW_STRICT_ROWS` | `60` | Minimum strict-trade-eligible rows in the last 24 h to trigger a refresh. |
| `CRYPTO_MODEL_NIGHTLY_MAX_AGE_HOURS` | `336` | Force-refresh if the model's `trained_at` is older than two weeks. |
| `CRYPTO_MODEL_NIGHTLY_ASSETS` | `BTC,ETH,SOL,XRP,BNB,DOGE,HYPE` | Comma-separated ordered list of assets to evaluate. |

## Strategy C and Other Strategy Knobs

These are not training-only, but they shape strategy labels and future evidence.

| Setting | Default | Effect |
| --- | --- | --- |
| `STRATEGY_C_ENABLED` | `false` | Enables Strategy C. |
| `STRATEGY_C_SHADOW_ONLY` | `true` | Keeps Strategy C shadowed. |
| `STRATEGY_C_CADENCE_IDLE_SECONDS` | `3600` | Idle cadence. |
| `STRATEGY_C_CADENCE_APPROACH_SECONDS` | `900` | Approach cadence. |
| `STRATEGY_C_CADENCE_NEAR_THRESHOLD_SECONDS` | `150` | Near-threshold cadence. |
| `STRATEGY_C_CADENCE_POST_PEAK_SECONDS` | `900` | Post-peak cadence. |
| `STRATEGY_C_NEAR_THRESHOLD_MARGIN_F` | `2.0` | Near-threshold margin. |
| `STRATEGY_C_APPROACH_MARGIN_F` | `5.0` | Approach margin. |
| `STRATEGY_C_REQUIRED_CONSECUTIVE_CONFIRMATIONS` | `2` | Confirmation count. |
| `STRATEGY_C_MAX_OBSERVATION_AGE_MINUTES` | `30` | Observation staleness. |
| `STRATEGY_C_MAX_FORECAST_RESIDUAL_F` | `8.0` | Residual ceiling. |
| `STRATEGY_C_MAX_CLI_VARIANCE_DEGF` | `1.5` | CLI variance ceiling. |
| `STRATEGY_C_MIN_TIME_TO_SETTLEMENT_MINUTES` | `60` | Minimum time to settlement. |
| `STRATEGY_C_LOCKED_YES_DISCOUNT_CENTS` | `1` | YES locked discount. |
| `STRATEGY_C_LOCKED_NO_DISCOUNT_CENTS` | `1` | NO locked discount. |
| `STRATEGY_C_MIN_EDGE_CENTS` | `2` | Minimum edge. |
| `STRATEGY_C_MAX_BOOK_AGE_SECONDS` | `30` | Book age ceiling. |
| `STRATEGY_C_RECENT_ADVERSE_WINDOW_MINUTES` | `15` | Adverse movement window. |
| `STRATEGY_C_RACE_DETECTION_ENABLED` | `true` | Enables race detection. |

Monotonicity arbitrage knobs:

- `MONOTONICITY_ARB_ENABLED=false`
- `MONOTONICITY_ARB_SHADOW_ONLY=true`
- `MONOTONICITY_ARB_ATOMIC_EXECUTION_READY=false`
- `MONOTONICITY_ARB_MIN_NET_EDGE_CENTS=2`
- `MONOTONICITY_ARB_MAX_NOTIONAL_DOLLARS=25.0`
- `MONOTONICITY_ARB_MAX_PROPOSALS_PER_MINUTE=5`
- `MONOTONICITY_ARB_CADENCE_SECONDS=60`

## Data Quality and Research Knobs

These decide whether rows are fresh, scoreable, or safe to use.

| Setting | Default | Effect |
| --- | --- | --- |
| `RESEARCH_REFRESH_FAILED_COOLDOWN_SECONDS` | `300` | Cooldown after failed refresh. |
| `RESEARCH_REFRESH_COOLDOWN_SECONDS` | `120` | Dossier refresh cooldown. |
| `WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS` | `300` | Weather dossier refresh interval. |
| `WEATHER_RESEARCH_REFRESH_MARGIN_SECONDS` | `180` | Refresh margin. |
| `WEATHER_RESEARCH_REFRESH_CONCURRENCY` | `4` | Refresh concurrency. |
| `RESEARCH_WEB_MAX_RESULTS` | `5` | Web result cap. |
| `RESEARCH_WEB_MAX_QUERIES` | `2` | Web query cap. |
| `SOURCE_HEALTH_PAUSE_NEW_ENTRIES_ENABLED` | `true` | Pauses new entries when source health breaks. |
| `SOURCE_HEALTH_BROKEN_PAUSE_CONSECUTIVE_CYCLES` | `3` | Broken cycles before pause. |
| `SOURCE_HEALTH_EXPECTED_CADENCE_SECONDS` | `21600` | Expected source heartbeat cadence. |
| `SOURCE_HEALTH_CONSISTENCY_DEVIATION_SCALE_F` | `12.0` | Source consistency deviation scale. |
| `SIGNALS_ATTENTION_LOOKBACK_HOURS` | `24` | Signals-worth-attention window. |

Useful commands:

- `training-status`
- `training-build-list --limit`
- `research-audit --limit`
- `strategy-audit room ROOM_ID`
- `strategy-audit backfill --days --limit`
- `strategy-audit summary --days --limit`
- `baseline-model-card --historical --shadow --output`

## Recommended Tuning Protocol

1. Decide whether you are changing data selection, data collection, a model artifact, runtime thresholds, or promotion gates. Do not mix these in one experiment unless the goal is an integrated policy change.
2. Snapshot current state: `training-status`, `historical-status --verbose`, relevant `parameter-pack status`, and current agent/heuristic pack versions.
3. Generate a read-only artifact first: use `--dry-run`, `evaluate`, `preview`, `status`, `report`, or `validate` commands where available.
4. Build or refresh the exact dataset used for the decision and keep the JSONL path in the report.
5. Run the matching gate: parameter-pack gate, learned-gate, NWS parser gate, gate-learning recommend, backtesting validate, crypto replay gate, or weather model validation.
6. Canary or stage before promotion when the workflow supports it.
7. After promotion, treat all future rooms as a new policy regime and keep old/new pack versions separate during analysis.

## Knobs That Require Code Changes

These are real training levers, but they are not exposed as env settings or CLI flags today:

- historical checkpoint labels and times
- historical Gemini split percentages
- role SFT role set and task names
- forward-readiness dominance warnings and no-trade warning ratios
- modeling `MIN_MODELING_ROWS`, `TRAIN_FRACTION`, and built-in Platt optimizer constants
- backtesting `DEFAULT_MIN_TRAIN_ROWS` and `DEFAULT_MIN_TEST_ROWS`
- learned-head max blend weight and minimum Sharpe improvement defaults unless called through wrapper code
- NWS parser minimum availability and max schema failure rate unless called through wrapper code
