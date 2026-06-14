# Database

## Primary tables

Room and transcript state:

- `rooms`
- `room_messages`
- `artifacts`
- `room_campaigns`
- `room_research_health`
- `room_strategy_audits`

Market and source data:

- `raw_exchange_events`
- `raw_weather_events`
- `market_state`
- `market_price_history`
- `forecast_snapshots`
- `climatology_priors`
- `source_health_logs`

Crypto evidence, training, and replay:

- `crypto_market_snapshots`
- `crypto_market_candlesticks`
- `crypto_spot_ohlc`
- `crypto_funding_rates`
- `crypto_order_book_snapshots`
- `crypto_trade_ticks`
- `crypto_settlement_benchmark_windows`
- `crypto_model_artifacts`
- `crypto_decision_outcomes`
- `crypto_training_feature_rows`
- `crypto_data_quality_runs`
- `crypto_execution_examples`

Execution and risk:

- `signals`
- `trade_tickets`
- `decision_traces`
- `risk_verdicts`
- `orders`
- `fills`
- `positions`
- `weather_bootstrap_events`
- `weather_bootstrap_historical_evidence`

Research, learning, and historical replay:

- `research_dossiers`
- `research_runs`
- `research_sources`
- `research_claims`
- `agent_packs`
- `parameter_packs`
- `critique_runs`
- `evaluation_runs`
- `promotion_events`
- `historical_intelligence_runs`
- `historical_pipeline_runs`
- `heuristic_packs`
- `heuristic_pack_promotions`
- `heuristic_patch_suggestions`
- `training_dataset_builds`
- `training_dataset_build_items`
- `training_readiness`
- `historical_import_runs`
- `historical_market_snapshots`
- `historical_weather_snapshots`
- `historical_checkpoint_archives`
- `historical_settlement_labels`
- `historical_replay_runs`
- `decision_corpus_builds`
- `decision_corpus_rows`

Control plane, auth, and memory:

- `ops_events`
- `web_users`
- `web_sessions`
- `memory_notes`
- `memory_embeddings`
- `checkpoints`
- `deployment_control`
- `cli_reconciliation`

Strategy and weather calibration:

- `strategies`
- `strategy_results`
- `city_strategy_assignments`
- `strategy_promotions`
- `city_assignment_events`
- `strategy_promotion_events`
- `strategy_codex_runs`
- `station_sigma_params`
- `global_lead_factor`
- `strategy_c_rooms`
- `cli_station_variance`
- `monotonicity_arb_proposals`

## Notable rules

- `room_messages` are append-only and sequence-ordered per room.
- `trade_tickets.client_order_id` is unique.
- `orders.client_order_id` is unique for idempotent execution tracing.
- `deployment_control` is a singleton row controlling active color and kill switch status.
- `checkpoints` holds daemon heartbeats, rollout markers, collector progress, and other idempotent runtime cursors.
- Crypto model artifacts, feature rows, data-quality runs, and replay gates are shared through production Postgres so `trainer_production` can refresh models without restarting live daemons.

## Restore checks

After a restore:

1. Verify `deployment_control.active_color`.
2. Re-enable the kill switch before resuming trading.
3. Reconcile positions, fills, and open orders against Kalshi.
