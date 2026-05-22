from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    app_name: str = "kalshi-bot"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_color: str = "blue"
    app_shadow_mode: bool = True
    app_auto_init_db: bool = False
    app_enable_kill_switch: bool = True
    web_auth_enabled: bool = True
    web_auth_cookie_name: str | None = None
    web_auth_cookie_domain: str | None = None
    web_auth_session_ttl_seconds: int = 1_209_600
    web_auth_allowed_registration_emails: str = "m7.ga.77@gmail.com"
    web_site_kind: str = "combined"
    web_demo_host: str = "demo.ai-al.site"
    web_production_host: str = "prod.ai-al.site"
    web_strategies_host: str = "strategy.ai-al.site"

    @model_validator(mode="after")
    def default_web_auth_cookie_name(self) -> "Settings":
        configured = str(self.web_auth_cookie_name or "").strip()
        if configured:
            self.web_auth_cookie_name = configured
            return self

        env_slug = "".join(ch if ch.isalnum() else "_" for ch in str(self.kalshi_env or "demo").strip().lower()) or "shared"
        self.web_auth_cookie_name = f"kalshi_bot_session_{env_slug}"
        return self

    database_url: str | None = None
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "kalshi_bot"
    postgres_user: str = "postgres"
    postgres_password: str | None = None
    postgres_secondary_host: str | None = None
    strategy_regression_read_source: str = "primary"

    kalshi_env: str = "demo"
    kalshi_read_api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_READ_API_KEY_ID", "KALSHI_API_KEY"),
    )
    kalshi_read_private_key_path: str | None = None
    kalshi_write_api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_WRITE_API_KEY_ID", "KALSHI_API_KEY"),
    )
    kalshi_write_private_key_path: str | None = None
    live_kalshi_api_key: str | None = None
    live_kalshi_read_private_key_path: str | None = None
    live_kalshi_write_private_key_path: str | None = None
    demo_kalshi_api_key: str | None = None
    demo_kalshi_read_private_key_path: str | None = None
    demo_kalshi_write_private_key_path: str | None = None
    kalshi_subaccount: int = 0
    kalshi_taker_fee_rate: float = 0.07
    kalshi_leaderboard_base_url: str = "https://api.elections.kalshi.com/v1"
    kalshi_leaderboard_path: str = "/social/leaderboard"
    kalshi_leaderboard_web_url: str = "https://kalshi.com/social/leaderboard"
    kalshi_leaderboard_timeout_seconds: float = 30.0
    kalshi_leaderboard_user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    )
    kalshi_leaderboard_cookie: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_LEADERBOARD_COOKIE", "KALSHI_WEB_COOKIE"),
    )
    kalshi_leaderboard_authorization: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_LEADERBOARD_AUTHORIZATION", "KALSHI_WEB_AUTHORIZATION"),
    )
    kalshi_leaderboard_csrf_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_LEADERBOARD_CSRF_TOKEN", "KALSHI_WEB_CSRF_TOKEN"),
    )

    weather_user_agent: str = "kalshi-bot/0.1 (ops@example.com)"
    weather_market_map_path: str = "docs/examples/weather_markets.example.yaml"
    weather_request_timeout_seconds: float = 30.0
    weather_retry_attempts: int = 3
    weather_retry_base_delay_seconds: float = 0.25

    llm_hosted_base_url: str = "https://api.openai.com/v1"
    llm_hosted_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("LLM_HOSTED_API_KEY", "OPENAI_API_KEY"),
    )
    llm_hosted_model: str = "gpt-4o"
    codex_model: str = "gpt-4o"
    llm_local_base_url: str = "http://localhost:11434/v1"
    llm_local_api_key: str = "dummy"
    llm_local_model: str = "llama3.1:8b"
    gemini_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("GEMINI_KEY", "GEMINI_API_KEY"),
    )
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    gemini_model_researcher: str = "gemini-2.5-flash"
    gemini_model_president: str = "gemini-2.5-pro"
    gemini_model_trader: str = "gemini-2.5-pro"
    gemini_model_risk_officer: str = "gemini-2.5-flash"
    gemini_model_ops_monitor: str = "gemini-2.5-flash"
    gemini_model_memory_librarian: str = "gemini-2.5-flash"
    llm_calls_enabled: bool = False
    active_agent_pack_version: str = "builtin-deterministic-v1"
    llm_request_timeout_seconds: float = 30.0
    llm_trading_enabled: bool = False

    trigger_broken_book_retry_seconds: int = 30
    research_refresh_failed_cooldown_seconds: int = 300
    risk_order_pct: float = 0.05
    risk_position_pct: float = 0.10
    risk_daily_loss_pct: float = 0.20
    risk_daily_loss_sensitivity_pct: float = 0.10
    risk_daily_loss_sensitivity_edge_multiplier: float = 2.0
    risk_daily_loss_sensitivity_size_multiplier: float = 0.50
    risk_max_concurrent_tickers: int = 10
    # Override-only dollar caps — used in tests or hard-ceiling scenarios.
    # In production leave unset; supervisor derives caps from live balance × pct.
    risk_max_order_notional_dollars: float | None = None
    risk_max_position_notional_dollars: float | None = None
    risk_daily_loss_limit_dollars: float | None = None
    # P2-2: edge-scaled (fractional-Kelly) sizing. Off by default until
    # calibration confirms the fair-value signal is well-calibrated. When on,
    # the Kelly notional is still capped by the existing flat-percentage limits.
    risk_edge_scaled_sizing_enabled: bool = False
    risk_edge_scaled_kelly_multiplier: float = 0.25
    risk_daily_loss_dollars_by_strategy: dict[str, float] = Field(default_factory=dict)
    weather_live_probe_min_loss_dollars: float = 1.0
    weather_live_probe_min_confidence: float = 0.90
    weather_live_probe_min_net_edge_bps: int = 5000
    weather_live_probe_max_order_notional_dollars: float = 0.25
    weather_live_probe_daily_notional_dollars: float = 1.0
    weather_live_probe_cooldown_seconds: int = 3600
    weather_live_balance_discontinuity_ratio: float = 0.50
    weather_rejected_opportunity_scorer_enabled: bool = True
    weather_rejected_opportunity_lookback_hours: int = 168
    weather_rejected_opportunity_min_settled: int = 5
    weather_rejected_opportunity_min_accuracy: float = 0.60
    weather_rejected_opportunity_auto_enable: bool = True
    weather_static_signal_backoff_enabled: bool = True
    weather_static_signal_backoff_min_evaluations: int = 3
    weather_static_signal_backoff_window_minutes: int = 30
    weather_static_signal_backoff_cooldown_seconds: int = 1800
    weather_static_signal_backoff_price_move_dollars: float = 0.05
    weather_static_signal_backoff_edge_move_bps: int = 500
    weather_static_signal_backoff_final_minutes: int = 90

    crypto_enabled: bool = True
    crypto_15m_enabled: bool = True
    crypto_1h_enabled: bool = True
    crypto_auto_frequencies: str = "15m"
    kalshi_rest_rate_limit_per_second: float = 8.0
    kalshi_rest_rate_limit_burst: int = 16
    crypto_trading_enabled: bool = False
    crypto_history_lookback_days: int = 365
    crypto_collect_settled_candles_enabled: bool = True
    crypto_settled_pagination_stop_at_cutoff: bool = False
    crypto_historical_pagination_stop_at_cutoff: bool = False
    crypto_history_candle_concurrency: int = 1
    crypto_order_mode: str = "passive_then_taker"
    crypto_passive_timeout_seconds: int = 5
    crypto_taker_fallback_close_seconds: int = 0
    crypto_live_min_market_age_seconds: int = 180
    crypto_min_training_samples: int = 250
    # Training data window + caps. Snapshots are scoped to settled markets first
    # (see PlatformRepository.list_crypto_settled_market_snapshots), so the cap
    # governs trainable decision points rather than raw recent rows. Lookback
    # bounds all three sources to a time window to keep memory predictable.
    crypto_train_lookback_days: int = 60
    crypto_train_max_snapshots: int = 500_000
    crypto_train_max_candlesticks: int = 500_000
    crypto_train_max_spot_rows: int = 600_000
    crypto_replay_min_resolved_markets: int = 500
    crypto_replay_min_trade_candidates: int = 50
    crypto_replay_min_net_pl_dollars: float = 0.0
    crypto_replay_max_hard_cap_breaches: int = 0
    crypto_replay_require_calibration_better_than_mid: bool = False
    crypto_replay_require_pnl_beats_market_mid: bool = True
    crypto_replay_min_pnl_advantage_dollars: float = 0.0
    crypto_default_order_count_fp: float = 1.0
    crypto_dynamic_order_sizing_enabled: bool = True
    crypto_dynamic_order_sizing_scope: str = "live_quality"
    crypto_dynamic_order_target_position_pct: float = 0.10
    crypto_history_auto_enabled: bool = True
    crypto_history_auto_interval_seconds: int = 3600
    crypto_history_auto_lookback_days: int = 2
    crypto_quote_evidence_enabled: bool = True
    crypto_quote_evidence_interval_seconds: int = 60
    crypto_spot_request_timeout_seconds: float = 30.0
    coinbase_cdp_api_key_file: str | None = "cdp_api_key.json"
    coinbase_cdp_key_name: str | None = None
    coinbase_cdp_private_key: str | None = None
    coinbase_advanced_trade_authenticated_enabled: bool = True
    crypto_spot_proxy_fallback_enabled: bool = False
    crypto_spot_coinbase_max_stale_seconds: int = 180
    crypto_spot_coingecko_max_stale_seconds: int = 90
    crypto_spot_current_auto_enabled: bool = True
    crypto_spot_current_interval_seconds: int = 30
    crypto_spot_history_auto_enabled: bool = True
    crypto_spot_history_auto_lookback_days: int = 2
    crypto_replay_min_spot_coverage_pct: float = 0.80
    crypto_autonomy_enabled: bool = False
    crypto_production_autonomy_enabled: bool = False
    # Sleep interval for idle/inactive-color states only. The active-color loop
    # runs continuously (asyncio.sleep(0) between iterations) so this value has
    # no effect on trading cadence.
    crypto_autonomy_idle_interval_seconds: int = 5
    crypto_autonomy_min_seconds_to_close: int = 0
    # UTC hours (0-23) during which the autonomy loop should skip new entries.
    # Comma-separated string so it's easily set via env var, e.g. "12,13,14,15,16"
    crypto_autonomy_skip_hours_utc: str = ""
    crypto_late_sure_thing_enabled: bool = True
    crypto_late_sure_thing_max_seconds_to_close: int = 300
    crypto_late_sure_thing_standard_max_seconds_to_close: int = 180
    crypto_late_sure_thing_min_probability: float = 0.85
    crypto_late_sure_thing_extended_min_probability: float = 0.90
    crypto_late_sure_thing_min_market_probability: float = 0.75
    crypto_late_sure_thing_near_strike_momentum_guard_enabled: bool = True
    crypto_late_sure_thing_near_strike_max_moneyness_pct: float = 0.0001
    crypto_late_sure_thing_near_strike_min_adverse_return_pct: float = 0.0001
    crypto_late_sure_thing_near_strike_min_adverse_returns: int = 2
    crypto_late_sure_thing_near_strike_min_probability: float = 0.90
    crypto_late_sure_thing_reversal_guard_enabled: bool = True
    crypto_late_sure_thing_reversal_guard_min_seconds_to_close: int = 181
    crypto_late_sure_thing_reversal_guard_min_adverse_return_pct: float = 0.0001
    crypto_late_sure_thing_target_distance_guard_enabled: bool = True
    crypto_late_sure_thing_min_target_distance_volatility: float = 3.0
    crypto_last_minute_passive_enabled: bool = True
    crypto_last_minute_passive_assets: str = "live"
    crypto_last_minute_passive_max_seconds_to_close: int = 60
    crypto_last_minute_passive_bid_by_asset: str = "BTC:0.55,ETH:0.54,XRP:0.54,SOL:0.63,DOGE:0.65,BNB:0.77,HYPE:0.84"
    crypto_last_minute_passive_require_no_cross: bool = True
    crypto_last_minute_passive_risk_mode: str = "normal_cap"
    crypto_last_minute_passive_price_matrix_enabled: bool = True
    crypto_last_minute_passive_price_matrix_min_samples: int = 30
    crypto_last_minute_passive_price_matrix_min_fills: int = 3
    crypto_last_minute_passive_price_matrix_min_fill_rate: float = 0.10
    crypto_last_minute_passive_price_matrix_min_net_pnl_dollars: float = 0.0
    crypto_last_minute_passive_price_matrix_fallback: str = "fixed_bid"
    crypto_last_minute_passive_price_ladder: str = "0.01:0.99:0.01"
    crypto_market_price_anchor_enabled: bool = True
    crypto_market_price_anchor_weight: float = 0.75
    crypto_autonomy_max_rooms_per_run: int = 7
    crypto_autonomy_max_per_asset_per_run: int = 1
    crypto_shadow_exploration_max_candidates_per_run: int = 12
    crypto_shadow_exploration_max_per_asset_per_run: int = 2
    crypto_shadow_exploration_min_expected_net_edge_dollars: float = -0.03
    crypto_shadow_exploration_max_spread_bps: int = 500
    crypto_live_max_spread_bps: int = 1000
    crypto_empirical_bucket_gate_enabled: bool = True
    crypto_empirical_bucket_gate_assets: str = "live"
    crypto_empirical_bucket_min_samples: int = 20
    crypto_empirical_bucket_min_net_pnl_dollars: float = 0.0
    crypto_empirical_bucket_min_win_rate: float = 0.55
    crypto_empirical_late_override_enabled: bool = True
    crypto_empirical_late_override_max_seconds_to_close: int = 180
    crypto_empirical_late_override_reasons: str = "empirical_bucket_missing,empirical_bucket_low_win_rate"
    crypto_empirical_late_override_max_count_fp: float = 1.0
    crypto_empirical_late_override_negative_pnl_enabled: bool = False

    crypto_take_profit_enabled: bool = True
    crypto_take_profit_threshold_pct: float = 0.20
    crypto_take_profit_check_interval_seconds: int = 30
    crypto_take_profit_stale_snapshot_seconds: int = 120

    stop_loss_enabled: bool = False
    stop_loss_threshold_pct: float = 0.10
    stop_loss_profit_protection_threshold_pct: float = 0.15
    stop_loss_reentry_cooldown_seconds: int = 14400
    stop_loss_momentum_reentry_window_seconds: int = 300
    stop_loss_submit_cooldown_seconds: int = 300
    stop_loss_check_interval_seconds: int = 60
    stop_loss_momentum_slope_threshold_cents_per_min: float = -0.2
    stop_loss_momentum_reentry_slope_threshold_cents_per_min: float = -0.2
    stop_loss_momentum_min_hold_minutes: int = 30
    momentum_weight_scale_cents_per_min: float = 1.0
    momentum_slope_veto_cents_per_min: float | None = None
    momentum_weight_floor: float = 0.3
    momentum_veto_staleness_gate: float = 0.5
    momentum_weight_shadow_mode: bool = True
    momentum_calibration_auto_enabled: bool = False
    momentum_calibration_nightly_hour_local: int = 2
    momentum_calibration_nightly_timezone: str = "America/Los_Angeles"
    momentum_calibration_nightly_lookback_days: int = 90
    momentum_calibration_tier1_max_delta_fraction: float = 0.10
    momentum_calibration_tier2_max_delta_fraction: float = 0.20
    momentum_calibration_tier1_max_ci_width_fraction: float = 0.30
    momentum_calibration_sanity_max_ci_width_fraction: float = 0.50
    momentum_calibration_tier1_auto_promote_enabled: bool = False
    momentum_calibration_min_slope_coverage: float = 0.80
    momentum_calibration_recent_coverage_days: int = 7
    momentum_calibration_min_observations: int = 1000
    momentum_calibration_skip_critical_threshold: int = 4
    crypto_model_nightly_auto_enabled: bool = False
    crypto_model_nightly_timezone: str = "America/Los_Angeles"
    crypto_model_nightly_hour_local: int = 3
    crypto_model_nightly_min_new_strict_rows: int = 60
    crypto_model_nightly_max_age_hours: int = 24
    crypto_model_nightly_assets: str = "BTC,ETH,SOL,XRP,BNB,DOGE,HYPE"
    # Upper bound on the per-frequency status() precondition the nightly runs
    # before deciding what to refresh. status() is an analytics-grade scan of the
    # large crypto_market_snapshots table; bounding it keeps a slow/hung query
    # from wedging the entire regen (it degrades to age-based refresh instead).
    crypto_model_nightly_status_timeout_seconds: float = 120.0
    risk_max_order_count_fp: float = 500.0
    risk_max_position_count_fp_per_ticker: float = 200.0
    risk_allow_position_add_ons: bool = False
    crypto_position_add_ons_enabled: bool = True
    crypto_position_add_on_assets: str = "live"
    crypto_position_add_on_max_position_count_fp: float = 200.0
    crypto_position_add_on_max_ticket_count_fp: float = 500.0
    risk_safe_capital_reserve_ratio: float = 0.0
    risk_risky_capital_max_ratio: float = 0.0
    risk_stale_market_seconds: int = 60
    risk_stale_weather_seconds: int = 900
    risk_min_edge_bps: int = 500
    risk_fee_aware_edge_enabled: bool = True
    risk_max_credible_edge_bps: int = 10000
    risk_min_confidence: float = 0.80
    risk_min_contract_price_dollars: float = 0.50
    risk_min_probability_extremity_pct: float = 25.0
    risk_probability_midband_max_extra_edge_bps: int = 500
    strategy_min_abs_delta_f: float = 8.0
    strategy_min_remaining_payout_bps: int = 2000
    strategy_quality_edge_buffer_bps: int = 25
    gate_learning_min_support: int = 30
    autonomous_gate_tuning_enabled: bool = True
    autonomous_gate_tuning_source: str = "combined"
    autonomous_gate_tuning_days: int = 3650
    autonomous_gate_tuning_min_support: int = 30
    autonomous_gate_tuning_canary_min_settled_rows: int = 10
    autonomous_gate_tuning_canary_max_wait_hours: int = 72
    autonomous_gate_tuning_periodic_interval_seconds: int = 3600
    weather_prediction_enabled: bool = False
    weather_source_ensemble_enabled: bool = True
    weather_source_disagreement_widen_f: float = 3.0
    weather_source_disagreement_stand_down_f: float = 8.0
    weather_source_disagreement_sigma_multiplier_max: float = 2.0
    weather_nowcast_high_so_far_enabled: bool = True
    weather_residual_model_enabled: bool = False
    weather_residual_min_mae_improvement_pct: float = 0.02
    weather_residual_min_crps_improvement_pct: float = 0.0
    weather_residual_min_brier_improvement_pct: float = 0.0
    weather_residual_model_max_age_hours: int = 168
    weather_intraday_model_enabled: bool = False
    weather_intraday_model_max_age_hours: int = 168
    weather_intraday_min_train_rows: int = 500
    weather_intraday_min_holdout_rows: int = 100
    weather_intraday_min_brier_improvement_pct: float = 0.01
    weather_intraday_max_calibration_error: float = 0.20
    weather_intraday_min_calibration_bucket_rows: int = 50
    weather_intraday_min_series_holdout_rows: int = 30
    weather_intraday_max_series_brier_regression: float = 0.05
    sigma_calibration_enabled: bool = True
    sigma_min_samples_beats_global: int = 100
    sigma_min_samples_beats_yaml: int = 200
    sigma_min_crps_improvement: float = 0.0
    sigma_lead_correction_enabled: bool = True

    trade_behavior_production_entry_freeze_enabled: bool = True
    trade_behavior_entry_freeze_reason: str = "trade_behavior_retraining_freeze"
    trade_behavior_freeze_min_edge_bps: int = 500
    trade_behavior_empirical_gate_enabled: bool = True
    trade_behavior_empirical_gate_min_settled_fills: int = 20
    trade_behavior_empirical_gate_min_net_pnl_dollars: float = 0.0
    trade_behavior_empirical_gate_lookback_days: int = 180
    trade_behavior_snapshot_scoreability_since: str | None = None
    signals_attention_lookback_hours: int = 24
    decision_policy_variants_shadow_enabled: bool = True
    low_price_high_edge_live_enabled: bool = False
    intraday_separation_override_live_enabled: bool = False
    remaining_payout_relaxation_live_enabled: bool = False
    empirical_bootstrap_live_enabled: bool = False
    policy_variant_min_quality_adjusted_edge_bps: int = 1500
    low_price_variant_min_entry_price_dollars: float = 0.05
    remaining_payout_variant_min_payout_bps: int = 1000
    empirical_bootstrap_min_evaluations: int = 3
    empirical_bootstrap_min_edge_bps: int = 1500
    empirical_bootstrap_last_edge_bps: int = 2500
    intraday_resolved_low_fair_yes: float = 0.10
    intraday_resolved_high_fair_yes: float = 0.90
    static_edge_min_evaluations: int = 3
    static_fair_min_evaluations: int = 5

    strategy_c_cadence_idle_seconds: int = 3600
    strategy_c_cadence_approach_seconds: int = 900
    strategy_c_cadence_near_threshold_seconds: int = 150
    strategy_c_cadence_post_peak_seconds: int = 900
    strategy_c_near_threshold_margin_f: float = 2.0
    strategy_c_approach_margin_f: float = 5.0
    strategy_c_required_consecutive_confirmations: int = 2
    strategy_c_max_observation_age_minutes: int = 30
    strategy_c_max_forecast_residual_f: float = 8.0
    strategy_c_max_cli_variance_degf: float = 1.5
    strategy_c_min_time_to_settlement_minutes: int = 60
    strategy_c_locked_yes_discount_cents: int = 1
    strategy_c_locked_no_discount_cents: int = 1
    strategy_c_min_edge_cents: int = 2
    strategy_c_max_book_age_seconds: int = 30
    strategy_c_recent_adverse_window_minutes: int = 15
    strategy_c_race_detection_enabled: bool = True
    strategy_c_max_order_notional_dollars: float = 50.0
    strategy_c_max_position_notional_dollars: float = 50.0
    strategy_c_enabled: bool = False
    strategy_c_shadow_only: bool = True

    monotonicity_arb_enabled: bool = False
    monotonicity_arb_shadow_only: bool = True
    monotonicity_arb_atomic_execution_ready: bool = False
    monotonicity_arb_min_net_edge_cents: int = 2
    monotonicity_arb_max_notional_dollars: float = 25.0
    monotonicity_arb_max_proposals_per_minute: int = 5
    monotonicity_arb_cadence_seconds: int = 60

    memory_embedding_dimensions: int = 16
    sse_poll_interval_seconds: float = 1.0
    research_stale_seconds: int = 900
    research_stale_grace_factor: float = 2.0  # dossier within stale_seconds * factor may still trade at reduced size
    research_stale_tolerance_notional_factor: float = 0.5  # notional cap multiplier when stale_tolerance_active
    research_refresh_cooldown_seconds: int = 120
    weather_research_refresh_interval_seconds: int = 300
    weather_research_refresh_margin_seconds: int = 180
    weather_research_refresh_concurrency: int = 4
    research_web_max_results: int = 5
    research_web_max_queries: int = 2
    stream_error_log_cooldown_seconds: int = 900
    stream_orderbook_persist_interval_seconds: float = 0.0
    trigger_enable_auto_rooms: bool = False
    trigger_cooldown_seconds: int = 300
    trigger_price_move_bypass_bps: int = 1500
    trigger_max_spread_bps: int = 250
    trigger_max_concurrent_rooms: int = 12
    trigger_active_room_stale_seconds: int = 1800
    trigger_marketability_recheck_seconds: int = 60
    trigger_marketability_waitlist_ttl_seconds: int = 1800
    trigger_marketability_recheck_limit: int = 50
    daemon_reconcile_interval_seconds: int = 60
    daemon_reconcile_stale_kill_switch_seconds: int = 300
    daemon_heartbeat_interval_seconds: int = 60
    daemon_heartbeat_unhealthy_grace_seconds: int = 45
    daemon_active_color_cache_seconds: float = 0.0
    daemon_market_update_throttle_seconds: float = 0.0
    daemon_market_history_interval_seconds: int = 60
    daemon_market_history_retention_hours: int = 24
    daemon_memory_note_retention_days: int = 90
    daemon_start_with_reconcile: bool = True
    daemon_startup_grace_seconds: int = 30
    daemon_startup_jitter_seconds: int = 45
    source_health_pause_new_entries_enabled: bool = True
    source_health_broken_pause_consecutive_cycles: int = 3
    source_health_expected_cadence_seconds: int = 21600
    source_health_consistency_deviation_scale_f: float = 12.0
    self_improve_window_days: int = 14
    self_improve_holdout_ratio: float = 0.2
    self_improve_min_improvement: float = 0.02
    self_improve_max_critical_regression: float = 0.01
    self_improve_canary_min_rooms: int = 25
    self_improve_canary_min_seconds: int = 7200
    self_improve_canary_max_seconds: int = 21600  # 6h — canary stalled if not promoted within this window
    self_improve_live_monitor_seconds: int = 86400
    self_improve_research_gate_failure_threshold: float = 0.6
    self_improve_blocked_order_threshold: float = 0.8
    training_window_days: int = 30
    training_status_room_limit: int = 500
    training_min_complete_rooms: int = 25
    training_min_market_diversity: int = 4
    training_min_settled_rooms: int = 10
    training_min_trade_positive_rooms: int = 8
    training_good_research_threshold: float = 0.7
    training_campaign_enabled: bool = False
    training_campaign_rooms_per_run: int = 3
    training_campaign_lookback_hours: int = 24
    training_campaign_cooldown_seconds: int = 600
    training_campaign_max_recent_per_market: int = 5
    historical_import_page_size: int = 500
    historical_import_max_pages: int = 25
    historical_replay_market_snapshot_lookback_hours: int = 36
    historical_replay_market_stale_seconds: int = 900
    historical_weather_archive_path: str = "data/historical_weather"
    historical_forecast_archive_provider_enabled: bool = True
    historical_forecast_archive_base_url: str = "https://single-runs-api.open-meteo.com/v1/forecast"
    historical_forecast_archive_api_key: str | None = None
    historical_forecast_archive_model_preference: str = "gfs_seamless"
    historical_forecast_archive_timeout_seconds: float = 30.0
    historical_forecast_archive_max_retries: int = 2
    historical_checkpoint_capture_lead_seconds: int = 300
    historical_checkpoint_capture_grace_seconds: int = 900
    active_heuristic_pack_version: str = "historical-baseline-v1"
    historical_intelligence_window_days: int = 365
    historical_intelligence_min_full_market_days: int = 3
    historical_intelligence_min_segment_support: int = 5
    historical_intelligence_min_composite_improvement: float = 0.02
    historical_intelligence_auto_promote: bool = True
    historical_intelligence_daily_run_seconds: int = 86400
    historical_pipeline_bootstrap_days: int = 365
    historical_pipeline_chunk_days: int = 14
    historical_pipeline_daily_run_seconds: int = 86400
    historical_pipeline_incremental_days: int = 7
    decision_corpus_auto_promote_interval_seconds: int = 86400
    strategy_regression_daily_run_seconds: int = 86400
    strategy_regression_promote_floor_clusters: int = 30
    strategy_regression_min_clusters_for_ranking: int = 3
    strategy_regression_min_sortino_for_promotion: float = 0.5
    strategy_regression_sortino_downside_epsilon_dollars: float = 1.0
    strategy_codex_nightly_enabled: bool = False
    strategy_codex_nightly_timezone: str = "America/Los_Angeles"
    strategy_codex_nightly_hour_local: int = 1
    strategy_auto_evolve_enabled: bool = False
    strategy_auto_evolve_window_days: int = 180
    strategy_auto_evolve_assign_eligible: bool = False
    strategy_auto_evolve_accept_suggestions: bool = True
    strategy_auto_evolve_activate_suggestions: bool = False
    strategy_auto_evolve_max_threshold_delta_pct: float = 0.30
    strategy_auto_evolve_min_improvement_bps: int = 100
    strategy_auto_evolve_min_city_improvement_bps: int = 100
    strategy_auto_evolve_max_regression_bps: int = 50
    strategy_auto_evolve_max_run_age_seconds: int = 172800
    strategy_auto_evolve_min_corpus_rows: int = 500
    strategy_auto_evolve_min_corpus_cities: int = 3
    strategy_auto_evolve_min_city_rows: int = 25
    strategy_auto_evolve_cooldown_seconds: int = 86400
    strategy_auto_evolve_greenfield_enabled: bool = False
    strategy_auto_evolve_reference_strategy_name: str | None = None
    strategy_auto_evolve_reference_run_id: str | None = None
    strategy_auto_evolve_max_cities_per_cycle: int = 3
    strategy_auto_evolve_accept_max_run_age_seconds: int = 3600
    strategy_auto_evolve_city_assignment_cooldown_days: int = 14
    strategy_auto_evolve_min_city_corpus_days: int = 14
    strategy_auto_evolve_min_recent_live_resolved_fills: int = 5
    strategy_auto_evolve_backtest_min_resolved_regression_rooms: int = 30
    strategy_auto_evolve_backtest_min_candidate_trades: int = 10
    strategy_auto_evolve_assignment_min_improvement_bps: int = 200
    strategy_auto_evolve_per_city_max_negative_delta_bps: int = 100
    strategy_auto_evolve_greenfield_min_win_rate_bps: int = 5500
    strategy_auto_evolve_greenfield_min_resolved_trades: int = 10
    strategy_auto_evolve_greenfield_reference_win_rate: float = 0.50
    strategy_auto_evolve_incumbent_health_win_rate_floor_bps: int = 4500
    strategy_auto_evolve_watchdog_win_rate_degradation_bps: int = 1000
    strategy_auto_evolve_watchdog_min_resolved_live_fills: int = 5
    strategy_corpus_excluded_date_ranges: str = ""
    historical_execution_confidence_min_market_days: int = 60
    historical_directional_confidence_min_full_market_days: int = 30
    historical_directional_confidence_min_holdout_market_days: int = 7

    @model_validator(mode="after")
    def _validate_auto_evolve_flags(self) -> "Settings":
        if self.strategy_auto_evolve_activate_suggestions and not self.strategy_auto_evolve_accept_suggestions:
            raise ValueError(
                "strategy_auto_evolve_activate_suggestions requires "
                "strategy_auto_evolve_accept_suggestions=True"
            )
        if self.strategy_auto_evolve_assign_eligible and not self.strategy_auto_evolve_activate_suggestions:
            raise ValueError(
                "strategy_auto_evolve_assign_eligible requires strategy_auto_evolve_activate_suggestions=True"
            )
        if self.strategy_auto_evolve_assign_eligible and not self.strategy_auto_evolve_accept_suggestions:
            raise ValueError(
                "strategy_auto_evolve_assign_eligible requires "
                "strategy_auto_evolve_accept_suggestions=True"
            )
        if not 0.0 <= self.strategy_auto_evolve_max_threshold_delta_pct <= 1.0:
            raise ValueError("strategy_auto_evolve_max_threshold_delta_pct must be between 0.0 and 1.0")

        non_negative_fields = {
            "strategy_auto_evolve_min_improvement_bps": self.strategy_auto_evolve_min_improvement_bps,
            "strategy_auto_evolve_min_city_improvement_bps": self.strategy_auto_evolve_min_city_improvement_bps,
            "strategy_auto_evolve_max_regression_bps": self.strategy_auto_evolve_max_regression_bps,
            "strategy_auto_evolve_min_corpus_rows": self.strategy_auto_evolve_min_corpus_rows,
            "strategy_auto_evolve_min_corpus_cities": self.strategy_auto_evolve_min_corpus_cities,
            "strategy_auto_evolve_min_city_rows": self.strategy_auto_evolve_min_city_rows,
            "strategy_auto_evolve_cooldown_seconds": self.strategy_auto_evolve_cooldown_seconds,
            "strategy_auto_evolve_max_cities_per_cycle": self.strategy_auto_evolve_max_cities_per_cycle,
            "strategy_auto_evolve_city_assignment_cooldown_days": self.strategy_auto_evolve_city_assignment_cooldown_days,
            "strategy_auto_evolve_min_city_corpus_days": self.strategy_auto_evolve_min_city_corpus_days,
            "strategy_auto_evolve_min_recent_live_resolved_fills": self.strategy_auto_evolve_min_recent_live_resolved_fills,
            "strategy_auto_evolve_backtest_min_resolved_regression_rooms": self.strategy_auto_evolve_backtest_min_resolved_regression_rooms,
            "strategy_auto_evolve_backtest_min_candidate_trades": self.strategy_auto_evolve_backtest_min_candidate_trades,
            "strategy_auto_evolve_assignment_min_improvement_bps": self.strategy_auto_evolve_assignment_min_improvement_bps,
            "strategy_auto_evolve_per_city_max_negative_delta_bps": self.strategy_auto_evolve_per_city_max_negative_delta_bps,
            "strategy_auto_evolve_greenfield_min_win_rate_bps": self.strategy_auto_evolve_greenfield_min_win_rate_bps,
            "strategy_auto_evolve_greenfield_min_resolved_trades": self.strategy_auto_evolve_greenfield_min_resolved_trades,
            "strategy_auto_evolve_incumbent_health_win_rate_floor_bps": self.strategy_auto_evolve_incumbent_health_win_rate_floor_bps,
            "strategy_auto_evolve_watchdog_win_rate_degradation_bps": self.strategy_auto_evolve_watchdog_win_rate_degradation_bps,
            "strategy_auto_evolve_watchdog_min_resolved_live_fills": self.strategy_auto_evolve_watchdog_min_resolved_live_fills,
        }
        for field_name, value in non_negative_fields.items():
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")

        positive_fields = {
            "strategy_auto_evolve_max_run_age_seconds": self.strategy_auto_evolve_max_run_age_seconds,
            "strategy_auto_evolve_accept_max_run_age_seconds": self.strategy_auto_evolve_accept_max_run_age_seconds,
        }
        for field_name, value in positive_fields.items():
            if value <= 0:
                raise ValueError(f"{field_name} must be positive")

        if self.strategy_auto_evolve_reference_strategy_name is not None:
            reference_strategy_name = self.strategy_auto_evolve_reference_strategy_name.strip()
            self.strategy_auto_evolve_reference_strategy_name = reference_strategy_name or None
        if self.strategy_auto_evolve_reference_run_id is not None:
            reference_run_id = self.strategy_auto_evolve_reference_run_id.strip()
            self.strategy_auto_evolve_reference_run_id = reference_run_id or None
        self.strategy_corpus_excluded_date_ranges = self.strategy_corpus_excluded_date_ranges.strip()
        if self.strategy_corpus_excluded_date_ranges:
            for raw_range in self.strategy_corpus_excluded_date_ranges.split(","):
                bounds = raw_range.strip().split("/")
                if len(bounds) != 2:
                    raise ValueError("strategy_corpus_excluded_date_ranges must use YYYY-MM-DD/YYYY-MM-DD ranges")
                try:
                    start = date.fromisoformat(bounds[0].strip())
                    end = date.fromisoformat(bounds[1].strip())
                except ValueError as exc:
                    raise ValueError(
                        "strategy_corpus_excluded_date_ranges must use YYYY-MM-DD/YYYY-MM-DD ranges"
                    ) from exc
                if start > end:
                    raise ValueError("strategy_corpus_excluded_date_ranges start must be <= end")
        return self

    def model_post_init(self, __context: object) -> None:
        if self.database_url:
            return
        auth = quote(self.postgres_user, safe="")
        if self.postgres_password:
            auth = f"{auth}:{quote(self.postgres_password, safe='')}"
        self.database_url = (
            f"postgresql+asyncpg://{auth}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def secondary_database_url(self) -> str | None:
        if not self.postgres_secondary_host:
            return None
        auth = quote(self.postgres_user, safe="")
        if self.postgres_password:
            auth = f"{auth}:{quote(self.postgres_password, safe='')}"
        return f"postgresql+asyncpg://{auth}@{self.postgres_secondary_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def kalshi_rest_base_url(self) -> str:
        if self._is_live_kalshi_env:
            return "https://api.elections.kalshi.com/trade-api/v2"
        return "https://demo-api.kalshi.co/trade-api/v2"

    @property
    def kalshi_websocket_url(self) -> str:
        if self._is_live_kalshi_env:
            return "wss://api.elections.kalshi.com/trade-api/ws/v2"
        return "wss://demo-api.kalshi.co/trade-api/ws/v2"

    @property
    def _is_live_kalshi_env(self) -> bool:
        return str(self.kalshi_env or "").strip().lower() in {"production", "prod", "live"}

    @property
    def weather_market_map_file(self) -> Path:
        return Path(self.weather_market_map_path)

    @property
    def execution_enabled(self) -> bool:
        return not self.app_shadow_mode

    @property
    def web_auth_allowed_registration_email_set(self) -> set[str]:
        return {
            item.strip().lower()
            for item in self.web_auth_allowed_registration_emails.split(",")
            if item.strip()
        }

    @property
    def web_site_urls(self) -> dict[str, str]:
        return {
            "demo": f"https://{self.web_demo_host}",
            "production": f"https://{self.web_production_host}",
            "strategies": f"https://{self.web_strategies_host}",
        }

    def api_key_id(self, write: bool) -> str | None:
        direct = self.kalshi_write_api_key_id if write else self.kalshi_read_api_key_id
        if direct:
            return direct
        if self._is_live_kalshi_env:
            return self.live_kalshi_api_key
        return self.demo_kalshi_api_key

    def key_path(self, write: bool) -> Path | None:
        raw = self.kalshi_write_private_key_path if write else self.kalshi_read_private_key_path
        if raw:
            return Path(raw)
        env_specific = None
        if self._is_live_kalshi_env:
            env_specific = self.live_kalshi_write_private_key_path if write else self.live_kalshi_read_private_key_path
            if env_specific is None and write:
                env_specific = self.live_kalshi_read_private_key_path
        else:
            env_specific = self.demo_kalshi_write_private_key_path if write else self.demo_kalshi_read_private_key_path
            if env_specific is None and write:
                env_specific = self.demo_kalshi_read_private_key_path
        if env_specific:
            return Path(env_specific)
        fallback = Path("Kalshi-1.txt")
        return fallback if fallback.exists() else None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
