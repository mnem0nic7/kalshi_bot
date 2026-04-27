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

    weather_user_agent: str = "kalshi-bot/0.1 (ops@example.com)"
    weather_market_map_path: str = "docs/examples/weather_markets.example.yaml"

    llm_hosted_base_url: str = "https://api.openai.com/v1"
    llm_hosted_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("LLM_HOSTED_API_KEY", "OPENAI_API_KEY"),
    )
    llm_hosted_model: str = "gpt-5.4"
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
    active_agent_pack_version: str = "builtin-gemini-v1"
    llm_request_timeout_seconds: float = 30.0
    llm_trading_enabled: bool = False

    trigger_broken_book_retry_seconds: int = 30
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
    # calibration (see /api/strategies/calibration) confirms the fair-value
    # signal is well-calibrated. When on, the Kelly notional is capped by the
    # existing flat-percentage limits so this can only ever reduce risk.
    risk_edge_scaled_sizing_enabled: bool = False
    risk_edge_scaled_kelly_multiplier: float = 0.25  # quarter-Kelly
    # Per-strategy dollar-denominated hard-loss cap. Empty = no per-strategy cap.
    # Example env var value: '{"A": 500, "C": 100}' (JSON-parsed by pydantic-settings).
    risk_daily_loss_dollars_by_strategy: dict[str, float] = Field(default_factory=dict)
    stop_loss_threshold_pct: float = 0.10
    stop_loss_profit_protection_threshold_pct: float = 0.15
    stop_loss_reentry_cooldown_seconds: int = 14400
    stop_loss_momentum_reentry_window_seconds: int = 300
    stop_loss_submit_cooldown_seconds: int = 300
    stop_loss_check_interval_seconds: int = 60
    stop_loss_momentum_slope_threshold_cents_per_min: float = -0.2
    stop_loss_momentum_reentry_slope_threshold_cents_per_min: float = -0.2
    stop_loss_momentum_min_hold_minutes: int = 30
    # Step 3 momentum weight config keys (placeholder defaults; calibrated values written to DB checkpoint).
    # momentum_weight_scale_cents_per_min: the denominator in w = max(floor, 1 - slope_against/scale).
    # momentum_slope_veto_cents_per_min: hard veto gate; None = disabled until first calibration ships.
    # momentum_weight_floor: minimum weight applied to edge_effective_bps.
    # momentum_veto_staleness_gate: staleness_factor must exceed this before veto can fire.
    # momentum_weight_shadow_mode: when True, post-processor stamps analytics fields but does not
    #   enforce edge_effective_bps in eligibility decisions — enforcement falls back to raw edge.
    momentum_weight_scale_cents_per_min: float = 1.0
    momentum_slope_veto_cents_per_min: float | None = None
    momentum_weight_floor: float = 0.3
    momentum_veto_staleness_gate: float = 0.5
    momentum_weight_shadow_mode: bool = True
    # Phase 2 — nightly automation
    momentum_calibration_auto_enabled: bool = False
    momentum_calibration_nightly_hour_local: int = 2
    momentum_calibration_nightly_timezone: str = "America/Los_Angeles"
    momentum_calibration_nightly_lookback_days: int = 90
    # Phase 2 — tier thresholds
    momentum_calibration_tier1_max_delta_fraction: float = 0.10
    momentum_calibration_tier2_max_delta_fraction: float = 0.20
    momentum_calibration_tier1_max_ci_width_fraction: float = 0.30
    momentum_calibration_sanity_max_ci_width_fraction: float = 0.50
    momentum_calibration_tier1_auto_promote_enabled: bool = False
    # Phase 2 — coverage gate
    momentum_calibration_min_slope_coverage: float = 0.80
    momentum_calibration_recent_coverage_days: int = 7
    momentum_calibration_min_observations: int = 1000
    # Phase 2 — skip escalation
    momentum_calibration_skip_critical_threshold: int = 4
    risk_max_order_count_fp: float = 500.0
    risk_max_position_count_fp_per_ticker: float = 200.0
    risk_allow_position_add_ons: bool = False
    risk_safe_capital_reserve_ratio: float = 0.0
    risk_risky_capital_max_ratio: float = 0.0
    risk_stale_market_seconds: int = 60
    risk_stale_weather_seconds: int = 900
    risk_min_edge_bps: int = 500
    risk_max_credible_edge_bps: int = 5000
    # PENDING_CALIBRATION: raised from 0.60 to 0.80 based on N=3 winning trades
    # (AUS-T86/CHI-T78/SFO-T70) all having confidence ≥ 0.80 at entry. Unblocking
    # experiment: collect ≥ 30 settled trades across confidence deciles and verify
    # the win-rate cliff is at 0.80 and not lower.
    risk_min_confidence: float = 0.80
    risk_min_contract_price_dollars: float = 0.25
    # Probability distance from 50%: inside this band, require extra edge that ramps
    # linearly to risk_probability_midband_max_extra_edge_bps at fair_yes=0.50.
    # Set risk_min_probability_extremity_pct to 0.0 to disable.
    risk_min_probability_extremity_pct: float = 25.0
    risk_probability_midband_max_extra_edge_bps: int = 500
    # PENDING_CALIBRATION: 8.0°F boundary derived from N=3 winners (delta_f=10–13°F).
    # Unblocking experiment: collect ≥ 30 settled trades with |delta_f| 4–12°F and
    # verify loss rate drops materially above 8°F versus below.
    strategy_min_abs_delta_f: float = 8.0
    strategy_min_remaining_payout_bps: int = 300
    strategy_quality_edge_buffer_bps: int = 25
    sigma_lead_correction_enabled: bool = True

    # Strategy C adaptive polling cadence (ThresholdProximityMonitor, §4.1.4)
    strategy_c_cadence_idle_seconds: int = 3600
    strategy_c_cadence_approach_seconds: int = 900
    strategy_c_cadence_near_threshold_seconds: int = 150
    strategy_c_cadence_post_peak_seconds: int = 900
    strategy_c_near_threshold_margin_f: float = 2.0
    strategy_c_approach_margin_f: float = 5.0

    # Strategy C lock-confirmation gates (§4.1.4)
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

    # Addition 3: Monotonicity Arb Scanner (§4.3)
    monotonicity_arb_enabled: bool = False
    monotonicity_arb_shadow_only: bool = True
    # Live execution of the two-leg arb requires an atomic executor that can
    # place leg 1, place leg 2, and unwind leg 1 if leg 2 fails. That executor
    # is NOT built yet — see services/monotonicity_scanner.py docstring.
    # This flag is an explicit acknowledgement that the atomic path exists
    # before the risk gate will allow a non-shadow outcome. Flipping shadow_only
    # to False without this flag is rejected with 'risk_blocked', not silently
    # downgraded to shadow.
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
    research_web_max_results: int = 5
    research_web_max_queries: int = 2
    trigger_enable_auto_rooms: bool = False
    trigger_cooldown_seconds: int = 300
    trigger_price_move_bypass_bps: int = 1500
    trigger_max_spread_bps: int = 1200
    trigger_max_concurrent_rooms: int = 12
    trigger_active_room_stale_seconds: int = 1800
    daemon_reconcile_interval_seconds: int = 60
    daemon_reconcile_stale_kill_switch_seconds: int = 300
    daemon_heartbeat_interval_seconds: int = 60
    daemon_market_history_interval_seconds: int = 60
    daemon_market_history_retention_hours: int = 24
    daemon_memory_note_retention_days: int = 90
    daemon_start_with_reconcile: bool = True
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
    strategy_codex_nightly_enabled: bool = True
    strategy_codex_nightly_timezone: str = "America/Los_Angeles"
    strategy_codex_nightly_hour_local: int = 1
    strategy_auto_evolve_enabled: bool = True
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
        if self.kalshi_env == "production":
            return "https://api.elections.kalshi.com/trade-api/v2"
        return "https://demo-api.kalshi.co/trade-api/v2"

    @property
    def kalshi_websocket_url(self) -> str:
        if self.kalshi_env == "production":
            return "wss://api.elections.kalshi.com/trade-api/ws/v2"
        return "wss://demo-api.kalshi.co/trade-api/ws/v2"

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
        if self.kalshi_env == "production":
            return self.live_kalshi_api_key
        return self.demo_kalshi_api_key

    def key_path(self, write: bool) -> Path | None:
        raw = self.kalshi_write_private_key_path if write else self.kalshi_read_private_key_path
        if raw:
            return Path(raw)
        env_specific = None
        if self.kalshi_env == "production":
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
