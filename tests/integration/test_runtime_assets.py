from __future__ import annotations

from pathlib import Path


def test_systemd_assets_use_workspace_path_and_watchdog_timer() -> None:
    compose_service = Path("infra/systemd/kalshi-bot-compose.service").read_text(encoding="utf-8")
    watchdog_service = Path("infra/systemd/kalshi-bot-watchdog.service").read_text(encoding="utf-8")
    watchdog_timer = Path("infra/systemd/kalshi-bot-watchdog.timer").read_text(encoding="utf-8")

    assert "WorkingDirectory=/workspace/kalshi_bot" in compose_service
    assert "./infra/scripts/start-stack.sh systemd_boot" in compose_service
    assert "./infra/scripts/watchdog-run-once.sh" in watchdog_service
    assert "OnUnitActiveSec=1min" in watchdog_timer


def test_compose_file_declares_service_healthchecks() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")

    assert "healthcheck:" in compose_text
    assert "app_demo_blue:" in compose_text
    assert "app_production_blue:" in compose_text
    assert "daemon_demo_blue:" in compose_text
    assert "daemon_production_blue:" in compose_text
    assert "http://127.0.0.1:8000/healthz" in compose_text
    assert "kalshi_bot.docker_healthcheck\", \"daemon" in compose_text
    assert "caddy" in compose_text
    assert "${HTTP_PORT:-80}:80" in compose_text
    assert "${HTTPS_PORT:-443}:443" in compose_text


def test_runtime_scripts_can_disable_generic_daemon_pairs() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")
    blue_green = Path("scripts/blue_green_redeploy.sh").read_text(encoding="utf-8")

    assert "ENABLE_DEMO_DAEMON" in start_stack
    assert "ENABLE_PRODUCTION_DAEMON" in start_stack
    assert "ENABLE_DEMO_DAEMON" in restart_color
    assert "ENABLE_PRODUCTION_DAEMON" in restart_color
    assert "ENABLE_DEMO_DAEMON" in blue_green
    assert "ENABLE_PRODUCTION_DAEMON" in blue_green


def test_runtime_scripts_align_btc_touch_singleton_to_active_color() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")
    blue_green = Path("scripts/blue_green_redeploy.sh").read_text(encoding="utf-8")

    assert "active_color_for_env()" in start_stack
    assert 'export CRYPTO_BTC15M_TOUCH20_APP_COLOR="${production_active_color}"' in start_stack
    assert 'export CRYPTO_1H_TOUCH20_APP_COLOR="${production_active_color}"' in start_stack
    assert 'export CRYPTO_CURRENT_APP_COLOR="${production_active_color}"' in start_stack
    assert 'export CRYPTO_1H_CURRENT_APP_COLOR="${production_active_color}"' in start_stack
    assert "active_color_for_env()" in restart_color
    assert 'export CRYPTO_BTC15M_TOUCH20_APP_COLOR="${production_active_color}"' in restart_color
    assert 'export CRYPTO_1H_TOUCH20_APP_COLOR="${production_active_color}"' in restart_color
    assert 'export CRYPTO_CURRENT_APP_COLOR="${production_active_color}"' in restart_color
    assert 'export CRYPTO_1H_CURRENT_APP_COLOR="${production_active_color}"' in restart_color
    assert 'export CRYPTO_BTC15M_TOUCH20_APP_COLOR="${TARGET}"' in blue_green
    assert 'export CRYPTO_1H_TOUCH20_APP_COLOR="${TARGET}"' in blue_green
    assert 'export CRYPTO_CURRENT_APP_COLOR="${TARGET}"' in blue_green
    assert 'export CRYPTO_1H_CURRENT_APP_COLOR="${TARGET}"' in blue_green


def test_btc_current_collector_refreshes_settled_labels() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")
    env_example = Path(".env.example").read_text(encoding="utf-8")
    env_lines = set(env_example.splitlines())

    assert "CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS:-0.50" in compose_text
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.50" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_MAX_CONTRACT_PRICE_DOLLARS=0.50" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE=0.458" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_MIN_RULE_SCORE=0.458" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT=0.30" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE=600" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.20" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0005" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_BUCKET_PRICE_BAND_CENTS=10" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_STOP_LOSS_PCT=0.30" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_MIN_SECONDS_TO_CLOSE=600" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_MIN_CONTRACT_PRICE_DOLLARS=0.20" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_MIN_ALIGNED_MOMENTUM=0.0005" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_BUCKET_PRICE_BAND_CENTS=10" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_EVERY_CYCLES" in compose_text
    assert "CRYPTO_BTC15M_TOUCH20_BUCKET_PRICE_BAND_CENTS" in compose_text
    assert 'assets="$${CRYPTO_15M_TOUCH20_RULES_ASSETS:-BTC}"' in compose_text
    assert 'replay_gate_every="$${CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_EVERY_CYCLES:-0}"' in compose_text
    assert '--days "$${CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_DAYS:-30}"' in compose_text
    assert 'sleep "$${CRYPTO_BTC15M_TOUCH20_LOOP_INTERVAL_SECONDS:-15}"' in compose_text
    assert "crypto-non-model-touch20 replay" in compose_text
    assert "crypto-non-model-touch20 gate" in compose_text
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_EVERY_CYCLES=0" in env_lines
    assert "PRODUCTION_CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_DAYS=30" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_EVERY_CYCLES=0" in env_lines
    assert "CRYPTO_BTC15M_TOUCH20_REPLAY_GATE_DAYS=30" in env_lines
    assert "CRYPTO_CURRENT_15M_SETTLED_EVERY_CYCLES" in compose_text
    assert "CRYPTO_CURRENT_15M_SETTLED_DAYS" in compose_text
    assert "crypto-history collect-settled" in compose_text
    assert "--skip-candles" in compose_text
    assert "--skip-quality" in compose_text
    assert "CRYPTO_CURRENT_15M_SETTLED_EVERY_CYCLES=20" in env_lines
    assert "CRYPTO_CURRENT_15M_SETTLED_DAYS=3" in env_lines


def test_compose_declares_opt_in_crypto_1h_touch20_runtime() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")
    env_example = Path(".env.example").read_text(encoding="utf-8")
    env_lines = set(env_example.splitlines())
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")
    blue_green = Path("scripts/blue_green_redeploy.sh").read_text(encoding="utf-8")

    assert "crypto_current_1h_production:" in compose_text
    assert "CRYPTO_CURRENT_1H_ASSETS" in compose_text
    assert 'assets="$${CRYPTO_CURRENT_1H_ASSETS:-$${PRODUCTION_CRYPTO_CURRENT_1H_ASSETS:-BTC,HYPE,ETH,BNB,SOL,DOGE,XRP}}"' in compose_text
    assert 'CRYPTO_1H_TOUCH20_ASSET_SETTINGS: "${PRODUCTION_CRYPTO_1H_TOUCH20_ASSET_SETTINGS:-{}}"' in compose_text
    assert "CRYPTO_1H_TOUCH20_RULES_ASSETS: ${PRODUCTION_CRYPTO_1H_TOUCH20_RULES_ASSETS:-BTC,HYPE,ETH,BNB,SOL,DOGE,XRP}" in compose_text
    assert 'settled_every="$${CRYPTO_1H_CURRENT_SETTLED_EVERY_CYCLES:-0}"' in compose_text
    assert '--days "$${CRYPTO_1H_CURRENT_SETTLED_DAYS:-2}"' in compose_text
    assert 'sleep "$${CRYPTO_1H_CURRENT_INTERVAL_SECONDS:-15}"' in compose_text
    assert "PRODUCTION_CRYPTO_CURRENT_1H_ASSETS=BTC,HYPE,ETH,BNB,SOL,DOGE,XRP" in env_lines
    assert "crypto_non_model_1h_touch20_production:" in compose_text
    assert "--frequency 1h" in compose_text
    assert 'assets="$${CRYPTO_1H_TOUCH20_RULES_ASSETS:-BTC,HYPE,ETH,BNB,SOL,DOGE,XRP}"' in compose_text
    assert "CRYPTO_1H_TOUCH20_REPLAY_GATE_EVERY_CYCLES" in compose_text
    assert "crypto-non-model-touch20 replay" in compose_text
    assert "crypto-non-model-touch20 gate" in compose_text
    assert "PRODUCTION_CRYPTO_1H_TOUCH20_RULES_ENABLED=false" in env_lines
    assert "PRODUCTION_CRYPTO_1H_TOUCH20_RULES_TRADING_ENABLED=false" in env_lines
    assert "PRODUCTION_CRYPTO_1H_TOUCH20_REPLAY_GATE_EVERY_CYCLES=0" in env_lines
    assert "CRYPTO_1H_TOUCH20_RULES_ENABLED=false" in env_lines
    assert "CRYPTO_1H_TOUCH20_BUCKET_TIME_BAND_MINUTES=15" in env_lines
    assert "ENABLE_CRYPTO_CURRENT_1H_CONTAINER=false" in env_lines
    assert "ENABLE_CRYPTO_1H_TOUCH20_CONTAINER=false" in env_lines
    assert "CRYPTO_1H_CURRENT_INTERVAL_SECONDS=15" in env_lines
    assert "CRYPTO_1H_CURRENT_SETTLED_EVERY_CYCLES=0" in env_lines
    assert "CRYPTO_1H_CURRENT_SETTLED_DAYS=2" in env_lines
    assert "CRYPTO_1H_CURRENT_REPLAY_GATE_ENABLED=false" in env_lines
    assert "CRYPTO_1H_CURRENT_SETTLED_LABEL_PROPAGATION_ENABLED=false" in env_lines
    assert "runtime_services+=(crypto_current_1h_production)" in start_stack
    assert "runtime_services+=(crypto_non_model_1h_touch20_production)" in start_stack
    assert "ENABLE_CRYPTO_CURRENT_1H_CONTAINER" in restart_color
    assert "crypto_non_model_1h_touch20_production" in restart_color
    assert "crypto_current_1h_production" in blue_green
    assert "crypto_non_model_1h_touch20_production" in blue_green


def test_sync_web_color_can_disable_strategies_site_container() -> None:
    sync_web_color = Path("infra/scripts/sync-web-color.sh").read_text(encoding="utf-8")
    env_example = Path(".env.example").read_text(encoding="utf-8")

    assert "ENABLE_WEB_STRATEGIES_CONTAINER" in sync_web_color
    assert "ENABLE_WEB_STRATEGIES_CONTAINER=true" in env_example
    assert 'web_services+=("web_strategies")' in sync_web_color
    assert "stop web_strategies" in sync_web_color
    assert "rm -f web_strategies" in sync_web_color


def test_compose_declares_opt_in_crypto_1h_refresh_container() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")

    assert "crypto_1h_production:" in compose_text
    assert "--frequency 1h" in compose_text
    assert 'CRYPTO_AUTO_FREQUENCIES: "1h"' in compose_text
    assert '--settled-days "${CRYPTO_1H_SETTLED_DAYS:-7}"' in compose_text
    assert "scripts/crypto_live_path_refresh.sh" in compose_text
    assert "ENABLE_CRYPTO_1H_CONTAINER" in start_stack
    assert "runtime_services+=(crypto_1h_production)" in start_stack
    assert "COPY scripts/crypto_live_path_refresh.sh ./scripts/crypto_live_path_refresh.sh" in Path(
        "infra/docker/Dockerfile"
    ).read_text(encoding="utf-8")


def test_compose_declares_opt_in_crypto_1h_daemon_pair() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")

    assert "daemon_production_crypto_1h_blue:" in compose_text
    assert "daemon_production_crypto_1h_green:" in compose_text
    assert '"--crypto-only", "--heartbeat-role", "crypto_1h"' in compose_text
    assert "DAEMON_HEARTBEAT_ROLE: crypto_1h" in compose_text
    assert 'CRYPTO_AUTO_FREQUENCIES: "1h"' in compose_text
    assert "ENABLE_CRYPTO_1H_DAEMON" in start_stack
    assert "daemon_production_crypto_1h_blue daemon_production_crypto_1h_green" in start_stack
    assert "ENABLE_CRYPTO_1H_DAEMON" in restart_color
    assert 'runtime_services+=("daemon_production_crypto_1h_${color}")' in restart_color


def test_compose_uses_env_scoped_live_weather_switches() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")

    assert "APP_SHADOW_MODE: ${DEMO_APP_SHADOW_MODE:-true}" in compose_text
    assert "APP_SHADOW_MODE: ${PRODUCTION_APP_SHADOW_MODE:-true}" in compose_text
    assert "TRIGGER_ENABLE_AUTO_ROOMS: ${DEMO_TRIGGER_ENABLE_AUTO_ROOMS:-false}" in compose_text
    assert "TRIGGER_ENABLE_AUTO_ROOMS: ${PRODUCTION_TRIGGER_ENABLE_AUTO_ROOMS:-false}" in compose_text
    assert "STRATEGY_C_ENABLED: ${DEMO_STRATEGY_C_ENABLED:-false}" in compose_text
    assert "STRATEGY_C_SHADOW_ONLY: ${DEMO_STRATEGY_C_SHADOW_ONLY:-true}" in compose_text
    assert "STRATEGY_C_ENABLED: ${PRODUCTION_STRATEGY_C_ENABLED:-false}" in compose_text
    assert "STRATEGY_C_SHADOW_ONLY: ${PRODUCTION_STRATEGY_C_SHADOW_ONLY:-true}" in compose_text
    assert "CRYPTO_TRADING_ENABLED: ${DEMO_CRYPTO_TRADING_ENABLED:-false}" in compose_text
    assert "CRYPTO_TRADING_ENABLED: ${PRODUCTION_CRYPTO_TRADING_ENABLED:-false}" in compose_text
    assert "MONOTONICITY_ARB_ENABLED: ${MONOTONICITY_ARB_ENABLED:-false}" in compose_text
    assert "MONOTONICITY_ARB_SHADOW_ONLY: ${MONOTONICITY_ARB_SHADOW_ONLY:-true}" in compose_text


def test_compose_defaults_crypto_to_model_trained_replay_only() -> None:
    compose_text = Path("infra/docker-compose.yml").read_text(encoding="utf-8")
    env_example = Path(".env.example").read_text(encoding="utf-8")

    assert "CRYPTO_MODEL_TRAINED_REPLAY_ONLY: ${CRYPTO_MODEL_TRAINED_REPLAY_ONLY:-true}" in compose_text
    assert "CRYPTO_TOUCH_STRATEGY_ENABLED: ${CRYPTO_TOUCH_STRATEGY_ENABLED:-false}" in compose_text
    assert "CRYPTO_TOUCH_STRATEGY_ENABLED: ${PRODUCTION_CRYPTO_TOUCH_STRATEGY_ENABLED:-false}" in compose_text
    assert "CRYPTO_1H_TOUCH_STRATEGY_ENABLED: ${PRODUCTION_CRYPTO_1H_TOUCH_STRATEGY_ENABLED:-false}" in compose_text
    assert "CRYPTO_1H_TOUCH_TAKE_PROFIT_PCT: ${PRODUCTION_CRYPTO_1H_TOUCH_TAKE_PROFIT_PCT:-0.20}" in compose_text
    assert "CRYPTO_TAKE_PROFIT_THRESHOLD_PCT: ${PRODUCTION_CRYPTO_TAKE_PROFIT_THRESHOLD_PCT:-0.20}" in compose_text
    assert "CRYPTO_TRAINING_PREFLIGHT_ENABLED: ${PRODUCTION_CRYPTO_TRAINING_PREFLIGHT_ENABLED:-true}" in compose_text
    assert (
        "CRYPTO_TRAINING_FEATURE_STORE_ENABLED: ${PRODUCTION_CRYPTO_TRAINING_FEATURE_STORE_ENABLED:-true}"
        in compose_text
    )
    assert "CRYPTO_MODEL_TRAINED_REPLAY_ONLY=true" in env_example
    assert "PRODUCTION_CRYPTO_TOUCH_STRATEGY_ENABLED=false" in env_example
    assert "PRODUCTION_CRYPTO_1H_TOUCH_STRATEGY_ENABLED=false" in env_example
    assert "PRODUCTION_CRYPTO_TAKE_PROFIT_THRESHOLD_PCT=0.20" in env_example
    assert "PRODUCTION_CRYPTO_TRAINING_PREFLIGHT_ENABLED=true" in env_example
    assert "PRODUCTION_CRYPTO_TRAINING_FEATURE_STORE_ENABLED=true" in env_example


def test_runtime_scripts_rebuild_migrate_image_before_using_it() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    watchdog = Path("infra/scripts/watchdog-run-once.sh").read_text(encoding="utf-8")
    dockerfile = Path("infra/docker/Dockerfile").read_text(encoding="utf-8")
    dockerignore = Path(".dockerignore").read_text(encoding="utf-8")

    assert 'docker compose -f "${compose_file}" ${compose_env_file} build migrate_demo >/dev/null' in start_stack
    assert "run_migrate" in start_stack
    assert 'docker compose -f "${compose_file}" ${compose_env_file} build "migrate_${env_name}" >/dev/null' in watchdog
    assert "COPY infra/config ./infra/config" in dockerfile
    assert "infra/*" in dockerignore
    assert "!infra/config/" in dockerignore
    assert "!infra/config/**" in dockerignore


def test_runtime_scripts_refresh_caddy_after_app_recreate() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")
    sync_web_color = Path("infra/scripts/sync-web-color.sh").read_text(encoding="utf-8")

    assert "wait_for_services_health 180" in start_stack
    assert "app_demo_blue app_demo_green app_production_blue app_production_green" in start_stack
    assert "infra/scripts/sync-web-color.sh all" in start_stack
    assert 'web_services+=("web_demo")' in sync_web_color
    assert 'web_services+=("web_production")' in sync_web_color
    assert 'web_services+=("web_strategies")' in sync_web_color
    assert 'docker compose -f "${compose_file}" ${compose_env_file} up -d --no-deps --force-recreate caddy' in start_stack
    assert 'app_${env_name}_${color}' in restart_color
    assert 'daemon_${env_name}_${color}' in restart_color
    assert 'if [[ "${refresh_caddy}" == "true" ]]; then' in restart_color
    assert 'docker compose -f "${compose_file}" ${compose_env_file} up -d --no-deps --force-recreate caddy' in restart_color


def test_github_vps_workflows_use_portable_ssh_options() -> None:
    workflow_paths = [
        Path(".github/workflows/bootstrap-vps.yml"),
        Path(".github/workflows/redeploy.yml"),
        Path(".github/workflows/rollback-agent-pack.yml"),
        Path(".github/workflows/self-improve.yml"),
        Path(".github/workflows/sync-gemini-runtime.yml"),
    ]

    for workflow_path in workflow_paths:
        workflow_text = workflow_path.read_text(encoding="utf-8")
        assert "DEPLOY_SSH_PORT" in workflow_text, workflow_path
        assert "DEPLOY_SSH_ADDRESS_FAMILY" in workflow_text, workflow_path
        assert "StrictHostKeyChecking=accept-new" in workflow_text, workflow_path
        assert "-p \"${DEPLOY_SSH_PORT}\"" in workflow_text, workflow_path
        assert "AddressFamily=${DEPLOY_SSH_ADDRESS_FAMILY}" in workflow_text, workflow_path


def test_self_improve_workflow_preflights_training_readiness() -> None:
    workflow_text = Path(".github/workflows/self-improve.yml").read_text(encoding="utf-8")

    assert "target:" in workflow_text
    assert "parameter_pack" in workflow_text
    assert "agent_pack_legacy" in workflow_text
    assert 'target="${INPUT_TARGET:-parameter_pack}"' in workflow_text
    assert "infra/scripts/run-parameter-pack.sh hard-caps" in workflow_text
    assert "infra/scripts/run-parameter-pack.sh status" in workflow_text
    assert "not_ready_for_parameter_pack_replay" in workflow_text
    assert 'infra/scripts/run-self-improve.sh status")' in workflow_text
    assert "APP_SERVICE=app_demo_blue" in workflow_text
    assert "APP_SERVICE=app_blue" not in workflow_text
    assert "not_ready_for_critique" in workflow_text
    assert "ready_for_critique" in workflow_text
    assert "missing_indicators" in workflow_text
    assert "exit 0" in workflow_text


def test_parameter_pack_wrapper_targets_parameter_pack_cli() -> None:
    script = Path("infra/scripts/run-parameter-pack.sh")
    script_text = script.read_text(encoding="utf-8")

    assert script.stat().st_mode & 0o111
    assert "usage: run-parameter-pack.sh" in script_text
    assert 'service="${APP_SERVICE:-app_demo_blue}"' in script_text
    assert 'python -m kalshi_bot.cli parameter-pack "$@"' in script_text


def test_self_improve_workflow_preserves_hard_failures() -> None:
    workflow_text = Path(".github/workflows/self-improve.yml").read_text(encoding="utf-8")

    assert "Candidate version missing from critique output" in workflow_text
    assert "Evaluation run id missing from eval output" in workflow_text
    assert "Inactive color missing from promote output" in workflow_text
    assert 'jq -e \'type == "object" and (.passed | type == "boolean")\' eval.json' in workflow_text


def test_self_improve_workflow_requires_promotion_readiness_before_staging() -> None:
    workflow_text = Path(".github/workflows/self-improve.yml").read_text(encoding="utf-8")

    assert "not_ready_for_promotion" in workflow_text
    assert "promotion-status.json" in workflow_text
    assert 'ready_for_promotion="$(jq -r \'.training_readiness.ready_for_promotion\' promotion-status.json)"' in workflow_text


def test_deterministic_autonomy_docs_anchor_phase_zero_trace_replay() -> None:
    autonomy = Path("docs/deterministic_autonomy_plan.md").read_text(encoding="utf-8")
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")
    self_improve = Path("docs/self_improve.md").read_text(encoding="utf-8")
    strategy = Path("docs/strategy/weather-temp-taker.md").read_text(encoding="utf-8")

    assert "Score: **8.2/10**" in autonomy
    assert "Phase 0" in autonomy
    assert "decision_traces" in autonomy
    assert "adapter-first" in autonomy
    assert "probability_engine" in autonomy
    assert "forecast_snapshots" in autonomy
    assert "risk.sizing" in autonomy
    assert "exit_score" in autonomy
    assert "source_health" in autonomy
    assert "source_health_logs" in autonomy
    assert "pause_new_entries" in autonomy
    assert "parameter_pack" in autonomy
    assert "parameter_packs" in autonomy
    assert "hard_caps.yaml" in autonomy
    assert "parameter_pack_default.yaml" in autonomy
    assert "parameter_search" in autonomy
    assert "promotion_gates" in autonomy
    assert "drift_watcher" in autonomy
    assert "parameter-pack stage" in autonomy
    assert "parameter-pack status" in autonomy
    assert "parameter-pack drift" in autonomy
    assert "parameter-pack grid" in autonomy
    assert "parameter-pack select" in autonomy
    assert "parameter-pack record-starvation" in autonomy
    assert "promotion_starvation" in autonomy
    assert "`promotion_starvation` checkpoint" in autonomy
    assert "warning/error ops event" in autonomy
    assert "clears any existing starvation streak" in autonomy
    assert "stalled" in autonomy
    assert "parameter-pack canary" in autonomy
    assert "parameter-pack promote-staged" in autonomy
    assert "parameter-pack rollback-staged" in autonomy
    assert "deployment_control.notes.parameter_packs" in autonomy
    assert "learned_head" in autonomy
    assert "holdout gate for nonzero learned weight" in autonomy
    assert "online_calibrator" in autonomy
    assert "nws_discussion_parser" in autonomy
    assert "shadow availability/schema evidence" in autonomy
    assert "LLM_TRADING_ENABLED=false" in architecture
    assert "climatology priors" in architecture
    assert "source health logs" in architecture
    assert "parameter packs" in architecture
    assert "hard-cap config hashes" in architecture
    assert "zero-weight fallback" in architecture
    assert "parameter-pack hard-caps" in self_improve
    assert "parameter-pack validate candidate-pack.json --strict" in self_improve
    assert "parameter-pack validate" in self_improve
    assert "parameter-pack gate" in self_improve
    assert "--hard-caps infra/config/hard_caps.yaml" in self_improve
    assert "parameter-pack stage" in self_improve
    assert "parameter-pack status" in self_improve
    assert "parameter-pack drift" in self_improve
    assert "parameter-pack grid" in self_improve
    assert "parameter-pack select" in self_improve
    assert "parameter-pack record-starvation" in self_improve
    assert "promotion_starvation" in self_improve
    assert "checkpoint state" in self_improve
    assert "ops event and checkpoint" in self_improve
    assert "clears any existing starvation streak" in self_improve
    assert "parameter-pack learned-gate" in self_improve
    assert "parameter-pack nws-parser-gate" in self_improve
    assert "does not mutate runtime state" in self_improve
    assert "SELF_IMPROVE_CANARY_MAX_SECONDS" in self_improve
    assert "parameter-pack canary" in self_improve
    assert "parameter-pack promote-staged" in self_improve
    assert "operator-only" in self_improve
    assert "parameter-pack rollback-staged" in self_improve
    assert "risk-engine bypasses" in self_improve
    assert "not an activator" in self_improve
    assert "max_drawdown_pct" in self_improve
    assert "not_ready_for_parameter_pack_replay" in self_improve
    assert "run-parameter-pack.sh status" in self_improve
    assert "decision-trace replay" in strategy


def test_rollback_agent_pack_targets_existing_demo_app_service() -> None:
    workflow_text = Path(".github/workflows/rollback-agent-pack.yml").read_text(encoding="utf-8")

    assert "APP_SERVICE=app_demo_blue" in workflow_text
    assert "APP_SERVICE=app_blue" not in workflow_text


def test_promote_script_targets_env_scoped_postgres_and_control_row() -> None:
    promote = Path("infra/scripts/promote.sh").read_text(encoding="utf-8")

    assert "usage: promote.sh <demo|production> <blue|green>" in promote
    assert 'postgres_service="postgres_${env_name}"' in promote
    assert "INSERT INTO deployment_control" in promote
    assert "TRUE, NULL, '{}', NOW()" in promote
    assert "ON CONFLICT (id) DO UPDATE" in promote
    assert "active_color=EXCLUDED.active_color" in promote
    assert "exec -T postgres \\" not in promote
    assert "id='default'" not in promote
