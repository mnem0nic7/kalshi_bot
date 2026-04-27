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
    assert "http://127.0.0.1:8000/readyz" in compose_text
    assert "kalshi_bot.cli\", \"health-check\", \"daemon" in compose_text
    assert "caddy" in compose_text
    assert "${HTTP_PORT:-80}:80" in compose_text
    assert "${HTTPS_PORT:-443}:443" in compose_text


def test_runtime_scripts_rebuild_migrate_image_before_using_it() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    watchdog = Path("infra/scripts/watchdog-run-once.sh").read_text(encoding="utf-8")

    assert 'docker compose -f "${compose_file}" ${compose_env_file} build "migrate_${env_name}" >/dev/null' in start_stack
    assert "run_migrate" in start_stack
    assert 'docker compose -f "${compose_file}" build "migrate_${env_name}" >/dev/null' in watchdog


def test_runtime_scripts_refresh_caddy_after_app_recreate() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")

    assert 'wait_for_service_health app_demo_blue 180' in start_stack
    assert 'wait_for_service_health app_demo_green 180' in start_stack
    assert 'wait_for_service_health app_production_blue 180' in start_stack
    assert 'wait_for_service_health app_production_green 180' in start_stack
    assert 'wait_for_service_health web_demo 180' in start_stack
    assert 'wait_for_service_health web_production 180' in start_stack
    assert 'wait_for_service_health web_strategies 180' in start_stack
    assert 'docker compose -f "${compose_file}" ${compose_env_file} up -d --force-recreate caddy' in start_stack
    assert 'app_${env_name}_${color}' in restart_color
    assert 'daemon_${env_name}_${color}' in restart_color
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

    assert 'infra/scripts/run-self-improve.sh status")' in workflow_text
    assert "APP_SERVICE=app_demo_blue" in workflow_text
    assert "APP_SERVICE=app_blue" not in workflow_text
    assert "not_ready_for_critique" in workflow_text
    assert "ready_for_critique" in workflow_text
    assert "missing_indicators" in workflow_text
    assert "exit 0" in workflow_text


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
    assert "promotion_gates" in autonomy
    assert "drift_watcher" in autonomy
    assert "learned_head" in autonomy
    assert "online_calibrator" in autonomy
    assert "nws_discussion_parser" in autonomy
    assert "LLM_TRADING_ENABLED=false" in architecture
    assert "climatology priors" in architecture
    assert "source health logs" in architecture
    assert "parameter packs" in architecture
    assert "zero-weight fallback" in architecture
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
