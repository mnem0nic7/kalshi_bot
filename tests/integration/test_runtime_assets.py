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
