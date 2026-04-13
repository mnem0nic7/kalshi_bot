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
    assert "http://127.0.0.1:8000/readyz" in compose_text
    assert "kalshi_bot.cli\", \"health-check\", \"daemon" in compose_text


def test_runtime_scripts_rebuild_migrate_image_before_using_it() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    watchdog = Path("infra/scripts/watchdog-run-once.sh").read_text(encoding="utf-8")

    assert 'docker compose -f "${compose_file}" build migrate >/dev/null' in start_stack
    assert "run_migrate" in start_stack
    assert 'docker compose -f "${compose_file}" build migrate >/dev/null' in watchdog


def test_runtime_scripts_refresh_nginx_after_app_recreate() -> None:
    start_stack = Path("infra/scripts/start-stack.sh").read_text(encoding="utf-8")
    restart_color = Path("infra/scripts/restart-color.sh").read_text(encoding="utf-8")

    assert 'wait_for_service_health app_blue 180' in start_stack
    assert 'wait_for_service_health app_green 180' in start_stack
    assert 'docker compose -f "${compose_file}" up -d --force-recreate nginx' in start_stack
    assert 'docker compose -f "${compose_file}" up -d --no-deps --force-recreate nginx' in restart_color
