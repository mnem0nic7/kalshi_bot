from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from kalshi_bot import cli as cli_module
from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


def test_python_module_cli_entrypoint_runs_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "kalshi-bot-cli" in result.stdout


def test_python_module_cli_exposes_strategy_promotion_watchdog_evaluate_and_resolve() -> None:
    top_level = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    command_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "strategy-promotion-watchdog", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    evaluate_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "strategy-promotion-watchdog", "evaluate", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    resolve_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "strategy-promotion-watchdog", "resolve", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert top_level.returncode == 0
    assert "strategy-promotion-watchdog" in top_level.stdout
    assert command_help.returncode == 0
    assert "evaluate" in command_help.stdout
    assert "resolve" in command_help.stdout
    assert evaluate_help.returncode == 0
    assert "--promotion-id" in evaluate_help.stdout
    assert resolve_help.returncode == 0
    assert "--promotion-id" in resolve_help.stdout
    assert "--action" in resolve_help.stdout
    assert "--resolved-by" in resolve_help.stdout
    assert "--note" in resolve_help.stdout


def test_python_module_cli_exposes_strategy_promotion_secondary_sync_sweep() -> None:
    top_level = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    command_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "strategy-promotion-secondary-sync", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    sweep_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "strategy-promotion-secondary-sync", "sweep", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert top_level.returncode == 0
    assert "strategy-promotion-secondary-sync" in top_level.stdout
    assert command_help.returncode == 0
    assert "sweep" in command_help.stdout
    assert sweep_help.returncode == 0
    assert "--limit" in sweep_help.stdout
    assert "--source" in sweep_help.stdout


def test_python_module_cli_entrypoint_reports_operator_errors_cleanly(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/cli.db"
    env["APP_AUTO_INIT_DB"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "self-improve",
            "eval",
            "--candidate-version",
            "builtin-gemini-v1",
            "--limit",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    assert '"error"' in result.stderr
    assert "Training corpus is not ready for evaluation" in result.stderr
    assert "Traceback" not in result.stderr


def test_trading_audit_cli_json_smoke(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/trading-audit-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "trading-audit",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = result.stdout
    assert '"audit"' in payload
    assert '"fill_summary"' in payload
    assert '"issues"' in payload
    assert '"read_only": true' in payload
    assert "Traceback" not in result.stderr


@pytest.mark.asyncio
async def test_decision_corpus_build_cli_passes_active_kalshi_env(monkeypatch, capsys) -> None:
    calls: list[dict[str, object]] = []

    class FakeDecisionCorpusService:
        async def build(self, **kwargs):
            calls.append(kwargs)
            return {"status": "dry_run", "kalshi_env": kwargs["kalshi_env"]}

    class FakeContainer:
        settings = SimpleNamespace(kalshi_env="live")
        decision_corpus_service = FakeDecisionCorpusService()

        async def close(self) -> None:
            return None

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    args = cli_module.build_parser().parse_args(
        [
            "decision-corpus",
            "build",
            "--date-from",
            "2026-04-20",
            "--date-to",
            "2026-04-21",
            "--dry-run",
        ]
    )

    exit_code = await cli_module._run_cli(args)

    assert exit_code == 0
    assert calls[0]["kalshi_env"] == "live"
    assert '"kalshi_env": "live"' in capsys.readouterr().out


def test_trade_analysis_cli_report_json_smoke(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/trade-analysis-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "trade-analysis",
            "report",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = result.stdout
    assert '"schema_version": "trade-analysis-v1"' in payload
    assert '"row_count"' in payload
    assert '"read_only": true' in payload
    assert "Traceback" not in result.stderr


@pytest.mark.asyncio
async def test_ignore_strategy_promotion_secondary_status_cli_updates_resolution_audit(tmp_path) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path}/promotion-ignore-cli.db"
    settings = Settings(database_url=database_url)
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    await init_models(engine)
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        promotion = await repo.create_strategy_promotion(
            promoted_strategy_name="moderate",
            previous_city_assignments={"KXHIGHNY": "aggressive"},
            new_city_assignments={"KXHIGHNY": "moderate"},
            secondary_sync_status="failed",
        )
        await session.commit()
        promotion_id = promotion.id
    await engine.dispose()

    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["APP_AUTO_INIT_DB"] = "true"
    env["KALSHI_ENV"] = "demo"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "ignore-strategy-promotion-secondary-status",
            "--promotion-id",
            str(promotion_id),
            "--field",
            "secondary_sync_status",
            "--resolved-by",
            "ops@example.com",
            "--note",
            "Operator reviewed secondary sync failure and accepted drift.",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert '"updated_count": 1' in result.stdout
    assert "Traceback" not in result.stderr

    verify_engine = create_engine(settings)
    verify_factory = create_session_factory(verify_engine)
    async with verify_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        updated = await repo.get_strategy_promotion(promotion_id)
    await verify_engine.dispose()

    assert updated is not None
    assert updated.secondary_sync_status == "ignored_by_operator"
    assert updated.secondary_sync_resolution is not None
    assert updated.secondary_sync_resolution["action"] == "ignored_by_operator"
    assert updated.secondary_sync_resolution["resolved_by"] == "ops@example.com"
    assert updated.secondary_sync_resolution["note"] == "Operator reviewed secondary sync failure and accepted drift."
    assert "resolved_at" in updated.secondary_sync_resolution


def test_ignore_strategy_promotion_secondary_status_bulk_requires_explicit_env(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/promotion-ignore-env-guard.db"
    env["APP_AUTO_INIT_DB"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "ignore-strategy-promotion-secondary-status",
            "--all",
            "--field",
            "secondary_sync_status",
            "--resolved-by",
            "ops@example.com",
            "--note",
            "Operator reviewed bulk secondary sync failures.",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    assert "Bulk secondary status ignore requires explicit --kalshi-env" in result.stderr
    assert "Traceback" not in result.stderr
