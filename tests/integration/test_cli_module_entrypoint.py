from __future__ import annotations

import os
import subprocess
import sys


def test_python_module_cli_entrypoint_runs_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "kalshi-bot-cli" in result.stdout


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
