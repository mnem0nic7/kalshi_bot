from __future__ import annotations

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
