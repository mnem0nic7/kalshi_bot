from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest

from kalshi_bot import cli as cli_module
from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.learning.parameter_pack import default_parameter_pack


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


def test_python_module_cli_exposes_decision_trace_show_and_replay() -> None:
    top_level = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    command_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "decision-trace", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert top_level.returncode == 0
    assert "decision-trace" in top_level.stdout
    assert command_help.returncode == 0
    assert "show" in command_help.stdout
    assert "replay" in command_help.stdout


def test_python_module_cli_exposes_crypto_history_status_and_autonomy_run_once() -> None:
    parser = cli_module.build_parser()

    history_args = parser.parse_args(["crypto-history", "status", "--kalshi-env", "production", "--frequency", "15m", "--json"])
    history_bootstrap_args = parser.parse_args(
        ["crypto-history", "bootstrap", "--kalshi-env", "production", "--frequency", "15m", "--assets", "XRP"]
    )
    spot_args = parser.parse_args(["crypto-spot", "status", "--kalshi-env", "production", "--frequency", "15m", "--assets", "BTC", "--json"])
    model_args = parser.parse_args(["crypto-model", "train", "--kalshi-env", "production", "--frequency", "15m"])
    replay_args = parser.parse_args(["crypto-replay", "gate", "--kalshi-env", "production", "--frequency", "15m"])
    status_args = parser.parse_args(["crypto-status", "--kalshi-env", "production"])
    autonomy_args = parser.parse_args(["crypto-autonomy", "run-once", "--kalshi-env", "production", "--frequency", "15m", "--json"])
    live_path_status_args = parser.parse_args(
        ["crypto-live-path", "status", "--kalshi-env", "production", "--frequency", "15m", "--assets", "BTC", "ETH", "--json"]
    )
    live_path_refresh_args = parser.parse_args(
        [
            "crypto-live-path",
            "refresh",
            "--kalshi-env",
            "production",
            "--frequency",
            "15m",
            "--history-days",
            "2",
            "--spot-days",
            "2",
            "--replay-days",
            "30",
            "--assets",
            "XRP",
            "--json",
        ]
    )
    model_quality_args = parser.parse_args(
        ["model-quality", "status", "--kalshi-env", "demo", "--domain", "all", "--json"]
    )

    assert history_args.command == "crypto-history"
    assert history_args.crypto_history_command == "status"
    assert history_args.kalshi_env == "production"
    assert history_args.frequency == "15m"
    assert history_bootstrap_args.crypto_history_command == "bootstrap"
    assert history_bootstrap_args.assets == ["XRP"]
    assert spot_args.command == "crypto-spot"
    assert spot_args.crypto_spot_command == "status"
    assert spot_args.kalshi_env == "production"
    assert spot_args.assets == ["BTC"]
    assert model_args.command == "crypto-model"
    assert model_args.kalshi_env == "production"
    assert replay_args.command == "crypto-replay"
    assert replay_args.kalshi_env == "production"
    assert status_args.command == "crypto-status"
    assert status_args.kalshi_env == "production"
    assert autonomy_args.command == "crypto-autonomy"
    assert autonomy_args.crypto_autonomy_command == "run-once"
    assert autonomy_args.kalshi_env == "production"
    assert live_path_status_args.command == "crypto-live-path"
    assert live_path_status_args.crypto_live_path_command == "status"
    assert live_path_status_args.assets == ["BTC", "ETH"]
    assert live_path_refresh_args.crypto_live_path_command == "refresh"
    assert live_path_refresh_args.history_days == 2
    assert live_path_refresh_args.spot_days == 2
    assert live_path_refresh_args.replay_days == 30
    assert live_path_refresh_args.assets == ["XRP"]
    assert model_quality_args.command == "model-quality"
    assert model_quality_args.model_quality_command == "status"
    assert model_quality_args.kalshi_env == "demo"
    assert model_quality_args.domain == "all"


@pytest.mark.asyncio
async def test_crypto_live_path_refresh_uses_forecast_service(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_status_payload(args: SimpleNamespace, container: SimpleNamespace) -> dict[str, object]:
        return {"status": "collecting", "ready_assets": [], "summary": {}}

    class HistoryService:
        async def collect_open(self, **kwargs: object) -> dict[str, object]:
            calls.append(("collect_open", kwargs))
            return {"status": "ok"}

        async def bootstrap(self, **kwargs: object) -> dict[str, object]:
            calls.append(("bootstrap", kwargs))
            return {"status": "ok"}

    class SpotService:
        async def backfill(self, **kwargs: object) -> dict[str, object]:
            calls.append(("spot_backfill", kwargs))
            return {"status": "ok"}

    class ForecastService:
        async def train(self, **kwargs: object) -> dict[str, object]:
            calls.append(("forecast_train", kwargs))
            return {"status": "trained"}

    class ReplayService:
        async def run(self, **kwargs: object) -> dict[str, object]:
            calls.append(("replay_run", kwargs))
            return {"status": "warn"}

        async def gate(self, **kwargs: object) -> dict[str, object]:
            calls.append(("replay_gate", kwargs))
            return {"status": "blocked"}

    calls: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(cli_module, "_crypto_live_path_status_payload", fake_status_payload)
    args = SimpleNamespace(
        crypto_live_path_command="refresh",
        assets=["XRP"],
        frequency="15m",
        history_days=2,
        spot_days=2,
        replay_days=30,
        require_ready=False,
    )
    container = SimpleNamespace(
        crypto_history_service=HistoryService(),
        crypto_spot_service=SpotService(),
        crypto_forecast_service=ForecastService(),
        crypto_replay_service=ReplayService(),
    )

    exit_code = await cli_module._run_crypto_live_path_command(args, container)

    assert exit_code == 0
    assert [name for name, _kwargs in calls] == [
        "collect_open",
        "bootstrap",
        "spot_backfill",
        "forecast_train",
        "replay_run",
        "replay_gate",
    ]


def test_python_module_cli_exposes_weather_prediction_commands() -> None:
    parser = cli_module.build_parser()

    prediction_args = parser.parse_args(["weather-prediction", "evaluate", "--series", "KXHIGHNY"])
    diagnostics_args = parser.parse_args(["weather-prediction", "station-diagnostics", "--min-days", "10"])
    sigma_args = parser.parse_args(["weather-sigma", "refit", "--dry-run"])
    residual_args = parser.parse_args(["weather-residual", "train", "--kalshi-env", "demo", "--dry-run"])
    intraday_eval_args = parser.parse_args(
        ["weather-intraday", "evaluate", "--kalshi-env", "production", "--series", "KXHIGHNY", "--json"]
    )
    intraday_train_args = parser.parse_args(["weather-intraday", "train", "--kalshi-env", "production", "--json"])
    intraday_status_args = parser.parse_args(["weather-intraday", "status", "--kalshi-env", "production", "--json"])

    assert prediction_args.command == "weather-prediction"
    assert prediction_args.weather_prediction_command == "evaluate"
    assert prediction_args.series == ["KXHIGHNY"]
    assert diagnostics_args.weather_prediction_command == "station-diagnostics"
    assert diagnostics_args.min_days == 10
    assert sigma_args.command == "weather-sigma"
    assert sigma_args.weather_sigma_command == "refit"
    assert sigma_args.dry_run is True
    assert residual_args.command == "weather-residual"
    assert residual_args.weather_residual_command == "train"
    assert residual_args.kalshi_env == "demo"
    assert intraday_eval_args.command == "weather-intraday"
    assert intraday_eval_args.weather_intraday_command == "evaluate"
    assert intraday_eval_args.kalshi_env == "production"
    assert intraday_eval_args.series == ["KXHIGHNY"]
    assert intraday_train_args.weather_intraday_command == "train"
    assert intraday_status_args.weather_intraday_command == "status"


@pytest.mark.asyncio
async def test_weather_intraday_cli_commands_emit_stable_json(monkeypatch, capsys) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeWeatherPredictionService:
        async def train_intraday_model(self, **kwargs):
            calls.append(("train_intraday_model", kwargs))
            return {
                "active": not kwargs["dry_run"],
                "kalshi_env": kwargs["kalshi_env"],
                "dry_run": kwargs["dry_run"],
                "gates": {"min_train_rows": {"passed": True}},
            }

        async def status(self, **kwargs):
            calls.append(("status", kwargs))
            return {
                "kalshi_env": kwargs["kalshi_env"],
                "intraday_model": {"active": True},
                "intraday_model_usable": True,
            }

    class FakeContainer:
        weather_prediction_service = FakeWeatherPredictionService()

        async def close(self) -> None:
            return None

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    parser = cli_module.build_parser()

    evaluate_exit = await cli_module._run_cli(
        parser.parse_args(
            ["weather-intraday", "evaluate", "--kalshi-env", "production", "--series", "KXHIGHNY", "--json"]
        )
    )
    evaluate_payload = json.loads(capsys.readouterr().out)

    train_exit = await cli_module._run_cli(
        parser.parse_args(["weather-intraday", "train", "--kalshi-env", "production", "--json"])
    )
    train_payload = json.loads(capsys.readouterr().out)

    status_exit = await cli_module._run_cli(
        parser.parse_args(["weather-intraday", "status", "--kalshi-env", "production", "--json"])
    )
    status_payload = json.loads(capsys.readouterr().out)

    assert evaluate_exit == 0
    assert train_exit == 0
    assert status_exit == 0
    assert evaluate_payload == {
        "active": False,
        "kalshi_env": "production",
        "dry_run": True,
        "gates": {"min_train_rows": {"passed": True}},
    }
    assert train_payload["active"] is True
    assert train_payload["dry_run"] is False
    assert status_payload["intraday_model_usable"] is True
    assert calls == [
        (
            "train_intraday_model",
            {"kalshi_env": "production", "dry_run": True, "series": ["KXHIGHNY"]},
        ),
        (
            "train_intraday_model",
            {"kalshi_env": "production", "dry_run": False, "series": None},
        ),
        ("status", {"kalshi_env": "production"}),
    ]


def test_python_module_cli_exposes_parameter_pack_commands() -> None:
    top_level = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    command_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    gate_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "gate", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert top_level.returncode == 0
    assert "parameter-pack" in top_level.stdout
    assert command_help.returncode == 0
    assert "validate" in command_help.stdout
    assert "gate" in command_help.stdout
    assert "drift" in command_help.stdout
    assert "select" in command_help.stdout
    assert "grid" in command_help.stdout
    assert "learned-gate" in command_help.stdout
    assert "nws-parser-gate" in command_help.stdout
    assert "record-starvation" in command_help.stdout
    assert "stage" in command_help.stdout
    assert "rollback-staged" in command_help.stdout
    assert "canary" in command_help.stdout
    assert "promote-staged" in command_help.stdout
    assert "status" in command_help.stdout
    assert "hard-caps" in command_help.stdout
    assert "seed-default" in command_help.stdout
    assert gate_help.returncode == 0
    assert "--candidate-report" in gate_help.stdout
    assert "--current-report" in gate_help.stdout
    assert "--hard-caps" in gate_help.stdout
    stage_help = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "stage", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert stage_help.returncode == 0
    assert "--candidate-pack" in stage_help.stdout


def test_parameter_pack_validate_cli_sanitizes_candidate_json(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    candidate = {
        "version": "candidate-v1",
        "status": "candidate",
        "parameters": {
            "pseudo_count": 999,
            "kelly_fraction": -1.0,
            "max_position_usd": 10_000,
        },
    }
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "validate", str(candidate_path)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["pack"]["parameters"]["pseudo_count"] == 32
    assert payload["pack"]["parameters"]["kelly_fraction"] == 0.01
    assert payload["dropped_hard_cap_parameters"] == ["max_position_usd"]


def test_parameter_pack_validate_strict_cli_rejects_hard_caps(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-strict-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text(
        json.dumps(
            {
                "version": "candidate-v1",
                "status": "candidate",
                "parameters": {"max_position_usd": 10_000},
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "validate", str(candidate_path), "--strict"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["error"] == "candidate_contains_hard_cap_parameters"


def test_parameter_pack_hard_caps_cli_prints_sealed_config_hash(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/hard-caps-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"

    result = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "hard-caps"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert len(payload["config_hash"]) == 64
    assert payload["hard_caps"]["operator_only"] is True
    assert payload["hard_caps"]["hard_caps"]["max_position_pct"] == 0.10


def test_parameter_pack_gate_cli_returns_success_for_passing_reports(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-gate-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    current_path = tmp_path / "current.json"
    candidate_path = tmp_path / "candidate.json"
    current_path.write_text(
        json.dumps(
            {
                "coverage": 0.98,
                "brier": 0.20,
                "ece": 0.05,
                "sharpe": 1.0,
                "max_drawdown": 0.10,
                "resolved_trades": 100,
                "city_win_rates": {"NY": 0.58},
                "pack_hash": "current",
                "rerun_pack_hash": "current",
            }
        ),
        encoding="utf-8",
    )
    candidate_path.write_text(
        json.dumps(
            {
                "coverage": 0.97,
                "brier": 0.19,
                "ece": 0.04,
                "sharpe": 0.98,
                "max_drawdown": 0.04,
                "resolved_trades": 100,
                "city_win_rates": {"NY": 0.56},
                "hard_cap_touches": 0,
                "pack_hash": "candidate",
                "rerun_pack_hash": "candidate",
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "gate",
            "--candidate-report",
            str(candidate_path),
            "--current-report",
            str(current_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["passed"] is True
    assert payload["failures"] == []
    assert len(payload["hard_caps"]["config_hash"]) == 64
    assert payload["hard_caps"]["max_drawdown_pct"] == 0.05


def test_parameter_pack_gate_cli_uses_sealed_hard_drawdown_cap(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-gate-cap-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    current_path = tmp_path / "current.json"
    candidate_path = tmp_path / "candidate.json"
    current_path.write_text(
        json.dumps(
            {
                "coverage": 0.98,
                "brier": 0.20,
                "ece": 0.05,
                "sharpe": 1.0,
                "max_drawdown": 0.30,
                "resolved_trades": 100,
                "pack_hash": "current",
                "rerun_pack_hash": "current",
            }
        ),
        encoding="utf-8",
    )
    candidate_path.write_text(
        json.dumps(
            {
                "coverage": 0.97,
                "brier": 0.19,
                "ece": 0.04,
                "sharpe": 0.98,
                "max_drawdown": 0.21,
                "resolved_trades": 100,
                "hard_cap_touches": 0,
                "pack_hash": "candidate",
                "rerun_pack_hash": "candidate",
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "gate",
            "--candidate-report",
            str(candidate_path),
            "--current-report",
            str(current_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["failures"] == ["drawdown_regression"]
    assert payload["comparisons"]["max_drawdown"]["maximum"] == 0.05


def test_parameter_pack_drift_cli_reports_pause_decision(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-drift-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    window_path = tmp_path / "drift-window.json"
    window_path.write_text(
        json.dumps(
            {
                "rolling_7d_brier": 0.24,
                "trailing_30d_brier": 0.20,
                "rolling_ece": 0.09,
                "predicted_win_rate": 0.60,
                "realized_win_rate": 0.52,
                "trade_count": 150,
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "drift", "--window", str(window_path)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["pause_new_entries"] is True
    assert payload["trigger_pack_search"] is True
    assert payload["reasons"] == ["brier_relative_drift", "ece_above_limit", "win_rate_divergence"]


def test_parameter_pack_select_cli_outputs_first_passing_candidate(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-select-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    current_path = tmp_path / "current-report.json"
    candidates_path = tmp_path / "candidates.json"
    current_path.write_text(
        json.dumps(
            {
                "coverage": 0.99,
                "brier": 0.20,
                "ece": 0.05,
                "sharpe": 1.0,
                "max_drawdown": 0.10,
                "resolved_trades": 100,
                "city_win_rates": {"NY": 0.58},
                "pack_hash": "current",
                "rerun_pack_hash": "current",
            }
        ),
        encoding="utf-8",
    )
    candidates_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "version": "bad-candidate",
                        "parameters": {"pseudo_count": 10},
                        "holdout_report": {
                            "coverage": 0.90,
                            "brier": 0.19,
                            "ece": 0.04,
                            "sharpe": 1.0,
                            "max_drawdown": 0.04,
                            "resolved_trades": 100,
                            "city_win_rates": {"NY": 0.58},
                            "hard_cap_touches": 0,
                        },
                    },
                    {
                        "version": "good-candidate",
                        "parameters": {"pseudo_count": 12},
                        "holdout_report": {
                            "coverage": 0.98,
                            "brier": 0.19,
                            "ece": 0.04,
                            "sharpe": 1.0,
                            "max_drawdown": 0.04,
                            "resolved_trades": 100,
                            "city_win_rates": {"NY": 0.58},
                            "hard_cap_touches": 0,
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "select",
            "--candidates",
            str(candidates_path),
            "--current-report",
            str(current_path),
            "--starvation-tolerance",
            "2",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["selected"] is True
    assert payload["promotion_starvation"] is False
    assert payload["selected_candidate"]["version"] == "good-candidate"
    assert payload["selected_candidate"]["holdout_report"]["pack_hash"] == payload["selected_candidate"]["pack_hash"]
    assert payload["evaluated"][0]["failures"] == ["coverage_below_minimum"]


def test_parameter_pack_record_starvation_cli_writes_warning_event(tmp_path) -> None:
    db_path = tmp_path / "parameter-pack-starvation-cli.db"
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"
    env["APP_AUTO_INIT_DB"] = "true"
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(
        json.dumps(
            {
                "selected": False,
                "promotion_starvation": True,
                "starvation_tolerance": 2,
                "evaluated": [{"version": "bad-candidate", "failures": ["coverage_below_minimum"]}],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "record-starvation",
            "--selection",
            str(selection_path),
            "--escalation-threshold",
            "2",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "promotion_starvation"
    assert payload["consecutive_starvations"] == 1
    assert payload["escalated"] is False
    status = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "status"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert status.returncode == 0
    status_payload = json.loads(status.stdout)
    assert status_payload["promotion_starvation"]["status"] == "promotion_starvation"
    assert status_payload["promotion_starvation"]["consecutive_starvations"] == 1
    assert status_payload["promotion_starvation"]["escalated"] is False
    assert status_payload["promotion_starvation"]["checkpoint_name"] == "parameter_pack_promotion_starvation:demo"
    with sqlite3.connect(db_path) as conn:
        event = conn.execute(
            "select severity, source, summary, payload from ops_events order by created_at desc limit 1"
        ).fetchone()
        checkpoint = conn.execute(
            "select payload from checkpoints where stream_name = ?",
            ("parameter_pack_promotion_starvation:demo",),
        ).fetchone()
    assert event is not None
    assert event[0] == "warning"
    assert event[1] == "parameter_pack"
    assert "promotion starvation" in event[2]
    event_payload = json.loads(event[3])
    assert event_payload["consecutive_starvations"] == 1
    assert event_payload["selection"]["evaluated"][0]["version"] == "bad-candidate"
    assert checkpoint is not None
    checkpoint_payload = json.loads(checkpoint[0])
    assert checkpoint_payload["escalated"] is False


def test_parameter_pack_grid_cli_outputs_bounded_candidates(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-grid-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    grid_path = tmp_path / "grid.json"
    grid_path.write_text(
        json.dumps({"parameters": {"pseudo_count": [4, 999], "kelly_fraction": [0.20]}}),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "grid",
            "--grid",
            str(grid_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 2
    assert payload["candidates"][0]["pack"]["parameters"]["pseudo_count"] == 4
    assert payload["candidates"][1]["pack"]["parameters"]["pseudo_count"] == 32


def test_parameter_pack_learned_gate_cli_forces_zero_weight_on_regression(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-learned-gate-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    closed_path = tmp_path / "closed-form.json"
    learned_path = tmp_path / "learned.json"
    closed_path.write_text(json.dumps({"brier": 0.20, "ece": 0.05, "sharpe": 1.0}), encoding="utf-8")
    learned_path.write_text(
        json.dumps({"brier": 0.19, "ece": 0.07, "sharpe": 1.06}),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "learned-gate",
            "--closed-form-report",
            str(closed_path),
            "--learned-report",
            str(learned_path),
            "--requested-weight",
            "0.25",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert payload["learned_weight"] == 0.0
    assert payload["failures"] == ["ece_not_improved"]


def test_parameter_pack_nws_parser_gate_cli_reports_shadow_failure(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-nws-parser-gate-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    window_path = tmp_path / "parser-window.json"
    window_path.write_text(
        json.dumps({"attempts": 100, "successful_parses": 92, "schema_failures": 2}),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "nws-parser-gate",
            "--window",
            str(window_path),
            "--requested-feature-weight",
            "0.25",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert payload["feature_weight"] == 0.0
    assert payload["failures"] == ["parser_availability_below_minimum", "schema_failure_rate_above_maximum"]


def test_parameter_pack_stage_cli_records_staged_candidate(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/parameter-pack-stage-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"
    current = default_parameter_pack()
    candidate = replace(
        default_parameter_pack(version="candidate-stage-cli-v1"),
        status="candidate",
        parameters={**current.parameters, "pseudo_count": 12},
    )
    candidate_pack_path = tmp_path / "candidate-pack.json"
    current_report_path = tmp_path / "current-report.json"
    candidate_report_path = tmp_path / "candidate-report.json"
    canary_report_path = tmp_path / "canary-report.json"
    candidate_pack_path.write_text(json.dumps(candidate.to_dict()), encoding="utf-8")
    current_report_path.write_text(
        json.dumps(
            {
                "coverage": 0.98,
                "brier": 0.20,
                "ece": 0.05,
                "sharpe": 1.0,
                "max_drawdown": 0.10,
                "resolved_trades": 100,
                "pack_hash": current.pack_hash,
                "rerun_pack_hash": current.pack_hash,
            }
        ),
        encoding="utf-8",
    )
    candidate_report_path.write_text(
        json.dumps(
            {
                "coverage": 0.98,
                "brier": 0.19,
                "ece": 0.04,
                "sharpe": 1.0,
                "max_drawdown": 0.04,
                "resolved_trades": 100,
                "hard_cap_touches": 0,
                "pack_hash": candidate.pack_hash,
                "rerun_pack_hash": candidate.pack_hash,
            }
        ),
        encoding="utf-8",
    )
    canary_report_path.write_text(
        json.dumps(
            {
                "completed_shadow_rooms": 25,
                "elapsed_seconds": 7200,
                "brier": 0.20,
                "risk_engine_bypasses": 0,
                "data_source_kill_events": 0,
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "stage",
            "--candidate-pack",
            str(candidate_pack_path),
            "--candidate-report",
            str(candidate_report_path),
            "--current-report",
            str(current_report_path),
            "--reason",
            "cli_test_stage",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "staged"
    assert payload["candidate_version"] == candidate.version
    assert payload["previous_version"] == current.version
    assert payload["target_color"] == "green"
    assert payload["gate"]["passed"] is True

    canary = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "canary",
            "--report",
            str(canary_report_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert canary.returncode == 0
    canary_payload = json.loads(canary.stdout)
    assert canary_payload["status"] == "canary_passed"
    assert canary_payload["passed"] is True

    rollback = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "parameter-pack",
            "rollback-staged",
            "--reason",
            "cli_test_rollback",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert rollback.returncode == 0
    rollback_payload = json.loads(rollback.stdout)
    assert rollback_payload["status"] == "rolled_back"
    assert rollback_payload["candidate_version"] == candidate.version
    assert rollback_payload["reason"] == "cli_test_rollback"

    status = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.cli", "parameter-pack", "status", "--limit", "5"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert status.returncode == 0
    status_payload = json.loads(status.stdout)
    assert status_payload["parameter_packs"]["status"] == "rolled_back"
    assert status_payload["parameter_packs"]["candidate_version"] == candidate.version
    assert status_payload["recent_promotions"][0]["candidate_version"] == candidate.version


@pytest.mark.asyncio
async def test_shadow_run_cli_fails_when_trace_is_missing(monkeypatch, capsys) -> None:
    class FakeShadowTrainingService:
        async def run_shadow_room(self, market_ticker: str, *, name=None, prompt=None, reason="shadow_run"):
            return SimpleNamespace(
                room_id="room-1",
                market_ticker=market_ticker,
                stage="complete",
                decision_trace_id=None,
            )

    class FakeContainer:
        shadow_training_service = FakeShadowTrainingService()

        async def close(self) -> None:
            return None

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    args = cli_module.build_parser().parse_args(["shadow-run", "KXHIGHNY-26APR27-T69"])

    exit_code = await cli_module._run_cli(args)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "without a deterministic decision trace" in captured.err


@pytest.mark.asyncio
async def test_shadow_sweep_cli_reports_trace_ids_and_fails_when_any_are_missing(monkeypatch, capsys) -> None:
    class FakeShadowTrainingService:
        async def run_shadow_sweep(self, *, markets=None, limit=None, reason="shadow_sweep"):
            return [
                SimpleNamespace(
                    room_id="room-1",
                    market_ticker="KXHIGHNY-26APR27-T69",
                    room_name="shadow one",
                    stage="complete",
                    decision_trace_id="trace-1",
                ),
                SimpleNamespace(
                    room_id="room-2",
                    market_ticker="KXHIGHCHI-26APR27-T66",
                    room_name="shadow two",
                    stage="complete",
                    decision_trace_id=None,
                ),
            ]

    class FakeContainer:
        shadow_training_service = FakeShadowTrainingService()

        async def close(self) -> None:
            return None

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    args = cli_module.build_parser().parse_args(["shadow-sweep", "--limit", "2"])

    exit_code = await cli_module._run_cli(args)

    captured = capsys.readouterr()
    assert exit_code == 1
    payload = json.loads(captured.err)
    assert payload["results"][0]["decision_trace_id"] == "trace-1"
    assert payload["results"][1]["decision_trace_id"] is None
    assert "missing deterministic decision traces" in payload["error"]


@pytest.mark.asyncio
async def test_shadow_campaign_cli_reports_trace_ids_and_fails_when_any_are_missing(monkeypatch, capsys) -> None:
    class FakeShadowCampaignService:
        async def run(self, request):
            return [
                SimpleNamespace(
                    room_id="room-1",
                    market_ticker="KXHIGHNY-26APR27-T69",
                    room_name="shadow one",
                    stage="complete",
                    decision_trace_id="trace-1",
                ),
                SimpleNamespace(
                    room_id="room-2",
                    market_ticker="KXHIGHCHI-26APR27-T66",
                    room_name="shadow two",
                    stage="complete",
                    decision_trace_id=None,
                ),
            ]

    class FakeContainer:
        shadow_campaign_service = FakeShadowCampaignService()

        async def close(self) -> None:
            return None

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    args = cli_module.build_parser().parse_args(["shadow-campaign", "run", "--limit", "2"])

    exit_code = await cli_module._run_cli(args)

    captured = capsys.readouterr()
    assert exit_code == 1
    payload = json.loads(captured.err)
    assert payload["results"][0]["decision_trace_id"] == "trace-1"
    assert payload["results"][1]["decision_trace_id"] is None
    assert "missing deterministic decision traces" in payload["error"]


def test_shadow_campaign_cli_accepts_weather_domain() -> None:
    args = cli_module.build_parser().parse_args(["shadow-campaign", "run", "--domain", "weather", "--limit", "25"])

    assert args.command == "shadow-campaign"
    assert args.shadow_campaign_command == "run"
    assert args.domain == "weather"
    assert args.limit == 25


def test_training_backfill_research_health_cli_arguments() -> None:
    args = cli_module.build_parser().parse_args(
        [
            "training-backfill",
            "research-health",
            "--origins",
            "shadow",
            "--days",
            "30",
            "--market-prefix",
            "KXHIGH",
            "--limit",
            "2000",
        ]
    )

    assert args.command == "training-backfill"
    assert args.training_backfill_command == "research-health"
    assert args.origins == ["shadow"]
    assert args.market_prefix == ["KXHIGH"]


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


def test_baseline_model_card_cli_writes_read_only_card_without_db(tmp_path) -> None:
    historical = tmp_path / "historical.jsonl"
    shadow = tmp_path / "shadow.jsonl"
    output = tmp_path / "baseline-card.json"
    historical.write_text(
        json.dumps(
            {
                "market_ticker": "KXHIGHNY-26APR01-T70",
                "coverage_class": "full_checkpoint_coverage",
                "split": "holdout",
                "historical_provenance": {
                    "local_market_day": "2026-04-01",
                    "coverage_class": "full_checkpoint_coverage",
                    "settlement_label_signature": json.dumps({"settlement_value_dollars": "1.0000"}),
                },
                "signal": {"fair_yes_dollars": "0.7500"},
                "outcome": {"final_status": "stand_down", "orders_submitted": 0},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    shadow.write_text(
        json.dumps(
            {
                "room_origin": "shadow",
                "room": {"market_ticker": "KXHIGHCHI-26MAY01-T66", "room_origin": "shadow"},
                "decision_trace_id": "trace-1",
                "signal": {"fair_yes_dollars": "0.2500"},
                "settlement": {"settlement_value_dollars": "0.0000"},
                "outcome": {"final_status": "stand_down", "orders_submitted": 0},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "baseline-model-card",
            "--historical",
            str(historical),
            "--shadow",
            str(shadow),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert output.exists()
    payload = json.loads(output.read_text(encoding="utf-8"))
    stdout_payload = json.loads(result.stdout)
    assert payload["read_only"] is True
    assert stdout_payload["output"] == str(output)
    assert payload["row_counts"]["total"] == 2


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


@pytest.mark.asyncio
async def test_create_web_user_cli_runs_inside_active_session_and_updates_existing_user(monkeypatch, capsys) -> None:
    users: dict[str, SimpleNamespace] = {}

    class FakeSession:
        active = False

        async def __aenter__(self):
            self.active = True
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.active = False
            return None

        async def commit(self) -> None:
            assert self.active

    class FakeSessionFactory:
        def __call__(self) -> FakeSession:
            return FakeSession()

    class FakeContainer:
        session_factory = FakeSessionFactory()

        async def close(self) -> None:
            return None

    class FakePlatformRepository:
        def __init__(self, session: FakeSession) -> None:
            self.session = session

        def _assert_active(self) -> None:
            assert self.session.active

        async def get_web_user_by_email(self, email: str) -> SimpleNamespace | None:
            self._assert_active()
            return users.get(email)

        async def create_web_user(
            self,
            *,
            email: str,
            password_hash: str,
            password_salt: str,
            is_active: bool = True,
        ) -> SimpleNamespace:
            self._assert_active()
            user = SimpleNamespace(
                email=email,
                password_hash=password_hash,
                password_salt=password_salt,
                is_active=is_active,
            )
            users[email] = user
            return user

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    monkeypatch.setattr(cli_module, "PlatformRepository", FakePlatformRepository)
    parser = cli_module.build_parser()

    create_exit = await cli_module._run_cli(
        parser.parse_args(
            [
                "create-web-user",
                "--email",
                "Operator@Example.COM",
                "--password",
                "first-password",
            ]
        )
    )
    user = users["operator@example.com"]
    first_hash = user.password_hash
    user.is_active = False

    update_exit = await cli_module._run_cli(
        parser.parse_args(
            [
                "create-web-user",
                "--email",
                "operator@example.com",
                "--password",
                "second-password",
            ]
        )
    )

    assert create_exit == 0
    assert update_exit == 0
    assert user.is_active is True
    assert user.password_hash != first_hash
    output = capsys.readouterr().out
    assert '"action": "created"' in output
    assert '"action": "updated"' in output
    assert '"email": "operator@example.com"' in output


@pytest.mark.asyncio
async def test_health_check_app_cli_delegates_to_watchdog_service(monkeypatch, capsys) -> None:
    calls: list[dict[str, str]] = []

    class FakeWatchdogService:
        async def app_health(self, *, color: str, kalshi_env: str) -> dict[str, object]:
            calls.append({"color": color, "kalshi_env": kalshi_env})
            return {"healthy": False, "color": color, "kalshi_env": kalshi_env}

    class FakeContainer:
        settings = SimpleNamespace(kalshi_env="demo")
        watchdog_service = FakeWatchdogService()

        async def close(self) -> None:
            return None

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return FakeContainer()

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)

    exit_code = await cli_module._run_cli(
        cli_module.build_parser().parse_args(["health-check", "app", "--color", "green"])
    )

    assert exit_code == 1
    assert calls == [{"color": "green", "kalshi_env": "demo"}]
    output = capsys.readouterr().out
    assert '"healthy": false' in output
    assert '"kalshi_env": "demo"' in output


@pytest.mark.asyncio
async def test_watchdog_status_cli_runs_inside_active_session_and_commits(monkeypatch, capsys) -> None:
    calls: list[str] = []

    class FakeSession:
        def __init__(self) -> None:
            self.active = False
            self.commits = 0

        async def __aenter__(self):
            self.active = True
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.active = False
            return None

        async def commit(self) -> None:
            assert self.active
            self.commits += 1

    class FakeSessionFactory:
        def __init__(self) -> None:
            self.sessions: list[FakeSession] = []

        def __call__(self) -> FakeSession:
            session = FakeSession()
            self.sessions.append(session)
            return session

    class FakePlatformRepository:
        def __init__(self, session: FakeSession) -> None:
            assert session.active
            self.session = session

    class FakeWatchdogService:
        async def get_status(self, repo: FakePlatformRepository, *, kalshi_env: str) -> dict[str, object]:
            assert repo.session.active
            calls.append(kalshi_env)
            return {"status": "ok", "kalshi_env": kalshi_env}

    class FakeContainer:
        def __init__(self) -> None:
            self.settings = SimpleNamespace(kalshi_env="production")
            self.session_factory = FakeSessionFactory()
            self.watchdog_service = FakeWatchdogService()

        async def close(self) -> None:
            return None

    container = FakeContainer()

    async def fake_build(*, bootstrap_db: bool):
        assert bootstrap_db is True
        return container

    monkeypatch.setattr(cli_module.AppContainer, "build", fake_build)
    monkeypatch.setattr(cli_module, "PlatformRepository", FakePlatformRepository)

    exit_code = await cli_module._run_cli(
        cli_module.build_parser().parse_args(["watchdog", "status"])
    )

    assert exit_code == 0
    assert calls == ["production"]
    assert container.session_factory.sessions[0].commits == 1
    output = capsys.readouterr().out
    assert '"status": "ok"' in output
    assert '"kalshi_env": "production"' in output


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


def test_overnight_readiness_cli_report_json_smoke(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/overnight-readiness-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "overnight-readiness",
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
    assert '"schema_version": "overnight-readiness-v1"' in payload
    assert '"ready_for_live"' in payload
    assert '"read_only": true' in payload
    assert "Traceback" not in result.stderr


def test_trade_behavior_validate_cli_modes_json_smoke(tmp_path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path}/trade-behavior-cli.db"
    env["APP_AUTO_INIT_DB"] = "true"

    fast = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "trade-behavior",
            "validate",
            "--mode",
            "fast",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert fast.returncode == 0
    assert '"mode": "fast"' in fast.stdout
    assert '"fast_gate"' in fast.stdout
    assert "Traceback" not in fast.stderr

    detailed = subprocess.run(
        [
            sys.executable,
            "-m",
            "kalshi_bot.cli",
            "trade-behavior",
            "validate",
            "--mode",
            "detailed",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert detailed.returncode == 0
    assert '"mode": "detailed"' in detailed.stdout
    assert '"analysis"' in detailed.stdout
    assert "Traceback" not in detailed.stderr


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
