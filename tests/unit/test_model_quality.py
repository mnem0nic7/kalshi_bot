from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services import model_quality as model_quality_module
from kalshi_bot.services.model_quality import build_model_quality_report


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=UTC)


class FakeCryptoMarketService:
    async def status(self, *, frequency: str = "15m") -> dict[str, Any]:
        return {
            "status": "ok",
            "frequency": frequency,
            "data_quality": {
                "status": "ready",
                "settled_market_count": 120,
                "snapshot_count": 240,
                "candle_count": 240,
                "asset_counts": {"BTC": 60, "ETH": 60},
            },
            "global_live_blockers": [],
        }


class FakeCryptoForecastService:
    def __init__(self, *, live_count: int = 60, shadow_count: int = 12) -> None:
        self.live_count = live_count
        self.shadow_count = shadow_count

    async def candidates(self, *, frequency: str = "15m", days: int | None = None) -> dict[str, Any]:
        return {
            "schema_version": "crypto-model-candidates-v2",
            "status": "ok",
            "frequency": frequency,
            "days": days,
            "live_quality_policy": {"selected_count": self.live_count, "net_pnl": "12.5000"},
            "shadow_exploration_policy": {"selected_count": self.shadow_count, "net_pnl": "3.2500"},
        }


class FakeCryptoReplayService:
    def __init__(
        self,
        *,
        passed: bool = True,
        trade_count: int = 60,
        net_pnl: str = "12.5000",
        reasons: list[str] | None = None,
        worse_metrics: bool = False,
    ) -> None:
        self.passed = passed
        self.trade_count = trade_count
        self.net_pnl = net_pnl
        self.reasons = reasons or []
        self.worse_metrics = worse_metrics

    async def validate(self, *, frequency: str = "15m", days: int | None = None) -> dict[str, Any]:
        if self.worse_metrics:
            calibration = {"brier": 0.260, "log_loss": 0.720, "ece": 0.060}
            market_mid = {"brier": 0.250, "log_loss": 0.700, "ece": 0.050}
        else:
            calibration = {"brier": 0.210, "log_loss": 0.610, "ece": 0.025}
            market_mid = {"brier": 0.250, "log_loss": 0.700, "ece": 0.050}
        return {
            "schema_version": "crypto-replay-report-v1",
            "status": "pass" if self.passed else "fail",
            "frequency": frequency,
            "days": days,
            "metrics": {
                "resolved_sample_count": 120,
                "market_mid_brier": market_mid["brier"],
                "market_mid_log_loss": market_mid["log_loss"],
                "market_mid_ece": market_mid["ece"],
                "calibration_brier": calibration["brier"],
                "calibration_log_loss": calibration["log_loss"],
                "calibration_ece": calibration["ece"],
                "trade_candidate_count": self.trade_count,
                "net_simulated_pl_dollars": self.net_pnl,
            },
            "walk_forward": {
                "fold_count": 4,
                "bucket_matrix": [{"bucket_key": "BTC|yes|50-59c", "net_pnl": self.net_pnl}],
            },
            "promotion_gate": {"passed": self.passed, "reasons": self.reasons},
        }


async def _session_factory(tmp_path):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/model-quality.db")
    engine = create_engine(settings)
    await init_models(engine)
    return settings, create_session_factory(engine)


async def _record_crypto_artifacts(session_factory) -> None:
    async with session_factory() as session:
        repo = PlatformRepository(session)
        for artifact_type, version in (
            ("model", "crypto-model-good"),
            ("backtest", "crypto-backtest-good"),
            ("replay_gate", "crypto-gate-good"),
        ):
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=artifact_type,
                version=version,
                status="passed",
                sample_count=120,
                metrics={"net_pnl_after_fees": "12.5000"},
                payload={"version": version},
                kalshi_env="demo",
                trained_at=NOW,
            )
        await session.commit()


@pytest.mark.asyncio
async def test_model_quality_crypto_persists_artifact_trend(tmp_path) -> None:
    settings, session_factory = await _session_factory(tmp_path)
    await _record_crypto_artifacts(session_factory)
    forecast = FakeCryptoForecastService(live_count=60, shadow_count=12)
    replay = FakeCryptoReplayService(passed=True, trade_count=60, net_pnl="12.5000")

    first = await build_model_quality_report(
        settings=settings,
        session_factory=session_factory,
        decision_corpus_service=object(),
        trading_audit_service=object(),
        trade_analysis_service=object(),
        crypto_market_service=FakeCryptoMarketService(),
        crypto_forecast_service=forecast,
        crypto_replay_service=replay,
        kalshi_env="demo",
        domain="crypto",
        days=180,
        persist=True,
        now=NOW,
    )

    assert first["status"] == "pass"
    assert first["domains"]["crypto"]["promotion_ready"] is True
    assert first["domains"]["crypto"]["artifacts"]["model"]["version"] == "crypto-model-good"
    assert first["checkpoint"]["history_count"] == 1

    forecast.live_count = 62
    forecast.shadow_count = 10
    replay.trade_count = 62
    replay.net_pnl = "15.2500"
    second = await build_model_quality_report(
        settings=settings,
        session_factory=session_factory,
        decision_corpus_service=object(),
        trading_audit_service=object(),
        trade_analysis_service=object(),
        crypto_market_service=FakeCryptoMarketService(),
        crypto_forecast_service=forecast,
        crypto_replay_service=replay,
        kalshi_env="demo",
        domain="crypto",
        days=180,
        persist=True,
        now=NOW,
    )

    assert second["checkpoint"]["history_count"] == 2
    trend = second["trend"]["changes"]["crypto"]
    assert trend["strict_candidate_delta"] == 2
    assert trend["shadow_candidate_delta"] == -2
    assert trend["net_pnl_delta"] == 2.75


@pytest.mark.asyncio
async def test_model_quality_crypto_blocks_degraded_candidate_model(tmp_path) -> None:
    settings, session_factory = await _session_factory(tmp_path)

    report = await build_model_quality_report(
        settings=settings,
        session_factory=session_factory,
        decision_corpus_service=object(),
        trading_audit_service=object(),
        trade_analysis_service=object(),
        crypto_market_service=FakeCryptoMarketService(),
        crypto_forecast_service=FakeCryptoForecastService(live_count=8, shadow_count=12),
        crypto_replay_service=FakeCryptoReplayService(
            passed=False,
            trade_count=8,
            net_pnl="-4.0000",
            reasons=["candidate_count_below_minimum", "model_metrics_do_not_beat_market_mid"],
            worse_metrics=True,
        ),
        kalshi_env="demo",
        domain="crypto",
        days=180,
        persist=False,
        now=NOW,
    )

    crypto = report["domains"]["crypto"]
    assert report["status"] == "warn"
    assert crypto["promotion_ready"] is False
    assert crypto["prediction"]["metrics"]["brier"]["improved"] is False
    assert crypto["trade_selection"]["strict_candidate_count"] == 8
    assert "candidate_count_below_minimum" in crypto["blockers"]
    assert "model_metrics_do_not_beat_market_mid" in crypto["blockers"]


@pytest.mark.asyncio
async def test_model_quality_weather_summarizes_shadow_readiness(monkeypatch, tmp_path) -> None:
    settings, session_factory = await _session_factory(tmp_path)

    async def fake_modeling_report(**kwargs):
        return {
            "status": "pass",
            "decision_corpus": {"status": "ok"},
            "dataset": {"row_count": 80},
            "issues": [],
            "prediction_calibration": {
                "status": "ok",
                "eligible_rows": 80,
                "train_rows": 56,
                "test_rows": 24,
                "fold_count": 4,
                "baseline": {"brier": 0.24, "log_loss": 0.68, "ece": 0.06},
                "calibrated": {"brier": 0.20, "log_loss": 0.61, "ece": 0.03},
                "promotion_ready": True,
                "promotion_blockers": [],
            },
            "trade_selection": {
                "status": "ok",
                "eligible_rows": 80,
                "train_rows": 56,
                "test_rows": 24,
                "fold_count": 4,
                "baseline_rule_path": {"net_pnl": "1.0000", "max_drawdown": "4.0000"},
                "selected_path": {"net_pnl": "6.5000", "max_drawdown": "2.0000"},
                "promotion_ready": True,
                "promotion_blockers": [],
            },
            "bucket_matrix_summary": {"eligible_for_live_review_count": 3},
        }

    monkeypatch.setattr(model_quality_module, "build_modeling_report", fake_modeling_report)

    report = await build_model_quality_report(
        settings=settings,
        session_factory=session_factory,
        decision_corpus_service=object(),
        trading_audit_service=object(),
        trade_analysis_service=object(),
        crypto_market_service=object(),
        crypto_forecast_service=object(),
        crypto_replay_service=object(),
        kalshi_env="demo",
        domain="weather",
        days=30,
        now=NOW,
    )

    weather = report["domains"]["weather"]
    assert report["status"] == "pass"
    assert weather["baseline"] == "current_weather_scorer_rule_path"
    assert weather["promotion_ready"] is True
    assert weather["prediction"]["metrics"]["log_loss"]["improved"] is True
    assert weather["trade_selection"]["selected_net_pnl"] == "6.5000"


@pytest.mark.asyncio
async def test_model_quality_weather_warn_issue_blocks_promotion(monkeypatch, tmp_path) -> None:
    settings, session_factory = await _session_factory(tmp_path)

    async def fake_modeling_report(**kwargs):
        return {
            "status": "warn",
            "decision_corpus": {"status": "missing"},
            "dataset": {"row_count": 80},
            "issues": [
                {
                    "severity": "warn",
                    "code": "decision_corpus_missing_or_stale",
                    "summary": "No current decision corpus is promoted for this environment.",
                }
            ],
            "prediction_calibration": {
                "status": "ok",
                "eligible_rows": 80,
                "baseline": {"brier": 0.24, "log_loss": 0.68, "ece": 0.06},
                "calibrated": {"brier": 0.20, "log_loss": 0.61, "ece": 0.03},
                "promotion_ready": True,
            },
            "trade_selection": {
                "status": "ok",
                "eligible_rows": 80,
                "baseline_rule_path": {"net_pnl": "1.0000", "max_drawdown": "4.0000"},
                "selected_path": {"net_pnl": "6.5000", "max_drawdown": "2.0000"},
                "promotion_ready": True,
            },
        }

    monkeypatch.setattr(model_quality_module, "build_modeling_report", fake_modeling_report)

    report = await build_model_quality_report(
        settings=settings,
        session_factory=session_factory,
        decision_corpus_service=object(),
        trading_audit_service=object(),
        trade_analysis_service=object(),
        crypto_market_service=object(),
        crypto_forecast_service=object(),
        crypto_replay_service=object(),
        kalshi_env="demo",
        domain="weather",
        days=30,
        now=NOW,
    )

    weather = report["domains"]["weather"]
    assert report["status"] == "warn"
    assert weather["promotion_ready"] is False
    assert "decision_corpus_missing_or_stale" in weather["blockers"]
