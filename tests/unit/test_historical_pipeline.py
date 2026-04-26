from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.historical_pipeline import HistoricalPipelineService


class _FakeHistoricalTrainingService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, tuple[str, ...]]] = []
        self.status_payload = {
            "confidence_state": "execution_confident_only",
            "confidence_scorecard": {"confidence_state": "execution_confident_only"},
            "historical_build_readiness": {"training_ready": False, "distinct_full_coverage_market_days": 0},
        }
        self.audit_payload = {
            "refresh_needed": True,
            "affected_market_days": [{"local_market_day": "2026-04-10"}],
        }
        self.build_requests: list[str] = []

    async def import_weather_history(self, *, date_from, date_to, series=None):
        self.calls.append(("import", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return {"status": "completed"}

    async def backfill_market_checkpoints(self, *, date_from, date_to, series=None):
        self.calls.append(("market_backfill", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return {"status": "completed"}

    async def backfill_weather_archives(self, *, date_from, date_to, series=None):
        self.calls.append(("weather_backfill", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return {"status": "completed"}

    async def backfill_external_forecast_archives(self, *, date_from, date_to, series=None):
        self.calls.append(("forecast_archive_backfill", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return {"status": "completed"}

    async def backfill_settlements(self, *, date_from, date_to, series=None):
        self.calls.append(("settlement_backfill", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return {"status": "completed"}

    async def audit_historical_replay(self, *, date_from, date_to, series=None, verbose=False):
        self.calls.append(("audit", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return self.audit_payload

    async def refresh_historical_replay(self, *, date_from, date_to, series=None):
        self.calls.append(("refresh", date_from.isoformat(), date_to.isoformat(), tuple(series or ())))
        return {"status": "completed", "date_from": date_from.isoformat(), "date_to": date_to.isoformat()}

    async def get_status(self, *, verbose=False):
        self.calls.append(("status", "", "", tuple()))
        return self.status_payload

    async def build_historical_dataset(self, request):
        self.build_requests.append(request.mode)
        return {"status": "completed", "build": {"mode": request.mode}}


class _FakeHistoricalIntelligenceService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[str, ...], bool]] = []

    async def run(self, request):
        self.calls.append((request.date_from, request.date_to, tuple(request.origins), request.auto_promote))
        return {
            "status": "completed",
            "confidence_state": "execution_confident_only",
            "confidence_scorecard": {"confidence_state": "execution_confident_only"},
        }

    async def get_status(self):
        return {"confidence_state": "execution_confident_only"}


@pytest.mark.asyncio
async def test_historical_pipeline_bootstrap_runs_steps_in_order(tmp_path) -> None:
    training = _FakeHistoricalTrainingService()
    intelligence = _FakeHistoricalIntelligenceService()
    settings = SimpleNamespace(
        historical_pipeline_bootstrap_days=365,
        historical_pipeline_chunk_days=14,
        historical_pipeline_incremental_days=7,
        historical_intelligence_auto_promote=True,
    )
    db_settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/pipeline.db")
    engine = create_engine(db_settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = HistoricalPipelineService(
        settings,
        session_factory,
        training,
        intelligence,
    )
    expected_start, expected_end = service.rolling_window(days=10)

    result = await service.bootstrap(days=10, series=["KXHIGHNY"])

    assert result["status"] == "completed"
    step_calls = [call[0] for call in training.calls if call[0] not in {"status"}]
    assert step_calls[:7] == [
        "import",
        "market_backfill",
        "weather_backfill",
        "forecast_archive_backfill",
        "settlement_backfill",
        "audit",
        "refresh",
    ]
    assert training.build_requests == ["outcome-eval"]
    assert intelligence.calls == [
        (expected_start.isoformat(), expected_end.isoformat(), ("historical_replay",), True)
    ]
    status = await service.status()
    assert status["latest_run"]["status"] == "completed"
    assert status["bootstrap_progress"]["completed_chunk_count"] == 1
    await engine.dispose()


def test_historical_pipeline_rolling_window_ends_yesterday() -> None:
    service = HistoricalPipelineService(
        SimpleNamespace(
            historical_pipeline_bootstrap_days=365,
            historical_pipeline_chunk_days=14,
            historical_pipeline_incremental_days=7,
            historical_intelligence_auto_promote=True,
        ),
        None,
        _FakeHistoricalTrainingService(),
        _FakeHistoricalIntelligenceService(),
    )

    start, end = service.rolling_window(days=5, reference_date=date(2026, 4, 12))

    assert start.isoformat() == "2026-04-07"
    assert end.isoformat() == "2026-04-11"


@pytest.mark.asyncio
async def test_historical_pipeline_bootstrap_chunks_and_resume(tmp_path) -> None:
    training = _FakeHistoricalTrainingService()
    intelligence = _FakeHistoricalIntelligenceService()
    settings = SimpleNamespace(
        historical_pipeline_bootstrap_days=365,
        historical_pipeline_chunk_days=2,
        historical_pipeline_incremental_days=7,
        historical_intelligence_auto_promote=True,
    )
    db_settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/pipeline-resume.db")
    engine = create_engine(db_settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = HistoricalPipelineService(settings, session_factory, training, intelligence)

    original_run_chunk = service._run_chunk
    seen_chunks: list[tuple[str, str]] = []

    async def flaky_run_chunk(**kwargs):
        seen_chunks.append((kwargs["chunk_from"].isoformat(), kwargs["chunk_to"].isoformat()))
        if len(seen_chunks) == 2:
            raise RuntimeError("boom")
        return await original_run_chunk(**kwargs)

    service._run_chunk = flaky_run_chunk  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="boom"):
        await service.bootstrap(days=4, series=["KXHIGHNY"], chunk_days=2)

    service._run_chunk = original_run_chunk  # type: ignore[method-assign]
    resumed = await service.resume()

    assert resumed["status"] == "completed"
    assert resumed["bootstrap_progress"]["completed_chunk_count"] == 2
    status = await service.status()
    assert status["bootstrap_progress"]["completed_chunk_count"] == 2
    await engine.dispose()
