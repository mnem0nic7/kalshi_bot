from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.models import ClimatologyPriorRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


@pytest.mark.asyncio
async def test_forecast_snapshot_and_climatology_prior_repository_round_trip(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/forecast_phase1.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        snapshot = await repo.save_forecast_snapshot(
            market_ticker="KXHIGHNY-26APR27-T69",
            kalshi_env="demo",
            fetched_at=datetime(2026, 4, 27, 18, tzinfo=UTC),
            parameter_pack_version=None,
            source_members={"GFS": [70.0, 71.0], "ECMWF": [69.0]},
            fused_pdf={"mean_f": 70.0, "sigma_f": 2.0},
            probability_output={"p_bucket_yes": 0.62},
            source_set_used=["GFS", "ECMWF"],
        )
        session.add(
            ClimatologyPriorRecord(
                station_id="KNYC",
                series_ticker="KXHIGHNY",
                day_of_year=117,
                bucket_low_f=69.0,
                bucket_high_f=None,
                p_yes=0.41,
                sample_count=30,
                normal_window_years=30,
                smoothing_days=14,
                payload={"source": "test"},
            )
        )
        await session.flush()
        prior = await repo.get_climatology_prior(
            station_id="KNYC",
            series_ticker="KXHIGHNY",
            day_of_year=117,
            bucket_low_f=69.0,
            bucket_high_f=None,
        )
        health_log = await repo.save_source_health_log(
            source="aggregate",
            is_aggregate=True,
            kalshi_env="demo",
            market_ticker="KXHIGHNY-26APR27-T69",
            station_id="KNYC",
            observed_at=datetime(2026, 4, 27, 18, 5, tzinfo=UTC),
            label="HEALTHY",
            score=0.96,
            success_score=1.0,
            freshness_score=1.0,
            completeness_score=0.9,
            consistency_score=1.0,
            payload={"sources": ["GFS", "ECMWF"]},
        )
        recent_health = await repo.list_recent_source_health_logs(
            kalshi_env="demo",
            aggregate_only=True,
            limit=1,
        )
        await session.commit()

    assert snapshot.market_ticker == "KXHIGHNY-26APR27-T69"
    assert snapshot.probability_output["p_bucket_yes"] == 0.62
    assert prior is not None
    assert prior.p_yes == 0.41
    assert health_log.label == "HEALTHY"
    assert recent_health[0].id == health_log.id

    await engine.dispose()
