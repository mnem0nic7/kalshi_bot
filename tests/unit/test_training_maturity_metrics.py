from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from kalshi_bot.config import Settings
from kalshi_bot.services.shadow_campaign import ShadowCampaignService
from kalshi_bot.services.training_corpus import TrainingCorpusService
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherSeriesTemplate


def test_settlement_urgency_bucket_boundaries() -> None:
    now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)

    assert ShadowCampaignService._settlement_urgency_bucket(None, now=now) == "later"
    assert (
        ShadowCampaignService._settlement_urgency_bucket(
            int((now + timedelta(hours=6)).timestamp()),
            now=now,
        )
        == "closing_soon"
    )
    assert (
        ShadowCampaignService._settlement_urgency_bucket(
            int((now + timedelta(hours=6, seconds=1)).timestamp()),
            now=now,
        )
        == "closing_today"
    )
    assert (
        ShadowCampaignService._settlement_urgency_bucket(
            int((now + timedelta(hours=24)).timestamp()),
            now=now,
        )
        == "closing_today"
    )
    assert (
        ShadowCampaignService._settlement_urgency_bucket(
            int((now + timedelta(hours=24, seconds=1)).timestamp()),
            now=now,
        )
        == "later"
    )


def test_training_corpus_partitions_active_and_legacy_failure_counts() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///ignored.db")
    directory = WeatherMarketDirectory(
        {},
        {
            "KXHIGHNY": WeatherSeriesTemplate(
                series_ticker="KXHIGHNY",
                display_name="NYC Daily High Temperature",
                station_id="KNYC",
                location_name="NYC",
                latitude=40.0,
                longitude=-73.0,
            )
        },
    )
    service = TrainingCorpusService(
        settings,
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        directory,
    )

    active, legacy = service._partition_failed_reason_counts(
        [
            SimpleNamespace(market_ticker="KXHIGHNY-26APR12-T70", error_text="404 market not found"),
            SimpleNamespace(market_ticker="WEATHER-NYC-HIGH-80F", error_text="404 market not found"),
            SimpleNamespace(market_ticker=None, error_text="weather source timeout"),
        ]
    )

    assert active == {"market lookup failures": 1}
    assert legacy == {
        "market lookup failures": 1,
        "weather source failures": 1,
    }
