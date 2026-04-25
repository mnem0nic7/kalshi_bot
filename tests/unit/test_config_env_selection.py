from pathlib import Path

import pytest

from kalshi_bot.config import Settings


def test_demo_prefixed_credentials_are_selected() -> None:
    settings = Settings(
        kalshi_env="demo",
        demo_kalshi_api_key="demo-key-id",
        demo_kalshi_read_private_key_path="Kalshi-2-Demo.txt",
        demo_kalshi_write_private_key_path="Kalshi-2-Demo.txt",
    )

    assert settings.api_key_id(write=False) == "demo-key-id"
    assert settings.api_key_id(write=True) == "demo-key-id"
    assert settings.key_path(write=False) == Path("Kalshi-2-Demo.txt")
    assert settings.key_path(write=True) == Path("Kalshi-2-Demo.txt")


def test_database_url_is_derived_from_postgres_components() -> None:
    settings = Settings(
        database_url=None,
        postgres_host="db.internal",
        postgres_port=5433,
        postgres_db="kalshi_bot",
        postgres_user="bot",
        postgres_password="safe-local-password",
    )

    assert settings.database_url == "postgresql+asyncpg://bot:safe-local-password@db.internal:5433/kalshi_bot"


def test_database_url_without_password_omits_empty_secret_slot() -> None:
    settings = Settings(
        database_url=None,
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="kalshi_bot",
        postgres_user="postgres",
        postgres_password=None,
    )

    assert settings.database_url == "postgresql+asyncpg://postgres@localhost:5432/kalshi_bot"


def test_database_url_escapes_reserved_characters_in_credentials() -> None:
    settings = Settings(
        database_url=None,
        postgres_host="db.internal",
        postgres_port=5432,
        postgres_db="kalshi_bot",
        postgres_user="bot@ops",
        postgres_password="p@ss:word/with?chars",
    )

    assert (
        settings.database_url
        == "postgresql+asyncpg://bot%40ops:p%40ss%3Aword%2Fwith%3Fchars@db.internal:5432/kalshi_bot"
    )


def test_gemini_key_alias_is_accepted(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_KEY", "alias-key")

    settings = Settings()

    assert settings.gemini_api_key == "alias-key"


def test_auto_evolve_gate_settings_have_safe_defaults() -> None:
    settings = Settings()

    assert settings.strategy_auto_evolve_min_improvement_bps == 100
    assert settings.strategy_auto_evolve_min_city_improvement_bps == 100
    assert settings.strategy_auto_evolve_max_regression_bps == 50
    assert settings.strategy_auto_evolve_max_run_age_seconds == 172800
    assert settings.strategy_auto_evolve_min_corpus_rows == 500
    assert settings.strategy_auto_evolve_min_corpus_cities == 3
    assert settings.strategy_auto_evolve_min_city_rows == 25
    assert settings.strategy_auto_evolve_cooldown_seconds == 86400
    assert settings.strategy_auto_evolve_greenfield_enabled is False
    assert settings.strategy_auto_evolve_reference_strategy_name is None
    assert settings.strategy_auto_evolve_reference_run_id is None


def test_auto_evolve_activate_requires_accept_suggestions() -> None:
    with pytest.raises(ValueError, match="accept_suggestions"):
        Settings(
            strategy_auto_evolve_accept_suggestions=False,
            strategy_auto_evolve_activate_suggestions=True,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("strategy_auto_evolve_min_improvement_bps", -1),
        ("strategy_auto_evolve_max_regression_bps", -1),
        ("strategy_auto_evolve_max_run_age_seconds", 0),
        ("strategy_auto_evolve_min_corpus_rows", -1),
        ("strategy_auto_evolve_min_corpus_cities", -1),
        ("strategy_auto_evolve_min_city_rows", -1),
        ("strategy_auto_evolve_cooldown_seconds", -1),
        ("strategy_auto_evolve_max_cities_per_cycle", -1),
    ],
)
def test_auto_evolve_gate_settings_validate_ranges(field_name: str, value: int) -> None:
    with pytest.raises(ValueError, match=field_name):
        Settings(**{field_name: value})


def test_auto_evolve_threshold_delta_pct_is_bounded() -> None:
    with pytest.raises(ValueError, match="strategy_auto_evolve_max_threshold_delta_pct"):
        Settings(strategy_auto_evolve_max_threshold_delta_pct=1.01)


def test_auto_evolve_reference_settings_are_stripped() -> None:
    settings = Settings(
        strategy_auto_evolve_reference_strategy_name="  baseline  ",
        strategy_auto_evolve_reference_run_id="  run-123  ",
    )

    assert settings.strategy_auto_evolve_reference_strategy_name == "baseline"
    assert settings.strategy_auto_evolve_reference_run_id == "run-123"


def test_strategy_corpus_excluded_date_ranges_are_validated() -> None:
    settings = Settings(strategy_corpus_excluded_date_ranges=" 2026-04-19/2026-04-23 ")

    assert settings.strategy_corpus_excluded_date_ranges == "2026-04-19/2026-04-23"


@pytest.mark.parametrize(
    "raw_ranges",
    [
        "2026-04-19",
        "2026-04-23/2026-04-19",
        "not-a-date/2026-04-19",
    ],
)
def test_strategy_corpus_excluded_date_ranges_reject_invalid_values(raw_ranges: str) -> None:
    with pytest.raises(ValueError, match="strategy_corpus_excluded_date_ranges"):
        Settings(strategy_corpus_excluded_date_ranges=raw_ranges)
