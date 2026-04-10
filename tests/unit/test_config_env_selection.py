from pathlib import Path

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
