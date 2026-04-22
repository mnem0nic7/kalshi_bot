from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.web.auth import hash_session_token
from kalshi_bot.web.app import create_app


ALLOWED_EMAIL = "m7.ga.77@gmail.com"
PASSWORD = "s3cure-passphrase"


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _create_auth_enabled_app(tmp_path, monkeypatch):
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "auth.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    monkeypatch.setenv("WEB_AUTH_ENABLED", "true")
    monkeypatch.setenv("WEB_AUTH_ALLOWED_REGISTRATION_EMAILS", ALLOWED_EMAIL)
    get_settings.cache_clear()
    return create_app()


def test_root_redirects_to_login_when_auth_enabled(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    get_settings.cache_clear()


def test_api_requires_auth_when_auth_enabled(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/api/dashboard/strategies")

    assert response.status_code == 401
    assert response.json()["error"] == "auth_required"
    get_settings.cache_clear()


def test_healthz_stays_public_when_auth_enabled(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    get_settings.cache_clear()


def test_register_rejects_non_allowlisted_email(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/register",
            data={"email": "someone@example.com", "password": PASSWORD},
        )

    assert response.status_code == 403
    assert "This email is not eligible to register for this site." in response.text
    assert ALLOWED_EMAIL not in response.text
    get_settings.cache_clear()


def test_register_page_does_not_display_allowed_email(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/register")

    assert response.status_code == 200
    assert ALLOWED_EMAIL not in response.text
    assert "approved operator account" in response.text
    get_settings.cache_clear()


def test_register_allowlisted_email_creates_session_and_unlocks_site(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)
    settings = get_settings()

    with TestClient(app) as client:
        response = client.post(
            "/register",
            data={"email": ALLOWED_EMAIL, "password": PASSWORD},
            follow_redirects=False,
        )

        faq_response = client.get("/faq")

    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert client.cookies.get(settings.web_auth_cookie_name)
    assert faq_response.status_code == 200
    get_settings.cache_clear()


def test_login_rejects_wrong_password(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app) as client:
        register_response = client.post(
            "/register",
            data={"email": ALLOWED_EMAIL, "password": PASSWORD},
            follow_redirects=False,
        )
        assert register_response.status_code == 303

        logout_response = client.post("/logout", follow_redirects=False)
        assert logout_response.status_code == 303

        response = client.post(
            "/login",
            data={"email": ALLOWED_EMAIL, "password": "wrong-password"},
        )

    assert response.status_code == 401
    assert "Invalid email or password." in response.text
    get_settings.cache_clear()


def test_logout_clears_session_cookie(tmp_path, monkeypatch) -> None:
    app = _create_auth_enabled_app(tmp_path, monkeypatch)
    settings = get_settings()

    with TestClient(app) as client:
        client.post(
            "/register",
            data={"email": ALLOWED_EMAIL, "password": PASSWORD},
            follow_redirects=False,
        )
        assert client.cookies.get(settings.web_auth_cookie_name)

        response = client.post("/logout", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert settings.web_auth_cookie_name not in client.cookies
    get_settings.cache_clear()


def test_register_uses_shared_cookie_domain_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WEB_AUTH_COOKIE_DOMAIN", ".ai-al.site")
    app = _create_auth_enabled_app(tmp_path, monkeypatch)

    with TestClient(app, base_url="https://demo.ai-al.site") as client:
        response = client.post(
            "/register",
            data={"email": ALLOWED_EMAIL, "password": PASSWORD},
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert "Domain=.ai-al.site" in response.headers["set-cookie"]
    get_settings.cache_clear()


def test_authenticated_api_request_refreshes_session_expiry_and_cookie(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WEB_AUTH_SESSION_TTL_SECONDS", "60")
    app = _create_auth_enabled_app(tmp_path, monkeypatch)
    settings = get_settings()

    with TestClient(app) as client:
        register_response = client.post(
            "/register",
            data={"email": ALLOWED_EMAIL, "password": PASSWORD},
            follow_redirects=False,
        )
        assert register_response.status_code == 303
        cookie_token = client.cookies.get(settings.web_auth_cookie_name)
        assert cookie_token

        async def shorten_session_expiry() -> datetime:
            async with app.state.container.session_factory() as session:
                repo = PlatformRepository(session)
                session_record = await repo.get_web_session_by_token_hash(hash_session_token(cookie_token))
                assert session_record is not None
                session_record.expires_at = datetime.now(UTC) + timedelta(seconds=5)
                await session.commit()
                return session_record.expires_at

        shortened_expiry = asyncio.run(shorten_session_expiry())

        response = client.get("/api/dashboard/strategies")

        async def read_session_expiry() -> datetime:
            async with app.state.container.session_factory() as session:
                repo = PlatformRepository(session)
                session_record = await repo.get_web_session_by_token_hash(hash_session_token(cookie_token))
                assert session_record is not None
                await session.commit()
                return session_record.expires_at

        refreshed_expiry = asyncio.run(read_session_expiry())

    assert response.status_code == 200
    assert f"{settings.web_auth_cookie_name}=" in response.headers.get("set-cookie", "")
    assert "Max-Age=60" in response.headers.get("set-cookie", "")
    assert _as_utc(refreshed_expiry) > _as_utc(shortened_expiry) + timedelta(seconds=30)
    get_settings.cache_clear()
