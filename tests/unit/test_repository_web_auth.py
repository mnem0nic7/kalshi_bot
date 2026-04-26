from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kalshi_bot.db.models import WebSession, WebUser
from kalshi_bot.db.repositories import PlatformRepository


class FakeWebAuthSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.flush_count = 0

    def add(self, record: object) -> None:
        self.added.append(record)

    async def flush(self) -> None:
        self.flush_count += 1


@pytest.mark.asyncio
async def test_web_auth_repository_slice_creates_user_and_session_records() -> None:
    session = FakeWebAuthSession()
    repo = PlatformRepository(session, kalshi_env="demo")  # type: ignore[arg-type]
    expires_at = datetime.now(UTC) + timedelta(hours=1)

    user = await repo.create_web_user(
        email="operator@example.com",
        password_hash="hash",
        password_salt="salt",
    )
    web_session = await repo.create_web_session(
        user_id="user-1",
        token_hash="token-hash",
        expires_at=expires_at,
    )

    assert isinstance(user, WebUser)
    assert user.email == "operator@example.com"
    assert user.is_active is True
    assert isinstance(web_session, WebSession)
    assert web_session.user_id == "user-1"
    assert web_session.token_hash == "token-hash"
    assert web_session.expires_at is expires_at
    assert session.added == [user, web_session]
    assert session.flush_count == 2


@pytest.mark.asyncio
async def test_web_auth_repository_slice_touches_sessions_and_records_logins() -> None:
    session = FakeWebAuthSession()
    repo = PlatformRepository(session, kalshi_env="demo")  # type: ignore[arg-type]
    user = WebUser(
        id="user-1",
        email="operator@example.com",
        password_hash="hash",
        password_salt="salt",
        is_active=True,
    )
    web_session = WebSession(
        id="session-1",
        user_id="user-1",
        token_hash="token-hash",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    seen_at = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
    expires_at = datetime(2026, 4, 26, 13, 0, tzinfo=UTC)

    async def get_web_user(user_id: str) -> WebUser | None:
        assert user_id == "user-1"
        return user

    async def get_web_session(session_id: str) -> WebSession | None:
        assert session_id == "session-1"
        return web_session

    repo.get_web_user = get_web_user  # type: ignore[method-assign]
    repo.get_web_session = get_web_session  # type: ignore[method-assign]

    touched_user = await repo.record_web_user_login("user-1", logged_in_at=seen_at)
    touched_session = await repo.touch_web_session("session-1", seen_at=seen_at, expires_at=expires_at)

    assert touched_user is user
    assert user.last_login_at is seen_at
    assert touched_session is web_session
    assert web_session.last_seen_at is seen_at
    assert web_session.expires_at is expires_at
    assert session.flush_count == 2
