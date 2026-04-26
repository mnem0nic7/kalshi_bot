from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import delete as sa_delete
from sqlalchemy import select

from kalshi_bot.db.models import WebSession, WebUser


class WebAuthRepositoryMixin:
    async def get_web_user(self, user_id: str) -> WebUser | None:
        stmt = select(WebUser).where(WebUser.id == user_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_web_user_by_email(self, email: str) -> WebUser | None:
        stmt = select(WebUser).where(WebUser.email == email)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def create_web_user(
        self,
        *,
        email: str,
        password_hash: str,
        password_salt: str,
        is_active: bool = True,
    ) -> WebUser:
        user = WebUser(
            email=email,
            password_hash=password_hash,
            password_salt=password_salt,
            is_active=is_active,
        )
        self.session.add(user)
        await self.session.flush()
        return user

    async def record_web_user_login(self, user_id: str, *, logged_in_at: datetime | None = None) -> WebUser | None:
        user = await self.get_web_user(user_id)
        if user is None:
            return None
        user.last_login_at = logged_in_at or datetime.now(UTC)
        await self.session.flush()
        return user

    async def create_web_session(
        self,
        *,
        user_id: str,
        token_hash: str,
        expires_at: datetime,
        last_seen_at: datetime | None = None,
    ) -> WebSession:
        record = WebSession(
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at,
            last_seen_at=last_seen_at,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_web_session(self, session_id: str) -> WebSession | None:
        stmt = select(WebSession).where(WebSession.id == session_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_web_session_by_token_hash(self, token_hash: str) -> WebSession | None:
        stmt = select(WebSession).where(WebSession.token_hash == token_hash)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def touch_web_session(
        self,
        session_id: str,
        *,
        seen_at: datetime | None = None,
        expires_at: datetime | None = None,
    ) -> WebSession | None:
        record = await self.get_web_session(session_id)
        if record is None:
            return None
        record.last_seen_at = seen_at or datetime.now(UTC)
        if expires_at is not None:
            record.expires_at = expires_at
        await self.session.flush()
        return record

    async def delete_web_session(self, session_id: str) -> int:
        stmt = sa_delete(WebSession).where(WebSession.id == session_id)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount or 0

    async def delete_web_session_by_token_hash(self, token_hash: str) -> int:
        stmt = sa_delete(WebSession).where(WebSession.token_hash == token_hash)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount or 0

    async def prune_expired_web_sessions(self, *, now: datetime | None = None) -> int:
        cutoff = now or datetime.now(UTC)
        stmt = sa_delete(WebSession).where(WebSession.expires_at < cutoff)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount or 0
