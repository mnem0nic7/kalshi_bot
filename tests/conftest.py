from __future__ import annotations

import pytest

from kalshi_bot.config import get_settings


@pytest.fixture(autouse=True)
def disable_web_auth_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEB_AUTH_ENABLED", "false")
    get_settings.cache_clear()
