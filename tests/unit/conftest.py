"""Unit-test hermeticity.

Unit tests must not inherit the developer/production ``.env``. Pydantic-settings
reads the repo ``.env`` for any field not passed explicitly, so production
band-pass overrides (e.g. ``CRYPTO_MAX_ENTRY_PRICE_DOLLARS``,
``RISK_MIN_CONTRACT_PRICE_DOLLARS``) leak into ``Settings(**kwargs)`` and break
hermetic assertions. Neutralize ``env_file`` for the duration of each unit test
so ``Settings`` resolves from code defaults + explicit kwargs + real os.environ
only (the autouse web-auth fixture in the root conftest still applies).
"""
from __future__ import annotations

import pytest

from kalshi_bot.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _hermetic_settings_env() -> None:
    original = Settings.model_config.get("env_file")
    Settings.model_config["env_file"] = None
    get_settings.cache_clear()
    try:
        yield
    finally:
        Settings.model_config["env_file"] = original
        get_settings.cache_clear()
