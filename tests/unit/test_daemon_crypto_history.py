from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.services.daemon import DaemonService


@pytest.mark.asyncio
async def test_crypto_history_cycle_collects_settled_before_bootstrap(tmp_path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class _HistoryService:
        async def collect_settled(self, **kwargs: object) -> dict[str, object]:
            calls.append(("collect_settled", kwargs))
            return {"status": "ok"}

        async def bootstrap(self, **kwargs: object) -> dict[str, object]:
            calls.append(("bootstrap", kwargs))
            return {"status": "ok"}

    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-history.db",
        crypto_history_auto_enabled=True,
        crypto_history_auto_lookback_days=2,
    )
    daemon.crypto_history_service = _HistoryService()

    async def active_color() -> bool:
        return True

    daemon._is_active_color = active_color  # type: ignore[method-assign]

    await daemon._run_crypto_history_cycle()

    assert calls == [
        ("collect_settled", {"days": 2, "frequency": "15m"}),
        ("bootstrap", {"days": 2, "frequency": "15m"}),
    ]


@pytest.mark.asyncio
async def test_crypto_history_cycle_collects_enabled_auto_frequencies(tmp_path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class _HistoryService:
        async def collect_settled(self, **kwargs: object) -> dict[str, object]:
            calls.append(("collect_settled", kwargs))
            return {"status": "ok"}

        async def bootstrap(self, **kwargs: object) -> dict[str, object]:
            calls.append(("bootstrap", kwargs))
            return {"status": "ok"}

    daemon = object.__new__(DaemonService)
    daemon.settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/daemon-crypto-history-1h.db",
        crypto_history_auto_enabled=True,
        crypto_history_auto_lookback_days=3,
        crypto_auto_frequencies="15m,1h",
        crypto_1h_enabled=True,
    )
    daemon.crypto_history_service = _HistoryService()

    async def active_color() -> bool:
        return True

    daemon._is_active_color = active_color  # type: ignore[method-assign]

    await daemon._run_crypto_history_cycle()

    assert calls == [
        ("collect_settled", {"days": 3, "frequency": "15m"}),
        ("bootstrap", {"days": 3, "frequency": "15m"}),
        ("collect_settled", {"days": 3, "frequency": "1h"}),
        ("bootstrap", {"days": 3, "frequency": "1h"}),
    ]
