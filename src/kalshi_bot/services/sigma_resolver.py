from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.weather.sigma_calibration import load_active_lead_factors, load_active_sigma_params


@dataclass(slots=True)
class SigmaCalibrationSnapshot:
    sigma_params: dict
    lead_factors: dict[str, float]
    loaded_at: datetime


class SigmaResolver:
    """Load active sigma calibration with a small in-process TTL cache."""

    def __init__(self, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self._cache: SigmaCalibrationSnapshot | None = None
        self._lock = asyncio.Lock()

    async def snapshot(self, *, force: bool = False) -> SigmaCalibrationSnapshot:
        now = datetime.now(UTC)
        ttl = timedelta(hours=1)
        if not force and self._cache is not None and now - self._cache.loaded_at < ttl:
            return self._cache
        async with self._lock:
            now = datetime.now(UTC)
            if not force and self._cache is not None and now - self._cache.loaded_at < ttl:
                return self._cache
            if not self.settings.sigma_calibration_enabled:
                snapshot = SigmaCalibrationSnapshot({}, {}, now)
            else:
                async with self.session_factory() as session:
                    snapshot = SigmaCalibrationSnapshot(
                        sigma_params=await load_active_sigma_params(session),
                        lead_factors=await load_active_lead_factors(session),
                        loaded_at=now,
                    )
            self._cache = snapshot
            return snapshot
