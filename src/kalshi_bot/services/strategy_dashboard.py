from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.services.strategy_regression import WINDOW_DAYS as DEFAULT_STRATEGY_WINDOW_DAYS
from kalshi_bot.weather.mapping import WeatherMarketDirectory


class StrategyDashboardService:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        strategy_codex_service: Any | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.strategy_codex_service = strategy_codex_service

    async def build_dashboard(
        self,
        *,
        window_days: int = DEFAULT_STRATEGY_WINDOW_DAYS,
        series_ticker: str | None = None,
        strategy_name: str | None = None,
        include_codex_lab: bool = False,
    ) -> dict[str, Any]:
        from kalshi_bot.web.control_room import build_strategies_dashboard_core

        container = SimpleNamespace(
            session_factory=self.session_factory,
            weather_directory=self.weather_directory,
            strategy_codex_service=self.strategy_codex_service,
        )
        return await build_strategies_dashboard_core(
            container,
            window_days=window_days,
            series_ticker=series_ticker,
            strategy_name=strategy_name,
            include_codex_lab=include_codex_lab,
        )
