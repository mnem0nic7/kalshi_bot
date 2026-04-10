from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "kalshi-bot"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_color: str = "blue"
    app_shadow_mode: bool = True
    app_auto_init_db: bool = False
    app_enable_kill_switch: bool = True

    database_url: str | None = None
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "kalshi_bot"
    postgres_user: str = "postgres"
    postgres_password: str | None = None

    kalshi_env: str = "demo"
    kalshi_read_api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_READ_API_KEY_ID", "KALSHI_API_KEY"),
    )
    kalshi_read_private_key_path: str | None = None
    kalshi_write_api_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("KALSHI_WRITE_API_KEY_ID", "KALSHI_API_KEY"),
    )
    kalshi_write_private_key_path: str | None = None
    live_kalshi_api_key: str | None = None
    live_kalshi_read_private_key_path: str | None = None
    live_kalshi_write_private_key_path: str | None = None
    demo_kalshi_api_key: str | None = None
    demo_kalshi_read_private_key_path: str | None = None
    demo_kalshi_write_private_key_path: str | None = None
    kalshi_subaccount: int = 0

    weather_user_agent: str = "kalshi-bot/0.1 (ops@example.com)"
    weather_market_map_path: str = "docs/examples/weather_markets.example.yaml"

    llm_hosted_base_url: str = "https://api.openai.com/v1"
    llm_hosted_api_key: str | None = None
    llm_hosted_model: str = "gpt-5.4"
    llm_local_base_url: str = "http://localhost:11434/v1"
    llm_local_api_key: str = "dummy"
    llm_local_model: str = "llama3.1:8b"
    llm_request_timeout_seconds: float = 30.0

    risk_max_order_notional_dollars: float = 50.0
    risk_max_position_notional_dollars: float = 250.0
    risk_daily_loss_limit_dollars: float = 100.0
    risk_max_order_count_fp: float = 25.0
    risk_stale_market_seconds: int = 30
    risk_stale_weather_seconds: int = 900
    risk_min_edge_bps: int = 50

    memory_embedding_dimensions: int = 16
    sse_poll_interval_seconds: float = 1.0
    research_stale_seconds: int = 900
    research_refresh_cooldown_seconds: int = 120
    research_web_max_results: int = 5
    research_web_max_queries: int = 2
    trigger_enable_auto_rooms: bool = False
    trigger_cooldown_seconds: int = 300
    trigger_max_spread_bps: int = 1200
    trigger_max_concurrent_rooms: int = 4
    daemon_reconcile_interval_seconds: int = 300
    daemon_heartbeat_interval_seconds: int = 60
    daemon_start_with_reconcile: bool = True

    def model_post_init(self, __context: object) -> None:
        if self.database_url:
            return
        auth = quote(self.postgres_user, safe="")
        if self.postgres_password:
            auth = f"{auth}:{quote(self.postgres_password, safe='')}"
        self.database_url = (
            f"postgresql+asyncpg://{auth}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def kalshi_rest_base_url(self) -> str:
        if self.kalshi_env == "production":
            return "https://api.elections.kalshi.com/trade-api/v2"
        return "https://demo-api.kalshi.co/trade-api/v2"

    @property
    def kalshi_websocket_url(self) -> str:
        if self.kalshi_env == "production":
            return "wss://api.elections.kalshi.com/trade-api/ws/v2"
        return "wss://demo-api.kalshi.co/trade-api/ws/v2"

    @property
    def weather_market_map_file(self) -> Path:
        return Path(self.weather_market_map_path)

    @property
    def execution_enabled(self) -> bool:
        return not self.app_shadow_mode

    def api_key_id(self, write: bool) -> str | None:
        direct = self.kalshi_write_api_key_id if write else self.kalshi_read_api_key_id
        if direct:
            return direct
        if self.kalshi_env == "production":
            return self.live_kalshi_api_key
        return self.demo_kalshi_api_key

    def key_path(self, write: bool) -> Path | None:
        raw = self.kalshi_write_private_key_path if write else self.kalshi_read_private_key_path
        if raw:
            return Path(raw)
        env_specific = None
        if self.kalshi_env == "production":
            env_specific = self.live_kalshi_write_private_key_path if write else self.live_kalshi_read_private_key_path
            if env_specific is None and write:
                env_specific = self.live_kalshi_read_private_key_path
        else:
            env_specific = self.demo_kalshi_write_private_key_path if write else self.demo_kalshi_read_private_key_path
            if env_specific is None and write:
                env_specific = self.demo_kalshi_read_private_key_path
        if env_specific:
            return Path(env_specific)
        fallback = Path("Kalshi-1.txt")
        return fallback if fallback.exists() else None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
