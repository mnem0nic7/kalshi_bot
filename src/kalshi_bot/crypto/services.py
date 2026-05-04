from __future__ import annotations

import hashlib
import json
import logging
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import (
    AgentRole,
    ContractSide,
    MessageKind,
    RiskStatus,
    RoomOrigin,
    RoomStage,
    StandDownReason,
    StrategyCode,
    TradeAction,
    WeatherResolutionState,
)
from kalshi_bot.core.fixed_point import make_client_order_id, quantize_count, quantize_price
from kalshi_bot.core.schemas import ExecReceiptPayload, RoomCreate, RoomMessageCreate, TradeEligibilityVerdict, TradeTicket
from kalshi_bot.crypto.models import CryptoMarket, CryptoSeries
from kalshi_bot.crypto.parsing import (
    normalize_candlestick,
    normalize_frequency,
    parse_crypto_market,
    parse_crypto_series,
)
from kalshi_bot.db.models import CryptoMarketCandlestickRecord, CryptoMarketSnapshotRecord, Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.fee_model import current_fee_model_version, estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext, approved_ticket_for_verdict
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars

logger = logging.getLogger(__name__)

CRYPTO_ASSET_MODES_KEY = "crypto_asset_modes"
CRYPTO_ASSET_MODE_OFF = "off"
CRYPTO_ASSET_MODE_SHADOW = "shadow"
CRYPTO_ASSET_MODE_LIVE = "live"
CRYPTO_ASSET_MODES = {
    CRYPTO_ASSET_MODE_OFF,
    CRYPTO_ASSET_MODE_SHADOW,
    CRYPTO_ASSET_MODE_LIVE,
}


def _version(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{digest}"


def _money_text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _clamp_price(value: Decimal) -> Decimal:
    return quantize_price(min(Decimal("0.9900"), max(Decimal("0.0100"), value)))


def _rows_from_response(response: dict[str, Any], key: str) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [row for row in response if isinstance(row, dict)]
    rows = response.get(key)
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def normalize_asset_symbol(asset_symbol: str) -> str:
    normalized = "".join(ch for ch in str(asset_symbol or "").strip().upper() if ch.isalnum())
    if not normalized:
        raise ValueError("asset_symbol is required")
    return normalized


def normalize_asset_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized not in CRYPTO_ASSET_MODES:
        raise ValueError(f"unsupported crypto asset mode: {mode}")
    return normalized


class CryptoAssetControlService:
    def __init__(self, *, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    @staticmethod
    def normalize_symbol(asset_symbol: str) -> str:
        return normalize_asset_symbol(asset_symbol)

    @staticmethod
    def normalize_mode(mode: str) -> str:
        return normalize_asset_mode(mode)

    def modes_from_notes(self, notes: dict[str, Any] | None) -> dict[str, str]:
        raw_modes = (notes or {}).get(CRYPTO_ASSET_MODES_KEY) or {}
        if not isinstance(raw_modes, dict):
            return {}
        modes: dict[str, str] = {}
        for raw_symbol, raw_mode in raw_modes.items():
            try:
                symbol = normalize_asset_symbol(str(raw_symbol))
                mode = normalize_asset_mode(str(raw_mode))
            except ValueError:
                continue
            modes[symbol] = mode
        return modes

    def mode_for_control(self, control: Any, asset_symbol: str) -> str:
        symbol = normalize_asset_symbol(asset_symbol)
        return self.modes_from_notes(getattr(control, "notes", None)).get(symbol, CRYPTO_ASSET_MODE_SHADOW)

    def asset_mode_summary(
        self,
        *,
        asset_symbols: list[str] | None,
        modes: dict[str, str],
    ) -> dict[str, Any]:
        symbols = {normalize_asset_symbol(symbol) for symbol in (asset_symbols or []) if str(symbol or "").strip()}
        symbols.update(modes)
        resolved = {symbol: modes.get(symbol, CRYPTO_ASSET_MODE_SHADOW) for symbol in sorted(symbols)}
        counts = {mode: 0 for mode in sorted(CRYPTO_ASSET_MODES)}
        for mode in resolved.values():
            counts[mode] = counts.get(mode, 0) + 1
        return {"modes": resolved, "counts": counts}

    def global_live_blockers(
        self,
        *,
        control: Any,
        replay_gate: Any | None,
        has_write_credentials: bool,
        frequency: str = "15m",
    ) -> list[str]:
        blockers: list[str] = []
        normalized_frequency = normalize_frequency(frequency) or "15m"
        if not self.settings.crypto_enabled:
            blockers.append("Crypto is disabled.")
        if normalized_frequency == "15m" and not self.settings.crypto_15m_enabled:
            blockers.append("15-minute crypto is disabled.")
        if not self.settings.crypto_trading_enabled:
            blockers.append("Global crypto trading is disabled.")
        if self.settings.app_shadow_mode:
            blockers.append("App shadow mode is enabled.")
        if getattr(control, "kill_switch_enabled", False):
            blockers.append("Kill switch is enabled.")
        active_color = str(getattr(control, "active_color", "") or "")
        if active_color and active_color != self.settings.app_color:
            blockers.append(f"Active color is {active_color}; this app is {self.settings.app_color}.")
        gate_status = getattr(replay_gate, "status", None) if replay_gate is not None else None
        if gate_status != "passed":
            blockers.append(f"Crypto replay gate is {gate_status or 'missing'}.")
        if not has_write_credentials:
            blockers.append("Kalshi write credentials are missing.")
        return blockers

    def market_live_status(
        self,
        *,
        control: Any,
        replay_gate: Any | None,
        market: CryptoMarket,
        has_write_credentials: bool,
    ) -> dict[str, Any]:
        mode = self.mode_for_control(control, market.asset_symbol)
        global_blockers = self.global_live_blockers(
            control=control,
            replay_gate=replay_gate,
            has_write_credentials=has_write_credentials,
            frequency=market.frequency,
        )
        blockers = list(global_blockers) if mode == CRYPTO_ASSET_MODE_LIVE else [
            f"Asset {market.asset_symbol} mode is {mode}; set it to live to allow live orders."
        ]
        return {
            "asset_mode": mode,
            "live_eligible": mode == CRYPTO_ASSET_MODE_LIVE and not global_blockers,
            "live_blockers": blockers,
            "global_live_blockers": global_blockers,
        }

    async def list_asset_modes(
        self,
        *,
        asset_symbols: list[str] | None = None,
        kalshi_env: str | None = None,
    ) -> dict[str, Any]:
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            control = await repo.get_deployment_control(kalshi_env=env)
            modes = self.modes_from_notes(control.notes)
            await session.commit()
        return self.asset_mode_summary(asset_symbols=asset_symbols, modes=modes)

    async def set_asset_mode(
        self,
        asset_symbol: str,
        mode: str,
        *,
        kalshi_env: str | None = None,
        actor: str = "operator",
    ) -> dict[str, Any]:
        symbol = normalize_asset_symbol(asset_symbol)
        normalized_mode = normalize_asset_mode(mode)
        env = kalshi_env or self.settings.kalshi_env
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=env)
            previous_mode = CRYPTO_ASSET_MODE_SHADOW

            def update_modes(previous_value: Any) -> dict[str, str]:
                nonlocal previous_mode
                modes = self.modes_from_notes({CRYPTO_ASSET_MODES_KEY: previous_value})
                previous_mode = modes.get(symbol, CRYPTO_ASSET_MODE_SHADOW)
                modes[symbol] = normalized_mode
                return modes

            control, _ = await repo.update_deployment_note_key(
                CRYPTO_ASSET_MODES_KEY,
                update_modes,
                kalshi_env=env,
            )
            await repo.log_ops_event(
                severity="info",
                summary=f"Crypto asset mode set: {symbol} {normalized_mode}",
                source="crypto_asset_control",
                payload={
                    "asset_symbol": symbol,
                    "mode": normalized_mode,
                    "previous_mode": previous_mode,
                    "actor": actor,
                    "kalshi_env": env,
                },
                kalshi_env=env,
            )
            await session.commit()
        return {
            "status": "ok",
            "asset_symbol": symbol,
            "mode": normalized_mode,
            "previous_mode": previous_mode,
            "asset_modes": self.modes_from_notes(control.notes),
        }


class CryptoMarketService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
        agent_pack_service: AgentPackService,
        asset_control_service: CryptoAssetControlService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.agent_pack_service = agent_pack_service
        self.asset_control_service = asset_control_service

    async def discover_series(self, *, frequency: str = "15m") -> list[CryptoSeries]:
        if not self.settings.crypto_enabled:
            return []
        wanted = normalize_frequency(frequency) or "15m"
        cursor: str | None = None
        seen_cursors: set[str] = set()
        series: list[CryptoSeries] = []
        for _ in range(20):
            params: dict[str, Any] = {"category": "Crypto", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            response = await self.kalshi.list_series(**params)
            for row in _rows_from_response(response, "series"):
                parsed = parse_crypto_series(row, frequency=wanted)
                if parsed is not None:
                    series.append(parsed)
            cursor = response.get("cursor") or response.get("next_cursor")
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        deduped: dict[str, CryptoSeries] = {item.series_ticker: item for item in series}
        return sorted(deduped.values(), key=lambda item: item.asset_symbol)

    async def discover_markets(
        self,
        *,
        frequency: str = "15m",
        status: str | None = "open",
        persist: bool = True,
    ) -> list[CryptoMarket]:
        series_rows = await self.discover_series(frequency=frequency)
        markets: list[CryptoMarket] = []
        for series in series_rows:
            response = await self.kalshi.list_markets(
                series_ticker=series.series_ticker,
                limit=1000,
                **({"status": status} if status else {}),
            )
            for row in _rows_from_response(response, "markets"):
                parsed = parse_crypto_market(row, series=series, frequency=frequency)
                if parsed is not None:
                    markets.append(parsed)
        markets.sort(key=lambda market: (market.close_time or datetime.max.replace(tzinfo=UTC), market.asset_symbol))
        if persist and markets:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                for market in markets:
                    await self.record_market_snapshot(repo, market, source_kind="live")
                await session.commit()
        return markets

    async def get_market(self, market_ticker: str, *, persist: bool = True) -> CryptoMarket:
        response = await self.kalshi.get_market(market_ticker)
        market = parse_crypto_market(response, frequency="15m")
        if market is None:
            raise KeyError(market_ticker)
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                await self.record_market_snapshot(repo, market, source_kind="live")
                await session.commit()
        return market

    async def record_market_snapshot(
        self,
        repo: PlatformRepository,
        market: CryptoMarket,
        *,
        source_kind: str,
        observed_at: datetime | None = None,
    ) -> CryptoMarketSnapshotRecord:
        return await repo.record_crypto_market_snapshot(
            kalshi_env=self.settings.kalshi_env,
            series_ticker=market.series_ticker,
            market_ticker=market.market_ticker,
            event_ticker=market.event_ticker,
            asset_symbol=market.asset_symbol,
            frequency=market.frequency,
            title=market.title,
            status=market.status,
            open_time=market.open_time,
            close_time=market.close_time,
            expected_expiration_time=market.expected_expiration_time,
            target_price_dollars=market.target_price_dollars,
            yes_bid_dollars=market.yes_bid_dollars,
            yes_ask_dollars=market.yes_ask_dollars,
            no_bid_dollars=market.no_bid_dollars,
            no_ask_dollars=market.no_ask_dollars,
            last_price_dollars=market.last_price_dollars,
            volume=market.volume,
            open_interest=market.open_interest,
            settlement_result=market.settlement_result,
            observed_at=observed_at or datetime.now(UTC),
            source_kind=source_kind,
            payload=market.to_payload(),
        )

    async def dashboard_payload(self, *, frequency: str = "15m", current_only: bool = True) -> dict[str, Any]:
        try:
            markets = await self.discover_markets(frequency=frequency, status="open", persist=True)
            source = "kalshi_live"
        except Exception:
            logger.warning("crypto market discovery failed; using stored snapshots", exc_info=True)
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                rows = await repo.list_latest_crypto_market_snapshots(frequency=normalize_frequency(frequency) or "15m")
                await session.commit()
            markets = [_market_from_snapshot(row) for row in rows]
            source = "stored_snapshots"
        total_open_markets = len(markets)
        if current_only:
            markets = _nearest_market_per_asset(markets)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            signal_payloads = await repo.latest_signal_payloads_for_markets(
                market_tickers=[market.market_ticker for market in markets],
                kalshi_env=self.settings.kalshi_env,
            )
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            active_rooms: dict[str, dict[str, str]] = {}
            for market in markets:
                room = await repo.get_latest_active_room_for_market(
                    market.market_ticker,
                    kalshi_env=self.settings.kalshi_env,
                )
                if room is not None:
                    active_rooms[market.market_ticker] = {"id": room.id, "stage": room.stage}
            await session.commit()
        gate_payload = gate.payload if gate is not None else {}
        asset_symbols = sorted({market.asset_symbol for market in markets})
        mode_summary = self.asset_control_service.asset_mode_summary(
            asset_symbols=asset_symbols,
            modes=self.asset_control_service.modes_from_notes(control.notes),
        )
        global_live_blockers = self.asset_control_service.global_live_blockers(
            control=control,
            replay_gate=gate,
            has_write_credentials=self.kalshi.write_credentials is not None,
            frequency=frequency,
        )
        return {
            "market_domain": "crypto",
            "frequency": normalize_frequency(frequency) or "15m",
            "source": source,
            "total_open_markets": total_open_markets,
            "current_only": current_only,
            "settings": {
                "crypto_enabled": self.settings.crypto_enabled,
                "crypto_15m_enabled": self.settings.crypto_15m_enabled,
                "crypto_trading_enabled": self.settings.crypto_trading_enabled,
                "crypto_autonomy_enabled": self.settings.crypto_autonomy_enabled,
                "crypto_order_mode": self.settings.crypto_order_mode,
            },
            "asset_modes": mode_summary["modes"],
            "asset_mode_counts": mode_summary["counts"],
            "global_live_blockers": global_live_blockers,
            "replay_gate": {
                "status": gate.status if gate is not None else "missing",
                "version": gate.version if gate is not None else None,
                "metrics": gate.metrics if gate is not None else {},
                "payload": gate_payload,
            },
            "markets": [
                {
                    **market.to_payload(),
                    **self.asset_control_service.market_live_status(
                        control=control,
                        replay_gate=gate,
                        market=market,
                        has_write_credentials=self.kalshi.write_credentials is not None,
                    ),
                    "signal": signal_payloads.get(market.market_ticker),
                    "active_room": active_rooms.get(market.market_ticker),
                }
                for market in markets
            ],
            "updated_at": datetime.now(UTC).isoformat(),
        }

    async def create_room_for_market(self, market_ticker: str, *, reason: str = "crypto_dashboard") -> dict[str, Any]:
        market = await self.get_market(market_ticker, persist=True)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.ensure_deployment_control(self.settings.app_color)
            pack = await self.agent_pack_service.get_pack_for_color(repo, control.active_color)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            live_status = self.asset_control_service.market_live_status(
                control=control,
                replay_gate=gate,
                market=market,
                has_write_credentials=self.kalshi.write_credentials is not None,
            )
            shadow_mode = self.settings.app_shadow_mode or not live_status["live_eligible"]
            room = await repo.create_room(
                RoomCreate(
                    name=f"{market.asset_symbol} 15 Minute Crypto",
                    market_ticker=market.market_ticker,
                    prompt=(
                        "Crypto 15m workflow. "
                        f"asset={market.asset_symbol} target={_money_text(market.target_price_dollars)} "
                        f"close_time={market.close_time.isoformat() if market.close_time else 'unknown'} "
                        f"asset_mode={live_status['asset_mode']} live_eligible={live_status['live_eligible']} "
                        f"reason={reason}"
                    ),
                ),
                active_color=control.active_color,
                shadow_mode=shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self.settings.kalshi_env,
                room_origin=RoomOrigin.SHADOW.value if shadow_mode else RoomOrigin.LIVE.value,
                agent_pack_version=pack.version,
            )
            await repo.save_artifact(
                room_id=room.id,
                artifact_type="market_snapshot",
                source="crypto_market_service",
                title=f"{market.asset_symbol} crypto market snapshot",
                payload={
                    "market_domain": "crypto",
                    "frequency": market.frequency,
                    "strategy_code": StrategyCode.CRYPTO_15M.value,
                    "asset_mode": live_status["asset_mode"],
                    "live_eligible": live_status["live_eligible"],
                    "live_blockers": live_status["live_blockers"],
                    "global_live_blockers": live_status["global_live_blockers"],
                    "reason": reason,
                    "market": market.to_payload(),
                },
            )
            await session.commit()
        return {
            "room_id": room.id,
            "redirect": f"/rooms/{room.id}",
            "market_ticker": market.market_ticker,
            "asset_symbol": market.asset_symbol,
            "asset_mode": live_status["asset_mode"],
            "live_eligible": live_status["live_eligible"],
            "live_blockers": live_status["live_blockers"],
        }

    async def status(self, *, frequency: str = "15m") -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            snapshots = await repo.list_latest_crypto_market_snapshots(frequency=normalize_frequency(frequency) or "15m")
            model = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=normalize_frequency(frequency) or "15m",
                artifact_type="backtest",
                kalshi_env=self.settings.kalshi_env,
            )
            all_snapshots = await repo.list_crypto_market_snapshots(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                limit=100_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                limit=200_000,
            )
            await session.commit()
        asset_symbols = sorted({snapshot.asset_symbol for snapshot in snapshots})
        mode_summary = self.asset_control_service.asset_mode_summary(
            asset_symbols=asset_symbols,
            modes=self.asset_control_service.modes_from_notes(control.notes),
        )
        return {
            "market_domain": "crypto",
            "frequency": normalize_frequency(frequency) or "15m",
            "crypto_enabled": self.settings.crypto_enabled,
            "crypto_15m_enabled": self.settings.crypto_15m_enabled,
            "crypto_trading_enabled": self.settings.crypto_trading_enabled,
            "crypto_autonomy_enabled": self.settings.crypto_autonomy_enabled,
            "stored_market_count": len(snapshots),
            "asset_modes": mode_summary["modes"],
            "asset_mode_counts": mode_summary["counts"],
            "global_live_blockers": self.asset_control_service.global_live_blockers(
                control=control,
                replay_gate=gate,
                has_write_credentials=self.kalshi.write_credentials is not None,
                frequency=frequency,
            ),
            "model": _artifact_summary(model),
            "backtest": _artifact_summary(backtest),
            "replay_gate": _artifact_summary(gate),
            "data_quality": _crypto_data_quality(
                all_snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            ),
        }

    async def is_crypto_room(self, room_id: str) -> bool:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            room = await repo.get_room(room_id)
            if room is None:
                return False
            artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="market_snapshot")
            if artifact is not None and (artifact.payload or {}).get("market_domain") == "crypto":
                return True
            snapshot = await repo.get_latest_crypto_market_snapshot(room.market_ticker, kalshi_env=room.kalshi_env)
            await session.commit()
        return snapshot is not None


class CryptoHistoryService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
        market_service: CryptoMarketService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.market_service = market_service

    async def bootstrap(self, *, days: int | None = None, frequency: str = "15m") -> dict[str, Any]:
        lookback_days = days or self.settings.crypto_history_lookback_days
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        live_markets = await self.market_service.discover_markets(frequency=frequency, status="open", persist=True)
        historical_markets: list[CryptoMarket] = []
        series_rows = await self.market_service.discover_series(frequency=frequency)
        errors: list[dict[str, str]] = []
        series_stats: list[dict[str, Any]] = []
        for series in series_rows:
            result = await self._list_historical_markets(series.series_ticker)
            errors.extend({"series_ticker": series.series_ticker, "error": error} for error in result["errors"])
            markets_in_window = 0
            for row in result["rows"]:
                parsed = parse_crypto_market(row, series=series, frequency=frequency)
                if parsed is None:
                    continue
                if parsed.close_time is None or parsed.close_time >= cutoff:
                    markets_in_window += 1
                    historical_markets.append(parsed)
            series_stats.append(
                {
                    "series_ticker": series.series_ticker,
                    "asset_symbol": series.asset_symbol,
                    "pages_fetched": result["pages_fetched"],
                    "rows_seen": result["rows_seen"],
                    "markets_in_window": markets_in_window,
                    "errors": result["errors"],
                }
            )
        all_markets = {market.market_ticker: market for market in [*historical_markets, *live_markets]}
        historical_tickers = {market.market_ticker for market in historical_markets}
        candle_stats: dict[str, Any] = {
            "stored": 0,
            "markets_attempted": 0,
            "markets_skipped_existing": 0,
            "errors": [],
        }
        commit_batch_size = 250
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for index, market in enumerate(all_markets.values(), start=1):
                await self.market_service.record_market_snapshot(
                    repo,
                    market,
                    source_kind="historical" if market.market_ticker in historical_tickers else "live",
                    observed_at=market.close_time or datetime.now(UTC),
                )
                capture = await self._capture_candles(repo, market, cutoff=cutoff)
                candle_stats["stored"] += int(capture["stored"])
                if capture["status"] == "skipped_existing":
                    candle_stats["markets_skipped_existing"] += 1
                else:
                    candle_stats["markets_attempted"] += 1
                if capture.get("error"):
                    candle_stats["errors"].append({"market_ticker": market.market_ticker, "error": capture["error"]})
                if index % commit_batch_size == 0:
                    await session.commit()
            await session.commit()
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=100_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=normalize_frequency(frequency) or "15m",
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=200_000,
            )
            await session.commit()
        return {
            "status": "ok",
            "frequency": normalize_frequency(frequency) or "15m",
            "lookback_days": lookback_days,
            "markets_stored": len(all_markets),
            "live_markets": len(live_markets),
            "historical_markets": len(historical_markets),
            "candles_stored": candle_stats["stored"],
            "candle_capture": {
                **candle_stats,
                "errors": candle_stats["errors"][:10],
                "error_count": len(candle_stats["errors"]),
            },
            "series": series_stats,
            "pages_fetched": sum(int(item["pages_fetched"]) for item in series_stats),
            "historical_rows_seen": sum(int(item["rows_seen"]) for item in series_stats),
            "data_quality": _crypto_data_quality(
                snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            ),
            "errors": errors[:10],
        }

    async def daily(self, *, frequency: str = "15m") -> dict[str, Any]:
        return await self.bootstrap(days=2, frequency=frequency)

    async def status(self, *, frequency: str = "15m", days: int | None = None) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=200_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=500_000,
            )
            await session.commit()
        return {
            "status": "ok",
            "frequency": freq,
            "days": days,
            "data_quality": _crypto_data_quality(
                snapshots,
                candles,
                min_training_samples=self.settings.crypto_min_training_samples,
            ),
        }

    async def _list_historical_markets(self, series_ticker: str) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        pages_fetched = 0
        for _ in range(100):
            params: dict[str, Any] = {"series_ticker": series_ticker, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            try:
                response = await self.kalshi.list_historical_markets(**params)
            except httpx.HTTPError as exc:
                errors.append(str(exc))
                break
            page_rows = _rows_from_response(response, "markets")
            rows.extend(page_rows)
            pages_fetched += 1
            cursor = response.get("cursor") or response.get("next_cursor")
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        return {
            "rows": rows,
            "rows_seen": len(rows),
            "pages_fetched": pages_fetched,
            "errors": errors,
        }

    async def _capture_candles(self, repo: PlatformRepository, market: CryptoMarket, *, cutoff: datetime) -> dict[str, Any]:
        now = datetime.now(UTC)
        if market.close_time is not None and market.close_time < now:
            existing = await repo.list_crypto_market_candlesticks(
                frequency=market.frequency,
                kalshi_env=self.settings.kalshi_env,
                market_ticker=market.market_ticker,
                limit=1,
            )
            if existing:
                return {"status": "skipped_existing", "stored": 0}
        end_time = min(now, market.close_time or market.expected_expiration_time or now)
        if market.close_time is not None and market.close_time < now:
            end_time = min(now, market.close_time + timedelta(minutes=1))
        start_time = market.open_time or (end_time - timedelta(minutes=20))
        start_time = max(cutoff, start_time)
        if start_time >= end_time:
            start_time = end_time - timedelta(minutes=20)
        params = {
            "period_interval": 1,
            "start_ts": int(start_time.timestamp()),
            "end_ts": int(end_time.timestamp()),
        }
        try:
            if market.close_time is not None and market.close_time < now:
                response = await self.kalshi.get_historical_market_candlesticks(
                    market.series_ticker,
                    market.market_ticker,
                    **params,
                )
            else:
                response = await self.kalshi.get_market_candlesticks(market.series_ticker, market.market_ticker, **params)
        except httpx.HTTPError as exc:
            logger.info("crypto candlestick capture skipped for %s", market.market_ticker, exc_info=True)
            return {"status": "error", "stored": 0, "error": str(exc)}
        count = 0
        for row in _rows_from_response(response, "candlesticks") or _rows_from_response(response, "candles"):
            candle = normalize_candlestick(row)
            if candle is None:
                continue
            await repo.upsert_crypto_market_candlestick(
                kalshi_env=self.settings.kalshi_env,
                series_ticker=market.series_ticker,
                market_ticker=market.market_ticker,
                asset_symbol=market.asset_symbol,
                frequency=market.frequency,
                period_interval=candle["period_interval"],
                end_period_ts=candle["end_period_ts"],
                open_dollars=candle["open_dollars"],
                high_dollars=candle["high_dollars"],
                low_dollars=candle["low_dollars"],
                close_dollars=candle["close_dollars"],
                volume=candle["volume"],
                payload=candle["payload"],
            )
            count += 1
        return {"status": "ok", "stored": count}


class CryptoForecastService:
    def __init__(self, *, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def train(self, *, frequency: str = "15m") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            rows = await repo.list_crypto_market_snapshots(frequency=freq, kalshi_env=self.settings.kalshi_env, limit=100_000)
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                limit=200_000,
            )
            decision_rows = _crypto_decision_rows(rows, candles)
            sample_count = len(decision_rows)
            payload = _fit_crypto_calibration(decision_rows)
            metrics = _crypto_model_metrics(decision_rows, payload, settings=self.settings)
            status = "trained" if sample_count >= self.settings.crypto_min_training_samples else "insufficient_data"
            artifact_payload = {
                **payload,
                "frequency": freq,
                "trained_from": "point_in_time_crypto_snapshots_and_candles",
                "feature_set": [
                    "asset",
                    "time_to_close",
                    "target_price",
                    "yes_bid",
                    "yes_ask",
                    "spread",
                    "mid",
                    "volume",
                    "open_interest",
                    "candlestick_momentum",
                    "recent_same_asset_behavior",
                ],
            }
            artifact = await repo.record_crypto_model_artifact(
                frequency=freq,
                artifact_type="model",
                version=_version("crypto-15m-model", {"metrics": metrics, "payload": artifact_payload}),
                status=status,
                sample_count=sample_count,
                metrics=metrics,
                payload=artifact_payload,
                kalshi_env=self.settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
            await session.commit()
        return {"status": status, "version": artifact.version, "metrics": metrics, "payload": artifact_payload}

    async def forecast(self, market: CryptoMarket) -> StrategySignal:
        features = self.features(market)
        if not self.settings.crypto_enabled or not self.settings.crypto_15m_enabled:
            return self._stand_down(market, StandDownReason.CRYPTO_DISABLED, "Crypto trading workflow is disabled.", features)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            artifact = await repo.get_latest_crypto_model_artifact(
                frequency=market.frequency,
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=market.frequency,
                artifact_type="backtest",
                kalshi_env=self.settings.kalshi_env,
            )
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        mid = market.mid_yes_dollars or market.last_price_dollars or Decimal("0.5000")
        if artifact is None or artifact.status != "trained":
            return self._stand_down(
                market,
                StandDownReason.CRYPTO_MODEL_UNAVAILABLE,
                "Crypto model artifact is missing or not trained; stand down.",
                features,
                fair=mid,
            )
        payload = artifact.payload or {}
        adjustment = Decimal(int(payload.get("global_adjustment_bps") or 0)) / Decimal("10000")
        adjustment += Decimal(int((payload.get("asset_adjustments_bps") or {}).get(market.asset_symbol, 0))) / Decimal("20000")
        momentum = _recent_momentum_adjustment(features)
        fair = _clamp_price(mid + adjustment + momentum)
        action, side, target_yes, edge_bps, trace = _crypto_recommendation(
            market=market,
            fair_yes=fair,
            min_edge_bps=self.settings.risk_min_edge_bps,
        )
        confidence = min(0.95, max(self.settings.risk_min_confidence, 0.80 + abs(edge_bps) / 20000))
        eligibility = None
        stand_down_reason = None
        outcome = trace["outcome"]
        summary = (
            f"{market.asset_symbol} 15m fair yes {fair}; "
            f"{'recommend ' + side.value.upper() if side is not None else 'no trade'} edge {edge_bps}bps."
        )
        if side is None:
            stand_down_reason = StandDownReason.NO_ACTIONABLE_EDGE
            eligibility = TradeEligibilityVerdict(
                eligible=False,
                reasons=["No crypto edge clears the configured minimum."],
                stand_down_reason=stand_down_reason,
                evaluation_outcome=outcome,
                candidate_trace=trace,
            )
        else:
            eligibility = TradeEligibilityVerdict(
                eligible=True,
                reasons=[],
                evaluation_outcome=outcome,
                candidate_trace=trace,
            )
        return StrategySignal(
            fair_yes_dollars=fair,
            confidence=confidence,
            edge_bps=edge_bps,
            recommended_action=action,
            recommended_side=side,
            target_yes_price_dollars=target_yes,
            summary=summary,
            resolution_state=WeatherResolutionState.UNRESOLVED,
            eligibility=eligibility,
            stand_down_reason=stand_down_reason,
            evaluation_outcome=outcome,
            candidate_trace={
                **trace,
                "market_domain": "crypto",
                "frequency": market.frequency,
                "strategy_code": StrategyCode.CRYPTO_15M.value,
                "features": features,
                "model_version": artifact.version,
                "model_metrics": artifact.metrics,
                "prediction_model": {
                    "baseline_probability": _money_text(mid),
                    "calibrated_probability": _money_text(fair),
                    "calibration_version": artifact.version,
                    "status": artifact.status,
                    "reason": None,
                },
                "trade_selection_model": {
                    "expected_net_pnl": _money_text(
                        _expected_crypto_net_pnl(
                            market,
                            side,
                            fair,
                            fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)),
                        )
                        if side is not None
                        else None
                    ),
                    "decision": "selected" if side is not None else "stand_down",
                    "status": "shadow_only",
                    "reason": "crypto_live_trading_disabled" if not self.settings.crypto_trading_enabled else None,
                    "backtest_version": backtest.version if backtest is not None else None,
                    "replay_gate_status": gate.status if gate is not None else "missing",
                },
            },
            capital_bucket="safe",
            confidence_band="high" if confidence >= 0.85 else "medium",
        )

    def features(self, market: CryptoMarket) -> dict[str, Any]:
        now = datetime.now(UTC)
        time_to_close_seconds = None
        if market.close_time is not None:
            time_to_close_seconds = max(0, int((market.close_time - now).total_seconds()))
        return {
            "asset": market.asset_symbol,
            "time_to_close_seconds": time_to_close_seconds,
            "target_price_dollars": _money_text(market.target_price_dollars),
            "yes_bid_dollars": _money_text(market.yes_bid_dollars),
            "yes_ask_dollars": _money_text(market.yes_ask_dollars),
            "no_bid_dollars": _money_text(market.no_bid_dollars),
            "no_ask_dollars": _money_text(market.no_ask_dollars),
            "spread_bps": market.spread_bps,
            "mid_yes_dollars": _money_text(market.mid_yes_dollars),
            "last_price_dollars": _money_text(market.last_price_dollars),
            "volume": market.volume,
            "open_interest": market.open_interest,
        }

    def _stand_down(
        self,
        market: CryptoMarket,
        reason: StandDownReason,
        summary: str,
        features: dict[str, Any],
        *,
        fair: Decimal | None = None,
    ) -> StrategySignal:
        fair_yes = _clamp_price(fair or market.mid_yes_dollars or market.last_price_dollars or Decimal("0.5000"))
        return StrategySignal(
            fair_yes_dollars=fair_yes,
            confidence=0.0,
            edge_bps=0,
            recommended_action=None,
            recommended_side=None,
            target_yes_price_dollars=None,
            summary=summary,
            resolution_state=WeatherResolutionState.UNRESOLVED,
            eligibility=TradeEligibilityVerdict(
                eligible=False,
                reasons=[summary],
                stand_down_reason=reason,
                evaluation_outcome="stand_down",
                candidate_trace={"market_domain": "crypto", "features": features},
            ),
            stand_down_reason=reason,
            evaluation_outcome="stand_down",
            candidate_trace={
                "market_domain": "crypto",
                "frequency": market.frequency,
                "strategy_code": StrategyCode.CRYPTO_15M.value,
                "stand_down_reason": reason.value,
                "features": features,
            },
        )


class CryptoReplayService:
    def __init__(self, *, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def run(
        self,
        *,
        frequency: str = "15m",
        days: int | None = None,
        limit: int | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        report = await self._build_report(
            frequency=frequency,
            days=days,
            limit=limit,
            command="run",
        )
        if persist:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                artifact = await repo.record_crypto_model_artifact(
                    frequency=report["frequency"],
                    artifact_type="backtest",
                    version=_version("crypto-15m-backtest", report),
                    status=report["status"],
                    sample_count=int((report.get("dataset") or {}).get("row_count") or 0),
                    metrics=report.get("metrics") or {},
                    payload=report,
                    kalshi_env=self.settings.kalshi_env,
                    trained_at=datetime.now(UTC),
                )
                await session.commit()
            report["version"] = artifact.version
        return report

    async def validate(
        self,
        *,
        frequency: str = "15m",
        days: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        return await self._build_report(
            frequency=frequency,
            days=days,
            limit=limit,
            command="validate",
        )

    async def gate(self, *, frequency: str = "15m") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            model = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="backtest",
                kalshi_env=self.settings.kalshi_env,
            )
            metrics = dict((backtest.metrics if backtest is not None else None) or (model.metrics if model is not None else {}) or {})
            if model is None:
                metrics["model_missing"] = True
            if backtest is None:
                metrics["backtest_missing"] = True
            gate = self.evaluate_gate(metrics)
            artifact = await repo.record_crypto_model_artifact(
                frequency=freq,
                artifact_type="replay_gate",
                version=_version("crypto-15m-gate", gate),
                status="passed" if gate["passed"] else "blocked",
                sample_count=int(metrics.get("resolved_sample_count") or 0),
                metrics=metrics,
                payload=gate,
                kalshi_env=self.settings.kalshi_env,
                trained_at=datetime.now(UTC),
            )
            control = await repo.ensure_deployment_control(self.settings.app_color)
            notes = dict(control.notes or {})
            notes["crypto_replay_gate"] = {
                "status": artifact.status,
                "version": artifact.version,
                "updated_at": datetime.now(UTC).isoformat(),
                "reasons": gate["reasons"],
            }
            control.notes = notes
            await session.commit()
        return {"status": artifact.status, "version": artifact.version, **gate}

    def evaluate_gate(self, metrics: dict[str, Any]) -> dict[str, Any]:
        reasons: list[str] = []
        resolved = int(metrics.get("resolved_sample_count") or metrics.get("sample_count") or 0)
        candidates = int(metrics.get("trade_candidate_count") or 0)
        net_pl = float(metrics.get("net_simulated_pl_dollars") or 0.0)
        hard_cap_breaches = int(metrics.get("hard_cap_breaches") or 0)
        calibration = metrics.get("calibration_brier")
        market_mid = metrics.get("market_mid_brier")
        candle_count = int(metrics.get("candle_count") or 0)
        leakage_rows = int(metrics.get("leakage_row_count") or 0)
        if metrics.get("model_missing"):
            reasons.append("Crypto model artifact is missing.")
        if metrics.get("backtest_missing"):
            reasons.append("Crypto backtest artifact is missing.")
        if candle_count <= 0:
            reasons.append("Crypto candlestick coverage is missing.")
        if leakage_rows > 0:
            reasons.append(f"Replay includes {leakage_rows} non-point-in-time rows.")
        if resolved < self.settings.crypto_replay_min_resolved_markets:
            reasons.append(
                f"Resolved sample coverage {resolved} below minimum {self.settings.crypto_replay_min_resolved_markets}."
            )
        if candidates < self.settings.crypto_replay_min_trade_candidates:
            reasons.append(
                f"Trade candidate count {candidates} below minimum {self.settings.crypto_replay_min_trade_candidates}."
            )
        if net_pl <= self.settings.crypto_replay_min_net_pl_dollars:
            reasons.append(f"Net simulated P/L ${net_pl:.2f} does not clear required positive threshold.")
        if hard_cap_breaches > self.settings.crypto_replay_max_hard_cap_breaches:
            reasons.append(f"Replay hard-cap breaches {hard_cap_breaches} exceed limit.")
        if self.settings.crypto_replay_require_calibration_better_than_mid:
            if calibration is None or market_mid is None or float(calibration) >= float(market_mid):
                reasons.append("Calibration does not beat the market-mid baseline.")
        return {
            "passed": not reasons,
            "reasons": reasons,
            "requirements": {
                "min_resolved_markets": self.settings.crypto_replay_min_resolved_markets,
                "min_trade_candidates": self.settings.crypto_replay_min_trade_candidates,
                "min_net_pl_dollars": self.settings.crypto_replay_min_net_pl_dollars,
                "max_hard_cap_breaches": self.settings.crypto_replay_max_hard_cap_breaches,
                "calibration_better_than_mid": self.settings.crypto_replay_require_calibration_better_than_mid,
                "requires_candles": True,
                "requires_point_in_time_rows": True,
            },
        }

    async def _build_report(
        self,
        *,
        frequency: str,
        days: int | None,
        limit: int | None,
        command: str,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        cutoff = datetime.now(UTC) - timedelta(days=days) if days and days > 0 else None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=200_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=self.settings.kalshi_env,
                since=cutoff,
                limit=500_000,
            )
            model = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        rows = _crypto_decision_rows(snapshots, candles)
        rows.sort(key=lambda row: (row.get("decision_ts") or datetime.max.replace(tzinfo=UTC), str(row.get("market_ticker"))))
        if limit and limit > 0:
            rows = rows[-limit:]
        backtest = _evaluate_crypto_walk_forward(rows, settings=self.settings)
        data_quality = _crypto_data_quality(
            snapshots,
            candles,
            min_training_samples=self.settings.crypto_min_training_samples,
        )
        metrics = {
            **(backtest.get("metrics") or {}),
            "sample_count": len(rows),
            "resolved_sample_count": len(rows),
            "candle_count": data_quality["candle_count"],
            "leakage_row_count": 0,
        }
        gate = self.evaluate_gate(metrics)
        issues: list[dict[str, Any]] = []
        if not self.settings.crypto_trading_enabled:
            issues.append({"severity": "info", "code": "crypto_trading_disabled", "message": "Global crypto trading is disabled."})
        if not self.settings.crypto_autonomy_enabled:
            issues.append({"severity": "info", "code": "crypto_autonomy_disabled", "message": "Crypto autonomy is disabled."})
        for reason in gate["reasons"]:
            severity = "fail" if command == "validate" else "warn"
            issues.append({"severity": severity, "code": _issue_code(reason), "message": reason})
        status = "pass"
        if any(issue["severity"] == "fail" for issue in issues):
            status = "fail"
        elif any(issue["severity"] == "warn" for issue in issues):
            status = "warn"
        return {
            "schema_version": "crypto-backtest-report-v1",
            "status": status,
            "command": command,
            "kalshi_env": self.settings.kalshi_env,
            "frequency": freq,
            "days": days,
            "dataset": {
                "row_count": len(rows),
                "snapshot_count": len(snapshots),
                "settled_snapshot_count": sum(1 for row in snapshots if row.settlement_result in {"yes", "no"}),
                "assets": sorted({str(row.get("asset_symbol")) for row in rows}),
            },
            "data_quality": data_quality,
            "model": _artifact_summary(model),
            "walk_forward": backtest,
            "metrics": metrics,
            "promotion_gate": gate,
            "issues": issues,
        }


class CryptoExecutionService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        base_execution_service: ExecutionService,
        asset_control_service: CryptoAssetControlService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.base_execution_service = base_execution_service
        self.asset_control_service = asset_control_service

    @staticmethod
    def passive_yes_price(market: CryptoMarket, side: ContractSide) -> Decimal | None:
        tick = Decimal("0.0001")
        if side == ContractSide.YES:
            if market.yes_ask_dollars is not None:
                ceiling = market.yes_ask_dollars - tick
            else:
                ceiling = Decimal("0.9900")
            base = (market.yes_bid_dollars or Decimal("0.0000")) + tick
            return _clamp_price(min(base, ceiling))
        if market.yes_bid_dollars is not None:
            floor = market.yes_bid_dollars + tick
        else:
            floor = Decimal("0.0100")
        if market.yes_ask_dollars is not None:
            base = market.yes_ask_dollars - tick
        else:
            base = floor
        return _clamp_price(max(base, floor))

    async def execute(
        self,
        *,
        room: Room,
        control: Any,
        ticket: TradeTicket,
        client_order_id: str,
        fair_yes_dollars: Decimal,
        market: CryptoMarket,
        signal: StrategySignal,
    ) -> ExecReceiptPayload:
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=room.kalshi_env)
            fresh_control = await repo.get_deployment_control(kalshi_env=room.kalshi_env)
            asset_mode = self.asset_control_service.mode_for_control(fresh_control, market.asset_symbol)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=room.kalshi_env,
            )
            await session.commit()
        if asset_mode != CRYPTO_ASSET_MODE_LIVE:
            if self.settings.app_shadow_mode or room.shadow_mode:
                return ExecReceiptPayload(
                    status="shadow_skipped",
                    client_order_id=client_order_id,
                    details={
                        "reason": "crypto asset is shadowed",
                        "asset_symbol": market.asset_symbol,
                        "asset_mode": asset_mode,
                    },
                )
            return ExecReceiptPayload(
                status="crypto_asset_live_disabled",
                client_order_id=client_order_id,
                details={
                    "reason": "crypto asset mode is not live",
                    "asset_symbol": market.asset_symbol,
                    "asset_mode": asset_mode,
                },
            )
        if not self.settings.crypto_trading_enabled:
            return ExecReceiptPayload(
                status="crypto_trading_disabled",
                client_order_id=client_order_id,
                details={"reason": "crypto_trading_enabled is false"},
            )
        if gate is None or gate.status != "passed":
            return ExecReceiptPayload(
                status="crypto_replay_gate_blocked",
                client_order_id=client_order_id,
                details={
                    "reason": "crypto replay gate has not passed",
                    "gate_status": gate.status if gate is not None else "missing",
                    "gate_version": gate.version if gate is not None else None,
                },
            )
        passive_price = self.passive_yes_price(market, ticket.side)
        if self.settings.crypto_order_mode == "passive_then_taker" and passive_price is not None:
            passive_ticket = ticket.model_copy(
                update={"yes_price_dollars": passive_price, "time_in_force": "gtc"}
            )
            passive_receipt = await self.base_execution_service.execute(
                room=room,
                control=fresh_control,
                ticket=passive_ticket,
                client_order_id=f"{client_order_id}:maker",
                fair_yes_dollars=fair_yes_dollars,
            )
            if passive_receipt.status not in {"unfilled_cancelled", "requote_edge_lost"}:
                passive_receipt.details = {**passive_receipt.details, "crypto_order_mode": "passive_then_taker"}
                return passive_receipt
            if not self._allow_taker_fallback(market, signal):
                return ExecReceiptPayload(
                    status="passive_unfilled_taker_blocked",
                    client_order_id=client_order_id,
                    details={"passive_receipt": passive_receipt.model_dump(mode="json")},
                )
        return await self.base_execution_service.execute(
            room=room,
            control=fresh_control,
            ticket=ticket,
            client_order_id=f"{client_order_id}:taker",
            fair_yes_dollars=fair_yes_dollars,
        )

    def _allow_taker_fallback(self, market: CryptoMarket, signal: StrategySignal) -> bool:
        if market.close_time is None:
            return False
        seconds_to_close = (market.close_time - datetime.now(UTC)).total_seconds()
        return seconds_to_close <= self.settings.crypto_taker_fallback_close_seconds and signal.edge_bps >= self.settings.risk_min_edge_bps


class CryptoWorkflowService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        market_service: CryptoMarketService,
        forecast_service: CryptoForecastService,
        risk_engine: DeterministicRiskEngine,
        execution_service: CryptoExecutionService,
        asset_control_service: CryptoAssetControlService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.market_service = market_service
        self.forecast_service = forecast_service
        self.risk_engine = risk_engine
        self.execution_service = execution_service
        self.asset_control_service = asset_control_service

    async def run_room(self, room_id: str, *, reason: str = "manual") -> None:
        market: CryptoMarket | None = None
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is None:
                    raise KeyError(room_id)
                await repo.update_room_stage(room.id, RoomStage.RESEARCHING)
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.SYSTEM,
                        kind=MessageKind.OBSERVATION,
                        stage=RoomStage.RESEARCHING,
                        content=f"Crypto 15m workflow started ({reason}).",
                        payload={"market_domain": "crypto", "frequency": "15m", "reason": reason},
                    ),
                )
                await session.commit()

            market = await self.market_service.get_market(room.market_ticker, persist=True)
            signal = await self.forecast_service.forecast(market)

            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is None:
                    raise KeyError(room_id)
                control = await repo.ensure_deployment_control(self.settings.app_color)
                gate = await repo.get_latest_crypto_model_artifact(
                    frequency=market.frequency,
                    artifact_type="replay_gate",
                    kalshi_env=room.kalshi_env,
                )
                backtest = await repo.get_latest_crypto_model_artifact(
                    frequency=market.frequency,
                    artifact_type="backtest",
                    kalshi_env=room.kalshi_env,
                )
                live_status = self.asset_control_service.market_live_status(
                    control=control,
                    replay_gate=gate,
                    market=market,
                    has_write_credentials=self.market_service.kalshi.write_credentials is not None,
                )
                market_artifact = await repo.save_artifact(
                    room_id=room.id,
                    artifact_type="market_snapshot",
                    source="crypto_workflow",
                    title=f"{market.asset_symbol} 15m crypto snapshot",
                    payload={
                        "market_domain": "crypto",
                        "frequency": market.frequency,
                        "strategy_code": StrategyCode.CRYPTO_15M.value,
                        "asset_mode": live_status["asset_mode"],
                        "live_eligible": live_status["live_eligible"],
                        "live_blockers": live_status["live_blockers"],
                        "global_live_blockers": live_status["global_live_blockers"],
                        "market": market.to_payload(),
                    },
                )
                await repo.update_room_stage(room.id, RoomStage.PROPOSING)
                signal_record = await repo.save_signal(
                    room_id=room.id,
                    market_ticker=market.market_ticker,
                    fair_yes_dollars=signal.fair_yes_dollars,
                    edge_bps=signal.edge_bps,
                    confidence=signal.confidence,
                    summary=signal.summary,
                    payload={
                        "market_domain": "crypto",
                        "frequency": market.frequency,
                        "strategy_code": StrategyCode.CRYPTO_15M.value,
                        "recommended_action": signal.recommended_action.value if signal.recommended_action else None,
                        "recommended_side": signal.recommended_side.value if signal.recommended_side else None,
                        "target_yes_price_dollars": _money_text(signal.target_yes_price_dollars),
                        "stand_down_reason": signal.stand_down_reason.value if signal.stand_down_reason else None,
                        "evaluation_outcome": signal.evaluation_outcome,
                        "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility else None,
                        "candidate_trace": signal.candidate_trace,
                        "market_artifact_id": market_artifact.id,
                        "crypto_modeling": {
                            "model_version": (signal.candidate_trace or {}).get("model_version"),
                            "backtest_version": backtest.version if backtest is not None else None,
                            "replay_gate_status": gate.status if gate is not None else "missing",
                            "data_quality_status": (
                                ((backtest.payload or {}).get("data_quality") or {}).get("status")
                                if backtest is not None
                                else None
                            ),
                        },
                    },
                )
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.TRADER,
                        kind=MessageKind.TRADE_IDEA,
                        stage=RoomStage.PROPOSING,
                        content=signal.summary,
                        payload={"signal_id": signal_record.id, **(signal_record.payload or {})},
                    ),
                )
                if not _signal_is_tradeable(signal):
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    await session.commit()
                    return

                count_fp = quantize_count(Decimal(str(self.settings.crypto_default_order_count_fp)))
                ticket = TradeTicket(
                    market_ticker=market.market_ticker,
                    action=TradeAction.BUY,
                    side=signal.recommended_side,
                    yes_price_dollars=signal.target_yes_price_dollars,
                    count_fp=count_fp,
                    capital_bucket=signal.capital_bucket,
                    time_in_force="immediate_or_cancel",
                    note="CRYPTO_15M passive-first candidate",
                )
                client_order_id = make_client_order_id(room.id, market.market_ticker, ticket.nonce)
                ticket_record = await repo.save_trade_ticket(
                    room.id,
                    ticket,
                    client_order_id,
                    strategy_code=StrategyCode.CRYPTO_15M.value,
                )
                ticket_record.payload = {
                    **(ticket_record.payload or {}),
                    "market_domain": "crypto",
                    "frequency": market.frequency,
                    "asset_symbol": market.asset_symbol,
                    "asset_mode": live_status["asset_mode"],
                    "live_eligible": live_status["live_eligible"],
                    "crypto_modeling": (signal_record.payload or {}).get("crypto_modeling"),
                }
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.TRADER,
                        kind=MessageKind.TRADE_TICKET,
                        stage=RoomStage.PROPOSING,
                        content=f"Proposed crypto {ticket.side.value.upper()} ticket for {ticket.count_fp} contracts.",
                        payload={**ticket_record.payload, "strategy_code": StrategyCode.CRYPTO_15M.value},
                    ),
                )
                await repo.update_room_stage(room.id, RoomStage.RISK)
                risk_context = await self._risk_context(repo, room, ticket, market)
                verdict = self.risk_engine.evaluate(
                    room=room,
                    control=control,
                    ticket=ticket,
                    signal=signal,
                    context=risk_context,
                )
                await repo.save_risk_verdict(
                    room_id=room.id,
                    ticket_id=ticket_record.id,
                    status=verdict.status,
                    reasons=verdict.reasons,
                    approved_notional_dollars=verdict.approved_notional_dollars,
                    approved_count_fp=verdict.approved_count_fp,
                    payload=verdict.model_dump(mode="json"),
                )
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.RISK_OFFICER,
                        kind=MessageKind.RISK_VERDICT,
                        stage=RoomStage.RISK,
                        content=f"Crypto risk verdict: {verdict.status.value}.",
                        payload=verdict.model_dump(mode="json"),
                    ),
                )
                if verdict.status != RiskStatus.APPROVED:
                    await repo.update_trade_ticket_status(ticket_record.id, "blocked")
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    await session.commit()
                    return

                approved_ticket = approved_ticket_for_verdict(ticket, verdict)
                await repo.update_trade_ticket_status(ticket_record.id, "approved")
                await repo.update_room_stage(room.id, RoomStage.EXECUTING)
                receipt = await self.execution_service.execute(
                    room=room,
                    control=control,
                    ticket=approved_ticket,
                    client_order_id=client_order_id,
                    fair_yes_dollars=signal.fair_yes_dollars,
                    market=market,
                    signal=signal,
                )
                no_order_statuses = {
                    "shadow_skipped",
                    "inactive_color_skipped",
                    "crypto_asset_live_disabled",
                    "crypto_trading_disabled",
                    "crypto_replay_gate_blocked",
                }
                if receipt.external_order_id or receipt.status not in no_order_statuses:
                    await repo.save_order(
                        ticket_id=ticket_record.id,
                        client_order_id=client_order_id,
                        market_ticker=approved_ticket.market_ticker,
                        status=receipt.status,
                        side=approved_ticket.side.value,
                        action=approved_ticket.action.value,
                        yes_price_dollars=approved_ticket.yes_price_dollars,
                        count_fp=approved_ticket.count_fp,
                        raw=receipt.details,
                        kalshi_order_id=receipt.external_order_id,
                        kalshi_env=room.kalshi_env,
                        strategy_code=StrategyCode.CRYPTO_15M.value,
                    )
                await repo.update_trade_ticket_status(ticket_record.id, receipt.status)
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.EXECUTION_CLERK,
                        kind=MessageKind.EXEC_RECEIPT,
                        stage=RoomStage.EXECUTING,
                        content=f"Crypto execution status: {receipt.status}.",
                        payload=receipt.model_dump(mode="json"),
                    ),
                )
                await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                await session.commit()
        except Exception:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is not None:
                    await repo.update_room_stage(room.id, RoomStage.FAILED)
                    await repo.append_message(
                        room.id,
                        RoomMessageCreate(
                            role=AgentRole.SYSTEM,
                            kind=MessageKind.OPS_ALERT,
                            stage=RoomStage.FAILED,
                            content="Crypto workflow failed; see logs.",
                            payload={"market_domain": "crypto", "market_ticker": market.market_ticker if market else None},
                        ),
                    )
                    await session.commit()
            raise

    async def _risk_context(
        self,
        repo: PlatformRepository,
        room: Room,
        ticket: TradeTicket,
        market: CryptoMarket,
    ) -> RiskContext:
        positions = await repo.list_positions_for_ticker(
            room.market_ticker,
            kalshi_env=room.kalshi_env,
            subaccount=self.settings.kalshi_subaccount,
        )
        open_position = max(positions, key=lambda p: p.count_fp) if positions else None
        all_positions = await repo.list_positions(limit=500, kalshi_env=room.kalshi_env, subaccount=self.settings.kalshi_subaccount)
        pending_order_count_fp = await repo.get_pending_buy_count_fp(
            room.market_ticker,
            ticket.side.value,
            kalshi_env=room.kalshi_env,
        )
        strategy_daily_pnl = await repo.get_daily_realized_pnl_dollars_by_strategy(
            strategy_code=StrategyCode.CRYPTO_15M.value,
            kalshi_env=room.kalshi_env,
        )
        current_position_notional = (
            estimate_notional_dollars(
                ContractSide(open_position.side),
                open_position.average_price_dollars,
                open_position.count_fp,
            )
            if open_position is not None
            else Decimal("0")
        )
        return RiskContext(
            market_observed_at=datetime.now(UTC),
            research_observed_at=datetime.now(UTC),
            current_position_notional_dollars=current_position_notional,
            current_position_count_fp=open_position.count_fp if open_position is not None else Decimal("0"),
            current_position_side=open_position.side if open_position is not None else None,
            pending_order_count_fp=pending_order_count_fp,
            open_ticker_count=len({position.market_ticker for position in all_positions}),
            strategy_code=StrategyCode.CRYPTO_15M.value,
            strategy_daily_realized_pnl_dollars=strategy_daily_pnl,
        )


class CryptoAutonomyService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        market_service: CryptoMarketService,
        asset_control_service: CryptoAssetControlService,
        workflow_service: CryptoWorkflowService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.market_service = market_service
        self.asset_control_service = asset_control_service
        self.workflow_service = workflow_service

    async def run_once(self, *, frequency: str = "15m", force: bool = False) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        if not self.settings.crypto_autonomy_enabled and not force:
            return {"status": "disabled", "frequency": freq, "reason": "crypto_autonomy_enabled is false"}
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="replay_gate",
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        if control.active_color != self.settings.app_color:
            return {
                "status": "inactive_color",
                "frequency": freq,
                "active_color": control.active_color,
                "app_color": self.settings.app_color,
            }

        discovered = await self.market_service.discover_markets(frequency=freq, status="open", persist=True)
        created: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        min_seconds = max(0, int(self.settings.crypto_autonomy_min_seconds_to_close))
        markets, ineligible = _eligible_market_per_asset(discovered, min_seconds_to_close=min_seconds)
        skipped.extend(ineligible)

        for market in markets:
            try:
                seconds_to_close = int((market.close_time - datetime.now(UTC)).total_seconds())

                live_status = self.asset_control_service.market_live_status(
                    control=control,
                    replay_gate=gate,
                    market=market,
                    has_write_credentials=self.market_service.kalshi.write_credentials is not None,
                )
                if live_status["asset_mode"] == CRYPTO_ASSET_MODE_OFF:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "asset_mode_off",
                        }
                    )
                    continue

                async with self.session_factory() as session:
                    repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                    existing = await repo.get_latest_room_for_market(
                        market.market_ticker,
                        kalshi_env=self.settings.kalshi_env,
                    )
                    await session.commit()
                if existing is not None:
                    skipped.append(
                        {
                            "market_ticker": market.market_ticker,
                            "asset_symbol": market.asset_symbol,
                            "reason": "room_already_exists",
                            "room_id": existing.id,
                        }
                    )
                    continue

                result = await self.market_service.create_room_for_market(
                    market.market_ticker,
                    reason="crypto_autonomy",
                )
                await self.workflow_service.run_room(result["room_id"], reason="crypto_autonomy")
                created.append(
                    {
                        **result,
                        "seconds_to_close": seconds_to_close,
                        "requested_asset_mode": live_status["asset_mode"],
                    }
                )
            except Exception as exc:
                logger.warning("crypto autonomy failed for %s", market.market_ticker, exc_info=True)
                errors.append(
                    {
                        "market_ticker": market.market_ticker,
                        "asset_symbol": market.asset_symbol,
                        "error": str(exc),
                    }
                )

        return {
            "status": "ok",
            "frequency": freq,
            "forced": force,
            "checked_markets": len(discovered),
            "eligible_markets": len(markets),
            "created": created,
            "skipped": skipped,
            "errors": errors,
        }


def _market_from_snapshot(row: CryptoMarketSnapshotRecord) -> CryptoMarket:
    payload = row.payload or {}
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else payload
    return CryptoMarket(
        market_ticker=row.market_ticker,
        series_ticker=row.series_ticker,
        event_ticker=row.event_ticker,
        asset_symbol=row.asset_symbol,
        frequency=row.frequency,
        title=row.title,
        status=row.status,
        open_time=row.open_time,
        close_time=row.close_time,
        expected_expiration_time=row.expected_expiration_time,
        target_price_dollars=row.target_price_dollars,
        yes_bid_dollars=row.yes_bid_dollars,
        yes_ask_dollars=row.yes_ask_dollars,
        no_bid_dollars=row.no_bid_dollars,
        no_ask_dollars=row.no_ask_dollars,
        last_price_dollars=row.last_price_dollars,
        volume=row.volume,
        open_interest=row.open_interest,
        settlement_result=row.settlement_result,
        raw=raw,
    )


def _artifact_summary(artifact: Any | None) -> dict[str, Any]:
    if artifact is None:
        return {"status": "missing", "version": None, "metrics": {}, "payload": {}}
    return {
        "status": artifact.status,
        "version": artifact.version,
        "sample_count": artifact.sample_count,
        "metrics": artifact.metrics,
        "payload": artifact.payload,
        "updated_at": artifact.updated_at.isoformat() if artifact.updated_at else None,
    }


def _nearest_market_per_asset(markets: list[CryptoMarket]) -> list[CryptoMarket]:
    now = datetime.now(UTC)
    by_asset: dict[str, CryptoMarket] = {}
    for market in markets:
        existing = by_asset.get(market.asset_symbol)
        if existing is None or _market_sort_key(market, now) < _market_sort_key(existing, now):
            by_asset[market.asset_symbol] = market
    return sorted(by_asset.values(), key=lambda market: (market.close_time or datetime.max.replace(tzinfo=UTC), market.asset_symbol))


def _market_sort_key(market: CryptoMarket, now: datetime) -> tuple[int, float, str]:
    if market.close_time is None:
        return (2, float("inf"), market.market_ticker)
    seconds = (market.close_time - now).total_seconds()
    if seconds >= 0:
        return (0, seconds, market.market_ticker)
    return (1, abs(seconds), market.market_ticker)


def _eligible_market_per_asset(
    markets: list[CryptoMarket],
    *,
    min_seconds_to_close: int,
) -> tuple[list[CryptoMarket], list[dict[str, Any]]]:
    now = datetime.now(UTC)
    grouped: dict[str, list[CryptoMarket]] = {}
    for market in markets:
        grouped.setdefault(market.asset_symbol, []).append(market)

    selected: list[CryptoMarket] = []
    skipped: list[dict[str, Any]] = []
    for asset_symbol, asset_markets in sorted(grouped.items()):
        ordered = sorted(asset_markets, key=lambda market: _market_sort_key(market, now))
        chosen: CryptoMarket | None = None
        latest_skip: dict[str, Any] | None = None
        for market in ordered:
            if market.close_time is None:
                latest_skip = {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "missing_close_time",
                }
                continue
            seconds_to_close = int((market.close_time - now).total_seconds())
            if seconds_to_close < min_seconds_to_close:
                latest_skip = {
                    "market_ticker": market.market_ticker,
                    "asset_symbol": market.asset_symbol,
                    "reason": "too_close_to_close",
                    "seconds_to_close": seconds_to_close,
                }
                continue
            chosen = market
            break
        if chosen is not None:
            selected.append(chosen)
        elif latest_skip is not None:
            skipped.append(latest_skip)
        else:
            skipped.append({"asset_symbol": asset_symbol, "reason": "no_markets"})

    return (
        sorted(selected, key=lambda market: (market.close_time or datetime.max.replace(tzinfo=UTC), market.asset_symbol)),
        skipped,
    )


def _row_mid(row: CryptoMarketSnapshotRecord) -> Decimal | None:
    if row.yes_bid_dollars is not None and row.yes_ask_dollars is not None:
        return (row.yes_bid_dollars + row.yes_ask_dollars) / Decimal("2")
    return row.last_price_dollars


def _recent_momentum_adjustment(features: dict[str, Any]) -> Decimal:
    last_raw = features.get("last_price_dollars")
    mid_raw = features.get("mid_yes_dollars")
    if last_raw in (None, "") or mid_raw in (None, ""):
        return Decimal("0")
    return ((Decimal(str(last_raw)) - Decimal(str(mid_raw))) / Decimal("4")).quantize(Decimal("0.0001"))


def _crypto_recommendation(
    *,
    market: CryptoMarket,
    fair_yes: Decimal,
    min_edge_bps: int,
) -> tuple[TradeAction | None, ContractSide | None, Decimal | None, int, dict[str, Any]]:
    min_edge = Decimal(min_edge_bps) / Decimal("10000")
    yes_edge = fair_yes - market.yes_ask_dollars if market.yes_ask_dollars is not None else None
    no_target_yes = quantize_price(Decimal("1.0000") - market.no_ask_dollars) if market.no_ask_dollars is not None else market.yes_bid_dollars
    no_edge = market.yes_bid_dollars - fair_yes if market.yes_bid_dollars is not None else None
    candidates = [
        {
            "side": "yes",
            "target_yes_price_dollars": _money_text(market.yes_ask_dollars),
            "edge_bps": int((yes_edge * Decimal("10000")).to_integral_value()) if yes_edge is not None else None,
            "status": "eligible" if yes_edge is not None and yes_edge >= min_edge else "blocked",
        },
        {
            "side": "no",
            "target_yes_price_dollars": _money_text(no_target_yes),
            "edge_bps": int((no_edge * Decimal("10000")).to_integral_value()) if no_edge is not None else None,
            "status": "eligible" if no_edge is not None and no_edge >= min_edge and no_target_yes is not None else "blocked",
        },
    ]
    eligible = [candidate for candidate in candidates if candidate["status"] == "eligible"]
    if not eligible:
        edge_bps = max([int(candidate["edge_bps"]) for candidate in candidates if candidate["edge_bps"] is not None] or [0])
        return None, None, None, edge_bps, {
            "outcome": "no_candidate",
            "fair_yes_dollars": _money_text(fair_yes),
            "min_edge_bps": min_edge_bps,
            "spread_bps": market.spread_bps,
            "candidates": candidates,
        }
    selected = max(eligible, key=lambda candidate: int(candidate["edge_bps"] or 0))
    side = ContractSide(selected["side"])
    target_yes = quantize_price(selected["target_yes_price_dollars"])
    edge_bps = int(selected["edge_bps"] or 0)
    return TradeAction.BUY, side, target_yes, edge_bps, {
        "outcome": "candidate_selected",
        "fair_yes_dollars": _money_text(fair_yes),
        "selected_side": side.value,
        "selected_edge_bps": edge_bps,
        "target_yes_price_dollars": _money_text(target_yes),
        "min_edge_bps": min_edge_bps,
        "spread_bps": market.spread_bps,
        "candidates": candidates,
    }


def _signal_is_tradeable(signal: StrategySignal) -> bool:
    return (
        signal.recommended_action is not None
        and signal.recommended_side is not None
        and signal.target_yes_price_dollars is not None
        and signal.eligibility is not None
        and signal.eligibility.eligible
    )


def _crypto_data_quality(
    snapshots: list[CryptoMarketSnapshotRecord],
    candles: list[CryptoMarketCandlestickRecord],
    *,
    min_training_samples: int,
) -> dict[str, Any]:
    assets = sorted({row.asset_symbol for row in snapshots} | {row.asset_symbol for row in candles})
    candle_markets = {row.market_ticker for row in candles}
    by_asset: dict[str, dict[str, Any]] = {}
    for asset in assets:
        asset_snapshots = [row for row in snapshots if row.asset_symbol == asset]
        asset_candles = [row for row in candles if row.asset_symbol == asset]
        settled = [row for row in asset_snapshots if row.settlement_result in {"yes", "no"}]
        snapshot_markets = {row.market_ticker for row in asset_snapshots}
        latest_observed = max((row.observed_at for row in asset_snapshots), default=None)
        latest_candle = max((row.end_period_ts for row in asset_candles), default=None)
        by_asset[asset] = {
            "snapshot_count": len(asset_snapshots),
            "settled_snapshot_count": len(settled),
            "candle_count": len(asset_candles),
            "market_count": len(snapshot_markets),
            "markets_missing_candles": len(snapshot_markets - candle_markets),
            "latest_observed_at": latest_observed.isoformat() if latest_observed else None,
            "latest_candle_at": latest_candle.isoformat() if latest_candle else None,
        }
    settled_snapshot_count = sum(1 for row in snapshots if row.settlement_result in {"yes", "no"})
    status = "ready" if settled_snapshot_count >= min_training_samples and candles else "needs_data"
    return {
        "status": status,
        "snapshot_count": len(snapshots),
        "settled_snapshot_count": settled_snapshot_count,
        "unresolved_snapshot_count": len(snapshots) - settled_snapshot_count,
        "candle_count": len(candles),
        "asset_count": len(assets),
        "assets": by_asset,
        "source_kind_counts": dict(Counter(row.source_kind for row in snapshots)),
    }


def _crypto_decision_rows(
    snapshots: list[CryptoMarketSnapshotRecord],
    candles: list[CryptoMarketCandlestickRecord],
) -> list[dict[str, Any]]:
    candles_by_market: dict[str, list[CryptoMarketCandlestickRecord]] = defaultdict(list)
    for candle in candles:
        candles_by_market[candle.market_ticker].append(candle)
    for market_candles in candles_by_market.values():
        market_candles.sort(key=lambda row: row.end_period_ts)

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, datetime]] = set()
    for snapshot in snapshots:
        if snapshot.settlement_result not in {"yes", "no"}:
            continue
        decision_ts = snapshot.observed_at
        close_time = snapshot.close_time or snapshot.expected_expiration_time
        if close_time is not None and decision_ts > close_time:
            continue
        key = (snapshot.market_ticker, decision_ts)
        if key in seen:
            continue
        seen.add(key)
        candle = _nearest_candle(candles_by_market.get(snapshot.market_ticker, []), decision_ts)
        mid = _row_mid(snapshot) or (candle.close_dollars if candle is not None else None)
        if mid is None:
            continue
        yes_bid = snapshot.yes_bid_dollars
        yes_ask = snapshot.yes_ask_dollars
        no_ask = snapshot.no_ask_dollars
        quote_source = "snapshot_quotes"
        if yes_bid is None or yes_ask is None:
            quote_source = "candlestick_close_proxy"
            yes_bid = mid
            yes_ask = mid
            no_ask = Decimal("1") - mid
        elif no_ask is None:
            no_ask = Decimal("1") - yes_bid
        prior_candle = _prior_candle(candles_by_market.get(snapshot.market_ticker, []), decision_ts)
        candle_momentum = None
        if candle is not None and prior_candle is not None and candle.close_dollars is not None and prior_candle.close_dollars is not None:
            candle_momentum = candle.close_dollars - prior_candle.close_dollars
        rows.append(
            {
                "row_id": f"{snapshot.market_ticker}:{decision_ts.isoformat()}",
                "market_ticker": snapshot.market_ticker,
                "series_ticker": snapshot.series_ticker,
                "asset_symbol": snapshot.asset_symbol,
                "frequency": snapshot.frequency,
                "source_kind": snapshot.source_kind,
                "quote_source": quote_source,
                "decision_ts": decision_ts,
                "settlement_ts": close_time,
                "market_day": decision_ts.date().isoformat(),
                "target_price_dollars": snapshot.target_price_dollars,
                "mid_yes_dollars": _clamp_price(mid),
                "yes_bid_dollars": _clamp_price(yes_bid),
                "yes_ask_dollars": _clamp_price(yes_ask),
                "no_ask_dollars": _clamp_price(no_ask) if no_ask is not None else None,
                "spread_bps": int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value()) if yes_bid is not None and yes_ask is not None else None,
                "volume": snapshot.volume,
                "open_interest": snapshot.open_interest,
                "time_to_close_seconds": int((close_time - decision_ts).total_seconds()) if close_time is not None else None,
                "settlement_result": snapshot.settlement_result,
                "label_yes": 1 if snapshot.settlement_result == "yes" else 0,
                "candle_count": len(candles_by_market.get(snapshot.market_ticker, [])),
                "candle_momentum_dollars": candle_momentum,
            }
        )
    return rows


def _nearest_candle(
    candles: list[CryptoMarketCandlestickRecord],
    decision_ts: datetime,
) -> CryptoMarketCandlestickRecord | None:
    eligible = [row for row in candles if row.end_period_ts <= decision_ts]
    return eligible[-1] if eligible else None


def _prior_candle(
    candles: list[CryptoMarketCandlestickRecord],
    decision_ts: datetime,
) -> CryptoMarketCandlestickRecord | None:
    eligible = [row for row in candles if row.end_period_ts < decision_ts]
    return eligible[-2] if len(eligible) >= 2 else None


def _fit_crypto_calibration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"global_adjustment_bps": 0, "asset_adjustments_bps": {}, "feature_weights": {}}
    outcome_avg = sum((Decimal(row["label_yes"]) for row in rows), Decimal("0")) / len(rows)
    mid_avg = sum((_decimal(row["mid_yes_dollars"]) for row in rows), Decimal("0")) / len(rows)
    global_adjustment = outcome_avg - mid_avg
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_asset[str(row["asset_symbol"])].append(row)
    asset_adjustments: dict[str, int] = {}
    for asset, asset_rows in by_asset.items():
        asset_outcome = sum((Decimal(row["label_yes"]) for row in asset_rows), Decimal("0")) / len(asset_rows)
        asset_mid = sum((_decimal(row["mid_yes_dollars"]) for row in asset_rows), Decimal("0")) / len(asset_rows)
        asset_adjustments[asset] = int(((asset_outcome - asset_mid - global_adjustment) * Decimal("10000")).to_integral_value())
    return {
        "global_adjustment_bps": int((global_adjustment * Decimal("10000")).to_integral_value()),
        "asset_adjustments_bps": asset_adjustments,
        "feature_weights": {
            "candlestick_momentum": 0.25,
            "spread_penalty_bps_per_100bps": -8,
            "time_to_close_decay": 0.10,
        },
    }


def _predict_crypto_probability(row: dict[str, Any], model: dict[str, Any] | None) -> Decimal:
    mid = _decimal(row.get("mid_yes_dollars"))
    if not model:
        return _clamp_price(mid)
    adjustment = Decimal(int(model.get("global_adjustment_bps") or 0)) / Decimal("10000")
    adjustment += Decimal(int((model.get("asset_adjustments_bps") or {}).get(str(row.get("asset_symbol")), 0))) / Decimal("20000")
    momentum = _decimal(row.get("candle_momentum_dollars") or Decimal("0")) * Decimal("0.25")
    spread_bps = int(row.get("spread_bps") or 0)
    spread_penalty = Decimal(max(0, spread_bps - 100)) / Decimal("10000") / Decimal("8")
    return _clamp_price(mid + adjustment + momentum - spread_penalty)


def _crypto_model_metrics(
    rows: list[dict[str, Any]],
    model: dict[str, Any],
    *,
    settings: Settings,
) -> dict[str, Any]:
    baseline_predictions: list[tuple[Decimal, int]] = []
    calibrated_predictions: list[tuple[Decimal, int]] = []
    simulated = []
    for row in rows:
        label = int(row["label_yes"])
        baseline = _decimal(row["mid_yes_dollars"])
        predicted = _predict_crypto_probability(row, model)
        baseline_predictions.append((baseline, label))
        calibrated_predictions.append((predicted, label))
        simulated.append(_simulate_crypto_trade(row, predicted, settings=settings))
    fillable = [item for item in simulated if item["status"] == "fillable"]
    net = sum((_decimal(item["net_pnl"]) for item in fillable), Decimal("0"))
    fees = sum((_decimal(item["fees"]) for item in fillable), Decimal("0"))
    hard_cap_breaches = sum(1 for item in fillable if _decimal(item["net_pnl"]) < Decimal("-1.0000"))
    baseline_metrics = _probability_metrics_decimal(baseline_predictions)
    calibrated_metrics = _probability_metrics_decimal(calibrated_predictions)
    return {
        "sample_count": len(rows),
        "resolved_sample_count": len(rows),
        "trade_candidate_count": len(fillable),
        "net_simulated_pl_dollars": float(net),
        "fees_dollars": float(fees),
        "hard_cap_breaches": hard_cap_breaches,
        "calibration_brier": calibrated_metrics["brier"],
        "market_mid_brier": baseline_metrics["brier"],
        "calibration_log_loss": calibrated_metrics["log_loss"],
        "market_mid_log_loss": baseline_metrics["log_loss"],
        "calibration_ece": calibrated_metrics["ece"],
        "market_mid_ece": baseline_metrics["ece"],
        "fee_model_version": current_fee_model_version(),
    }


def _evaluate_crypto_walk_forward(rows: list[dict[str, Any]], *, settings: Settings) -> dict[str, Any]:
    folds = _crypto_walk_forward_folds(rows, min_train_rows=max(2, min(settings.crypto_min_training_samples, 20)))
    if not folds:
        empty_metrics = _crypto_model_metrics([], {}, settings=settings)
        return {
            "status": "insufficient_data",
            "reason": "need_settled_point_in_time_crypto_rows_across_market_days",
            "fold_count": 0,
            "folds": [],
            "baseline_policy": _crypto_policy_metrics("baseline_market_mid", [], settings=settings),
            "candidate_policies": [
                _crypto_policy_metrics("calibrated_prediction", [], settings=settings),
                _crypto_policy_metrics("trade_selection_policy", [], settings=settings),
            ],
            "bucket_matrix": [],
            "metrics": empty_metrics,
        }
    baseline_trades: list[dict[str, Any]] = []
    calibrated_trades: list[dict[str, Any]] = []
    selection_trades: list[dict[str, Any]] = []
    baseline_predictions: list[tuple[Decimal, int]] = []
    calibrated_predictions: list[tuple[Decimal, int]] = []
    fold_summaries: list[dict[str, Any]] = []
    for fold in folds:
        model = _fit_crypto_calibration(fold["train_rows"])
        eligible_buckets = _eligible_crypto_buckets(fold["train_rows"], settings=settings)
        fold_baseline: list[dict[str, Any]] = []
        fold_calibrated: list[dict[str, Any]] = []
        fold_selection: list[dict[str, Any]] = []
        for row in fold["test_rows"]:
            baseline = _decimal(row["mid_yes_dollars"])
            calibrated = _predict_crypto_probability(row, model)
            baseline_predictions.append((baseline, int(row["label_yes"])))
            calibrated_predictions.append((calibrated, int(row["label_yes"])))
            baseline_trade = _simulate_crypto_trade(row, baseline, settings=settings)
            calibrated_trade = _simulate_crypto_trade(row, calibrated, settings=settings)
            if baseline_trade["status"] == "fillable":
                fold_baseline.append({**row, "simulation": baseline_trade})
            if calibrated_trade["status"] == "fillable":
                fold_calibrated.append({**row, "simulation": calibrated_trade})
                if _crypto_bucket_key(row, calibrated_trade) in eligible_buckets:
                    fold_selection.append({**row, "simulation": calibrated_trade})
        baseline_trades.extend(fold_baseline)
        calibrated_trades.extend(fold_calibrated)
        selection_trades.extend(fold_selection)
        fold_summaries.append(
            {
                "fold_id": fold["fold_id"],
                "train_rows": len(fold["train_rows"]),
                "test_rows": len(fold["test_rows"]),
                "baseline_selected_count": len(fold_baseline),
                "calibrated_selected_count": len(fold_calibrated),
                "trade_selection_selected_count": len(fold_selection),
                "train_cutoff_market_day": fold["train_cutoff_market_day"],
            }
        )
    baseline_policy = _crypto_policy_metrics("baseline_market_mid", baseline_trades, settings=settings)
    calibrated_policy = _crypto_policy_metrics("calibrated_prediction", calibrated_trades, settings=settings)
    selection_policy = _crypto_policy_metrics("trade_selection_policy", selection_trades, settings=settings)
    probability = {
        "baseline": _probability_metrics_decimal(baseline_predictions),
        "calibrated": _probability_metrics_decimal(calibrated_predictions),
    }
    return {
        "status": "ok",
        "fold_count": len(folds),
        "folds": fold_summaries,
        "prediction_metrics": probability,
        "baseline_policy": baseline_policy,
        "candidate_policies": [calibrated_policy, selection_policy],
        "bucket_matrix": _crypto_bucket_matrix(calibrated_trades, settings=settings),
        "metrics": {
            "sample_count": len(rows),
            "resolved_sample_count": len(rows),
            "trade_candidate_count": calibrated_policy["selected_count"],
            "net_simulated_pl_dollars": float(_decimal(calibrated_policy["net_pnl"])),
            "fees_dollars": float(_decimal(calibrated_policy["fees"])),
            "hard_cap_breaches": calibrated_policy["hard_cap_breaches"],
            "calibration_brier": probability["calibrated"]["brier"],
            "market_mid_brier": probability["baseline"]["brier"],
            "calibration_log_loss": probability["calibrated"]["log_loss"],
            "market_mid_log_loss": probability["baseline"]["log_loss"],
            "calibration_ece": probability["calibrated"]["ece"],
            "market_mid_ece": probability["baseline"]["ece"],
            "fee_model_version": current_fee_model_version(),
        },
    }


def _crypto_walk_forward_folds(rows: list[dict[str, Any]], *, min_train_rows: int) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (str(row.get("market_day")), row.get("decision_ts") or datetime.max.replace(tzinfo=UTC)))
    days = sorted({str(row["market_day"]) for row in ordered if row.get("market_day")})
    folds: list[dict[str, Any]] = []
    for day in days:
        train = [row for row in ordered if str(row.get("market_day")) < day]
        test = [row for row in ordered if str(row.get("market_day")) == day]
        if len(train) < min_train_rows or not test:
            continue
        folds.append(
            {
                "fold_id": f"crypto-wf-{len(folds) + 1}",
                "train_cutoff_market_day": day,
                "train_rows": train,
                "test_rows": test,
            }
        )
    return folds


def _simulate_crypto_trade(row: dict[str, Any], predicted_yes: Decimal, *, settings: Settings) -> dict[str, Any]:
    label_yes = int(row["label_yes"])
    yes_cost = _decimal(row.get("yes_ask_dollars"))
    no_cost = _decimal(row.get("no_ask_dollars") or (Decimal("1") - _decimal(row.get("yes_bid_dollars"))))
    yes_ev = predicted_yes - yes_cost
    no_ev = (Decimal("1") - predicted_yes) - no_cost
    side = "yes" if yes_ev >= no_ev else "no"
    cost = yes_cost if side == "yes" else no_cost
    edge = yes_ev if side == "yes" else no_ev
    fee = estimate_kalshi_taker_fee_dollars(
        price_dollars=cost,
        count=Decimal("1.00"),
        fee_rate=Decimal(str(settings.kalshi_taker_fee_rate)),
    )
    fee_edge = fee
    min_edge = Decimal(settings.risk_min_edge_bps) / Decimal("10000")
    if edge - fee_edge < min_edge:
        return {
            "status": "not_selected",
            "side": side,
            "reason": "fee_adjusted_edge_below_min",
            "expected_net_edge": str((edge - fee_edge).quantize(Decimal("0.0001"))),
        }
    payoff = Decimal(label_yes) if side == "yes" else Decimal(1 - label_yes)
    gross = payoff - cost
    net = gross - fee
    return {
        "status": "fillable",
        "side": side,
        "execution_price_dollars": str(cost.quantize(Decimal("0.0001"))),
        "gross_pnl": str(gross.quantize(Decimal("0.0001"))),
        "fees": str(fee.quantize(Decimal("0.0001"))),
        "net_pnl": str(net.quantize(Decimal("0.0001"))),
        "expected_net_edge": str((edge - fee_edge).quantize(Decimal("0.0001"))),
        "bucket_key": _crypto_bucket_key(row, {"side": side}),
    }


def _crypto_policy_metrics(policy_name: str, trade_rows: list[dict[str, Any]], *, settings: Settings) -> dict[str, Any]:
    values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in trade_rows]
    gross = [_decimal((row.get("simulation") or {}).get("gross_pnl")) for row in trade_rows]
    fees = [_decimal((row.get("simulation") or {}).get("fees")) for row in trade_rows]
    wins = sum(1 for value in values if value > 0)
    return {
        "policy_name": policy_name,
        "selected_count": len(trade_rows),
        "fillable_count": len(trade_rows),
        "coverage": None,
        "gross_pnl": str(sum(gross, Decimal("0")).quantize(Decimal("0.0001"))),
        "fees": str(sum(fees, Decimal("0")).quantize(Decimal("0.0001"))),
        "net_pnl": str(sum(values, Decimal("0")).quantize(Decimal("0.0001"))),
        "max_drawdown": str(_crypto_max_drawdown(values).quantize(Decimal("0.0001"))),
        "sortino": _ratio(_crypto_sortino(values)),
        "sharpe": _ratio(_crypto_sharpe(values)),
        "win_rate": _ratio(wins / len(values)) if values else None,
        "win_rate_display_only": True,
        "cluster_count": len({(row.get("asset_symbol"), row.get("market_day")) for row in trade_rows}),
        "hard_cap_breaches": sum(1 for value in values if value < Decimal("-1.0000")),
        "worst_buckets": _crypto_bucket_matrix(trade_rows, settings=settings)[:10],
    }


def _eligible_crypto_buckets(rows: list[dict[str, Any]], *, settings: Settings) -> set[str]:
    simulations = [{**row, "simulation": _simulate_crypto_trade(row, _decimal(row["mid_yes_dollars"]), settings=settings)} for row in rows]
    matrix = _crypto_bucket_matrix([row for row in simulations if row["simulation"]["status"] == "fillable"], settings=settings)
    eligible: set[str] = set()
    for bucket in matrix:
        if int(bucket["sample_count"]) >= settings.trade_behavior_empirical_gate_min_settled_fills and _decimal(bucket["net_pnl"]) > Decimal(str(settings.trade_behavior_empirical_gate_min_net_pnl_dollars)):
            eligible.add(bucket["bucket_key"])
    return eligible


def _crypto_bucket_matrix(trade_rows: list[dict[str, Any]], *, settings: Settings) -> list[dict[str, Any]]:
    del settings
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_rows:
        key = _crypto_bucket_key(row, row.get("simulation") or {})
        grouped[key].append(row)
    matrix: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        values = [_decimal((row.get("simulation") or {}).get("net_pnl")) for row in rows]
        fees = [_decimal((row.get("simulation") or {}).get("fees")) for row in rows]
        gross = [_decimal((row.get("simulation") or {}).get("gross_pnl")) for row in rows]
        wins = sum(1 for value in values if value > 0)
        first = rows[0]
        net = sum(values, Decimal("0"))
        matrix.append(
            {
                "bucket_key": key,
                "asset_symbol": first.get("asset_symbol"),
                "side": (first.get("simulation") or {}).get("side"),
                "entry_price_band": _price_band(_decimal((first.get("simulation") or {}).get("execution_price_dollars") or first.get("mid_yes_dollars"))),
                "spread_band": _spread_band(first.get("spread_bps")),
                "sample_count": len(values),
                "win_rate": _ratio(wins / len(values)) if values else None,
                "gross_pnl": str(sum(gross, Decimal("0")).quantize(Decimal("0.0001"))),
                "fees": str(sum(fees, Decimal("0")).quantize(Decimal("0.0001"))),
                "net_pnl": str(net.quantize(Decimal("0.0001"))),
            }
        )
    matrix.sort(key=lambda item: (_decimal(item["net_pnl"]), item["bucket_key"]))
    return matrix


def _crypto_bucket_key(row: dict[str, Any], simulation: dict[str, Any]) -> str:
    side = simulation.get("side") or "unknown"
    price = _decimal(simulation.get("execution_price_dollars") or row.get("mid_yes_dollars"))
    return "|".join(
        [
            str(row.get("asset_symbol") or "unknown"),
            str(side),
            _price_band(price),
            _spread_band(row.get("spread_bps")),
        ]
    )


def _expected_crypto_net_pnl(
    market: CryptoMarket,
    side: ContractSide,
    fair_yes: Decimal,
    *,
    fee_rate: Decimal,
) -> Decimal | None:
    if side == ContractSide.YES:
        cost = market.yes_ask_dollars
        probability = fair_yes
    else:
        cost = market.no_ask_dollars if market.no_ask_dollars is not None else (Decimal("1") - market.yes_bid_dollars if market.yes_bid_dollars is not None else None)
        probability = Decimal("1") - fair_yes
    if cost is None:
        return None
    fee = estimate_kalshi_taker_fee_dollars(price_dollars=cost, count=Decimal("1.00"), fee_rate=fee_rate)
    return (probability - cost - fee).quantize(Decimal("0.0001"))


def _probability_metrics_decimal(predictions: list[tuple[Decimal, int]]) -> dict[str, Any]:
    if not predictions:
        return {"sample_count": 0, "brier": None, "log_loss": None, "ece": None}
    brier = sum((float(probability) - label) ** 2 for probability, label in predictions) / len(predictions)
    log_loss = -sum(
        label * math.log(max(1e-9, float(probability)))
        + (1 - label) * math.log(max(1e-9, 1 - float(probability)))
        for probability, label in predictions
    ) / len(predictions)
    buckets: dict[int, list[tuple[Decimal, int]]] = defaultdict(list)
    for probability, label in predictions:
        buckets[min(9, int(float(probability) * 10))].append((probability, label))
    ece = 0.0
    reliability = []
    for bucket, values in sorted(buckets.items()):
        predicted = sum(float(probability) for probability, _ in values) / len(values)
        observed = sum(label for _, label in values) / len(values)
        ece += (len(values) / len(predictions)) * abs(predicted - observed)
        reliability.append(
            {
                "bucket": f"{bucket / 10:.1f}-{(bucket + 1) / 10:.1f}",
                "sample_count": len(values),
                "avg_prediction": _ratio(predicted),
                "observed_rate": _ratio(observed),
            }
        )
    return {
        "sample_count": len(predictions),
        "brier": _ratio(brier),
        "log_loss": _ratio(log_loss),
        "ece": _ratio(ece),
        "reliability_buckets": reliability,
    }


def _crypto_max_drawdown(values: list[Decimal]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _crypto_sharpe(values: list[Decimal]) -> float | None:
    if not values:
        return None
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    variance = sum((value - mean) ** 2 for value in floats) / len(floats)
    return mean / max(math.sqrt(variance), 0.01)


def _crypto_sortino(values: list[Decimal]) -> float | None:
    if not values:
        return None
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    downside = [value for value in floats if value < 0]
    downside_dev = math.sqrt(sum(value * value for value in downside) / len(downside)) if downside else 0.0
    return mean / max(downside_dev, 0.01)


def _price_band(price: Decimal) -> str:
    if price < Decimal("0.25"):
        return "0.00-0.25"
    if price < Decimal("0.50"):
        return "0.25-0.50"
    if price < Decimal("0.75"):
        return "0.50-0.75"
    return "0.75-1.00"


def _spread_band(spread_bps: Any) -> str:
    try:
        value = int(spread_bps or 0)
    except (TypeError, ValueError):
        return "unknown"
    if value <= 100:
        return "tight"
    if value <= 300:
        return "normal"
    return "wide"


def _ratio(value: float | None) -> float | None:
    return round(value, 6) if value is not None and math.isfinite(value) else None


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _issue_code(reason: str) -> str:
    lowered = "".join(ch if ch.isalnum() else "_" for ch in reason.lower()).strip("_")
    return lowered[:80] or "crypto_replay_issue"
