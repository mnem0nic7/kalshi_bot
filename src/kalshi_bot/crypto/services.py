from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
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
    parse_datetime,
)
from kalshi_bot.db.models import CryptoMarketSnapshotRecord, Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext, approved_ticket_for_verdict
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars

logger = logging.getLogger(__name__)


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


class CryptoMarketService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
        agent_pack_service: AgentPackService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.agent_pack_service = agent_pack_service

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
                "crypto_order_mode": self.settings.crypto_order_mode,
            },
            "replay_gate": {
                "status": gate.status if gate is not None else "missing",
                "version": gate.version if gate is not None else None,
                "metrics": gate.metrics if gate is not None else {},
                "payload": gate_payload,
            },
            "markets": [
                {
                    **market.to_payload(),
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
            room = await repo.create_room(
                RoomCreate(
                    name=f"{market.asset_symbol} 15 Minute Crypto",
                    market_ticker=market.market_ticker,
                    prompt=(
                        "Crypto 15m workflow. "
                        f"asset={market.asset_symbol} target={_money_text(market.target_price_dollars)} "
                        f"close_time={market.close_time.isoformat() if market.close_time else 'unknown'} "
                        f"reason={reason}"
                    ),
                ),
                active_color=control.active_color,
                shadow_mode=self.settings.app_shadow_mode or not self.settings.crypto_trading_enabled,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self.settings.kalshi_env,
                room_origin=RoomOrigin.SHADOW.value if self.settings.app_shadow_mode or not self.settings.crypto_trading_enabled else RoomOrigin.LIVE.value,
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
                    "market": market.to_payload(),
                },
            )
            await session.commit()
        return {"room_id": room.id, "redirect": f"/rooms/{room.id}", "market_ticker": market.market_ticker}

    async def status(self, *, frequency: str = "15m") -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
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
            await session.commit()
        return {
            "market_domain": "crypto",
            "frequency": normalize_frequency(frequency) or "15m",
            "crypto_enabled": self.settings.crypto_enabled,
            "crypto_15m_enabled": self.settings.crypto_15m_enabled,
            "crypto_trading_enabled": self.settings.crypto_trading_enabled,
            "stored_market_count": len(snapshots),
            "model": _artifact_summary(model),
            "replay_gate": _artifact_summary(gate),
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
        for series in series_rows:
            try:
                response = await self.kalshi.list_historical_markets(series_ticker=series.series_ticker, limit=1000)
            except httpx.HTTPError as exc:
                errors.append({"series_ticker": series.series_ticker, "error": str(exc)})
                continue
            for row in _rows_from_response(response, "markets"):
                parsed = parse_crypto_market(row, series=series, frequency=frequency)
                if parsed is None:
                    continue
                if parsed.close_time is None or parsed.close_time >= cutoff:
                    historical_markets.append(parsed)
        all_markets = {market.market_ticker: market for market in [*historical_markets, *live_markets]}
        candles_stored = 0
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for market in all_markets.values():
                await self.market_service.record_market_snapshot(
                    repo,
                    market,
                    source_kind="historical" if market.market_ticker in {m.market_ticker for m in historical_markets} else "live",
                    observed_at=market.close_time or datetime.now(UTC),
                )
                candles_stored += await self._capture_candles(repo, market, cutoff=cutoff)
            await session.commit()
        return {
            "status": "ok",
            "frequency": normalize_frequency(frequency) or "15m",
            "lookback_days": lookback_days,
            "markets_stored": len(all_markets),
            "live_markets": len(live_markets),
            "historical_markets": len(historical_markets),
            "candles_stored": candles_stored,
            "errors": errors[:10],
        }

    async def daily(self, *, frequency: str = "15m") -> dict[str, Any]:
        return await self.bootstrap(days=2, frequency=frequency)

    async def _capture_candles(self, repo: PlatformRepository, market: CryptoMarket, *, cutoff: datetime) -> int:
        params = {
            "period_interval": 1,
            "start_ts": int(cutoff.timestamp()),
            "end_ts": int(datetime.now(UTC).timestamp()),
        }
        try:
            if market.close_time is not None and market.close_time < datetime.now(UTC):
                response = await self.kalshi.get_historical_market_candlesticks(
                    market.series_ticker,
                    market.market_ticker,
                    **params,
                )
            else:
                response = await self.kalshi.get_market_candlesticks(market.series_ticker, market.market_ticker, **params)
        except httpx.HTTPError:
            logger.info("crypto candlestick capture skipped for %s", market.market_ticker, exc_info=True)
            return 0
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
        return count


class CryptoForecastService:
    def __init__(self, *, settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def train(self, *, frequency: str = "15m") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            rows = await repo.list_crypto_market_snapshots(frequency=freq, kalshi_env=self.settings.kalshi_env, limit=100_000)
            settled = [row for row in rows if row.settlement_result in {"yes", "no"} and _row_mid(row) is not None]
            sample_count = len(settled)
            payload: dict[str, Any] = {"asset_adjustments_bps": {}, "global_adjustment_bps": 0}
            metrics = {
                "sample_count": sample_count,
                "resolved_sample_count": sample_count,
                "trade_candidate_count": 0,
                "net_simulated_pl_dollars": 0.0,
                "hard_cap_breaches": 0,
                "calibration_brier": None,
                "market_mid_brier": None,
            }
            status = "insufficient_data"
            if sample_count >= self.settings.crypto_min_training_samples:
                outcome_sum = Decimal("0")
                mid_sum = Decimal("0")
                brier_model = Decimal("0")
                brier_mid = Decimal("0")
                by_asset: dict[str, list[tuple[Decimal, Decimal]]] = {}
                for row in settled:
                    outcome = Decimal("1") if row.settlement_result == "yes" else Decimal("0")
                    mid = _row_mid(row) or Decimal("0.5000")
                    outcome_sum += outcome
                    mid_sum += mid
                    by_asset.setdefault(row.asset_symbol, []).append((outcome, mid))
                global_adjust = (outcome_sum / sample_count) - (mid_sum / sample_count)
                global_adjust_bps = int((global_adjust * Decimal("10000")).to_integral_value())
                payload["global_adjustment_bps"] = global_adjust_bps
                for asset, items in by_asset.items():
                    outcome_avg = sum((item[0] for item in items), Decimal("0")) / len(items)
                    mid_avg = sum((item[1] for item in items), Decimal("0")) / len(items)
                    payload["asset_adjustments_bps"][asset] = int(((outcome_avg - mid_avg) * Decimal("10000")).to_integral_value())
                trade_candidates = 0
                simulated_pl = Decimal("0")
                for row in settled:
                    outcome = Decimal("1") if row.settlement_result == "yes" else Decimal("0")
                    mid = _row_mid(row) or Decimal("0.5000")
                    adjustment = Decimal(payload["global_adjustment_bps"]) / Decimal("10000")
                    adjustment += Decimal(payload["asset_adjustments_bps"].get(row.asset_symbol, 0)) / Decimal("20000")
                    predicted = _clamp_price(mid + adjustment)
                    brier_model += (predicted - outcome) ** 2
                    brier_mid += (mid - outcome) ** 2
                    edge = abs(predicted - mid)
                    if edge >= Decimal(self.settings.risk_min_edge_bps) / Decimal("10000"):
                        trade_candidates += 1
                        simulated_pl += (outcome - mid) if predicted >= mid else (mid - outcome)
                metrics.update(
                    {
                        "trade_candidate_count": trade_candidates,
                        "net_simulated_pl_dollars": float(simulated_pl),
                        "calibration_brier": float(brier_model / sample_count),
                        "market_mid_brier": float(brier_mid / sample_count),
                    }
                )
                status = "trained"
            artifact_payload = {
                **payload,
                "frequency": freq,
                "trained_from": "official_kalshi_market_snapshots",
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
                    "recent_same_series_behavior",
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

    async def gate(self, *, frequency: str = "15m") -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            model = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="model",
                kalshi_env=self.settings.kalshi_env,
            )
            metrics = dict(model.metrics if model is not None else {})
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
            },
        }


class CryptoExecutionService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        base_execution_service: ExecutionService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.base_execution_service = base_execution_service

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
        if not self.settings.crypto_trading_enabled:
            return ExecReceiptPayload(
                status="crypto_trading_disabled",
                client_order_id=client_order_id,
                details={"reason": "crypto_trading_enabled is false"},
            )
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=market.frequency,
                artifact_type="replay_gate",
                kalshi_env=room.kalshi_env,
            )
            await session.commit()
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
                control=control,
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
            control=control,
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
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.market_service = market_service
        self.forecast_service = forecast_service
        self.risk_engine = risk_engine
        self.execution_service = execution_service

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
                market_artifact = await repo.save_artifact(
                    room_id=room.id,
                    artifact_type="market_snapshot",
                    source="crypto_workflow",
                    title=f"{market.asset_symbol} 15m crypto snapshot",
                    payload={
                        "market_domain": "crypto",
                        "frequency": market.frequency,
                        "strategy_code": StrategyCode.CRYPTO_15M.value,
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
                await repo.append_message(
                    room.id,
                    RoomMessageCreate(
                        role=AgentRole.TRADER,
                        kind=MessageKind.TRADE_TICKET,
                        stage=RoomStage.PROPOSING,
                        content=f"Proposed crypto {ticket.side.value.upper()} ticket for {ticket.count_fp} contracts.",
                        payload={**ticket.model_dump(mode="json"), "strategy_code": StrategyCode.CRYPTO_15M.value},
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
                if receipt.external_order_id or receipt.status not in {"shadow_skipped", "inactive_color_skipped"}:
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
