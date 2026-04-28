from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.db.models import MarketState, ResearchDossierRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.position_governance import (
    STOP_LOSS_OUTCOME_FILLED_EXIT,
    STOP_LOSS_OUTCOME_SUBMIT_FAILED,
    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
    refresh_stop_loss_checkpoints,
    stop_loss_outcome_from_payloads,
    stop_loss_reentry_blocked,
    stop_loss_stopped_at_from_payloads,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory


class SupervisorProtocol(Protocol):
    async def run_room(self, room_id: str, reason: str = "manual") -> None: ...


class AutoTriggerService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        supervisor: SupervisorProtocol,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service
        self.supervisor = supervisor
        self._inflight_markets: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    async def handle_market_update(self, market_ticker: str) -> None:
        if not self.settings.trigger_enable_auto_rooms:
            return
        if not self.weather_directory.supports_market_ticker(market_ticker):
            return
        if market_ticker in self._inflight_markets:
            return

        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
            if control.active_color != self.settings.app_color:
                await session.commit()
                return
            pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
            thresholds = self.agent_pack_service.runtime_thresholds(pack)
            market_state = await repo.get_market_state(market_ticker, kalshi_env=self.settings.kalshi_env)
            if market_state is None or not self._market_is_actionable(market_state, thresholds):
                await session.commit()
                return
            terminal_market = self._terminal_market_lifecycle(market_state)
            if terminal_market is not None:
                await self._log_block_once_per_cooldown(
                    repo,
                    checkpoint_key=(
                        f"auto_trigger_block:{self.settings.kalshi_env}:{market_ticker}:"
                        "terminal_market"
                    ),
                    cooldown_seconds=thresholds.trigger_cooldown_seconds,
                    severity="info",
                    summary=f"Auto-trigger skipped for {market_ticker}: market lifecycle is terminal",
                    payload={"market_ticker": market_ticker, "reason": "terminal_market", **terminal_market},
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
                return
            dossier_record = await repo.get_research_dossier(market_ticker)
            resolved_research = self._fresh_resolved_research(dossier_record, now=datetime.now(UTC))
            if resolved_research is not None:
                await self._log_block_once_per_cooldown(
                    repo,
                    checkpoint_key=(
                        f"auto_trigger_block:{self.settings.kalshi_env}:{market_ticker}:"
                        "resolved_contract"
                    ),
                    cooldown_seconds=thresholds.trigger_cooldown_seconds,
                    severity="info",
                    summary=f"Auto-trigger skipped for {market_ticker}: latest research says contract is resolved",
                    payload={"market_ticker": market_ticker, "reason": "resolved_contract", **resolved_research},
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
                return

            active_count = await repo.count_active_rooms(
                color=self.settings.app_color,
                kalshi_env=self.settings.kalshi_env,
                updated_within_seconds=self.settings.trigger_active_room_stale_seconds,
            )
            if active_count >= self.settings.trigger_max_concurrent_rooms:
                await self._log_block_once_per_cooldown(
                    repo,
                    checkpoint_key=(
                        f"auto_trigger_block:{self.settings.kalshi_env}:{market_ticker}:"
                        "max_concurrent_rooms"
                    ),
                    cooldown_seconds=thresholds.trigger_cooldown_seconds,
                    severity="warning",
                    summary=f"Auto-trigger skipped for {market_ticker}: max concurrent rooms reached",
                    payload={"market_ticker": market_ticker},
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
                return

            existing_room = await repo.get_latest_active_room_for_market(
                market_ticker,
                kalshi_env=self.settings.kalshi_env,
            )
            if existing_room is not None:
                await session.commit()
                return

            open_position = await repo.get_position(
                market_ticker,
                self.settings.kalshi_subaccount,
                kalshi_env=self.settings.kalshi_env,
            )
            if open_position is not None and Decimal(str(open_position.count_fp)) > Decimal("0"):
                await self._log_block_once_per_cooldown(
                    repo,
                    checkpoint_key=(
                        f"auto_trigger_block:{self.settings.kalshi_env}:{market_ticker}:"
                        "open_position_governance"
                    ),
                    cooldown_seconds=thresholds.trigger_cooldown_seconds,
                    severity="warning",
                    summary=f"Auto-trigger blocked for {market_ticker}: live position already open",
                    payload={"market_ticker": market_ticker, "reason": "open_position_governance"},
                    kalshi_env=self.settings.kalshi_env,
                )
                await session.commit()
                return

            await refresh_stop_loss_checkpoints(
                repo,
                settings=self.settings,
                kalshi_env=self.settings.kalshi_env,
                subaccount=self.settings.kalshi_subaccount,
                market_tickers=[market_ticker],
                log_repairs=True,
            )
            submit_cp = await repo.get_checkpoint(f"stop_loss_submit:{self.settings.kalshi_env}:{market_ticker}")

            reentry_cp = await repo.get_checkpoint(f"stop_loss_reentry:{self.settings.kalshi_env}:{market_ticker}")
            if reentry_cp is not None:
                submit_payload = dict(getattr(submit_cp, "payload", {}) or {})
                reentry_payload = dict(reentry_cp.payload or {})
                reentry_status = stop_loss_outcome_from_payloads(
                    submit_payload,
                    reentry_payload,
                )
                if reentry_status in {
                    STOP_LOSS_OUTCOME_SUBMIT_FAILED,
                    STOP_LOSS_OUTCOME_SUBMITTED_PENDING_FILL,
                }:
                    await self._log_block_once_per_cooldown(
                        repo,
                        checkpoint_key=(
                            f"auto_trigger_block:{self.settings.kalshi_env}:{market_ticker}:"
                            "stop_loss_unresolved"
                        ),
                        cooldown_seconds=thresholds.trigger_cooldown_seconds,
                        severity="warning",
                        summary=f"Auto-trigger blocked for {market_ticker}: stop-loss still unresolved",
                        payload={"market_ticker": market_ticker, "stop_loss_outcome_status": reentry_status},
                        kalshi_env=self.settings.kalshi_env,
                    )
                    await session.commit()
                    return
                if reentry_status not in {None, STOP_LOSS_OUTCOME_FILLED_EXIT}:
                    await session.commit()
                    return
                stopped_at = stop_loss_stopped_at_from_payloads(submit_payload, reentry_payload)
                if stop_loss_reentry_blocked(
                    reentry_status,
                    stopped_at=stopped_at,
                    cooldown_seconds=self.settings.stop_loss_reentry_cooldown_seconds,
                ):
                    cooldown_expires_at = (
                        stopped_at + timedelta(seconds=self.settings.stop_loss_reentry_cooldown_seconds)
                        if stopped_at is not None
                        else None
                    )
                    await self._log_block_once_per_cooldown(
                        repo,
                        checkpoint_key=(
                            f"auto_trigger_block:{self.settings.kalshi_env}:{market_ticker}:"
                            "stop_loss_reentry_cooldown"
                        ),
                        cooldown_seconds=thresholds.trigger_cooldown_seconds,
                        severity="warning",
                        summary=f"Auto-trigger blocked for {market_ticker}: stop-loss re-entry cooldown active",
                        payload={
                            "market_ticker": market_ticker,
                            "reason": "stop_loss_reentry_cooldown",
                            "stop_loss_outcome_status": reentry_status,
                            "stopped_at": stopped_at.isoformat() if stopped_at is not None else None,
                            "cooldown_expires_at": (
                                cooldown_expires_at.isoformat() if cooldown_expires_at is not None else None
                            ),
                            "stopped_side": reentry_payload.get("stopped_side") or submit_payload.get("stopped_side"),
                        },
                        kalshi_env=self.settings.kalshi_env,
                    )
                    await session.commit()
                    return

            checkpoint = await repo.get_checkpoint(f"auto_trigger:{self.settings.kalshi_env}:{market_ticker}")
            if checkpoint is not None:
                last_triggered_at = checkpoint.payload.get("last_triggered_at")
                if last_triggered_at is not None:
                    last_trigger_time = datetime.fromisoformat(last_triggered_at)
                    cooldown = (
                        self.settings.trigger_broken_book_retry_seconds
                        if checkpoint.payload.get("book_broken")
                        else thresholds.trigger_cooldown_seconds
                    )
                    if datetime.now(UTC) - last_trigger_time < timedelta(seconds=cooldown):
                        # Bypass cooldown if mid price has moved enough since last trigger.
                        # If market state is stale (WebSocket down), skip the bypass — enforce full cooldown.
                        stale_cutoff = timedelta(seconds=self.settings.risk_stale_market_seconds)
                        state_is_fresh = (
                            market_state is not None
                            and market_state.observed_at is not None
                            and (datetime.now(UTC) - market_state.observed_at) <= stale_cutoff
                        )
                        last_mid_raw = checkpoint.payload.get("last_trigger_mid")
                        bypassed = False
                        if state_is_fresh and last_mid_raw is not None and self.settings.trigger_price_move_bypass_bps > 0:
                            current_mid = self._mid_dollars(market_state)
                            if current_mid is not None:
                                move_bps = int(abs(current_mid - Decimal(str(last_mid_raw))) * Decimal("10000"))
                                bypassed = move_bps >= self.settings.trigger_price_move_bypass_bps
                        if not bypassed:
                            await session.commit()
                            return

            if self._book_is_broken(market_state):
                await repo.set_checkpoint(
                    f"auto_trigger:{self.settings.kalshi_env}:{market_ticker}",
                    cursor=None,
                    payload={"last_triggered_at": datetime.now(UTC).isoformat(), "book_broken": True},
                )
                await session.commit()
                return

            spread_bps = self._spread_bps(market_state)
            room = await repo.create_room(
                RoomCreate(
                    name=f"auto {market_ticker}",
                    market_ticker=market_ticker,
                    prompt=f"Auto-triggered from live orderbook with spread {spread_bps}bps.",
                ),
                active_color=self.settings.app_color,
                shadow_mode=self.settings.app_shadow_mode,
                kill_switch_enabled=control.kill_switch_enabled,
                kalshi_env=self.settings.kalshi_env,
                agent_pack_version=pack.version,
            )
            current_mid = self._mid_dollars(market_state)
            await repo.set_checkpoint(
                f"auto_trigger:{self.settings.kalshi_env}:{market_ticker}",
                cursor=None,
                payload={
                    "last_triggered_at": datetime.now(UTC).isoformat(),
                    "room_id": room.id,
                    "spread_bps": spread_bps,
                    "agent_pack_version": pack.version,
                    "last_trigger_mid": str(current_mid) if current_mid is not None else None,
                },
            )
            await repo.log_ops_event(
                severity="info",
                summary=f"Auto-trigger launched room for {market_ticker}",
                source="auto_trigger",
                payload={"market_ticker": market_ticker, "room_id": room.id, "spread_bps": spread_bps, "agent_pack_version": pack.version},
                room_id=room.id,
            )
            await session.commit()

        self._inflight_markets.add(market_ticker)
        task = asyncio.create_task(self._run_room(market_ticker, room.id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_room(self, market_ticker: str, room_id: str) -> None:
        try:
            await self.supervisor.run_room(room_id, reason="auto_trigger")
        finally:
            self._inflight_markets.discard(market_ticker)

    async def wait_for_tasks(self) -> None:
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _log_block_once_per_cooldown(
        self,
        repo: PlatformRepository,
        *,
        checkpoint_key: str,
        cooldown_seconds: int,
        severity: str,
        summary: str,
        payload: dict,
        kalshi_env: str,
    ) -> bool:
        now = datetime.now(UTC)
        checkpoint = await repo.get_checkpoint(checkpoint_key)
        if checkpoint is not None:
            last_logged_at = checkpoint.payload.get("last_logged_at")
            try:
                last_logged = datetime.fromisoformat(last_logged_at) if last_logged_at else None
            except (TypeError, ValueError):
                last_logged = None
            if last_logged is not None and now - last_logged < timedelta(seconds=cooldown_seconds):
                return False

        await repo.log_ops_event(
            severity=severity,
            summary=summary,
            source="auto_trigger",
            payload=payload,
            kalshi_env=kalshi_env,
        )
        await repo.set_checkpoint(
            checkpoint_key,
            cursor=None,
            payload={
                "last_logged_at": now.isoformat(),
                "summary": summary,
                **payload,
            },
        )
        return True

    def _book_is_broken(self, market_state: MarketState) -> bool:
        yes_ask = market_state.yes_ask_dollars
        yes_bid = market_state.yes_bid_dollars
        if yes_ask is None or yes_bid is None:
            return True
        no_ask = Decimal("1") - yes_bid
        return (
            (yes_ask >= Decimal("0.9900") and no_ask >= Decimal("0.9400"))
            or (no_ask >= Decimal("0.9900") and yes_ask >= Decimal("0.9400"))
        )

    def _market_is_actionable(self, market_state: MarketState, thresholds: RuntimeThresholds) -> bool:
        yes_bid = market_state.yes_bid_dollars
        yes_ask = market_state.yes_ask_dollars
        if yes_bid is None or yes_ask is None:
            return False
        spread_bps = self._spread_bps(market_state)
        if spread_bps <= 0:
            return False
        return spread_bps <= thresholds.trigger_max_spread_bps

    def _fresh_resolved_research(
        self,
        dossier_record: ResearchDossierRecord | None,
        *,
        now: datetime,
    ) -> dict[str, Any] | None:
        if dossier_record is None:
            return None
        if self._record_is_expired(dossier_record, now=now):
            return None

        payload = dict(dossier_record.payload or {})
        freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
        if bool(freshness.get("stale")):
            return None

        trader_context = payload.get("trader_context") if isinstance(payload.get("trader_context"), dict) else {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        numeric_facts = (
            summary.get("current_numeric_facts")
            if isinstance(summary.get("current_numeric_facts"), dict)
            else {}
        )
        resolution_state = (
            trader_context.get("resolution_state")
            or numeric_facts.get("resolution_state")
        )
        if resolution_state not in {
            WeatherResolutionState.LOCKED_YES.value,
            WeatherResolutionState.LOCKED_NO.value,
        }:
            return None

        return {
            "resolution_state": str(resolution_state),
            "last_run_id": getattr(dossier_record, "last_run_id", None),
            "expires_at": self._iso_or_none(getattr(dossier_record, "expires_at", None)),
            "current_temp_f": numeric_facts.get("current_temp_f"),
            "threshold_f": numeric_facts.get("threshold_f"),
        }

    def _terminal_market_lifecycle(self, market_state: MarketState) -> dict[str, Any] | None:
        snapshot = market_state.snapshot if isinstance(market_state.snapshot, dict) else {}
        lifecycle = snapshot.get("lifecycle") if isinstance(snapshot.get("lifecycle"), dict) else None
        if lifecycle is None:
            return None

        status = self._lower_or_none(
            lifecycle.get("status")
            or lifecycle.get("state")
            or lifecycle.get("market_status")
            or lifecycle.get("trading_status")
        )
        result = self._lower_or_none(
            lifecycle.get("result")
            or lifecycle.get("settlement_result")
            or lifecycle.get("settled_result")
            or lifecycle.get("winning_side")
        )
        if status not in {"closed", "settled", "resolved", "finalized", "expired"} and result not in {"yes", "no"}:
            return None

        return {
            "lifecycle_status": status,
            "lifecycle_result": result,
            "lifecycle_event_type": lifecycle.get("event_type") or lifecycle.get("type"),
        }

    @staticmethod
    def _record_is_expired(dossier_record: ResearchDossierRecord, *, now: datetime) -> bool:
        expires_at = AutoTriggerService._as_utc(getattr(dossier_record, "expires_at", None))
        if expires_at is None:
            payload = dict(getattr(dossier_record, "payload", {}) or {})
            freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
            expires_at = AutoTriggerService._parse_datetime(freshness.get("expires_at"))
        return expires_at is not None and expires_at <= AutoTriggerService._as_utc(now)

    @staticmethod
    def _as_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return AutoTriggerService._as_utc(value)
        if not isinstance(value, str) or not value:
            return None
        try:
            return AutoTriggerService._as_utc(datetime.fromisoformat(value))
        except ValueError:
            return None

    @staticmethod
    def _iso_or_none(value: datetime | None) -> str | None:
        parsed = AutoTriggerService._as_utc(value)
        return parsed.isoformat() if parsed is not None else None

    @staticmethod
    def _lower_or_none(value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value).strip().lower()

    @staticmethod
    def _spread_bps(market_state: MarketState) -> int:
        yes_bid = Decimal(str(market_state.yes_bid_dollars or 0))
        yes_ask = Decimal(str(market_state.yes_ask_dollars or 0))
        return int(((yes_ask - yes_bid) * Decimal("10000")).to_integral_value())

    @staticmethod
    def _mid_dollars(market_state: MarketState) -> Decimal | None:
        yes_bid = market_state.yes_bid_dollars
        yes_ask = market_state.yes_ask_dollars
        if yes_bid is None:
            return None
        if yes_ask is None:
            return yes_bid
        return (yes_bid + yes_ask) / Decimal("2")
