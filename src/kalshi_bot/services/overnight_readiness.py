from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.crypto.parsing import normalize_frequency
from kalshi_bot.crypto.services import (
    CRYPTO_ASSET_MODE_LIVE,
    CRYPTO_LIVE_QUALITY,
    _crypto_artifact_type,
    _crypto_candidate_quality_report,
    _crypto_data_quality,
    _crypto_decision_rows,
    _crypto_spot_quality,
    normalize_asset_symbol,
)
from kalshi_bot.db.models import Checkpoint, DeploymentControl, FillRecord, OrderRecord, RiskVerdictRecord, Room, Signal, TradeTicketRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.crypto_spot import COINGECKO_IDS, COINBASE_PRODUCT_IDS


SCHEMA_VERSION = "overnight-readiness-v1"
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_START_HOUR = 22
DEFAULT_END_HOUR = 6
THIN_SAMPLE_WARNING_ROWS = 20
CRYPTO_MARKET_PREFIXES = (
    "KXBTC15M",
    "KXETH15M",
    "KXSOL15M",
    "KXXRP15M",
    "KXDOGE15M",
    "KXBNB15M",
    "KXHYPE15M",
    "KXBTC",
    "KXBTCD",
    "KXETH",
    "KXETHD",
    "KXSOL",
    "KXSOLE",
    "KXSOLD",
    "KXXRP",
    "KXXRPD",
    "KXRIPPLE",
    "KXDOGE",
    "KXDOGED",
    "KXBNB",
    "KXBNBD",
    "KXHYPE",
    "KXHYPED",
)


@dataclass(frozen=True, slots=True)
class OvernightWindow:
    timezone_name: str = DEFAULT_TIMEZONE
    start_hour: int = DEFAULT_START_HOUR
    end_hour: int = DEFAULT_END_HOUR

    def __post_init__(self) -> None:
        if not 0 <= self.start_hour <= 23:
            raise ValueError("start_hour must be between 0 and 23")
        if not 0 <= self.end_hour <= 23:
            raise ValueError("end_hour must be between 0 and 23")
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {self.timezone_name}") from exc

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    def contains(self, value: Any) -> bool:
        ts = _as_utc(value)
        if ts is None:
            return False
        local = ts.astimezone(self.timezone)
        hour = local.hour
        if self.start_hour == self.end_hour:
            return True
        if self.start_hour < self.end_hour:
            return self.start_hour <= hour < self.end_hour
        return hour >= self.start_hour or hour < self.end_hour

    def local_fields(self, value: Any) -> dict[str, Any]:
        ts = _as_utc(value)
        if ts is None:
            return {"local_ts": None, "local_date": None, "local_hour": None}
        local = ts.astimezone(self.timezone)
        return {
            "local_ts": local.isoformat(),
            "local_date": local.date().isoformat(),
            "local_hour": local.hour,
        }

    def to_payload(self) -> dict[str, Any]:
        return {
            "timezone": self.timezone_name,
            "start_hour": self.start_hour,
            "end_hour": self.end_hour,
            "start_inclusive": True,
            "end_exclusive": True,
        }


def is_in_overnight_window(
    value: Any,
    *,
    timezone_name: str = DEFAULT_TIMEZONE,
    start_hour: int = DEFAULT_START_HOUR,
    end_hour: int = DEFAULT_END_HOUR,
) -> bool:
    return OvernightWindow(timezone_name=timezone_name, start_hour=start_hour, end_hour=end_hour).contains(value)


class OvernightReadinessService:
    """Read-only live readiness evaluator for overnight trading."""

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        trade_analysis_service: Any,
        trading_audit_service: Any | None,
        crypto_asset_control_service: Any,
        has_write_credentials: bool | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.trade_analysis_service = trade_analysis_service
        self.trading_audit_service = trading_audit_service
        self.crypto_asset_control_service = crypto_asset_control_service
        self.has_write_credentials = has_write_credentials

    async def build_report(
        self,
        *,
        kalshi_env: str = "production",
        domains: str = "all",
        timezone_name: str = DEFAULT_TIMEZONE,
        start_hour: int = DEFAULT_START_HOUR,
        end_hour: int = DEFAULT_END_HOUR,
        days: int = 180,
        frequency: str = "15m",
        weather_analysis_mode: str = "fast",
        now: datetime | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        now_utc = _as_utc(now) or datetime.now(UTC)
        window = OvernightWindow(
            timezone_name=timezone_name,
            start_hour=start_hour,
            end_hour=end_hour,
        )
        domain_set = _domain_set(domains)
        shared = await self._shared_readiness(kalshi_env=kalshi_env, now=now_utc)

        domain_reports: dict[str, Any] = {}
        if "weather" in domain_set:
            domain_reports["weather"] = await self._weather_readiness(
                kalshi_env=kalshi_env,
                days=days,
                now=now_utc,
                window=window,
                limit=limit,
                analysis_mode=weather_analysis_mode,
            )
        if "crypto" in domain_set:
            domain_reports["crypto"] = await self._crypto_readiness(
                kalshi_env=kalshi_env,
                days=days,
                now=now_utc,
                window=window,
                frequency=frequency,
            )

        hard_blockers = _unique_issues(
            [*shared["hard_blockers"]]
            + [
                issue
                for report in domain_reports.values()
                for issue in report.get("hard_blockers", [])
            ]
        )
        warnings = _unique_issues(
            [*shared["warnings"]]
            + [
                issue
                for report in domain_reports.values()
                for issue in report.get("warnings", [])
            ]
        )
        status = _status(hard_blockers, warnings)
        report = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": now_utc.isoformat(),
            "kalshi_env": kalshi_env,
            "settings_kalshi_env": self.settings.kalshi_env,
            "status": status,
            "ready_for_live": not hard_blockers,
            "read_only": True,
            "window": window.to_payload(),
            "domains_requested": sorted(domain_set),
            "shared": shared,
            "domains": domain_reports,
            "hard_blockers": hard_blockers,
            "warnings": warnings,
            "operator_actions": self._operator_actions(hard_blockers, warnings),
        }
        return _json_safe(report)

    async def _shared_readiness(self, *, kalshi_env: str, now: datetime) -> dict[str, Any]:
        hard_blockers: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            control = await session.get(DeploymentControl, kalshi_env)
            heartbeat = await _get_checkpoint(session, f"daemon_heartbeat:{kalshi_env}:{self.settings.app_color}")
            reconcile = await _get_checkpoint(session, f"daemon_reconcile:{kalshi_env}:{self.settings.app_color}")

        if control is None:
            hard_blockers.append(_issue("global", "deployment_control_missing", f"No deployment control row exists for {kalshi_env}."))
            control_payload = {
                "present": False,
                "active_color": None,
                "kill_switch_enabled": None,
                "notes": {},
            }
        else:
            notes = dict(control.notes or {})
            control_payload = {
                "present": True,
                "active_color": control.active_color,
                "app_color": self.settings.app_color,
                "kill_switch_enabled": control.kill_switch_enabled,
                "execution_lock_holder": control.execution_lock_holder,
                "notes": notes,
            }
            if control.kill_switch_enabled:
                hard_blockers.append(_issue("global", "kill_switch_enabled", "Kill switch is enabled."))
            if control.active_color != self.settings.app_color:
                hard_blockers.append(
                    _issue(
                        "global",
                        "active_color_mismatch",
                        f"Active color is {control.active_color}; this app is {self.settings.app_color}.",
                    )
                )
            post_clear_reason = _post_clear_reconcile_blocker(control, reconcile)
            if post_clear_reason is not None:
                hard_blockers.append(_issue("global", "pending_reconcile_after_kill_switch_clear", post_clear_reason))

        if self.settings.app_shadow_mode:
            hard_blockers.append(_issue("global", "app_shadow_mode", "App shadow mode is enabled."))
        if not self._has_write_credentials(kalshi_env=kalshi_env):
            hard_blockers.append(_issue("global", "write_credentials_missing", "Kalshi write credentials are missing for live readiness."))
        if _normalize_env(self.settings.kalshi_env) != _normalize_env(kalshi_env):
            hard_blockers.append(
                _issue(
                    "global",
                    "settings_env_mismatch",
                    f"Settings are loaded for {self.settings.kalshi_env}; readiness target is {kalshi_env}.",
                )
            )

        daemon_payload = {
            "heartbeat": _checkpoint_payload(heartbeat, "heartbeat_at", now),
            "reconcile": _checkpoint_payload(reconcile, "reconciled_at", now),
        }
        heartbeat_age = daemon_payload["heartbeat"].get("age_seconds")
        if heartbeat_age is None:
            warnings.append(_issue("global", "daemon_heartbeat_missing", "No daemon heartbeat checkpoint exists for this app color.", severity="warn"))
        elif heartbeat_age > max(1, int(self.settings.daemon_reconcile_stale_kill_switch_seconds)):
            warnings.append(_issue("global", "daemon_heartbeat_stale", "Daemon heartbeat checkpoint is stale.", severity="warn"))
        reconcile_age = daemon_payload["reconcile"].get("age_seconds")
        if reconcile_age is None:
            warnings.append(_issue("global", "daemon_reconcile_missing", "No daemon reconcile checkpoint exists for this app color.", severity="warn"))
        elif reconcile_age > max(1, int(self.settings.daemon_reconcile_stale_kill_switch_seconds)):
            warnings.append(_issue("global", "daemon_reconcile_stale", "Daemon reconcile checkpoint is stale.", severity="warn"))

        return {
            "status": _status(hard_blockers, warnings),
            "hard_blockers": _unique_issues(hard_blockers),
            "warnings": _unique_issues(warnings),
            "deployment_control": control_payload,
            "daemon": daemon_payload,
            "settings": {
                "app_shadow_mode": self.settings.app_shadow_mode,
                "app_color": self.settings.app_color,
                "kalshi_env": self.settings.kalshi_env,
                "has_write_credentials": self._has_write_credentials(kalshi_env=kalshi_env),
            },
        }

    async def _weather_readiness(
        self,
        *,
        kalshi_env: str,
        days: int,
        now: datetime,
        window: OvernightWindow,
        limit: int | None,
        analysis_mode: str,
    ) -> dict[str, Any]:
        if analysis_mode not in {"fast", "detailed"}:
            raise ValueError("weather_analysis_mode must be fast or detailed")

        rows: list[dict[str, Any]] = []
        if analysis_mode == "detailed":
            dataset = await self.trade_analysis_service.build_dataset(
                kalshi_env=kalshi_env,
                days=days,
                now=now,
                limit=limit,
            )
            rows = [
                {**row, "overnight": window.local_fields(row.get("decision_ts"))}
                for row in getattr(dataset, "rows", [])
                if window.contains(row.get("decision_ts"))
            ]
            evidence = _weather_detailed_evidence(rows, days=days)
        else:
            evidence = await self._weather_fast_evidence(kalshi_env=kalshi_env, days=days, now=now, window=window)
            rows = [{}] * int(evidence["row_count"])
        audit = None
        if analysis_mode == "detailed" and self.trading_audit_service is not None:
            audit = await self.trading_audit_service.build_report(kalshi_env=kalshi_env, days=days, focus="money-safety", now=now)

        hard_blockers: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        if not rows:
            hard_blockers.append(_issue("weather", "weather_insufficient_overnight_evidence", "No weather decision rows were found inside the overnight window."))
        elif len(rows) < THIN_SAMPLE_WARNING_ROWS:
            warnings.append(
                _issue(
                    "weather",
                    "weather_thin_overnight_sample",
                    f"Only {len(rows)} weather decision rows were found inside the overnight window.",
                    severity="warn",
                )
            )
        if _normalize_env(kalshi_env) == "production" and self.settings.trade_behavior_production_entry_freeze_enabled:
            hard_blockers.append(
                _issue(
                    "weather",
                    "trade_behavior_production_entry_freeze_enabled",
                    f"Weather production entries are frozen: {self.settings.trade_behavior_entry_freeze_reason}.",
                )
            )
        if not self.settings.trigger_enable_auto_rooms:
            hard_blockers.append(_issue("weather", "weather_auto_rooms_disabled", "Weather auto-room triggering is disabled."))

        pnl = evidence.get("pnl") or {"scored_count": 0, "net_pnl_dollars": None, "avg_pnl_dollars": None}
        if rows and analysis_mode == "detailed" and pnl["scored_count"] == 0:
            warnings.append(_issue("weather", "weather_missing_settlement_pnl_coverage", "No overnight weather rows have scored P&L.", severity="warn"))
        if rows and analysis_mode == "fast":
            warnings.append(
                _issue(
                    "weather",
                    "weather_fast_summary_without_pnl",
                    "Fast weather readiness used indexed room/ticket/fill counts; run detailed mode for P&L/exclusion diagnostics.",
                    severity="warn",
                )
            )

        evidence = {**evidence, "audit": _weather_audit_summary(audit)}
        hard_blockers = _unique_issues(hard_blockers)
        warnings = _unique_issues(warnings)
        return {
            "status": _status(hard_blockers, warnings),
            "ready_for_live": not hard_blockers,
            "hard_blockers": hard_blockers,
            "warnings": warnings,
            "evidence": evidence,
        }

    async def _weather_fast_evidence(
        self,
        *,
        kalshi_env: str,
        days: int,
        now: datetime,
        window: OvernightWindow,
    ) -> dict[str, Any]:
        cutoff = now - timedelta(days=days)
        async with self.session_factory() as session:
            room_rows = list(
                (
                    await session.execute(
                        select(
                            Room.id,
                            Room.market_ticker,
                            Room.created_at,
                            Room.shadow_mode,
                            Room.room_origin,
                            Room.stage,
                        )
                        .where(Room.kalshi_env == kalshi_env, Room.created_at >= cutoff)
                        .order_by(Room.created_at.desc(), Room.id.desc())
                    )
                ).all()
            )

            room_rows = [
                row
                for row in room_rows
                if _weather_ticker(row.market_ticker) and window.contains(row.created_at)
            ]
            room_ids = [row.id for row in room_rows]
            signals_by_room: dict[str, Signal] = {}
            tickets_by_room: dict[str, TradeTicketRecord] = {}
            risks_by_ticket: dict[str, RiskVerdictRecord] = {}
            orders_by_ticket: dict[str, list[OrderRecord]] = {}
            fills_by_order: dict[str, list[FillRecord]] = {}

            for batch in _batches(room_ids):
                signals = list(
                    (
                        await session.execute(
                            select(Signal).where(Signal.room_id.in_(batch)).order_by(Signal.created_at.asc(), Signal.id.asc())
                        )
                    ).scalars()
                )
                for signal in signals:
                    signals_by_room[signal.room_id] = signal
                tickets = list(
                    (
                        await session.execute(
                            select(TradeTicketRecord)
                            .where(TradeTicketRecord.room_id.in_(batch))
                            .order_by(TradeTicketRecord.created_at.asc(), TradeTicketRecord.id.asc())
                        )
                    ).scalars()
                )
                for ticket in tickets:
                    tickets_by_room[ticket.room_id] = ticket

            ticket_ids = [ticket.id for ticket in tickets_by_room.values()]
            for batch in _batches(ticket_ids):
                risks = list(
                    (
                        await session.execute(
                            select(RiskVerdictRecord)
                            .where(RiskVerdictRecord.ticket_id.in_(batch))
                            .order_by(RiskVerdictRecord.created_at.asc(), RiskVerdictRecord.id.asc())
                        )
                    ).scalars()
                )
                for risk in risks:
                    risks_by_ticket[risk.ticket_id] = risk
                orders = list(
                    (
                        await session.execute(
                            select(OrderRecord)
                            .where(OrderRecord.trade_ticket_id.in_(batch))
                            .order_by(OrderRecord.created_at.asc(), OrderRecord.id.asc())
                        )
                    ).scalars()
                )
                for order in orders:
                    if order.trade_ticket_id:
                        orders_by_ticket.setdefault(str(order.trade_ticket_id), []).append(order)

            order_ids = [order.id for orders in orders_by_ticket.values() for order in orders]
            for batch in _batches(order_ids):
                fills = list((await session.execute(select(FillRecord).where(FillRecord.order_id.in_(batch)))).scalars())
                for fill in fills:
                    if fill.order_id:
                        fills_by_order.setdefault(str(fill.order_id), []).append(fill)

        decision_counts: Counter[str] = Counter()
        risk_counts: Counter[str] = Counter()
        order_counts: Counter[str] = Counter()
        by_series: Counter[str] = Counter()
        for row in room_rows:
            ticket = tickets_by_room.get(row.id)
            risk = risks_by_ticket.get(ticket.id) if ticket is not None else None
            orders = orders_by_ticket.get(ticket.id, []) if ticket is not None else []
            fills = [fill for order in orders for fill in fills_by_order.get(order.id, [])]
            decision_counts[_weather_fast_decision_status(ticket=ticket, risk=risk, orders=orders, fills=fills, has_signal=row.id in signals_by_room)] += 1
            if risk is not None:
                risk_counts[str(risk.status or "<unknown>")] += 1
            for order in orders:
                order_counts[str(order.status or "<unknown>")] += 1
            by_series[_series_from_ticker(row.market_ticker)] += 1

        return {
            "analysis_mode": "fast",
            "window_days": days,
            "row_count": len(room_rows),
            "training_eligible_count": None,
            "excluded_count": None,
            "by_decision_status": dict(decision_counts),
            "by_series": dict(by_series.most_common(20)),
            "by_room_origin": dict(Counter(str(row.room_origin or "<unknown>") for row in room_rows)),
            "by_stage": dict(Counter(str(row.stage or "<unknown>") for row in room_rows)),
            "shadow_mode_counts": dict(Counter("shadow" if row.shadow_mode else "live" for row in room_rows)),
            "signal_count": len(signals_by_room),
            "ticket_count": len(tickets_by_room),
            "risk_status_counts": dict(risk_counts),
            "order_status_counts": dict(order_counts),
            "fill_count": sum(len(fills) for fills in fills_by_order.values()),
            "top_exclusion_reasons": [],
            "pnl": {"scored_count": 0, "net_pnl_dollars": None, "avg_pnl_dollars": None},
            "snapshot_source_kind_counts": {},
        }

    async def _crypto_readiness(
        self,
        *,
        kalshi_env: str,
        days: int,
        now: datetime,
        window: OvernightWindow,
        frequency: str,
    ) -> dict[str, Any]:
        freq = normalize_frequency(frequency) or "15m"
        cutoff = now - timedelta(days=days)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=kalshi_env)
            control = await session.get(DeploymentControl, kalshi_env)
            snapshots = await repo.list_crypto_market_snapshots(
                frequency=freq,
                kalshi_env=kalshi_env,
                since=cutoff,
                limit=100_000,
            )
            candles = await repo.list_crypto_market_candlesticks(
                frequency=freq,
                kalshi_env=kalshi_env,
                since=cutoff,
                limit=200_000,
            )
            spot_rows = await repo.list_crypto_spot_ohlc(
                frequency=freq,
                kalshi_env=kalshi_env,
                since=cutoff,
                limit=500_000,
            )
            model = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="model",
                kalshi_env=kalshi_env,
            )
            backtest = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="backtest",
                kalshi_env=kalshi_env,
            )
            gate = await repo.get_latest_crypto_model_artifact(
                frequency=freq,
                artifact_type="replay_gate",
                kalshi_env=kalshi_env,
            )
            control_for_modes = control or SimpleNamespace(notes={}, active_color="", kill_switch_enabled=False)
            asset_symbols = sorted(
                {
                    row.asset_symbol
                    for row in [*snapshots, *candles, *spot_rows]
                    if getattr(row, "asset_symbol", None)
                }
                | set(self.crypto_asset_control_service.modes_from_notes(getattr(control_for_modes, "notes", None)).keys())
            )
            mode_summary = self.crypto_asset_control_service.asset_mode_summary(
                asset_symbols=asset_symbols,
                modes=self.crypto_asset_control_service.modes_from_notes(getattr(control_for_modes, "notes", None)),
            )
            live_assets = [
                normalize_asset_symbol(asset)
                for asset, mode in mode_summary["modes"].items()
                if mode == CRYPTO_ASSET_MODE_LIVE
            ]
            model_by_live_asset = {
                asset: await repo.get_latest_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=_crypto_artifact_type("model", [asset]),
                    kalshi_env=kalshi_env,
                )
                for asset in live_assets
            }
            backtest_by_live_asset = {
                asset: await repo.get_latest_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=_crypto_artifact_type("backtest", [asset]),
                    kalshi_env=kalshi_env,
                )
                for asset in live_assets
            }
            gate_by_live_asset = {
                asset: await repo.get_latest_crypto_model_artifact(
                    frequency=freq,
                    artifact_type=_crypto_artifact_type("replay_gate", [asset]),
                    kalshi_env=kalshi_env,
                )
                for asset in live_assets
            }

        expected_assets_set = set(asset_symbols) | set(COINBASE_PRODUCT_IDS)
        if self.settings.crypto_spot_proxy_fallback_enabled:
            expected_assets_set.update(COINGECKO_IDS)
        expected_assets = sorted(expected_assets_set)
        data_quality = _crypto_data_quality(
            snapshots,
            candles,
            min_training_samples=self.settings.crypto_min_training_samples,
        )
        spot_quality = _crypto_spot_quality(
            spot_rows,
            expected_assets=expected_assets,
            min_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
            settings=self.settings,
        )
        overnight_snapshots = [row for row in snapshots if window.contains(row.observed_at)]
        overnight_candles = [row for row in candles if window.contains(row.end_period_ts)]
        overnight_spot_rows = [row for row in spot_rows if window.contains(row.end_ts)]
        decision_rows = _crypto_decision_rows(snapshots, candles, spot_rows)
        overnight_decision_rows = [
            {**row, "overnight": window.local_fields(row.get("decision_ts"))}
            for row in decision_rows
            if window.contains(row.get("decision_ts"))
        ]
        candidate_model_artifact = model
        if candidate_model_artifact is None and len(live_assets) == 1:
            candidate_model_artifact = model_by_live_asset.get(live_assets[0])
        model_payload = dict(candidate_model_artifact.payload or {}) if candidate_model_artifact is not None else None
        candidate_quality = _crypto_candidate_quality_report(
            overnight_decision_rows,
            model_payload,
            settings=self.settings,
        )

        scoped_gate = gate_by_live_asset.get(live_assets[0]) if len(live_assets) == 1 else gate
        global_live_blockers = self.crypto_asset_control_service.global_live_blockers(
            control=control_for_modes,
            replay_gate=scoped_gate,
            has_write_credentials=self._has_write_credentials(kalshi_env=kalshi_env),
            frequency=freq,
        )
        hard_blockers: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        if not self.settings.crypto_enabled:
            hard_blockers.append(_issue("crypto", "crypto_disabled", "Crypto is disabled."))
        if freq == "15m" and not self.settings.crypto_15m_enabled:
            hard_blockers.append(_issue("crypto", "crypto_15m_disabled", "15-minute crypto is disabled."))
        if freq == "1h" and not self.settings.crypto_1h_enabled:
            hard_blockers.append(_issue("crypto", "crypto_1h_disabled", "1-hour crypto is disabled."))
        if not self.settings.crypto_trading_enabled:
            hard_blockers.append(_issue("crypto", "crypto_trading_disabled", "Global crypto trading is disabled."))
        if not overnight_decision_rows:
            hard_blockers.append(_issue("crypto", "crypto_insufficient_overnight_evidence", "No crypto decision rows were found inside the overnight window."))
        elif len(overnight_decision_rows) < THIN_SAMPLE_WARNING_ROWS:
            warnings.append(
                _issue(
                    "crypto",
                    "crypto_thin_overnight_sample",
                    f"Only {len(overnight_decision_rows)} crypto decision rows were found inside the overnight window.",
                    severity="warn",
                )
            )
        if _normalize_env(kalshi_env) == "production" and not self.settings.crypto_production_autonomy_enabled:
            warnings.append(
                _issue(
                    "crypto",
                    "crypto_production_autonomy_not_supported",
                    "Crypto production autonomy requires CRYPTO_PRODUCTION_AUTONOMY_ENABLED=true.",
                    severity="warn",
                )
            )
        if not live_assets:
            hard_blockers.append(_issue("crypto", "crypto_no_live_assets", "No crypto assets are explicitly live."))
        for asset in live_assets:
            asset_model = model_by_live_asset.get(asset)
            asset_backtest = backtest_by_live_asset.get(asset)
            asset_gate = gate_by_live_asset.get(asset)
            if asset_model is None:
                hard_blockers.append(_issue("crypto", f"crypto_model_missing_{asset.lower()}", f"Crypto model artifact is missing for live asset {asset}."))
            elif asset_model.status != "trained":
                hard_blockers.append(_issue("crypto", f"crypto_model_not_trained_{asset.lower()}", f"Crypto model status for {asset} is {asset_model.status}; expected trained."))
            if asset_backtest is None:
                hard_blockers.append(_issue("crypto", f"crypto_backtest_missing_{asset.lower()}", f"Crypto backtest artifact is missing for live asset {asset}."))
            elif asset_backtest.status != "pass":
                hard_blockers.append(_issue("crypto", f"crypto_backtest_not_passing_{asset.lower()}", f"Crypto backtest status for {asset} is {asset_backtest.status}; expected pass."))
            if asset_gate is None:
                hard_blockers.append(_issue("crypto", f"crypto_replay_gate_missing_{asset.lower()}", f"Crypto replay gate artifact is missing for live asset {asset}."))
            elif asset_gate.status != "passed":
                hard_blockers.append(_issue("crypto", f"crypto_replay_gate_not_passed_{asset.lower()}", f"Crypto replay gate status for {asset} is {asset_gate.status}; expected passed."))
        if model is None:
            warnings.append(_issue("crypto", "crypto_aggregate_model_missing", "Aggregate crypto model artifact is missing; live readiness is scoped to explicit live assets.", severity="warn"))
        elif model.status != "trained":
            warnings.append(_issue("crypto", "crypto_aggregate_model_not_trained", f"Aggregate crypto model status is {model.status}; live readiness is scoped to explicit live assets.", severity="warn"))
        if backtest is None:
            warnings.append(_issue("crypto", "crypto_aggregate_backtest_missing", "Aggregate crypto backtest artifact is missing; live readiness is scoped to explicit live assets.", severity="warn"))
        elif backtest.status != "pass":
            warnings.append(_issue("crypto", "crypto_aggregate_backtest_not_passing", f"Aggregate crypto backtest status is {backtest.status}; live readiness is scoped to explicit live assets.", severity="warn"))
        if gate is None:
            warnings.append(_issue("crypto", "crypto_aggregate_replay_gate_missing", "Aggregate crypto replay gate artifact is missing; live readiness is scoped to explicit live assets.", severity="warn"))
        elif gate.status != "passed":
            warnings.append(_issue("crypto", "crypto_aggregate_replay_gate_not_passed", f"Aggregate crypto replay gate status is {gate.status}; live readiness is scoped to explicit live assets.", severity="warn"))
        candidate_by_asset = candidate_quality.get("by_asset") if isinstance(candidate_quality.get("by_asset"), dict) else {}
        for asset in live_assets:
            live_quality_count = int((candidate_by_asset.get(asset) or {}).get("live_quality_candidate_count") or 0)
            if live_quality_count <= 0:
                hard_blockers.append(_issue("crypto", f"crypto_no_live_quality_overnight_candidates_{asset.lower()}", f"No overnight {asset} candidates meet {CRYPTO_LIVE_QUALITY}."))
        for asset, mode in mode_summary["modes"].items():
            if mode != CRYPTO_ASSET_MODE_LIVE:
                warnings.append(
                    _issue(
                        "crypto",
                        f"crypto_asset_mode_not_live_{asset.lower()}",
                        f"Asset {asset} mode is {mode}; set it to live to allow live orders.",
                        severity="warn",
                        details={"asset_symbol": asset, "mode": mode},
                    )
                )
        data_assets = data_quality.get("assets") if isinstance(data_quality.get("assets"), dict) else {}
        spot_assets = spot_quality.get("assets") if isinstance(spot_quality.get("assets"), dict) else {}
        stale_spot_assets = set(spot_quality.get("stale_assets") or [])
        missing_spot_assets = set(spot_quality.get("missing_assets") or [])
        for asset in live_assets:
            data_summary = data_assets.get(asset) or {}
            if not data_summary or int(data_summary.get("snapshot_count") or 0) <= 0 or int(data_summary.get("candle_count") or 0) <= 0:
                hard_blockers.append(_issue("crypto", f"crypto_data_quality_not_ready_{asset.lower()}", f"Crypto market data quality is not ready for live asset {asset}."))
            spot_summary = spot_assets.get(asset) or {}
            if asset in missing_spot_assets or asset in stale_spot_assets or bool(spot_summary.get("proxy_only")):
                hard_blockers.append(_issue("crypto", f"crypto_spot_quality_not_ready_{asset.lower()}", f"Crypto spot data quality is not ready for live asset {asset}."))
        if data_quality.get("status") != "ready":
            warnings.append(_issue("crypto", "crypto_data_quality_not_ready", "Aggregate crypto market data quality is not ready.", severity="warn"))
        if spot_quality.get("status") != "ready":
            warnings.append(_issue("crypto", "crypto_spot_quality_not_ready", "Aggregate crypto spot data quality is not ready.", severity="warn"))

        hard_blockers = _unique_issues(hard_blockers)
        warnings = _unique_issues(warnings)
        return {
            "status": _status(hard_blockers, warnings),
            "ready_for_live": not hard_blockers,
            "frequency": freq,
            "hard_blockers": hard_blockers,
            "warnings": warnings,
            "evidence": {
                "window_days": days,
                "asset_symbols": asset_symbols,
                "live_asset_symbols": live_assets,
                "asset_modes": mode_summary["modes"],
                "asset_mode_counts": mode_summary["counts"],
                "crypto_production_autonomy_enabled": self.settings.crypto_production_autonomy_enabled,
                "global_live_blockers": global_live_blockers,
                "stored_snapshot_count": len(snapshots),
                "stored_candle_count": len(candles),
                "spot_row_count": len(spot_rows),
                "overnight_snapshot_count": len(overnight_snapshots),
                "overnight_candle_count": len(overnight_candles),
                "overnight_spot_row_count": len(overnight_spot_rows),
                "overnight_decision_row_count": len(overnight_decision_rows),
                "overnight_decision_rows_by_asset": dict(Counter(str(row.get("asset_symbol") or "<unknown>") for row in overnight_decision_rows)),
                "model": _artifact_summary(model),
                "backtest": _artifact_summary(backtest),
                "replay_gate": _artifact_summary(gate),
                "models_by_live_asset": {asset: _artifact_summary(artifact) for asset, artifact in model_by_live_asset.items()},
                "backtests_by_live_asset": {asset: _artifact_summary(artifact) for asset, artifact in backtest_by_live_asset.items()},
                "replay_gates_by_live_asset": {asset: _artifact_summary(artifact) for asset, artifact in gate_by_live_asset.items()},
                "data_quality": data_quality,
                "spot_quality": spot_quality,
                "candidate_quality": candidate_quality,
            },
        }

    def _has_write_credentials(self, *, kalshi_env: str) -> bool:
        if self.has_write_credentials is not None and _normalize_env(kalshi_env) == _normalize_env(self.settings.kalshi_env):
            return self.has_write_credentials
        if _normalize_env(kalshi_env) == "production":
            key_id = self.settings.kalshi_write_api_key_id or self.settings.live_kalshi_api_key
            key_path = self.settings.kalshi_write_private_key_path or self.settings.live_kalshi_write_private_key_path or self.settings.live_kalshi_read_private_key_path
        else:
            key_id = self.settings.kalshi_write_api_key_id or self.settings.demo_kalshi_api_key
            key_path = self.settings.kalshi_write_private_key_path or self.settings.demo_kalshi_write_private_key_path or self.settings.demo_kalshi_read_private_key_path
        return bool(key_id and key_path)

    @staticmethod
    def _operator_actions(hard_blockers: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> list[str]:
        del warnings
        actions: list[str] = []
        codes = {issue["code"] for issue in hard_blockers}
        if "app_shadow_mode" in codes:
            actions.append("Disable APP_SHADOW_MODE only after the report otherwise passes.")
        if "settings_env_mismatch" in codes:
            actions.append("Run the report from a process configured for the target KALSHI_ENV.")
        if "write_credentials_missing" in codes:
            actions.append("Configure target-environment Kalshi write credentials.")
        if "weather_auto_rooms_disabled" in codes:
            actions.append("Enable TRIGGER_ENABLE_AUTO_ROOMS for weather automation after validation.")
        if "trade_behavior_production_entry_freeze_enabled" in codes:
            actions.append("Lift the trade behavior production entry freeze only after retraining checks pass.")
        if "crypto_production_autonomy_not_supported" in codes:
            actions.append("Add or approve a production-safe crypto autonomy path before relying on overnight crypto automation.")
        if any(str(code).startswith("crypto_asset_mode_not_live_") for code in codes):
            actions.append("Promote intended crypto assets to live mode with crypto-asset-mode after shadow evidence is sufficient.")
        if any("insufficient_overnight_evidence" in str(code) for code in codes):
            actions.append("Collect more overnight shadow/demo evidence before live overnight trading.")
        return actions


def format_overnight_readiness_report(report: dict[str, Any]) -> str:
    lines = [
        "Overnight Trading Readiness",
        f"env={report['kalshi_env']} status={report['status']} ready_for_live={report['ready_for_live']}",
        (
            f"window={report['window']['start_hour']:02d}:00-"
            f"{report['window']['end_hour']:02d}:00 {report['window']['timezone']}"
        ),
        "",
    ]
    if report.get("hard_blockers"):
        lines.append("Hard blockers:")
        for issue in report["hard_blockers"][:20]:
            lines.append(f"- {issue['domain']}:{issue['code']} - {issue['summary']}")
    else:
        lines.append("Hard blockers: none")
    if report.get("warnings"):
        lines.extend(["", "Warnings:"])
        for issue in report["warnings"][:20]:
            lines.append(f"- {issue['domain']}:{issue['code']} - {issue['summary']}")
    for domain, domain_report in sorted((report.get("domains") or {}).items()):
        evidence = domain_report.get("evidence") or {}
        lines.extend(
            [
                "",
                f"{domain.title()} evidence:",
                f"- status={domain_report.get('status')} ready_for_live={domain_report.get('ready_for_live')}",
            ]
        )
        if domain == "weather":
            lines.append(
                f"- rows={evidence.get('row_count', 0)} eligible={evidence.get('training_eligible_count', 0)} "
                f"pnl_rows={(evidence.get('pnl') or {}).get('scored_count', 0)}"
            )
        if domain == "crypto":
            lines.append(
                f"- assets={','.join(evidence.get('asset_symbols') or []) or '<none>'} "
                f"overnight_decision_rows={evidence.get('overnight_decision_row_count', 0)}"
            )
    if report.get("operator_actions"):
        lines.extend(["", "Operator actions:"])
        for action in report["operator_actions"]:
            lines.append(f"- {action}")
    return "\n".join(lines)


async def _get_checkpoint(session: AsyncSession, stream_name: str) -> Checkpoint | None:
    return (
        await session.execute(
            select(Checkpoint)
            .where(Checkpoint.stream_name == stream_name)
            .order_by(Checkpoint.updated_at.desc(), Checkpoint.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


def _checkpoint_payload(checkpoint: Checkpoint | None, timestamp_key: str, now: datetime) -> dict[str, Any]:
    if checkpoint is None:
        return {"present": False, timestamp_key: None, "age_seconds": None}
    payload = checkpoint.payload if isinstance(checkpoint.payload, dict) else {}
    ts = _as_utc(payload.get(timestamp_key))
    return {
        "present": True,
        timestamp_key: ts.isoformat() if ts else None,
        "age_seconds": int((now - ts).total_seconds()) if ts else None,
        "payload": payload,
    }


def _post_clear_reconcile_blocker(control: DeploymentControl, reconcile: Checkpoint | None) -> str | None:
    cleared_at = _as_utc((control.notes or {}).get("kill_switch_cleared_at"))
    if cleared_at is None:
        return None
    if reconcile is None:
        return "Kill switch was recently cleared; waiting for first reconcile before executing."
    payload = reconcile.payload if isinstance(reconcile.payload, dict) else {}
    reconciled_at = _as_utc(payload.get("reconciled_at"))
    if reconciled_at is None:
        return "Kill switch was recently cleared; waiting for reconcile checkpoint to carry reconciled_at."
    if reconciled_at < cleared_at:
        return (
            f"Kill switch cleared at {cleared_at.isoformat()}; last reconcile was at "
            f"{reconciled_at.isoformat()}; waiting for a post-clear reconcile before executing."
        )
    return None


def _weather_audit_summary(audit: dict[str, Any] | None) -> dict[str, Any]:
    if audit is None:
        return {"available": False}
    return {
        "available": True,
        "counts": audit.get("counts") or {},
        "execution_funnel": audit.get("execution_funnel") or {},
        "signal_funnel": audit.get("signal_funnel") or {},
        "risk": audit.get("risk") or {},
        "issues": (audit.get("issues") or [])[:20],
    }


def _weather_detailed_evidence(rows: list[dict[str, Any]], *, days: int) -> dict[str, Any]:
    return {
        "analysis_mode": "detailed",
        "window_days": days,
        "row_count": len(rows),
        "training_eligible_count": sum(1 for row in rows if row.get("training_eligible")),
        "excluded_count": sum(1 for row in rows if not row.get("training_eligible")),
        "by_decision_status": dict(Counter(str(row.get("decision_status") or "<unknown>") for row in rows)),
        "by_series": dict(Counter(str(row.get("series_ticker") or "<unknown>") for row in rows).most_common(20)),
        "top_exclusion_reasons": Counter(
            reason
            for row in rows
            for reason in row.get("exclusion_reasons", [])
        ).most_common(20),
        "pnl": _pnl_summary(rows),
        "snapshot_source_kind_counts": dict(Counter(str(row.get("market_snapshot_source_kind") or "<missing>") for row in rows)),
    }


def _weather_fast_decision_status(
    *,
    ticket: TradeTicketRecord | None,
    risk: RiskVerdictRecord | None,
    orders: list[OrderRecord],
    fills: list[FillRecord],
    has_signal: bool,
) -> str:
    if fills:
        return "filled"
    if orders:
        failed = {"failed", "rejected", "rejected_503", "order_id_missing", "lock_denied", "write_credentials_missing"}
        if any(str(order.status).lower() in failed for order in orders):
            return "order_failed"
        return "ordered_unfilled"
    if risk is not None and risk.status == "approved":
        return "approved_no_order"
    if risk is not None:
        return f"risk_{risk.status}"
    if ticket is not None:
        return f"ticket_{ticket.status}"
    if has_signal:
        return "signal_only"
    return "room_only"


def _weather_ticker(market_ticker: str | None) -> bool:
    ticker = str(market_ticker or "").upper()
    return not any(ticker.startswith(prefix) for prefix in CRYPTO_MARKET_PREFIXES)


def _series_from_ticker(market_ticker: str | None) -> str:
    ticker = str(market_ticker or "").upper()
    return ticker.split("-", 1)[0] if ticker else "<unknown>"


def _batches(values: list[str], size: int = 5000) -> list[list[str]]:
    return [values[idx:idx + size] for idx in range(0, len(values), size)]


def _artifact_summary(artifact: Any | None) -> dict[str, Any]:
    if artifact is None:
        return {"status": "missing"}
    return {
        "status": artifact.status,
        "version": artifact.version,
        "sample_count": artifact.sample_count,
        "trained_at": artifact.trained_at.isoformat() if artifact.trained_at is not None else None,
        "metrics": artifact.metrics or {},
        "payload": artifact.payload or {},
    }


def _pnl_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [
        Decimal(str(value))
        for row in rows
        if (value := row.get("lifecycle_net_pnl_dollars") or row.get("gross_pnl_dollars")) not in (None, "")
    ]
    total = sum(values, Decimal("0"))
    return {
        "scored_count": len(values),
        "net_pnl_dollars": str(total.quantize(Decimal("0.0001"))),
        "avg_pnl_dollars": str((total / Decimal(len(values))).quantize(Decimal("0.0001"))) if values else None,
    }


def _issue(
    domain: str,
    code: str,
    summary: str,
    *,
    severity: str = "critical",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {"domain": domain, "severity": severity, "code": code, "summary": summary}
    if details:
        payload["details"] = details
    return payload


def _unique_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for issue in issues:
        key = (str(issue.get("domain")), str(issue.get("code")))
        if key in seen:
            continue
        seen.add(key)
        unique.append(issue)
    return unique


def _status(hard_blockers: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> str:
    if hard_blockers:
        return "fail"
    if warnings:
        return "warn"
    return "pass"


def _domain_set(domains: str) -> set[str]:
    normalized = str(domains or "all").strip().lower()
    if normalized == "all":
        return {"weather", "crypto"}
    if normalized in {"weather", "crypto"}:
        return {normalized}
    raise ValueError("domains must be one of: weather, crypto, all")


def _normalize_env(value: str | None) -> str:
    env = str(value or "").strip().lower()
    if env in {"prod", "live"}:
        return "production"
    return env or "demo"


def _as_utc(value: Any | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value
