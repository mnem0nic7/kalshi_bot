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
    _crypto_candidate_quality_report,
    _crypto_data_quality,
    _crypto_decision_rows,
    _crypto_spot_quality,
)
from kalshi_bot.db.models import Checkpoint, DeploymentControl
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.crypto_spot import COINGECKO_IDS, COINBASE_PRODUCT_IDS


SCHEMA_VERSION = "overnight-readiness-v1"
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_START_HOUR = 22
DEFAULT_END_HOUR = 6
THIN_SAMPLE_WARNING_ROWS = 20


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
            "ready_for_live": status == "pass",
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
    ) -> dict[str, Any]:
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
        audit = None
        if self.trading_audit_service is not None:
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

        pnl = _pnl_summary(rows)
        if rows and pnl["scored_count"] == 0:
            warnings.append(_issue("weather", "weather_missing_settlement_pnl_coverage", "No overnight weather rows have scored P&L.", severity="warn"))

        evidence = {
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
            "pnl": pnl,
            "snapshot_source_kind_counts": dict(Counter(str(row.get("market_snapshot_source_kind") or "<missing>") for row in rows)),
            "audit": _weather_audit_summary(audit),
        }
        hard_blockers = _unique_issues(hard_blockers)
        warnings = _unique_issues(warnings)
        return {
            "status": _status(hard_blockers, warnings),
            "ready_for_live": not hard_blockers,
            "hard_blockers": hard_blockers,
            "warnings": warnings,
            "evidence": evidence,
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
        expected_assets = sorted(set(asset_symbols) | set(COINBASE_PRODUCT_IDS) | set(COINGECKO_IDS))
        data_quality = _crypto_data_quality(
            snapshots,
            candles,
            min_training_samples=self.settings.crypto_min_training_samples,
        )
        spot_quality = _crypto_spot_quality(
            spot_rows,
            expected_assets=expected_assets,
            min_coverage_pct=self.settings.crypto_replay_min_spot_coverage_pct,
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
        model_payload = dict(model.payload or {}) if model is not None else None
        candidate_quality = _crypto_candidate_quality_report(
            overnight_decision_rows,
            model_payload,
            settings=self.settings,
        )

        global_live_blockers = self.crypto_asset_control_service.global_live_blockers(
            control=control_for_modes,
            replay_gate=gate,
            has_write_credentials=self._has_write_credentials(kalshi_env=kalshi_env),
            frequency=freq,
        )
        hard_blockers: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        if not self.settings.crypto_enabled:
            hard_blockers.append(_issue("crypto", "crypto_disabled", "Crypto is disabled."))
        if freq == "15m" and not self.settings.crypto_15m_enabled:
            hard_blockers.append(_issue("crypto", "crypto_15m_disabled", "15-minute crypto is disabled."))
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
        if _normalize_env(kalshi_env) == "production":
            hard_blockers.append(
                _issue(
                    "crypto",
                    "crypto_production_autonomy_not_supported",
                    "CryptoAutonomyService is currently demo-only for autonomous overnight trading.",
                )
            )
        if model is None:
            hard_blockers.append(_issue("crypto", "crypto_model_missing", "Crypto model artifact is missing."))
        elif model.status != "trained":
            hard_blockers.append(_issue("crypto", "crypto_model_not_trained", f"Crypto model status is {model.status}; expected trained."))
        if backtest is None:
            hard_blockers.append(_issue("crypto", "crypto_backtest_missing", "Crypto backtest artifact is missing."))
        elif backtest.status != "pass":
            hard_blockers.append(_issue("crypto", "crypto_backtest_not_passing", f"Crypto backtest status is {backtest.status}; expected pass."))
        if gate is None:
            hard_blockers.append(_issue("crypto", "crypto_replay_gate_missing", "Crypto replay gate artifact is missing."))
        elif gate.status != "passed":
            hard_blockers.append(_issue("crypto", "crypto_replay_gate_not_passed", f"Crypto replay gate status is {gate.status}; expected passed."))
        live_quality_count = int((candidate_quality.get("live_quality_policy") or {}).get("selected_count") or 0)
        if live_quality_count <= 0:
            hard_blockers.append(_issue("crypto", "crypto_no_live_quality_overnight_candidates", f"No overnight candidates meet {CRYPTO_LIVE_QUALITY}."))
        for asset, mode in mode_summary["modes"].items():
            if mode != CRYPTO_ASSET_MODE_LIVE:
                hard_blockers.append(
                    _issue(
                        "crypto",
                        f"crypto_asset_mode_not_live_{asset.lower()}",
                        f"Asset {asset} mode is {mode}; set it to live to allow live orders.",
                        details={"asset_symbol": asset, "mode": mode},
                    )
                )
        if data_quality.get("status") != "ready":
            warnings.append(_issue("crypto", "crypto_data_quality_not_ready", "Crypto market data quality is not ready.", severity="warn"))
        if spot_quality.get("status") != "ready":
            warnings.append(_issue("crypto", "crypto_spot_quality_not_ready", "Crypto spot data quality is not ready.", severity="warn"))

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
                "asset_modes": mode_summary["modes"],
                "asset_mode_counts": mode_summary["counts"],
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
