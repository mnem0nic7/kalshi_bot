from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.core.auto_evolve_safety import delta_cap_error, delta_to_bps, rate_to_bps
from kalshi_bot.services.strategy_regression_ranking import RANKING_VERSION as STRATEGY_RANKING_VERSION

logger = logging.getLogger(__name__)

AUTO_EVOLVE_SOURCE = "strategy_auto_evolve"
AUTO_EVOLVE_EVENT_KIND = "auto_evolve"
AUTO_EVOLVE_ASSIGNED_BY = "auto_evolve"
MANUAL_INSUFFICIENT_DATA_ROLLBACK_TRIGGER = "manual_insufficient_data_review"
MIN_OPERATOR_NOTE_LENGTH = 20

# Schema-defined upper bounds used as the delta-cap reference when current value is zero.
# Ratios are bounded by the schema [0, 1]. Bps/seconds fields have no declared max, so we
# use practical ceilings that represent the outer edge of any sane configuration.
_THRESHOLD_FIELD_CEILING: dict[str, float] = {
    "risk_min_edge_bps": 10000.0,
    "risk_max_order_notional_dollars": 0.0,       # validates > 0; can never be zero
    "risk_max_position_notional_dollars": 0.0,    # validates > 0; can never be zero
    "trigger_max_spread_bps": 10000.0,
    "trigger_cooldown_seconds": 86400.0,
    "strategy_quality_edge_buffer_bps": 10000.0,
    "strategy_min_remaining_payout_bps": 0.0,     # validates > 0; can never be zero
    "risk_safe_capital_reserve_ratio": 1.0,
    "risk_risky_capital_max_ratio": 1.0,
}


class StrategyAutoEvolveService:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        strategy_regression_service: Any,
        strategy_codex_service: Any,
        strategy_dashboard_service: Any,
        trading_audit_service: Any | None = None,
        trade_analysis_service: Any | None = None,
        secondary_session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.secondary_session_factory = secondary_session_factory
        self.strategy_regression_service = strategy_regression_service
        self.strategy_codex_service = strategy_codex_service
        self.strategy_dashboard_service = strategy_dashboard_service
        self.trading_audit_service = trading_audit_service
        self.trade_analysis_service = trade_analysis_service

    @property
    def checkpoint_name(self) -> str:
        return f"daemon_strategy_auto_evolve:{self.settings.kalshi_env}"

    async def dashboard_payload(self) -> dict[str, Any]:
        checkpoint = await self._get_checkpoint_payload()
        return {
            "enabled": bool(self.settings.strategy_auto_evolve_enabled),
            "mode": "auto_evolve",
            "window_days": self.settings.strategy_auto_evolve_window_days,
            "accept_suggestions": bool(self.settings.strategy_auto_evolve_accept_suggestions),
            "activate_suggestions": bool(self.settings.strategy_auto_evolve_activate_suggestions),
            "assign_eligible": bool(self.settings.strategy_auto_evolve_assign_eligible),
            "checkpoint": self.checkpoint_name,
            "last_run": checkpoint,
            "last_status": checkpoint.get("status") if checkpoint else None,
            "last_ran_at": checkpoint.get("ran_at") if checkpoint else None,
            "accepted_strategy": checkpoint.get("accepted_strategy") if checkpoint else None,
            "activated_strategy": checkpoint.get("activated_strategy") if checkpoint else None,
            "assignment_changes": checkpoint.get("assignment_changes", []) if checkpoint else [],
            "assignment_change_count": len(checkpoint.get("assignment_changes", [])) if checkpoint else 0,
            "provider": checkpoint.get("provider") if checkpoint else None,
            "model": checkpoint.get("model") if checkpoint else None,
        }

    async def run_once(self, *, trigger_source: str = "manual") -> dict[str, Any]:
        try:
            return await self._run_once(trigger_source=trigger_source)
        except Exception as exc:
            logger.warning("strategy auto-evolve failed", exc_info=True)
            payload = self._base_payload(trigger_source=trigger_source)
            payload.update({"status": "failed", "reason": "unhandled_error", "error": str(exc)})
            await self._record_result(payload, severity="warning", summary="Strategy Auto-Evolve failed")
            return payload

    async def run_promotion_watchdog_once(self, *, trigger_source: str = "nightly") -> dict[str, Any]:
        now = datetime.now(UTC)
        processed: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            promotions = await repo.list_strategy_promotions_due_for_watchdog(
                now=now,
                kalshi_env=self.settings.kalshi_env,
            )
            for promotion in promotions:
                try:
                    processed.append(await self._evaluate_promotion_watchdog(repo, promotion, now=now))
                except asyncio.CancelledError:
                    raise
                except ValueError as exc:
                    processed.append(
                        await self._record_watchdog_evaluation_failure(
                            repo,
                            promotion,
                            now=now,
                            reason="invalid_watchdog_metrics",
                            exc=exc,
                        )
                    )
                except Exception as exc:
                    processed.append(
                        await self._record_watchdog_evaluation_failure(
                            repo,
                            promotion,
                            now=now,
                            reason="evaluation_error",
                            exc=exc,
                        )
                    )
            await session.commit()
        secondary_retry_sweep: dict[str, Any] | None = None
        if self.secondary_session_factory is not None:
            secondary_retry_sweep = await self.sweep_secondary_strategy_promotion_syncs(
                trigger_source=trigger_source,
            )
        return {
            "status": "completed",
            "mode": "strategy_promotion_watchdog",
            "trigger_source": trigger_source,
            "kalshi_env": self.settings.kalshi_env,
            "evaluated_at": now.isoformat(),
            "due_count": len(processed),
            "processed": processed,
            "secondary_sync_sweep": secondary_retry_sweep,
            "secondary_rollback_syncs": (secondary_retry_sweep or {}).get("rollback_syncs", []),
        }

    async def evaluate_strategy_promotion(
        self,
        promotion_id: int,
        *,
        trigger_source: str = "manual",
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            promotion = await repo.get_strategy_promotion(promotion_id)
            if promotion is None:
                raise KeyError(f"Strategy promotion {promotion_id} not found")
            result = await self._evaluate_promotion_watchdog(repo, promotion, now=now)
            await session.commit()

        if result.get("status") == "rolled_back" and self.secondary_session_factory is not None:
            result["secondary_rollback_sync"] = await self._sync_secondary_rollback(promotion_id)
        result["trigger_source"] = trigger_source
        return result

    async def resolve_strategy_promotion_insufficient_data(
        self,
        promotion_id: int,
        *,
        action: str,
        resolved_by: str,
        note: str,
    ) -> dict[str, Any]:
        resolution = self._operator_resolution_payload(
            action=action,
            resolved_by=resolved_by,
            note=note,
        )
        now = datetime.now(UTC)
        secondary_rollback_sync: dict[str, Any] | None = None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            promotion = await repo.get_strategy_promotion(promotion_id)
            if promotion is None:
                raise KeyError(f"Strategy promotion {promotion_id} not found")
            if promotion.watchdog_status != "insufficient_data":
                raise ValueError("Only insufficient_data strategy promotions can be manually resolved")
            live_snapshot = await self._promotion_live_snapshot(repo, promotion, now=now)
            resolution["live_snapshot"] = live_snapshot
            resolution["live_snapshot_at_resolution"] = live_snapshot
            if action == "approve":
                await repo.update_strategy_promotion(
                    promotion_id,
                    watchdog_status="passed",
                    watchdog_last_eval_at=now,
                    watchdog_last_eval_reason="operator_approved_insufficient_data",
                    resolution_data=resolution,
                )
                await session.commit()
                return {
                    "promotion_id": promotion_id,
                    "action": action,
                    "status": "passed",
                    "reason": "operator_approved_insufficient_data",
                    "resolution": resolution,
                }
            if action != "rollback":
                raise ValueError("action must be approve or rollback")

            rollback = await self._rollback_promotion_batch(
                repo,
                promotion,
                now=now,
                trigger=MANUAL_INSUFFICIENT_DATA_ROLLBACK_TRIGGER,
            )
            await repo.update_strategy_promotion(
                promotion_id,
                watchdog_status="rolled_back",
                rollback_at=now,
                rollback_trigger=MANUAL_INSUFFICIENT_DATA_ROLLBACK_TRIGGER,
                watchdog_last_eval_at=now,
                watchdog_last_eval_reason=f"rolled_back:{MANUAL_INSUFFICIENT_DATA_ROLLBACK_TRIGGER}",
                rollback_details=rollback,
                resolution_data=resolution,
                secondary_rollback_status="pending" if self.secondary_session_factory is not None else "not_applicable",
            )
            await session.commit()

        if self.secondary_session_factory is not None:
            secondary_rollback_sync = await self._sync_secondary_rollback(promotion_id)
        result = {
            "promotion_id": promotion_id,
            "action": action,
            "status": "rolled_back",
            "reason": MANUAL_INSUFFICIENT_DATA_ROLLBACK_TRIGGER,
            "resolution": resolution,
            "rollback": rollback,
        }
        if secondary_rollback_sync is not None:
            result["secondary_rollback_sync"] = secondary_rollback_sync
        return result

    async def resolve_promotion_watchdog_insufficient_data(
        self,
        *,
        promotion_id: int,
        action: str,
        resolved_by: str,
        note: str,
    ) -> dict[str, Any]:
        return await self.resolve_strategy_promotion_insufficient_data(
            promotion_id,
            action=action,
            resolved_by=resolved_by,
            note=note,
        )

    def _operator_resolution_payload(self, *, action: str, resolved_by: str, note: str) -> dict[str, Any]:
        action = action.strip()
        resolved_by = resolved_by.strip()
        note = note.strip()
        if action not in {"approve", "rollback"}:
            raise ValueError("action must be approve or rollback")
        if not resolved_by:
            raise ValueError("resolved_by must be non-empty")
        if len(note) < MIN_OPERATOR_NOTE_LENGTH:
            raise ValueError(f"note must be at least {MIN_OPERATOR_NOTE_LENGTH} characters")
        return {
            "action": action,
            "resolved_by": resolved_by,
            "resolved_at": datetime.now(UTC).isoformat(),
            "note": note,
        }

    async def _promotion_live_snapshot(self, repo: PlatformRepository, promotion: Any, *, now: datetime) -> dict[str, Any]:
        assignments: dict[str, Any] = {}
        for ticker_key in dict(promotion.new_city_assignments or {}):
            ticker = str(ticker_key)
            assignment = await repo.get_city_strategy_assignment(ticker, kalshi_env=self.settings.kalshi_env)
            current_strategy = assignment.strategy_name if assignment is not None else None
            metrics = await repo.get_strategy_city_fill_metrics_since(
                series_ticker=ticker,
                strategy_name=promotion.promoted_strategy_name,
                since=promotion.promoted_at,
                kalshi_env=self.settings.kalshi_env,
            )
            assignments[ticker] = {
                "strategy_name": current_strategy,
                "current_strategy": current_strategy,
                "still_assigned_to_promoted_strategy": current_strategy == promotion.promoted_strategy_name,
                "snapshot_at": now.isoformat(),
                "resolved_live_fills_since_promotion": int(metrics.get("resolved_live_fills") or 0),
                "win_rate_since_promotion": metrics.get("win_rate"),
                "realized_pnl_since_promotion": metrics.get("realized_pnl"),
            }
        return {
            "watchdog_status": promotion.watchdog_status,
            "promoted_strategy_name": promotion.promoted_strategy_name,
            "promoted_at": promotion.promoted_at.isoformat(),
            "snapshot_at": now.isoformat(),
            "assignments": assignments,
        }

    async def _record_watchdog_evaluation_failure(
        self,
        repo: PlatformRepository,
        promotion: Any,
        *,
        now: datetime,
        reason: str,
        exc: Exception,
    ) -> dict[str, Any]:
        detail = str(exc)[:500]
        severity = self._watchdog_failure_severity(now, promotion)
        updates: dict[str, Any] = {
            "watchdog_last_eval_at": now,
            "watchdog_last_eval_reason": reason,
        }
        status = promotion.watchdog_status
        if now >= self._as_utc(promotion.watchdog_extended_due_at):
            status = "insufficient_data"
            updates["watchdog_status"] = status
            updates["watchdog_last_eval_reason"] = f"insufficient_data:{reason}"
            updates["rollback_metrics"] = {"reason": reason, "detail": detail}
        elif promotion.watchdog_status == "pending" and now >= self._as_utc(promotion.watchdog_due_at):
            status = "extended"
            updates["watchdog_status"] = status
            updates["watchdog_extended_reason"] = reason
            updates["watchdog_extended_detail"] = detail
        await repo.update_strategy_promotion(promotion.id, **updates)
        await repo.log_ops_event(
            severity=severity,
            summary="Strategy promotion watchdog evaluation failed",
            source=AUTO_EVOLVE_SOURCE,
            payload={
                "code": "watchdog_evaluation_error",
                "reason": reason,
                "promotion_id": promotion.id,
                "strategy_name": promotion.promoted_strategy_name,
                "exc_type": type(exc).__name__,
                "error": detail,
                "watchdog_due_at": promotion.watchdog_due_at.isoformat(),
                "watchdog_extended_due_at": promotion.watchdog_extended_due_at.isoformat(),
            },
        )
        return {"promotion_id": promotion.id, "status": status, "reason": reason, "error": detail}

    def _watchdog_failure_severity(self, now: datetime, promotion: Any) -> str:
        if now >= self._as_utc(promotion.watchdog_extended_due_at):
            return "critical"
        if now >= self._as_utc(promotion.watchdog_due_at):
            return "high"
        return "warning"

    async def sweep_secondary_strategy_promotion_syncs(
        self,
        *,
        trigger_source: str = "manual",
        limit: int = 50,
    ) -> dict[str, Any]:
        if self.secondary_session_factory is None:
            return {
                "status": "skipped",
                "reason": "secondary_not_configured",
                "mode": "strategy_promotion_secondary_sync_sweep",
                "trigger_source": trigger_source,
            }

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            promotions = await repo.list_strategy_promotions_due_for_secondary_sync(
                kalshi_env=self.settings.kalshi_env,
                limit=limit,
            )
            await session.commit()

        assignment_syncs: list[dict[str, Any]] = []
        rollback_syncs: list[dict[str, Any]] = []
        for promotion in promotions:
            if promotion.secondary_sync_status in {"pending", "failed"}:
                assignment_syncs.append(
                    await self._sync_secondary_assignment(
                        int(promotion.id),
                        trigger_source=trigger_source,
                    )
                )
            if promotion.secondary_rollback_status in {"pending", "failed"}:
                rollback_syncs.append(await self._sync_secondary_rollback(int(promotion.id)))

        return {
            "status": "completed",
            "mode": "strategy_promotion_secondary_sync_sweep",
            "trigger_source": trigger_source,
            "kalshi_env": self.settings.kalshi_env,
            "due_count": len(promotions),
            "assignment_syncs": assignment_syncs,
            "rollback_syncs": rollback_syncs,
        }

    async def _sync_secondary_assignment(self, promotion_id: int, *, trigger_source: str) -> dict[str, Any]:
        if self.secondary_session_factory is None:
            return {"promotion_id": promotion_id, "status": "skipped", "reason": "secondary_not_configured"}
        attempted_at = datetime.now(UTC)
        try:
            async with self.session_factory() as session:
                primary_repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                promotion = await primary_repo.get_strategy_promotion(promotion_id)
                if promotion is None:
                    return {"promotion_id": promotion_id, "status": "missing_promotion"}
                new_assignments = dict(promotion.new_city_assignments or {})
                ready: list[dict[str, Any]] = []
                skipped: list[dict[str, Any]] = []
                for ticker_key, snapshot in new_assignments.items():
                    ticker = str(ticker_key)
                    expected_strategy = self._strategy_name_from_assignment_snapshot(
                        snapshot,
                        fallback=promotion.promoted_strategy_name,
                    )
                    if expected_strategy is None:
                        skipped.append({
                            "series_ticker": ticker,
                            "reason": "missing_promoted_strategy",
                        })
                        continue
                    current = await primary_repo.get_city_strategy_assignment(ticker, kalshi_env=self.settings.kalshi_env)
                    current_strategy = current.strategy_name if current is not None else None
                    if current_strategy != expected_strategy:
                        skipped.append({
                            "series_ticker": ticker,
                            "reason": "primary_assignment_changed",
                            "current_strategy": current_strategy,
                            "expected_strategy": expected_strategy,
                        })
                        continue
                    previous = dict(promotion.previous_city_assignments or {}).get(ticker)
                    previous_strategy = previous.get("strategy_name") if isinstance(previous, dict) else previous
                    if previous_strategy is None and isinstance(snapshot, dict):
                        previous_strategy = snapshot.get("previous_strategy")
                    ready.append({
                        "series_ticker": ticker,
                        "strategy_name": expected_strategy,
                        "previous_strategy": previous_strategy,
                    })
                await session.commit()

            async with self.secondary_session_factory() as secondary_session:
                secondary_repo = PlatformRepository(secondary_session, kalshi_env=self.settings.kalshi_env)
                applied: list[dict[str, Any]] = []
                for item in ready:
                    ticker = str(item["series_ticker"])
                    expected_strategy = str(item["strategy_name"])
                    previous_strategy = item.get("previous_strategy")
                    current = await secondary_repo.get_city_strategy_assignment(ticker, kalshi_env=self.settings.kalshi_env)
                    current_strategy = current.strategy_name if current is not None else None
                    if current_strategy not in {previous_strategy, expected_strategy}:
                        skipped.append({
                            "series_ticker": ticker,
                            "reason": "secondary_assignment_changed",
                            "current_strategy": current_strategy,
                            "expected_strategy": expected_strategy,
                            "previous_strategy": previous_strategy,
                        })
                        continue
                    await secondary_repo.set_city_strategy_assignment(
                        ticker,
                        expected_strategy,
                        assigned_by=AUTO_EVOLVE_ASSIGNED_BY,
                        kalshi_env=self.settings.kalshi_env,
                    )
                    applied.append({
                        "series_ticker": ticker,
                        "strategy_name": expected_strategy,
                    })
                await secondary_session.commit()

            resolution = {
                "attempted_at": attempted_at.isoformat(),
                "trigger_source": trigger_source,
                "applied": applied,
                "skipped": skipped,
                "applied_count": len(applied),
                "skipped_count": len(skipped),
            }
            async with self.session_factory() as session:
                primary_repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                promotion = await primary_repo.get_strategy_promotion(promotion_id)
                promotion_details = dict(promotion.promotion_details or {}) if promotion is not None else {}
                promotion_details["secondary_sync_skipped_cities"] = skipped
                await primary_repo.update_strategy_promotion(
                    promotion_id,
                    secondary_sync_status="synced",
                    secondary_sync_error=None,
                    secondary_sync_resolution=resolution,
                    promotion_details=promotion_details,
                )
                await session.commit()
            return {
                "promotion_id": promotion_id,
                "status": "synced",
                "applied_count": len(applied),
                "skipped_count": len(skipped),
                "skipped": skipped,
            }
        except Exception as exc:
            detail = str(exc)[:500]
            async with self.session_factory() as session:
                primary_repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                await primary_repo.update_strategy_promotion(
                    promotion_id,
                    secondary_sync_status="failed",
                    secondary_sync_error=detail,
                )
                await primary_repo.log_ops_event(
                    severity="high",
                    summary="Secondary assignment sync failed",
                    source=AUTO_EVOLVE_SOURCE,
                    payload={
                        "code": "secondary_assignment_sync_failed",
                        "promotion_id": promotion_id,
                        "exc_type": type(exc).__name__,
                        "error": detail,
                    },
                )
                await session.commit()
            return {"promotion_id": promotion_id, "status": "failed", "error": detail}

    @staticmethod
    def _strategy_name_from_assignment_snapshot(snapshot: Any, *, fallback: str | None = None) -> str | None:
        if isinstance(snapshot, dict):
            value = snapshot.get("new_strategy") or snapshot.get("strategy_name")
        else:
            value = snapshot
        if value is None:
            value = fallback
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    async def _sync_secondary_rollback(self, promotion_id: int) -> dict[str, Any]:
        if self.secondary_session_factory is None:
            return {"promotion_id": promotion_id, "status": "skipped", "reason": "secondary_not_configured"}
        attempted_at = datetime.now(UTC)
        try:
            async with self.session_factory() as session:
                primary_repo = PlatformRepository(session)
                promotion = await primary_repo.get_strategy_promotion(promotion_id)
                if promotion is None:
                    return {"promotion_id": promotion_id, "status": "missing_promotion"}
                rollback_details = dict(promotion.rollback_details or {})
                restored = list(rollback_details.get("restored") or [])
                promoted_strategy_name = promotion.promoted_strategy_name
                await session.commit()

            async with self.secondary_session_factory() as secondary_session:
                secondary_repo = PlatformRepository(secondary_session, kalshi_env=self.settings.kalshi_env)
                applied: list[dict[str, Any]] = []
                skipped: list[dict[str, Any]] = []
                for item in restored:
                    ticker = str(item.get("series_ticker") or "")
                    if not ticker:
                        continue
                    current = await secondary_repo.get_city_strategy_assignment(ticker, kalshi_env=self.settings.kalshi_env)
                    current_strategy = current.strategy_name if current is not None else None
                    if current_strategy != promoted_strategy_name:
                        skipped.append({
                            "series_ticker": ticker,
                            "reason": "secondary_rollback_skipped_manual_override",
                            "current_strategy": current_strategy,
                            "expected_strategy": promoted_strategy_name,
                        })
                        continue
                    restored_strategy = item.get("restored_strategy")
                    if restored_strategy is None:
                        await secondary_repo.delete_city_strategy_assignment(ticker, kalshi_env=self.settings.kalshi_env)
                        applied.append({"series_ticker": ticker, "restored_strategy": None})
                    else:
                        await secondary_repo.set_city_strategy_assignment(
                            ticker,
                            str(restored_strategy),
                            assigned_by="watchdog_rollback",
                            kalshi_env=self.settings.kalshi_env,
                        )
                        applied.append({"series_ticker": ticker, "restored_strategy": restored_strategy})
                await secondary_session.commit()

            async with self.session_factory() as session:
                primary_repo = PlatformRepository(session)
                promotion = await primary_repo.get_strategy_promotion(promotion_id)
                rollback_details = dict(promotion.rollback_details or {}) if promotion is not None else {}
                rollback_details["secondary_rollback_skipped_cities"] = skipped
                await primary_repo.update_strategy_promotion(
                    promotion_id,
                    secondary_rollback_status="synced",
                    secondary_rollback_error=None,
                    secondary_rollback_resolution={
                        "attempted_at": attempted_at.isoformat(),
                        "restored": applied,
                        "restored_count": len(applied),
                        "skipped": skipped,
                        "skipped_count": len(skipped),
                    },
                    rollback_details=rollback_details,
                )
                await session.commit()
            return {
                "promotion_id": promotion_id,
                "status": "synced",
                "restored_count": len(applied),
                "skipped_count": len(skipped),
                "skipped": skipped,
            }
        except Exception as exc:
            async with self.session_factory() as session:
                primary_repo = PlatformRepository(session)
                await primary_repo.update_strategy_promotion(
                    promotion_id,
                    secondary_rollback_status="failed",
                    secondary_rollback_error=str(exc)[:500],
                )
                await primary_repo.log_ops_event(
                    severity="high",
                    summary="Secondary rollback sync failed",
                    source=AUTO_EVOLVE_SOURCE,
                    payload={
                        "code": "secondary_rollback_sync_failed",
                        "promotion_id": promotion_id,
                        "exc_type": type(exc).__name__,
                        "error": str(exc)[:500],
                    },
                )
                await session.commit()
            return {"promotion_id": promotion_id, "status": "failed", "error": str(exc)[:500]}

    async def _run_once(self, *, trigger_source: str) -> dict[str, Any]:
        payload = self._base_payload(trigger_source=trigger_source)
        if not self.settings.strategy_auto_evolve_enabled:
            payload.update({"status": "skipped", "reason": "disabled"})
            await self._record_result(payload, severity="info", summary="Strategy Auto-Evolve skipped: disabled")
            return payload

        active_color = await self._active_deployment_color()
        if active_color and active_color != self.settings.app_color:
            payload.update(
                {
                    "status": "skipped",
                    "reason": "inactive_color",
                    "active_color": active_color,
                }
            )
            await self._log_result_event(
                payload,
                severity="info",
                summary="Strategy Auto-Evolve skipped: inactive deployment color",
            )
            return payload

        previous = await self._get_checkpoint_payload()
        if previous.get("local_date") == payload["local_date"] and previous.get("status") in {
            "already_completed",
            "completed",
            "completed_with_failures",
        }:
            payload.update(
                {
                    "status": "already_completed",
                    "reason": "same_local_date_already_completed",
                    "previous_ran_at": previous.get("ran_at"),
                    "run_ids": previous.get("run_ids", []),
                    "accepted_strategy": previous.get("accepted_strategy"),
                    "activated_strategy": previous.get("activated_strategy"),
                    "assignment_changes": previous.get("assignment_changes", []),
                    "assignment_skips": previous.get("assignment_skips", []),
                }
            )
            await self._set_checkpoint(payload)
            return payload

        if not self.strategy_codex_service.is_available():
            payload.update({"status": "skipped", "reason": "codex_unavailable"})
            await self._record_result(
                payload,
                severity="warning",
                summary="Strategy Auto-Evolve skipped: strategy provider unavailable",
            )
            return payload

        audit_summary = await self._trading_audit_summary()
        analysis_summary = await self._trade_analysis_summary()
        payload["trading_audit"] = audit_summary
        payload["trade_analysis"] = analysis_summary
        if audit_summary.get("blocked"):
            payload.update({"status": "skipped", "reason": "trading_audit_blocked"})
            await self._record_result(
                payload,
                severity="warning",
                summary="Strategy Auto-Evolve skipped: trading audit blockers present",
            )
            return payload

        regression_payload = await self._ensure_fresh_regression()
        payload["regression"] = regression_payload
        if not regression_payload.get("fresh"):
            payload.update({"status": "skipped", "reason": "fresh_regression_unavailable"})
            await self._record_result(
                payload,
                severity="warning",
                summary="Strategy Auto-Evolve skipped: fresh regression unavailable",
            )
            return payload

        snapshot = await self.strategy_dashboard_service.build_dashboard(
            window_days=self.settings.strategy_auto_evolve_window_days,
            include_codex_lab=False,
        )
        snapshot["trading_audit"] = audit_summary
        snapshot["trade_analysis"] = analysis_summary
        run_views = await self.strategy_codex_service.execute_modes_for_snapshot(
            modes=["evaluate", "suggest"],
            dashboard_snapshot=snapshot,
            window_days=self.settings.strategy_auto_evolve_window_days,
            trigger_source=trigger_source,
        )
        payload["run_ids"] = [run.get("id") for run in run_views if run.get("id")]
        payload["run_statuses"] = [run.get("status") for run in run_views]
        payload.update(self._provider_summary(run_views))

        errors: list[dict[str, Any]] = []
        suggestion = self._suggestion_run(run_views)
        accepted_strategy: str | None = None
        activated_strategy: str | None = None
        if self.settings.strategy_auto_evolve_accept_suggestions:
            accepted_strategy, accept_error = await self._accept_suggestion_if_ready(suggestion)
            if accept_error is not None:
                errors.append(accept_error)
        else:
            payload["suggestion_skipped_reason"] = "accept_disabled"

        if accepted_strategy and self.settings.strategy_auto_evolve_activate_suggestions:
            try:
                activation = await self.strategy_codex_service.activate_strategy(accepted_strategy)
                activated_strategy = activation.get("strategy_name") or accepted_strategy
            except Exception as exc:
                errors.append({"stage": "activate", "strategy_name": accepted_strategy, "error": str(exc)})
        elif accepted_strategy:
            payload["activation_skipped_reason"] = "activate_disabled"

        assignment_snapshot = await self.strategy_dashboard_service.build_dashboard(
            window_days=self.settings.strategy_auto_evolve_window_days,
            include_codex_lab=False,
        )
        assignment_snapshot["trading_audit"] = audit_summary
        assignment_snapshot["trade_analysis"] = analysis_summary
        if errors:
            assignment_result = {"changes": [], "skips": [], "errors": []}
        else:
            assignment_result = (
                await self._apply_eligible_assignments(
                    assignment_snapshot,
                    trigger_source=trigger_source,
                    candidate_strategy=activated_strategy,
                )
                if self.settings.strategy_auto_evolve_assign_eligible
                else {"changes": [], "skips": [], "errors": [{"stage": "assign", "reason": "assign_disabled"}]}
            )
        errors.extend(assignment_result.get("errors", []))

        payload.update(
            {
                "status": "completed" if not errors else "completed_with_failures",
                "accepted_strategy": accepted_strategy,
                "activated_strategy": activated_strategy,
                "assignment_changes": assignment_result.get("changes", []),
                "assignment_skips": assignment_result.get("skips", []),
                "promotion_id": assignment_result.get("promotion_id"),
                "errors": errors,
            }
        )
        summary = (
            f"Strategy Auto-Evolve completed: "
            f"{'activated ' + activated_strategy if activated_strategy else 'no strategy activated'}, "
            f"applied {len(payload['assignment_changes'])} assignment(s)"
        )
        await self._record_result(
            payload,
            severity="warning" if errors else "info",
            summary=summary,
        )
        return payload

    async def _accept_suggestion_if_ready(self, suggestion: dict[str, Any] | None) -> tuple[str | None, dict[str, Any] | None]:
        if suggestion is None:
            return None, {"stage": "accept", "reason": "suggestion_missing"}
        if suggestion.get("status") != "completed":
            return None, {"stage": "accept", "reason": "suggestion_not_completed", "status": suggestion.get("status")}
        backtest = dict((suggestion.get("result") or {}).get("backtest") or {})
        if backtest.get("status") != "ok":
            return None, {"stage": "accept", "reason": "backtest_not_ok", "backtest_status": backtest.get("status")}
        stale_error = await self._validate_accept_freshness(suggestion, backtest)
        if stale_error is not None:
            return None, stale_error
        quality_error = await self._validate_backtest_quality(backtest)
        if quality_error is not None:
            return None, quality_error

        proposed_thresholds = dict(((suggestion.get("result") or {}).get("candidate") or {}).get("thresholds") or {})
        if proposed_thresholds:
            cap_error = await self._validate_delta_cap(
                proposed_thresholds,
                baseline_thresholds=dict(((suggestion.get("result") or {}).get("threshold_baseline") or {})),
            )
            if cap_error is not None:
                logger.warning(
                    "auto-evolve suggestion rejected: delta cap exceeded — %s",
                    cap_error.get("violations"),
                )
                return None, cap_error

        saved_name = suggestion.get("saved_strategy_name")
        if saved_name:
            return str(saved_name), None
        if not suggestion.get("can_accept", False):
            return None, {"stage": "accept", "reason": "run_not_accept_eligible", "run_id": suggestion.get("id")}
        try:
            accepted = await self.strategy_codex_service.accept_run(str(suggestion["id"]))
        except Exception as exc:
            return None, {"stage": "accept", "run_id": suggestion.get("id"), "error": str(exc)}
        return accepted.get("strategy_name"), None

    async def _validate_accept_freshness(self, suggestion: dict[str, Any], backtest: dict[str, Any]) -> dict[str, Any] | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            current_corpus = await repo.get_current_decision_corpus_build(kalshi_env=self.settings.kalshi_env)
            await session.commit()
        current_corpus_id = current_corpus.id if current_corpus is not None else None
        backtest_corpus_id = backtest.get("corpus_build_id")
        if current_corpus_id is None:
            return {"stage": "accept", "reason": "insufficient_corpus", "backtest_status": "insufficient_data"}
        if backtest_corpus_id is None:
            return {
                "stage": "accept",
                "reason": "backtest_corpus_missing",
                "run_id": suggestion.get("id"),
                "current_corpus_build_id": current_corpus_id,
            }
        if str(backtest_corpus_id) != str(current_corpus_id):
            return {
                "stage": "accept",
                "reason": "stale_backtest_corpus",
                "run_id": suggestion.get("id"),
                "backtest_corpus_build_id": backtest_corpus_id,
                "current_corpus_build_id": current_corpus_id,
            }

        raw_completed_at = suggestion.get("completed_at") or suggestion.get("finished_at")
        if raw_completed_at is None:
            return {"stage": "accept", "reason": "invalid_run_timestamp", "run_id": suggestion.get("id")}
        completed_at = self._parse_datetime(raw_completed_at)
        if completed_at is None:
            return {"stage": "accept", "reason": "invalid_run_timestamp", "run_id": suggestion.get("id")}
        max_age = timedelta(seconds=self.settings.strategy_auto_evolve_accept_max_run_age_seconds)
        if datetime.now(UTC) - completed_at > max_age:
            return {
                "stage": "accept",
                "reason": "stale_backtest_run",
                "run_id": suggestion.get("id"),
                "completed_at": completed_at.isoformat(),
                "max_age_seconds": self.settings.strategy_auto_evolve_accept_max_run_age_seconds,
            }
        return None

    async def _validate_backtest_quality(self, backtest: dict[str, Any]) -> dict[str, Any] | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            current_corpus = await repo.get_current_decision_corpus_build(kalshi_env=self.settings.kalshi_env)
            await session.commit()
        if current_corpus is None:
            return {"stage": "accept", "reason": "insufficient_corpus", "backtest_status": "insufficient_data"}
        resolved_rooms = self._int_metric(backtest.get("resolved_regression_rooms"))
        if resolved_rooms is None:
            return {
                "stage": "accept",
                "reason": "backtest_resolved_rooms_missing",
                "backtest_status": "insufficient_data",
                "corpus_build_id": current_corpus.id,
            }
        if resolved_rooms < self.settings.strategy_auto_evolve_backtest_min_resolved_regression_rooms:
            return {
                "stage": "accept",
                "reason": "insufficient_corpus",
                "backtest_status": "insufficient_data",
                "resolved_regression_rooms": resolved_rooms,
                "min_rows": self.settings.strategy_auto_evolve_backtest_min_resolved_regression_rooms,
                "corpus_build_id": current_corpus.id,
            }

        candidate_metrics = dict(backtest.get("candidate_metrics") or {})
        candidate_trades = self._int_metric(backtest.get("candidate_hypothetical_trades"))
        if candidate_trades is None:
            return {
                "stage": "accept",
                "reason": "candidate_hypothetical_trades_missing",
                "backtest_status": "insufficient_data",
                "corpus_build_id": current_corpus.id,
            }
        if candidate_trades < self.settings.strategy_auto_evolve_backtest_min_candidate_trades:
            return {
                "stage": "accept",
                "reason": "insufficient_backtest_trades",
                "backtest_status": "insufficient_data",
                "candidate_hypothetical_trades": candidate_trades,
                "min_candidate_trades": self.settings.strategy_auto_evolve_backtest_min_candidate_trades,
                "corpus_build_id": current_corpus.id,
            }

        candidate_result = {}
        for row in list(backtest.get("candidate_result_rows") or []):
            if isinstance(row, dict):
                candidate_result = dict(row)
                break

        def candidate_metric(name: str) -> Any:
            value = candidate_metrics.get(name)
            return candidate_result.get(name) if value is None else value

        floor_failures: list[str] = []
        if candidate_metric("promotion_candidate") is not True:
            floor_failures.append("promotion_candidate")
        cluster_count = self._int_metric(candidate_metric("cluster_count"))
        if cluster_count is None or cluster_count < self.settings.strategy_regression_promote_floor_clusters:
            floor_failures.append("cluster_count")
        sortino = self._numeric_value(candidate_metric("sortino"), default=None)
        if sortino is None or sortino < self.settings.strategy_regression_min_sortino_for_promotion:
            floor_failures.append("sortino")
        total_pnl = self._numeric_value(
            self._first_present(
                candidate_metric("total_pnl_dollars"),
                candidate_metric("total_net_pnl_dollars"),
            ),
            default=None,
        )
        if total_pnl is None or total_pnl <= 0:
            floor_failures.append("total_net_pnl")
        if candidate_metric("below_support_floor") is True:
            floor_failures.append("below_support_floor")
        if candidate_metric("insufficient_for_ranking") is True:
            floor_failures.append("insufficient_for_ranking")
        if floor_failures:
            return {
                "stage": "accept",
                "reason": "failed_quality_floor",
                "floor_failures": sorted(set(floor_failures)),
                "cluster_count": cluster_count,
                "min_clusters": self.settings.strategy_regression_promote_floor_clusters,
                "sortino": sortino,
                "min_sortino": self.settings.strategy_regression_min_sortino_for_promotion,
                "total_pnl_dollars": total_pnl,
                "promotion_candidate": candidate_metric("promotion_candidate"),
                "corpus_build_id": current_corpus.id,
            }

        baseline = dict(
            backtest.get("assignment_weighted_baseline")
            or backtest.get("current_assignment_weighted_baseline")
            or {}
        )
        baseline_corpus_id = baseline.get("corpus_build_id") or backtest.get("baseline_corpus_build_id")
        if not baseline or baseline_corpus_id is None:
            return {
                "stage": "accept",
                "reason": "assignment_weighted_baseline_missing",
                "corpus_build_id": current_corpus.id,
            }
        if str(baseline_corpus_id) != str(current_corpus.id):
            return {
                "stage": "accept",
                "reason": "stale_assignment_weighted_baseline_corpus",
                "baseline_corpus_build_id": baseline_corpus_id,
                "current_corpus_build_id": current_corpus.id,
            }
        candidate_win_rate = candidate_metrics.get("assignment_weighted_win_rate")
        if candidate_win_rate is None:
            candidate_win_rate = candidate_metrics.get("overall_win_rate")
        baseline_win_rate = baseline.get("assignment_weighted_win_rate")
        if baseline_win_rate is None:
            baseline_win_rate = baseline.get("overall_win_rate")
        if candidate_win_rate is None or baseline_win_rate is None:
            return {
                "stage": "accept",
                "reason": "assignment_weighted_baseline_missing",
                "corpus_build_id": current_corpus.id,
            }
        improvement_bps = delta_to_bps(candidate_win_rate, baseline_win_rate)
        min_improvement_bps = self.settings.strategy_auto_evolve_min_improvement_bps
        if improvement_bps < min_improvement_bps:
            return {
                "stage": "accept",
                "reason": "failed_quality_floor",
                "candidate_win_rate": candidate_win_rate,
                "baseline_win_rate": baseline_win_rate,
                "improvement_bps": improvement_bps,
                "min_improvement_bps": min_improvement_bps,
                "corpus_build_id": current_corpus.id,
            }
        return None

    @staticmethod
    def _int_metric(raw: Any) -> int | None:
        if isinstance(raw, bool) or raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    async def _validate_delta_cap(self, proposed: dict[str, Any], *, baseline_thresholds: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Return an error dict if any numeric threshold field moves beyond the configured cap."""
        current = dict(baseline_thresholds or {})
        if not current:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                active_strategies = await repo.list_strategies(active_only=True)
                await session.commit()
            if not active_strategies:
                return None
            current_record = max(active_strategies, key=lambda s: s.created_at)
            current = dict(current_record.thresholds or {})

        unknown_fields = sorted(set(proposed) - set(_THRESHOLD_FIELD_CEILING))
        if unknown_fields:
            return {"stage": "accept", "reason": "invalid_threshold_schema", "unknown_fields": unknown_fields}
        complete_proposed = {**current, **proposed}
        return delta_cap_error(
            current,
            complete_proposed,
            max_delta_pct=self.settings.strategy_auto_evolve_max_threshold_delta_pct,
            field_ceilings=_THRESHOLD_FIELD_CEILING,
        )

    async def _apply_eligible_assignments(
        self,
        snapshot: dict[str, Any],
        *,
        trigger_source: str = "manual",
        candidate_strategy: str | None = None,
    ) -> dict[str, Any]:
        changes: list[dict[str, Any]] = []
        skips: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        cycle_cap = self.settings.strategy_auto_evolve_max_cities_per_cycle
        rows = list(snapshot.get("city_matrix") or [])
        snapshot_summary = dict(snapshot.get("summary") or {})
        snapshot_corpus_build_id = snapshot_summary.get("corpus_build_id")
        snapshot_run_at = snapshot_summary.get("last_regression_run")

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            current_corpus = await repo.get_current_decision_corpus_build(kalshi_env=self.settings.kalshi_env)
            current_corpus_build_id = current_corpus.id if current_corpus is not None else None
            locked_cities = await repo.list_auto_evolve_locked_city_tickers(kalshi_env=self.settings.kalshi_env)
            await session.commit()
        if current_corpus_build_id is None:
            return {
                "changes": [],
                "skips": skips,
                "errors": [{"stage": "assign", "reason": "decision_corpus_missing"}],
                "promotion_id": None,
            }
        if snapshot_corpus_build_id != current_corpus_build_id:
            return {
                "changes": [],
                "skips": skips,
                "errors": [
                    {
                        "stage": "assign",
                        "reason": "stale_assignment_snapshot",
                        "snapshot_corpus_build_id": snapshot_corpus_build_id,
                        "current_corpus_build_id": current_corpus_build_id,
                    }
                ],
                "promotion_id": None,
            }

        eligible: list[dict[str, Any]] = []
        for row in rows:
            series_ticker = row.get("series_ticker")
            recommendation = dict(row.get("recommendation") or {})
            recommended_name = recommendation.get("strategy_name")
            previous_name = (row.get("assignment") or {}).get("strategy_name")
            ticker = str(series_ticker) if series_ticker else None
            if not ticker:
                skips.append({"series_ticker": series_ticker, "reason": "not_recommended"})
                continue
            if ticker in locked_cities:
                skips.append({"series_ticker": ticker, "reason": "watchdog_locked"})
                continue
            cooldown_skip = await self._assignment_cooldown_skip(ticker)
            if cooldown_skip is not None:
                skips.append(cooldown_skip)
                continue
            if (
                not row.get("approval_eligible")
                or not recommended_name
                or recommendation.get("status") not in {"strong_recommendation", "lean_recommendation"}
            ):
                skips.append(
                    {
                        "series_ticker": ticker,
                        "reason": "not_recommended",
                        "recommendation_status": recommendation.get("status"),
                    }
                )
                continue
            if candidate_strategy is not None and recommended_name != candidate_strategy:
                skips.append(
                    {
                        "series_ticker": ticker,
                        "reason": "not_recommended",
                        "recommendation_status": recommendation.get("status"),
                        "strategy_name": recommended_name,
                        "candidate_strategy": candidate_strategy,
                    }
                )
                continue
            if previous_name == recommended_name:
                skips.append({
                    "series_ticker": ticker,
                    "reason": "already_matching",
                    "strategy_name": recommended_name,
                })
                continue
            city_corpus_days = self._numeric_value(
                row.get("city_corpus_days")
                or row.get("eligible_corpus_days")
                or recommendation.get("city_corpus_days"),
                default=0.0,
            )
            if city_corpus_days < self.settings.strategy_auto_evolve_min_city_corpus_days:
                skips.append({
                    "series_ticker": ticker,
                    "reason": "insufficient_corpus_coverage",
                    "city_corpus_days": city_corpus_days,
                    "min_city_corpus_days": self.settings.strategy_auto_evolve_min_city_corpus_days,
                })
                continue

            greenfield = not previous_name
            candidate_win_rate = self._numeric_value(
                recommendation.get("win_rate")
                or row.get("candidate_win_rate")
                or recommendation.get("candidate_win_rate"),
                default=None,
            )
            candidate_trades = int(
                self._numeric_value(
                    recommendation.get("resolved_trade_count")
                    or row.get("candidate_resolved_trades")
                    or row.get("candidate_resolved_trade_count"),
                    default=0.0,
                )
                or 0
            )
            candidate_pnl = self._numeric_value(
                recommendation.get("total_pnl_dollars")
                or row.get("candidate_total_pnl")
                or row.get("candidate_total_pnl_dollars"),
                default=None,
            )
            improvement = self._numeric_value(
                row.get("expected_improvement")
                if row.get("expected_improvement") is not None
                else row.get("gap_to_assignment"),
                default=None,
            )
            ranking_quality = self._ranking_quality_payload(row, recommendation)
            if ranking_quality["present"] and ranking_quality["floor_failures"]:
                skips.append({
                    "series_ticker": ticker,
                    "reason": "below_improvement_floor",
                    "floor_failures": ranking_quality["floor_failures"],
                    "ranking_version": ranking_quality["ranking_version"],
                    "sortino": ranking_quality["sortino"],
                    "min_sortino": self.settings.strategy_regression_min_sortino_for_promotion,
                    "cluster_count": ranking_quality["cluster_count"],
                    "min_clusters": self.settings.strategy_regression_promote_floor_clusters,
                    "total_net_pnl_dollars": ranking_quality["total_net_pnl_dollars"],
                    "promotion_candidate": ranking_quality["promotion_candidate"],
                })
                continue

            if greenfield:
                if not self.settings.strategy_auto_evolve_greenfield_enabled:
                    skips.append({
                        "series_ticker": ticker,
                        "reason": "greenfield_disabled",
                        "strategy_name": recommended_name,
                    })
                    continue
                if ranking_quality["present"]:
                    sort_score = ranking_quality["sortino"] or 0.0
                else:
                    floor_failures: list[str] = []
                    try:
                        candidate_win_rate_bps = rate_to_bps(candidate_win_rate) if candidate_win_rate is not None else None
                    except ValueError as exc:
                        skips.append({"series_ticker": ticker, "reason": "invalid_metric", "error": str(exc)})
                        continue
                    if (
                        candidate_win_rate_bps is None
                        or candidate_win_rate_bps < self.settings.strategy_auto_evolve_greenfield_min_win_rate_bps
                    ):
                        floor_failures.append("greenfield_win_rate")
                    if candidate_trades < self.settings.strategy_auto_evolve_greenfield_min_resolved_trades:
                        floor_failures.append("greenfield_resolved_trades")
                    if candidate_pnl is None or candidate_pnl < 0:
                        floor_failures.append("greenfield_pnl")
                    if floor_failures:
                        skips.append({
                            "series_ticker": ticker,
                            "reason": "below_improvement_floor",
                            "floor_failures": floor_failures,
                            "candidate_win_rate": candidate_win_rate,
                            "candidate_resolved_trades": candidate_trades,
                            "candidate_total_pnl": candidate_pnl,
                        })
                        continue
                    reference = self.settings.strategy_auto_evolve_greenfield_reference_win_rate
                    sort_score = (
                        candidate_win_rate - reference
                        if candidate_win_rate is not None
                        else (improvement if improvement is not None else 0.0)
                    )
                assignment_type = "greenfield"
            else:
                live_fills = int(
                    self._numeric_value(
                        row.get("incumbent_strategy_live_fill_count_30d")
                        or row.get("recent_live_resolved_fills")
                        or row.get("resolved_live_fills_30d"),
                        default=0.0,
                    )
                    or 0
                )
                if live_fills <= 0:
                    live_fills = await self._incumbent_recent_live_fill_count(
                        series_ticker=ticker,
                        strategy_name=str(previous_name),
                    )
                if live_fills < self.settings.strategy_auto_evolve_min_recent_live_resolved_fills:
                    skips.append({
                        "series_ticker": ticker,
                        "reason": "insufficient_live_fills",
                        "recent_live_resolved_fills": live_fills,
                        "min_recent_live_resolved_fills": self.settings.strategy_auto_evolve_min_recent_live_resolved_fills,
                    })
                    continue
                improvement_value = improvement if improvement is not None else 0.0
                if not ranking_quality["present"]:
                    try:
                        improvement_bps = delta_to_bps(improvement_value)
                    except ValueError as exc:
                        skips.append({"series_ticker": ticker, "reason": "invalid_metric", "error": str(exc)})
                        continue
                    if improvement_bps < self.settings.strategy_auto_evolve_assignment_min_improvement_bps:
                        skips.append({
                            "series_ticker": ticker,
                            "reason": "below_improvement_floor",
                            "expected_improvement": improvement_value,
                            "min_improvement_bps": self.settings.strategy_auto_evolve_assignment_min_improvement_bps,
                        })
                        continue
                incumbent_health = dict(row.get("incumbent_health") or {})
                incumbent_is_healthy = incumbent_health.get("status") == "healthy" or row.get("incumbent_healthy") is True
                if not incumbent_is_healthy and not incumbent_health.get("status"):
                    incumbent_win_rate = self._numeric_value(
                        incumbent_health.get("win_rate_30d")
                        or row.get("incumbent_win_rate_30d")
                        or row.get("current_assignment_win_rate"),
                        default=None,
                    )
                    incumbent_pnl = self._numeric_value(
                        incumbent_health.get("realized_pnl_30d")
                        or row.get("incumbent_realized_pnl_30d")
                        or row.get("current_assignment_total_pnl_dollars"),
                        default=None,
                    )
                    incumbent_fill_count = int(
                        self._numeric_value(
                            incumbent_health.get("fill_count_30d")
                            or row.get("incumbent_live_fill_count_30d")
                            or live_fills,
                            default=0.0,
                        )
                        or 0
                    )
                    try:
                        incumbent_win_rate_bps = rate_to_bps(incumbent_win_rate) if incumbent_win_rate is not None else None
                    except ValueError as exc:
                        skips.append({"series_ticker": ticker, "reason": "invalid_metric", "error": str(exc)})
                        continue
                    incumbent_is_healthy = bool(
                        incumbent_win_rate_bps is not None
                        and incumbent_win_rate_bps >= self.settings.strategy_auto_evolve_incumbent_health_win_rate_floor_bps
                        and incumbent_pnl is not None
                        and incumbent_pnl >= 0
                        and incumbent_fill_count >= self.settings.strategy_auto_evolve_min_recent_live_resolved_fills
                    )
                    if incumbent_is_healthy:
                        incumbent_health = {
                            **incumbent_health,
                            "status": "healthy",
                            "win_rate_30d": incumbent_win_rate,
                            "realized_pnl_30d": incumbent_pnl,
                            "fill_count_30d": incumbent_fill_count,
                            "health_source": "metrics_fallback",
                        }
                if incumbent_is_healthy:
                    skips.append({
                        "series_ticker": ticker,
                        "reason": "incumbent_healthy",
                        "incumbent_health": incumbent_health or None,
                        "candidate_estimated_improvement": improvement_value,
                    })
                    continue
                sort_score = ranking_quality["sortino"] if ranking_quality["present"] else improvement_value
                assignment_type = "reassignment"

            row["_auto_evolve_sort_score"] = float(sort_score or 0.0)
            row["_auto_evolve_ranking_mode"] = ranking_quality["present"]
            row["_auto_evolve_ranking_sortino"] = ranking_quality["sortino"]
            row["_auto_evolve_ranking_cluster_count"] = ranking_quality["cluster_count"]
            row["_auto_evolve_ranking_total_net_pnl"] = ranking_quality["total_net_pnl_dollars"]
            row["_auto_evolve_assignment_type"] = assignment_type
            eligible.append(row)

        if any(row.get("_auto_evolve_ranking_mode") for row in eligible):
            eligible.sort(
                key=lambda r: (
                    0 if r.get("_auto_evolve_ranking_mode") else 1,
                    -(r.get("_auto_evolve_ranking_sortino") if r.get("_auto_evolve_ranking_sortino") is not None else -1.0),
                    -int(r.get("_auto_evolve_ranking_cluster_count") or 0),
                    -(r.get("_auto_evolve_ranking_total_net_pnl") if r.get("_auto_evolve_ranking_total_net_pnl") is not None else float("-inf")),
                    r.get("series_ticker") or "",
                )
            )
        else:
            eligible.sort(
                key=lambda r: (
                    -(r.get("_auto_evolve_sort_score") or 0.0),
                    -int(((r.get("recommendation") or {}).get("resolved_trade_count") or r.get("candidate_resolved_trade_count") or 0)),
                    -(self._numeric_value(r.get("candidate_total_pnl") or ((r.get("recommendation") or {}).get("total_pnl_dollars")), default=0.0) or 0.0),
                    r.get("series_ticker") or "",
                )
            )
        for index, row in enumerate(eligible, start=1):
            row["_auto_evolve_eligible_rank"] = index

        to_assign = eligible[:cycle_cap]
        for row in eligible[cycle_cap:]:
            recommendation = dict(row.get("recommendation") or {})
            skips.append({
                "series_ticker": row.get("series_ticker"),
                "reason": "cycle_cap_exceeded",
                "strategy_name": recommendation.get("strategy_name"),
                "recommendation_status": recommendation.get("status"),
                "eligible_rank": row.get("_auto_evolve_eligible_rank"),
                "sort_score": row.get("_auto_evolve_sort_score"),
                "max_cities_per_cycle": cycle_cap,
                "would_assign": cycle_cap == 0,
            })

        promotion_id: int | None = None
        if to_assign:
            now = datetime.now(UTC)
            previous_snapshot: dict[str, Any] = {}
            new_snapshot: dict[str, Any] = {}
            pending_changes: list[dict[str, Any]] = []
            try:
                async with self.session_factory() as session:
                    repo = PlatformRepository(session)
                    for row in to_assign:
                        ticker = str(row.get("series_ticker"))
                        recommendation = dict(row.get("recommendation") or {})
                        recommended_name = str(recommendation.get("strategy_name"))
                        previous_name = (row.get("assignment") or {}).get("strategy_name")
                        previous_snapshot[ticker] = self._previous_assignment_snapshot(row, now=now)
                        new_snapshot[ticker] = {
                            "assignment_type": row.get("_auto_evolve_assignment_type"),
                            "previous_strategy": previous_name,
                            "new_strategy": recommended_name,
                            "evidence_corpus_build_id": snapshot_corpus_build_id,
                            "evidence_run_at": snapshot_run_at,
                            "eligible_rank": row.get("_auto_evolve_eligible_rank"),
                            "sort_score": row.get("_auto_evolve_sort_score"),
                            "ranking_version": recommendation.get("ranking_version") or row.get("ranking_version"),
                            "sortino": recommendation.get("sortino") or row.get("candidate_sortino"),
                            "cluster_count": recommendation.get("cluster_count") or row.get("candidate_cluster_count"),
                            "total_net_pnl_dollars": (
                                recommendation.get("total_net_pnl_dollars")
                                or row.get("candidate_total_net_pnl_dollars")
                            ),
                            "promotion_candidate": recommendation.get("promotion_candidate") or row.get("promotion_candidate"),
                            "expected_improvement": row.get("expected_improvement")
                            if row.get("expected_improvement") is not None
                            else row.get("gap_to_assignment"),
                            "candidate_win_rate": recommendation.get("win_rate") or row.get("candidate_win_rate"),
                            "candidate_resolved_trades": recommendation.get("resolved_trade_count")
                            or row.get("candidate_resolved_trade_count"),
                        }
                    promotion = await repo.create_strategy_promotion(
                        promoted_strategy_name=str((dict(to_assign[0].get("recommendation") or {})).get("strategy_name")),
                        previous_city_assignments=previous_snapshot,
                        new_city_assignments=new_snapshot,
                        baseline_metrics={"assignment_count": len(to_assign)},
                        promotion_details={
                            "assignment_changes": list(new_snapshot.values()),
                            "evidence_corpus_build_id": snapshot_corpus_build_id,
                            "evidence_run_at": snapshot_run_at,
                        },
                        trigger_source=trigger_source,
                        secondary_sync_status="pending" if self.secondary_session_factory is not None else "not_applicable",
                        promoted_at=now,
                        watchdog_due_at=now + timedelta(days=7),
                        watchdog_extended_due_at=now + timedelta(days=14),
                    )
                    promotion_id = promotion.id

                    for row in to_assign:
                        ticker = str(row.get("series_ticker"))
                        recommendation = dict(row.get("recommendation") or {})
                        recommended_name = str(recommendation.get("strategy_name"))
                        previous_name = (row.get("assignment") or {}).get("strategy_name")
                        await repo.set_city_strategy_assignment(
                            ticker,
                            recommended_name,
                            assigned_by=AUTO_EVOLVE_ASSIGNED_BY,
                            kalshi_env=self.settings.kalshi_env,
                            evidence_corpus_build_id=snapshot_corpus_build_id,
                            evidence_run_at=snapshot_run_at,
                        )
                        await repo.record_city_assignment_event(
                            series_ticker=ticker,
                            previous_strategy=previous_name,
                            new_strategy=recommended_name,
                            event_type="auto_evolve_assign",
                            actor=AUTO_EVOLVE_SOURCE,
                            promotion_id=promotion_id,
                            kalshi_env=self.settings.kalshi_env,
                            note=f"Auto-evolve promotion {promotion_id}",
                            metadata={
                                "eligible_rank": row.get("_auto_evolve_eligible_rank"),
                                "sort_score": row.get("_auto_evolve_sort_score"),
                                "recommendation_status": recommendation.get("status"),
                                "corpus_build_id": snapshot_corpus_build_id,
                                "basis_run_at": snapshot_run_at,
                            },
                        )
                        pending_changes.append({
                            "series_ticker": ticker,
                            "assignment_type": row.get("_auto_evolve_assignment_type"),
                            "previous_strategy": previous_name,
                            "new_strategy": recommended_name,
                            "evidence_corpus_build_id": snapshot_corpus_build_id,
                            "evidence_run_at": snapshot_run_at,
                            "recommendation_status": recommendation.get("status"),
                            "recommendation_label": recommendation.get("label"),
                            "eligible_rank": row.get("_auto_evolve_eligible_rank"),
                            "sort_score": row.get("_auto_evolve_sort_score"),
                            "ranking_version": recommendation.get("ranking_version") or row.get("ranking_version"),
                            "sortino": recommendation.get("sortino") or row.get("candidate_sortino"),
                            "cluster_count": recommendation.get("cluster_count") or row.get("candidate_cluster_count"),
                            "total_net_pnl_dollars": (
                                recommendation.get("total_net_pnl_dollars")
                                or row.get("candidate_total_net_pnl_dollars")
                            ),
                            "promotion_candidate": recommendation.get("promotion_candidate") or row.get("promotion_candidate"),
                            "gap_to_runner_up": row.get("gap_to_runner_up"),
                            "gap_to_assignment": row.get("gap_to_assignment"),
                        })
                    await session.commit()
            except Exception as exc:
                return {"changes": [], "skips": skips, "errors": [{"stage": "assign", "error": str(exc)}], "promotion_id": None}
            changes.extend(pending_changes)

        if changes and self.secondary_session_factory is not None:
            if promotion_id is not None:
                secondary_result = await self._sync_secondary_assignment(
                    promotion_id,
                    trigger_source=trigger_source,
                )
                if secondary_result.get("status") == "failed":
                    errors.append({
                        "stage": "assign_secondary",
                        "error": secondary_result.get("error"),
                        "changes_attempted": len(changes),
                    })
        return {"changes": changes, "skips": skips, "errors": errors, "promotion_id": promotion_id}

    async def _assignment_cooldown_skip(self, series_ticker: str) -> dict[str, Any] | None:
        cooldown_days = self.settings.strategy_auto_evolve_city_assignment_cooldown_days
        if cooldown_days <= 0:
            return None
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            event = await repo.latest_city_assignment_event(
                series_ticker=series_ticker,
                kalshi_env=self.settings.kalshi_env,
                event_types={"auto_evolve_assign", "manual_assign", "manual_override"},
            )
            await session.commit()
        if event is None:
            return None
        cooldown_until = event.created_at + timedelta(days=cooldown_days)
        if datetime.now(UTC) >= cooldown_until:
            return None
        return {
            "series_ticker": series_ticker,
            "reason": "assignment_cooldown",
            "last_assignment_at": event.created_at.isoformat(),
            "cooldown_until": cooldown_until.isoformat(),
            "cooldown_days": cooldown_days,
        }

    @staticmethod
    def _numeric_value(value: Any, *, default: float | None) -> float | None:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _ranking_quality_payload(self, row: dict[str, Any], recommendation: dict[str, Any]) -> dict[str, Any]:
        ranking_version = (
            recommendation.get("ranking_version")
            or row.get("ranking_version")
            or (STRATEGY_RANKING_VERSION if recommendation.get("sortino") is not None or row.get("candidate_sortino") is not None else None)
        )
        present = ranking_version == STRATEGY_RANKING_VERSION
        sortino = self._numeric_value(
            self._first_present(
                recommendation.get("sortino"),
                row.get("candidate_sortino"),
                row.get("sortino"),
            ),
            default=None,
        )
        cluster_count_raw = self._first_present(
            recommendation.get("cluster_count"),
            row.get("candidate_cluster_count"),
            row.get("cluster_count"),
        )
        cluster_count = self._int_metric(cluster_count_raw)
        total_net_pnl = self._numeric_value(
            self._first_present(
                recommendation.get("total_net_pnl_dollars"),
                row.get("candidate_total_net_pnl_dollars"),
                row.get("total_net_pnl_dollars"),
                row.get("candidate_total_pnl_dollars"),
                recommendation.get("total_pnl_dollars"),
            ),
            default=None,
        )
        promotion_candidate = (
            recommendation.get("promotion_candidate")
            if recommendation.get("promotion_candidate") is not None
            else row.get("promotion_candidate")
        )
        below_support_floor = (
            recommendation.get("below_support_floor")
            if recommendation.get("below_support_floor") is not None
            else row.get("below_support_floor")
        )
        insufficient_for_ranking = (
            recommendation.get("insufficient_for_ranking")
            if recommendation.get("insufficient_for_ranking") is not None
            else row.get("insufficient_for_ranking")
        )
        floor_failures: list[str] = []
        if present:
            if promotion_candidate is not True:
                floor_failures.append("promotion_candidate")
            if cluster_count is None or cluster_count < self.settings.strategy_regression_promote_floor_clusters:
                floor_failures.append("cluster_count")
            if sortino is None or sortino < self.settings.strategy_regression_min_sortino_for_promotion:
                floor_failures.append("sortino")
            if total_net_pnl is None or total_net_pnl <= 0:
                floor_failures.append("total_net_pnl")
            if below_support_floor is True:
                floor_failures.append("below_support_floor")
            if insufficient_for_ranking is True:
                floor_failures.append("insufficient_for_ranking")
        return {
            "present": present,
            "ranking_version": ranking_version,
            "sortino": sortino,
            "cluster_count": cluster_count,
            "total_net_pnl_dollars": total_net_pnl,
            "promotion_candidate": promotion_candidate,
            "floor_failures": sorted(set(floor_failures)),
        }

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    def _previous_assignment_snapshot(self, row: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        assignment = dict(row.get("assignment") or {})
        incumbent_health = dict(row.get("incumbent_health") or {})
        snapshot: dict[str, Any] = {
            "strategy_name": assignment.get("strategy_name"),
            "snapshot_at": now.isoformat(),
        }
        metrics = {
            "incumbent_win_rate_30d": self._first_present(
                row.get("incumbent_win_rate_30d"),
                incumbent_health.get("win_rate_30d"),
                row.get("incumbent_win_rate"),
            ),
            "incumbent_realized_pnl_30d": self._first_present(
                row.get("incumbent_realized_pnl_30d"),
                incumbent_health.get("realized_pnl_30d"),
                row.get("incumbent_realized_pnl"),
            ),
            "incumbent_live_fill_count_30d": self._first_present(
                row.get("incumbent_strategy_live_fill_count_30d"),
                incumbent_health.get("fill_count_30d"),
                row.get("recent_live_resolved_fills"),
                row.get("resolved_live_fills_30d"),
            ),
        }
        for key, value in metrics.items():
            if value is not None:
                snapshot[key] = value
        return snapshot

    async def _incumbent_recent_live_fill_count(self, *, series_ticker: str, strategy_name: str) -> int:
        since = datetime.now(UTC) - timedelta(days=30)
        async with self.session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
            assignment = await repo.get_city_strategy_assignment(series_ticker, kalshi_env=self.settings.kalshi_env)
            if assignment is None or assignment.strategy_name != strategy_name:
                await session.commit()
                return 0
            assigned_at = self._as_utc(assignment.assigned_at)
            if assigned_at > since:
                since = assigned_at
            metrics = await repo.get_strategy_city_fill_metrics_since(
                series_ticker=series_ticker,
                strategy_name=strategy_name,
                since=since,
                kalshi_env=self.settings.kalshi_env,
            )
            await session.commit()
        return int(metrics.get("resolved_live_fills") or 0)

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    async def _evaluate_promotion_watchdog(self, repo: PlatformRepository, promotion: Any, *, now: datetime) -> dict[str, Any]:
        batch_cities = set(dict(promotion.new_city_assignments or {}).keys())
        live_assignments = await repo.get_cities_assigned_to_strategy(
            promotion.promoted_strategy_name,
            kalshi_env=self.settings.kalshi_env,
        )
        eval_cities = [assignment.series_ticker for assignment in live_assignments if assignment.series_ticker in batch_cities]
        if not eval_cities:
            await repo.update_strategy_promotion(
                promotion.id,
                watchdog_status="passed",
                watchdog_last_eval_at=now,
                watchdog_last_eval_reason="all_cities_manually_corrected",
                rollback_metrics={"eval_cities": [], "manually_corrected_cities": sorted(batch_cities)},
            )
            return {"promotion_id": promotion.id, "status": "passed", "reason": "all_cities_manually_corrected"}

        city_metrics: dict[str, dict[str, Any]] = {}
        total_fills = 0
        total_wins = 0
        total_pnl = 0.0
        weighted_baseline = 0.0
        previous = dict(promotion.previous_city_assignments or {})
        for ticker in eval_cities:
            metrics = await repo.get_strategy_city_fill_metrics_since(
                series_ticker=ticker,
                strategy_name=promotion.promoted_strategy_name,
                since=promotion.promoted_at,
                kalshi_env=self.settings.kalshi_env,
            )
            fills = int(metrics.get("resolved_live_fills") or 0)
            wins = int(metrics.get("win_count") or 0)
            pnl = float(metrics.get("realized_pnl") or 0.0)
            baseline_win_rate = self._baseline_win_rate_for_city(previous.get(ticker))
            metrics["baseline_win_rate"] = baseline_win_rate
            city_metrics[ticker] = metrics
            total_fills += fills
            total_wins += wins
            total_pnl += pnl
            weighted_baseline += fills * baseline_win_rate

        if total_fills < self.settings.strategy_auto_evolve_watchdog_min_resolved_live_fills:
            if now >= self._as_utc(promotion.watchdog_extended_due_at):
                await repo.update_strategy_promotion(
                    promotion.id,
                    watchdog_status="insufficient_data",
                    watchdog_last_eval_at=now,
                    watchdog_last_eval_reason="insufficient_data:insufficient_fills",
                    rollback_metrics={"city_metrics": city_metrics, "total_fills": total_fills},
                )
                return {"promotion_id": promotion.id, "status": "insufficient_data", "reason": "insufficient_fills"}
            updates = {
                "watchdog_status": "extended",
                "watchdog_last_eval_at": now,
                "watchdog_last_eval_reason": "insufficient_fills",
                "rollback_metrics": {"city_metrics": city_metrics, "total_fills": total_fills},
            }
            if promotion.watchdog_status == "pending":
                updates["watchdog_extended_reason"] = "insufficient_fills"
            await repo.update_strategy_promotion(promotion.id, **updates)
            return {"promotion_id": promotion.id, "status": "extended", "reason": "insufficient_fills"}

        aggregate_win_rate = total_wins / total_fills
        aggregate_baseline = weighted_baseline / total_fills if total_fills else 0.0
        trigger: str | None = None
        if rate_to_bps(aggregate_win_rate) <= rate_to_bps(aggregate_baseline) - self.settings.strategy_auto_evolve_watchdog_win_rate_degradation_bps:
            trigger = "aggregate_win_rate_breach"
        elif total_pnl < 0:
            trigger = "aggregate_pnl_breach"
        else:
            for ticker, metrics in city_metrics.items():
                fills = int(metrics.get("resolved_live_fills") or 0)
                if fills < self.settings.strategy_auto_evolve_watchdog_min_resolved_live_fills:
                    continue
                post_win_rate = metrics.get("win_rate")
                baseline_win_rate = metrics.get("baseline_win_rate")
                if post_win_rate is not None and baseline_win_rate is not None:
                    if rate_to_bps(post_win_rate) <= rate_to_bps(baseline_win_rate) - self.settings.strategy_auto_evolve_watchdog_win_rate_degradation_bps:
                        trigger = f"per_city_win_rate_breach:{ticker}"
                        break
                if float(metrics.get("realized_pnl") or 0.0) < 0:
                    trigger = f"per_city_pnl_breach:{ticker}"
                    break

        rollback_metrics = {
            "aggregate": {
                "post_win_rate": aggregate_win_rate,
                "baseline_win_rate": aggregate_baseline,
                "resolved_live_fills": total_fills,
                "realized_pnl": total_pnl,
            },
            "city_metrics": city_metrics,
        }
        if trigger is None:
            await repo.update_strategy_promotion(
                promotion.id,
                watchdog_status="passed",
                watchdog_last_eval_at=now,
                watchdog_last_eval_reason="passed",
                rollback_metrics=rollback_metrics,
            )
            return {"promotion_id": promotion.id, "status": "passed", "reason": "passed"}

        rollback = await self._rollback_promotion_batch(repo, promotion, now=now, trigger=trigger)
        await repo.update_strategy_promotion(
            promotion.id,
            watchdog_status="rolled_back",
            rollback_at=now,
            rollback_trigger=trigger,
            watchdog_last_eval_at=now,
            watchdog_last_eval_reason=f"rolled_back:{trigger}",
            rollback_metrics=rollback_metrics,
            rollback_details=rollback,
            secondary_rollback_status="pending" if self.secondary_session_factory is not None else "not_applicable",
        )
        return {"promotion_id": promotion.id, "status": "rolled_back", "reason": trigger, "rollback": rollback}

    def _baseline_win_rate_for_city(self, previous_state: Any) -> float:
        if not isinstance(previous_state, dict):
            return self.settings.strategy_auto_evolve_greenfield_reference_win_rate
        for key in ("baseline_win_rate", "incumbent_win_rate_30d", "win_rate_30d"):
            value = self._numeric_value(previous_state.get(key), default=None)
            if value is not None:
                return value
        if previous_state.get("strategy_name") is None:
            return self.settings.strategy_auto_evolve_greenfield_reference_win_rate
        return self.settings.strategy_auto_evolve_greenfield_reference_win_rate

    async def _rollback_promotion_batch(
        self,
        repo: PlatformRepository,
        promotion: Any,
        *,
        now: datetime,
        trigger: str,
    ) -> dict[str, Any]:
        restored: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        reactivated: dict[str, set[str]] = {}
        previous = dict(promotion.previous_city_assignments or {})
        for ticker, prev_state in previous.items():
            current = await repo.get_city_strategy_assignment(str(ticker), kalshi_env=self.settings.kalshi_env)
            current_name = current.strategy_name if current is not None else None
            if current_name != promotion.promoted_strategy_name:
                skipped.append({
                    "series_ticker": ticker,
                    "reason": "rollback_skipped_manual_override",
                    "current_strategy": current_name,
                    "expected_strategy": promotion.promoted_strategy_name,
                })
                continue
            prior_name = prev_state.get("strategy_name") if isinstance(prev_state, dict) else None
            if prior_name is None:
                await repo.delete_city_strategy_assignment(str(ticker), kalshi_env=self.settings.kalshi_env)
                await repo.record_city_assignment_event(
                    series_ticker=str(ticker),
                    previous_strategy=promotion.promoted_strategy_name,
                    new_strategy=None,
                    event_type="rollback_delete",
                    actor=AUTO_EVOLVE_SOURCE,
                    promotion_id=promotion.id,
                    kalshi_env=self.settings.kalshi_env,
                    note=f"Watchdog rollback {promotion.id}: {trigger}",
                )
                restored.append({"series_ticker": ticker, "restored_strategy": None})
                continue
            strategy = await repo.get_strategy_by_name(str(prior_name))
            if strategy is not None and not strategy.is_active:
                await repo.set_strategy_active(str(prior_name), is_active=True)
                reactivated.setdefault(str(prior_name), set()).add(str(ticker))
            await repo.set_city_strategy_assignment(
                str(ticker),
                str(prior_name),
                assigned_by="watchdog_rollback",
                kalshi_env=self.settings.kalshi_env,
            )
            await repo.record_city_assignment_event(
                series_ticker=str(ticker),
                previous_strategy=promotion.promoted_strategy_name,
                new_strategy=str(prior_name),
                event_type="rollback_restore",
                actor=AUTO_EVOLVE_SOURCE,
                promotion_id=promotion.id,
                kalshi_env=self.settings.kalshi_env,
                note=f"Watchdog rollback {promotion.id}: {trigger}",
            )
            restored.append({"series_ticker": ticker, "restored_strategy": prior_name})

        deactivated = None
        remaining = await repo.count_city_assignments_for_strategy(
            promotion.promoted_strategy_name,
            kalshi_env=self.settings.kalshi_env,
        )
        if remaining == 0:
            await repo.set_strategy_active(promotion.promoted_strategy_name, is_active=False)
            deactivated = promotion.promoted_strategy_name
        rollback_details = {
            "restored": restored,
            "skipped": skipped,
            "deactivated_strategy": deactivated,
            "reactivated_strategies": [
                {
                    "strategy_name": strategy_name,
                    "reason": "rollback_restore_assignment",
                    "series_tickers": sorted(tickers),
                }
                for strategy_name, tickers in reactivated.items()
            ],
        }
        await repo.update_strategy_promotion(promotion.id, rollback_skipped_cities=skipped)
        return rollback_details

    async def _ensure_fresh_regression(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        last_run_at = await self._checkpoint_time("strategy_regression")
        max_age = timedelta(seconds=max(3600, self.settings.strategy_regression_daily_run_seconds))
        regression_result = None
        refreshed = False
        if last_run_at is None or now - last_run_at > max_age:
            regression_result = await self.strategy_regression_service.run_regression()
            refreshed = True
            last_run_at = await self._checkpoint_time("strategy_regression")
        fresh = last_run_at is not None and now - last_run_at <= max_age
        return {
            "fresh": fresh,
            "last_run_at": last_run_at.isoformat() if last_run_at is not None else None,
            "refreshed": refreshed,
            "result": regression_result,
        }

    async def _trading_audit_summary(self) -> dict[str, Any]:
        if self.trading_audit_service is None:
            return {"available": False, "blocked": False, "reason": "service_unavailable"}
        report = await self.trading_audit_service.build_report(
            kalshi_env=self.settings.kalshi_env,
            days=7,
            focus="money-safety",
        )
        issues = list(report.get("issues") or [])
        blockers = [
            issue for issue in issues
            if str(issue.get("severity") or "").lower() in {"critical", "high"}
        ]
        return {
            "available": True,
            "blocked": bool(blockers),
            "blocker_count": len(blockers),
            "issue_count": len(issues),
            "issues": [
                {
                    "severity": issue.get("severity"),
                    "code": issue.get("code"),
                    "summary": issue.get("summary"),
                }
                for issue in issues[:20]
            ],
            "counts": report.get("counts", {}),
            "pnl": report.get("pnl", {}),
            "attribution": report.get("attribution", {}),
            "execution_funnel": report.get("execution_funnel", {}),
            "stop_loss": {
                "event_count": (report.get("stop_loss") or {}).get("event_count"),
                "clusters": (report.get("stop_loss") or {}).get("clusters", [])[:5],
            },
        }

    async def _trade_analysis_summary(self) -> dict[str, Any]:
        if self.trade_analysis_service is None:
            return {"available": False, "reason": "service_unavailable"}
        try:
            return await self.trade_analysis_service.summary_for_auto_evolve(
                kalshi_env=self.settings.kalshi_env,
                days=self.settings.strategy_auto_evolve_window_days,
            )
        except Exception as exc:
            return {"available": False, "reason": "summary_failed", "error": str(exc)}

    async def _checkpoint_time(self, stream_name: str) -> datetime | None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            checkpoint = await repo.get_checkpoint(stream_name)
            await session.commit()
        payload = checkpoint.payload if checkpoint is not None and isinstance(checkpoint.payload, dict) else {}
        raw = payload.get("ran_at") or payload.get("reconciled_at") or payload.get("followed_at")
        if not isinstance(raw, str) or not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    async def _active_deployment_color(self) -> str | None:
        try:
            async with self.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control(kalshi_env=self.settings.kalshi_env)
                await session.commit()
            return str(control.active_color)
        except Exception:
            logger.warning("auto-evolve active-color check failed; failing closed", exc_info=True)
            return "unknown"

    async def _get_checkpoint_payload(self) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            checkpoint = await repo.get_checkpoint(self.checkpoint_name)
            await session.commit()
        if checkpoint is None or not isinstance(checkpoint.payload, dict):
            return {}
        return dict(checkpoint.payload)

    async def _set_checkpoint(self, payload: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint(self.checkpoint_name, None, payload)
            await session.commit()

    async def _record_result(self, payload: dict[str, Any], *, severity: str, summary: str) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.set_checkpoint(self.checkpoint_name, None, payload)
            await repo.log_ops_event(
                severity=severity,
                summary=summary,
                source=AUTO_EVOLVE_SOURCE,
                payload={
                    "event_kind": AUTO_EVOLVE_EVENT_KIND,
                    **payload,
                },
            )
            await session.commit()

    async def _log_result_event(self, payload: dict[str, Any], *, severity: str, summary: str) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.log_ops_event(
                severity=severity,
                summary=summary,
                source=AUTO_EVOLVE_SOURCE,
                payload=payload,
            )
            await session.commit()

    def _base_payload(self, *, trigger_source: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        timezone = ZoneInfo(self.settings.strategy_codex_nightly_timezone)
        return {
            "ran_at": now.isoformat(),
            "local_date": now.astimezone(timezone).date().isoformat(),
            "trigger_source": trigger_source,
            "kalshi_env": self.settings.kalshi_env,
            "app_color": self.settings.app_color,
            "window_days": self.settings.strategy_auto_evolve_window_days,
            "enabled": bool(self.settings.strategy_auto_evolve_enabled),
        }

    @staticmethod
    def _suggestion_run(run_views: list[dict[str, Any]]) -> dict[str, Any] | None:
        return next((run for run in run_views if run.get("mode") == "suggest"), None)

    @staticmethod
    def _parse_datetime(raw: Any) -> datetime | None:
        if isinstance(raw, datetime):
            parsed = raw
        elif isinstance(raw, str) and raw:
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
        else:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _provider_summary(run_views: list[dict[str, Any]]) -> dict[str, Any]:
        first = next((run for run in run_views if run.get("provider") or run.get("model")), {})
        return {
            "provider": first.get("provider"),
            "model": first.get("model"),
        }
