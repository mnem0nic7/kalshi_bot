from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.strategy_regression import WINDOW_DAYS as DEFAULT_STRATEGY_WINDOW_DAYS

logger = logging.getLogger(__name__)

AUTO_EVOLVE_SOURCE = "strategy_auto_evolve"
AUTO_EVOLVE_EVENT_KIND = "auto_evolve"
AUTO_EVOLVE_ASSIGNED_BY = "auto_evolve"

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
        return f"daemon_strategy_auto_evolve:{self.settings.kalshi_env}:{self.settings.app_color}"

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

    async def _run_once(self, *, trigger_source: str) -> dict[str, Any]:
        payload = self._base_payload(trigger_source=trigger_source)
        if not self.settings.strategy_auto_evolve_enabled:
            payload.update({"status": "skipped", "reason": "disabled"})
            await self._record_result(payload, severity="info", summary="Strategy Auto-Evolve skipped: disabled")
            return payload

        previous = await self._get_checkpoint_payload()
        if previous.get("local_date") == payload["local_date"] and previous.get("status") in {
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
                await self._apply_eligible_assignments(assignment_snapshot)
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

        proposed_thresholds = dict(((suggestion.get("result") or {}).get("candidate") or {}).get("thresholds") or {})
        if proposed_thresholds:
            cap_error = await self._validate_delta_cap(proposed_thresholds)
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

    async def _validate_delta_cap(self, proposed: dict[str, Any]) -> dict[str, Any] | None:
        """Return an error dict if any numeric threshold field moves beyond the configured cap."""
        cap = self.settings.strategy_auto_evolve_max_threshold_delta_pct
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            active_strategies = await repo.list_strategies(active_only=True)
        if not active_strategies:
            return None
        # Use the most recently created active strategy as the baseline
        current_record = max(active_strategies, key=lambda s: s.created_at)
        current = current_record.thresholds or {}
        violations: list[dict[str, Any]] = []
        for field, ceiling in _THRESHOLD_FIELD_CEILING.items():
            raw_current = current.get(field)
            raw_proposed = proposed.get(field)
            if raw_current is None or raw_proposed is None:
                continue
            current_f = float(raw_current)
            proposed_f = float(raw_proposed)
            if current_f == 0.0:
                if ceiling == 0.0:
                    # Field can never be zero by schema; skip
                    continue
                # Zero-value: use schema ceiling as reference
                allowed_min = 0.0
                allowed_max = ceiling * cap
            else:
                ref = abs(current_f)
                allowed_min = current_f - ref * cap
                allowed_max = current_f + ref * cap
            if not (allowed_min <= proposed_f <= allowed_max):
                violations.append({
                    "field": field,
                    "current": current_f,
                    "proposed": proposed_f,
                    "allowed_min": round(allowed_min, 6),
                    "allowed_max": round(allowed_max, 6),
                })
        if violations:
            return {
                "stage": "accept",
                "reason": "delta_cap_exceeded",
                "cap_pct": cap,
                "violations": violations,
            }
        return None

    async def _apply_eligible_assignments(self, snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        changes: list[dict[str, Any]] = []
        skips: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        cycle_cap = self.settings.strategy_auto_evolve_max_cities_per_cycle
        rows = list(snapshot.get("city_matrix") or [])

        # Separate ineligible rows from eligible candidates
        eligible: list[dict[str, Any]] = []
        for row in rows:
            series_ticker = row.get("series_ticker")
            recommendation = dict(row.get("recommendation") or {})
            recommended_name = recommendation.get("strategy_name")
            previous_name = (row.get("assignment") or {}).get("strategy_name")
            if not row.get("approval_eligible") or not series_ticker or not recommended_name:
                skips.append({
                    "series_ticker": series_ticker,
                    "reason": "not_eligible",
                    "recommendation_status": recommendation.get("status"),
                })
                continue
            if previous_name == recommended_name:
                skips.append({
                    "series_ticker": series_ticker,
                    "reason": "already_matching",
                    "strategy_name": recommended_name,
                })
                continue
            eligible.append(row)

        # Sort by improvement vs current assignment (desc), then runner-up gap (desc), then ticker (asc)
        eligible.sort(
            key=lambda r: (
                -(r.get("gap_to_assignment") or 0.0),
                -(r.get("gap_to_runner_up") or 0.0),
                r.get("series_ticker") or "",
            )
        )

        # Apply per-cycle cap: defer remainder to next nightly run
        to_assign = eligible[:cycle_cap]
        for row in eligible[cycle_cap:]:
            recommendation = dict(row.get("recommendation") or {})
            skips.append({
                "series_ticker": row.get("series_ticker"),
                "reason": "cycle_cap_exceeded",
                "strategy_name": recommendation.get("strategy_name"),
                "recommendation_status": recommendation.get("status"),
            })

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            for row in to_assign:
                series_ticker = row.get("series_ticker")
                recommendation = dict(row.get("recommendation") or {})
                recommended_name = recommendation.get("strategy_name")
                previous_name = (row.get("assignment") or {}).get("strategy_name")
                try:
                    await repo.set_city_strategy_assignment(
                        str(series_ticker),
                        str(recommended_name),
                        assigned_by=AUTO_EVOLVE_ASSIGNED_BY,
                    )
                    changes.append({
                        "series_ticker": series_ticker,
                        "previous_strategy": previous_name,
                        "new_strategy": recommended_name,
                        "recommendation_status": recommendation.get("status"),
                        "recommendation_label": recommendation.get("label"),
                        "gap_to_runner_up": row.get("gap_to_runner_up"),
                        "gap_to_assignment": row.get("gap_to_assignment"),
                    })
                except Exception as exc:
                    errors.append({"stage": "assign", "series_ticker": series_ticker, "error": str(exc)})
            await session.commit()

        if changes and self.secondary_session_factory is not None:
            try:
                async with self.secondary_session_factory() as session:
                    repo = PlatformRepository(session)
                    for change in changes:
                        await repo.set_city_strategy_assignment(
                            str(change["series_ticker"]),
                            str(change["new_strategy"]),
                            assigned_by=AUTO_EVOLVE_ASSIGNED_BY,
                        )
                    await session.commit()
            except Exception as exc:
                errors.append({"stage": "assign_secondary", "error": str(exc), "changes_attempted": len(changes)})
        return {"changes": changes, "skips": skips, "errors": errors}

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
        return {
            "fresh": last_run_at is not None,
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
    def _provider_summary(run_views: list[dict[str, Any]]) -> dict[str, Any]:
        first = next((run for run in run_views if run.get("provider") or run.get("model")), {})
        return {
            "provider": first.get("provider"),
            "model": first.get("model"),
        }
