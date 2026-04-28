from __future__ import annotations

import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.fixed_point import quantize_price
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.fee_model import current_fee_model_version, estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.trading_audit import TradingAuditService


SOURCE_HISTORICAL_REPLAY = "historical-replay"
SUPPORT_LEVELS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("L1_station_season_lead_regime", ("station_id", "season_bucket", "lead_bucket", "trade_regime")),
    ("L2_station_season_lead", ("station_id", "season_bucket", "lead_bucket")),
    ("L3_station_season", ("station_id", "season_bucket")),
    ("L4_season_lead", ("season_bucket", "lead_bucket")),
    ("L5_global", ()),
)
SUPPORTED_N = 100
SUPPORTED_MARKET_DAYS = 20
SUPPORTED_RECENCY_DAYS = 365
EXPLORATORY_N = 30
EXPLORATORY_MARKET_DAYS = 10
AUTO_PROMOTION_TRIGGER_NEW_RESOLVED_ROOMS = 50
AUTO_PROMOTION_MIN_INTERVAL = timedelta(days=7)
AUTO_PROMOTION_MIN_RESOLVED_ROOMS = 30
AUTO_PROMOTION_MIN_SETTLEMENT_COVERAGE = 0.95
AUTO_PROMOTION_MIN_STRATEGY_CODE_ATTRIBUTION = 0.99
AUTO_PROMOTION_SETTLEMENT_LAG_DAYS = 2
ATTRIBUTION_AUDIT_CODES = {
    "missing_fill_strategy_attribution",
    "unlinked_fills_with_recoverable_order_attribution",
}
AUTO_PROMOTION_ALLOWED_SOURCE_PROVENANCE = {"historical_replay_full_checkpoint"}


@dataclass(slots=True)
class _SupportStats:
    n: int
    market_days: int
    recency_days: int | None


def _as_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except (TypeError, ValueError):
        return None


def _date_from_local_day(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def season_bucket_for_day(local_market_day: str) -> str:
    local_day = _date_from_local_day(local_market_day)
    month = local_day.month if local_day is not None else 1
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "fall"


def lead_bucket_for_minutes(minutes: int | None) -> str:
    if minutes is None:
        return "unknown"
    hours = minutes / 60
    if hours < 2:
        return "imminent"
    if hours < 6:
        return "near"
    if hours < 12:
        return "mid"
    if hours <= 24:
        return "far"
    return "multi_day"


class DecisionCorpusService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        trading_audit_service: Any | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.trading_audit_service = trading_audit_service or TradingAuditService(settings, session_factory)

    async def build(
        self,
        *,
        date_from: date,
        date_to: date,
        source: str = SOURCE_HISTORICAL_REPLAY,
        dry_run: bool = False,
        notes: str | None = None,
        parent_build_id: str | None = None,
        kalshi_env: str | None = None,
    ) -> dict[str, Any]:
        if source != SOURCE_HISTORICAL_REPLAY:
            raise ValueError("PR1 decision corpus only supports --source historical-replay")
        if date_from > date_to:
            raise ValueError("date_from must be <= date_to")

        version = self._build_version()
        filters = {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "source": source,
            "requires_settlement_label": True,
        }
        if kalshi_env is not None:
            filters["kalshi_env"] = kalshi_env
        source_payload = {"type": "historical_replay_rooms", "kalshi_env": kalshi_env}

        rows = await self._build_candidate_rows(date_from=date_from, date_to=date_to, kalshi_env=kalshi_env)
        rows = self._apply_support_labels(rows, reference_date=date_to)
        if not rows:
            raise ValueError("no eligible historical replay decisions found for decision corpus build")

        if dry_run:
            return {
                "status": "dry_run",
                "version": version,
                "row_count": len(rows),
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "kalshi_env": kalshi_env,
                "support_distribution": self._support_distribution(rows),
            }

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            build = await repo.create_decision_corpus_build(
                version=version,
                date_from=date_from,
                date_to=date_to,
                source=source_payload,
                filters=filters,
                git_sha=self._git_sha(),
                parent_build_id=parent_build_id,
                notes=notes,
            )
            await session.commit()
            try:
                for row in rows:
                    row["corpus_build_id"] = build.id
                    await repo.add_decision_corpus_row(**row)
                await repo.mark_decision_corpus_build_successful(build.id, row_count=len(rows))
                await repo.log_ops_event(
                    severity="info",
                    summary=f"Decision corpus build completed with {len(rows)} rows",
                    source="decision_corpus",
                    payload={
                        "event_kind": "decision_corpus_build_completed",
                        "build_id": build.id,
                        "version": version,
                        "row_count": len(rows),
                        "date_from": date_from.isoformat(),
                        "date_to": date_to.isoformat(),
                    },
                )
                await session.commit()
                return {
                    "status": "successful",
                    "build_id": build.id,
                    "version": version,
                    "row_count": len(rows),
                    "support_distribution": self._support_distribution(rows),
                }
            except Exception as exc:
                await session.rollback()
                async with self.session_factory() as failed_session:
                    failed_repo = PlatformRepository(failed_session)
                    await failed_repo.mark_decision_corpus_build_failed(
                        build.id,
                        failure_reason=str(exc),
                        row_count=len(rows),
                    )
                    await failed_repo.log_ops_event(
                        severity="error",
                        summary="Decision corpus build failed",
                        source="decision_corpus",
                        payload={
                            "event_kind": "decision_corpus_build_failed",
                            "build_id": build.id,
                            "version": version,
                            "failure_reason": str(exc),
                        },
                    )
                    await failed_session.commit()
                raise

    async def list_builds(
        self,
        *,
        status: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            builds = await repo.list_decision_corpus_builds(
                status=status,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
            )
        return [self._build_to_dict(build) for build in builds]

    async def inspect_build(self, build_id: str, *, sample_limit: int = 5) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            build = await repo.get_decision_corpus_build(build_id)
            if build is None:
                raise KeyError(f"Decision corpus build {build_id} not found")
            rows = await repo.list_decision_corpus_rows(build_id=build_id)
        by_station: dict[str, int] = defaultdict(int)
        by_day: dict[str, int] = defaultdict(int)
        by_regime: dict[str, int] = defaultdict(int)
        by_support: dict[str, int] = defaultdict(int)
        by_source_provenance: dict[str, int] = defaultdict(int)
        by_coverage_class: dict[str, int] = defaultdict(int)
        by_market_source_kind: dict[str, int] = defaultdict(int)
        by_weather_source_kind: dict[str, int] = defaultdict(int)
        support_by_provenance: dict[str, int] = defaultdict(int)
        clean_primary_rows = 0
        clean_primary_days: set[str] = set()
        degraded_rows = 0
        for row in rows:
            by_station[row.station_id or row.series_ticker or "unknown"] += 1
            by_day[row.local_market_day] += 1
            by_regime[row.trade_regime or "unknown"] += 1
            by_support[row.support_status] += 1
            source_provenance = row.source_provenance or "unknown"
            source_details = row.source_details or {}
            by_source_provenance[source_provenance] += 1
            by_coverage_class[str(source_details.get("coverage_class") or "unknown")] += 1
            by_market_source_kind[str(source_details.get("market_source_kind") or "unknown")] += 1
            by_weather_source_kind[str(source_details.get("weather_source_kind") or "unknown")] += 1
            support_by_provenance[f"{source_provenance}:{row.support_status or 'unknown'}"] += 1
            if source_provenance in {
                "historical_replay_full_checkpoint",
                "historical_replay_partial_checkpoint",
                "historical_replay_late_only",
            } and row.support_status in {"supported", "exploratory"}:
                clean_primary_rows += 1
                clean_primary_days.add(row.local_market_day)
            if source_provenance in {
                "historical_replay_external_forecast_repair",
                "historical_replay_unknown",
            }:
                degraded_rows += 1
        return {
            "build": self._build_to_dict(build),
            "row_count": len(rows),
            "by_station": dict(sorted(by_station.items())),
            "by_local_market_day": dict(sorted(by_day.items())),
            "by_trade_regime": dict(sorted(by_regime.items())),
            "by_support_status": dict(sorted(by_support.items())),
            "source_diagnostics": {
                "by_source_provenance": dict(sorted(by_source_provenance.items())),
                "by_coverage_class": dict(sorted(by_coverage_class.items())),
                "by_market_source_kind": dict(sorted(by_market_source_kind.items())),
                "by_weather_source_kind": dict(sorted(by_weather_source_kind.items())),
                "support_status_by_provenance": dict(sorted(support_by_provenance.items())),
                "clean_primary_rows": clean_primary_rows,
                "clean_primary_market_days": len(clean_primary_days),
                "degraded_rows": degraded_rows,
                "gap_to_exploratory": {
                    "clean_primary_rows": max(0, EXPLORATORY_N - clean_primary_rows),
                    "clean_primary_market_days": max(0, EXPLORATORY_MARKET_DAYS - len(clean_primary_days)),
                },
                "gap_to_supported": {
                    "clean_primary_rows": max(0, SUPPORTED_N - clean_primary_rows),
                    "clean_primary_market_days": max(0, SUPPORTED_MARKET_DAYS - len(clean_primary_days)),
                },
                "next_check": (
                    "Run `kalshi-bot-cli historical-status --verbose` before promoting or rebuilding "
                    "to confirm source archive freshness and replay coverage gaps."
                ),
            },
            "samples": [self._row_sample(row) for row in rows[:sample_limit]],
        }

    async def validate_build(self, build_id: str) -> dict[str, Any]:
        errors: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            build = await repo.get_decision_corpus_build(build_id)
            if build is None:
                raise KeyError(f"Decision corpus build {build_id} not found")
            rows = await repo.list_decision_corpus_rows(build_id=build_id)

        seen: set[tuple[Any, ...]] = set()
        for row in rows:
            identity = (row.room_id, row.market_ticker, row.checkpoint_ts, row.policy_version, row.model_version)
            if identity in seen:
                errors.append({"row_id": row.id, "code": "duplicate_identity"})
            seen.add(identity)
            for column in ("room_id", "market_ticker", "local_market_day", "checkpoint_ts", "kalshi_env", "support_status", "support_level", "source_provenance"):
                if getattr(row, column) in (None, ""):
                    errors.append({"row_id": row.id, "code": "missing_required", "column": column})
            expected = self._expected_target_pnl(row)
            if expected is not None and row.pnl_counterfactual_target_frictionless != expected:
                errors.append(
                    {
                        "row_id": row.id,
                        "code": "target_pnl_mismatch",
                        "expected": str(expected),
                        "actual": str(row.pnl_counterfactual_target_frictionless),
                    }
                )
        return {"ok": not errors, "build_id": build_id, "row_count": len(rows), "errors": errors}

    async def promote(self, build_id: str, *, kalshi_env: str, actor: str | None = None) -> dict[str, Any]:
        validation = await self.validate_build(build_id)
        if not validation["ok"]:
            raise ValueError(
                {
                    "status": "rejected",
                    "reason": "validation_failed",
                    "build_id": build_id,
                    "validation": validation,
                }
            )

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            env = repo._resolved_kalshi_env(kalshi_env)
            build = await repo.get_decision_corpus_build(build_id)
            if build is None:
                raise KeyError(f"Decision corpus build {build_id} not found")
            if build.status != "successful":
                raise ValueError("Only successful decision corpus builds can be promoted")
            rows = await repo.list_decision_corpus_rows(build_id=build_id)
            build_envs = self._build_kalshi_envs(build.source or {}, build.filters or {})
            row_envs = sorted({row.kalshi_env for row in rows if row.kalshi_env not in (None, "")})
            mismatched_metadata_envs = sorted(env_value for env_value in build_envs if env_value != env)
            mismatched_row_envs = sorted(row_env for row_env in row_envs if row_env != env)
            if mismatched_metadata_envs or mismatched_row_envs:
                raise ValueError(
                    {
                        "status": "rejected",
                        "reason": "kalshi_env_mismatch",
                        "build_id": build_id,
                        "target_kalshi_env": env,
                        "build_kalshi_envs": sorted(build_envs),
                        "row_kalshi_envs": row_envs,
                    }
                )
            metrics = await repo.decision_corpus_promotion_source_metrics(
                date_from=build.date_from.isoformat(),
                date_to=build.date_to.isoformat(),
                kalshi_env=env,
            )
            excluded_windows = await self._excluded_date_windows(
                repo,
                kalshi_env=env,
                date_from=build.date_from,
                date_to=build.date_to,
            )
            await session.commit()

        provenance_metrics = await self._source_provenance_metrics(
            date_from=date.fromisoformat(metrics["date_from"]),
            date_to=date.fromisoformat(metrics["date_to"]),
            kalshi_env=metrics["kalshi_env"],
        )
        gates = await self._promotion_gates(
            kalshi_env=metrics["kalshi_env"],
            now=datetime.now(UTC),
            metrics=metrics,
            excluded_windows=excluded_windows,
            provenance_metrics=provenance_metrics,
        )
        if not gates["ok"]:
            raise ValueError(
                {
                    "status": "rejected",
                    "reason": "promotion_gates_failed",
                    "build_id": build_id,
                    "kalshi_env": metrics["kalshi_env"],
                    "metrics": metrics,
                    "gates": gates,
                }
            )

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            result = await repo.promote_decision_corpus_build(
                build_id,
                kalshi_env=kalshi_env,
                actor=actor,
            )
            await session.commit()
            return result

    async def nightly_auto_promote(
        self,
        *,
        kalshi_env: str,
        now: datetime | None = None,
        actor: str = "daemon",
    ) -> dict[str, Any]:
        now = _as_datetime(now) or datetime.now(UTC)
        date_to = now.date() - timedelta(days=AUTO_PROMOTION_SETTLEMENT_LAG_DAYS)
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            env = repo._resolved_kalshi_env(kalshi_env)
            bounds = await repo.decision_corpus_replay_date_bounds(
                kalshi_env=env,
                date_to=date_to.isoformat(),
            )
            if bounds is None:
                return {"status": "skipped", "reason": "no_replay_source", "kalshi_env": env}
            current_checkpoint = await repo.get_checkpoint(repo.decision_corpus_current_checkpoint_name(kalshi_env=env))
            current = await repo.get_current_decision_corpus_build(kalshi_env=env)
            last_promoted_at = self._last_promoted_at(current_checkpoint, current)
            new_after_date = current.date_to.isoformat() if current is not None else None
            metrics = await repo.decision_corpus_promotion_source_metrics(
                date_from=bounds["date_from"],
                date_to=bounds["date_to"],
                kalshi_env=env,
                new_after_date=new_after_date,
            )
            excluded_windows = await self._excluded_date_windows(
                repo,
                kalshi_env=env,
                date_from=date.fromisoformat(bounds["date_from"]),
                date_to=date.fromisoformat(bounds["date_to"]),
            )
            await session.commit()

        provenance_metrics = await self._source_provenance_metrics(
            date_from=date.fromisoformat(bounds["date_from"]),
            date_to=date.fromisoformat(bounds["date_to"]),
            kalshi_env=env,
        )

        trigger = {
            **metrics,
            "source_provenance": provenance_metrics,
            "current_build_id": current.id if current is not None else None,
            "current_date_to": current.date_to.isoformat() if current is not None else None,
            "last_promoted_at": last_promoted_at.isoformat() if last_promoted_at is not None else None,
            "min_new_resolved_rooms": AUTO_PROMOTION_TRIGGER_NEW_RESOLVED_ROOMS,
            "min_interval_days": AUTO_PROMOTION_MIN_INTERVAL.days,
        }
        if last_promoted_at is not None and now - last_promoted_at < AUTO_PROMOTION_MIN_INTERVAL:
            return {"status": "skipped", "reason": "promotion_interval", "trigger": trigger}
        if metrics["new_resolved_rooms"] < AUTO_PROMOTION_TRIGGER_NEW_RESOLVED_ROOMS:
            return {"status": "skipped", "reason": "new_resolved_rooms", "trigger": trigger}
        if provenance_metrics["total_rows"] > 0 and provenance_metrics["allowed_rows"] == 0:
            return {
                "status": "skipped",
                "reason": "source_provenance",
                "kalshi_env": env,
                "trigger": trigger,
            }

        gates = await self._promotion_gates(
            kalshi_env=env,
            now=now,
            metrics=metrics,
            excluded_windows=excluded_windows,
            provenance_metrics=provenance_metrics,
        )
        if not gates["ok"]:
            payload = {
                "event_kind": "decision_corpus_auto_promotion_failed",
                "status": "failed",
                "reason": "promotion_gates_failed",
                "kalshi_env": env,
                "trigger": trigger,
                "gates": gates,
                "retained_build_id": current.id if current is not None else None,
            }
            await self._log_auto_promotion_event(
                severity="warning",
                summary=f"Decision corpus auto-promotion failed gates for {env}",
                payload=payload,
            )
            return payload

        try:
            build_result = await self.build(
                date_from=date.fromisoformat(bounds["date_from"]),
                date_to=date.fromisoformat(bounds["date_to"]),
                source=SOURCE_HISTORICAL_REPLAY,
                notes=f"nightly_auto_promotion:{env}",
                parent_build_id=current.id if current is not None else None,
                kalshi_env=env,
            )
            validation = await self.validate_build(build_result["build_id"])
            if not validation["ok"]:
                payload = {
                    "event_kind": "decision_corpus_auto_promotion_failed",
                    "status": "failed",
                    "reason": "validation_failed",
                    "kalshi_env": env,
                    "trigger": trigger,
                    "gates": gates,
                    "build": build_result,
                    "validation": validation,
                    "retained_build_id": current.id if current is not None else None,
                }
                await self._log_auto_promotion_event(
                    severity="warning",
                    summary=f"Decision corpus auto-promotion validation failed for {env}",
                    payload=payload,
                )
                return payload
            promotion = await self.promote(build_result["build_id"], kalshi_env=env, actor=actor)
            return {
                "event_kind": "decision_corpus_auto_promotion_completed",
                "status": "promoted",
                "kalshi_env": env,
                "trigger": trigger,
                "gates": gates,
                "build": build_result,
                "validation": validation,
                "promotion": promotion,
            }
        except Exception as exc:
            payload = {
                "event_kind": "decision_corpus_auto_promotion_failed",
                "status": "failed",
                "reason": "promotion_error",
                "error": str(exc),
                "kalshi_env": env,
                "trigger": trigger,
                "gates": gates,
                "retained_build_id": current.id if current is not None else None,
            }
            await self._log_auto_promotion_event(
                severity="error",
                summary=f"Decision corpus auto-promotion failed for {env}",
                payload=payload,
            )
            return payload

    async def current(self, *, kalshi_env: str) -> dict[str, Any]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            build = await repo.get_current_decision_corpus_build(kalshi_env=kalshi_env)
            if build is None:
                return {"status": "missing", "kalshi_env": kalshi_env, "build": None}
            return {"status": "ok", "kalshi_env": kalshi_env, "build": self._build_to_dict(build)}

    async def _promotion_gates(
        self,
        *,
        kalshi_env: str,
        now: datetime,
        metrics: dict[str, Any],
        excluded_windows: list[dict[str, Any]],
        provenance_metrics: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        audit_unavailable: str | None = None
        try:
            audit_report = await self.trading_audit_service.build_report(
                kalshi_env=kalshi_env,
                days=7,
                focus="money-safety",
                now=now,
            )
        except Exception as exc:
            audit_report = {}
            audit_unavailable = str(exc)
        total_fills = int(audit_report.get("fill_summary", {}).get("total_fills") or 0)
        missing_fills = int(audit_report.get("attribution", {}).get("missing_fill_strategy_count") or 0)
        strategy_code_attribution = 1.0 if total_fills == 0 else (total_fills - missing_fills) / total_fills
        audit_blockers = [
            {
                "severity": issue.get("severity"),
                "code": issue.get("code"),
                "summary": issue.get("summary"),
            }
            for issue in audit_report.get("issues", [])
            if str(issue.get("severity") or "").lower() in {"critical", "high"}
            and issue.get("code") not in ATTRIBUTION_AUDIT_CODES
        ]
        if audit_unavailable is not None:
            audit_blockers.append(
                {
                    "severity": "high",
                    "code": "trading_audit_unavailable",
                    "summary": audit_unavailable,
                }
            )
        if provenance_metrics is None:
            provenance_metrics = {
                "total_rows": int(metrics.get("resolved_rooms") or 0),
                "allowed_rows": int(metrics.get("resolved_rooms") or 0),
                "disallowed_rows": 0,
                "by_source_provenance": {},
                "disallowed_by_source_provenance": {},
                "not_evaluated": True,
            }
        checks = {
            "resolved_rooms": {
                "ok": metrics["resolved_rooms"] >= AUTO_PROMOTION_MIN_RESOLVED_ROOMS,
                "actual": metrics["resolved_rooms"],
                "minimum": AUTO_PROMOTION_MIN_RESOLVED_ROOMS,
            },
            "settlement_coverage": {
                "ok": metrics["settlement_coverage"] >= AUTO_PROMOTION_MIN_SETTLEMENT_COVERAGE,
                "actual": metrics["settlement_coverage"],
                "minimum": AUTO_PROMOTION_MIN_SETTLEMENT_COVERAGE,
            },
            "strategy_code_attribution": {
                "ok": strategy_code_attribution >= AUTO_PROMOTION_MIN_STRATEGY_CODE_ATTRIBUTION,
                "actual": strategy_code_attribution,
                "minimum": AUTO_PROMOTION_MIN_STRATEGY_CODE_ATTRIBUTION,
                "total_fills": total_fills,
                "missing_fill_strategy_count": missing_fills,
            },
            "excluded_date_windows": {
                "ok": not excluded_windows,
                "windows": excluded_windows,
            },
            "source_provenance": {
                "ok": provenance_metrics["total_rows"] > 0 and provenance_metrics["disallowed_rows"] == 0,
                **provenance_metrics,
                "allowed": sorted(AUTO_PROMOTION_ALLOWED_SOURCE_PROVENANCE),
            },
            "audit_blockers": {
                "ok": not audit_blockers,
                "blockers": audit_blockers,
            },
        }
        failed = [name for name, check in checks.items() if not check["ok"]]
        return {"ok": not failed, "failed": failed, "checks": checks}

    async def _excluded_date_windows(
        self,
        repo: PlatformRepository,
        *,
        kalshi_env: str,
        date_from: date,
        date_to: date,
    ) -> list[dict[str, Any]]:
        checkpoint = await repo.get_checkpoint(f"decision_corpus_excluded_date_windows:{kalshi_env}")
        payload = checkpoint.payload if checkpoint is not None and isinstance(checkpoint.payload, dict) else {}
        candidates = payload.get("windows") if isinstance(payload.get("windows"), list) else []
        overlaps: list[dict[str, Any]] = []
        for start, end in self._configured_excluded_date_ranges():
            if start <= date_to and end >= date_from:
                overlaps.append(
                    {
                        "date_from": start.isoformat(),
                        "date_to": end.isoformat(),
                        "reason": "strategy_corpus_excluded_date_ranges",
                    }
                )
        for item in candidates:
            if not isinstance(item, dict) or item.get("active") is False:
                continue
            start = self._date_from_window_value(item.get("date_from") or item.get("start"))
            end = self._date_from_window_value(item.get("date_to") or item.get("end")) or start
            if start is None or end is None:
                continue
            if start <= date_to and end >= date_from:
                overlaps.append(
                    {
                        "date_from": start.isoformat(),
                        "date_to": end.isoformat(),
                        "reason": item.get("reason"),
                    }
                )
        return overlaps

    def _configured_excluded_date_ranges(self) -> list[tuple[date, date]]:
        ranges: list[tuple[date, date]] = []
        raw = self.settings.strategy_corpus_excluded_date_ranges.strip()
        if not raw:
            return ranges
        for raw_range in raw.split(","):
            start_raw, end_raw = [part.strip() for part in raw_range.strip().split("/", 1)]
            ranges.append((date.fromisoformat(start_raw), date.fromisoformat(end_raw)))
        return ranges

    async def _source_provenance_metrics(
        self,
        *,
        date_from: date,
        date_to: date,
        kalshi_env: str,
    ) -> dict[str, Any]:
        rows = await self._build_candidate_rows(date_from=date_from, date_to=date_to, kalshi_env=kalshi_env)
        by_source: dict[str, int] = defaultdict(int)
        for row in rows:
            by_source[str(row.get("source_provenance") or "unknown")] += 1
        disallowed = {
            source: count
            for source, count in sorted(by_source.items())
            if source not in AUTO_PROMOTION_ALLOWED_SOURCE_PROVENANCE
        }
        allowed_rows = sum(
            count for source, count in by_source.items() if source in AUTO_PROMOTION_ALLOWED_SOURCE_PROVENANCE
        )
        return {
            "total_rows": len(rows),
            "allowed_rows": allowed_rows,
            "disallowed_rows": sum(disallowed.values()),
            "by_source_provenance": dict(sorted(by_source.items())),
            "disallowed_by_source_provenance": disallowed,
        }

    async def _log_auto_promotion_event(self, *, severity: str, summary: str, payload: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.log_ops_event(
                severity=severity,
                summary=summary,
                source="decision_corpus",
                kalshi_env=payload.get("kalshi_env"),
                payload=payload,
            )
            await session.commit()

    @staticmethod
    def _build_kalshi_envs(source: dict[str, Any], filters: dict[str, Any]) -> set[str]:
        envs: set[str] = set()
        for payload in (source, filters):
            value = payload.get("kalshi_env") if isinstance(payload, dict) else None
            if value not in (None, ""):
                envs.add(str(value))
        return envs

    @staticmethod
    def _date_from_window_value(value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if value in (None, ""):
            return None
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            return None

    @staticmethod
    def _last_promoted_at(checkpoint: Any | None, current: Any | None) -> datetime | None:
        payload = checkpoint.payload if checkpoint is not None and isinstance(checkpoint.payload, dict) else {}
        promoted_at = _as_datetime(payload.get("promoted_at"))
        if promoted_at is not None:
            return promoted_at
        for value in (
            getattr(current, "finished_at", None),
            getattr(current, "updated_at", None),
            getattr(current, "created_at", None),
        ):
            parsed = _as_datetime(value)
            if parsed is not None:
                return parsed
        return None

    async def _build_candidate_rows(
        self,
        *,
        date_from: date,
        date_to: date,
        kalshi_env: str | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            runs = await repo.list_historical_replay_runs(
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                status="completed",
                kalshi_env=kalshi_env,
                limit=1_000_000,
            )
            for run in runs:
                if not run.room_id:
                    continue
                settlement = await repo.get_historical_settlement_label(run.market_ticker)
                if settlement is None or settlement.kalshi_result not in {"yes", "no"}:
                    continue
                room = await repo.get_room(run.room_id)
                signal = await repo.get_latest_signal_for_room(run.room_id)
                if room is None or signal is None:
                    continue
                ticket = await repo.get_latest_trade_ticket_for_room(run.room_id)
                market_artifact = await repo.get_latest_artifact(room_id=run.room_id, artifact_type="market_snapshot")
                weather_artifact = await repo.get_latest_artifact(room_id=run.room_id, artifact_type="weather_bundle")
                rows.append(
                    self._row_from_source(
                        run=run,
                        room=room,
                        signal=signal,
                        ticket=ticket,
                        settlement=settlement,
                        market_snapshot=(market_artifact.payload if market_artifact is not None else {}),
                        weather_bundle=(weather_artifact.payload if weather_artifact is not None else {}),
                    )
                )
        rows.sort(key=lambda row: (row["local_market_day"], row["checkpoint_ts"].isoformat(), row["market_ticker"], row["room_id"]))
        return rows

    def _row_from_source(
        self,
        *,
        run: Any,
        room: Any,
        signal: Any,
        ticket: Any | None,
        settlement: Any,
        market_snapshot: dict[str, Any],
        weather_bundle: dict[str, Any],
    ) -> dict[str, Any]:
        signal_payload = dict(signal.payload or {})
        historical_provenance = dict((run.payload or {}).get("historical_provenance") or {})
        mapping = self._mapping_payload(market_snapshot, weather_bundle)
        station_id = mapping.get("station_id") or historical_provenance.get("station_id")
        timezone_name = mapping.get("timezone_name") or historical_provenance.get("timezone_name") or "UTC"
        target_yes, recommended_side = self._target_and_side(signal_payload=signal_payload, ticket=ticket)
        eligibility = signal_payload.get("eligibility") if isinstance(signal_payload.get("eligibility"), dict) else {}
        time_to_settlement = self._time_to_settlement_minutes(
            checkpoint_ts=run.checkpoint_ts,
            local_market_day=run.local_market_day,
            timezone_name=timezone_name,
            settlement_ts=settlement.settlement_ts,
        )
        quote_observed_at = self._quote_observed_at(market_snapshot)
        fair_yes = quantize_price(signal.fair_yes_dollars) if signal.fair_yes_dollars is not None else None
        pnls = self._pnl_fields(
            recommended_side=recommended_side,
            target_yes_price=target_yes,
            fair_yes=fair_yes,
            settlement_result=settlement.kalshi_result,
        )
        source_provenance, source_details = self._source_provenance(run.payload or {})
        forecast_delta_f = _as_float(signal_payload.get("forecast_delta_f") or eligibility.get("forecast_delta_f"))
        abs_forecast_delta_f = abs(forecast_delta_f) if forecast_delta_f is not None else None
        forecast_delta_gap_f = (
            round(max(0.0, float(self.settings.strategy_min_abs_delta_f) - abs_forecast_delta_f), 2)
            if abs_forecast_delta_f is not None
            else None
        )
        return {
            "room_id": room.id,
            "market_ticker": run.market_ticker,
            "series_ticker": run.series_ticker,
            "station_id": station_id,
            "local_market_day": run.local_market_day,
            "checkpoint_ts": run.checkpoint_ts,
            "kalshi_env": room.kalshi_env,
            "deployment_color": room.active_color,
            "model_version": str(signal_payload.get("agent_pack_version") or room.agent_pack_version or run.agent_pack_version or "unknown"),
            "policy_version": str(signal_payload.get("heuristic_pack_version") or signal_payload.get("agent_pack_version") or room.agent_pack_version or "unknown"),
            "source_asof_ts": _as_datetime(historical_provenance.get("asof_ts")) or run.checkpoint_ts,
            "quote_observed_at": quote_observed_at,
            "quote_captured_at": quote_observed_at or run.checkpoint_ts,
            "time_to_settlement_at_checkpoint_minutes": time_to_settlement,
            "fair_yes_dollars": fair_yes,
            "confidence": float(signal.confidence) if signal.confidence is not None else None,
            "edge_bps": signal.edge_bps,
            "recommended_side": recommended_side,
            "target_yes_price_dollars": target_yes,
            "eligibility_status": "eligible" if bool(eligibility.get("eligible")) else "blocked",
            "stand_down_reason": signal_payload.get("stand_down_reason") or eligibility.get("stand_down_reason"),
            "trade_regime": signal_payload.get("trade_regime") or "unknown",
            "liquidity_regime": self._liquidity_regime(market_snapshot),
            "support_status": "insufficient",
            "support_level": "L5_global",
            "support_n": 0,
            "support_market_days": 0,
            "support_recency_days": None,
            "backoff_path": [],
            "settlement_result": settlement.kalshi_result,
            "settlement_value_dollars": settlement.settlement_value_dollars,
            **pnls,
            "counterfactual_count": Decimal("1.00") if pnls["pnl_counterfactual_target_frictionless"] is not None else None,
            "executed_count": None,
            "fee_model_version": current_fee_model_version() if pnls["fee_counterfactual_dollars"] is not None else None,
            "source_provenance": source_provenance,
            "source_details": source_details,
            "signal_payload": signal_payload,
            "quote_snapshot": market_snapshot,
            "settlement_payload": {
                "market_ticker": settlement.market_ticker,
                "kalshi_result": settlement.kalshi_result,
                "settlement_value_dollars": str(settlement.settlement_value_dollars) if settlement.settlement_value_dollars is not None else None,
                "settlement_ts": settlement.settlement_ts.isoformat() if settlement.settlement_ts is not None else None,
                "crosscheck_status": settlement.crosscheck_status,
                "payload": settlement.payload or {},
            },
            "diagnostics": {
                "historical_replay_run_id": run.id,
                "checkpoint_label": run.checkpoint_label,
                "season_bucket": season_bucket_for_day(run.local_market_day),
                "lead_bucket": lead_bucket_for_minutes(time_to_settlement),
                "forecast_delta_f": forecast_delta_f,
                "abs_forecast_delta_f": abs_forecast_delta_f,
                "configured_min_abs_delta_f": float(self.settings.strategy_min_abs_delta_f),
                "forecast_delta_gap_f": forecast_delta_gap_f,
            },
        }

    def _pnl_fields(
        self,
        *,
        recommended_side: str | None,
        target_yes_price: Decimal | None,
        fair_yes: Decimal | None,
        settlement_result: str | None,
    ) -> dict[str, Decimal | None]:
        payload: dict[str, Decimal | None] = {
            "pnl_counterfactual_target_frictionless": None,
            "pnl_counterfactual_target_with_fees": None,
            "pnl_model_fair_frictionless": None,
            "pnl_executed_realized": None,
            "fee_counterfactual_dollars": None,
        }
        if settlement_result not in {"yes", "no"}:
            return payload
        outcome_yes = Decimal("1") if settlement_result == "yes" else Decimal("0")
        side = (recommended_side or "").lower()
        if side in {"yes", "no"} and target_yes_price is not None:
            if side == "yes":
                target_pnl = outcome_yes - target_yes_price
                fee_price = target_yes_price
            else:
                target_pnl = (Decimal("1") - outcome_yes) - (Decimal("1") - target_yes_price)
                fee_price = Decimal("1") - target_yes_price
            fee = estimate_kalshi_taker_fee_dollars(
                price_dollars=fee_price,
                count=Decimal("1"),
                fee_rate=Decimal(str(self.settings.kalshi_taker_fee_rate)),
            )
            payload["pnl_counterfactual_target_frictionless"] = target_pnl.quantize(Decimal("0.000001"))
            payload["fee_counterfactual_dollars"] = fee.quantize(Decimal("0.000001"))
            payload["pnl_counterfactual_target_with_fees"] = (target_pnl - fee).quantize(Decimal("0.000001"))
        if side in {"yes", "no"} and fair_yes is not None:
            fair_pnl = outcome_yes - fair_yes if side == "yes" else (Decimal("1") - outcome_yes) - (Decimal("1") - fair_yes)
            payload["pnl_model_fair_frictionless"] = fair_pnl.quantize(Decimal("0.000001"))
        return payload

    def _target_and_side(self, *, signal_payload: dict[str, Any], ticket: Any | None) -> tuple[Decimal | None, str | None]:
        if ticket is not None:
            return quantize_price(ticket.yes_price_dollars), str(ticket.side).lower()
        side = signal_payload.get("recommended_side")
        candidate_trace = signal_payload.get("candidate_trace") if isinstance(signal_payload.get("candidate_trace"), dict) else {}
        if side is None:
            side = candidate_trace.get("selected_side")
        target_raw = signal_payload.get("target_yes_price_dollars")
        if target_raw is None:
            for candidate in candidate_trace.get("candidates") or []:
                if isinstance(candidate, dict) and candidate.get("status") == "selected":
                    target_raw = candidate.get("target_yes_price_dollars")
                    side = side or candidate.get("side")
                    break
        target = quantize_price(target_raw) if target_raw not in (None, "") else None
        return target, str(side).lower() if side not in (None, "") else None

    def _source_provenance(self, replay_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        provenance = dict(replay_payload.get("historical_provenance") or {})
        coverage = str(provenance.get("coverage_class") or "").strip()
        source_kinds = " ".join(
            str(provenance.get(key) or "")
            for key in ("market_source_kind", "weather_source_kind")
        ).lower()
        if "open_meteo" in source_kinds or "forecast_archive" in source_kinds or "external" in source_kinds:
            label = "historical_replay_external_forecast_repair"
        elif coverage == "full_checkpoint_coverage":
            label = "historical_replay_full_checkpoint"
        elif coverage == "late_only_coverage":
            label = "historical_replay_late_only"
        elif coverage in {"partial_checkpoint_coverage", "outcome_only_coverage", "no_replayable_coverage"}:
            label = "historical_replay_partial_checkpoint"
        else:
            label = "historical_replay_unknown"
        details = {
            "coverage_class": coverage or None,
            "market_source_kind": provenance.get("market_source_kind"),
            "weather_source_kind": provenance.get("weather_source_kind"),
            "checkpoint_label": provenance.get("checkpoint_label"),
            "asof_nominal": provenance.get("checkpoint_ts"),
            "source_coverage": provenance.get("source_coverage") or {},
        }
        return label, details

    def _apply_support_labels(self, rows: list[dict[str, Any]], *, reference_date: date) -> list[dict[str, Any]]:
        stats_by_level: dict[str, dict[tuple[Any, ...], _SupportStats]] = {}
        for level, dimensions in SUPPORT_LEVELS:
            buckets: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                buckets[self._support_key(row, dimensions)].append(row)
            stats_by_level[level] = {
                key: self._support_stats(bucket_rows, reference_date=reference_date)
                for key, bucket_rows in buckets.items()
            }
        for row in rows:
            path: list[dict[str, Any]] = []
            first_exploratory: dict[str, Any] | None = None
            selected: dict[str, Any] | None = None
            for level, dimensions in SUPPORT_LEVELS:
                key = self._support_key(row, dimensions)
                stats = stats_by_level[level][key]
                status, failed_on = self._support_status(stats)
                entry = {
                    "level": level,
                    "n": stats.n,
                    "market_days": stats.market_days,
                    "recency_days": stats.recency_days,
                    "status": status,
                    "failed_on": failed_on,
                }
                path.append(entry)
                if status == "supported":
                    selected = entry
                    break
                if status == "exploratory" and first_exploratory is None:
                    first_exploratory = entry
            if selected is None:
                selected = first_exploratory or path[-1]
            row["support_level"] = selected["level"]
            row["support_status"] = selected["status"]
            row["support_n"] = selected["n"]
            row["support_market_days"] = selected["market_days"]
            row["support_recency_days"] = selected["recency_days"]
            row["backoff_path"] = path
        return rows

    def _support_key(self, row: dict[str, Any], dimensions: tuple[str, ...]) -> tuple[Any, ...]:
        if not dimensions:
            return ("global",)
        values: list[Any] = []
        for dimension in dimensions:
            if dimension == "season_bucket":
                values.append(season_bucket_for_day(row["local_market_day"]))
            elif dimension == "lead_bucket":
                values.append(lead_bucket_for_minutes(row.get("time_to_settlement_at_checkpoint_minutes")))
            else:
                values.append(row.get(dimension) or "unknown")
        return tuple(values)

    def _support_stats(self, rows: list[dict[str, Any]], *, reference_date: date) -> _SupportStats:
        market_days = {
            (row.get("series_ticker") or row.get("station_id") or row["market_ticker"], row["local_market_day"])
            for row in rows
        }
        ages: list[int] = []
        for row in rows:
            local_day = _date_from_local_day(str(row["local_market_day"]))
            if local_day is not None:
                ages.append(max(0, (reference_date - local_day).days))
        ages.sort()
        recency = ages[len(ages) // 2] if ages else None
        return _SupportStats(n=len(rows), market_days=len(market_days), recency_days=recency)

    def _support_status(self, stats: _SupportStats) -> tuple[str, list[str]]:
        failed_supported: list[str] = []
        if stats.n < SUPPORTED_N:
            failed_supported.append("n")
        if stats.market_days < SUPPORTED_MARKET_DAYS:
            failed_supported.append("market_days")
        if stats.recency_days is None or stats.recency_days > SUPPORTED_RECENCY_DAYS:
            failed_supported.append("recency")
        if not failed_supported:
            return "supported", []
        failed_exploratory: list[str] = []
        if stats.n < EXPLORATORY_N:
            failed_exploratory.append("n")
        if stats.market_days < EXPLORATORY_MARKET_DAYS:
            failed_exploratory.append("market_days")
        if not failed_exploratory:
            return "exploratory", failed_supported
        return "insufficient", failed_exploratory

    def _expected_target_pnl(self, row: Any) -> Decimal | None:
        if row.settlement_result not in {"yes", "no"} or row.recommended_side not in {"yes", "no"} or row.target_yes_price_dollars is None:
            return None
        outcome_yes = Decimal("1") if row.settlement_result == "yes" else Decimal("0")
        target = Decimal(str(row.target_yes_price_dollars))
        pnl = outcome_yes - target if row.recommended_side == "yes" else (Decimal("1") - outcome_yes) - (Decimal("1") - target)
        return pnl.quantize(Decimal("0.000001"))

    @staticmethod
    def _mapping_payload(market_snapshot: dict[str, Any], weather_bundle: dict[str, Any]) -> dict[str, Any]:
        for payload in (weather_bundle, market_snapshot):
            mapping = payload.get("mapping") if isinstance(payload, dict) else None
            if isinstance(mapping, dict):
                return mapping
        return {}

    @staticmethod
    def _quote_observed_at(market_snapshot: dict[str, Any]) -> datetime | None:
        market = market_snapshot.get("market") if isinstance(market_snapshot, dict) else None
        if isinstance(market, dict):
            found = _as_datetime(market.get("observed_at") or market.get("asof_ts"))
            if found is not None:
                return found
        return _as_datetime(market_snapshot.get("observed_at") or market_snapshot.get("asof_ts")) if isinstance(market_snapshot, dict) else None

    @staticmethod
    def _liquidity_regime(market_snapshot: dict[str, Any]) -> str:
        market = market_snapshot.get("market", market_snapshot) if isinstance(market_snapshot, dict) else {}
        bid = _as_decimal(market.get("yes_bid_dollars"))
        ask = _as_decimal(market.get("yes_ask_dollars"))
        if bid is None or ask is None:
            return "unknown"
        spread_bps = int(((ask - bid) * Decimal("10000")).to_integral_value())
        if spread_bps <= 200:
            return "tight"
        if spread_bps <= 800:
            return "normal"
        return "wide"

    @staticmethod
    def _time_to_settlement_minutes(
        *,
        checkpoint_ts: datetime,
        local_market_day: str,
        timezone_name: str,
        settlement_ts: datetime | None,
    ) -> int | None:
        checkpoint = checkpoint_ts.astimezone(UTC) if checkpoint_ts.tzinfo else checkpoint_ts.replace(tzinfo=UTC)
        if settlement_ts is not None:
            target = settlement_ts.astimezone(UTC) if settlement_ts.tzinfo else settlement_ts.replace(tzinfo=UTC)
            return int((target - checkpoint).total_seconds() // 60)
        local_day = _date_from_local_day(local_market_day)
        if local_day is None:
            return None
        try:
            zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            zone = ZoneInfo("UTC")
        target = datetime.combine(local_day, time(23, 59), tzinfo=zone).astimezone(UTC)
        return int((target - checkpoint).total_seconds() // 60)

    @staticmethod
    def _support_distribution(rows: list[dict[str, Any]]) -> dict[str, int]:
        counts = {"supported": 0, "exploratory": 0, "insufficient": 0}
        for row in rows:
            status = str(row.get("support_status") or "insufficient")
            counts[status] = counts.get(status, 0) + 1
        return counts

    @staticmethod
    def _build_to_dict(build: Any) -> dict[str, Any]:
        return {
            "id": build.id,
            "version": build.version,
            "status": build.status,
            "git_sha": build.git_sha,
            "source": build.source or {},
            "filters": build.filters or {},
            "date_from": build.date_from.isoformat(),
            "date_to": build.date_to.isoformat(),
            "row_count": build.row_count,
            "parent_build_id": build.parent_build_id,
            "created_at": build.created_at.isoformat() if build.created_at else None,
            "finished_at": build.finished_at.isoformat() if build.finished_at else None,
            "failure_reason": build.failure_reason,
            "notes": build.notes,
        }

    @staticmethod
    def _row_sample(row: Any) -> dict[str, Any]:
        return {
            "id": row.id,
            "room_id": row.room_id,
            "market_ticker": row.market_ticker,
            "checkpoint_ts": row.checkpoint_ts.isoformat(),
            "recommended_side": row.recommended_side,
            "target_yes_price_dollars": str(row.target_yes_price_dollars) if row.target_yes_price_dollars is not None else None,
            "settlement_result": row.settlement_result,
            "support_status": row.support_status,
        }

    @staticmethod
    def _build_version() -> str:
        return f"decision-corpus-pr1-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    @staticmethod
    def _git_sha() -> str | None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stdout.strip() or None
        except Exception:
            return None
