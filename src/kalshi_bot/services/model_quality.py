from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.modeling import build_modeling_report


SCHEMA_VERSION = "model-quality-report-v1"
CHECKPOINT_PREFIX = "model_quality"
HISTORY_LIMIT = 30


def _now_iso(now: datetime | None = None) -> str:
    value = now or datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and result not in {float("inf"), float("-inf")} else None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _status_from_issues(issues: list[dict[str, Any]]) -> str:
    if any(str(issue.get("severity")) == "fail" for issue in issues):
        return "fail"
    if any(str(issue.get("severity")) in {"warn", "warning"} for issue in issues):
        return "warn"
    return "pass"


def _artifact_summary(artifact: Any | None) -> dict[str, Any]:
    if artifact is None:
        return {"status": "missing", "version": None, "sample_count": 0, "metrics": {}}
    return {
        "status": artifact.status,
        "version": artifact.version,
        "sample_count": artifact.sample_count,
        "metrics": artifact.metrics or {},
        "updated_at": artifact.updated_at.isoformat() if artifact.updated_at else None,
    }


def _metric_pair(
    current: dict[str, Any],
    baseline: dict[str, Any],
    key: str,
    *,
    lower_is_better: bool = True,
) -> dict[str, Any]:
    current_value = _float_or_none(current.get(key))
    baseline_value = _float_or_none(baseline.get(key))
    improved = None
    delta = None
    if current_value is not None and baseline_value is not None:
        delta = baseline_value - current_value if lower_is_better else current_value - baseline_value
        improved = delta > 0
    return {
        "current": current_value,
        "baseline": baseline_value,
        "delta": round(delta, 6) if delta is not None else None,
        "improved": improved,
    }


def _prediction_quality(prediction: dict[str, Any]) -> dict[str, Any]:
    baseline = dict(prediction.get("baseline") or {})
    calibrated = dict(prediction.get("calibrated") or {})
    metrics = {
        "brier": _metric_pair(calibrated, baseline, "brier"),
        "log_loss": _metric_pair(calibrated, baseline, "log_loss"),
        "ece": _metric_pair(calibrated, baseline, "ece"),
    }
    all_improved = all(item["improved"] is True for item in metrics.values())
    return {
        "status": prediction.get("status"),
        "eligible_rows": _int_or_none(prediction.get("eligible_rows")),
        "train_rows": _int_or_none(prediction.get("train_rows")),
        "test_rows": _int_or_none(prediction.get("test_rows")),
        "fold_count": _int_or_none(prediction.get("fold_count")),
        "metrics": metrics,
        "promotion_ready": bool(prediction.get("promotion_ready")) and all_improved,
        "blockers": list(prediction.get("promotion_blockers") or ([] if all_improved else ["prediction_metrics_do_not_all_improve"])),
    }


def _selection_quality(selection: dict[str, Any]) -> dict[str, Any]:
    baseline = dict(selection.get("baseline_rule_path") or {})
    selected = dict(selection.get("selected_path") or {})
    baseline_net = _decimal_or_none(baseline.get("net_pnl"))
    selected_net = _decimal_or_none(selected.get("net_pnl"))
    baseline_drawdown = _decimal_or_none(baseline.get("max_drawdown"))
    selected_drawdown = _decimal_or_none(selected.get("max_drawdown"))
    net_improved = selected_net is not None and baseline_net is not None and selected_net > baseline_net
    drawdown_improved = (
        selected_drawdown is not None
        and baseline_drawdown is not None
        and selected_drawdown <= baseline_drawdown
    )
    return {
        "status": selection.get("status"),
        "eligible_rows": _int_or_none(selection.get("eligible_rows")),
        "train_rows": _int_or_none(selection.get("train_rows")),
        "test_rows": _int_or_none(selection.get("test_rows")),
        "fold_count": _int_or_none(selection.get("fold_count")),
        "baseline_net_pnl": str(baseline_net) if baseline_net is not None else None,
        "selected_net_pnl": str(selected_net) if selected_net is not None else None,
        "baseline_max_drawdown": str(baseline_drawdown) if baseline_drawdown is not None else None,
        "selected_max_drawdown": str(selected_drawdown) if selected_drawdown is not None else None,
        "net_pnl_improved": net_improved,
        "drawdown_not_worse": drawdown_improved,
        "promotion_ready": bool(selection.get("promotion_ready")) and net_improved and drawdown_improved,
        "blockers": list(
            selection.get("promotion_blockers")
            or ([] if net_improved and drawdown_improved else ["trade_selection_does_not_improve_net_pnl_and_drawdown"])
        ),
    }


def _weather_quality_summary(modeling_report: dict[str, Any]) -> dict[str, Any]:
    prediction = _prediction_quality(dict(modeling_report.get("prediction_calibration") or {}))
    selection = _selection_quality(dict(modeling_report.get("trade_selection") or {}))
    issues = list(modeling_report.get("issues") or [])
    blocking_issue = any(
        str(issue.get("severity")) in {"fail", "warn", "warning"} for issue in issues
    )
    promotion_ready = prediction["promotion_ready"] and selection["promotion_ready"] and not blocking_issue
    blockers = [issue.get("code") for issue in issues if issue.get("code")]
    blockers.extend(prediction["blockers"])
    blockers.extend(selection["blockers"])
    return {
        "domain": "weather",
        "status": "pass" if promotion_ready else _status_from_issues(issues) if issues else "warn",
        "promotion_ready": promotion_ready,
        "model_quality": "promotion_ready" if promotion_ready else "shadow_research",
        "baseline": "current_weather_scorer_rule_path",
        "decision_corpus_status": (modeling_report.get("decision_corpus") or {}).get("status"),
        "dataset": modeling_report.get("dataset") or {},
        "prediction": prediction,
        "trade_selection": selection,
        "bucket_matrix_summary": modeling_report.get("bucket_matrix_summary") or {},
        "blockers": sorted({str(item) for item in blockers if item}),
        "source_status": modeling_report.get("status"),
    }


def _crypto_quality_summary(
    *,
    status_report: dict[str, Any],
    candidates_report: dict[str, Any],
    replay_report: dict[str, Any],
    model_artifact: Any | None,
    backtest_artifact: Any | None,
    gate_artifact: Any | None,
) -> dict[str, Any]:
    metrics = dict(replay_report.get("metrics") or {})
    prediction = _prediction_quality(
        {
            "status": "ok" if metrics.get("calibration_brier") is not None else "insufficient_data",
            "eligible_rows": metrics.get("resolved_sample_count") or metrics.get("sample_count") or 0,
            "fold_count": (replay_report.get("walk_forward") or {}).get("fold_count") or 0,
            "baseline": {
                "brier": metrics.get("market_mid_brier"),
                "log_loss": metrics.get("market_mid_log_loss"),
                "ece": metrics.get("market_mid_ece"),
            },
            "calibrated": {
                "brier": metrics.get("calibration_brier"),
                "log_loss": metrics.get("calibration_log_loss"),
                "ece": metrics.get("calibration_ece"),
            },
            "promotion_ready": not (replay_report.get("promotion_gate") or {}).get("reasons"),
        }
    )
    strict_candidates = int(metrics.get("trade_candidate_count") or 0)
    strict_net = _decimal_or_none(metrics.get("net_simulated_pl_dollars"))
    gate = dict(replay_report.get("promotion_gate") or {})
    gate_reasons = [str(reason) for reason in gate.get("reasons") or []]
    promotion_ready = bool(gate.get("passed")) and prediction["promotion_ready"]
    status = "pass" if promotion_ready else "warn"
    return {
        "domain": "crypto",
        "status": status,
        "promotion_ready": promotion_ready,
        "model_quality": "promotion_ready" if promotion_ready else "shadow_research",
        "baseline": "crypto_market_mid",
        "data_quality": status_report.get("data_quality") or {},
        "spot_quality": status_report.get("spot_quality") or {},
        "shadow_evidence": status_report.get("shadow_evidence") or {},
        "readiness_score": status_report.get("readiness_score") or {},
        "global_live_blockers": status_report.get("global_live_blockers") or [],
        "prediction": prediction,
        "trade_selection": {
            "strict_candidate_count": strict_candidates,
            "shadow_candidate_count": int((candidates_report.get("shadow_exploration_policy") or {}).get("selected_count") or 0),
            "live_quality_candidate_count": int((candidates_report.get("live_quality_policy") or {}).get("selected_count") or 0),
            "net_pnl_after_fees": str(strict_net) if strict_net is not None else None,
            "worst_buckets": ((replay_report.get("walk_forward") or {}).get("bucket_matrix") or [])[:10],
            "promotion_ready": bool(gate.get("passed")),
            "blockers": gate_reasons,
        },
        "artifacts": {
            "model": _artifact_summary(model_artifact),
            "backtest": _artifact_summary(backtest_artifact),
            "replay_gate": _artifact_summary(gate_artifact),
        },
        "blockers": gate_reasons + list(status_report.get("global_live_blockers") or []),
        "source_status": replay_report.get("status"),
    }


def _compact_history_entry(report: dict[str, Any]) -> dict[str, Any]:
    domains = {}
    for name, summary in (report.get("domains") or {}).items():
        prediction_metrics = ((summary.get("prediction") or {}).get("metrics") or {})
        domains[name] = {
            "status": summary.get("status"),
            "promotion_ready": summary.get("promotion_ready"),
            "model_quality": summary.get("model_quality"),
            "prediction_deltas": {
                key: value.get("delta") for key, value in prediction_metrics.items() if isinstance(value, dict)
            },
            "strict_candidate_count": (summary.get("trade_selection") or {}).get("strict_candidate_count"),
            "shadow_candidate_count": (summary.get("trade_selection") or {}).get("shadow_candidate_count"),
            "net_pnl_after_fees": (summary.get("trade_selection") or {}).get("net_pnl_after_fees")
            or (summary.get("trade_selection") or {}).get("selected_net_pnl"),
            "blocker_count": len(summary.get("blockers") or []),
        }
    return {
        "generated_at": report.get("generated_at"),
        "kalshi_env": report.get("kalshi_env"),
        "domain": report.get("domain"),
        "status": report.get("status"),
        "domains": domains,
    }


def _trend_from_history(history: list[dict[str, Any]], current: dict[str, Any]) -> dict[str, Any]:
    if not history:
        return {"status": "new", "previous_generated_at": None, "changes": {}}
    previous = history[-1]
    changes: dict[str, Any] = {}
    for domain, summary in (current.get("domains") or {}).items():
        current_domain = _compact_history_entry({"domains": {domain: summary}})["domains"][domain]
        previous_domain = ((previous.get("domains") or {}).get(domain) or {})
        changes[domain] = {
            "status_changed": previous_domain.get("status") != current_domain.get("status"),
            "promotion_ready_changed": previous_domain.get("promotion_ready") != current_domain.get("promotion_ready"),
            "strict_candidate_delta": _numeric_delta(
                current_domain.get("strict_candidate_count"),
                previous_domain.get("strict_candidate_count"),
            ),
            "shadow_candidate_delta": _numeric_delta(
                current_domain.get("shadow_candidate_count"),
                previous_domain.get("shadow_candidate_count"),
            ),
            "net_pnl_delta": _numeric_delta(
                current_domain.get("net_pnl_after_fees"),
                previous_domain.get("net_pnl_after_fees"),
            ),
        }
    return {
        "status": "ok",
        "previous_generated_at": previous.get("generated_at"),
        "changes": changes,
    }


def _numeric_delta(current: Any, previous: Any) -> float | None:
    current_value = _float_or_none(current)
    previous_value = _float_or_none(previous)
    if current_value is None or previous_value is None:
        return None
    return round(current_value - previous_value, 6)


def _aggregate_status(domains: dict[str, dict[str, Any]]) -> str:
    statuses = {str(summary.get("status")) for summary in domains.values()}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def format_model_quality_report(report: dict[str, Any]) -> str:
    lines = [
        "Model Quality",
        f"env={report.get('kalshi_env')} domain={report.get('domain')} status={str(report.get('status')).upper()}",
    ]
    for domain, summary in (report.get("domains") or {}).items():
        prediction = summary.get("prediction") or {}
        selection = summary.get("trade_selection") or {}
        lines.append(
            f"- {domain}: quality={summary.get('model_quality')} promotion_ready={summary.get('promotion_ready')} "
            f"prediction_rows={prediction.get('eligible_rows')}"
        )
        if domain == "crypto":
            readiness = summary.get("readiness_score") or {}
            lines.append(
                f"  strict_candidates={selection.get('strict_candidate_count')} "
                f"shadow_candidates={selection.get('shadow_candidate_count')} "
                f"net={selection.get('net_pnl_after_fees')} "
                f"readiness={readiness.get('score')}/10"
            )
        else:
            lines.append(
                f"  selected_net={selection.get('selected_net_pnl')} "
                f"baseline_net={selection.get('baseline_net_pnl')}"
            )
        blockers = list(summary.get("blockers") or [])
        if blockers:
            lines.append(f"  blockers={', '.join(blockers[:5])}")
    trend = report.get("trend") or {}
    if trend.get("previous_generated_at"):
        lines.append(f"Trend previous={trend.get('previous_generated_at')}")
    return "\n".join(lines)


async def build_model_quality_report(
    *,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    decision_corpus_service: Any,
    trading_audit_service: Any,
    trade_analysis_service: Any,
    crypto_market_service: Any,
    crypto_forecast_service: Any,
    crypto_replay_service: Any,
    kalshi_env: str = "demo",
    domain: str = "all",
    days: int = 180,
    frequency: str = "15m",
    persist: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _now_iso(now)
    selected_domains = ["weather", "crypto"] if domain == "all" else [domain]
    summaries: dict[str, dict[str, Any]] = {}
    async with session_factory() as session:
        repo = PlatformRepository(session)
        previous_checkpoint = await repo.get_checkpoint(f"{CHECKPOINT_PREFIX}:{kalshi_env}:{domain}")
        previous_history = list(((previous_checkpoint.payload if previous_checkpoint else None) or {}).get("history") or [])
        await session.commit()

    if "weather" in selected_domains:
        modeling_report = await build_modeling_report(
            settings=settings,
            decision_corpus_service=decision_corpus_service,
            trading_audit_service=trading_audit_service,
            trade_analysis_service=trade_analysis_service,
            kalshi_env=kalshi_env,
            days=days,
            command="status",
            limit=20,
            row_limit=500,
            now=now,
        )
        summaries["weather"] = _weather_quality_summary(modeling_report)

    if "crypto" in selected_domains:
        status_report = await crypto_market_service.status(frequency=frequency)
        candidates_report = await crypto_forecast_service.candidates(frequency=frequency, days=days)
        replay_report = await crypto_replay_service.validate(frequency=frequency, days=days)
        async with session_factory() as session:
            repo = PlatformRepository(session)
            model_artifact = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type="model",
                kalshi_env=kalshi_env,
            )
            backtest_artifact = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type="backtest",
                kalshi_env=kalshi_env,
            )
            gate_artifact = await repo.get_latest_crypto_model_artifact(
                frequency=frequency,
                artifact_type="replay_gate",
                kalshi_env=kalshi_env,
            )
            await session.commit()
        summaries["crypto"] = _crypto_quality_summary(
            status_report=status_report,
            candidates_report=candidates_report,
            replay_report=replay_report,
            model_artifact=model_artifact,
            backtest_artifact=backtest_artifact,
            gate_artifact=gate_artifact,
        )

    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "kalshi_env": kalshi_env,
        "domain": domain,
        "window_days": days,
        "frequency": frequency,
        "status": _aggregate_status(summaries),
        "production_entry_freeze_required": True,
        "crypto_live_trading_required": False,
        "domains": summaries,
        "trend": _trend_from_history(previous_history, {"domains": summaries}),
        "persisted": False,
    }
    if persist:
        entry = _compact_history_entry(report)
        history = [*previous_history, entry][-HISTORY_LIMIT:]
        payload = {
            "schema_version": "model-quality-checkpoint-v1",
            "latest": entry,
            "history": history,
            "report": _json_safe({
                "schema_version": report["schema_version"],
                "generated_at": report["generated_at"],
                "kalshi_env": report["kalshi_env"],
                "domain": report["domain"],
                "status": report["status"],
                "domains": report["domains"],
            }),
        }
        async with session_factory() as session:
            repo = PlatformRepository(session)
            checkpoint = await repo.set_checkpoint(
                f"{CHECKPOINT_PREFIX}:{kalshi_env}:{domain}",
                cursor=generated_at,
                payload=payload,
            )
            await session.commit()
        report["persisted"] = True
        report["checkpoint"] = {
            "stream_name": checkpoint.stream_name,
            "updated_at": checkpoint.updated_at.isoformat() if checkpoint.updated_at else None,
            "history_count": len(history),
        }
    return _json_safe(report)
