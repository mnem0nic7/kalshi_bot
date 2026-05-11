from __future__ import annotations

import argparse
import asyncio
import csv
from dataclasses import asdict
from datetime import UTC, date, datetime
import io
import json
import os
from pathlib import Path
import sys

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from kalshi_bot.core.enums import RoomOrigin
from kalshi_bot.core.schemas import (
    HeuristicPackPromoteRequest,
    HeuristicPackRollbackRequest,
    HistoricalIntelligenceRunRequest,
    HistoricalTrainingBuildRequest,
    RoomCreate,
    ShadowCampaignRequest,
    TrainingBuildRequest,
)
from kalshi_bot.db.models import Room, Signal, StrategyPromotionRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import init_models
from kalshi_bot.forecast.learned_head import (
    LearnedHeadHoldoutMetrics,
    evaluate_learned_head_gate,
)
from kalshi_bot.forecast.nws_discussion_parser import (
    NwsParserHealthWindow,
    evaluate_nws_parser_health,
)
from kalshi_bot.learning.drift_watcher import DriftWindow, evaluate_calibration_drift
from kalshi_bot.learning.hard_caps import DEFAULT_HARD_CAPS_PATH, load_hard_caps
from kalshi_bot.learning.parameter_pack import (
    DEFAULT_PARAMETER_PACK_PATH,
    default_parameter_pack,
    load_parameter_pack,
    parameter_pack_from_dict,
    sanitize_parameter_pack,
)
from kalshi_bot.learning.parameter_search import (
    generate_parameter_pack_grid,
    select_parameter_pack_candidate,
)
from kalshi_bot.learning.promotion_gates import (
    HoldoutMetrics,
    evaluate_parameter_pack_promotion,
    promotion_gate_config_from_hard_caps,
)
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.kalshi_leaderboard import (
    LEADERBOARD_CATEGORIES,
    LEADERBOARD_NAMES,
    LEADERBOARD_SOURCES,
    LEADERBOARD_TIME_WINDOWS,
    KalshiLeaderboardScraper,
    leaderboard_snapshot_to_csv,
)
from kalshi_bot.logging import configure_logging
from kalshi_bot.services.container import AppContainer
from kalshi_bot.config import get_settings
from kalshi_bot.services.baseline_model_card import write_baseline_model_card
from kalshi_bot.services.decision_trace import decision_trace_record_to_dict, replay_decision_trace
from kalshi_bot.services.decision_policy_variants import DecisionPolicyVariantService
from kalshi_bot.services.parameter_packs import ParameterPackCanaryConfig, ParameterPackPromotionService
from kalshi_bot.services.position_governance import refresh_stop_loss_checkpoints
from kalshi_bot.services.signal_attention import SignalAttentionService, attention_rows_to_csv
from kalshi_bot.services.trade_analysis import format_trade_analysis_report
from kalshi_bot.services.backtesting import (
    build_backtesting_report,
    format_backtesting_report,
    write_backtesting_report,
)
from kalshi_bot.services.model_quality import build_model_quality_report, format_model_quality_report
from kalshi_bot.services.modeling import build_modeling_report, format_modeling_report
from kalshi_bot.services.gate_learning import (
    GateLearningService,
    format_gate_learning_report,
    format_gate_recommendation_report,
)
from kalshi_bot.services.overnight_readiness import (
    OvernightReadinessService,
    format_overnight_readiness_report,
)
from kalshi_bot.services.trade_behavior_validation import (
    build_trade_behavior_validation_report,
    format_trade_behavior_validation_report,
)
from kalshi_bot.services.trade_behavior_quality import (
    build_trade_behavior_quality_report,
    format_trade_behavior_quality_report,
)
from kalshi_bot.services.trading_audit import format_trading_audit_text


CRYPTO_ENV_COMMANDS = {
    "crypto-history",
    "crypto-spot",
    "crypto-model",
    "crypto-replay",
    "crypto-status",
    "crypto-autonomy",
    "crypto-asset-mode",
}


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _policy_audit_best_block_reason(candidate_trace: dict[str, object]) -> str | None:
    baseline = candidate_trace.get("baseline_block_reason")
    if baseline not in (None, ""):
        return str(baseline)
    candidates = [item for item in candidate_trace.get("candidates") or [] if isinstance(item, dict)]
    if not candidates:
        return None

    def score(candidate: dict[str, object]) -> tuple[int, int]:
        edge = _float_or_none(candidate.get("edge_bps"))
        qa_edge = _float_or_none(candidate.get("quality_adjusted_edge_bps"))
        return (
            int(qa_edge) if qa_edge is not None else -1_000_000,
            int(edge) if edge is not None else -1_000_000,
        )

    return str(max(candidates, key=score).get("reason") or "")


def _policy_audit_csv(rows: list[dict[str, object]]) -> str:
    output = io.StringIO()
    columns = ["market_ticker", "room_id", "updated_at", "baseline_block_reason", "matched_variants", "live_variants"]
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def _shadow_run_payload(result) -> dict[str, object | None]:
    return {
        "room_id": result.room_id,
        "market_ticker": result.market_ticker,
        "room_name": getattr(result, "room_name", None),
        "stage": result.stage,
        "decision_trace_id": result.decision_trace_id,
    }


def _read_json_file(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _secondary_ignore_resolution(
    *,
    resolved_by: str,
    note: str,
    resolved_at: datetime | None = None,
) -> dict[str, str]:
    resolved_by = resolved_by.strip()
    note = note.strip()
    if not resolved_by:
        raise ValueError("--resolved-by must be non-empty")
    if len(note) < 20:
        raise ValueError("--note must be at least 20 characters")
    return {
        "action": "ignored_by_operator",
        "resolved_by": resolved_by,
        "resolved_at": (resolved_at or datetime.now(UTC)).isoformat(),
        "note": note,
    }


def _secondary_ignore_update_values(
    fields: list[str],
    resolution: dict[str, str],
) -> dict[str, object]:
    values: dict[str, object] = {}
    if "secondary_sync_status" in fields:
        values["secondary_sync_status"] = "ignored_by_operator"
        values["secondary_sync_resolution"] = dict(resolution)
    if "secondary_rollback_status" in fields:
        values["secondary_rollback_status"] = "ignored_by_operator"
        values["secondary_rollback_resolution"] = dict(resolution)
    return values


async def _run_health_check_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.health_command == "app":
        payload = await container.watchdog_service.app_health(
            color=args.color,
            kalshi_env=container.settings.kalshi_env,
        )
        print(json.dumps(payload, indent=2))
        return 0 if payload["healthy"] else 1
    if args.health_command == "daemon":
        async with container.session_factory() as session:
            repo = PlatformRepository(session)
            payload = await container.watchdog_service.daemon_health(
                repo,
                color=args.color,
                kalshi_env=container.settings.kalshi_env,
            )
            await session.commit()
        print(json.dumps(payload, indent=2))
        return 0 if payload["healthy"] else 1
    raise ValueError(f"Unknown command: {args.command}")


async def _run_decision_trace_command(args: argparse.Namespace, container: AppContainer) -> int:
    async with container.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=container.settings.kalshi_env)
        record = await repo.get_decision_trace(args.decision_id)
        await session.commit()
    if record is None:
        print(json.dumps({"error": f"Decision trace {args.decision_id} not found"}), file=sys.stderr)
        return 1
    if args.decision_trace_command == "show":
        print(json.dumps(decision_trace_record_to_dict(record), indent=2))
        return 0
    if args.decision_trace_command == "replay":
        result = replay_decision_trace(record.trace, expected_trace_hash=record.trace_hash)
        payload = {
            "decision_trace_id": record.id,
            "room_id": record.room_id,
            "market_ticker": record.market_ticker,
            "kalshi_env": record.kalshi_env,
            **result.to_dict(),
        }
        print(json.dumps(payload, indent=2))
        return 0 if result.ok else 1
    print(json.dumps({"error": f"Unknown decision-trace action {args.decision_trace_command}"}), file=sys.stderr)
    return 1


async def _run_parameter_pack_command(args: argparse.Namespace, container: AppContainer) -> int:
    action = args.parameter_pack_command
    if action == "default":
        pack = load_parameter_pack(args.path) if args.path is not None else default_parameter_pack()
        print(json.dumps({"pack_hash": pack.pack_hash, "pack": pack.to_dict()}, indent=2))
        return 0
    if action == "hard-caps":
        caps = load_hard_caps(args.path)
        print(json.dumps({"config_hash": caps.config_hash, "hard_caps": caps.to_dict()}, indent=2))
        return 0
    if action == "validate":
        pack = sanitize_parameter_pack(parameter_pack_from_dict(_read_json_file(Path(args.path))))
        dropped = pack.metadata.get("dropped_hard_cap_parameters", [])
        if args.strict and dropped:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": "candidate_contains_hard_cap_parameters",
                        "dropped_hard_cap_parameters": dropped,
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "ok": True,
                    "pack_hash": pack.pack_hash,
                    "dropped_hard_cap_parameters": dropped,
                    "pack": pack.to_dict(),
                },
                indent=2,
            )
        )
        return 0
    if action == "gate":
        candidate = HoldoutMetrics.from_dict(_read_json_file(Path(args.candidate_report)))
        current = HoldoutMetrics.from_dict(_read_json_file(Path(args.current_report)))
        hard_caps = load_hard_caps(args.hard_caps)
        result = evaluate_parameter_pack_promotion(
            candidate=candidate,
            current=current,
            config=promotion_gate_config_from_hard_caps(hard_caps),
        )
        payload = result.to_dict()
        payload["hard_caps"] = {
            "config_hash": hard_caps.config_hash,
            "max_drawdown_pct": hard_caps.hard_caps["max_drawdown_pct"],
        }
        print(json.dumps(payload, indent=2))
        return 0 if result.passed else 1
    if action == "drift":
        decision = evaluate_calibration_drift(DriftWindow.from_dict(_read_json_file(Path(args.window))))
        print(json.dumps(decision.to_dict(), indent=2))
        return 0 if not decision.pause_new_entries else 1
    if action == "select":
        result = select_parameter_pack_candidate(
            search_payload=_read_json_file(Path(args.candidates)),
            current_report=_read_json_file(Path(args.current_report)),
            hard_caps=load_hard_caps(args.hard_caps),
            starvation_tolerance=args.starvation_tolerance,
        )
        print(json.dumps(result.to_dict(), indent=2))
        return 0 if result.selected is not None else 1
    if action == "grid":
        result = generate_parameter_pack_grid(
            _read_json_file(Path(args.grid)),
            limit=args.limit,
        )
        print(json.dumps(result.to_dict(), indent=2))
        return 0
    if action == "learned-gate":
        result = evaluate_learned_head_gate(
            closed_form=LearnedHeadHoldoutMetrics.from_dict(_read_json_file(Path(args.closed_form_report))),
            learned=LearnedHeadHoldoutMetrics.from_dict(_read_json_file(Path(args.learned_report))),
            requested_weight=args.requested_weight,
        )
        print(json.dumps(result.to_dict(), indent=2))
        return 0 if result.passed else 1
    if action == "nws-parser-gate":
        result = evaluate_nws_parser_health(
            NwsParserHealthWindow.from_dict(_read_json_file(Path(args.window))),
            requested_feature_weight=args.requested_feature_weight,
        )
        print(json.dumps(result.to_dict(), indent=2))
        return 0 if result.passed else 1

    async with container.session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=container.settings.kalshi_env)
        if action == "record-starvation":
            result = await ParameterPackPromotionService().record_promotion_starvation(
                repo,
                selection_payload=_read_json_file(Path(args.selection)),
                reason=args.reason,
                escalation_threshold=args.escalation_threshold,
            )
            await session.commit()
            print(json.dumps(result.to_dict(), indent=2))
            return 0
        if action == "status":
            await ParameterPackPromotionService().mark_stalled_if_expired(
                repo,
                max_age_seconds=container.settings.self_improve_canary_max_seconds,
            )
            control = await repo.get_deployment_control()
            packs = await repo.list_parameter_packs(limit=args.limit)
            raw_promotions = await repo.list_promotion_events(limit=max(args.limit * 3, args.limit))
            promotions = [
                record
                for record in raw_promotions
                if dict(record.payload or {}).get("kind") == "parameter_pack"
            ][: args.limit]
            champion = await repo.get_champion_parameter_pack()
            starvation_checkpoint_name = f"parameter_pack_promotion_starvation:{container.settings.kalshi_env}"
            starvation_checkpoint = await repo.get_checkpoint(starvation_checkpoint_name)
            starvation_state = (
                dict(starvation_checkpoint.payload or {})
                if starvation_checkpoint is not None
                else {
                    "event_kind": "parameter_pack_promotion_starvation",
                    "consecutive_starvations": 0,
                    "escalated": False,
                    "status": "none",
                }
            )
            starvation_state.setdefault(
                "status",
                "promotion_starvation" if starvation_checkpoint is not None else "none",
            )
            starvation_state["checkpoint_name"] = starvation_checkpoint_name
            await session.commit()
            print(
                json.dumps(
                    {
                        "kalshi_env": container.settings.kalshi_env,
                        "active_color": control.active_color,
                        "parameter_packs": dict((control.notes or {}).get("parameter_packs") or {}),
                        "champion": (
                            {
                                "version": champion.version,
                                "status": champion.status,
                                "pack_hash": champion.pack_hash,
                            }
                            if champion is not None
                            else None
                        ),
                        "promotion_starvation": starvation_state,
                        "recent_packs": [
                            {
                                "version": record.version,
                                "status": record.status,
                                "parent_version": record.parent_version,
                                "source": record.source,
                                "pack_hash": record.pack_hash,
                                "updated_at": record.updated_at.isoformat(),
                            }
                            for record in packs
                        ],
                        "recent_promotions": [
                            {
                                "id": record.id,
                                "status": record.status,
                                "candidate_version": record.candidate_version,
                                "previous_version": record.previous_version,
                                "target_color": record.target_color,
                                "rollback_reason": record.rollback_reason,
                            }
                            for record in promotions
                        ],
                    },
                    indent=2,
                )
            )
            return 0
        if action == "stage":
            candidate = sanitize_parameter_pack(parameter_pack_from_dict(_read_json_file(Path(args.candidate_pack))))
            dropped = candidate.metadata.get("dropped_hard_cap_parameters", [])
            if dropped:
                print(
                    json.dumps(
                        {
                            "ok": False,
                            "error": "candidate_contains_hard_cap_parameters",
                            "dropped_hard_cap_parameters": dropped,
                        },
                        indent=2,
                    ),
                    file=sys.stderr,
                )
                return 1
            service = ParameterPackPromotionService()
            result = await service.stage_candidate(
                repo,
                candidate_pack=candidate,
                candidate_report=_read_json_file(Path(args.candidate_report)),
                current_report=_read_json_file(Path(args.current_report)),
                hard_caps=load_hard_caps(args.hard_caps),
                reason=args.reason,
                target_color=args.target_color,
            )
            await session.commit()
            print(json.dumps(result.to_dict(), indent=2))
            return 0
        if action == "rollback-staged":
            service = ParameterPackPromotionService()
            result = await service.rollback_staged(repo, reason=args.reason)
            await session.commit()
            print(json.dumps(result.to_dict(), indent=2))
            return 0
        if action == "canary":
            service = ParameterPackPromotionService()
            result = await service.evaluate_staged_canary(
                repo,
                canary_report=_read_json_file(Path(args.report)),
                config=ParameterPackCanaryConfig(
                    min_shadow_rooms=args.min_shadow_rooms,
                    min_elapsed_seconds=args.min_elapsed_seconds,
                    max_brier_ratio=args.max_brier_ratio,
                ),
            )
            await session.commit()
            print(json.dumps(result.to_dict(), indent=2))
            return 0 if result.status != "canary_failed" else 1
        if action == "promote-staged":
            service = ParameterPackPromotionService()
            result = await service.promote_canary_passed(repo, reason=args.reason)
            await session.commit()
            print(json.dumps(result.to_dict(), indent=2))
            return 0
        if action == "seed-default":
            pack = load_parameter_pack(args.path) if args.path is not None else default_parameter_pack()
            record = await repo.update_parameter_pack(pack, holdout_report={})
            await session.commit()
            print(
                json.dumps(
                    {
                        "version": record.version,
                        "status": record.status,
                        "pack_hash": record.pack_hash,
                        "stored": True,
                    },
                    indent=2,
                )
            )
            return 0
        if action == "list":
            records = await repo.list_parameter_packs(limit=args.limit)
            await session.commit()
            print(
                json.dumps(
                    [
                        {
                            "version": record.version,
                            "status": record.status,
                            "parent_version": record.parent_version,
                            "source": record.source,
                            "pack_hash": record.pack_hash,
                            "created_at": record.created_at.isoformat(),
                            "updated_at": record.updated_at.isoformat(),
                        }
                        for record in records
                    ],
                    indent=2,
                )
            )
            return 0
        if action == "show":
            if args.version == "default":
                pack = default_parameter_pack()
                print(json.dumps({"pack_hash": pack.pack_hash, "pack": pack.to_dict()}, indent=2))
                return 0
            record = await repo.get_parameter_pack(args.version)
            await session.commit()
            if record is None:
                print(json.dumps({"error": f"Parameter pack {args.version} not found"}), file=sys.stderr)
                return 1
            print(
                json.dumps(
                    {
                        "version": record.version,
                        "status": record.status,
                        "parent_version": record.parent_version,
                        "source": record.source,
                        "description": record.description,
                        "pack_hash": record.pack_hash,
                        "payload": record.payload,
                        "holdout_report": record.holdout_report,
                    },
                    indent=2,
                )
            )
            return 0
    print(json.dumps({"error": f"Unknown parameter-pack action {action}"}), file=sys.stderr)
    return 1


async def _run_watchdog_command(args: argparse.Namespace, container: AppContainer) -> int:
    async with container.session_factory() as session:
        repo = PlatformRepository(session)
        if args.watchdog_command == "status":
            payload = await container.watchdog_service.get_status(
                repo,
                kalshi_env=container.settings.kalshi_env,
            )
            await session.commit()
            print(json.dumps(payload, indent=2))
            return 0
        if args.watchdog_command == "run-once":
            payload = await container.watchdog_service.run_once(
                repo,
                app_statuses={
                    "blue": args.app_blue_status,
                    "green": args.app_green_status,
                },
                source=args.source,
            )
            await session.commit()
            print(json.dumps(payload, indent=2))
            return 0
        if args.watchdog_command == "record-action":
            payload = await container.watchdog_service.record_action(
                repo,
                action=args.action,
                outcome=args.outcome,
                reason=args.reason,
                target_color=args.target_color,
                failed_color=args.failed_color,
                source=args.source,
            )
            await session.commit()
            print(json.dumps(payload, indent=2))
            return 0
        if args.watchdog_command == "mark-boot":
            payload = await container.watchdog_service.record_boot(
                repo,
                status=args.status,
                reason=args.reason,
                payload={"working_directory": str(Path.cwd())},
            )
            await session.commit()
            print(json.dumps(payload, indent=2))
            return 0
    raise ValueError(f"Unknown command: {args.command}")


async def _run_create_room_command(
    args: argparse.Namespace,
    container: AppContainer,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    control = await repo.get_deployment_control()
    pack = await container.agent_pack_service.get_pack_for_color(repo, container.settings.app_color)
    room = await repo.create_room(
        RoomCreate(name=args.name, market_ticker=args.market_ticker, prompt=args.prompt),
        active_color=container.settings.app_color,
        shadow_mode=container.settings.app_shadow_mode,
        kill_switch_enabled=control.kill_switch_enabled,
        kalshi_env=container.settings.kalshi_env,
        agent_pack_version=pack.version,
    )
    await session.commit()
    print(room.id)
    return 0


async def _run_run_room_command(
    args: argparse.Namespace,
    container: AppContainer,
    session: AsyncSession,
) -> int:
    await session.commit()
    await container.run_room(args.room_id, reason=args.reason)
    print(f"room {args.room_id} completed")
    return 0


async def _run_crypto_history_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.crypto_history_command == "bootstrap":
        result = await container.crypto_history_service.bootstrap(days=args.days, frequency=args.frequency)
    elif args.crypto_history_command == "daily":
        result = await container.crypto_history_service.daily(frequency=args.frequency)
    elif args.crypto_history_command == "status":
        result = await container.crypto_history_service.status(
            frequency=args.frequency,
            days=args.days if args.days and args.days > 0 else None,
        )
    else:
        raise ValueError(f"unknown crypto-history command {args.crypto_history_command}")
    print(json.dumps(result, indent=2, default=str))
    return 0


async def _run_crypto_spot_command(args: argparse.Namespace, container: AppContainer) -> int:
    asset_symbols = args.assets if getattr(args, "assets", None) else None
    if args.crypto_spot_command == "backfill":
        result = await container.crypto_spot_service.backfill(
            days=args.days,
            frequency=args.frequency,
            asset_symbols=asset_symbols,
        )
    elif args.crypto_spot_command == "status":
        result = await container.crypto_spot_service.status(
            frequency=args.frequency,
            days=args.days if args.days and args.days > 0 else None,
            asset_symbols=asset_symbols,
        )
    else:
        raise ValueError(f"unknown crypto-spot command {args.crypto_spot_command}")
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") in {"ok", "warn"} else 1


async def _run_crypto_model_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.crypto_model_command == "train":
        result = await container.crypto_forecast_service.train(frequency=args.frequency)
    elif args.crypto_model_command == "candidates":
        result = await container.crypto_forecast_service.candidates(
            frequency=args.frequency,
            days=args.days if args.days and args.days > 0 else None,
        )
    else:
        raise ValueError(f"unknown crypto-model command {args.crypto_model_command}")
    print(json.dumps(result, indent=2, default=str))
    if args.crypto_model_command == "train":
        return 0 if result.get("status") == "trained" else 1
    return 0 if result.get("status") in {"ok", "warn"} else 1


async def _run_crypto_replay_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.crypto_replay_command == "gate":
        result = await container.crypto_replay_service.gate(frequency=args.frequency)
    elif args.crypto_replay_command == "run":
        result = await container.crypto_replay_service.run(
            frequency=args.frequency,
            days=args.days,
            limit=args.limit if args.limit and args.limit > 0 else None,
        )
    elif args.crypto_replay_command == "validate":
        result = await container.crypto_replay_service.validate(
            frequency=args.frequency,
            days=args.days,
            limit=args.limit if args.limit and args.limit > 0 else None,
        )
    else:
        raise ValueError(f"unknown crypto-replay command {args.crypto_replay_command}")
    print(json.dumps(result, indent=2, default=str))
    if args.crypto_replay_command == "validate":
        return 0 if result.get("status") != "fail" else 1
    if args.crypto_replay_command == "gate":
        return 0 if result.get("status") == "passed" else 1
    return 0 if result.get("status") in {"pass", "warn"} else 1


async def _run_crypto_status_command(container: AppContainer) -> int:
    result = await container.crypto_market_service.status(frequency="15m")
    print(json.dumps(result, indent=2, default=str))
    return 0


def _apply_crypto_cli_env(args: argparse.Namespace, container: AppContainer) -> None:
    requested = _crypto_cli_env_override(args)
    if requested:
        container.settings.kalshi_env = requested


def _crypto_cli_env_override(args: argparse.Namespace) -> str | None:
    if getattr(args, "command", None) not in CRYPTO_ENV_COMMANDS:
        return None
    requested = str(getattr(args, "kalshi_env", "") or "").strip()
    return requested or None


async def _run_training_backfill_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.training_backfill_command != "research-health":
        raise ValueError(f"unknown training-backfill command {args.training_backfill_command}")
    result = await container.research_health_backfill_service.backfill_research_health(
        origins=args.origins,
        days=args.days,
        market_prefixes=args.market_prefix,
        limit=args.limit,
        overwrite=args.overwrite,
        include_non_complete=args.include_non_complete,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


async def _run_crypto_autonomy_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.crypto_autonomy_command != "run-once":
        raise ValueError(f"unknown crypto-autonomy command {args.crypto_autonomy_command}")
    result = await container.crypto_autonomy_service.run_once(frequency=args.frequency, force=True)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") in {"ok", "inactive_color"} else 1


async def _run_crypto_asset_mode_command(args: argparse.Namespace, container: AppContainer) -> int:
    if args.crypto_asset_mode_command == "list":
        asset_symbols: list[str] | None = None
        try:
            markets = await container.crypto_market_service.discover_markets(
                frequency=args.frequency,
                status="open",
                persist=False,
            )
            asset_symbols = sorted({market.asset_symbol for market in markets})
        except Exception:
            asset_symbols = None
        result = await container.crypto_asset_control_service.list_asset_modes(asset_symbols=asset_symbols)
    elif args.crypto_asset_mode_command == "set":
        result = await container.crypto_asset_control_service.set_asset_mode(
            args.symbol,
            args.mode,
            actor="cli",
        )
    else:
        raise ValueError(f"unknown crypto-asset-mode command {args.crypto_asset_mode_command}")
    print(json.dumps(result, indent=2, default=str))
    return 0


async def _run_reconcile_command(
    container: AppContainer,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    summary = await container.reconciliation_service.reconcile(
        repo,
        subaccount=container.settings.kalshi_subaccount,
        kalshi_env=container.settings.kalshi_env,
    )
    await session.commit()
    print(json.dumps(asdict(summary), indent=2))
    return 0


async def _run_promote_command(
    args: argparse.Namespace,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    control = await repo.set_active_color(args.color)
    await session.commit()
    print(f"active_color={control.active_color}")
    return 0


async def _run_kill_switch_command(
    args: argparse.Namespace,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    enabled = args.state == "on"
    control = await repo.set_kill_switch(enabled)
    await session.commit()
    print(f"kill_switch_enabled={control.kill_switch_enabled}")
    return 0


async def _run_status_command(
    container: AppContainer,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    control = await repo.get_deployment_control()
    positions = await repo.list_positions(limit=10, kalshi_env=container.settings.kalshi_env)
    ops_events = await repo.list_ops_events(limit=10, kalshi_env=container.settings.kalshi_env)
    await session.commit()
    payload = {
        "kalshi_env": container.settings.kalshi_env,
        "active_color": control.active_color,
        "kill_switch_enabled": control.kill_switch_enabled,
        "execution_lock_holder": control.execution_lock_holder,
        "positions": [
            {
                "market_ticker": position.market_ticker,
                "subaccount": position.subaccount,
                "side": position.side,
                "count_fp": str(position.count_fp),
                "average_price_dollars": str(position.average_price_dollars),
            }
            for position in positions
        ],
        "ops_events": [
            {
                "severity": event.severity,
                "summary": event.summary,
                "source": event.source,
            }
            for event in ops_events
        ],
    }
    print(json.dumps(payload, indent=2))
    return 0


async def _run_intel_command(
    args: argparse.Namespace,
    container: AppContainer,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    ticker: str | None = getattr(args, "market", None)
    if ticker:
        dossier = await container.research_coordinator.get_latest_dossier(ticker)
        if dossier is None:
            print(json.dumps({"market_ticker": ticker, "status": "missing"}))
            return 2
        gate = dossier.gate
        payload = {
            "market_ticker": ticker,
            "gate_passed": gate.passed,
            "gate_reasons": list(gate.reasons or []),
            "fair_yes_dollars": str(dossier.trader_context.fair_yes_dollars or ""),
            "confidence": dossier.trader_context.confidence,
            "stale": dossier.freshness.stale,
            "refreshed_at": dossier.freshness.refreshed_at.isoformat() if dossier.freshness.refreshed_at else None,
        }
        print(json.dumps(payload, indent=2))
        return 0 if gate.passed else 2

    configured_tickers = [
        str(m.market_ticker)
        for m in container.weather_directory.all()
        if getattr(m, "market_ticker", None)
    ]
    records = await repo.list_research_dossiers(limit=max(len(configured_tickers) * 4, 200))
    await session.commit()
    by_ticker = {r.market_ticker: r.payload or {} for r in records}
    rows = []
    for t in configured_tickers:
        d = by_ticker.get(t, {})
        gate_d = d.get("gate") or {}
        tc = d.get("trader_context") or {}
        summary_d = d.get("summary") or {}
        rows.append({
            "ticker": t,
            "gate_passed": bool(gate_d.get("passed")),
            "gate_reasons": list(gate_d.get("reasons") or []),
            "fair_yes_dollars": str(tc.get("fair_yes_dollars") or ""),
            "confidence": _float_or_none(summary_d.get("research_confidence")),
        })
    rows.sort(key=lambda r: (0 if r["gate_passed"] else 1, -(r["confidence"] or 0.0)))
    print(json.dumps(rows, indent=2))
    return 0


async def _run_repair_stop_loss_checkpoints_command(
    args: argparse.Namespace,
    container: AppContainer,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    refreshed = await refresh_stop_loss_checkpoints(
        repo,
        settings=container.settings,
        kalshi_env=container.settings.kalshi_env,
        subaccount=container.settings.kalshi_subaccount,
        market_tickers=args.market_tickers or None,
        log_repairs=True,
    )
    await session.commit()
    print(
        json.dumps(
            [
                {
                    "market_ticker": item.market_ticker,
                    "outcome_status": item.outcome_status,
                    "repaired": item.repaired,
                }
                for item in refreshed
            ],
            indent=2,
        )
    )
    return 0


async def _run_create_web_user_command(
    args: argparse.Namespace,
    repo: PlatformRepository,
    session: AsyncSession,
) -> int:
    from kalshi_bot.web.auth import hash_password, normalize_auth_email

    email = normalize_auth_email(args.email)
    password_hash, password_salt = hash_password(args.password)
    existing = await repo.get_web_user_by_email(email)
    if existing is not None:
        existing.password_hash = password_hash
        existing.password_salt = password_salt
        existing.is_active = True
        await session.commit()
        print(json.dumps({"action": "updated", "email": email}))
    else:
        await repo.create_web_user(
            email=email,
            password_hash=password_hash,
            password_salt=password_salt,
        )
        await session.commit()
        print(json.dumps({"action": "created", "email": email}))
    return 0


async def _run_cli(args: argparse.Namespace) -> int:
    if args.command == "baseline-model-card":
        result = write_baseline_model_card(
            historical_path=Path(args.historical),
            shadow_path=Path(args.shadow),
            output_path=Path(args.output),
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "kalshi-leaderboard":
        previous_kalshi_env = os.environ.get("KALSHI_ENV")
        if args.kalshi_env:
            os.environ["KALSHI_ENV"] = args.kalshi_env
            get_settings.cache_clear()
        settings = get_settings()
        kalshi = KalshiClient(settings)
        scraper = KalshiLeaderboardScraper(settings, kalshi)
        try:
            snapshot = await scraper.fetch(
                name=args.name,
                time_window=args.time,
                category=args.category,
                limit=args.limit,
                source=args.source,
                require_auth=not args.allow_unsigned,
            )
            if args.format == "csv":
                print(leaderboard_snapshot_to_csv(snapshot), end="")
            else:
                print(json.dumps(snapshot.to_dict(), indent=2))
            return 0
        finally:
            await scraper.close()
            await kalshi.close()
            if args.kalshi_env:
                if previous_kalshi_env is None:
                    os.environ.pop("KALSHI_ENV", None)
                else:
                    os.environ["KALSHI_ENV"] = previous_kalshi_env
                get_settings.cache_clear()

    crypto_env_override = _crypto_cli_env_override(args)
    previous_kalshi_env = os.environ.get("KALSHI_ENV")
    if crypto_env_override:
        os.environ["KALSHI_ENV"] = crypto_env_override
        get_settings.cache_clear()
    try:
        container = await AppContainer.build(bootstrap_db=args.command not in {"init-db", "trading-audit", "trade-analysis", "overnight-readiness"})
    except Exception:
        if crypto_env_override:
            if previous_kalshi_env is None:
                os.environ.pop("KALSHI_ENV", None)
            else:
                os.environ["KALSHI_ENV"] = previous_kalshi_env
            get_settings.cache_clear()
        raise
    _apply_crypto_cli_env(args, container)
    try:
        if args.command == "init-db":
            await init_models(container.engine)
            print("database initialized")
            return 0

        if args.command == "discover":
            discoveries = await container.discovery_service.discover_configured_markets()
            if args.json:
                print(
                    json.dumps(
                        [
                            {
                                "market_ticker": item.mapping.market_ticker,
                                "station_id": item.mapping.station_id,
                                "status": item.status,
                                "yes_bid_dollars": str(item.yes_bid_dollars) if item.yes_bid_dollars is not None else None,
                                "yes_ask_dollars": str(item.yes_ask_dollars) if item.yes_ask_dollars is not None else None,
                                "no_ask_dollars": str(item.no_ask_dollars) if item.no_ask_dollars is not None else None,
                                "can_trade": item.can_trade,
                                "notes": item.notes,
                            }
                            for item in discoveries
                        ],
                        indent=2,
                    )
                )
            else:
                for item in discoveries:
                    print(
                        f"{item.mapping.market_ticker} status={item.status} "
                        f"yes_bid={item.yes_bid_dollars} yes_ask={item.yes_ask_dollars} "
                        f"can_trade={item.can_trade} notes={'; '.join(item.notes) or 'ok'}"
                    )
            return 0

        if args.command == "stream":
            markets = args.markets or await container.discovery_service.list_stream_markets()
            processed = await container.stream_service.stream(
                market_tickers=markets,
                include_private=not args.public_only,
                max_messages=args.max_messages,
                on_market_update=container.auto_trigger_service.handle_market_update if args.auto_trigger else None,
            )
            if args.auto_trigger:
                await container.auto_trigger_service.wait_for_tasks()
            print(json.dumps({"processed_messages": processed, "markets": markets}, indent=2))
            return 0

        if args.command == "daemon":
            result = await container.daemon_service.run(
                markets=args.markets,
                public_only=args.public_only,
                auto_trigger=(False if args.no_auto_trigger else True) if args.auto_trigger or args.no_auto_trigger else None,
                max_messages=args.max_messages,
                run_seconds=args.run_seconds,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "crypto-history":
            return await _run_crypto_history_command(args, container)

        if args.command == "crypto-spot":
            return await _run_crypto_spot_command(args, container)

        if args.command == "crypto-model":
            return await _run_crypto_model_command(args, container)

        if args.command == "crypto-replay":
            return await _run_crypto_replay_command(args, container)

        if args.command == "crypto-status":
            return await _run_crypto_status_command(container)

        if args.command == "crypto-autonomy":
            return await _run_crypto_autonomy_command(args, container)

        if args.command == "crypto-asset-mode":
            return await _run_crypto_asset_mode_command(args, container)

        if args.command == "model-quality":
            report = await build_model_quality_report(
                settings=container.settings,
                session_factory=container.session_factory,
                decision_corpus_service=container.decision_corpus_service,
                trading_audit_service=container.trading_audit_service,
                trade_analysis_service=container.trade_analysis_service,
                crypto_market_service=container.crypto_market_service,
                crypto_forecast_service=container.crypto_forecast_service,
                crypto_replay_service=container.crypto_replay_service,
                kalshi_env=args.kalshi_env,
                domain=args.domain,
                days=args.days,
                frequency=args.frequency,
                persist=args.persist,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_model_quality_report(report))
            return 0

        if args.command == "overnight-readiness":
            report = await OvernightReadinessService(
                settings=container.settings,
                session_factory=container.session_factory,
                trade_analysis_service=container.trade_analysis_service,
                trading_audit_service=container.trading_audit_service,
                crypto_asset_control_service=container.crypto_asset_control_service,
                has_write_credentials=container.kalshi.write_credentials is not None,
            ).build_report(
                kalshi_env=args.kalshi_env,
                domains=args.domains,
                timezone_name=args.timezone,
                start_hour=args.start_hour,
                end_hour=args.end_hour,
                days=args.days,
                frequency=args.frequency,
                weather_analysis_mode=args.weather_analysis_mode,
                limit=args.limit,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_overnight_readiness_report(report))
            return 0

        if args.command == "weather-prediction":
            if args.weather_prediction_command == "evaluate":
                result = await container.weather_prediction_service.evaluate(series=args.series or None)
            elif args.weather_prediction_command == "station-diagnostics":
                result = await container.weather_prediction_service.station_diagnostics(min_days=args.min_days)
            else:  # pragma: no cover - argparse enforces choices
                raise ValueError(f"unknown weather-prediction command {args.weather_prediction_command}")
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "weather-sigma":
            if args.weather_sigma_command == "refit":
                result = await container.weather_prediction_service.refit_sigma(
                    version=args.version,
                    dry_run=args.dry_run,
                )
            elif args.weather_sigma_command == "status":
                snapshot = await container.sigma_resolver.snapshot(force=True)
                result = {
                    "sigma_calibration_enabled": container.settings.sigma_calibration_enabled,
                    "station_sigma_row_count": len(snapshot.sigma_params),
                    "lead_factor_count": len(snapshot.lead_factors),
                    "loaded_at": snapshot.loaded_at.isoformat(),
                    "lead_factors": snapshot.lead_factors,
                    "qualifying_station_sigma_row_count": sum(
                        1
                        for params in snapshot.sigma_params.values()
                        if params.get("sample_count", 0) >= container.settings.sigma_min_samples_beats_global
                        and (params.get("crps_improvement_vs_global") or 0.0)
                        > container.settings.sigma_min_crps_improvement
                    ),
                }
            else:  # pragma: no cover - argparse enforces choices
                raise ValueError(f"unknown weather-sigma command {args.weather_sigma_command}")
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "weather-residual":
            if args.weather_residual_command == "train":
                result = await container.weather_prediction_service.train_residual_model(
                    kalshi_env=args.kalshi_env,
                    dry_run=args.dry_run,
                )
            elif args.weather_residual_command == "evaluate":
                result = await container.weather_prediction_service.train_residual_model(
                    kalshi_env=args.kalshi_env,
                    dry_run=True,
                )
            elif args.weather_residual_command == "status":
                result = await container.weather_prediction_service.status(kalshi_env=args.kalshi_env)
            else:  # pragma: no cover - argparse enforces choices
                raise ValueError(f"unknown weather-residual command {args.weather_residual_command}")
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "weather-intraday":
            if args.weather_intraday_command == "evaluate":
                result = await container.weather_prediction_service.train_intraday_model(
                    kalshi_env=args.kalshi_env,
                    dry_run=True,
                    series=args.series or None,
                )
            elif args.weather_intraday_command == "train":
                result = await container.weather_prediction_service.train_intraday_model(
                    kalshi_env=args.kalshi_env,
                    dry_run=False,
                    series=args.series or None,
                )
            elif args.weather_intraday_command == "status":
                result = await container.weather_prediction_service.status(kalshi_env=args.kalshi_env)
            else:  # pragma: no cover - argparse enforces choices
                raise ValueError(f"unknown weather-intraday command {args.weather_intraday_command}")
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "research-refresh":
            dossier = await container.research_coordinator.refresh_market_dossier(
                args.market_ticker,
                trigger_reason="cli_refresh",
                force=True,
            )
            print(json.dumps(dossier.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "research-show":
            dossier = await container.research_coordinator.get_latest_dossier(args.market_ticker)
            if dossier is None:
                print(json.dumps({"market_ticker": args.market_ticker, "status": "missing"}, indent=2))
            else:
                print(json.dumps(dossier.model_dump(mode="json"), indent=2))
            return 0

        if args.command == "research-failures":
            failures = await container.research_coordinator.list_failed_runs(limit=args.limit)
            print(json.dumps(failures, indent=2))
            return 0

        if args.command == "research-audit":
            issues = await container.training_corpus_service.research_audit(limit=args.limit)
            print(json.dumps([issue.model_dump(mode="json") for issue in issues], indent=2))
            return 0

        if args.command == "strategy-audit":
            if args.strategy_audit_command == "room":
                result = await container.training_corpus_service.strategy_audit_room(args.room_id)
                print(json.dumps(result.model_dump(mode="json"), indent=2))
                return 0
            if args.strategy_audit_command == "backfill":
                result = await container.training_corpus_service.backfill_strategy_audits(days=args.days, limit=args.limit)
                print(json.dumps(result, indent=2))
                return 0
            if args.strategy_audit_command == "summary":
                result = await container.training_corpus_service.strategy_audit_summary(days=args.days, limit=args.limit)
                print(json.dumps(result.model_dump(mode="json"), indent=2))
                return 0

        if args.command == "training-export":
            room_ids = [args.room_id] if args.room_id else None
            output_path = Path(args.output)
            if args.mode == "bundles":
                bundles = await container.training_export_service.export_room_bundles(
                    room_ids=room_ids,
                    market_ticker=args.market_ticker,
                    limit=args.limit,
                    include_non_complete=args.include_non_complete,
                )
                payload = [bundle.model_dump(mode="json") for bundle in bundles]
            else:
                examples = await container.training_export_service.export_role_training_examples(
                    room_ids=room_ids,
                    market_ticker=args.market_ticker,
                    limit=args.limit,
                    include_non_complete=args.include_non_complete,
                    roles=args.roles,
                )
                payload = [example.model_dump(mode="json") for example in examples]
            _write_jsonl(output_path, payload)
            print(json.dumps({"output": str(output_path), "count": len(payload), "mode": args.mode}, indent=2))
            return 0

        if args.command == "training-status":
            print(json.dumps(await container.training_corpus_service.get_status(persist_readiness=True), indent=2))
            return 0

        if args.command == "signals-worth-attention":
            async with container.session_factory() as session:
                service = SignalAttentionService(container.settings)
                rows = service.detect_patterns(
                    await service.load_rows(
                        session,
                        kalshi_env=args.kalshi_env,
                        lookback_hours=args.lookback_hours,
                    )
                )
            if args.format == "csv":
                print(attention_rows_to_csv(rows), end="")
            else:
                print(
                    json.dumps(
                        {
                            "kalshi_env": args.kalshi_env,
                            "lookback_hours": args.lookback_hours,
                            "patterns": rows,
                        },
                        indent=2,
                    )
                )
            return 0

        if args.command == "gate-learning":
            service = GateLearningService(container.settings)
            if args.gate_learning_command == "report":
                async with container.session_factory() as session:
                    report = await service.build_report(
                        kalshi_env=args.kalshi_env,
                        days=args.days,
                        source=args.source,
                        min_support=args.min_support,
                        session=session,
                        policy_scope=args.policy_scope,
                        series_ticker=args.series_ticker,
                        side=args.side,
                        month=args.month,
                        lane=args.lane,
                        episode_level=args.episode_level,
                    )
                if args.format == "json":
                    print(json.dumps(report, indent=2))
                else:
                    print(format_gate_learning_report(report))
                return 0
            if args.gate_learning_command == "recommend":
                report = await service.build_recommendation_report(
                    kalshi_env=args.kalshi_env,
                    days=args.days,
                    source=args.source,
                    min_support=args.min_support,
                    policy_scope=args.policy_scope,
                    series_ticker=args.series_ticker,
                    side=args.side,
                    month=args.month,
                    lane=args.lane,
                    episode_level=args.episode_level,
                )
                if args.format == "json":
                    print(json.dumps(report, indent=2))
                else:
                    print(format_gate_recommendation_report(report))
                return 0
            raise ValueError(f"unknown gate-learning command {args.gate_learning_command}")

        if args.command == "autonomous-gates":
            if args.autonomous_gates_command == "status":
                payload = await container.autonomous_gate_tuning_service.status(
                    kalshi_env=args.kalshi_env,
                    domain=args.domain,
                    policy_scope=args.scope,
                    series_ticker=args.series_ticker,
                    side=args.side,
                    month=args.month,
                    lane=args.lane,
                )
            elif args.autonomous_gates_command == "run":
                payload = await container.autonomous_gate_tuning_service.run(
                    kalshi_env=args.kalshi_env,
                    source=args.source,
                    days=args.days,
                    min_support=args.min_support,
                    dry_run=args.dry_run,
                    triggered_by="cli",
                    domain=args.domain,
                    policy_scope=args.scope,
                    series_ticker=args.series_ticker,
                    side=args.side,
                    month=args.month,
                    lane=args.lane,
                    bootstrap_promote_from_historical=args.bootstrap_promote_from_historical,
                )
            else:
                raise ValueError(f"unknown autonomous-gates command {args.autonomous_gates_command}")
            if args.format == "json":
                print(json.dumps(payload, indent=2, default=str))
            else:
                print(json.dumps(payload, indent=2, default=str))
            return 0

        if args.command == "decision-policy-variants-audit":
            async with container.session_factory() as session:
                stmt = (
                    select(Signal, Room)
                    .join(Room, Signal.room_id == Room.id)
                    .where(Room.kalshi_env == args.kalshi_env)
                    .order_by(Signal.updated_at.desc(), Signal.id.desc())
                    .limit(args.limit)
                )
                result = await session.execute(stmt)
                service = DecisionPolicyVariantService(container.settings)
                detail_rows: list[dict[str, object]] = []
                baseline_counts: dict[str, int] = {}
                shadow_counts: dict[str, int] = {}
                live_counts: dict[str, int] = {}
                market_sets: dict[str, set[str]] = {}
                for signal, room in result.all():
                    payload = signal.payload if isinstance(signal.payload, dict) else {}
                    trace = payload.get("candidate_trace") if isinstance(payload.get("candidate_trace"), dict) else {}
                    candidates = [item for item in trace.get("candidates") or [] if isinstance(item, dict)]
                    baseline = _policy_audit_best_block_reason(trace)
                    if baseline:
                        baseline_counts[baseline] = baseline_counts.get(baseline, 0) + 1
                    policy_result = service.evaluate_candidate_variants(candidates)
                    matched = sorted(policy_result.policy_variants)
                    live = sorted(
                        key for key, value in policy_result.policy_variants.items()
                        if isinstance(value, dict) and value.get("live_enabled")
                    )
                    for key, value in policy_result.policy_variants.items():
                        if value.get("shadow_enabled"):
                            shadow_counts[key] = shadow_counts.get(key, 0) + 1
                        if value.get("live_enabled"):
                            live_counts[key] = live_counts.get(key, 0) + 1
                        market_sets.setdefault(key, set()).add(signal.market_ticker)
                    detail_rows.append(
                        {
                            "market_ticker": signal.market_ticker,
                            "room_id": room.id,
                            "updated_at": signal.updated_at.isoformat() if signal.updated_at is not None else "",
                            "baseline_block_reason": baseline or "",
                            "matched_variants": ",".join(matched),
                            "live_variants": ",".join(live),
                        }
                    )
            if args.format == "csv":
                print(_policy_audit_csv(detail_rows), end="")
            else:
                print(
                    json.dumps(
                        {
                            "kalshi_env": args.kalshi_env,
                            "limit": args.limit,
                            "baseline": {"blockers": baseline_counts},
                            "shadow": {"would_have_entered_by_variant": shadow_counts},
                            "live": {"would_have_entered_by_variant": live_counts},
                            "unique_markets_by_variant": {
                                key: len(value) for key, value in sorted(market_sets.items())
                            },
                            "rows": detail_rows,
                        },
                        indent=2,
                    )
                )
            return 0

        if args.command == "trading-audit":
            audit_days = 3650 if args.full_history else args.days
            if args.trading_audit_command == "repair":
                if args.repair_target == "stale-positions":
                    result = await container.trading_audit_service.repair_stale_positions(
                        kalshi_env=args.kalshi_env,
                        dry_run=args.dry_run,
                        limit=args.limit,
                        subaccount=container.settings.kalshi_subaccount,
                    )
                elif args.repair_target == "market-snapshots":
                    result = await container.trading_audit_service.repair_market_snapshots(
                        kalshi_env=args.kalshi_env,
                        days=audit_days,
                        dry_run=args.dry_run,
                        limit=args.limit,
                    )
                else:
                    result = await container.trading_audit_service.repair_attribution(
                        kalshi_env=args.kalshi_env,
                        days=audit_days,
                        dry_run=args.dry_run,
                        limit=args.limit,
                    )
                print(json.dumps(result, indent=2))
                return 0
            report = await container.trading_audit_service.build_report(
                kalshi_env=args.kalshi_env,
                days=audit_days,
                focus=args.focus,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_trading_audit_text(report))
            return 0

        if args.command == "trade-analysis":
            analysis_days = 3650 if args.full_history else args.days
            if args.trade_analysis_command == "dataset":
                result = await container.trade_analysis_service.write_dataset(
                    kalshi_env=args.kalshi_env,
                    days=analysis_days,
                    output=Path(args.output),
                    limit=args.limit,
                )
                print(json.dumps(result, indent=2))
                return 0
            if args.trade_analysis_command == "model-eval":
                result = await container.trade_analysis_service.model_eval(
                    dataset_path=Path(args.dataset),
                )
                print(json.dumps(result, indent=2))
                return 0
            report = await container.trade_analysis_service.build_report(
                kalshi_env=args.kalshi_env,
                days=analysis_days,
                limit=args.limit,
                buckets=args.buckets,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_trade_analysis_report(report))
            return 0

        if args.command == "trade-behavior":
            behavior_days = 3650 if args.full_history else args.days
            if args.trade_behavior_command == "quality":
                report = await build_trade_behavior_quality_report(
                    settings=container.settings,
                    trading_audit_service=container.trading_audit_service,
                    trade_analysis_service=container.trade_analysis_service,
                    kalshi_env=args.kalshi_env,
                    days=behavior_days,
                    min_samples=args.min_samples,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(report, indent=2))
                else:
                    print(format_trade_behavior_quality_report(report))
            else:
                report = await build_trade_behavior_validation_report(
                    settings=container.settings,
                    session_factory=container.session_factory,
                    watchdog_service=container.watchdog_service,
                    trading_audit_service=container.trading_audit_service,
                    trade_analysis_service=container.trade_analysis_service,
                    kalshi_env=args.kalshi_env,
                    days=behavior_days,
                    since_hours=args.since_hours,
                    mode=args.mode,
                )
                if args.json:
                    print(json.dumps(report, indent=2))
                else:
                    print(format_trade_behavior_validation_report(report))
            return 0

        if args.command == "modeling":
            modeling_days = 3650 if args.full_history else args.days
            modeling_row_limit = None if args.source == "gate-learning-bundles" else args.limit if args.limit and args.limit > 0 else None
            report = await build_modeling_report(
                settings=container.settings,
                decision_corpus_service=container.decision_corpus_service,
                trading_audit_service=container.trading_audit_service,
                trade_analysis_service=container.trade_analysis_service,
                kalshi_env=args.kalshi_env,
                days=modeling_days,
                command=args.modeling_command,
                dataset_source=args.source,
                limit=args.limit,
                row_limit=modeling_row_limit,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_modeling_report(report))
            if args.modeling_command == "validate" and report.get("status") == "fail":
                return 1
            return 0

        if args.command == "backtesting":
            backtesting_days = 3650 if args.full_history else args.days
            backtesting_row_limit = (
                None
                if args.dataset_source == "gate-learning-bundles"
                else args.limit if args.limit and args.limit > 0 else None
            )
            report = await build_backtesting_report(
                settings=container.settings,
                session_factory=container.session_factory,
                decision_corpus_service=container.decision_corpus_service,
                trade_analysis_service=container.trade_analysis_service,
                kalshi_env=args.kalshi_env,
                days=backtesting_days,
                full_history=bool(args.full_history),
                command=args.backtesting_command,
                dataset_source=args.dataset_source,
                limit=args.limit,
                row_limit=backtesting_row_limit,
            )
            if args.output:
                write_backtesting_report(report, Path(args.output))
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_backtesting_report(report))
            if args.backtesting_command == "validate" and report.get("status") == "fail":
                return 1
            return 0

        if args.command == "training-build":
            if getattr(args, "training_build_scope", None) == "historical":
                if not args.date_from or not args.date_to:
                    raise ValueError("training-build historical requires --date-from and --date-to")
                if args.mode not in {"bundles", "role-sft", "decision-eval", "outcome-eval", "gemini-finetune"}:
                    raise ValueError("training-build historical supports bundles, role-sft, decision-eval, outcome-eval, or gemini-finetune")
                request = HistoricalTrainingBuildRequest(
                    mode=args.mode,
                    limit=args.limit,
                    date_from=args.date_from,
                    date_to=args.date_to,
                    series=args.series or [],
                    quality_cleaned_only=args.quality_cleaned_only,
                    include_pathology_examples=args.include_pathology_examples,
                    require_full_checkpoints=args.require_full_checkpoints,
                    late_only_ok=args.late_only_ok,
                    origins=args.origins or [RoomOrigin.HISTORICAL_REPLAY.value],
                    output=args.output,
                )
                print(json.dumps(await container.historical_training_service.build_historical_dataset(request), indent=2))
                return 0
            if args.mode not in {"room-bundles", "role-sft", "evaluation-holdout"}:
                raise ValueError("training-build supports room-bundles, role-sft, or evaluation-holdout")
            request = TrainingBuildRequest(
                mode=args.mode,
                limit=args.limit,
                days=args.days,
                settled_only=args.settled_only,
                include_non_complete=args.include_non_complete,
                good_research_only=args.good_research_only,
                quality_cleaned_only=args.quality_cleaned_only,
                market_ticker=args.market_ticker,
                origins=args.origins,
                output=args.output,
            )
            print(json.dumps(await container.training_corpus_service.build_dataset(request), indent=2))
            return 0

        if args.command == "training-backfill":
            return await _run_training_backfill_command(args, container)

        if args.command == "historical-status":
            print(json.dumps(await container.historical_training_service.get_status(verbose=args.verbose), indent=2))
            return 0

        if args.command == "historical-pipeline":
            if args.historical_pipeline_command == "status":
                print(json.dumps(await container.historical_pipeline_service.status(verbose=args.verbose), indent=2))
                return 0
            if args.historical_pipeline_command == "bootstrap":
                print(
                    json.dumps(
                        await container.historical_pipeline_service.bootstrap(
                            days=args.days,
                            series=args.series or None,
                            chunk_days=args.chunk_days,
                        ),
                        indent=2,
                    )
                )
                return 0
            if args.historical_pipeline_command == "resume":
                print(
                    json.dumps(
                        await container.historical_pipeline_service.resume(series=args.series or None),
                        indent=2,
                    )
                )
                return 0
            if args.historical_pipeline_command == "daily":
                print(
                    json.dumps(
                        await container.historical_pipeline_service.daily(series=args.series or None),
                        indent=2,
                    )
                )
                return 0

        if args.command == "historical-intelligence":
            if args.historical_intelligence_command == "status":
                print(json.dumps(await container.historical_intelligence_service.get_status(), indent=2))
                return 0
            if args.historical_intelligence_command == "run":
                print(
                    json.dumps(
                        await container.historical_intelligence_service.run(
                            HistoricalIntelligenceRunRequest(
                                date_from=args.date_from,
                                date_to=args.date_to,
                                origins=args.origins or [RoomOrigin.HISTORICAL_REPLAY.value],
                                auto_promote=args.auto_promote,
                            )
                        ),
                        indent=2,
                    )
                )
                return 0
            if args.historical_intelligence_command == "explain":
                print(
                    json.dumps(
                        await container.historical_intelligence_service.explain(series=args.series or None),
                        indent=2,
                    )
                )
                return 0

        if args.command == "heuristic-pack":
            if args.heuristic_pack_command == "status":
                print(json.dumps(await container.historical_intelligence_service.get_status(), indent=2))
                return 0
            if args.heuristic_pack_command == "promote":
                print(
                    json.dumps(
                        await container.historical_intelligence_service.promote(
                            candidate_version=HeuristicPackPromoteRequest(
                                candidate_version=args.candidate_version,
                                reason=args.reason,
                            ).candidate_version,
                            reason=args.reason,
                        ),
                        indent=2,
                    )
                )
                return 0
            if args.heuristic_pack_command == "rollback":
                print(
                    json.dumps(
                        await container.historical_intelligence_service.rollback(
                            reason=HeuristicPackRollbackRequest(reason=args.reason).reason,
                        ),
                        indent=2,
                    )
                )
                return 0

        if args.command == "historical-import" and args.historical_kind == "weather":
            result = await container.historical_training_service.import_weather_history(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-replay" and args.historical_kind == "weather":
            result = await container.historical_training_service.replay_weather_history(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-repair" and args.historical_repair_command == "audit":
            result = await container.historical_training_service.audit_historical_replay(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
                verbose=args.verbose,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-repair" and args.historical_repair_command == "refresh":
            result = await container.historical_training_service.refresh_historical_replay(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-backfill" and args.historical_backfill_kind == "market":
            result = await container.historical_training_service.backfill_market_checkpoints(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-backfill" and args.historical_backfill_kind == "weather-archive":
            result = await container.historical_training_service.backfill_weather_archives(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
                archive_raw_events=not args.import_only,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-backfill" and args.historical_backfill_kind == "forecast-archive":
            result = await container.historical_training_service.backfill_external_forecast_archives(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-backfill" and args.historical_backfill_kind == "settlements":
            result = await container.historical_training_service.backfill_settlements(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-archive" and args.historical_archive_command == "capture":
            result = await container.historical_training_service.capture_weather_archives_once(series=args.series or None)
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-archive" and args.historical_archive_command == "checkpoint-capture":
            result = await container.historical_training_service.capture_checkpoint_archives_once(
                series=args.series or None,
                due_only=bool(args.once),
                source_kind="manual_checkpoint_capture_once",
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "historical-archive" and args.historical_archive_command == "checkpoint-status":
            result = await container.historical_training_service.checkpoint_capture_status(
                date_from=date.fromisoformat(args.date_from),
                date_to=date.fromisoformat(args.date_to),
                series=args.series or None,
                verbose=args.verbose,
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "training-build-list":
            builds = await container.training_corpus_service.list_builds(limit=args.limit)
            print(json.dumps([build.model_dump(mode="json") for build in builds], indent=2))
            return 0

        if args.command == "decision-corpus":
            subcommand = args.decision_corpus_command
            if subcommand == "build":
                result = await container.decision_corpus_service.build(
                    date_from=date.fromisoformat(args.date_from),
                    date_to=date.fromisoformat(args.date_to),
                    source=args.source,
                    dry_run=args.dry_run,
                    notes=args.notes,
                    parent_build_id=args.parent_build_id,
                    kalshi_env=container.settings.kalshi_env,
                )
                print(json.dumps(result, indent=2))
                return 0
            if subcommand == "list-builds":
                builds = await container.decision_corpus_service.list_builds(
                    status=args.status,
                    date_from=date.fromisoformat(args.date_from) if args.date_from else None,
                    date_to=date.fromisoformat(args.date_to) if args.date_to else None,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(builds, indent=2))
                else:
                    for build in builds:
                        print(
                            f"{build['id']} status={build['status']} rows={build['row_count']} "
                            f"range={build['date_from']}..{build['date_to']} version={build['version']} "
                            f"created={build['created_at']} finished={build['finished_at']} git={build['git_sha']}"
                        )
                return 0
            if subcommand == "inspect-build":
                result = await container.decision_corpus_service.inspect_build(args.build_id)
                print(json.dumps(result, indent=2))
                return 0
            if subcommand == "validate":
                result = await container.decision_corpus_service.validate_build(args.build_id)
                print(json.dumps(result, indent=2))
                return 0 if result.get("ok") else 1
            if subcommand == "promote":
                result = await container.decision_corpus_service.promote(
                    args.build_id,
                    kalshi_env=args.env,
                    actor=args.actor,
                )
                print(json.dumps(result, indent=2))
                return 0
            if subcommand == "current":
                result = await container.decision_corpus_service.current(kalshi_env=args.env)
                print(json.dumps(result, indent=2))
                return 0 if result.get("status") == "ok" else 1
            if subcommand == "calibration-report":
                result = await container.decision_corpus_calibration_service.calibration_report(
                    build_id=args.build_id,
                    kalshi_env=args.env,
                    output=Path(args.output),
                )
                return int(result.get("exit_code", 0))

        if args.command == "strategy-regression":
            subcommand = args.strategy_regression_command
            if subcommand == "rank":
                try:
                    result = await container.strategy_regression_ranking_service.rank_report(
                        build_id=args.build_id,
                        kalshi_env=args.env,
                        output=Path(args.output),
                    )
                except (ValueError, KeyError) as exc:
                    message = exc.args[0] if exc.args else str(exc)
                    print(json.dumps({"error": message}, indent=2), file=sys.stderr)
                    return 2
                return int(result.get("exit_code", 0))

        if args.command == "self-improve":
            action = args.self_improve_command
            if action == "status":
                print(json.dumps(await container.self_improve_service.get_status(), indent=2))
                return 0
            if action == "critique":
                result = await container.self_improve_service.critique_recent_rooms(days=args.days, limit=args.limit)
                print(json.dumps(result.payload, indent=2))
                return 0
            if action == "eval":
                result = await container.self_improve_service.evaluate_candidate(
                    candidate_version=args.candidate_version,
                    days=args.days,
                    limit=args.limit,
                )
                print(json.dumps(result.payload, indent=2))
                return 0
            if action == "promote":
                result = await container.self_improve_service.promote_candidate(
                    evaluation_run_id=args.evaluation_run_id,
                    reason=args.reason,
                )
                print(json.dumps(result.payload, indent=2))
                return 0
            if action == "rollback":
                result = await container.self_improve_service.rollback(reason=args.reason)
                print(json.dumps(result.payload, indent=2))
                return 0

        if args.command == "health-check":
            return await _run_health_check_command(args, container)

        if args.command == "watchdog":
            return await _run_watchdog_command(args, container)

        if args.command == "decision-trace":
            return await _run_decision_trace_command(args, container)

        if args.command == "parameter-pack":
            return await _run_parameter_pack_command(args, container)

        if args.command == "shadow-run":
            result = await container.shadow_training_service.run_shadow_room(
                args.market_ticker,
                name=args.name,
                prompt=args.prompt,
                reason=args.reason,
            )
            payload = _shadow_run_payload(result)
            if result.decision_trace_id is None:
                print(
                    json.dumps(
                        {
                            **payload,
                            "error": "Shadow run completed without a deterministic decision trace",
                        },
                        indent=2,
                    ),
                    file=sys.stderr,
                )
                return 1
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "shadow-c-sweep":
            signals = await container.strategy_cleanup_service.sweep()
            print(
                json.dumps(
                    [
                        {
                            "ticker": s.ticker,
                            "station": s.station,
                            "resolution_state": s.resolution_state.value,
                            "observed_max_f": s.observed_max_f,
                            "threshold_f": s.threshold_f,
                            "edge_cents": s.edge_cents,
                            "target_price_cents": s.target_price_cents,
                            "side": s.side.value,
                            "shadow": s.shadow,
                            "suppression_reason": s.suppression_reason,
                        }
                        for s in signals
                    ],
                    indent=2,
                )
            )
            return 0

        if args.command == "strategy-c-status":
            status = await container.strategy_cleanup_service.get_status()
            print(json.dumps(status, indent=2, default=str))
            return 0

        if args.command == "monotonicity-scan":
            proposals = await container.monotonicity_arb_service.sweep()
            print(
                json.dumps(
                    [
                        {
                            "station": p.station,
                            "event_date": str(p.event_date),
                            "ticker_low": p.ticker_low,
                            "ticker_high": p.ticker_high,
                            "threshold_low_f": p.threshold_low_f,
                            "threshold_high_f": p.threshold_high_f,
                            "ask_yes_low_cents": p.ask_yes_low_cents,
                            "ask_no_high_cents": p.ask_no_high_cents,
                            "total_cost_cents": p.total_cost_cents,
                            "gross_edge_cents": p.gross_edge_cents,
                            "fee_estimate_cents": p.fee_estimate_cents,
                            "net_edge_cents": p.net_edge_cents,
                            "contracts_proposed": p.contracts_proposed,
                            "execution_outcome": p.execution_outcome,
                            "suppression_reason": p.suppression_reason,
                        }
                        for p in proposals
                    ],
                    indent=2,
                )
            )
            return 0

        if args.command == "monotonicity-status":
            status = await container.monotonicity_arb_service.get_status()
            print(json.dumps(status, indent=2, default=str))
            return 0

        if args.command == "record-strategy-promotion":
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                try:
                    event = await repo.record_strategy_promotion(
                        strategy=args.strategy,
                        from_state=args.from_state,
                        to_state=args.to_state,
                        actor=args.actor,
                        evidence_ref=args.evidence_ref,
                        notes=args.notes,
                        kalshi_env=container.settings.kalshi_env,
                    )
                except ValueError as exc:
                    print(json.dumps({"error": str(exc)}), file=sys.stderr)
                    return 2
                await session.commit()
            print(json.dumps({
                "id": event.id,
                "strategy": event.strategy,
                "from_state": event.from_state,
                "to_state": event.to_state,
                "actor": event.actor,
                "kalshi_env": event.kalshi_env,
                "created_at": event.created_at.isoformat(),
            }, indent=2))
            return 0

        if args.command == "list-strategy-promotions":
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                events = await repo.list_strategy_promotions(
                    strategy=args.strategy,
                    kalshi_env=container.settings.kalshi_env,
                    limit=args.limit,
                )
            print(json.dumps([
                {
                    "id": e.id,
                    "strategy": e.strategy,
                    "from_state": e.from_state,
                    "to_state": e.to_state,
                    "actor": e.actor,
                    "evidence_ref": e.evidence_ref,
                    "notes": e.notes,
                    "kalshi_env": e.kalshi_env,
                    "created_at": e.created_at.isoformat(),
                }
                for e in events
            ], indent=2))
            return 0

        if args.command == "ignore-strategy-promotion-secondary-status":
            fields = list(dict.fromkeys(args.field))
            promotion_ids = list(dict.fromkeys(args.promotion_id or []))
            if args.all and promotion_ids:
                raise ValueError("Use either --all or --promotion-id, not both")
            if not args.all and not promotion_ids:
                raise ValueError("Provide --promotion-id or --all")
            if (args.all or len(promotion_ids) > 1) and not args.kalshi_env:
                raise ValueError("Bulk secondary status ignore requires explicit --kalshi-env")

            resolution = _secondary_ignore_resolution(resolved_by=args.resolved_by, note=args.note)
            updated: list[dict[str, object]] = []
            async with container.session_factory() as session:
                repo = PlatformRepository(session, kalshi_env=args.kalshi_env or container.settings.kalshi_env)
                if args.all:
                    status_filters = []
                    if "secondary_sync_status" in fields:
                        status_filters.append(
                            StrategyPromotionRecord.secondary_sync_status.in_(["pending", "failed"])
                        )
                    if "secondary_rollback_status" in fields:
                        status_filters.append(
                            StrategyPromotionRecord.secondary_rollback_status.in_(["pending", "failed"])
                        )
                    stmt = (
                        select(StrategyPromotionRecord)
                        .where(
                            StrategyPromotionRecord.kalshi_env == args.kalshi_env,
                            or_(*status_filters),
                        )
                        .order_by(StrategyPromotionRecord.id.asc())
                    )
                    records = list((await session.execute(stmt)).scalars())
                    for record in records:
                        record_fields = [
                            field
                            for field in fields
                            if getattr(record, field) in {"pending", "failed"}
                        ]
                        if not record_fields:
                            continue
                        values = _secondary_ignore_update_values(record_fields, resolution)
                        updated_record = await repo.update_strategy_promotion(record.id, **values)
                        updated.append(
                            {
                                "id": updated_record.id,
                                "kalshi_env": updated_record.kalshi_env,
                                "fields": record_fields,
                            }
                        )
                else:
                    values = _secondary_ignore_update_values(fields, resolution)
                    for promotion_id in promotion_ids:
                        updated_record = await repo.update_strategy_promotion(promotion_id, **values)
                        updated.append(
                            {
                                "id": updated_record.id,
                                "kalshi_env": updated_record.kalshi_env,
                                "fields": fields,
                            }
                        )
                await session.commit()
            print(
                json.dumps(
                    {
                        "updated_count": len(updated),
                        "updated": updated,
                        "resolution": resolution,
                    },
                    indent=2,
                )
            )
            return 0

        if args.command == "strategy-promotion-watchdog":
            if args.strategy_promotion_watchdog_command == "evaluate":
                payload = await container.strategy_auto_evolve_service.evaluate_strategy_promotion(
                    args.promotion_id,
                    trigger_source=args.source,
                )
                print(json.dumps(payload, indent=2))
                return 0
            if args.strategy_promotion_watchdog_command == "resolve":
                payload = await container.strategy_auto_evolve_service.resolve_strategy_promotion_insufficient_data(
                    args.promotion_id,
                    action=args.action,
                    resolved_by=args.resolved_by,
                    note=args.note,
                )
                print(json.dumps(payload, indent=2))
                return 0

        if args.command == "strategy-promotion-secondary-sync":
            if args.strategy_promotion_secondary_sync_command == "sweep":
                payload = await container.strategy_auto_evolve_service.sweep_secondary_strategy_promotion_syncs(
                    trigger_source=args.source,
                    limit=args.limit,
                )
                print(json.dumps(payload, indent=2))
                return 0

        if args.command == "shadow-sweep":
            results = await container.shadow_training_service.run_shadow_sweep(
                markets=args.markets,
                limit=args.limit,
                reason=args.reason,
            )
            payload = [_shadow_run_payload(item) for item in results]
            missing_traces = [item for item in payload if item.get("decision_trace_id") is None]
            if missing_traces:
                print(
                    json.dumps(
                        {
                            "results": payload,
                            "error": "Shadow sweep completed with rooms missing deterministic decision traces",
                        },
                        indent=2,
                    ),
                    file=sys.stderr,
                )
                return 1
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "shadow-campaign" and args.shadow_campaign_command == "run":
            request = ShadowCampaignRequest(limit=args.limit, reason=args.reason, domain=args.domain)
            results = await container.shadow_campaign_service.run(request)
            payload = [_shadow_run_payload(item) for item in results]
            missing_traces = [item for item in payload if item.get("decision_trace_id") is None]
            if missing_traces:
                print(
                    json.dumps(
                        {
                            "results": payload,
                            "error": "Shadow campaign completed with rooms missing deterministic decision traces",
                        },
                        indent=2,
                    ),
                    file=sys.stderr,
                )
                return 1
            print(json.dumps(payload, indent=2))
            return 0

        if args.command == "calibrate-momentum":
            from kalshi_bot.services.momentum_calibration import MomentumCalibrationService

            svc = MomentumCalibrationService(container.session_factory, container.kalshi, container.settings)
            sub = args.calibrate_momentum_command
            if sub == "backfill-slopes":
                result = await svc.backfill_slopes(args.date_from, args.date_to)
                print(json.dumps(result, indent=2))
                return 0
            if sub == "preview":
                result = await svc.preview(
                    args.date_from,
                    args.date_to,
                    output_path=Path(args.output) if args.output else None,
                )
                print(json.dumps(result, indent=2))
                return 0
            if sub == "stage":
                result = await svc.stage(
                    args.date_from,
                    args.date_to,
                    min_observations=args.min_observations,
                    staged_by=args.staged_by,
                    force=args.force,
                    output_path=Path(args.output) if args.output else None,
                )
                print(json.dumps(result, indent=2))
                return 0 if result.get("ok") else 1
            if sub == "promote":
                result = await svc.promote(activated_by=args.activated_by)
                print(json.dumps(result, indent=2))
                return 0 if result.get("ok") else 1
            if sub == "reject":
                result = await svc.reject()
                print(json.dumps(result, indent=2))
                return 0
            if sub == "status":
                result = await svc.status()
                print(json.dumps(result, indent=2))
                return 0

        async with container.session_factory() as session:
            repo = PlatformRepository(session)

            if args.command == "create-room":
                return await _run_create_room_command(args, container, repo, session)

            if args.command == "run-room":
                return await _run_run_room_command(args, container, session)

            if args.command == "reconcile":
                return await _run_reconcile_command(container, repo, session)

            if args.command == "promote":
                return await _run_promote_command(args, repo, session)

            if args.command == "kill-switch":
                return await _run_kill_switch_command(args, repo, session)

            if args.command == "status":
                return await _run_status_command(container, repo, session)

            if args.command == "intel":
                return await _run_intel_command(args, container, repo, session)

            if args.command == "repair-stop-loss-checkpoints":
                return await _run_repair_stop_loss_checkpoints_command(args, container, repo, session)

            if args.command == "create-web-user":
                return await _run_create_web_user_command(args, repo, session)

        raise ValueError(f"Unknown command: {args.command}")
    finally:
        await container.close()
        if crypto_env_override:
            if previous_kalshi_env is None:
                os.environ.pop("KALSHI_ENV", None)
            else:
                os.environ["KALSHI_ENV"] = previous_kalshi_env
            get_settings.cache_clear()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kalshi-bot-cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_kalshi_env_argument(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument("--kalshi-env", default=None)

    subparsers.add_parser("init-db")

    kalshi_leaderboard = subparsers.add_parser(
        "kalshi-leaderboard",
        help="Read Kalshi's social leaderboard.",
    )
    kalshi_leaderboard.add_argument("--kalshi-env", choices=["demo", "production"], default="production")
    kalshi_leaderboard.add_argument("--name", choices=LEADERBOARD_NAMES, default="projected_pnl")
    kalshi_leaderboard.add_argument("--time", choices=LEADERBOARD_TIME_WINDOWS, default="daily")
    kalshi_leaderboard.add_argument(
        "--category",
        default="",
        help="Leaderboard category; omit for all categories. Known values: "
        + ", ".join(item or "(all)" for item in LEADERBOARD_CATEGORIES),
    )
    kalshi_leaderboard.add_argument("--limit", type=int, default=100)
    kalshi_leaderboard.add_argument("--source", choices=LEADERBOARD_SOURCES, default="direct")
    kalshi_leaderboard.add_argument("--format", choices=["json", "csv"], default="json")
    kalshi_leaderboard.add_argument(
        "--allow-unsigned",
        action="store_true",
        help="Try the request without read credentials if no Kalshi key is configured.",
    )

    discover = subparsers.add_parser("discover")
    discover.add_argument("--json", action="store_true")

    signals_attention = subparsers.add_parser("signals-worth-attention")
    signals_attention.add_argument("--kalshi-env", default="production")
    signals_attention.add_argument("--lookback-hours", type=int, default=24)
    signals_attention.add_argument("--format", choices=["csv", "json"], default="json")

    policy_audit = subparsers.add_parser("decision-policy-variants-audit")
    policy_audit.add_argument("--kalshi-env", default="production")
    policy_audit.add_argument("--limit", type=int, default=500)
    policy_audit.add_argument("--format", choices=["csv", "json"], default="json")

    gate_learning = subparsers.add_parser("gate-learning")
    gate_learning_subparsers = gate_learning.add_subparsers(dest="gate_learning_command", required=True)
    for name in ("report", "recommend"):
        gate_learning_command = gate_learning_subparsers.add_parser(name)
        gate_learning_command.add_argument("--kalshi-env", default="production")
        gate_learning_command.add_argument("--days", type=int, default=180)
        gate_learning_command.add_argument("--format", choices=["json", "md"], default="json")
        gate_learning_command.add_argument("--source", choices=["historical", "forward-shadow", "combined"], default="combined")
        gate_learning_command.add_argument("--min-support", type=int, default=None)
        gate_learning_command.add_argument("--policy-scope", choices=["global", "cohort", "city"], default="global")
        gate_learning_command.add_argument("--series-ticker", default=None)
        gate_learning_command.add_argument("--side", choices=["yes", "no"], default=None)
        gate_learning_command.add_argument("--month", default=None)
        gate_learning_command.add_argument("--lane", default="entry_gate")
        gate_learning_command.add_argument("--episode-level", action="store_true")

    autonomous_gates = subparsers.add_parser("autonomous-gates")
    autonomous_gate_subparsers = autonomous_gates.add_subparsers(dest="autonomous_gates_command", required=True)
    autonomous_gate_run = autonomous_gate_subparsers.add_parser("run")
    autonomous_gate_run.add_argument("--kalshi-env", default="production")
    autonomous_gate_run.add_argument("--source", choices=["historical", "forward-shadow", "combined"], default=None)
    autonomous_gate_run.add_argument("--days", type=int, default=None)
    autonomous_gate_run.add_argument("--min-support", type=int, default=None)
    autonomous_gate_run.add_argument("--dry-run", action="store_true")
    autonomous_gate_run.add_argument("--domain", choices=["weather", "crypto", "all"], default="all")
    autonomous_gate_run.add_argument("--scope", choices=["global", "cohort", "city"], default="global")
    autonomous_gate_run.add_argument("--series-ticker", default=None)
    autonomous_gate_run.add_argument("--side", choices=["yes", "no"], default=None)
    autonomous_gate_run.add_argument("--month", default=None)
    autonomous_gate_run.add_argument("--lane", default="entry_gate")
    autonomous_gate_run.add_argument(
        "--bootstrap-promote-from-historical",
        action="store_true",
        help="One-time startup path: promote a staged weather candidate using reserved historical holdout evidence.",
    )
    autonomous_gate_run.add_argument("--format", choices=["json"], default="json")
    autonomous_gate_status = autonomous_gate_subparsers.add_parser("status")
    autonomous_gate_status.add_argument("--kalshi-env", default="production")
    autonomous_gate_status.add_argument("--domain", choices=["weather", "crypto", "all"], default="all")
    autonomous_gate_status.add_argument("--scope", choices=["global", "cohort", "city"], default="global")
    autonomous_gate_status.add_argument("--series-ticker", default=None)
    autonomous_gate_status.add_argument("--side", choices=["yes", "no"], default=None)
    autonomous_gate_status.add_argument("--month", default=None)
    autonomous_gate_status.add_argument("--lane", default="entry_gate")
    autonomous_gate_status.add_argument("--format", choices=["json"], default="json")

    stream = subparsers.add_parser("stream")
    stream.add_argument("--markets", nargs="*", default=None)
    stream.add_argument("--public-only", action="store_true")
    stream.add_argument("--max-messages", type=int, default=None)
    stream.add_argument("--auto-trigger", action="store_true")

    daemon = subparsers.add_parser("daemon")
    daemon.add_argument("--markets", nargs="*", default=None)
    daemon.add_argument("--public-only", action="store_true")
    daemon.add_argument("--max-messages", type=int, default=None)
    daemon.add_argument("--run-seconds", type=float, default=None)
    daemon_trigger_group = daemon.add_mutually_exclusive_group()
    daemon_trigger_group.add_argument("--auto-trigger", action="store_true")
    daemon_trigger_group.add_argument("--no-auto-trigger", action="store_true")

    crypto_history = subparsers.add_parser("crypto-history")
    crypto_history_subparsers = crypto_history.add_subparsers(dest="crypto_history_command", required=True)
    crypto_history_bootstrap = crypto_history_subparsers.add_parser("bootstrap")
    add_kalshi_env_argument(crypto_history_bootstrap)
    crypto_history_bootstrap.add_argument("--days", type=int, default=180)
    crypto_history_bootstrap.add_argument("--frequency", default="15m")
    crypto_history_daily = crypto_history_subparsers.add_parser("daily")
    add_kalshi_env_argument(crypto_history_daily)
    crypto_history_daily.add_argument("--frequency", default="15m")
    crypto_history_status = crypto_history_subparsers.add_parser("status")
    add_kalshi_env_argument(crypto_history_status)
    crypto_history_status.add_argument("--frequency", default="15m")
    crypto_history_status.add_argument("--days", type=int, default=0)
    crypto_history_status.add_argument("--json", action="store_true")

    crypto_spot = subparsers.add_parser("crypto-spot")
    crypto_spot_subparsers = crypto_spot.add_subparsers(dest="crypto_spot_command", required=True)
    crypto_spot_backfill = crypto_spot_subparsers.add_parser("backfill")
    add_kalshi_env_argument(crypto_spot_backfill)
    crypto_spot_backfill.add_argument("--days", type=int, default=180)
    crypto_spot_backfill.add_argument("--frequency", default="15m")
    crypto_spot_backfill.add_argument("--assets", nargs="*", default=None)
    crypto_spot_backfill.add_argument("--json", action="store_true")
    crypto_spot_status = crypto_spot_subparsers.add_parser("status")
    add_kalshi_env_argument(crypto_spot_status)
    crypto_spot_status.add_argument("--frequency", default="15m")
    crypto_spot_status.add_argument("--days", type=int, default=0)
    crypto_spot_status.add_argument("--assets", nargs="*", default=None)
    crypto_spot_status.add_argument("--json", action="store_true")

    crypto_model = subparsers.add_parser("crypto-model")
    crypto_model_subparsers = crypto_model.add_subparsers(dest="crypto_model_command", required=True)
    crypto_model_train = crypto_model_subparsers.add_parser("train")
    add_kalshi_env_argument(crypto_model_train)
    crypto_model_train.add_argument("--frequency", default="15m")
    crypto_model_candidates = crypto_model_subparsers.add_parser("candidates")
    add_kalshi_env_argument(crypto_model_candidates)
    crypto_model_candidates.add_argument("--frequency", default="15m")
    crypto_model_candidates.add_argument("--days", type=int, default=30)
    crypto_model_candidates.add_argument("--json", action="store_true")

    crypto_replay = subparsers.add_parser("crypto-replay")
    crypto_replay_subparsers = crypto_replay.add_subparsers(dest="crypto_replay_command", required=True)
    crypto_replay_gate = crypto_replay_subparsers.add_parser("gate")
    add_kalshi_env_argument(crypto_replay_gate)
    crypto_replay_gate.add_argument("--frequency", default="15m")
    for name in ("run", "validate"):
        crypto_replay_command = crypto_replay_subparsers.add_parser(name)
        add_kalshi_env_argument(crypto_replay_command)
        crypto_replay_command.add_argument("--frequency", default="15m")
        crypto_replay_command.add_argument("--days", type=int, default=30)
        crypto_replay_command.add_argument("--limit", type=int, default=0)
        crypto_replay_command.add_argument("--json", action="store_true")

    crypto_status = subparsers.add_parser("crypto-status")
    add_kalshi_env_argument(crypto_status)

    crypto_autonomy = subparsers.add_parser("crypto-autonomy")
    crypto_autonomy_subparsers = crypto_autonomy.add_subparsers(dest="crypto_autonomy_command", required=True)
    crypto_autonomy_run_once = crypto_autonomy_subparsers.add_parser("run-once")
    add_kalshi_env_argument(crypto_autonomy_run_once)
    crypto_autonomy_run_once.add_argument("--frequency", default="15m")
    crypto_autonomy_run_once.add_argument("--json", action="store_true")

    crypto_asset_mode = subparsers.add_parser("crypto-asset-mode")
    crypto_asset_mode_subparsers = crypto_asset_mode.add_subparsers(dest="crypto_asset_mode_command", required=True)
    crypto_asset_mode_list = crypto_asset_mode_subparsers.add_parser("list")
    add_kalshi_env_argument(crypto_asset_mode_list)
    crypto_asset_mode_list.add_argument("--frequency", default="15m")
    crypto_asset_mode_set = crypto_asset_mode_subparsers.add_parser("set")
    add_kalshi_env_argument(crypto_asset_mode_set)
    crypto_asset_mode_set.add_argument("symbol")
    crypto_asset_mode_set.add_argument("mode", choices=["off", "shadow", "live"])

    model_quality = subparsers.add_parser("model-quality")
    model_quality_subparsers = model_quality.add_subparsers(dest="model_quality_command", required=True)
    model_quality_status = model_quality_subparsers.add_parser("status")
    model_quality_status.add_argument("--kalshi-env", choices=["demo", "production"], default="demo")
    model_quality_status.add_argument("--domain", choices=["weather", "crypto", "all"], default="all")
    model_quality_status.add_argument("--days", type=int, default=180)
    model_quality_status.add_argument("--frequency", default="15m")
    model_quality_status.add_argument("--persist", action="store_true")
    model_quality_status.add_argument("--json", action="store_true")

    overnight_readiness = subparsers.add_parser(
        "overnight-readiness",
        help="Read-only live readiness evaluation for Pacific overnight trading.",
    )
    overnight_readiness.add_argument("overnight_readiness_command", nargs="?", choices=["report"], default="report")
    overnight_readiness.add_argument("--kalshi-env", choices=["demo", "production"], default="production")
    overnight_readiness.add_argument("--domains", choices=["weather", "crypto", "all"], default="all")
    overnight_readiness.add_argument("--timezone", default="America/Los_Angeles")
    overnight_readiness.add_argument("--start-hour", type=int, default=22)
    overnight_readiness.add_argument("--end-hour", type=int, default=6)
    overnight_readiness.add_argument("--days", type=int, default=180)
    overnight_readiness.add_argument("--frequency", default="15m")
    overnight_readiness.add_argument("--weather-analysis-mode", choices=["fast", "detailed"], default="fast")
    overnight_readiness.add_argument("--limit", type=int, default=None)
    overnight_readiness.add_argument("--json", action="store_true")

    weather_prediction = subparsers.add_parser("weather-prediction")
    weather_prediction_subparsers = weather_prediction.add_subparsers(dest="weather_prediction_command", required=True)
    weather_prediction_evaluate = weather_prediction_subparsers.add_parser("evaluate")
    weather_prediction_evaluate.add_argument("--series", nargs="*", default=None)
    weather_prediction_subparsers.add_parser("station-diagnostics").add_argument("--min-days", type=int, default=5)

    weather_sigma = subparsers.add_parser("weather-sigma")
    weather_sigma_subparsers = weather_sigma.add_subparsers(dest="weather_sigma_command", required=True)
    weather_sigma_refit = weather_sigma_subparsers.add_parser("refit")
    weather_sigma_refit.add_argument("--version", default=None)
    weather_sigma_refit.add_argument("--dry-run", action="store_true")
    weather_sigma_subparsers.add_parser("status")

    weather_residual = subparsers.add_parser("weather-residual")
    weather_residual_subparsers = weather_residual.add_subparsers(dest="weather_residual_command", required=True)
    for name in ("train", "evaluate"):
        weather_residual_command = weather_residual_subparsers.add_parser(name)
        weather_residual_command.add_argument("--kalshi-env", default="demo")
        weather_residual_command.add_argument("--dry-run", action="store_true")
    weather_residual_status = weather_residual_subparsers.add_parser("status")
    weather_residual_status.add_argument("--kalshi-env", default="demo")

    weather_intraday = subparsers.add_parser("weather-intraday")
    weather_intraday_subparsers = weather_intraday.add_subparsers(dest="weather_intraday_command", required=True)
    for name in ("evaluate", "train"):
        weather_intraday_command = weather_intraday_subparsers.add_parser(name)
        weather_intraday_command.add_argument("--kalshi-env", default="demo")
        weather_intraday_command.add_argument("--series", nargs="*", default=None)
        weather_intraday_command.add_argument("--json", action="store_true")
    weather_intraday_status = weather_intraday_subparsers.add_parser("status")
    weather_intraday_status.add_argument("--kalshi-env", default="demo")
    weather_intraday_status.add_argument("--json", action="store_true")

    create_room = subparsers.add_parser("create-room")
    create_room.add_argument("--name", required=True)
    create_room.add_argument("--market-ticker", required=True)
    create_room.add_argument("--prompt", default=None)

    research_refresh = subparsers.add_parser("research-refresh")
    research_refresh.add_argument("market_ticker")

    research_show = subparsers.add_parser("research-show")
    research_show.add_argument("market_ticker")

    research_failures = subparsers.add_parser("research-failures")
    research_failures.add_argument("--limit", type=int, default=10)

    research_audit = subparsers.add_parser("research-audit")
    research_audit.add_argument("--limit", type=int, default=50)

    strategy_audit = subparsers.add_parser("strategy-audit")
    strategy_audit_subparsers = strategy_audit.add_subparsers(dest="strategy_audit_command", required=True)
    strategy_audit_room = strategy_audit_subparsers.add_parser("room")
    strategy_audit_room.add_argument("room_id")
    strategy_audit_backfill = strategy_audit_subparsers.add_parser("backfill")
    strategy_audit_backfill.add_argument("--days", type=int, default=30)
    strategy_audit_backfill.add_argument("--limit", type=int, default=200)
    strategy_audit_summary = strategy_audit_subparsers.add_parser("summary")
    strategy_audit_summary.add_argument("--days", type=int, default=None)
    strategy_audit_summary.add_argument("--limit", type=int, default=100)

    training_export = subparsers.add_parser("training-export")
    training_export.add_argument("--output", required=True)
    training_export.add_argument("--mode", choices=["bundles", "role-sft"], default="bundles")
    training_export.add_argument("--room-id", default=None)
    training_export.add_argument("--market-ticker", default=None)
    training_export.add_argument("--limit", type=int, default=100)
    training_export.add_argument("--include-non-complete", action="store_true")
    training_export.add_argument(
        "--roles",
        nargs="*",
        default=None,
        choices=["researcher", "president", "trader", "memory_librarian"],
    )

    subparsers.add_parser("training-status")

    trading_audit = subparsers.add_parser(
        "trading-audit",
        help="Read-only money/safety audit of recent trading behavior.",
    )
    trading_audit.add_argument("trading_audit_command", nargs="?", choices=["report", "repair"], default="report")
    trading_audit.add_argument("--kalshi-env", default="production")
    trading_audit.add_argument("--days", type=int, default=7)
    trading_audit.add_argument("--full-history", action="store_true")
    trading_audit.add_argument("--buckets", action="store_true")
    trading_audit.add_argument("--focus", choices=["money-safety"], default="money-safety")
    trading_audit.add_argument("--json", action="store_true")
    trading_audit.add_argument("--limit", type=int, default=500)
    trading_audit.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    trading_audit.add_argument("--repair-target", choices=["attribution", "stale-positions", "market-snapshots"], default="attribution")

    trade_analysis = subparsers.add_parser(
        "trade-analysis",
        help="Build read-only no-leakage trade analysis datasets and baseline model cards",
    )
    trade_analysis.add_argument("trade_analysis_command", choices=["dataset", "report", "model-eval"])
    trade_analysis.add_argument("--kalshi-env", default="production")
    trade_analysis.add_argument("--days", type=int, default=180)
    trade_analysis.add_argument("--full-history", action="store_true")
    trade_analysis.add_argument("--buckets", action="store_true")
    trade_analysis.add_argument("--limit", type=int, default=None)
    trade_analysis.add_argument("--json", action="store_true")
    trade_analysis.add_argument("--output", default="data/trade_analysis.jsonl")
    trade_analysis.add_argument("--dataset", default="data/trade_analysis.jsonl")

    trade_behavior = subparsers.add_parser(
        "trade-behavior",
        help="Validate trade behavior gates, audits, and training coverage.",
    )
    trade_behavior_subparsers = trade_behavior.add_subparsers(dest="trade_behavior_command", required=True)
    trade_behavior_validate = trade_behavior_subparsers.add_parser("validate")
    trade_behavior_validate.add_argument("--kalshi-env", default="production")
    trade_behavior_validate.add_argument("--days", type=int, default=7)
    trade_behavior_validate.add_argument("--full-history", action="store_true")
    trade_behavior_validate.add_argument("--since-hours", type=int, default=24)
    trade_behavior_validate.add_argument("--mode", choices=["fast", "detailed"], default="detailed")
    trade_behavior_validate.add_argument("--json", action="store_true")
    trade_behavior_quality = trade_behavior_subparsers.add_parser("quality")
    trade_behavior_quality.add_argument("--kalshi-env", default="production")
    trade_behavior_quality.add_argument("--days", type=int, default=180)
    trade_behavior_quality.add_argument("--full-history", action="store_true")
    trade_behavior_quality.add_argument("--min-samples", type=int, default=None)
    trade_behavior_quality.add_argument("--limit", type=int, default=20)
    trade_behavior_quality.add_argument("--json", action="store_true")

    modeling = subparsers.add_parser(
        "modeling",
        help="Two-stage prediction calibration and trade-selection shadow workflow.",
    )
    modeling_subparsers = modeling.add_subparsers(dest="modeling_command", required=True)
    for name in ("status", "backtest", "validate", "train-shadow"):
        modeling_command = modeling_subparsers.add_parser(name)
        modeling_command.add_argument("--kalshi-env", default="demo")
        modeling_command.add_argument("--days", type=int, default=180)
        modeling_command.add_argument("--full-history", action="store_true")
        modeling_command.add_argument(
            "--limit",
            type=int,
            default=20,
            help="Bound rows and bucket output; use 0 for an unbounded dataset pass.",
        )
        modeling_command.add_argument("--source", choices=["trade-analysis", "gate-learning-bundles"], default="trade-analysis")
        modeling_command.add_argument("--json", action="store_true")

    backtesting = subparsers.add_parser(
        "backtesting",
        help="Fidelity-first walk-forward backtests and promotion readiness checks.",
    )
    backtesting_subparsers = backtesting.add_subparsers(dest="backtesting_command", required=True)
    for name in ("status", "run", "validate"):
        backtesting_command = backtesting_subparsers.add_parser(name)
        backtesting_command.add_argument("--kalshi-env", default="demo")
        backtesting_command.add_argument("--days", type=int, default=180)
        backtesting_command.add_argument("--full-history", action="store_true")
        backtesting_command.add_argument(
            "--limit",
            type=int,
            default=20,
            help="Bound dataset rows, bucket output, and diagnostics; use 0 for an unbounded dataset pass.",
        )
        backtesting_command.add_argument(
            "--dataset-source",
            choices=["auto", "trade-analysis", "decision-corpus", "gate-learning-bundles"],
            default="auto",
        )
        backtesting_command.add_argument("--json", action="store_true")
        backtesting_command.add_argument("--output", default=None)

    training_build = subparsers.add_parser("training-build")
    training_build.add_argument("training_build_scope", nargs="?", choices=["historical"])
    training_build.add_argument(
        "--mode",
        choices=["room-bundles", "role-sft", "evaluation-holdout", "bundles", "decision-eval", "outcome-eval", "gemini-finetune"],
        default="room-bundles",
    )
    training_build.add_argument("--limit", type=int, default=200)
    training_build.add_argument("--days", type=int, default=30)
    training_build.add_argument("--date-from", default=None)
    training_build.add_argument("--date-to", default=None)
    training_build.add_argument("--series", nargs="*", default=None)
    training_build.add_argument("--settled-only", action="store_true")
    training_build.add_argument("--include-non-complete", action="store_true")
    training_build.add_argument("--good-research-only", action="store_true")
    training_build.add_argument(
        "--quality-cleaned-only",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    training_build.add_argument("--market-ticker", default=None)
    training_build.add_argument("--include-pathology-examples", action="store_true")
    training_build.add_argument(
        "--require-full-checkpoints",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    training_build.add_argument("--late-only-ok", action="store_true")
    training_build.add_argument("--origins", nargs="*", default=None)
    training_build.add_argument("--output", default=None)

    training_build_list = subparsers.add_parser("training-build-list")
    training_build_list.add_argument("--limit", type=int, default=20)

    training_backfill = subparsers.add_parser("training-backfill")
    training_backfill_subparsers = training_backfill.add_subparsers(dest="training_backfill_command", required=True)
    training_backfill_research = training_backfill_subparsers.add_parser("research-health")
    training_backfill_research.add_argument("--origins", nargs="+", default=[RoomOrigin.SHADOW.value])
    training_backfill_research.add_argument("--days", type=int, default=30)
    training_backfill_research.add_argument("--market-prefix", action="append", default=[])
    training_backfill_research.add_argument("--limit", type=int, default=2000)
    training_backfill_research.add_argument("--overwrite", action="store_true")
    training_backfill_research.add_argument("--include-non-complete", action="store_true")

    baseline_model_card = subparsers.add_parser(
        "baseline-model-card",
        help="Write a read-only historical plus shadow baseline model card.",
    )
    baseline_model_card.add_argument("--historical", default="data/training/historical_decision_baseline.jsonl")
    baseline_model_card.add_argument("--shadow", default="data/training/forward_shadow_bundles.jsonl")
    baseline_model_card.add_argument("--output", default="data/training/baseline_model_card.json")

    decision_corpus = subparsers.add_parser("decision-corpus")
    decision_corpus_subparsers = decision_corpus.add_subparsers(dest="decision_corpus_command", required=True)
    decision_corpus_build = decision_corpus_subparsers.add_parser("build")
    decision_corpus_build.add_argument("--date-from", required=True)
    decision_corpus_build.add_argument("--date-to", required=True)
    decision_corpus_build.add_argument("--source", default="historical-replay", choices=["historical-replay"])
    decision_corpus_build.add_argument("--dry-run", action="store_true")
    decision_corpus_build.add_argument("--notes", default=None)
    decision_corpus_build.add_argument("--parent-build-id", default=None)
    decision_corpus_list = decision_corpus_subparsers.add_parser("list-builds")
    decision_corpus_list.add_argument("--status", default=None)
    decision_corpus_list.add_argument("--date-from", default=None)
    decision_corpus_list.add_argument("--date-to", default=None)
    decision_corpus_list.add_argument("--limit", type=int, default=20)
    decision_corpus_list.add_argument("--json", action="store_true")
    decision_corpus_inspect = decision_corpus_subparsers.add_parser("inspect-build")
    decision_corpus_inspect.add_argument("build_id")
    decision_corpus_validate = decision_corpus_subparsers.add_parser("validate")
    decision_corpus_validate.add_argument("build_id")
    decision_corpus_promote = decision_corpus_subparsers.add_parser("promote")
    decision_corpus_promote.add_argument("build_id")
    decision_corpus_promote.add_argument("--env", default="demo")
    decision_corpus_promote.add_argument("--actor", default=None)
    decision_corpus_current = decision_corpus_subparsers.add_parser("current")
    decision_corpus_current.add_argument("--env", default="demo")
    decision_corpus_calibration = decision_corpus_subparsers.add_parser("calibration-report")
    calibration_selector = decision_corpus_calibration.add_mutually_exclusive_group(required=True)
    calibration_selector.add_argument("--env", default=None)
    calibration_selector.add_argument("--build-id", default=None)
    decision_corpus_calibration.add_argument("--output", required=True)

    strategy_regression = subparsers.add_parser("strategy-regression")
    strategy_regression_subparsers = strategy_regression.add_subparsers(
        dest="strategy_regression_command",
        required=True,
    )
    strategy_regression_rank = strategy_regression_subparsers.add_parser("rank")
    strategy_regression_selector = strategy_regression_rank.add_mutually_exclusive_group(required=True)
    strategy_regression_selector.add_argument("--env", default=None)
    strategy_regression_selector.add_argument("--build-id", default=None)
    strategy_regression_rank.add_argument("--output", required=True)

    historical_status = subparsers.add_parser("historical-status")
    historical_status.add_argument("--verbose", action="store_true")

    historical_pipeline = subparsers.add_parser("historical-pipeline")
    historical_pipeline_subparsers = historical_pipeline.add_subparsers(
        dest="historical_pipeline_command",
        required=True,
    )
    historical_pipeline_status = historical_pipeline_subparsers.add_parser("status")
    historical_pipeline_status.add_argument("--verbose", action="store_true")
    historical_pipeline_bootstrap = historical_pipeline_subparsers.add_parser("bootstrap")
    historical_pipeline_bootstrap.add_argument("--days", type=int, default=None)
    historical_pipeline_bootstrap.add_argument("--chunk-days", type=int, default=None)
    historical_pipeline_bootstrap.add_argument("--series", nargs="*", default=None)
    historical_pipeline_resume = historical_pipeline_subparsers.add_parser("resume")
    historical_pipeline_resume.add_argument("--series", nargs="*", default=None)
    historical_pipeline_daily = historical_pipeline_subparsers.add_parser("daily")
    historical_pipeline_daily.add_argument("--series", nargs="*", default=None)

    historical_intelligence = subparsers.add_parser("historical-intelligence")
    historical_intelligence_subparsers = historical_intelligence.add_subparsers(
        dest="historical_intelligence_command",
        required=True,
    )
    historical_intelligence_subparsers.add_parser("status")
    historical_intelligence_run = historical_intelligence_subparsers.add_parser("run")
    historical_intelligence_run.add_argument("--date-from", required=True)
    historical_intelligence_run.add_argument("--date-to", required=True)
    historical_intelligence_run.add_argument("--origins", nargs="*", default=None)
    historical_intelligence_run.add_argument(
        "--auto-promote",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    historical_intelligence_explain = historical_intelligence_subparsers.add_parser("explain")
    historical_intelligence_explain.add_argument("--series", nargs="*", default=None)

    heuristic_pack = subparsers.add_parser("heuristic-pack")
    heuristic_pack_subparsers = heuristic_pack.add_subparsers(dest="heuristic_pack_command", required=True)
    heuristic_pack_subparsers.add_parser("status")
    heuristic_pack_promote = heuristic_pack_subparsers.add_parser("promote")
    heuristic_pack_promote.add_argument("--candidate-version", default=None)
    heuristic_pack_promote.add_argument("--reason", default="manual_promote")
    heuristic_pack_rollback = heuristic_pack_subparsers.add_parser("rollback")
    heuristic_pack_rollback.add_argument("--reason", default="manual_rollback")

    historical_import = subparsers.add_parser("historical-import")
    historical_import.add_argument("historical_kind", choices=["weather"])
    historical_import.add_argument("--date-from", required=True)
    historical_import.add_argument("--date-to", required=True)
    historical_import.add_argument("--series", nargs="*", default=None)

    historical_backfill = subparsers.add_parser("historical-backfill")
    historical_backfill_subparsers = historical_backfill.add_subparsers(dest="historical_backfill_kind", required=True)
    historical_backfill_market = historical_backfill_subparsers.add_parser("market")
    historical_backfill_market.add_argument("--date-from", required=True)
    historical_backfill_market.add_argument("--date-to", required=True)
    historical_backfill_market.add_argument("--series", nargs="*", default=None)
    historical_backfill_weather = historical_backfill_subparsers.add_parser("weather-archive")
    historical_backfill_weather.add_argument("--date-from", required=True)
    historical_backfill_weather.add_argument("--date-to", required=True)
    historical_backfill_weather.add_argument("--series", nargs="*", default=None)
    historical_backfill_weather.add_argument("--import-only", action="store_true")
    historical_backfill_forecast = historical_backfill_subparsers.add_parser("forecast-archive")
    historical_backfill_forecast.add_argument("--date-from", required=True)
    historical_backfill_forecast.add_argument("--date-to", required=True)
    historical_backfill_forecast.add_argument("--series", nargs="*", default=None)
    historical_backfill_settlements = historical_backfill_subparsers.add_parser("settlements")
    historical_backfill_settlements.add_argument("--date-from", required=True)
    historical_backfill_settlements.add_argument("--date-to", required=True)
    historical_backfill_settlements.add_argument("--series", nargs="*", default=None)

    historical_archive = subparsers.add_parser("historical-archive")
    historical_archive_subparsers = historical_archive.add_subparsers(dest="historical_archive_command", required=True)
    historical_archive_capture = historical_archive_subparsers.add_parser("capture")
    historical_archive_capture.add_argument("--once", action="store_true")
    historical_archive_capture.add_argument("--series", nargs="*", default=None)
    historical_archive_checkpoint_capture = historical_archive_subparsers.add_parser("checkpoint-capture")
    historical_archive_checkpoint_capture.add_argument("--once", action="store_true")
    historical_archive_checkpoint_capture.add_argument("--series", nargs="*", default=None)
    historical_archive_checkpoint_status = historical_archive_subparsers.add_parser("checkpoint-status")
    historical_archive_checkpoint_status.add_argument("--date-from", required=True)
    historical_archive_checkpoint_status.add_argument("--date-to", required=True)
    historical_archive_checkpoint_status.add_argument("--series", nargs="*", default=None)
    historical_archive_checkpoint_status.add_argument("--verbose", action="store_true")

    historical_replay = subparsers.add_parser("historical-replay")
    historical_replay.add_argument("historical_kind", choices=["weather"])
    historical_replay.add_argument("--date-from", required=True)
    historical_replay.add_argument("--date-to", required=True)
    historical_replay.add_argument("--series", nargs="*", default=None)

    historical_repair = subparsers.add_parser("historical-repair")
    historical_repair_subparsers = historical_repair.add_subparsers(dest="historical_repair_command", required=True)
    historical_repair_audit = historical_repair_subparsers.add_parser("audit")
    historical_repair_audit.add_argument("--date-from", required=True)
    historical_repair_audit.add_argument("--date-to", required=True)
    historical_repair_audit.add_argument("--series", nargs="*", default=None)
    historical_repair_audit.add_argument("--verbose", action="store_true")
    historical_repair_refresh = historical_repair_subparsers.add_parser("refresh")
    historical_repair_refresh.add_argument("--date-from", required=True)
    historical_repair_refresh.add_argument("--date-to", required=True)
    historical_repair_refresh.add_argument("--series", nargs="*", default=None)

    self_improve = subparsers.add_parser("self-improve")
    self_improve_subparsers = self_improve.add_subparsers(dest="self_improve_command", required=True)

    self_improve_subparsers.add_parser("status")

    critique = self_improve_subparsers.add_parser("critique")
    critique.add_argument("--days", type=int, default=None)
    critique.add_argument("--limit", type=int, default=200)

    evaluate = self_improve_subparsers.add_parser("eval")
    evaluate.add_argument("--candidate-version", required=True)
    evaluate.add_argument("--days", type=int, default=None)
    evaluate.add_argument("--limit", type=int, default=200)

    promote_pack = self_improve_subparsers.add_parser("promote")
    promote_pack.add_argument("--evaluation-run-id", required=True)
    promote_pack.add_argument("--reason", default="manual_promote")

    rollback_pack = self_improve_subparsers.add_parser("rollback")
    rollback_pack.add_argument("--reason", default="manual_rollback")

    health_check = subparsers.add_parser("health-check")
    health_subparsers = health_check.add_subparsers(dest="health_command", required=True)
    health_app = health_subparsers.add_parser("app")
    health_app.add_argument("--color", required=True, choices=["blue", "green"])
    health_daemon = health_subparsers.add_parser("daemon")
    health_daemon.add_argument("--color", required=True, choices=["blue", "green"])

    watchdog = subparsers.add_parser("watchdog")
    watchdog_subparsers = watchdog.add_subparsers(dest="watchdog_command", required=True)
    watchdog_subparsers.add_parser("status")

    watchdog_run_once = watchdog_subparsers.add_parser("run-once")
    watchdog_run_once.add_argument("--app-blue-status", default="unknown")
    watchdog_run_once.add_argument("--app-green-status", default="unknown")
    watchdog_run_once.add_argument("--source", default="manual_watchdog")

    watchdog_record = watchdog_subparsers.add_parser("record-action")
    watchdog_record.add_argument("--action", required=True)
    watchdog_record.add_argument("--outcome", required=True, choices=["succeeded", "failed"])
    watchdog_record.add_argument("--reason", required=True)
    watchdog_record.add_argument("--target-color", default=None)
    watchdog_record.add_argument("--failed-color", default=None)
    watchdog_record.add_argument("--source", default="watchdog_timer")

    watchdog_boot = watchdog_subparsers.add_parser("mark-boot")
    watchdog_boot.add_argument("--status", default="success")
    watchdog_boot.add_argument("--reason", default="systemd_boot")

    shadow_run = subparsers.add_parser("shadow-run")
    shadow_run.add_argument("market_ticker")
    shadow_run.add_argument("--name", default=None)
    shadow_run.add_argument("--prompt", default=None)
    shadow_run.add_argument("--reason", default="cli_shadow_run")

    decision_trace = subparsers.add_parser("decision-trace", help="Show or replay deterministic decision traces")
    decision_trace_subparsers = decision_trace.add_subparsers(dest="decision_trace_command", required=True)
    decision_trace_show = decision_trace_subparsers.add_parser("show", help="Print a stored deterministic decision trace")
    decision_trace_show.add_argument("decision_id")
    decision_trace_replay = decision_trace_subparsers.add_parser(
        "replay",
        help="Recompute normalized intent hashes from a stored deterministic decision trace",
    )
    decision_trace_replay.add_argument("decision_id")

    parameter_pack = subparsers.add_parser(
        "parameter-pack",
        help="Inspect and validate deterministic parameter packs without promotion side effects",
    )
    parameter_pack_subparsers = parameter_pack.add_subparsers(dest="parameter_pack_command", required=True)
    parameter_pack_default = parameter_pack_subparsers.add_parser("default", help="Print the built-in deterministic parameter pack")
    parameter_pack_default.add_argument("--path", default=None, help=f"Parameter pack YAML path (default: {DEFAULT_PARAMETER_PACK_PATH})")
    parameter_pack_hard_caps = parameter_pack_subparsers.add_parser("hard-caps", help="Print and hash the sealed hard-cap config")
    parameter_pack_hard_caps.add_argument("--path", default=str(DEFAULT_HARD_CAPS_PATH))
    parameter_pack_status = parameter_pack_subparsers.add_parser("status", help="Show staged parameter-pack rollout state")
    parameter_pack_status.add_argument("--limit", type=int, default=10)
    parameter_pack_seed_default = parameter_pack_subparsers.add_parser("seed-default", help="Persist the built-in parameter pack to the database")
    parameter_pack_seed_default.add_argument("--path", default=None, help=f"Parameter pack YAML path (default: {DEFAULT_PARAMETER_PACK_PATH})")
    parameter_pack_list = parameter_pack_subparsers.add_parser("list", help="List stored deterministic parameter packs")
    parameter_pack_list.add_argument("--limit", type=int, default=20)
    parameter_pack_show = parameter_pack_subparsers.add_parser("show", help="Show a stored pack by version, or 'default'")
    parameter_pack_show.add_argument("version")
    parameter_pack_validate = parameter_pack_subparsers.add_parser(
        "validate",
        help="Sanitize a candidate parameter-pack JSON file and print its deterministic hash",
    )
    parameter_pack_validate.add_argument("path")
    parameter_pack_validate.add_argument("--strict", action="store_true", help="Fail if the candidate includes hard-cap parameters")
    parameter_pack_gate = parameter_pack_subparsers.add_parser(
        "gate",
        help="Evaluate candidate/current holdout JSON reports against promotion gates",
    )
    parameter_pack_gate.add_argument("--candidate-report", required=True)
    parameter_pack_gate.add_argument("--current-report", required=True)
    parameter_pack_gate.add_argument("--hard-caps", default=str(DEFAULT_HARD_CAPS_PATH))
    parameter_pack_drift = parameter_pack_subparsers.add_parser(
        "drift",
        help="Evaluate calibration drift window JSON without mutating runtime state",
    )
    parameter_pack_drift.add_argument("--window", required=True)
    parameter_pack_select = parameter_pack_subparsers.add_parser(
        "select",
        help="Select the first replay-gated parameter-pack candidate without mutating runtime state",
    )
    parameter_pack_select.add_argument("--candidates", required=True)
    parameter_pack_select.add_argument("--current-report", required=True)
    parameter_pack_select.add_argument("--hard-caps", default=str(DEFAULT_HARD_CAPS_PATH))
    parameter_pack_select.add_argument("--starvation-tolerance", type=int, default=10)
    parameter_pack_grid = parameter_pack_subparsers.add_parser(
        "grid",
        help="Generate deterministic bounded parameter-pack candidates for offline replay",
    )
    parameter_pack_grid.add_argument("--grid", required=True)
    parameter_pack_grid.add_argument("--limit", type=int, default=None)
    parameter_pack_learned_gate = parameter_pack_subparsers.add_parser(
        "learned-gate",
        help="Gate optional learned-head holdout metrics before allowing nonzero blend weight",
    )
    parameter_pack_learned_gate.add_argument("--closed-form-report", required=True)
    parameter_pack_learned_gate.add_argument("--learned-report", required=True)
    parameter_pack_learned_gate.add_argument("--requested-weight", type=float, default=0.0)
    parameter_pack_nws_parser_gate = parameter_pack_subparsers.add_parser(
        "nws-parser-gate",
        help="Gate optional NWS parser features on shadow availability and schema-validity evidence",
    )
    parameter_pack_nws_parser_gate.add_argument("--window", required=True)
    parameter_pack_nws_parser_gate.add_argument("--requested-feature-weight", type=float, default=0.0)
    parameter_pack_record_starvation = parameter_pack_subparsers.add_parser(
        "record-starvation",
        help="Record a parameter-pack promotion-starvation ops event without changing live risk",
    )
    parameter_pack_record_starvation.add_argument("--selection", required=True)
    parameter_pack_record_starvation.add_argument("--reason", default="manual_parameter_pack_promotion_starvation")
    parameter_pack_record_starvation.add_argument("--escalation-threshold", type=int, default=3)
    parameter_pack_stage = parameter_pack_subparsers.add_parser(
        "stage",
        help="Stage a gated parameter pack on the inactive color without changing live risk",
    )
    parameter_pack_stage.add_argument("--candidate-pack", required=True)
    parameter_pack_stage.add_argument("--candidate-report", required=True)
    parameter_pack_stage.add_argument("--current-report", required=True)
    parameter_pack_stage.add_argument("--hard-caps", default=str(DEFAULT_HARD_CAPS_PATH))
    parameter_pack_stage.add_argument("--target-color", choices=["blue", "green"], default=None)
    parameter_pack_stage.add_argument("--reason", default="manual_parameter_pack_stage")
    parameter_pack_rollback = parameter_pack_subparsers.add_parser(
        "rollback-staged",
        help="Mark the staged parameter-pack candidate rolled back without changing live risk",
    )
    parameter_pack_rollback.add_argument("--reason", default="manual_parameter_pack_rollback")
    parameter_pack_canary = parameter_pack_subparsers.add_parser(
        "canary",
        help="Evaluate staged parameter-pack shadow-canary evidence without activating live risk",
    )
    parameter_pack_canary.add_argument("--report", required=True)
    parameter_pack_canary.add_argument("--min-shadow-rooms", type=int, default=25)
    parameter_pack_canary.add_argument("--min-elapsed-seconds", type=int, default=7200)
    parameter_pack_canary.add_argument("--max-brier-ratio", type=float, default=1.20)
    parameter_pack_promote = parameter_pack_subparsers.add_parser(
        "promote-staged",
        help="Mark a canary-passed parameter pack champion without changing active color",
    )
    parameter_pack_promote.add_argument("--reason", default="manual_parameter_pack_promote")

    subparsers.add_parser("shadow-c-sweep", help="Strategy C: evaluate lock-confirmation signals across all configured markets")
    subparsers.add_parser("strategy-c-status", help="Strategy C: show aggregate sweep metrics and lock tracker state")
    subparsers.add_parser("monotonicity-scan", help="Addition 3: run one monotonicity arb scan tick across all open KXHIGH* markets")
    subparsers.add_parser("monotonicity-status", help="Addition 3: show aggregate monotonicity arb proposal metrics")

    record_promotion = subparsers.add_parser(
        "record-strategy-promotion",
        help="P2-3: append one row to the strategy_promotion_events audit log.",
    )
    record_promotion.add_argument("--strategy", required=True, help="Short code: A, C, ARB, ...")
    record_promotion.add_argument("--from-state", required=True, help="e.g. shadow")
    record_promotion.add_argument("--to-state", required=True, help="e.g. live")
    record_promotion.add_argument("--actor", required=True, help="Operator identity (git user, @handle, etc.)")
    record_promotion.add_argument("--evidence-ref", default=None, help="URL / PR # / dashboard snapshot")
    record_promotion.add_argument("--notes", default=None, help="Free-text rationale")

    list_promotions = subparsers.add_parser(
        "list-strategy-promotions",
        help="P2-3: list recent strategy_promotion_events rows.",
    )
    list_promotions.add_argument("--strategy", default=None)
    list_promotions.add_argument("--limit", type=int, default=25)

    ignore_secondary = subparsers.add_parser(
        "ignore-strategy-promotion-secondary-status",
        help="Mark secondary promotion sync or rollback status ignored by an operator.",
    )
    ignore_secondary.add_argument("--promotion-id", type=int, action="append", default=None)
    ignore_secondary.add_argument(
        "--all",
        action="store_true",
        help="Ignore all pending/failed matching rows in --kalshi-env",
    )
    ignore_secondary.add_argument(
        "--kalshi-env",
        default=None,
        help="Required for --all or multiple --promotion-id values",
    )
    ignore_secondary.add_argument(
        "--field",
        choices=["secondary_sync_status", "secondary_rollback_status"],
        action="append",
        required=True,
    )
    ignore_secondary.add_argument(
        "--resolved-by",
        required=True,
        help="Operator identity for the resolution audit",
    )
    ignore_secondary.add_argument("--note", required=True, help="Resolution note, minimum 20 characters")

    promotion_watchdog = subparsers.add_parser(
        "strategy-promotion-watchdog",
        help="Evaluate or resolve auto-evolve strategy promotion watchdog records.",
    )
    promotion_watchdog_subparsers = promotion_watchdog.add_subparsers(
        dest="strategy_promotion_watchdog_command",
        required=True,
    )
    promotion_watchdog_evaluate = promotion_watchdog_subparsers.add_parser(
        "evaluate",
        help="Evaluate a single strategy promotion watchdog row regardless of due date.",
    )
    promotion_watchdog_evaluate.add_argument("--promotion-id", type=int, required=True)
    promotion_watchdog_evaluate.add_argument("--source", default="manual_strategy_promotion_watchdog")

    promotion_watchdog_resolve = promotion_watchdog_subparsers.add_parser(
        "resolve",
        help="Resolve an insufficient_data strategy promotion with an operator note.",
    )
    promotion_watchdog_resolve.add_argument("--promotion-id", type=int, required=True)
    promotion_watchdog_resolve.add_argument("--action", choices=["approve", "rollback"], required=True)
    promotion_watchdog_resolve.add_argument("--resolved-by", required=True)
    promotion_watchdog_resolve.add_argument("--note", required=True, help="Resolution note, minimum 20 characters")

    promotion_secondary_sync = subparsers.add_parser(
        "strategy-promotion-secondary-sync",
        help="Retry secondary assignment or rollback sync for strategy promotion rows.",
    )
    promotion_secondary_sync_subparsers = promotion_secondary_sync.add_subparsers(
        dest="strategy_promotion_secondary_sync_command",
        required=True,
    )
    promotion_secondary_sync_sweep = promotion_secondary_sync_subparsers.add_parser(
        "sweep",
        help="Retry pending or failed secondary sync rows in the active environment.",
    )
    promotion_secondary_sync_sweep.add_argument("--source", default="manual_strategy_promotion_secondary_sync")
    promotion_secondary_sync_sweep.add_argument("--limit", type=int, default=50)

    shadow_sweep = subparsers.add_parser("shadow-sweep")
    shadow_sweep.add_argument("--markets", nargs="*", default=None)
    shadow_sweep.add_argument("--limit", type=int, default=None)
    shadow_sweep.add_argument("--reason", default="cli_shadow_sweep")

    shadow_campaign = subparsers.add_parser("shadow-campaign")
    shadow_campaign_subparsers = shadow_campaign.add_subparsers(dest="shadow_campaign_command", required=True)
    shadow_campaign_run = shadow_campaign_subparsers.add_parser("run")
    shadow_campaign_run.add_argument("--limit", type=int, default=3)
    shadow_campaign_run.add_argument("--reason", default="cli_shadow_campaign")
    shadow_campaign_run.add_argument("--domain", choices=["weather"], default="weather")

    run_room = subparsers.add_parser("run-room")
    run_room.add_argument("room_id")
    run_room.add_argument("--reason", default="cli_run")

    subparsers.add_parser("reconcile")

    promote = subparsers.add_parser("promote")
    promote.add_argument("color", choices=["blue", "green"])

    kill_switch = subparsers.add_parser("kill-switch")
    kill_switch.add_argument("state", choices=["on", "off"])

    repair_stop_loss = subparsers.add_parser("repair-stop-loss-checkpoints")
    repair_stop_loss.add_argument("market_tickers", nargs="*")

    subparsers.add_parser("status")

    intel = subparsers.add_parser("intel", help="Show current trading intel for configured markets")
    intel.add_argument("--market", dest="market", default=None, metavar="TICKER", help="Show intel for a single market ticker")

    create_web_user = subparsers.add_parser("create-web-user", help="Create or reset a web UI user account")
    create_web_user.add_argument("--email", required=True, help="User email address")
    create_web_user.add_argument("--password", required=True, help="Plaintext password (hashed before storage)")

    calibrate_momentum = subparsers.add_parser("calibrate-momentum", help="Step-2 momentum calibration tooling")
    calibrate_momentum_sub = calibrate_momentum.add_subparsers(dest="calibrate_momentum_command", required=True)

    cm_backfill = calibrate_momentum_sub.add_parser("backfill-slopes", help="Fetch Kalshi candlesticks and write slopes to Signal.payload")
    cm_backfill.add_argument("--date-from", required=True, help="Start local_market_day (YYYY-MM-DD)")
    cm_backfill.add_argument("--date-to", required=True, help="End local_market_day (YYYY-MM-DD)")

    cm_preview = calibrate_momentum_sub.add_parser("preview", help="Full analysis (fit + buckets + CIs). Read-only, never writes DB.")
    cm_preview.add_argument("--date-from", required=True, help="Start local_market_day (YYYY-MM-DD)")
    cm_preview.add_argument("--date-to", required=True, help="End local_market_day (YYYY-MM-DD)")
    cm_preview.add_argument("--output", default=None, help="JSONL output path for per-room records")

    cm_stage = calibrate_momentum_sub.add_parser("stage", help="Full analysis + sanity bounds + write pending checkpoint")
    cm_stage.add_argument("--date-from", required=True, help="Start local_market_day (YYYY-MM-DD)")
    cm_stage.add_argument("--date-to", required=True, help="End local_market_day (YYYY-MM-DD)")
    cm_stage.add_argument("--min-observations", type=int, default=1000, help="Minimum usable observations required (default: 1000)")
    cm_stage.add_argument("--staged-by", default=None, help="Operator identity recorded in checkpoint (default: $USER)")
    cm_stage.add_argument("--force", action="store_true", help="Overwrite stale pending (>=24h) without prompting")
    cm_stage.add_argument("--output", default=None, help="JSONL output path for per-room records")

    cm_promote = calibrate_momentum_sub.add_parser("promote", help="Rename pending checkpoint to active")
    cm_promote.add_argument("--activated-by", default=None, help="Operator identity recorded in checkpoint (default: $USER)")

    calibrate_momentum_sub.add_parser("reject", help="Clear pending calibration (idempotent)")

    calibrate_momentum_sub.add_parser("status", help="Print current active + pending calibration state")

    return parser


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    try:
        sys.exit(asyncio.run(_run_cli(args)))
    except (ValueError, KeyError, RuntimeError) as exc:
        message = exc.args[0] if exc.args else str(exc)
        print(json.dumps({"error": message}, indent=2), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
