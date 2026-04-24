from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import date
import json
from pathlib import Path
import sys

from kalshi_bot.config import get_settings
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
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import init_models
from kalshi_bot.logging import configure_logging
from kalshi_bot.services.container import AppContainer
from kalshi_bot.services.position_governance import refresh_stop_loss_checkpoints
from kalshi_bot.services.trade_analysis import format_trade_analysis_report
from kalshi_bot.services.trading_audit import format_trading_audit_text


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


async def _run_cli(args: argparse.Namespace) -> int:
    container = await AppContainer.build(bootstrap_db=args.command not in {"init-db", "trading-audit", "trade-analysis"})
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

        if args.command == "trading-audit":
            if args.trading_audit_command == "repair":
                result = await container.trading_audit_service.repair_attribution(
                    kalshi_env=args.kalshi_env,
                    days=args.days,
                    dry_run=args.dry_run,
                    limit=args.limit,
                )
                print(json.dumps(result, indent=2))
                return 0
            report = await container.trading_audit_service.build_report(
                kalshi_env=args.kalshi_env,
                days=args.days,
                focus=args.focus,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_trading_audit_text(report))
            return 0

        if args.command == "trade-analysis":
            if args.trade_analysis_command == "dataset":
                result = await container.trade_analysis_service.write_dataset(
                    kalshi_env=args.kalshi_env,
                    days=args.days,
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
                days=args.days,
                limit=args.limit,
            )
            if args.json:
                print(json.dumps(report, indent=2))
            else:
                print(format_trade_analysis_report(report))
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
                output=args.output,
            )
            print(json.dumps(await container.training_corpus_service.build_dataset(request), indent=2))
            return 0

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

        if args.command == "watchdog":
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

        if args.command == "shadow-run":
            result = await container.shadow_training_service.run_shadow_room(
                args.market_ticker,
                name=args.name,
                prompt=args.prompt,
                reason=args.reason,
            )
            print(json.dumps({"room_id": result.room_id, "market_ticker": result.market_ticker, "stage": result.stage}, indent=2))
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

        if args.command == "shadow-sweep":
            results = await container.shadow_training_service.run_shadow_sweep(
                markets=args.markets,
                limit=args.limit,
                reason=args.reason,
            )
            print(
                json.dumps(
                    [
                        {"room_id": item.room_id, "market_ticker": item.market_ticker, "room_name": item.room_name, "stage": item.stage}
                        for item in results
                    ],
                    indent=2,
                )
            )
            return 0

        if args.command == "shadow-campaign" and args.shadow_campaign_command == "run":
            request = ShadowCampaignRequest(limit=args.limit, reason=args.reason)
            results = await container.shadow_campaign_service.run(request)
            print(
                json.dumps(
                    [
                        {"room_id": item.room_id, "market_ticker": item.market_ticker, "room_name": item.room_name, "stage": item.stage}
                        for item in results
                    ],
                    indent=2,
                )
            )
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

            if args.command == "run-room":
                await session.commit()
                await container.supervisor.run_room(args.room_id, reason=args.reason)
                print(f"room {args.room_id} completed")
                return 0

            if args.command == "reconcile":
                summary = await container.reconciliation_service.reconcile(
                    repo,
                    subaccount=container.settings.kalshi_subaccount,
                    kalshi_env=container.settings.kalshi_env,
                )
                await session.commit()
                print(json.dumps(asdict(summary), indent=2))
                return 0

            if args.command == "promote":
                control = await repo.set_active_color(args.color)
                await session.commit()
                print(f"active_color={control.active_color}")
                return 0

            if args.command == "kill-switch":
                enabled = args.state == "on"
                control = await repo.set_kill_switch(enabled)
                await session.commit()
                print(f"kill_switch_enabled={control.kill_switch_enabled}")
                return 0

            if args.command == "status":
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

            if args.command == "intel":
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
                else:
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

            if args.command == "repair-stop-loss-checkpoints":
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

        if args.command == "create-web-user":
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

        raise ValueError(f"Unknown command: {args.command}")
    finally:
        await container.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kalshi-bot-cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")

    discover = subparsers.add_parser("discover")
    discover.add_argument("--json", action="store_true")

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
    trading_audit.add_argument("--focus", choices=["money-safety"], default="money-safety")
    trading_audit.add_argument("--json", action="store_true")
    trading_audit.add_argument("--limit", type=int, default=500)
    trading_audit.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)

    trade_analysis = subparsers.add_parser(
        "trade-analysis",
        help="Build read-only no-leakage trade analysis datasets and baseline model cards",
    )
    trade_analysis.add_argument("trade_analysis_command", choices=["dataset", "report", "model-eval"])
    trade_analysis.add_argument("--kalshi-env", default="production")
    trade_analysis.add_argument("--days", type=int, default=180)
    trade_analysis.add_argument("--limit", type=int, default=None)
    trade_analysis.add_argument("--json", action="store_true")
    trade_analysis.add_argument("--output", default="data/trade_analysis.jsonl")
    trade_analysis.add_argument("--dataset", default="data/trade_analysis.jsonl")

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

    shadow_sweep = subparsers.add_parser("shadow-sweep")
    shadow_sweep.add_argument("--markets", nargs="*", default=None)
    shadow_sweep.add_argument("--limit", type=int, default=None)
    shadow_sweep.add_argument("--reason", default="cli_shadow_sweep")

    shadow_campaign = subparsers.add_parser("shadow-campaign")
    shadow_campaign_subparsers = shadow_campaign.add_subparsers(dest="shadow_campaign_command", required=True)
    shadow_campaign_run = shadow_campaign_subparsers.add_parser("run")
    shadow_campaign_run.add_argument("--limit", type=int, default=3)
    shadow_campaign_run.add_argument("--reason", default="cli_shadow_campaign")

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
