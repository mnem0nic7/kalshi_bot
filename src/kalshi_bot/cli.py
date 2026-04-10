from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path

from kalshi_bot.config import get_settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import init_models
from kalshi_bot.logging import configure_logging
from kalshi_bot.services.container import AppContainer
from kalshi_bot.core.schemas import RoomCreate


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


async def _run_cli(args: argparse.Namespace) -> int:
    container = await AppContainer.build(bootstrap_db=args.command != "init-db")
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

        async with container.session_factory() as session:
            repo = PlatformRepository(session)

            if args.command == "create-room":
                control = await repo.get_deployment_control()
                room = await repo.create_room(
                    RoomCreate(name=args.name, market_ticker=args.market_ticker, prompt=args.prompt),
                    active_color=control.active_color,
                    shadow_mode=container.settings.app_shadow_mode,
                    kill_switch_enabled=control.kill_switch_enabled,
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
                summary = await container.reconciliation_service.reconcile(repo, subaccount=container.settings.kalshi_subaccount)
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
                positions = await repo.list_positions(limit=10)
                ops_events = await repo.list_ops_events(limit=10)
                await session.commit()
                payload = {
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

    run_room = subparsers.add_parser("run-room")
    run_room.add_argument("room_id")
    run_room.add_argument("--reason", default="cli_run")

    subparsers.add_parser("reconcile")

    promote = subparsers.add_parser("promote")
    promote.add_argument("color", choices=["blue", "green"])

    kill_switch = subparsers.add_parser("kill-switch")
    kill_switch.add_argument("state", choices=["on", "off"])

    subparsers.add_parser("status")
    return parser


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run_cli(args)))


if __name__ == "__main__":
    main()
