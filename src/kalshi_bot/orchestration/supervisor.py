from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.agents.room_agents import AgentSuite
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, MessageKind, RiskStatus, RoomStage
from kalshi_bot.core.fixed_point import as_decimal
from kalshi_bot.core.metrics import ACTIVE_ROOMS, ORDERS_TOTAL, ROOM_RUNS_TOTAL
from kalshi_bot.core.schemas import ExecReceiptPayload, MemoryNotePayload, RoomMessageCreate, RoomMessageRead
from kalshi_bot.db.models import Room
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.integrations.weather import NWSWeatherClient
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.services.memory import MemoryService
from kalshi_bot.services.research import ResearchCoordinator
from kalshi_bot.services.risk import DeterministicRiskEngine, RiskContext
from kalshi_bot.services.signal import StrategySignal, WeatherSignalEngine, evaluate_trade_eligibility
from kalshi_bot.weather.mapping import WeatherMarketDirectory

logger = logging.getLogger(__name__)


def _room_message_read(record) -> RoomMessageRead:
    return RoomMessageRead(
        id=record.id,
        room_id=record.room_id,
        role=record.role,
        kind=record.kind,
        content=record.content,
        payload=record.payload,
        sequence=record.sequence,
        stage=record.stage,
        created_at=record.created_at,
    )


class WorkflowSupervisor:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker,
        kalshi: KalshiClient,
        weather: NWSWeatherClient,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        signal_engine: WeatherSignalEngine,
        risk_engine: DeterministicRiskEngine,
        execution_service: ExecutionService,
        memory_service: MemoryService,
        research_coordinator: ResearchCoordinator,
        agents: AgentSuite,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi
        self.weather = weather
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service
        self.signal_engine = signal_engine
        self.risk_engine = risk_engine
        self.execution_service = execution_service
        self.memory_service = memory_service
        self.research_coordinator = research_coordinator
        self.agents = agents

    async def run_room(self, room_id: str, reason: str = "manual") -> None:
        ACTIVE_ROOMS.inc()
        try:
            await self._run_room_inner(room_id=room_id, reason=reason)
        finally:
            ACTIVE_ROOMS.dec()

    async def _run_room_inner(self, *, room_id: str, reason: str) -> None:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            control = await repo.ensure_deployment_control(self.settings.app_color)
            room = await repo.get_room(room_id)
            if room is None:
                raise ValueError(f"Room {room_id} not found")

            await repo.append_message(
                room_id,
                RoomMessageCreate(
                    role=AgentRole.SUPERVISOR,
                    kind=MessageKind.OBSERVATION,
                    stage=RoomStage.TRIGGERED,
                    content=f"Supervisor started workflow for {room.market_ticker} because {reason}.",
                    payload={"reason": reason},
                ),
            )
            await session.commit()

            try:
                pack = await self.agent_pack_service.get_pack_for_color(repo, self.settings.app_color)
                thresholds = self.agent_pack_service.runtime_thresholds(pack)
                role_models: dict[str, Any] = {
                    role_name: {
                        "provider": config.provider,
                        "model": config.model,
                        "temperature": config.temperature,
                    }
                    for role_name, config in pack.roles.items()
                }
                await repo.update_room_runtime(
                    room.id,
                    agent_pack_version=pack.version,
                    role_models=role_models,
                )
                await session.commit()
                market_response = await self.kalshi.get_market(room.market_ticker)
                market = market_response.get("market", market_response)
                mapping = self.weather_directory.resolve_market(room.market_ticker, market)
                weather_bundle = (
                    await self.weather.build_market_snapshot(mapping)
                    if mapping is not None and mapping.supports_structured_weather
                    else None
                )
                dossier = await self.research_coordinator.ensure_fresh_dossier(room.market_ticker, reason="room_start")
                delta = self.research_coordinator.build_room_delta(
                    dossier=dossier,
                    market_response=market_response,
                    weather_bundle=weather_bundle,
                )

                await repo.log_exchange_event("rest_market", "market_snapshot", market_response, market_ticker=room.market_ticker)
                if mapping is not None and mapping.station_id is not None and weather_bundle is not None:
                    await repo.log_weather_event(mapping.station_id, "weather_bundle", weather_bundle)
                market_state = await repo.upsert_market_state(
                    room.market_ticker,
                    snapshot=market,
                    yes_bid_dollars=as_decimal(market["yes_bid_dollars"]) if market.get("yes_bid_dollars") is not None else None,
                    yes_ask_dollars=as_decimal(market["yes_ask_dollars"]) if market.get("yes_ask_dollars") is not None else None,
                    last_trade_dollars=as_decimal(market["last_price_dollars"]) if market.get("last_price_dollars") is not None else None,
                )
                signal = self.research_coordinator.build_signal_from_dossier(
                    dossier,
                    market_response,
                    min_edge_bps=thresholds.risk_min_edge_bps,
                )
                signal.eligibility = evaluate_trade_eligibility(
                    settings=self.settings,
                    signal=signal,
                    market_snapshot=market_response,
                    market_observed_at=market_state.observed_at,
                    research_freshness=dossier.freshness,
                    thresholds=thresholds,
                )
                signal.strategy_mode = signal.eligibility.strategy_mode
                signal.stand_down_reason = signal.eligibility.stand_down_reason
                if signal.eligibility.reasons and not signal.eligibility.eligible:
                    signal.summary = f"{signal.summary} Stand down: {' '.join(signal.eligibility.reasons)}"
                await repo.save_signal(
                    room_id=room.id,
                    market_ticker=room.market_ticker,
                    fair_yes_dollars=signal.fair_yes_dollars,
                    edge_bps=signal.edge_bps,
                    confidence=signal.confidence,
                    summary=signal.summary,
                    payload={
                        "research_mode": dossier.mode,
                        "research_gate_passed": dossier.gate.passed,
                        "research_last_run_id": dossier.last_run_id,
                        "research_delta": delta.model_dump(mode="json"),
                        "trader_context": dossier.trader_context.model_dump(mode="json"),
                        "research_freshness": dossier.freshness.model_dump(mode="json"),
                        "effective_research_freshness": dossier.freshness.model_dump(mode="json"),
                        "resolution_state": signal.resolution_state.value,
                        "strategy_mode": signal.strategy_mode.value,
                        "eligibility": signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None,
                        "stand_down_reason": signal.stand_down_reason.value if signal.stand_down_reason is not None else None,
                        "agent_pack_version": pack.version,
                    },
                )
                await session.commit()

                recent_memories = [note.summary for note in await repo.list_recent_memory_notes(limit=5)]
                await repo.update_room_stage(room.id, RoomStage.RESEARCHING)
                researcher_message, researcher_usage = await self.agents.researcher_message(
                    signal=signal,
                    dossier=dossier,
                    delta=delta,
                    room=room,
                    recent_memories=recent_memories,
                    role_config=self.agent_pack_service.role_config(pack, AgentRole.RESEARCHER),
                )
                researcher_record = await repo.append_message(room.id, researcher_message)
                role_models[AgentRole.RESEARCHER.value] = researcher_usage
                dossier_artifact = await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="research_dossier_snapshot",
                    source="research",
                    title=f"Research dossier snapshot for {room.market_ticker}",
                    payload=dossier.model_dump(mode="json"),
                )
                await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="research_delta",
                    source="research",
                    title=f"Research delta for {room.market_ticker}",
                    payload=delta.model_dump(mode="json"),
                )
                await repo.save_artifact(
                    room_id=room.id,
                    message_id=researcher_record.id,
                    artifact_type="market_snapshot",
                    source="kalshi",
                    title=f"Market snapshot for {room.market_ticker}",
                    payload=market_response,
                )
                if weather_bundle is not None:
                    await repo.save_artifact(
                        room_id=room.id,
                        message_id=researcher_record.id,
                        artifact_type="weather_bundle",
                        source="nws",
                        title=f"Weather bundle for {room.market_ticker}",
                        payload=weather_bundle,
                    )
                for source in dossier.sources:
                    await repo.save_artifact(
                        room_id=room.id,
                        message_id=researcher_record.id,
                        artifact_type="research_source",
                        source=source.source_class,
                        title=source.title,
                        payload=source.model_dump(mode="json"),
                        url=source.url,
                        external_id=source.source_key,
                    )
                research_health = self.research_coordinator.training_quality_snapshot(dossier)
                await repo.upsert_room_research_health(
                    room_id=room.id,
                    market_ticker=room.market_ticker,
                    dossier_status=research_health["dossier_status"],
                    gate_passed=research_health["gate_passed"],
                    valid_dossier=research_health["valid_dossier"],
                    good_for_training=research_health["good_for_training"],
                    quality_score=research_health["quality_score"],
                    citation_coverage_score=research_health["citation_coverage_score"],
                    settlement_clarity_score=research_health["settlement_clarity_score"],
                    freshness_score=research_health["freshness_score"],
                    contradiction_count=research_health["contradiction_count"],
                    structured_completeness_score=research_health["structured_completeness_score"],
                    fair_value_score=research_health["fair_value_score"],
                    dossier_artifact_id=dossier_artifact.id,
                    payload=research_health["payload"],
                )
                await repo.update_room_campaign(
                    room.id,
                    dossier_artifact_id=dossier_artifact.id,
                    payload_updates={
                        "research_mode": dossier.mode,
                        "research_gate_passed": dossier.gate.passed,
                        "quality_score": dossier.quality.overall_score,
                    },
                )
                await session.commit()

                receipt = ExecReceiptPayload(status="no_trade", details={})
                final_status = "no_trade"
                rationale_ids = [researcher_record.id]

                if not dossier.gate.passed:
                    ops_record = await repo.append_message(
                        room.id,
                        await self.agents.ops_message(
                            summary=f"Research gate blocked the room: {' '.join(dossier.gate.reasons)}",
                            payload=dossier.gate.model_dump(mode='json'),
                        ),
                    )
                    rationale_ids.append(ops_record.id)
                    final_status = "research_blocked"
                    await session.commit()
                else:
                    await repo.update_room_stage(room.id, RoomStage.POSTURE)
                    president_message, president_usage = await self.agents.president_message(
                        signal=signal,
                        role_config=self.agent_pack_service.role_config(pack, AgentRole.PRESIDENT),
                    )
                    president_record = await repo.append_message(room.id, president_message)
                    role_models[AgentRole.PRESIDENT.value] = president_usage
                    rationale_ids.append(president_record.id)
                    await session.commit()

                    await repo.update_room_stage(room.id, RoomStage.PROPOSING)
                    trader_message, ticket, client_order_id, trader_usage = await self.agents.trader_message(
                        signal=signal,
                        room_id=room.id,
                        market_ticker=room.market_ticker,
                        rationale_ids=rationale_ids.copy(),
                        role_config=self.agent_pack_service.role_config(pack, AgentRole.TRADER),
                        max_order_notional_dollars=thresholds.risk_max_order_notional_dollars,
                    )
                    trader_record = await repo.append_message(room.id, trader_message)
                    role_models[AgentRole.TRADER.value] = trader_usage
                    rationale_ids.append(trader_record.id)
                    await session.commit()

                    if ticket is not None and client_order_id is not None:
                        ticket_record = await repo.save_trade_ticket(room.id, ticket, client_order_id, message_id=trader_record.id)
                        risk_context = RiskContext(
                            market_observed_at=market_state.observed_at,
                            research_observed_at=dossier.freshness.refreshed_at,
                            current_position_notional_dollars=Decimal("0"),
                        )
                        verdict = self.risk_engine.evaluate(
                            room=room,
                            control=control,
                            ticket=ticket,
                            signal=signal,
                            context=risk_context,
                            thresholds=thresholds,
                        )
                        await repo.save_risk_verdict(
                            room_id=room.id,
                            ticket_id=ticket_record.id,
                            status=verdict.status,
                            reasons=verdict.reasons,
                            approved_notional_dollars=verdict.approved_notional_dollars,
                            approved_count_fp=verdict.approved_count_fp,
                            payload=verdict.model_dump(mode="json"),
                        )
                        risk_message, risk_usage = await self.agents.risk_message(
                            verdict=verdict,
                            role_config=self.agent_pack_service.role_config(pack, AgentRole.RISK_OFFICER),
                        )
                        risk_record = await repo.append_message(room.id, risk_message)
                        role_models[AgentRole.RISK_OFFICER.value] = risk_usage
                        rationale_ids.append(risk_record.id)
                        await session.commit()

                        if verdict.status == RiskStatus.APPROVED:
                            await repo.update_room_stage(room.id, RoomStage.EXECUTING)
                            lock_acquired = await repo.acquire_execution_lock(
                                holder=self.settings.app_color,
                                color=self.settings.app_color,
                            )
                            if lock_acquired:
                                receipt = await self.execution_service.execute(
                                    room=room,
                                    control=control,
                                    ticket=ticket,
                                    client_order_id=client_order_id,
                                )
                            else:
                                receipt = ExecReceiptPayload(
                                    status="lock_denied",
                                    client_order_id=client_order_id,
                                    details={"reason": "execution lock held by another deployment color"},
                                )
                            ORDERS_TOTAL.labels(status=receipt.status).inc()
                            if receipt.external_order_id or receipt.status not in ("shadow_skipped", "inactive_color_skipped"):
                                await repo.save_order(
                                    ticket_id=ticket_record.id,
                                    client_order_id=client_order_id,
                                    market_ticker=ticket.market_ticker,
                                    status=receipt.status,
                                    side=ticket.side.value,
                                    action=ticket.action.value,
                                    yes_price_dollars=ticket.yes_price_dollars,
                                    count_fp=ticket.count_fp,
                                    raw=receipt.details,
                                    kalshi_order_id=receipt.external_order_id,
                                )
                        else:
                            receipt = ExecReceiptPayload(
                                status="blocked",
                                client_order_id=client_order_id,
                                details={"reasons": verdict.reasons},
                            )
                            ORDERS_TOTAL.labels(status="blocked").inc()

                        execution_record = await repo.append_message(
                            room.id,
                            await self.agents.execution_message(receipt.status, receipt.model_dump(mode="json")),
                        )
                        rationale_ids.append(execution_record.id)
                        final_status = receipt.status
                        await session.commit()
                    else:
                        ops_record = await repo.append_message(
                            room.id,
                            await self.agents.ops_message(
                                summary=(
                                    "Ops monitor noted that the room stood down before risk or execution because "
                                    "the setup was not actionable."
                                ),
                                payload={
                                    "market_ticker": room.market_ticker,
                                    "status": "stand_down",
                                    "eligibility": (
                                        signal.eligibility.model_dump(mode="json") if signal.eligibility is not None else None
                                    ),
                                },
                            ),
                        )
                        rationale_ids.append(ops_record.id)
                        final_status = "stand_down"
                        await session.commit()

                await repo.update_room_stage(room.id, RoomStage.AUDITING)
                auditor_record = await repo.append_message(
                    room.id,
                    await self.agents.auditor_message(final_status=final_status, rationale_ids=rationale_ids),
                )
                rationale_ids.append(auditor_record.id)
                await session.commit()

                all_messages = [_room_message_read(message) for message in await repo.list_messages(room.id)]
                memory_payload, memory_usage = await self.memory_service.build_note(
                    room,
                    all_messages,
                    memory_config=pack.memory,
                    role_config=self.agent_pack_service.role_config(pack, AgentRole.MEMORY_LIBRARIAN),
                )
                await repo.update_room_stage(room.id, RoomStage.MEMORY)
                await repo.append_message(room.id, await self.agents.memory_message(memory_payload))
                role_models[AgentRole.MEMORY_LIBRARIAN.value] = memory_usage
                await repo.save_memory_note(
                    room_id=room.id,
                    payload=memory_payload,
                    embedding=self.agents.providers.embed_text(memory_payload.summary),
                    provider="hash-router-v1",
                )
                await repo.update_room_campaign(
                    room.id,
                    payload_updates={
                        "final_status": final_status,
                        "room_completed_at": datetime.now(UTC).isoformat(),
                    },
                )
                await repo.update_room_runtime(room.id, role_models=role_models)
                await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                ROOM_RUNS_TOTAL.labels(status="success").inc()
                await session.commit()
            except Exception as exc:
                logger.exception("room workflow failed", extra={"room_id": room_id})
                await session.rollback()
                repo = PlatformRepository(session)
                room = await repo.get_room(room_id)
                if room is not None:
                    await repo.update_room_stage(room.id, RoomStage.FAILED)
                    await repo.log_ops_event(
                        severity="error",
                        summary=f"Workflow failed for room {room.market_ticker}",
                        source="supervisor",
                        payload={"error": str(exc)},
                        room_id=room.id,
                    )
                    await repo.append_message(
                        room.id,
                        await self.agents.ops_message(
                            summary=f"Ops monitor saw a workflow failure: {exc}",
                            payload={"error": str(exc)},
                        ),
                    )
                    await session.commit()
                ROOM_RUNS_TOTAL.labels(status="failure").inc()
                raise
