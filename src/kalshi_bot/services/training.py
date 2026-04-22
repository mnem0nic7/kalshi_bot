from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import json
from typing import Any, Iterable

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import RoleTrainingExample, RoomMessageRead, TrainingRoomBundle, TrainingRoomOutcome
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.counterfactuals import score_counterfactual_trade


class TrainingExportService:
    DEFAULT_ROLES = (
        AgentRole.RESEARCHER.value,
        AgentRole.PRESIDENT.value,
        AgentRole.TRADER.value,
        AgentRole.MEMORY_LIBRARIAN.value,
    )

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self.session_factory = session_factory

    async def build_room_bundle(self, room_id: str) -> TrainingRoomBundle:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            room = await repo.get_room(room_id)
            if room is None:
                raise KeyError(f"Room {room_id} not found")

            messages = [self._message_read(message) for message in await repo.list_messages(room_id)]
            signal = await repo.get_latest_signal_for_room(room_id)
            trade_ticket = await repo.get_latest_trade_ticket_for_room(room_id)
            risk_verdict = await repo.get_latest_risk_verdict_for_room(room_id)
            orders = await repo.list_orders_for_room(room_id)
            fills = await repo.list_fills_for_room(room_id)
            memory_note = await repo.get_latest_memory_note_for_room(room_id)
            dossier_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="research_dossier_snapshot")
            dossier_record = await repo.get_research_dossier(room.market_ticker)
            delta_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="research_delta")
            market_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="market_snapshot")
            weather_artifact = await repo.get_latest_artifact(room_id=room_id, artifact_type="weather_bundle")
            research_sources = await repo.list_artifacts(room_id=room_id, artifact_type="research_source", limit=100)
            campaign = await repo.get_room_campaign(room_id)
            research_health = await repo.get_room_research_health(room_id)
            strategy_audit = await repo.get_room_strategy_audit(room_id)
            historical_replay = await repo.get_historical_replay_run_by_room(room_id)
            settlement = await self._latest_settlement_for_market(repo, room.market_ticker)
            settlement_label = await repo.get_historical_settlement_label(room.market_ticker)
            await session.commit()

        outcome = self._derive_outcome(
            room=self._room_dict(room),
            messages=messages,
            signal=(self._signal_dict(signal) if signal is not None else None),
            dossier=(dossier_artifact.payload if dossier_artifact is not None else None),
            risk_verdict=(self._risk_verdict_dict(risk_verdict) if risk_verdict is not None else None),
            trade_ticket=(self._trade_ticket_dict(trade_ticket) if trade_ticket is not None else None),
            orders=[self._order_dict(order) for order in orders],
            fills=[self._fill_dict(fill) for fill in fills],
            settlement=(settlement or (self._settlement_label_dict(settlement_label) if settlement_label is not None else None)),
        )

        return TrainingRoomBundle(
            room_origin=room.room_origin,
            room=self._room_dict(room),
            campaign=(self._campaign_dict(campaign) if campaign is not None else None),
            research_health=(self._research_health_dict(research_health) if research_health is not None else None),
            strategy_audit=(dict(strategy_audit.payload or {}) if strategy_audit is not None else None),
            audit_source=(strategy_audit.audit_source if strategy_audit is not None else None),
            audit_version=(strategy_audit.audit_version if strategy_audit is not None else None),
            trainable_default=(strategy_audit.trainable_default if strategy_audit is not None else None),
            exclude_reason=(strategy_audit.exclude_reason if strategy_audit is not None else None),
            messages=messages,
            signal=self._signal_dict(signal) if signal is not None else None,
            research_dossier=(
                dossier_artifact.payload
                if dossier_artifact is not None
                else (dossier_record.payload if dossier_record is not None else None)
            ),
            research_delta=delta_artifact.payload if delta_artifact is not None else None,
            market_snapshot=market_artifact.payload if market_artifact is not None else None,
            weather_bundle=weather_artifact.payload if weather_artifact is not None else None,
            research_sources=[artifact.payload for artifact in research_sources],
            trade_ticket=self._trade_ticket_dict(trade_ticket) if trade_ticket is not None else None,
            risk_verdict=self._risk_verdict_dict(risk_verdict) if risk_verdict is not None else None,
            orders=[self._order_dict(order) for order in orders],
            fills=[self._fill_dict(fill) for fill in fills],
            memory_note=self._memory_note_dict(memory_note) if memory_note is not None else None,
            historical_provenance=(
                dict((historical_replay.payload or {}).get("historical_provenance") or {})
                if historical_replay is not None
                else None
            ),
            market_source_kind=(
                ((historical_replay.payload or {}).get("historical_provenance") or {}).get("market_source_kind")
                if historical_replay is not None
                else None
            ),
            weather_source_kind=(
                ((historical_replay.payload or {}).get("historical_provenance") or {}).get("weather_source_kind")
                if historical_replay is not None
                else None
            ),
            coverage_class=(
                ((historical_replay.payload or {}).get("historical_provenance") or {}).get("coverage_class")
                if historical_replay is not None
                else None
            ),
            replay_checkpoint_ts=(historical_replay.checkpoint_ts if historical_replay is not None else None),
            settlement=settlement,
            settlement_label=(self._settlement_label_dict(settlement_label) if settlement_label is not None else None),
            counterfactual_pnl_dollars=self._counterfactual_pnl(
                trade_ticket=(self._trade_ticket_dict(trade_ticket) if trade_ticket is not None else None),
                settlement=(settlement or (self._settlement_label_dict(settlement_label) if settlement_label is not None else None)),
            ),
            heuristic_pack_version=(
                (((signal.payload or {}) if signal is not None else {}).get("heuristic_pack_version"))
                if signal is not None
                else None
            ),
            intelligence_run_id=(
                (((signal.payload or {}) if signal is not None else {}).get("intelligence_run_id"))
                if signal is not None
                else None
            ),
            candidate_pack_id=(
                (((signal.payload or {}) if signal is not None else {}).get("candidate_pack_id"))
                if signal is not None
                else None
            ),
            rule_trace=list(
                ((((signal.payload or {}) if signal is not None else {}).get("rule_trace")) or [])
            ),
            support_window=(
                ((((signal.payload or {}) if signal is not None else {}).get("support_window")) or None)
                if signal is not None
                else None
            ),
            heuristic_summary=(
                ((((signal.payload or {}) if signal is not None else {}).get("heuristic_summary")) or None)
                if signal is not None
                else None
            ),
            outcome=outcome,
        )

    async def export_room_bundles(
        self,
        *,
        room_ids: list[str] | None = None,
        market_ticker: str | None = None,
        limit: int = 100,
        include_non_complete: bool = False,
        origins: list[str] | None = None,
        updated_since: datetime | None = None,
    ) -> list[TrainingRoomBundle]:
        target_room_ids = room_ids or await self._list_room_ids(
            market_ticker=market_ticker,
            limit=limit,
            include_non_complete=include_non_complete,
            origins=origins,
            updated_since=updated_since,
        )
        return [await self.build_room_bundle(room_id) for room_id in target_room_ids[:limit]]

    def build_role_training_examples(
        self,
        bundle: TrainingRoomBundle,
        *,
        roles: Iterable[str] | None = None,
    ) -> list[RoleTrainingExample]:
        requested_roles = {role.lower() for role in (roles or self.DEFAULT_ROLES)}
        messages = bundle.messages
        examples: list[RoleTrainingExample] = []

        for role, task in (
            (AgentRole.RESEARCHER.value, "write_research_observation"),
            (AgentRole.PRESIDENT.value, "write_policy_memo"),
            (AgentRole.TRADER.value, "propose_trade_ticket"),
            (AgentRole.MEMORY_LIBRARIAN.value, "distill_memory_note"),
        ):
            if role not in requested_roles:
                continue
            located = self._find_message(messages, role)
            if located is None:
                continue
            index, target_message = located
            prior_messages = [message.model_dump(mode="json") for message in messages[:index]]
            input_context = self._input_context_for_role(
                role=role,
                bundle=bundle,
                prior_messages=prior_messages,
            )
            examples.append(
                RoleTrainingExample(
                    room_id=bundle.room["id"],
                    market_ticker=bundle.room["market_ticker"],
                    role=role,
                    task=task,
                    messages=self._chat_messages_for_example(role=role, input_context=input_context, target_message=target_message),
                    input_context=input_context,
                    target=self._target_message_dict(target_message),
                    metadata={
                        "final_status": bundle.outcome.final_status,
                        "research_gate_passed": bundle.outcome.research_gate_passed,
                        "shadow_mode": bundle.outcome.shadow_mode,
                        "room_stage": bundle.outcome.room_stage,
                        "room_origin": bundle.room_origin or bundle.room.get("room_origin"),
                    },
                )
            )
        return examples

    async def export_role_training_examples(
        self,
        *,
        room_ids: list[str] | None = None,
        market_ticker: str | None = None,
        limit: int = 100,
        include_non_complete: bool = False,
        roles: Iterable[str] | None = None,
        origins: list[str] | None = None,
    ) -> list[RoleTrainingExample]:
        bundles = await self.export_room_bundles(
            room_ids=room_ids,
            market_ticker=market_ticker,
            limit=limit,
            include_non_complete=include_non_complete,
            origins=origins,
        )
        examples: list[RoleTrainingExample] = []
        for bundle in bundles:
            examples.extend(self.build_role_training_examples(bundle, roles=roles))
        return examples

    async def _list_room_ids(
        self,
        *,
        market_ticker: str | None,
        limit: int,
        include_non_complete: bool,
        origins: list[str] | None = None,
        updated_since: datetime | None = None,
    ) -> list[str]:
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            rooms = await repo.list_rooms_for_export(
                market_ticker=market_ticker,
                limit=limit,
                include_non_complete=include_non_complete,
                origins=origins,
                updated_since=updated_since,
            )
            await session.commit()
        return [room.id for room in rooms]

    async def _latest_settlement_for_market(self, repo: PlatformRepository, market_ticker: str) -> dict[str, Any] | None:
        events = await repo.list_exchange_events(stream_name="reconcile", event_type="settlements", limit=50)
        for event in events:
            settlements = event.payload.get("settlements")
            if not isinstance(settlements, list):
                continue
            for settlement in settlements:
                if not isinstance(settlement, dict):
                    continue
                ticker = settlement.get("market_ticker") or settlement.get("ticker")
                if ticker == market_ticker:
                    return settlement
        return None

    def _counterfactual_pnl(
        self,
        *,
        trade_ticket: dict[str, Any] | None,
        settlement: dict[str, Any] | None,
    ) -> Decimal | None:
        outcome = score_counterfactual_trade(trade_ticket=trade_ticket, settlement=settlement)
        return outcome.pnl_dollars if outcome is not None else None

    def _derive_outcome(
        self,
        *,
        room: dict[str, Any],
        messages: list[RoomMessageRead],
        signal: dict[str, Any] | None,
        dossier: dict[str, Any] | None,
        risk_verdict: dict[str, Any] | None,
        trade_ticket: dict[str, Any] | None,
        orders: list[dict[str, Any]],
        fills: list[dict[str, Any]],
        settlement: dict[str, Any] | None,
    ) -> TrainingRoomOutcome:
        final_status = room["stage"]
        for message in reversed(messages):
            if message.role == AgentRole.AUDITOR.value and isinstance(message.payload, dict) and message.payload.get("final_status"):
                final_status = str(message.payload["final_status"])
                break
            if message.role == AgentRole.EXECUTION_CLERK.value and isinstance(message.payload, dict) and message.payload.get("status"):
                final_status = str(message.payload["status"])
                break
        research_gate_passed = None
        if isinstance(dossier, dict):
            gate = dossier.get("gate")
            if isinstance(gate, dict):
                research_gate_passed = bool(gate.get("passed"))

        settlement_pnl = None
        if isinstance(settlement, dict):
            settlement_pnl = settlement.get("realized_pnl_dollars") or settlement.get("pnl_dollars")

        signal_payload = (signal or {}).get("payload") or {}
        eligibility = signal_payload.get("eligibility") if isinstance(signal_payload, dict) else None
        blocked_by = None
        if research_gate_passed is False:
            blocked_by = "research_gate"
        elif isinstance(eligibility, dict) and not bool(eligibility.get("eligible")):
            blocked_by = "eligibility"
        elif isinstance(risk_verdict, dict) and risk_verdict.get("status") == "blocked":
            blocked_by = "risk"

        return TrainingRoomOutcome(
            final_status=final_status,
            room_stage=room["stage"],
            shadow_mode=bool(room["shadow_mode"]),
            kill_switch_enabled=bool(room["kill_switch_enabled"]),
            research_gate_passed=research_gate_passed,
            risk_status=(risk_verdict.get("status") if isinstance(risk_verdict, dict) else None),
            resolution_state=signal_payload.get("resolution_state") if isinstance(signal_payload, dict) else None,
            eligibility_passed=(eligibility.get("eligible") if isinstance(eligibility, dict) else None),
            stand_down_reason=signal_payload.get("stand_down_reason") if isinstance(signal_payload, dict) else None,
            blocked_by=blocked_by,
            ticket_generated=trade_ticket is not None,
            orders_submitted=len(orders),
            fills_observed=len(fills),
            settlement_seen=settlement is not None,
            settlement_pnl_dollars=settlement_pnl,
        )

    def _input_context_for_role(
        self,
        *,
        role: str,
        bundle: TrainingRoomBundle,
        prior_messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        base = {
            "room": bundle.room,
            "prior_messages": prior_messages,
            "signal": bundle.signal,
            "outcome_hints": {
                "research_gate_passed": bundle.outcome.research_gate_passed,
                "shadow_mode": bundle.outcome.shadow_mode,
            },
        }
        if role == AgentRole.RESEARCHER.value:
            base.update(
                {
                    "research_dossier": bundle.research_dossier,
                    "research_delta": bundle.research_delta,
                    "market_snapshot": bundle.market_snapshot,
                    "weather_bundle": bundle.weather_bundle,
                    "research_sources": bundle.research_sources,
                }
            )
        elif role == AgentRole.PRESIDENT.value:
            base.update(
                {
                    "research_dossier": bundle.research_dossier,
                    "research_delta": bundle.research_delta,
                }
            )
        elif role == AgentRole.TRADER.value:
            base.update(
                {
                    "research_dossier": bundle.research_dossier,
                    "research_delta": bundle.research_delta,
                    "trade_ticket_context": (
                        bundle.research_dossier.get("trader_context")
                        if isinstance(bundle.research_dossier, dict)
                        else None
                    ),
                }
            )
        elif role == AgentRole.MEMORY_LIBRARIAN.value:
            base.update(
                {
                    "research_dossier": bundle.research_dossier,
                    "orders": bundle.orders,
                    "fills": bundle.fills,
                    "room_outcome": bundle.outcome.model_dump(mode="json"),
                }
            )
        return base

    def _chat_messages_for_example(
        self,
        *,
        role: str,
        input_context: dict[str, Any],
        target_message: RoomMessageRead,
    ) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": self._system_prompt_for_role(role)},
            {
                "role": "user",
                "content": (
                    f"Complete the {role} task using this structured context.\n\n"
                    f"{json.dumps(input_context, sort_keys=True)}"
                ),
            },
            {
                "role": "assistant",
                "content": json.dumps(self._target_message_dict(target_message), sort_keys=True),
            },
        ]

    @staticmethod
    def _find_message(messages: list[RoomMessageRead], role: str) -> tuple[int, RoomMessageRead] | None:
        for index, message in enumerate(messages):
            if message.role == role:
                return index, message
        return None

    @staticmethod
    def _system_prompt_for_role(role: str) -> str:
        prompts = {
            AgentRole.RESEARCHER.value: "You are the researcher agent in a Kalshi trading room. Be factual and concise.",
            AgentRole.PRESIDENT.value: "You are an advisory president agent setting posture for a trading room.",
            AgentRole.TRADER.value: "You are the trader agent. Speak clearly and reference the deterministic rationale.",
            AgentRole.MEMORY_LIBRARIAN.value: "You write concise trading memory notes for future retrieval.",
        }
        return prompts.get(role, f"You are the {role} agent.")

    @staticmethod
    def _message_read(record: Any) -> RoomMessageRead:
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

    @staticmethod
    def _target_message_dict(message: RoomMessageRead) -> dict[str, Any]:
        return {
            "kind": message.kind,
            "content": message.content,
            "payload": message.payload,
            "sequence": message.sequence,
            "stage": message.stage,
            "created_at": message.created_at.isoformat(),
        }

    @staticmethod
    def _room_dict(room: Any) -> dict[str, Any]:
        return {
            "id": room.id,
            "name": room.name,
            "market_ticker": room.market_ticker,
            "room_origin": room.room_origin,
            "prompt": room.prompt,
            "kalshi_env": room.kalshi_env,
            "stage": room.stage,
            "active_color": room.active_color,
            "shadow_mode": room.shadow_mode,
            "kill_switch_enabled": room.kill_switch_enabled,
            "agent_pack_version": room.agent_pack_version,
            "evaluation_run_id": room.evaluation_run_id,
            "role_models": room.role_models,
            "created_at": room.created_at.isoformat(),
            "updated_at": room.updated_at.isoformat(),
        }

    @staticmethod
    def _signal_dict(signal: Any) -> dict[str, Any]:
        return {
            "id": signal.id,
            "room_id": signal.room_id,
            "market_ticker": signal.market_ticker,
            "fair_yes_dollars": str(signal.fair_yes_dollars),
            "edge_bps": signal.edge_bps,
            "confidence": signal.confidence,
            "summary": signal.summary,
            "payload": signal.payload,
            "created_at": signal.created_at.isoformat(),
        }

    @staticmethod
    def _trade_ticket_dict(ticket: Any) -> dict[str, Any]:
        return {
            "id": ticket.id,
            "room_id": ticket.room_id,
            "message_id": ticket.message_id,
            "market_ticker": ticket.market_ticker,
            "action": ticket.action,
            "side": ticket.side,
            "yes_price_dollars": str(ticket.yes_price_dollars),
            "count_fp": str(ticket.count_fp),
            "time_in_force": ticket.time_in_force,
            "client_order_id": ticket.client_order_id,
            "status": ticket.status,
            "payload": ticket.payload,
            "created_at": ticket.created_at.isoformat(),
        }

    @staticmethod
    def _risk_verdict_dict(verdict: Any) -> dict[str, Any]:
        return {
            "id": verdict.id,
            "room_id": verdict.room_id,
            "ticket_id": verdict.ticket_id,
            "status": verdict.status,
            "reasons": verdict.reasons,
            "approved_notional_dollars": (
                str(verdict.approved_notional_dollars) if verdict.approved_notional_dollars is not None else None
            ),
            "approved_count_fp": str(verdict.approved_count_fp) if verdict.approved_count_fp is not None else None,
            "payload": verdict.payload,
            "created_at": verdict.created_at.isoformat(),
        }

    @staticmethod
    def _order_dict(order: Any) -> dict[str, Any]:
        return {
            "id": order.id,
            "trade_ticket_id": order.trade_ticket_id,
            "kalshi_order_id": order.kalshi_order_id,
            "client_order_id": order.client_order_id,
            "market_ticker": order.market_ticker,
            "status": order.status,
            "side": order.side,
            "action": order.action,
            "yes_price_dollars": str(order.yes_price_dollars),
            "count_fp": str(order.count_fp),
            "raw": order.raw,
            "created_at": order.created_at.isoformat(),
        }

    @staticmethod
    def _fill_dict(fill: Any) -> dict[str, Any]:
        return {
            "id": fill.id,
            "order_id": fill.order_id,
            "trade_id": fill.trade_id,
            "market_ticker": fill.market_ticker,
            "side": fill.side,
            "action": fill.action,
            "yes_price_dollars": str(fill.yes_price_dollars),
            "count_fp": str(fill.count_fp),
            "is_taker": fill.is_taker,
            "raw": fill.raw,
            "created_at": fill.created_at.isoformat(),
        }

    @staticmethod
    def _memory_note_dict(note: Any) -> dict[str, Any]:
        return {
            "id": note.id,
            "room_id": note.room_id,
            "title": note.title,
            "summary": note.summary,
            "tags": note.tags,
            "linked_message_ids": note.linked_message_ids,
            "created_at": note.created_at.isoformat(),
        }

    @staticmethod
    def _settlement_label_dict(label: Any) -> dict[str, Any]:
        return {
            "id": label.id,
            "market_ticker": label.market_ticker,
            "series_ticker": label.series_ticker,
            "local_market_day": label.local_market_day,
            "source_kind": label.source_kind,
            "kalshi_result": label.kalshi_result,
            "settlement_value_dollars": (
                str(label.settlement_value_dollars) if label.settlement_value_dollars is not None else None
            ),
            "settlement_ts": label.settlement_ts.isoformat() if label.settlement_ts is not None else None,
            "crosscheck_status": label.crosscheck_status,
            "crosscheck_high_f": str(label.crosscheck_high_f) if label.crosscheck_high_f is not None else None,
            "crosscheck_result": label.crosscheck_result,
            "payload": label.payload,
            "created_at": label.created_at.isoformat(),
            "updated_at": label.updated_at.isoformat(),
        }

    @staticmethod
    def _campaign_dict(campaign: Any) -> dict[str, Any]:
        return {
            "id": campaign.id,
            "room_id": campaign.room_id,
            "campaign_id": campaign.campaign_id,
            "trigger_source": campaign.trigger_source,
            "city_bucket": campaign.city_bucket,
            "market_regime_bucket": campaign.market_regime_bucket,
            "difficulty_bucket": campaign.difficulty_bucket,
            "outcome_bucket": campaign.outcome_bucket,
            "dossier_artifact_id": campaign.dossier_artifact_id,
            "payload": campaign.payload,
            "created_at": campaign.created_at.isoformat(),
            "updated_at": campaign.updated_at.isoformat(),
        }

    @staticmethod
    def _research_health_dict(record: Any) -> dict[str, Any]:
        return {
            "room_id": record.room_id,
            "market_ticker": record.market_ticker,
            "dossier_status": record.dossier_status,
            "gate_passed": record.gate_passed,
            "valid_dossier": record.valid_dossier,
            "good_for_training": record.good_for_training,
            "quality_score": record.quality_score,
            "citation_coverage_score": record.citation_coverage_score,
            "settlement_clarity_score": record.settlement_clarity_score,
            "freshness_score": record.freshness_score,
            "contradiction_count": record.contradiction_count,
            "structured_completeness_score": record.structured_completeness_score,
            "fair_value_score": record.fair_value_score,
            "dossier_artifact_id": record.dossier_artifact_id,
            "payload": record.payload,
            "updated_at": record.updated_at.isoformat(),
        }
