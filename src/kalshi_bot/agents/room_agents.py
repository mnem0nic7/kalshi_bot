from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole, MessageKind, RoomStage
from kalshi_bot.core.fixed_point import make_client_order_id
from kalshi_bot.core.schemas import MemoryNotePayload, ResearchDelta, ResearchDossier, RiskVerdictPayload, RoomMessageCreate, TradeTicket
from kalshi_bot.db.models import Room
from kalshi_bot.services.signal import StrategySignal, estimate_notional_dollars


class AgentSuite:
    def __init__(self, settings: Settings, providers: ProviderRouter) -> None:
        self.settings = settings
        self.providers = providers

    async def researcher_message(
        self,
        *,
        signal: StrategySignal,
        dossier: ResearchDossier,
        delta: ResearchDelta,
        room: Room,
        recent_memories: list[str],
    ) -> RoomMessageCreate:
        fallback = (
            f"{dossier.summary.narrative} Shared dossier cites {len(dossier.sources)} sources and "
            f"research gate is {'passing' if dossier.gate.passed else 'blocked'}. "
            f"Room delta: {delta.summary} Relevant memories: {', '.join(recent_memories) or 'none'}."
        )
        content = await self.providers.maybe_rewrite(
            role=AgentRole.RESEARCHER,
            fallback_text=fallback,
            system_prompt="You are the researcher agent in a Kalshi trading room. Be factual and concise.",
            user_prompt=fallback,
        )
        payload = {
            "thesis": dossier.trader_context.thesis,
            "evidence_ids": dossier.gate.cited_source_keys,
            "fair_yes_dollars": str(dossier.trader_context.fair_yes_dollars)
            if dossier.trader_context.fair_yes_dollars is not None
            else None,
            "edge_bps": signal.edge_bps,
            "research_gate_passed": dossier.gate.passed,
            "research_gate_reasons": dossier.gate.reasons,
            "delta": delta.model_dump(mode="json"),
        }
        return RoomMessageCreate(
            role=AgentRole.RESEARCHER,
            kind=MessageKind.OBSERVATION,
            stage=RoomStage.RESEARCHING,
            content=content,
            payload=payload,
        )

    async def president_message(self, *, signal: StrategySignal) -> RoomMessageCreate:
        posture = "press_when_clear" if signal.edge_bps >= self.settings.risk_min_edge_bps else "stay_disciplined"
        fallback = (
            f"Session posture is {posture}. Focus only on weather thresholds with fresh evidence; "
            f"do not stretch beyond configured limits or trade when the edge is ambiguous."
        )
        content = await self.providers.maybe_rewrite(
            role=AgentRole.PRESIDENT,
            fallback_text=fallback,
            system_prompt="You are an advisory president agent setting posture for a trading room.",
            user_prompt=fallback,
        )
        return RoomMessageCreate(
            role=AgentRole.PRESIDENT,
            kind=MessageKind.POLICY_MEMO,
            stage=RoomStage.POSTURE,
            content=content,
            payload={"posture": posture, "capital_tone": "small_clips", "constraints": ["respect risk engine"]},
        )

    async def trader_message(
        self,
        *,
        signal: StrategySignal,
        room_id: str,
        market_ticker: str,
        rationale_ids: list[str],
    ) -> tuple[RoomMessageCreate, TradeTicket | None, str | None]:
        if signal.recommended_action is None or signal.recommended_side is None or signal.target_yes_price_dollars is None:
            content = "No executable taker order clears the configured edge threshold right now."
            return (
                RoomMessageCreate(
                    role=AgentRole.TRADER,
                    kind=MessageKind.TRADE_IDEA,
                    stage=RoomStage.PROPOSING,
                    content=content,
                    payload={"decision": "stand_down", "edge_bps": signal.edge_bps},
                ),
                None,
                None,
            )

        price = signal.target_yes_price_dollars
        max_notional = Decimal(str(self.settings.risk_max_order_notional_dollars)) * Decimal(str(signal.confidence))
        unit_price = price if signal.recommended_side.value == "yes" else Decimal("1.0000") - price
        raw_count = (max_notional / max(unit_price, Decimal("0.01"))).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        capped_count = min(raw_count, Decimal(str(self.settings.risk_max_order_count_fp)))
        count_fp = max(capped_count, Decimal("1.00"))
        ticket = TradeTicket(
            market_ticker=market_ticker,
            action=signal.recommended_action,
            side=signal.recommended_side,
            yes_price_dollars=price,
            count_fp=count_fp,
            rationale_message_ids=rationale_ids,
            note=signal.summary,
        )
        client_order_id = make_client_order_id(room_id, market_ticker, ticket.nonce)
        fallback = (
            f"Propose {ticket.action.value} {ticket.side.value} {ticket.count_fp} contracts at yes {ticket.yes_price_dollars}. "
            f"Expected edge is {signal.edge_bps}bps and estimated notional is "
            f"{estimate_notional_dollars(ticket.side, ticket.yes_price_dollars, ticket.count_fp):.4f}."
        )
        content = await self.providers.maybe_rewrite(
            role=AgentRole.TRADER,
            fallback_text=fallback,
            system_prompt="You are the trader agent. Speak clearly and reference the deterministic rationale.",
            user_prompt=fallback,
        )
        return (
            RoomMessageCreate(
                role=AgentRole.TRADER,
                kind=MessageKind.TRADE_TICKET,
                stage=RoomStage.PROPOSING,
                content=content,
                payload=ticket.model_dump(mode="json"),
            ),
            ticket,
            client_order_id,
        )

    async def risk_message(self, *, verdict: RiskVerdictPayload) -> RoomMessageCreate:
        fallback = f"Deterministic risk verdict: {verdict.status.value}. " + " ".join(verdict.reasons)
        content = await self.providers.maybe_rewrite(
            role=AgentRole.RISK_OFFICER,
            fallback_text=fallback,
            system_prompt="You are the risk officer explaining a deterministic verdict.",
            user_prompt=fallback,
        )
        return RoomMessageCreate(
            role=AgentRole.RISK_OFFICER,
            kind=MessageKind.RISK_VERDICT,
            stage=RoomStage.RISK,
            content=content,
            payload=verdict.model_dump(mode="json"),
        )

    async def execution_message(self, status: str, payload: dict) -> RoomMessageCreate:
        return RoomMessageCreate(
            role=AgentRole.EXECUTION_CLERK,
            kind=MessageKind.EXEC_RECEIPT,
            stage=RoomStage.EXECUTING,
            content=f"Execution clerk recorded status {status}.",
            payload=payload,
        )

    async def ops_message(self, *, summary: str, payload: dict) -> RoomMessageCreate:
        return RoomMessageCreate(
            role=AgentRole.OPS_MONITOR,
            kind=MessageKind.OPS_ALERT,
            stage=RoomStage.EXECUTING,
            content=summary,
            payload=payload,
        )

    async def auditor_message(self, *, final_status: str, rationale_ids: list[str]) -> RoomMessageCreate:
        return RoomMessageCreate(
            role=AgentRole.AUDITOR,
            kind=MessageKind.INCIDENT_ACTION if final_status == "blocked" else MessageKind.OBSERVATION,
            stage=RoomStage.AUDITING,
            content=f"Auditor linked the decision chain to rationale messages: {', '.join(rationale_ids) or 'none'}.",
            payload={"final_status": final_status, "rationale_ids": rationale_ids},
        )

    async def memory_message(self, payload: MemoryNotePayload) -> RoomMessageCreate:
        return RoomMessageCreate(
            role=AgentRole.MEMORY_LIBRARIAN,
            kind=MessageKind.MEMORY_NOTE,
            stage=RoomStage.MEMORY,
            content=payload.summary,
            payload=payload.model_dump(mode="json"),
        )
