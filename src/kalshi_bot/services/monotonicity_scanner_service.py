"""MonotonicityArbScannerService — orchestrates periodic scans and DB persistence."""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StrategyCode
from kalshi_bot.db.models import MonotonicityArbProposal
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.decision_trace import TRACE_SCHEMA_VERSION, normalize_for_trace, stable_hash
from kalshi_bot.services.fee_model import estimate_kalshi_taker_fee_dollars
from kalshi_bot.services.monotonicity_scanner import ArbProposal, scan_for_violations

logger = logging.getLogger(__name__)


class MonotonicityArbScannerService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._kalshi = kalshi

    async def sweep(self) -> list[ArbProposal]:
        """Run one monotonicity arb scan tick across all open KXHIGH* markets.

        Fetches open markets from Kalshi, detects violations, persists proposals,
        and returns the full proposal list (suppressed and actionable).
        """
        if not self._settings.monotonicity_arb_enabled:
            logger.debug("monotonicity_arb: disabled — skipping sweep")
            return []

        markets = await self._fetch_open_kxhigh_markets()
        control = await self._load_control()

        proposals = scan_for_violations(
            markets,
            control=control,
            settings=self._settings,
        )

        for proposal in proposals:
            if proposal.execution_outcome == "live_ready":
                proposal = await self._execute_live_pair(proposal)
                await self._save_pair_trace(proposal)
            await self._persist(proposal)

        logger.info(
            "monotonicity_arb: sweep complete — %d markets, %d proposals (%d shadow)",
            len(markets),
            len(proposals),
            sum(1 for p in proposals if p.execution_outcome == "shadow"),
        )
        return proposals

    async def get_status(self) -> dict[str, Any]:
        """Return aggregate monotonicity arb metrics."""
        async with self._session_factory() as session:
            total = (await session.execute(
                select(func.count()).select_from(MonotonicityArbProposal)
            )).scalar_one()
            shadow = (await session.execute(
                select(func.count()).select_from(MonotonicityArbProposal).where(
                    MonotonicityArbProposal.execution_outcome == "shadow"
                )
            )).scalar_one()
            recent = (await session.execute(
                select(MonotonicityArbProposal)
                .order_by(MonotonicityArbProposal.detected_at.desc())
                .limit(10)
            )).scalars().all()

        return {
            "enabled": self._settings.monotonicity_arb_enabled,
            "shadow_only": self._settings.monotonicity_arb_shadow_only,
            "total_proposals": total,
            "shadow_proposals": shadow,
            "recent": [
                {
                    "ticker_low": r.ticker_low,
                    "ticker_high": r.ticker_high,
                    "net_edge_cents": r.net_edge_cents,
                    "execution_outcome": r.execution_outcome,
                    "detected_at": r.detected_at.isoformat(),
                }
                for r in recent
            ],
        }

    async def _fetch_open_kxhigh_markets(self) -> list[dict[str, Any]]:
        """Fetch all open KXHIGH* markets from Kalshi."""
        try:
            response = await self._kalshi.list_markets(
                status="open",
                series_ticker="KXHIGH",
                limit=200,
            )
            markets = response.get("markets", [])
            return [m for m in markets if m.get("ticker", "").startswith("KXHIGH")]
        except Exception:
            logger.warning("monotonicity_arb: failed to fetch markets", exc_info=True)
            return []

    async def _load_control(self):
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            return await repo.get_deployment_control(kalshi_env=self._settings.kalshi_env)

    async def _persist(self, proposal: ArbProposal) -> None:
        record = MonotonicityArbProposal(
            station=proposal.station,
            event_date=proposal.event_date,
            ticker_low=proposal.ticker_low,
            ticker_high=proposal.ticker_high,
            threshold_low_f=proposal.threshold_low_f,
            threshold_high_f=proposal.threshold_high_f,
            ask_yes_low_cents=proposal.ask_yes_low_cents,
            ask_no_high_cents=proposal.ask_no_high_cents,
            total_cost_cents=proposal.total_cost_cents,
            gross_edge_cents=proposal.gross_edge_cents,
            fee_estimate_cents=proposal.fee_estimate_cents,
            net_edge_cents=proposal.net_edge_cents,
            contracts_proposed=proposal.contracts_proposed,
            execution_outcome=proposal.execution_outcome,
            suppression_reason=proposal.suppression_reason,
            pair_id=proposal.pair_id,
            leg1_client_order_id=proposal.leg1_client_order_id,
            leg2_client_order_id=proposal.leg2_client_order_id,
            unwind_client_order_id=proposal.unwind_client_order_id,
            leg1_order_id=proposal.leg1_order_id,
            leg2_order_id=proposal.leg2_order_id,
            unwind_order_id=proposal.unwind_order_id,
            execution_payload=proposal.execution_payload or {},
            detected_at=proposal.detected_at,
        )
        async with self._session_factory() as session:
            session.add(record)
            await session.commit()

        logger.info(
            "monotonicity_arb: %s/%s T%.0f<T%.0f net=%.2f¢ outcome=%s",
            proposal.station,
            proposal.event_date,
            proposal.threshold_low_f,
            proposal.threshold_high_f,
            proposal.net_edge_cents,
            proposal.execution_outcome,
        )

    async def _execute_live_pair(self, proposal: ArbProposal) -> ArbProposal:
        pair_id = f"arb-{uuid4().hex[:16]}"
        proposal.pair_id = pair_id
        proposal.execution_payload = {"pair_id": pair_id, "events": []}
        low = await self._fresh_market(proposal.ticker_low)
        high = await self._fresh_market(proposal.ticker_high)
        repriced = self._repriced_pair(proposal, low=low, high=high)
        if repriced is None:
            proposal.execution_outcome = "suppressed"
            proposal.suppression_reason = "live_reprice_failed"
            proposal.execution_payload["events"].append({"event": "reprice_failed"})
            return proposal
        if repriced["net_edge_cents"] < self._settings.monotonicity_arb_min_net_edge_cents:
            proposal.execution_outcome = "suppressed"
            proposal.suppression_reason = "live_reprice_edge_below_min"
            proposal.execution_payload["events"].append({"event": "edge_lost", **repriced})
            return proposal

        proposal.ask_yes_low_cents = repriced["ask_yes_low_cents"]
        proposal.ask_no_high_cents = repriced["ask_no_high_cents"]
        proposal.total_cost_cents = repriced["total_cost_cents"]
        proposal.gross_edge_cents = repriced["gross_edge_cents"]
        proposal.fee_estimate_cents = repriced["fee_estimate_cents"]
        proposal.net_edge_cents = repriced["net_edge_cents"]
        proposal.contracts_proposed = max(1, proposal.contracts_proposed)

        leg1_price = Decimal(str(proposal.ask_yes_low_cents / 100)).quantize(Decimal("0.0001"))
        leg1 = await self._submit_leg(
            pair_id=pair_id,
            label="leg1",
            ticker=proposal.ticker_low,
            side="yes",
            yes_price=leg1_price,
            count=proposal.contracts_proposed,
        )
        proposal.leg1_client_order_id = leg1["client_order_id"]
        proposal.leg1_order_id = leg1.get("order_id")
        proposal.execution_payload["events"].append({"event": "leg1", **leg1})
        if not leg1["filled"]:
            proposal.execution_outcome = "leg1_failed"
            proposal.suppression_reason = leg1["status"]
            return proposal

        no_ask = Decimal(str(proposal.ask_no_high_cents / 100)).quantize(Decimal("0.0001"))
        leg2_yes_price = (Decimal("1.0000") - no_ask).quantize(Decimal("0.0001"))
        leg2 = await self._submit_leg(
            pair_id=pair_id,
            label="leg2",
            ticker=proposal.ticker_high,
            side="no",
            yes_price=leg2_yes_price,
            count=proposal.contracts_proposed,
        )
        proposal.leg2_client_order_id = leg2["client_order_id"]
        proposal.leg2_order_id = leg2.get("order_id")
        proposal.execution_payload["events"].append({"event": "leg2", **leg2})
        if leg2["filled"]:
            proposal.execution_outcome = "live_filled"
            proposal.suppression_reason = None
            return proposal

        unwind = await self._unwind_leg1(pair_id=pair_id, proposal=proposal)
        proposal.unwind_client_order_id = unwind.get("client_order_id")
        proposal.unwind_order_id = unwind.get("order_id")
        proposal.execution_payload["events"].append({"event": "unwind", **unwind})
        if unwind.get("submitted"):
            proposal.execution_outcome = "leg2_failed_unwind_submitted"
            proposal.suppression_reason = leg2["status"]
        else:
            proposal.execution_outcome = "unwind_failed_kill_switch"
            proposal.suppression_reason = unwind.get("status") or "unwind_failed"
            await self._engage_kill_switch(pair_id, proposal, unwind)
        return proposal

    async def _fresh_market(self, ticker: str) -> dict[str, Any] | None:
        try:
            response = await self._kalshi.get_market(ticker)
        except Exception:
            logger.warning("monotonicity_arb: failed to re-fetch %s", ticker, exc_info=True)
            return None
        return response.get("market", response)

    def _repriced_pair(self, proposal: ArbProposal, *, low: dict[str, Any] | None, high: dict[str, Any] | None) -> dict[str, float] | None:
        if low is None or high is None:
            return None
        ask_yes_low = self._price_cents(low.get("yes_ask_dollars"))
        ask_no_high = self._price_cents(high.get("no_ask_dollars"))
        if ask_no_high is None:
            bid_yes_high = self._price_cents(high.get("yes_bid_dollars"))
            ask_no_high = 100.0 - bid_yes_high if bid_yes_high is not None else None
        if ask_yes_low is None or ask_no_high is None:
            return None
        total_cost = ask_yes_low + ask_no_high
        gross_edge = 100.0 - total_cost
        fee = self._fee_cents(ask_yes_low / 100.0) + self._fee_cents(ask_no_high / 100.0)
        net_edge = gross_edge - fee
        if gross_edge <= 0:
            return None
        return {
            "ask_yes_low_cents": ask_yes_low,
            "ask_no_high_cents": ask_no_high,
            "total_cost_cents": total_cost,
            "gross_edge_cents": gross_edge,
            "fee_estimate_cents": fee,
            "net_edge_cents": net_edge,
        }

    async def _submit_leg(
        self,
        *,
        pair_id: str,
        label: str,
        ticker: str,
        side: str,
        yes_price: Decimal,
        count: int,
    ) -> dict[str, Any]:
        client_order_id = f"{pair_id}-{label}"
        if getattr(self._kalshi, "write_credentials", object()) is None:
            return {"client_order_id": client_order_id, "status": "write_credentials_missing", "filled": False}
        payload = {
            "ticker": ticker,
            "side": side,
            "action": "buy",
            "client_order_id": client_order_id,
            "count_fp": f"{Decimal(count):.2f}",
            "yes_price_dollars": f"{yes_price:.4f}",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }
        if self._settings.kalshi_subaccount:
            payload["subaccount"] = self._settings.kalshi_subaccount
        try:
            response = await self._kalshi.create_order(payload)
        except Exception as exc:
            return {
                "client_order_id": client_order_id,
                "status": type(exc).__name__,
                "filled": False,
                "error": str(exc),
                "payload": payload,
            }
        order = response.get("order", {})
        order_id = order.get("order_id")
        status = str(order.get("status") or "submitted")
        filled = status in {"filled", "executed"}
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            await repo.save_order(
                ticket_id=None,
                client_order_id=client_order_id,
                market_ticker=ticker,
                status=status,
                side=side,
                action="buy",
                yes_price_dollars=yes_price,
                count_fp=Decimal(count),
                raw={"pair_id": pair_id, "leg": label, **response},
                kalshi_order_id=order_id,
                kalshi_env=self._settings.kalshi_env,
                strategy_code=StrategyCode.MONOTONICITY_ARB.value,
            )
            await session.commit()
        return {
            "client_order_id": client_order_id,
            "order_id": order_id,
            "status": status,
            "filled": filled,
            "payload": payload,
        }

    async def _unwind_leg1(self, *, pair_id: str, proposal: ArbProposal) -> dict[str, Any]:
        market = await self._fresh_market(proposal.ticker_low)
        bid = self._price_dollars(market.get("yes_bid_dollars") if market else None)
        client_order_id = f"{pair_id}-unwind"
        if bid is None:
            return {"client_order_id": client_order_id, "status": "unwind_quote_missing", "submitted": False}
        payload = {
            "ticker": proposal.ticker_low,
            "side": "yes",
            "action": "sell",
            "client_order_id": client_order_id,
            "count_fp": f"{Decimal(proposal.contracts_proposed):.2f}",
            "yes_price_dollars": f"{bid:.4f}",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }
        if self._settings.kalshi_subaccount:
            payload["subaccount"] = self._settings.kalshi_subaccount
        try:
            response = await self._kalshi.create_order(payload)
        except Exception as exc:
            return {
                "client_order_id": client_order_id,
                "status": type(exc).__name__,
                "submitted": False,
                "error": str(exc),
            }
        order = response.get("order", {})
        status = str(order.get("status") or "submitted")
        order_id = order.get("order_id")
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            await repo.save_order(
                ticket_id=None,
                client_order_id=client_order_id,
                market_ticker=proposal.ticker_low,
                status=status,
                side="yes",
                action="sell",
                yes_price_dollars=bid,
                count_fp=Decimal(proposal.contracts_proposed),
                raw={"pair_id": pair_id, "leg": "unwind", **response},
                kalshi_order_id=order_id,
                kalshi_env=self._settings.kalshi_env,
                strategy_code=StrategyCode.MONOTONICITY_ARB.value,
            )
            await session.commit()
        return {"client_order_id": client_order_id, "order_id": order_id, "status": status, "submitted": True}

    async def _engage_kill_switch(self, pair_id: str, proposal: ArbProposal, unwind: dict[str, Any]) -> None:
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            await repo.set_kill_switch(True, kalshi_env=self._settings.kalshi_env)
            await repo.log_ops_event(
                severity="critical",
                summary=f"Monotonicity arb unwind failed for {proposal.ticker_low}/{proposal.ticker_high}; kill switch engaged",
                source="monotonicity_arb",
                kalshi_env=self._settings.kalshi_env,
                payload={
                    "pair_id": pair_id,
                    "ticker_low": proposal.ticker_low,
                    "ticker_high": proposal.ticker_high,
                    "unwind": unwind,
                },
            )
            await session.commit()

    async def _save_pair_trace(self, proposal: ArbProposal) -> None:
        pair_id = proposal.pair_id or f"{proposal.ticker_low}:{proposal.ticker_high}"
        inputs = normalize_for_trace(
            {
                "pair_id": pair_id,
                "ticker_low": proposal.ticker_low,
                "ticker_high": proposal.ticker_high,
                "threshold_low_f": proposal.threshold_low_f,
                "threshold_high_f": proposal.threshold_high_f,
                "ask_yes_low_cents": proposal.ask_yes_low_cents,
                "ask_no_high_cents": proposal.ask_no_high_cents,
                "contracts_proposed": proposal.contracts_proposed,
                "settings": {
                    "monotonicity_arb_min_net_edge_cents": self._settings.monotonicity_arb_min_net_edge_cents,
                    "monotonicity_arb_max_notional_dollars": self._settings.monotonicity_arb_max_notional_dollars,
                    "monotonicity_arb_shadow_only": self._settings.monotonicity_arb_shadow_only,
                    "monotonicity_arb_atomic_execution_ready": self._settings.monotonicity_arb_atomic_execution_ready,
                },
            }
        )
        final_status = proposal.execution_outcome
        decision_kind = "entry" if final_status in {"live_filled", "leg2_failed_unwind_submitted", "unwind_failed_kill_switch"} else "stand_down"
        trace = normalize_for_trace(
            {
                "schema_version": TRACE_SCHEMA_VERSION,
                "path_version": "monotonicity-pair.v1",
                "decision_kind": decision_kind,
                "market_ticker": proposal.ticker_low,
                "kalshi_env": self._settings.kalshi_env,
                "source_snapshot_ids": {
                    "pair_id": pair_id,
                    "ticker_low": proposal.ticker_low,
                    "ticker_high": proposal.ticker_high,
                },
                "inputs": inputs,
                "pair": {
                    "pair_id": pair_id,
                    "ticker_low": proposal.ticker_low,
                    "ticker_high": proposal.ticker_high,
                    "leg1_client_order_id": proposal.leg1_client_order_id,
                    "leg2_client_order_id": proposal.leg2_client_order_id,
                    "unwind_client_order_id": proposal.unwind_client_order_id,
                    "leg1_order_id": proposal.leg1_order_id,
                    "leg2_order_id": proposal.leg2_order_id,
                    "unwind_order_id": proposal.unwind_order_id,
                    "execution_payload": proposal.execution_payload or {},
                },
                "final_status": final_status,
                "evaluation_outcome": proposal.execution_outcome,
                "suppression_reason": proposal.suppression_reason,
            }
        )
        input_hash = stable_hash(inputs)
        trace_hash = stable_hash(
            {
                "path_version": trace["path_version"],
                "decision_kind": decision_kind,
                "market_ticker": proposal.ticker_low,
                "final_status": final_status,
                "pair_id": pair_id,
                "leg1_client_order_id": proposal.leg1_client_order_id,
                "leg2_client_order_id": proposal.leg2_client_order_id,
                "unwind_client_order_id": proposal.unwind_client_order_id,
            }
        )
        trace["input_hash"] = input_hash
        trace["trace_hash"] = trace_hash
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            await repo.save_decision_trace(
                room_id=None,
                ticket_id=None,
                market_ticker=proposal.ticker_low,
                kalshi_env=self._settings.kalshi_env,
                decision_kind=decision_kind,
                path_version="monotonicity-pair.v1",
                source_snapshot_ids=trace["source_snapshot_ids"],
                input_hash=input_hash,
                trace_hash=trace_hash,
                trace=trace,
            )
            await session.commit()

    @staticmethod
    def _price_cents(raw: Any) -> float | None:
        dollars = MonotonicityArbScannerService._price_dollars(raw)
        return float(dollars * Decimal("100")) if dollars is not None else None

    @staticmethod
    def _price_dollars(raw: Any) -> Decimal | None:
        if raw in (None, ""):
            return None
        try:
            value = Decimal(str(raw)).quantize(Decimal("0.0001"))
        except Exception:
            return None
        return value if value > Decimal("0") else None

    @staticmethod
    def _fee_cents(price_dollars: float) -> float:
        return float(
            estimate_kalshi_taker_fee_dollars(
                price_dollars=Decimal(str(price_dollars)),
                count=Decimal("1"),
                fee_rate=Decimal("0.07"),
            )
            * Decimal("100")
        )
