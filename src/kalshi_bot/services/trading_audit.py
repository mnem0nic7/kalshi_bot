from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import StandDownReason, StrategyCode
from kalshi_bot.db.models import (
    FillRecord,
    MarketPriceHistory,
    MarketState,
    OpsEvent,
    OrderRecord,
    PositionRecord,
    RiskVerdictRecord,
    Room,
    Signal,
    TradeTicketRecord,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _money(value: Decimal | None) -> str | None:
    return str(value.quantize(Decimal("0.0001"))) if value is not None else None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _strategy_key(value: str | None) -> str:
    return value or "<null>"


def _side_price(fill: FillRecord) -> Decimal:
    if fill.side == "yes":
        return Decimal(fill.yes_price_dollars)
    return Decimal("1") - Decimal(fill.yes_price_dollars)


def _inferred_strategy_for_orphaned_order(order: OrderRecord) -> str | None:
    if str(order.client_order_id or "").startswith("room:"):
        return StrategyCode.DIRECTIONAL.value
    return None


def _current_side_price(position: PositionRecord, market_state: MarketState | None) -> Decimal | None:
    if market_state is None:
        return None
    if position.side == "yes":
        return market_state.yes_bid_dollars
    if market_state.yes_ask_dollars is None:
        return None
    return Decimal("1") - Decimal(market_state.yes_ask_dollars)


_TERMINAL_BLOCKED_CANDIDATE_REASONS = {
    StandDownReason.RESOLVED_CONTRACT.value,
}


@dataclass(slots=True)
class _Lot:
    count: Decimal
    price: Decimal
    settlement_result: str | None


class TradingAuditService:
    """Read-only production trading behavior audit.

    This service intentionally performs no repository writes and no exchange calls.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory

    async def build_report(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 7,
        focus: str = "money-safety",
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        cutoff = now - timedelta(days=days)
        async with self.session_factory() as session:
            fills = await self._fills(session, kalshi_env=kalshi_env, cutoff=cutoff)
            orders = await self._orders(session, kalshi_env=kalshi_env, cutoff=cutoff)
            tickets = await self._tickets(session, kalshi_env=kalshi_env, cutoff=cutoff)
            risk_verdicts = await self._risk_verdicts(session, kalshi_env=kalshi_env, cutoff=cutoff)
            signals = await self._signals(session, kalshi_env=kalshi_env, cutoff=cutoff)
            positions = await self._positions(session, kalshi_env=kalshi_env)
            market_states = await self._market_states(session, kalshi_env=kalshi_env)
            price_history_count = await self._price_history_count(session, kalshi_env=kalshi_env, cutoff=cutoff)
            ops_events = await self._ops_events(session, kalshi_env=kalshi_env, cutoff=cutoff)
            counts = await self._record_counts(session, kalshi_env=kalshi_env, cutoff=cutoff)

        fill_summary = self._fill_summary(fills)
        pnl = self._gross_pnl(fills)
        attribution = self._attribution_gaps(fills=fills, orders=orders)
        funnel = self._execution_funnel(
            tickets=tickets,
            risk_verdicts=risk_verdicts,
            orders=orders,
            fills=fills,
            now=now,
        )
        signal_funnel = self._signal_funnel(signals=signals, tickets=tickets)
        stop_loss = self._stop_loss_clusters(ops_events, now=now)
        risk = self._risk_summary(risk_verdicts)
        ops = self._ops_summary(ops_events)
        exposure = self._position_exposure(
            positions=positions,
            market_states=market_states,
            now=now,
        )
        issues = self._issues(
            fill_summary=fill_summary,
            attribution=attribution,
            funnel=funnel,
            signal_funnel=signal_funnel,
            stop_loss=stop_loss,
            risk=risk,
            ops=ops,
            exposure=exposure,
        )

        return {
            "audit": {
                "kalshi_env": kalshi_env,
                "focus": focus,
                "window_days": days,
                "window_start": cutoff.isoformat(),
                "window_end": now.isoformat(),
                "read_only": True,
            },
            "counts": {**counts, "market_price_history": price_history_count},
            "fill_summary": fill_summary,
            "pnl": pnl,
            "attribution": attribution,
            "execution_funnel": funnel,
            "signal_funnel": signal_funnel,
            "stop_loss": stop_loss,
            "risk": risk,
            "ops": ops,
            "open_positions": exposure,
            "issues": issues,
        }

    async def repair_attribution(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 7,
        dry_run: bool = True,
        now: datetime | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        cutoff = now - timedelta(days=days)
        candidates: list[dict[str, Any]] = []
        updated = 0
        async with self.session_factory() as session:
            orders = await self._orders(session, kalshi_env=kalshi_env, cutoff=cutoff - timedelta(days=1))
            tickets = await self._tickets(session, kalshi_env=kalshi_env, cutoff=cutoff - timedelta(days=1))
            tickets_by_id = {ticket.id: ticket for ticket in tickets}
            tickets_by_client_order_id = {ticket.client_order_id: ticket for ticket in tickets}
            order_strategy_overrides: dict[str, str] = {}
            for order in orders:
                if order.strategy_code is not None:
                    continue
                ticket = tickets_by_id.get(str(order.trade_ticket_id)) if order.trade_ticket_id else None
                ticket = ticket or tickets_by_client_order_id.get(order.client_order_id)
                inferred_strategy = (
                    ticket.strategy_code
                    if ticket is not None and ticket.strategy_code is not None
                    else _inferred_strategy_for_orphaned_order(order)
                )
                if inferred_strategy is not None:
                    order_strategy_overrides[order.id] = inferred_strategy
                    if not dry_run:
                        order.strategy_code = inferred_strategy
            orders_by_kalshi_id = {order.kalshi_order_id: order for order in orders if order.kalshi_order_id}
            result = await session.execute(
                select(FillRecord)
                .where(FillRecord.kalshi_env == kalshi_env, FillRecord.created_at >= cutoff)
                .where((FillRecord.strategy_code.is_(None)) | (FillRecord.order_id.is_(None)))
                .order_by(FillRecord.created_at.asc())
            )
            fills = list(result.scalars())
            for fill in fills:
                new_order_id = fill.order_id
                new_strategy = fill.strategy_code
                reason: str | None = None
                evidence: dict[str, Any] = {}

                raw_order_id = (fill.raw or {}).get("order_id") if isinstance(fill.raw, dict) else None
                matched_order = orders_by_kalshi_id.get(raw_order_id)
                if matched_order is not None:
                    matched_order_strategy = matched_order.strategy_code or order_strategy_overrides.get(matched_order.id)
                    strategy_source = "order_strategy_code" if matched_order.strategy_code else None
                    if matched_order_strategy is None:
                        ticket = (
                            tickets_by_id.get(str(matched_order.trade_ticket_id))
                            if matched_order.trade_ticket_id
                            else None
                        )
                        ticket = ticket or tickets_by_client_order_id.get(matched_order.client_order_id)
                        matched_order_strategy = ticket.strategy_code if ticket is not None else None
                        strategy_source = "ticket_strategy_code" if matched_order_strategy else None
                    elif matched_order.id in order_strategy_overrides and matched_order.strategy_code is None:
                        strategy_source = "bot_room_client_order_id"
                    new_order_id = new_order_id or matched_order.id
                    new_strategy = new_strategy or matched_order_strategy
                    reason = "raw_order_id_match"
                    evidence = {
                        "raw_order_id": raw_order_id,
                        "local_order_id": matched_order.id,
                        "order_strategy_code": matched_order_strategy,
                        "strategy_source": strategy_source,
                    }

                if new_strategy is None and fill.action == "sell":
                    buy_result = await session.execute(
                        select(FillRecord)
                        .where(
                            FillRecord.kalshi_env == kalshi_env,
                            FillRecord.market_ticker == fill.market_ticker,
                            FillRecord.side == fill.side,
                            FillRecord.action == "buy",
                            FillRecord.strategy_code.is_not(None),
                            FillRecord.created_at <= fill.created_at,
                        )
                        .order_by(FillRecord.created_at.desc())
                        .limit(1)
                    )
                    buy_fill = buy_result.scalar_one_or_none()
                    if buy_fill is not None:
                        new_strategy = buy_fill.strategy_code
                        reason = reason or "same_ticker_side_buy_lot"
                        evidence = {
                            **evidence,
                            "buy_fill_id": buy_fill.id,
                            "buy_trade_id": buy_fill.trade_id,
                            "buy_strategy_code": buy_fill.strategy_code,
                        }

                order_changed = new_order_id is not None and fill.order_id != new_order_id
                strategy_changed = new_strategy is not None and fill.strategy_code != new_strategy
                if not (order_changed or strategy_changed):
                    continue

                candidate = {
                    "fill_id": fill.id,
                    "trade_id": fill.trade_id,
                    "market_ticker": fill.market_ticker,
                    "action": fill.action,
                    "side": fill.side,
                    "created_at": _iso(fill.created_at),
                    "reason": reason,
                    "old_order_id": fill.order_id,
                    "new_order_id": new_order_id,
                    "old_strategy_code": fill.strategy_code,
                    "new_strategy_code": new_strategy,
                    "evidence": evidence,
                }
                candidates.append(candidate)
                if not dry_run:
                    if order_changed:
                        fill.order_id = new_order_id
                    if strategy_changed:
                        fill.strategy_code = new_strategy
                    updated += 1
                    if updated >= limit:
                        break
                elif len(candidates) >= limit:
                    break

            if dry_run:
                await session.rollback()
            else:
                await session.commit()

        return {
            "kalshi_env": kalshi_env,
            "window_days": days,
            "dry_run": dry_run,
            "candidate_count": len(candidates),
            "updated_count": 0 if dry_run else updated,
            "candidates": candidates[:50],
        }

    async def _fills(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[FillRecord]:
        result = await session.execute(
            select(FillRecord)
            .where(FillRecord.kalshi_env == kalshi_env, FillRecord.created_at >= cutoff)
            .order_by(FillRecord.created_at.asc())
        )
        return list(result.scalars())

    async def _orders(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[OrderRecord]:
        result = await session.execute(
            select(OrderRecord)
            .where(OrderRecord.kalshi_env == kalshi_env, OrderRecord.created_at >= cutoff)
            .order_by(OrderRecord.created_at.asc())
        )
        return list(result.scalars())

    async def _tickets(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[TradeTicketRecord]:
        result = await session.execute(
            select(TradeTicketRecord)
            .join(Room, TradeTicketRecord.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env)
            .where(Room.room_origin != "historical_replay")
            .where(TradeTicketRecord.created_at >= cutoff)
            .order_by(TradeTicketRecord.created_at.asc())
        )
        return list(result.scalars())

    async def _risk_verdicts(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[RiskVerdictRecord]:
        result = await session.execute(
            select(RiskVerdictRecord)
            .join(Room, RiskVerdictRecord.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env)
            .where(RiskVerdictRecord.created_at >= cutoff)
            .order_by(RiskVerdictRecord.created_at.asc())
        )
        return list(result.scalars())

    async def _signals(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[Signal]:
        result = await session.execute(
            select(Signal)
            .join(Room, Signal.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env)
            .where(Room.room_origin != "historical_replay")
            .where(Signal.created_at >= cutoff)
            .order_by(Signal.created_at.desc())
        )
        return list(result.scalars())

    async def _positions(self, session: AsyncSession, *, kalshi_env: str) -> list[PositionRecord]:
        result = await session.execute(
            select(PositionRecord)
            .where(PositionRecord.kalshi_env == kalshi_env, PositionRecord.count_fp > 0)
            .order_by(PositionRecord.market_ticker.asc())
        )
        return list(result.scalars())

    async def _market_states(self, session: AsyncSession, *, kalshi_env: str) -> dict[str, MarketState]:
        result = await session.execute(select(MarketState).where(MarketState.kalshi_env == kalshi_env))
        return {row.market_ticker: row for row in result.scalars()}

    async def _price_history_count(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> int:
        result = await session.execute(
            select(MarketPriceHistory).where(
                MarketPriceHistory.kalshi_env == kalshi_env,
                MarketPriceHistory.created_at >= cutoff,
            )
        )
        return len(list(result.scalars()))

    async def _ops_events(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[OpsEvent]:
        result = await session.execute(
            select(OpsEvent)
            .where(OpsEvent.created_at >= cutoff)
            .where((OpsEvent.kalshi_env == kalshi_env) | (OpsEvent.kalshi_env.is_(None)))
            .order_by(OpsEvent.created_at.asc())
        )
        return list(result.scalars())

    async def _record_counts(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> dict[str, int]:
        counts: dict[str, int] = {}
        specs = [
            ("rooms", Room, (Room.kalshi_env == kalshi_env) & (Room.created_at >= cutoff)),
            ("orders", OrderRecord, (OrderRecord.kalshi_env == kalshi_env) & (OrderRecord.created_at >= cutoff)),
            ("fills", FillRecord, (FillRecord.kalshi_env == kalshi_env) & (FillRecord.created_at >= cutoff)),
            ("positions", PositionRecord, PositionRecord.kalshi_env == kalshi_env),
            ("ops_events", OpsEvent, OpsEvent.created_at >= cutoff),
        ]
        for key, model, predicate in specs:
            result = await session.execute(select(model).where(predicate))
            counts[key] = len(list(result.scalars()))
        signal_result = await session.execute(
            select(Signal)
            .join(Room, Signal.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env, Signal.created_at >= cutoff)
        )
        counts["signals"] = len(list(signal_result.scalars()))
        ticket_result = await session.execute(
            select(TradeTicketRecord)
            .join(Room, TradeTicketRecord.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env, TradeTicketRecord.created_at >= cutoff)
        )
        counts["trade_tickets"] = len(list(ticket_result.scalars()))
        risk_result = await session.execute(
            select(RiskVerdictRecord)
            .join(Room, RiskVerdictRecord.room_id == Room.id)
            .where(Room.kalshi_env == kalshi_env, RiskVerdictRecord.created_at >= cutoff)
        )
        counts["risk_verdicts"] = len(list(risk_result.scalars()))
        return counts

    def _fill_summary(self, fills: list[FillRecord]) -> dict[str, Any]:
        rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for fill in fills:
            key = (
                fill.action,
                fill.side,
                _strategy_key(fill.strategy_code),
                fill.settlement_result or "<null>",
            )
            row = rows.setdefault(
                key,
                {
                    "action": fill.action,
                    "side": fill.side,
                    "strategy_code": _strategy_key(fill.strategy_code),
                    "settlement_result": fill.settlement_result or "<null>",
                    "fills": 0,
                    "contracts": Decimal("0"),
                    "first_at": None,
                    "last_at": None,
                },
            )
            row["fills"] += 1
            row["contracts"] += Decimal(fill.count_fp)
            row["first_at"] = min(filter(None, [row["first_at"], _as_utc(fill.created_at)]), default=_as_utc(fill.created_at))
            row["last_at"] = max(filter(None, [row["last_at"], _as_utc(fill.created_at)]), default=_as_utc(fill.created_at))

        return {
            "total_fills": len(fills),
            "total_contracts": str(sum((Decimal(f.count_fp) for f in fills), Decimal("0.00"))),
            "rows": [
                {
                    **{k: v for k, v in row.items() if k not in {"contracts", "first_at", "last_at"}},
                    "contracts": str(row["contracts"]),
                    "first_at": _iso(row["first_at"]),
                    "last_at": _iso(row["last_at"]),
                }
                for row in sorted(rows.values(), key=lambda item: (item["action"], item["side"], item["strategy_code"], item["settlement_result"]))
            ],
        }

    def _gross_pnl(self, fills: list[FillRecord]) -> dict[str, Any]:
        lots_by_key: dict[tuple[str, str, str], list[_Lot]] = defaultdict(list)
        gross_pnl = Decimal("0")
        realized_trades = 0
        settled_trades = 0
        unsettled_open_contracts = Decimal("0")
        fee_total = Decimal("0")
        fee_seen = 0

        for fill in fills:
            fee = _decimal_or_none((fill.raw or {}).get("fee_cost"))
            if fee is not None:
                fee_total += fee
                fee_seen += 1

            key = (fill.market_ticker, fill.side, _strategy_key(fill.strategy_code))
            count = Decimal(fill.count_fp)
            price = _side_price(fill)
            if fill.action == "buy":
                lots_by_key[key].append(_Lot(count=count, price=price, settlement_result=fill.settlement_result))
                continue
            if fill.action != "sell":
                continue
            remaining = count
            for lot in lots_by_key[key]:
                if remaining <= 0:
                    break
                if lot.count <= 0:
                    continue
                matched = min(lot.count, remaining)
                gross_pnl += (price - lot.price) * matched
                lot.count -= matched
                remaining -= matched
                realized_trades += 1

        for lots in lots_by_key.values():
            for lot in lots:
                if lot.count <= 0:
                    continue
                if lot.settlement_result == "win":
                    gross_pnl += (Decimal("1") - lot.price) * lot.count
                    settled_trades += 1
                elif lot.settlement_result == "loss":
                    gross_pnl -= lot.price * lot.count
                    settled_trades += 1
                else:
                    unsettled_open_contracts += lot.count

        all_fees_present = bool(fills) and fee_seen == len(fills)
        return {
            "gross_pnl_dollars": _money(gross_pnl),
            "fee_total_dollars": _money(fee_total) if fee_seen else None,
            "net_pnl_dollars": _money(gross_pnl - fee_total) if all_fees_present else None,
            "fee_coverage": {"fills_with_fee": fee_seen, "total_fills": len(fills), "complete": all_fees_present},
            "realized_exit_matches": realized_trades,
            "settled_lots_scored": settled_trades,
            "unsettled_open_contracts": str(unsettled_open_contracts),
        }

    def _attribution_gaps(self, *, fills: list[FillRecord], orders: list[OrderRecord]) -> dict[str, Any]:
        orders_by_kalshi_id = {order.kalshi_order_id: order for order in orders if order.kalshi_order_id}
        missing_fills = [fill for fill in fills if fill.strategy_code is None]
        raw_order_matches = [
            fill
            for fill in missing_fills
            if isinstance(fill.raw, dict)
            and fill.raw.get("order_id") in orders_by_kalshi_id
            and orders_by_kalshi_id[fill.raw.get("order_id")].strategy_code
        ]
        top_tickers = Counter(fill.market_ticker for fill in missing_fills).most_common(10)
        return {
            "missing_fill_strategy_count": len(missing_fills),
            "missing_fill_strategy_contracts": str(sum((Decimal(f.count_fp) for f in missing_fills), Decimal("0.00"))),
            "missing_order_strategy_count": sum(1 for order in orders if order.strategy_code is None),
            "raw_order_id_could_recover_strategy_count": len(raw_order_matches),
            "top_missing_strategy_tickers": [
                {"market_ticker": ticker, "fills": count}
                for ticker, count in top_tickers
            ],
        }

    def _execution_funnel(
        self,
        *,
        tickets: list[TradeTicketRecord],
        risk_verdicts: list[RiskVerdictRecord],
        orders: list[OrderRecord],
        fills: list[FillRecord],
        now: datetime,
    ) -> dict[str, Any]:
        recent_cutoff = now - timedelta(hours=24)
        verdict_by_ticket = {verdict.ticket_id: verdict for verdict in risk_verdicts}
        orders_by_ticket: dict[str, list[OrderRecord]] = defaultdict(list)
        for order in orders:
            if order.trade_ticket_id:
                orders_by_ticket[order.trade_ticket_id].append(order)
        bad_statuses = {"order_id_missing"}
        failed_orders = [
            order
            for order in orders
            if order.status in bad_statuses or order.status.startswith("rejected_")
        ]
        recent_failed_orders = [o for o in failed_orders if _as_utc(o.created_at) and _as_utc(o.created_at) >= recent_cutoff]
        approved_tickets = [ticket for ticket in tickets if verdict_by_ticket.get(ticket.id) and verdict_by_ticket[ticket.id].status == "approved"]
        approved_without_order = [ticket for ticket in approved_tickets if not orders_by_ticket.get(ticket.id)]
        return {
            "tickets": len(tickets),
            "approved_tickets": len(approved_tickets),
            "blocked_tickets": sum(1 for verdict in risk_verdicts if verdict.status == "blocked"),
            "orders": len(orders),
            "fills": len(fills),
            "approved_without_order_count": len(approved_without_order),
            "failed_order_count": len(failed_orders),
            "recent_failed_order_count": len(recent_failed_orders),
            "failed_orders": [
                {
                    "client_order_id": order.client_order_id,
                    "market_ticker": order.market_ticker,
                    "status": order.status,
                    "strategy_code": order.strategy_code,
                    "created_at": _iso(order.created_at),
                }
                for order in failed_orders[:20]
            ],
        }

    def _signal_funnel(self, *, signals: list[Signal], tickets: list[TradeTicketRecord]) -> dict[str, Any]:
        ticketed_room_ids = {ticket.room_id for ticket in tickets}
        outcome_counts: Counter[str] = Counter()
        stand_down_counts: Counter[str] = Counter()
        side_counts: Counter[str] = Counter()
        top_markets: dict[str, dict[str, Any]] = {}
        selected_without_ticket: list[Signal] = []
        blocked_candidates: list[dict[str, Any]] = []

        for signal in signals:
            payload = dict(signal.payload or {})
            candidate_trace = dict(payload.get("candidate_trace") or {})
            eligibility = dict(payload.get("eligibility") or {})
            if not candidate_trace and isinstance(eligibility.get("candidate_trace"), dict):
                candidate_trace = dict(eligibility.get("candidate_trace") or {})
            outcome = str(
                payload.get("evaluation_outcome")
                or eligibility.get("evaluation_outcome")
                or candidate_trace.get("outcome")
                or "unknown"
            )
            side = str(payload.get("recommended_side") or candidate_trace.get("selected_side") or "none")
            stand_down_reason = str(
                payload.get("stand_down_reason")
                or eligibility.get("stand_down_reason")
                or "none"
            )
            outcome_counts[outcome] += 1
            side_counts[side] += 1
            if stand_down_reason != "none":
                stand_down_counts[stand_down_reason] += 1

            market = top_markets.setdefault(
                signal.market_ticker,
                {
                    "market_ticker": signal.market_ticker,
                    "signals": 0,
                    "candidate_selected": 0,
                    "max_edge_bps": int(signal.edge_bps),
                    "latest_at": _iso(signal.created_at),
                },
            )
            market["signals"] += 1
            market["max_edge_bps"] = max(int(market["max_edge_bps"]), int(signal.edge_bps))
            if outcome == "candidate_selected":
                market["candidate_selected"] += 1
                if signal.room_id not in ticketed_room_ids:
                    selected_without_ticket.append(signal)
            elif outcome == "pre_risk_filtered" and candidate_trace.get("outcome") == "candidate_selected":
                selected_candidate = self._selected_candidate_trace(candidate_trace)
                forecast_delta_f = _decimal_or_none(payload.get("forecast_delta_f"))
                abs_forecast_delta_f = float(abs(forecast_delta_f)) if forecast_delta_f is not None else None
                forecast_delta_gap_f = (
                    round(max(0.0, float(self.settings.strategy_min_abs_delta_f) - abs_forecast_delta_f), 2)
                    if abs_forecast_delta_f is not None
                    else None
                )
                blocked_candidates.append(
                    {
                        "room_id": signal.room_id,
                        "market_ticker": signal.market_ticker,
                        "stand_down_reason": stand_down_reason,
                        "selected_side": (
                            payload.get("recommended_side")
                            or candidate_trace.get("selected_side")
                            or selected_candidate.get("side")
                        ),
                        "selected_edge_bps": self._int_or_none(
                            candidate_trace.get("selected_edge_bps") or selected_candidate.get("edge_bps")
                        ),
                        "quality_adjusted_edge_bps": self._int_or_none(
                            selected_candidate.get("quality_adjusted_edge_bps")
                        ),
                        "spread_bps": self._int_or_none(
                            selected_candidate.get("spread_bps") or eligibility.get("market_spread_bps")
                        ),
                        "forecast_delta_f": payload.get("forecast_delta_f"),
                        "abs_forecast_delta_f": abs_forecast_delta_f,
                        "configured_min_abs_delta_f": float(self.settings.strategy_min_abs_delta_f),
                        "forecast_delta_gap_f": forecast_delta_gap_f,
                        "confidence": float(signal.confidence),
                        "created_at": _iso(signal.created_at),
                    }
                )

        top_market_rows = sorted(
            top_markets.values(),
            key=lambda row: (int(row["candidate_selected"]), int(row["signals"]), int(row["max_edge_bps"])),
            reverse=True,
        )
        blocked_candidates.sort(
            key=lambda row: (
                row["selected_edge_bps"] if row["selected_edge_bps"] is not None else -10_000,
                row["created_at"] or "",
            ),
            reverse=True,
        )
        non_terminal_blocked_candidates = [
            row
            for row in blocked_candidates
            if row["stand_down_reason"] not in _TERMINAL_BLOCKED_CANDIDATE_REASONS
        ]
        recent_selected_without_ticket = []
        for signal in selected_without_ticket[:20]:
            payload = dict(signal.payload or {})
            candidate_trace = dict(payload.get("candidate_trace") or {})
            recent_selected_without_ticket.append(
                {
                    "room_id": signal.room_id,
                    "market_ticker": signal.market_ticker,
                    "edge_bps": signal.edge_bps,
                    "confidence": signal.confidence,
                    "recommended_side": payload.get("recommended_side") or candidate_trace.get("selected_side"),
                    "created_at": _iso(signal.created_at),
                }
            )
        return {
            "signals": len(signals),
            "candidate_selected": int(outcome_counts.get("candidate_selected", 0)),
            "selected_without_ticket_count": len(selected_without_ticket),
            "outcome_counts": dict(outcome_counts),
            "recommended_side_counts": dict(side_counts),
            "top_stand_down_reasons": [
                {"reason": reason, "count": count}
                for reason, count in stand_down_counts.most_common(20)
            ],
            "top_markets": top_market_rows[:20],
            "blocked_candidate_count": len(blocked_candidates),
            "top_blocked_candidates": blocked_candidates[:20],
            "terminal_blocked_candidate_count": len(blocked_candidates) - len(non_terminal_blocked_candidates),
            "non_terminal_blocked_candidate_count": len(non_terminal_blocked_candidates),
            "top_non_terminal_blocked_candidates": non_terminal_blocked_candidates[:20],
            "non_terminal_blocked_reason_rollups": self._blocked_candidate_reason_rollups(non_terminal_blocked_candidates),
            "recent_selected_without_ticket": recent_selected_without_ticket,
        }

    @staticmethod
    def _selected_candidate_trace(candidate_trace: dict[str, Any]) -> dict[str, Any]:
        selected_side = candidate_trace.get("selected_side")
        if isinstance(selected_side, str):
            side_trace = candidate_trace.get(selected_side)
            if isinstance(side_trace, dict):
                return dict(side_trace)
        selected = candidate_trace.get("selected_candidate")
        if isinstance(selected, dict):
            return dict(selected)
        for candidate in candidate_trace.get("candidates") or []:
            if isinstance(candidate, dict) and candidate.get("status") == "selected":
                return dict(candidate)
        return {}

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _blocked_candidate_reason_rollups(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for candidate in candidates:
            grouped[str(candidate.get("stand_down_reason") or "unknown")].append(candidate)

        rows = []
        for reason, items in grouped.items():
            edge_values = [
                value
                for item in items
                if (value := self._int_or_none(item.get("selected_edge_bps"))) is not None
            ]
            quality_values = [
                value
                for item in items
                if (value := self._int_or_none(item.get("quality_adjusted_edge_bps"))) is not None
            ]
            spread_values = [
                value
                for item in items
                if (value := self._int_or_none(item.get("spread_bps"))) is not None
            ]
            forecast_values = [
                float(value)
                for item in items
                if (value := _decimal_or_none(item.get("abs_forecast_delta_f"))) is not None
            ]
            forecast_gap_values = [
                float(value)
                for item in items
                if (value := _decimal_or_none(item.get("forecast_delta_gap_f"))) is not None
            ]
            rows.append(
                {
                    "reason": reason,
                    "count": len(items),
                    "max_selected_edge_bps": max(edge_values) if edge_values else None,
                    "avg_selected_edge_bps": round(sum(edge_values) / len(edge_values), 2) if edge_values else None,
                    "avg_quality_adjusted_edge_bps": (
                        round(sum(quality_values) / len(quality_values), 2) if quality_values else None
                    ),
                    "avg_spread_bps": round(sum(spread_values) / len(spread_values), 2) if spread_values else None,
                    "avg_abs_forecast_delta_f": (
                        round(sum(forecast_values) / len(forecast_values), 2) if forecast_values else None
                    ),
                    "configured_min_abs_delta_f": float(self.settings.strategy_min_abs_delta_f),
                    "avg_forecast_delta_gap_f": (
                        round(sum(forecast_gap_values) / len(forecast_gap_values), 2) if forecast_gap_values else None
                    ),
                }
            )
        return sorted(rows, key=lambda row: (int(row["count"]), row["max_selected_edge_bps"] or -10_000), reverse=True)

    def _stop_loss_clusters(self, ops_events: list[OpsEvent], *, now: datetime) -> dict[str, Any]:
        stop_events = [event for event in ops_events if event.source == "stop_loss"]
        grouped: dict[tuple[str, str], list[OpsEvent]] = defaultdict(list)
        for event in stop_events:
            payload = event.payload or {}
            ticker = str(payload.get("market_ticker") or "unknown")
            trigger = str(payload.get("trigger") or "unknown")
            grouped[(ticker, trigger)].append(event)

        clusters = []
        cooldown = max(1, int(self.settings.stop_loss_submit_cooldown_seconds or 300))
        for (ticker, trigger), events in grouped.items():
            first_at = min((_as_utc(event.created_at) for event in events if event.created_at), default=None)
            last_at = max((_as_utc(event.created_at) for event in events if event.created_at), default=None)
            span_seconds = max(0, int(((last_at or now) - (first_at or now)).total_seconds()))
            expected_max = int(span_seconds / cooldown) + 2
            clusters.append({
                "market_ticker": ticker,
                "trigger": trigger,
                "events": len(events),
                "first_at": _iso(first_at),
                "last_at": _iso(last_at),
                "span_minutes": round(span_seconds / 60, 1),
                "cooldown_seconds": cooldown,
                "expected_max_events": expected_max,
                "exceeds_cooldown_expectation": len(events) > expected_max,
            })
        clusters.sort(key=lambda item: item["events"], reverse=True)
        return {"event_count": len(stop_events), "clusters": clusters[:20]}

    def _risk_summary(self, risk_verdicts: list[RiskVerdictRecord]) -> dict[str, Any]:
        status_counts = Counter(verdict.status for verdict in risk_verdicts)
        reason_counts: Counter[str] = Counter()
        for verdict in risk_verdicts:
            for reason in verdict.reasons or []:
                reason_counts[str(reason)] += 1
        return {
            "status_counts": dict(status_counts),
            "top_reasons": [
                {"reason": reason, "count": count}
                for reason, count in reason_counts.most_common(20)
            ],
        }

    def _ops_summary(self, ops_events: list[OpsEvent]) -> dict[str, Any]:
        counts = Counter((event.severity, event.source) for event in ops_events)
        stale_count = sum(
            1
            for event in ops_events
            if "stale" in (event.summary or "").lower()
            or "stale" in str(event.payload or {}).lower()
        )
        return {
            "event_count": len(ops_events),
            "stale_event_count": stale_count,
            "top_sources": [
                {"severity": severity, "source": source, "count": count}
                for (severity, source), count in counts.most_common(20)
            ],
        }

    def _position_exposure(
        self,
        *,
        positions: list[PositionRecord],
        market_states: dict[str, MarketState],
        now: datetime,
    ) -> dict[str, Any]:
        rows = []
        total_cost = Decimal("0")
        total_unrealized = Decimal("0")
        fresh_count = 0
        stale_or_missing_count = 0
        for position in positions:
            market_state = market_states.get(position.market_ticker)
            current = _current_side_price(position, market_state)
            cost = Decimal(position.average_price_dollars) * Decimal(position.count_fp)
            total_cost += cost
            observed_at = _as_utc(market_state.observed_at) if market_state is not None else None
            stale_seconds = None if observed_at is None else int((now - observed_at).total_seconds())
            is_stale = stale_seconds is None or stale_seconds > self.settings.risk_stale_market_seconds
            if is_stale:
                stale_or_missing_count += 1
            else:
                fresh_count += 1
            unrealized = None
            if current is not None:
                unrealized = (current - Decimal(position.average_price_dollars)) * Decimal(position.count_fp)
                total_unrealized += unrealized
            rows.append({
                "market_ticker": position.market_ticker,
                "side": position.side,
                "count_fp": str(position.count_fp),
                "average_price_dollars": str(position.average_price_dollars),
                "cost_basis_dollars": _money(cost),
                "mark_price_dollars": _money(current),
                "unrealized_pnl_dollars": _money(unrealized),
                "market_observed_at": _iso(observed_at),
                "stale_seconds": stale_seconds,
                "stale_or_missing_market_state": is_stale,
            })
        return {
            "position_count": len(positions),
            "fresh_mark_count": fresh_count,
            "stale_or_missing_mark_count": stale_or_missing_count,
            "total_cost_basis_dollars": _money(total_cost),
            "total_unrealized_pnl_dollars": _money(total_unrealized) if fresh_count else None,
            "positions": rows,
        }

    def _issues(
        self,
        *,
        fill_summary: dict[str, Any],
        attribution: dict[str, Any],
        funnel: dict[str, Any],
        signal_funnel: dict[str, Any],
        stop_loss: dict[str, Any],
        risk: dict[str, Any],
        ops: dict[str, Any],
        exposure: dict[str, Any],
    ) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        total_fills = int(fill_summary["total_fills"])
        missing_fills = int(attribution["missing_fill_strategy_count"])
        if missing_fills:
            issues.append({
                "severity": "critical" if total_fills and missing_fills / total_fills >= 0.25 else "high",
                "code": "missing_fill_strategy_attribution",
                "summary": "Fills without strategy attribution can distort per-strategy P&L and hard-loss caps.",
                "evidence": attribution,
            })
        if int(exposure["stale_or_missing_mark_count"]):
            issues.append({
                "severity": "critical",
                "code": "open_positions_stale_or_missing_market_state",
                "summary": "Open positions have stale or missing market marks.",
                "evidence": {
                    "stale_or_missing_mark_count": exposure["stale_or_missing_mark_count"],
                    "positions": [
                        row for row in exposure["positions"] if row["stale_or_missing_market_state"]
                    ][:10],
                },
            })
        repeated_clusters = [
            cluster for cluster in stop_loss["clusters"]
            if cluster["exceeds_cooldown_expectation"]
        ]
        if repeated_clusters:
            issues.append({
                "severity": "high",
                "code": "repeated_stop_loss_events",
                "summary": "Stop-loss events repeatedly targeted the same market, suggesting exits may not be resolving cleanly.",
                "evidence": {"clusters": repeated_clusters[:10]},
            })
        if funnel["approved_without_order_count"] or funnel["recent_failed_order_count"]:
            issues.append({
                "severity": "high",
                "code": "approved_trade_execution_gaps",
                "summary": "Approved tickets did not always produce a successful order path.",
                "evidence": {
                    "approved_without_order_count": funnel["approved_without_order_count"],
                    "failed_order_count": funnel["failed_order_count"],
                    "failed_orders": funnel["failed_orders"],
                },
            })
        if int(signal_funnel["selected_without_ticket_count"]):
            issues.append({
                "severity": "high",
                "code": "selected_signal_without_trade_ticket",
                "summary": "Some candidate-selected signals did not produce a trade ticket for risk/execution review.",
                "evidence": {
                    "selected_without_ticket_count": signal_funnel["selected_without_ticket_count"],
                    "recent": signal_funnel["recent_selected_without_ticket"],
                },
            })
        if attribution["raw_order_id_could_recover_strategy_count"]:
            issues.append({
                "severity": "high",
                "code": "unlinked_fills_with_recoverable_order_attribution",
                "summary": "Some fills are missing strategy attribution even though raw order IDs appear recoverable.",
                "evidence": {"count": attribution["raw_order_id_could_recover_strategy_count"]},
            })
        stale_risk_reasons = [
            row for row in risk["top_reasons"]
            if "stale" in row["reason"].lower()
        ]
        if stale_risk_reasons or ops["stale_event_count"]:
            issues.append({
                "severity": "medium",
                "code": "stale_data_blocks_or_events",
                "summary": "Stale market or research data is affecting trading decisions.",
                "evidence": {
                    "risk_reasons": stale_risk_reasons,
                    "ops_stale_event_count": ops["stale_event_count"],
                },
            })
        noisy_sources = [row for row in ops["top_sources"] if row["severity"] in {"warning", "error", "critical"} and row["count"] >= 10]
        if noisy_sources:
            issues.append({
                "severity": "medium",
                "code": "ops_warning_error_noise",
                "summary": "Recent warning/error event volume is high enough to obscure new incidents.",
                "evidence": {"top_sources": noisy_sources[:10]},
            })
        return issues


def format_trading_audit_text(report: dict[str, Any]) -> str:
    audit = report["audit"]
    pnl = report["pnl"]
    fills = report["fill_summary"]
    attribution = report["attribution"]
    funnel = report["execution_funnel"]
    exposure = report["open_positions"]
    stop_loss = report["stop_loss"]
    risk = report["risk"]
    issues = report["issues"]

    lines = [
        "Production Money + Safety Trading Audit",
        f"env={audit['kalshi_env']} focus={audit['focus']} window={audit['window_days']}d",
        "",
        f"Fills: {fills['total_fills']} ({fills['total_contracts']} contracts)",
        f"Gross P&L estimate: {pnl['gross_pnl_dollars']}  Net P&L: {pnl['net_pnl_dollars'] or 'fee coverage incomplete'}",
        f"Fees: {pnl['fee_total_dollars'] or 'n/a'} ({pnl['fee_coverage']['fills_with_fee']}/{pnl['fee_coverage']['total_fills']} fills)",
        f"Missing fill strategy: {attribution['missing_fill_strategy_count']} fills",
        f"Execution funnel: {funnel['approved_tickets']} approved tickets, {funnel['orders']} orders, {funnel['fills']} fills",
        f"Approved without order: {funnel['approved_without_order_count']}  Failed orders: {funnel['failed_order_count']}",
        f"Open positions: {exposure['position_count']}  stale/missing marks: {exposure['stale_or_missing_mark_count']}",
        f"Stop-loss events: {stop_loss['event_count']}",
        "",
        "Top risk reasons:",
    ]
    for row in risk["top_reasons"][:5]:
        lines.append(f"- {row['count']}: {row['reason']}")
    lines.append("")
    lines.append("Issues:")
    if not issues:
        lines.append("- none detected")
    else:
        for issue in issues:
            lines.append(f"- {issue['severity'].upper()} {issue['code']}: {issue['summary']}")
    return "\n".join(lines)
