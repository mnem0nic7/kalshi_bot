from __future__ import annotations

import hashlib
import json
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
    Artifact,
    Checkpoint,
    DecisionTraceRecord,
    FillRecord,
    HistoricalMarketSnapshotRecord,
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
from kalshi_bot.services.market_snapshot_archive import (
    DAEMON_MARKET_PRICE_SOURCE_KIND,
    DECISION_SIGNAL_MARKET_SOURCE_KIND,
    TRADE_ANALYSIS_CANDLESTICK_BACKFILL_SOURCE_KIND,
)
from kalshi_bot.services.trade_behavior import bucket_dimensions_from_key, bucket_key_for_fill


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


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


def _series_from_ticker(ticker: str) -> str | None:
    prefix = str(ticker or "").split("-")[0]
    return prefix or None


def _market_day_from_ticker(ticker: str) -> str:
    parts = str(ticker or "").split("-")
    return parts[1] if len(parts) >= 2 else "unknown"


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _side_price(fill: FillRecord) -> Decimal:
    if fill.side == "yes":
        return Decimal(fill.yes_price_dollars)
    return Decimal("1") - Decimal(fill.yes_price_dollars)


def _inferred_strategy_for_orphaned_order(order: OrderRecord) -> str | None:
    if str(order.client_order_id or "").startswith("room:"):
        return StrategyCode.DIRECTIONAL.value
    if order.trade_ticket_id is None and str(order.market_ticker or "").startswith("KXHIGH"):
        return StrategyCode.UNMANAGED.value
    return None


def _current_side_mark(position: PositionRecord, market_state: MarketState | None) -> dict[str, Any]:
    empty = {"price": None, "mark_source": None, "mark_conservative": False}
    if market_state is None:
        return empty
    if position.side == "yes":
        if market_state.yes_bid_dollars is not None:
            return {"price": market_state.yes_bid_dollars, "mark_source": "yes_bid", "mark_conservative": False}
        if market_state.yes_ask_dollars is not None:
            return {
                "price": Decimal("0.0000"),
                "mark_source": "one_sided_book_no_yes_bid",
                "mark_conservative": True,
            }
        return empty
    if position.side == "no":
        if market_state.yes_ask_dollars is not None:
            return {
                "price": Decimal("1") - Decimal(market_state.yes_ask_dollars),
                "mark_source": "no_bid_from_yes_ask",
                "mark_conservative": False,
            }
        if market_state.yes_bid_dollars is not None:
            return {
                "price": Decimal("0.0000"),
                "mark_source": "one_sided_book_no_no_bid",
                "mark_conservative": True,
            }
    return empty


def _current_side_price(position: PositionRecord, market_state: MarketState | None) -> Decimal | None:
    return _current_side_mark(position, market_state)["price"]


_TERMINAL_BLOCKED_CANDIDATE_REASONS = {
    StandDownReason.RESOLVED_CONTRACT.value,
}

_PRE_RISK_STAND_DOWN_REASONS = {
    "duplicate_suppressed",
    "missing_total_capital",
    "non_positive_suggested_count",
}

_POINT_IN_TIME_BACKFILL_SOURCE_KINDS = {
    "trade_analysis_backfill_room_artifact",
    "trade_analysis_backfill_signal_payload",
    "trade_analysis_backfill_market_price_history",
    "trade_analysis_backfill_historical_snapshot",
    TRADE_ANALYSIS_CANDLESTICK_BACKFILL_SOURCE_KIND,
}
_POINT_IN_TIME_MARKET_SOURCE_KINDS = {
    "checkpoint_captured_market_snapshot",
    "captured_market_snapshot",
    "reconstructed_market_checkpoint",
    DECISION_SIGNAL_MARKET_SOURCE_KIND,
    DAEMON_MARKET_PRICE_SOURCE_KIND,
    *_POINT_IN_TIME_BACKFILL_SOURCE_KINDS,
}

@dataclass(slots=True)
class _Lot:
    count: Decimal
    price: Decimal
    settlement_result: str | None


@dataclass(slots=True)
class _LedgerLot:
    count: Decimal
    price: Decimal
    settlement_result: str | None
    bucket_key: str


class TradingAuditService:
    """Read-only production trading behavior audit.

    This service intentionally performs no repository writes and no exchange calls.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: Any | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.kalshi = kalshi

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
            decision_traces = await self._decision_traces(session, kalshi_env=kalshi_env, cutoff=cutoff)
            positions = await self._positions(session, kalshi_env=kalshi_env)
            market_states = await self._market_states(session, kalshi_env=kalshi_env)
            price_history_count = await self._price_history_count(session, kalshi_env=kalshi_env, cutoff=cutoff)
            ops_events = await self._ops_events(session, kalshi_env=kalshi_env, cutoff=cutoff)
            counts = await self._record_counts(session, kalshi_env=kalshi_env, cutoff=cutoff)

        fill_summary = self._fill_summary(fills)
        pnl = self._gross_pnl(fills)
        lifecycle = self._lifecycle_ledger(fills, orders=orders)
        position_discrepancy = self._exchange_position_discrepancy(fills=fills, positions=positions)
        attribution = self._attribution_gaps(fills=fills, orders=orders)
        funnel = self._execution_funnel(
            tickets=tickets,
            risk_verdicts=risk_verdicts,
            orders=orders,
            fills=fills,
            now=now,
        )
        signal_funnel = self._signal_funnel(
            signals=signals,
            tickets=tickets,
            decision_traces=decision_traces,
            now=now,
        )
        stop_loss = self._stop_loss_clusters(ops_events, now=now)
        risk = self._risk_summary(risk_verdicts)
        ops = self._ops_summary(ops_events)
        trigger_diagnostics = self._trigger_diagnostics(ops_events)
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
            "lifecycle": lifecycle,
            "exchange_position_discrepancy": position_discrepancy,
            "attribution": attribution,
            "execution_funnel": funnel,
            "signal_funnel": signal_funnel,
            "stop_loss": stop_loss,
            "risk": risk,
            "ops": ops,
            "trigger_diagnostics": trigger_diagnostics,
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
                .order_by(FillRecord.created_at.asc())
            )
            fills = list(result.scalars())
            for fill in fills:
                new_order_id = fill.order_id
                new_strategy = fill.strategy_code
                new_side = fill.side
                reason: str | None = None
                evidence: dict[str, Any] = {}

                raw_order_id = (fill.raw or {}).get("order_id") if isinstance(fill.raw, dict) else None
                matched_order = orders_by_kalshi_id.get(raw_order_id)
                if matched_order is not None:
                    matched_order_strategy = matched_order.strategy_code or order_strategy_overrides.get(matched_order.id)
                    strategy_source = (
                        "order_strategy_code"
                        if matched_order.strategy_code and matched_order.id not in order_strategy_overrides
                        else None
                    )
                    if matched_order_strategy is None:
                        ticket = (
                            tickets_by_id.get(str(matched_order.trade_ticket_id))
                            if matched_order.trade_ticket_id
                            else None
                        )
                        ticket = ticket or tickets_by_client_order_id.get(matched_order.client_order_id)
                        matched_order_strategy = ticket.strategy_code if ticket is not None else None
                        strategy_source = "ticket_strategy_code" if matched_order_strategy else None
                    elif matched_order.id in order_strategy_overrides:
                        strategy_source = (
                            "bot_room_client_order_id"
                            if str(matched_order.client_order_id or "").startswith("room:")
                            else "unmanaged_exchange_order"
                        )
                    new_order_id = new_order_id or matched_order.id
                    new_strategy = new_strategy or matched_order_strategy
                    if matched_order.side in {"yes", "no"}:
                        new_side = matched_order.side
                    reason = "raw_order_id_match"
                    evidence = {
                        "raw_order_id": raw_order_id,
                        "local_order_id": matched_order.id,
                        "order_strategy_code": matched_order_strategy,
                        "order_side": matched_order.side,
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
                        new_side = buy_fill.side
                        reason = reason or "same_ticker_side_buy_lot"
                        evidence = {
                            **evidence,
                            "buy_fill_id": buy_fill.id,
                            "buy_trade_id": buy_fill.trade_id,
                            "buy_strategy_code": buy_fill.strategy_code,
                        }

                order_changed = new_order_id is not None and fill.order_id != new_order_id
                strategy_changed = new_strategy is not None and fill.strategy_code != new_strategy
                side_changed = new_side in {"yes", "no"} and fill.side != new_side
                if not (order_changed or strategy_changed or side_changed):
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
                    "old_side": fill.side,
                    "new_side": new_side,
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
                    if side_changed:
                        fill.side = new_side
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

    async def repair_stale_positions(
        self,
        *,
        kalshi_env: str = "demo",
        dry_run: bool = True,
        now: datetime | None = None,
        limit: int = 500,
        subaccount: int = 0,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        max_age_seconds = max(1, int(self.settings.daemon_reconcile_stale_kill_switch_seconds))

        def position_payload(position: PositionRecord) -> dict[str, Any]:
            return {
                "market_ticker": position.market_ticker,
                "side": position.side,
                "count_fp": str(position.count_fp),
                "average_price_dollars": str(position.average_price_dollars),
                "updated_at": _iso(position.updated_at),
            }

        async with self.session_factory() as session:
            checkpoint = (
                await session.execute(
                    select(Checkpoint).where(Checkpoint.stream_name == f"reconcile:{kalshi_env}")
                )
            ).scalar_one_or_none()
            payload = dict(checkpoint.payload or {}) if checkpoint is not None else {}
            reconciled_at = _parse_utc(payload.get("reconciled_at"))
            reconcile_age_seconds = (
                int((now - reconciled_at).total_seconds()) if reconciled_at is not None else None
            )
            fresh = reconcile_age_seconds is not None and reconcile_age_seconds <= max_age_seconds
            live_tickers = {str(ticker) for ticker in payload.get("live_tickers") or []}

            positions = list(
                (
                    await session.execute(
                        select(PositionRecord)
                        .where(
                            PositionRecord.kalshi_env == kalshi_env,
                            PositionRecord.subaccount == subaccount,
                            PositionRecord.count_fp != 0,
                        )
                        .order_by(PositionRecord.updated_at.desc(), PositionRecord.market_ticker.asc())
                    )
                ).scalars()
            )
            candidates = [
                position for position in positions if fresh and position.market_ticker not in live_tickers
            ][:limit]
            protected = [
                position for position in positions if (not fresh or position.market_ticker in live_tickers)
            ]
            updated = 0
            repaired_at = now.isoformat()
            for position in candidates:
                raw = dict(position.raw or {})
                candidate_payload = {
                    "repaired_at": repaired_at,
                    "reason": "absent_from_fresh_exchange_reconcile",
                    "reconcile_stream": f"reconcile:{kalshi_env}",
                    "reconciled_at": reconciled_at.isoformat() if reconciled_at is not None else None,
                    "old_count_fp": str(position.count_fp),
                    "old_side": position.side,
                    "live_tickers_count": len(live_tickers),
                }
                if not dry_run:
                    raw["trade_behavior_stale_position_repair"] = candidate_payload
                    position.raw = raw
                    position.count_fp = Decimal("0")
                    updated += 1
            candidate_rows = [position_payload(position) for position in candidates[:50]]
            protected_rows = [position_payload(position) for position in protected[:50]]
            if dry_run:
                await session.rollback()
            else:
                if updated:
                    await session.flush()
                await session.commit()

        return {
            "kalshi_env": kalshi_env,
            "dry_run": dry_run,
            "repair_target": "stale-positions",
            "fresh_reconcile": fresh,
            "reconcile_stream": f"reconcile:{kalshi_env}",
            "reconciled_at": reconciled_at.isoformat() if reconciled_at is not None else None,
            "reconcile_age_seconds": reconcile_age_seconds,
            "max_reconcile_age_seconds": max_age_seconds,
            "live_tickers": sorted(live_tickers),
            "candidate_count": len(candidates),
            "protected_count": len(protected),
            "updated_count": 0 if dry_run else updated,
            "candidates": candidate_rows,
            "protected": protected_rows,
            "status": "ready" if fresh else "stale_reconcile_checkpoint",
        }

    async def repair_market_snapshots(
        self,
        *,
        kalshi_env: str = "demo",
        days: int = 30,
        dry_run: bool = True,
        now: datetime | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        cutoff = now - timedelta(days=days)
        candidates: list[dict[str, Any]] = []
        updated = 0
        skipped_existing = 0
        unrecoverable = 0
        async with self.session_factory() as session:
            signals = await self._signals(session, kalshi_env=kalshi_env, cutoff=cutoff)
            for signal in signals:
                if len(candidates) >= limit:
                    break
                decision_ts = _as_utc(signal.created_at) or now
                existing = await self._existing_point_in_time_market_snapshot(
                    session,
                    market_ticker=signal.market_ticker,
                    decision_ts=decision_ts,
                )
                if existing is not None:
                    skipped_existing += 1
                    continue

                recovered = await self._recover_market_snapshot_for_signal(
                    session,
                    signal,
                    kalshi_env=kalshi_env,
                    decision_ts=decision_ts,
                )
                if recovered is None:
                    unrecoverable += 1
                    continue

                provenance = dict(recovered["snapshot_provenance"])
                recovered_observed_at = _as_utc(recovered.get("observed_at"))
                if (
                    recovered_observed_at is not None
                    and recovered_observed_at > decision_ts
                    and provenance.get("leakage_risk") in {"none", "point_in_time"}
                ):
                    provenance["leakage_risk"] = "future_quote"
                source_kind = str(provenance["source_kind"])
                source_id = f"trade_analysis_backfill:{signal.id}:{source_kind}"
                duplicate = (
                    await session.execute(
                        select(HistoricalMarketSnapshotRecord).where(
                            HistoricalMarketSnapshotRecord.market_ticker == signal.market_ticker,
                            HistoricalMarketSnapshotRecord.source_kind == source_kind,
                            HistoricalMarketSnapshotRecord.source_id == source_id,
                        )
                    )
                ).scalar_one_or_none()
                if duplicate is not None:
                    skipped_existing += 1
                    continue
                row_payload = {
                    "snapshot_provenance": {
                        **provenance,
                        "source_id": provenance.get("source_id"),
                        "backfill_source_id": source_id,
                        "backfilled_at": now.isoformat(),
                        "decision_ts": decision_ts.isoformat(),
                    },
                    "source_snapshot": recovered.get("payload") or {},
                }
                candidate = {
                    "signal_id": signal.id,
                    "room_id": signal.room_id,
                    "market_ticker": signal.market_ticker,
                    "decision_ts": decision_ts.isoformat(),
                    "source_kind": source_kind,
                    "source_id": source_id,
                    "leakage_risk": provenance.get("leakage_risk"),
                    "observed_at": _iso(recovered_observed_at),
                    "yes_bid_dollars": _money(recovered.get("yes_bid_dollars")),
                    "yes_ask_dollars": _money(recovered.get("yes_ask_dollars")),
                }
                candidates.append(candidate)
                if dry_run:
                    continue

                record = HistoricalMarketSnapshotRecord(
                    market_ticker=signal.market_ticker,
                    series_ticker=_series_from_ticker(signal.market_ticker),
                    station_id=None,
                    local_market_day=_market_day_from_ticker(signal.market_ticker),
                    asof_ts=recovered_observed_at or decision_ts,
                    source_kind=source_kind,
                    source_id=source_id,
                    source_hash=_hash_payload(row_payload),
                    yes_bid_dollars=recovered.get("yes_bid_dollars"),
                    yes_ask_dollars=recovered.get("yes_ask_dollars"),
                    no_ask_dollars=recovered.get("no_ask_dollars"),
                    last_price_dollars=recovered.get("last_price_dollars"),
                    payload=row_payload,
                )
                session.add(record)
                updated += 1

            if dry_run:
                await session.rollback()
            else:
                await session.commit()

        return {
            "kalshi_env": kalshi_env,
            "window_days": days,
            "dry_run": dry_run,
            "repair_target": "market-snapshots",
            "candidate_count": len(candidates),
            "updated_count": 0 if dry_run else updated,
            "skipped_existing_count": skipped_existing,
            "unrecoverable_count": unrecoverable,
            "candidates": candidates[:50],
        }

    async def _existing_point_in_time_market_snapshot(
        self,
        session: AsyncSession,
        *,
        market_ticker: str,
        decision_ts: datetime,
    ) -> HistoricalMarketSnapshotRecord | None:
        cutoff = decision_ts - timedelta(seconds=max(1, int(self.settings.risk_stale_market_seconds)))
        return (
            await session.execute(
                select(HistoricalMarketSnapshotRecord)
                .where(
                    HistoricalMarketSnapshotRecord.market_ticker == market_ticker,
                    HistoricalMarketSnapshotRecord.asof_ts <= decision_ts,
                    HistoricalMarketSnapshotRecord.asof_ts >= cutoff,
                    HistoricalMarketSnapshotRecord.source_kind.in_(sorted(_POINT_IN_TIME_MARKET_SOURCE_KINDS)),
                )
                .order_by(HistoricalMarketSnapshotRecord.asof_ts.desc(), HistoricalMarketSnapshotRecord.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    async def _recover_market_snapshot_for_signal(
        self,
        session: AsyncSession,
        signal: Signal,
        *,
        kalshi_env: str,
        decision_ts: datetime,
    ) -> dict[str, Any] | None:
        artifact = (
            await session.execute(
                select(Artifact)
                .where(Artifact.room_id == signal.room_id, Artifact.artifact_type == "market_snapshot")
                .order_by(Artifact.updated_at.desc(), Artifact.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if artifact is not None:
            recovered = self._market_snapshot_from_payload(
                artifact.payload,
                observed_at=_as_utc(artifact.created_at),
                source_kind="trade_analysis_backfill_room_artifact",
                source_id=artifact.id,
                leakage_risk="none",
            )
            if recovered is not None:
                return recovered

        signal_payload = dict(signal.payload or {})
        signal_snapshot = signal_payload.get("market_snapshot")
        if isinstance(signal_snapshot, dict):
            recovered = self._market_snapshot_from_payload(
                signal_snapshot,
                observed_at=_parse_utc(signal_snapshot.get("observed_at")) or _as_utc(signal.created_at),
                source_kind="trade_analysis_backfill_signal_payload",
                source_id=signal.id,
                leakage_risk="none",
            )
            if recovered is not None:
                return recovered

        stale_cutoff = decision_ts - timedelta(seconds=max(1, int(self.settings.risk_stale_market_seconds)))
        durable_decision = await self._latest_historical_market_snapshot(
            session,
            market_ticker=signal.market_ticker,
            source_kind=DECISION_SIGNAL_MARKET_SOURCE_KIND,
            decision_ts=decision_ts,
            cutoff=stale_cutoff,
        )
        if durable_decision is not None:
            return self._market_snapshot_from_historical(
                durable_decision,
                source_kind=DECISION_SIGNAL_MARKET_SOURCE_KIND,
                leakage_risk="none",
            )

        daemon_snapshot = await self._latest_historical_market_snapshot(
            session,
            market_ticker=signal.market_ticker,
            source_kind=DAEMON_MARKET_PRICE_SOURCE_KIND,
            decision_ts=decision_ts,
            cutoff=stale_cutoff,
        )
        if daemon_snapshot is not None:
            return self._market_snapshot_from_historical(
                daemon_snapshot,
                source_kind=DAEMON_MARKET_PRICE_SOURCE_KIND,
                leakage_risk="point_in_time",
            )

        history = (
            await session.execute(
                select(MarketPriceHistory)
                .where(
                    MarketPriceHistory.kalshi_env == kalshi_env,
                    MarketPriceHistory.market_ticker == signal.market_ticker,
                    MarketPriceHistory.observed_at <= decision_ts,
                )
                .order_by(MarketPriceHistory.observed_at.desc(), MarketPriceHistory.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if history is not None:
            return {
                "observed_at": history.observed_at,
                "yes_bid_dollars": history.yes_bid_dollars,
                "yes_ask_dollars": history.yes_ask_dollars,
                "no_ask_dollars": None,
                "last_price_dollars": history.last_trade_dollars,
                "payload": {},
                "snapshot_provenance": {
                    "recovered": True,
                    "source": "market_price_history",
                    "source_kind": "trade_analysis_backfill_market_price_history",
                    "source_id": history.id,
                    "leakage_risk": "point_in_time",
                },
            }

        historical = (
            await session.execute(
                select(HistoricalMarketSnapshotRecord)
                .where(
                    HistoricalMarketSnapshotRecord.market_ticker == signal.market_ticker,
                    HistoricalMarketSnapshotRecord.asof_ts <= decision_ts,
                    HistoricalMarketSnapshotRecord.source_kind.in_(sorted(_POINT_IN_TIME_MARKET_SOURCE_KINDS)),
                )
                .order_by(HistoricalMarketSnapshotRecord.asof_ts.desc(), HistoricalMarketSnapshotRecord.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if historical is not None:
            return self._market_snapshot_from_historical(
                historical,
                source_kind="trade_analysis_backfill_historical_snapshot",
                leakage_risk="point_in_time",
            )

        candlestick = await self._recover_market_snapshot_from_candlesticks(
            signal.market_ticker,
            decision_ts=decision_ts,
        )
        if candlestick is not None:
            return candlestick

        final = (
            await session.execute(
                select(HistoricalMarketSnapshotRecord)
                .where(
                    HistoricalMarketSnapshotRecord.market_ticker == signal.market_ticker,
                    HistoricalMarketSnapshotRecord.source_kind == "kalshi_final_market",
                )
                .order_by(HistoricalMarketSnapshotRecord.asof_ts.desc(), HistoricalMarketSnapshotRecord.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if final is not None:
            return self._market_snapshot_from_historical(
                final,
                source_kind="trade_analysis_backfill_final_market",
                leakage_risk="final_market",
            )
        return None

    async def _latest_historical_market_snapshot(
        self,
        session: AsyncSession,
        *,
        market_ticker: str,
        source_kind: str,
        decision_ts: datetime,
        cutoff: datetime,
    ) -> HistoricalMarketSnapshotRecord | None:
        return (
            await session.execute(
                select(HistoricalMarketSnapshotRecord)
                .where(
                    HistoricalMarketSnapshotRecord.market_ticker == market_ticker,
                    HistoricalMarketSnapshotRecord.source_kind == source_kind,
                    HistoricalMarketSnapshotRecord.asof_ts <= decision_ts,
                    HistoricalMarketSnapshotRecord.asof_ts >= cutoff,
                )
                .order_by(HistoricalMarketSnapshotRecord.asof_ts.desc(), HistoricalMarketSnapshotRecord.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    async def _recover_market_snapshot_from_candlesticks(
        self,
        market_ticker: str,
        *,
        decision_ts: datetime,
    ) -> dict[str, Any] | None:
        if self.kalshi is None:
            return None
        series = _series_from_ticker(market_ticker)
        if not series:
            return None
        lookback_hours = max(1, int(getattr(self.settings, "historical_replay_market_snapshot_lookback_hours", 36)))
        window_start = decision_ts - timedelta(hours=lookback_hours)
        selected: tuple[dict[str, Any], datetime] | None = None
        for period_interval in (1, 60):
            try:
                response = await self.kalshi.get_market_candlesticks(
                    series,
                    market_ticker,
                    period_interval=period_interval,
                    start_ts=int(window_start.timestamp()),
                    end_ts=int(decision_ts.timestamp()),
                )
            except Exception:
                continue
            for candlestick in response.get("candlesticks") or []:
                end_period_ts = candlestick.get("end_period_ts")
                if end_period_ts in (None, ""):
                    continue
                try:
                    end_at = datetime.fromtimestamp(int(end_period_ts), tz=UTC)
                except (OverflowError, OSError, ValueError):
                    continue
                if end_at <= decision_ts:
                    selected = (candlestick, end_at)
            if selected is not None:
                break
        if selected is None:
            return None
        candlestick, observed_at = selected
        stale_seconds = (decision_ts - observed_at).total_seconds()
        if stale_seconds < 0 or stale_seconds > float(self.settings.historical_replay_market_stale_seconds):
            return None
        yes_bid = _decimal_or_none((candlestick.get("yes_bid") or {}).get("close_dollars"))
        yes_ask = _decimal_or_none((candlestick.get("yes_ask") or {}).get("close_dollars"))
        last_price = _decimal_or_none((candlestick.get("price") or {}).get("close_dollars"))
        no_ask = (Decimal("1.0000") - yes_bid).quantize(Decimal("0.0001")) if yes_bid is not None else None
        if yes_bid is None and yes_ask is None:
            return None
        return {
            "observed_at": observed_at,
            "yes_bid_dollars": yes_bid,
            "yes_ask_dollars": yes_ask,
            "no_ask_dollars": no_ask,
            "last_price_dollars": last_price,
            "payload": {
                "candlestick": candlestick,
                "reconstructed_from": "kalshi_candlesticks",
                "market_ticker": market_ticker,
                "decision_ts": decision_ts.isoformat(),
            },
            "snapshot_provenance": {
                "recovered": True,
                "source": "kalshi_candlestick",
                "source_kind": TRADE_ANALYSIS_CANDLESTICK_BACKFILL_SOURCE_KIND,
                "source_id": f"{market_ticker}:{int(observed_at.timestamp())}",
                "leakage_risk": "point_in_time",
            },
        }

    @staticmethod
    def _market_snapshot_from_payload(
        payload: Any,
        *,
        observed_at: datetime | None,
        source_kind: str,
        source_id: str,
        leakage_risk: str,
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        market = payload.get("market") if isinstance(payload.get("market"), dict) else payload
        meta = payload.get("_snapshot_meta") if isinstance(payload.get("_snapshot_meta"), dict) else {}
        payload_observed_at = (
            _parse_utc(market.get("observed_at"))
            or _parse_utc(payload.get("observed_at"))
            or _parse_utc(meta.get("observed_at"))
            or observed_at
        )
        yes_bid = _decimal_or_none(market.get("yes_bid_dollars") or market.get("yes_bid"))
        yes_ask = _decimal_or_none(market.get("yes_ask_dollars") or market.get("yes_ask"))
        no_ask = _decimal_or_none(market.get("no_ask_dollars") or market.get("no_ask"))
        if yes_ask is None and no_ask is not None:
            yes_ask = Decimal("1") - no_ask
        if yes_bid is None and yes_ask is None:
            return None
        return {
            "observed_at": payload_observed_at,
            "yes_bid_dollars": yes_bid,
            "yes_ask_dollars": yes_ask,
            "no_ask_dollars": no_ask,
            "last_price_dollars": _decimal_or_none(
                market.get("last_price_dollars")
                or market.get("last_trade_dollars")
                or market.get("last_price")
            ),
            "payload": payload,
            "snapshot_provenance": {
                "recovered": True,
                "source": source_kind,
                "source_kind": source_kind,
                "source_id": source_id,
                "leakage_risk": leakage_risk,
            },
        }

    @staticmethod
    def _market_snapshot_from_historical(
        snapshot: HistoricalMarketSnapshotRecord,
        *,
        source_kind: str,
        leakage_risk: str,
    ) -> dict[str, Any]:
        return {
            "observed_at": snapshot.asof_ts,
            "yes_bid_dollars": snapshot.yes_bid_dollars,
            "yes_ask_dollars": snapshot.yes_ask_dollars,
            "no_ask_dollars": snapshot.no_ask_dollars,
            "last_price_dollars": snapshot.last_price_dollars,
            "payload": snapshot.payload or {},
            "snapshot_provenance": {
                "recovered": True,
                "source": snapshot.source_kind,
                "source_kind": source_kind,
                "source_id": snapshot.id,
                "leakage_risk": leakage_risk,
            },
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

    async def _decision_traces(self, session: AsyncSession, *, kalshi_env: str, cutoff: datetime) -> list[DecisionTraceRecord]:
        result = await session.execute(
            select(DecisionTraceRecord)
            .where(
                DecisionTraceRecord.kalshi_env == kalshi_env,
                DecisionTraceRecord.decision_time >= cutoff,
            )
            .order_by(DecisionTraceRecord.decision_time.asc(), DecisionTraceRecord.id.asc())
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
        realized_pnl = Decimal("0")
        settlement_pnl = Decimal("0")
        realized_trades = 0
        settled_trades = 0
        unsettled_open_contracts = Decimal("0")
        open_lot_count = 0
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
                matched_pnl = (price - lot.price) * matched
                gross_pnl += matched_pnl
                realized_pnl += matched_pnl
                lot.count -= matched
                remaining -= matched
                realized_trades += 1

        for lots in lots_by_key.values():
            for lot in lots:
                if lot.count <= 0:
                    continue
                if lot.settlement_result == "win":
                    lot_pnl = (Decimal("1") - lot.price) * lot.count
                    gross_pnl += lot_pnl
                    settlement_pnl += lot_pnl
                    settled_trades += 1
                elif lot.settlement_result == "loss":
                    lot_pnl = -lot.price * lot.count
                    gross_pnl += lot_pnl
                    settlement_pnl += lot_pnl
                    settled_trades += 1
                else:
                    open_lot_count += 1
                    unsettled_open_contracts += lot.count

        all_fees_present = bool(fills) and fee_seen == len(fills)
        return {
            "gross_pnl_dollars": _money(gross_pnl),
            "fill_ledger_realized_pnl_dollars": _money(realized_pnl),
            "settlement_pnl_dollars": _money(settlement_pnl),
            "fee_total_dollars": _money(fee_total) if fee_seen else None,
            "net_pnl_dollars": _money(gross_pnl - fee_total) if all_fees_present else None,
            "fee_coverage": {"fills_with_fee": fee_seen, "total_fills": len(fills), "complete": all_fees_present},
            "realized_exit_matches": realized_trades,
            "settled_lots_scored": settled_trades,
            "open_lot_count": open_lot_count,
            "unsettled_open_contracts": str(unsettled_open_contracts),
        }

    def _lifecycle_ledger(self, fills: list[FillRecord], *, orders: list[OrderRecord] | None = None) -> dict[str, Any]:
        lots_by_key: dict[tuple[str, str, str], list[_LedgerLot]] = defaultdict(list)
        rows: dict[str, dict[str, Any]] = {}
        orders_by_id = {order.id: order for order in orders or []}

        def ensure(bucket: str, fill: FillRecord) -> dict[str, Any]:
            dimensions = bucket_dimensions_from_key(bucket)
            row = rows.setdefault(
                bucket,
                {
                    "bucket_key": bucket,
                    "kalshi_env": fill.kalshi_env,
                    "series_ticker": dimensions["series_ticker"],
                    "station": dimensions["station"],
                    "strategy_code": dimensions["strategy_code"] if dimensions["strategy_code"] != "<unknown>" else _strategy_key(fill.strategy_code),
                    "side": dimensions["side"] if dimensions["side"] != "unknown" else fill.side,
                    "entry_price_band": dimensions["entry_price_band"],
                    "forecast_delta_band": dimensions["forecast_delta_band"],
                    "confidence_band": dimensions["confidence_band"],
                    "spread_band": dimensions["spread_band"],
                    "fills": 0,
                    "contracts": Decimal("0"),
                    "fees": Decimal("0"),
                    "lifecycle_gross_pnl": Decimal("0"),
                    "lifecycle_net_pnl": Decimal("0"),
                    "settled_or_closed_count": 0,
                    "win_count": 0,
                },
            )
            return row

        def add_fee(row: dict[str, Any], fee: Decimal) -> None:
            row["fees"] += fee
            row["lifecycle_net_pnl"] -= fee

        for fill in fills:
            strategy = _strategy_key(fill.strategy_code)
            key = (fill.market_ticker, fill.side, strategy)
            price = _side_price(fill)
            count = Decimal(fill.count_fp)
            fee = _decimal_or_none((fill.raw or {}).get("fee_cost")) or Decimal("0")
            if fill.action == "buy":
                bucket = bucket_key_for_fill(fill, orders_by_id.get(str(fill.order_id)))
                row = ensure(bucket, fill)
                row["fills"] += 1
                row["contracts"] += count
                add_fee(row, fee)
                lots_by_key[key].append(
                    _LedgerLot(
                        count=count,
                        price=price,
                        settlement_result=fill.settlement_result,
                        bucket_key=bucket,
                    )
                )
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
                row = ensure(lot.bucket_key, fill)
                pnl = (price - lot.price) * matched
                fee_share = fee * (matched / count) if count > 0 else Decimal("0")
                row["fills"] += 1
                row["contracts"] += matched
                row["lifecycle_gross_pnl"] += pnl
                row["lifecycle_net_pnl"] += pnl
                row["settled_or_closed_count"] += 1
                if pnl > 0:
                    row["win_count"] += 1
                add_fee(row, fee_share)
                lot.count -= matched
                remaining -= matched

        for lots in lots_by_key.values():
            for lot in lots:
                if lot.count <= 0 or lot.settlement_result not in {"win", "loss"}:
                    continue
                row = rows[lot.bucket_key]
                pnl = (
                    (Decimal("1") - lot.price) * lot.count
                    if lot.settlement_result == "win"
                    else -lot.price * lot.count
                )
                row["lifecycle_gross_pnl"] += pnl
                row["lifecycle_net_pnl"] += pnl
                row["settled_or_closed_count"] += 1
                if lot.settlement_result == "win":
                    row["win_count"] += 1

        out = []
        for row in rows.values():
            sample_count = int(row["settled_or_closed_count"])
            out.append({
                "bucket_key": row["bucket_key"],
                "kalshi_env": row["kalshi_env"],
                "series_ticker": row["series_ticker"],
                "station": row["station"],
                "strategy_code": row["strategy_code"],
                "side": row["side"],
                "entry_price_band": row["entry_price_band"],
                "forecast_delta_band": row["forecast_delta_band"],
                "confidence_band": row["confidence_band"],
                "spread_band": row["spread_band"],
                "fills": row["fills"],
                "contracts": str(row["contracts"]),
                "fees": _money(row["fees"]),
                "lifecycle_gross_pnl": _money(row["lifecycle_gross_pnl"]),
                "lifecycle_net_pnl": _money(row["lifecycle_net_pnl"]),
                "settled_or_closed_count": sample_count,
                "bucket_sample_count": sample_count,
                "bucket_win_rate": round(row["win_count"] / sample_count, 6) if sample_count else None,
                "bucket_net_pnl": _money(row["lifecycle_net_pnl"]),
            })

        out.sort(key=lambda item: (Decimal(str(item["lifecycle_net_pnl"] or "0")), item["bucket_key"]))
        return {
            "bucket_count": len(out),
            "buckets": out,
            "worst_buckets": out[:20],
            "best_buckets": list(reversed(out[-20:])),
        }

    def _exchange_position_discrepancy(
        self,
        *,
        fills: list[FillRecord],
        positions: list[PositionRecord],
    ) -> dict[str, Any]:
        ledger_open: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
        for fill in fills:
            if fill.settlement_result in {"win", "loss"}:
                continue
            key = (fill.market_ticker, fill.side)
            count = Decimal(fill.count_fp)
            if fill.action == "buy":
                ledger_open[key] += count
            elif fill.action == "sell":
                ledger_open[key] -= count
        ledger_open = {key: value for key, value in ledger_open.items() if value != 0}

        exchange_open: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
        for position in positions:
            exchange_open[(position.market_ticker, position.side)] += Decimal(position.count_fp)

        rows: list[dict[str, Any]] = []
        for key in sorted(set(ledger_open) | set(exchange_open)):
            ledger_count = ledger_open.get(key, Decimal("0"))
            exchange_count = exchange_open.get(key, Decimal("0"))
            discrepancy = ledger_count - exchange_count
            if discrepancy == 0:
                continue
            rows.append(
                {
                    "market_ticker": key[0],
                    "side": key[1],
                    "ledger_open_contracts": str(ledger_count),
                    "exchange_open_contracts": str(exchange_count),
                    "discrepancy_contracts": str(discrepancy),
                }
            )
        return {
            "discrepancy_count": len(rows),
            "total_abs_discrepancy_contracts": str(
                sum((abs(Decimal(row["discrepancy_contracts"])) for row in rows), Decimal("0"))
            ),
            "rows": rows[:20],
        }

    def _attribution_gaps(self, *, fills: list[FillRecord], orders: list[OrderRecord]) -> dict[str, Any]:
        orders_by_kalshi_id = {order.kalshi_order_id: order for order in orders if order.kalshi_order_id}
        missing_fills = [fill for fill in fills if fill.strategy_code is None]
        raw_order_matches = [
            fill
            for fill in missing_fills
            if isinstance(fill.raw, dict)
            and fill.raw.get("order_id") in orders_by_kalshi_id
            and (
                orders_by_kalshi_id[fill.raw.get("order_id")].strategy_code
                or _inferred_strategy_for_orphaned_order(orders_by_kalshi_id[fill.raw.get("order_id")])
            )
        ]
        inferable_missing_orders = [
            order
            for order in orders
            if order.strategy_code is None and _inferred_strategy_for_orphaned_order(order) is not None
        ]
        top_tickers = Counter(fill.market_ticker for fill in missing_fills).most_common(10)
        return {
            "missing_fill_strategy_count": len(missing_fills),
            "missing_fill_strategy_contracts": str(sum((Decimal(f.count_fp) for f in missing_fills), Decimal("0.00"))),
            "missing_order_strategy_count": sum(1 for order in orders if order.strategy_code is None),
            "inferable_missing_order_strategy_count": len(inferable_missing_orders),
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

    def _signal_funnel(
        self,
        *,
        signals: list[Signal],
        tickets: list[TradeTicketRecord],
        decision_traces: list[DecisionTraceRecord],
        now: datetime,
    ) -> dict[str, Any]:
        ticketed_room_ids = {ticket.room_id for ticket in tickets}
        traces_by_room: dict[str, DecisionTraceRecord] = {}
        for trace in decision_traces:
            if trace.room_id is None:
                continue
            prior = traces_by_room.get(trace.room_id)
            if prior is None or (_as_utc(trace.decision_time) or datetime.min.replace(tzinfo=UTC)) >= (
                _as_utc(prior.decision_time) or datetime.min.replace(tzinfo=UTC)
            ):
                traces_by_room[trace.room_id] = trace
        recent_cutoff = now - timedelta(hours=24)
        outcome_counts: Counter[str] = Counter()
        stand_down_counts: Counter[str] = Counter()
        side_counts: Counter[str] = Counter()
        top_markets: dict[str, dict[str, Any]] = {}
        selected_without_ticket: list[Signal] = []
        pre_risk_filtered_without_ticket: list[dict[str, Any]] = []
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
                    trace_record = traces_by_room.get(signal.room_id)
                    trace = dict(trace_record.trace or {}) if trace_record is not None else {}
                    final_stand_down_reason = self._final_stand_down_reason(payload, trace)
                    final_outcome = str(
                        payload.get("final_outcome")
                        or candidate_trace.get("final_outcome")
                        or trace.get("evaluation_outcome")
                        or trace.get("final_outcome")
                        or ""
                    )
                    if final_stand_down_reason in _PRE_RISK_STAND_DOWN_REASONS or final_outcome == "pre_risk_filtered":
                        outcome_counts["selected_pre_risk_filtered_without_ticket"] += 1
                        stand_down_counts[final_stand_down_reason or "pre_risk_filtered"] += 1
                        pre_risk_filtered_without_ticket.append(
                            self._selected_without_ticket_payload(
                                signal,
                                payload=payload,
                                candidate_trace=candidate_trace,
                                final_outcome=final_outcome or "pre_risk_filtered",
                                final_stand_down_reason=final_stand_down_reason,
                                decision_trace_id=trace_record.id if trace_record is not None else None,
                            )
                        )
                    else:
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
        recent_selected = [
            signal
            for signal in selected_without_ticket
            if _as_utc(signal.created_at) is not None and _as_utc(signal.created_at) >= recent_cutoff
        ]
        legacy_selected = [
            signal
            for signal in selected_without_ticket
            if _as_utc(signal.created_at) is None or _as_utc(signal.created_at) < recent_cutoff
        ]
        recent_selected_without_ticket = []
        for signal in recent_selected[:20]:
            payload = dict(signal.payload or {})
            candidate_trace = dict(payload.get("candidate_trace") or {})
            recent_selected_without_ticket.append(
                self._selected_without_ticket_payload(signal, payload=payload, candidate_trace=candidate_trace)
            )
        return {
            "signals": len(signals),
            "candidate_selected": int(outcome_counts.get("candidate_selected", 0)),
            "selected_without_ticket_count": len(selected_without_ticket),
            "recent_selected_without_ticket_count": len(recent_selected),
            "legacy_selected_without_ticket_count": len(legacy_selected),
            "selected_pre_risk_filtered_without_ticket_count": len(pre_risk_filtered_without_ticket),
            "selected_pre_risk_filtered_without_ticket": pre_risk_filtered_without_ticket[:20],
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
    def _final_stand_down_reason(payload: dict[str, Any], trace: dict[str, Any]) -> str | None:
        candidate_trace = dict(payload.get("candidate_trace") or {})
        sizing = dict(trace.get("sizing") or {})
        for value in (
            payload.get("final_stand_down_reason"),
            payload.get("stand_down_reason"),
            candidate_trace.get("final_stand_down_reason"),
            candidate_trace.get("eligibility_stand_down_reason"),
            sizing.get("stand_down_reason"),
            trace.get("stand_down_reason"),
        ):
            if value not in (None, "", "none"):
                return str(value)
        return None

    @staticmethod
    def _selected_without_ticket_payload(
        signal: Signal,
        *,
        payload: dict[str, Any],
        candidate_trace: dict[str, Any],
        final_outcome: str | None = None,
        final_stand_down_reason: str | None = None,
        decision_trace_id: str | None = None,
    ) -> dict[str, Any]:
        row = {
            "room_id": signal.room_id,
            "market_ticker": signal.market_ticker,
            "edge_bps": signal.edge_bps,
            "confidence": signal.confidence,
            "recommended_side": payload.get("recommended_side") or candidate_trace.get("selected_side"),
            "created_at": _iso(signal.created_at),
        }
        if final_outcome:
            row["final_outcome"] = final_outcome
        if final_stand_down_reason:
            row["final_stand_down_reason"] = final_stand_down_reason
        if decision_trace_id:
            row["decision_trace_id"] = decision_trace_id
        return row

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
        kill_switch_grouped: dict[tuple[str, str], list[OpsEvent]] = defaultdict(list)
        for event in stop_events:
            payload = event.payload or {}
            ticker = str(payload.get("market_ticker") or "unknown")
            trigger = str(payload.get("trigger") or "unknown")
            grouped[(ticker, trigger)].append(event)
            if payload.get("action") == "stop_loss_kill_switch_suppressed":
                kill_switch_grouped[(ticker, trigger)].append(event)

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

        kill_switch_clusters = []
        for (ticker, trigger), events in kill_switch_grouped.items():
            first_at = min((_as_utc(event.created_at) for event in events if event.created_at), default=None)
            last_at = max((_as_utc(event.created_at) for event in events if event.created_at), default=None)
            span_seconds = max(0, int(((last_at or now) - (first_at or now)).total_seconds()))
            latest = max(events, key=lambda event: _as_utc(event.created_at) or datetime.min.replace(tzinfo=UTC))
            latest_payload = latest.payload if isinstance(latest.payload, dict) else {}
            kill_switch_clusters.append({
                "market_ticker": ticker,
                "trigger": trigger,
                "events": len(events),
                "first_at": _iso(first_at),
                "last_at": _iso(last_at),
                "span_minutes": round(span_seconds / 60, 1),
                "cooldown_seconds": cooldown,
                "repeated": len(events) > 1,
                "persisted_past_cooldown": span_seconds >= cooldown,
                "latest_kill_switch": latest_payload.get("kill_switch") or {},
                "latest_reconcile": latest_payload.get("reconcile") or {},
            })
        kill_switch_clusters.sort(key=lambda item: (int(item["events"]), item["span_minutes"]), reverse=True)
        return {
            "event_count": len(stop_events),
            "clusters": clusters[:20],
            "kill_switch_suppressed_event_count": sum(len(events) for events in kill_switch_grouped.values()),
            "kill_switch_suppressed_clusters": kill_switch_clusters[:20],
        }

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

    def _trigger_diagnostics(self, ops_events: list[OpsEvent]) -> dict[str, Any]:
        tracked_reasons = {
            "market_state_missing",
            "one_sided_book",
            "no_taker_quote",
            "non_positive_spread",
            "spread_too_wide",
        }
        reason_counts: Counter[str] = Counter()
        actionability_counts: Counter[str] = Counter()
        one_sided_tradeable_count = 0
        market_rollups: dict[str, dict[str, Any]] = {}

        for event in ops_events:
            if event.source != "auto_trigger":
                continue
            payload = event.payload if isinstance(event.payload, dict) else {}
            probe = payload.get("one_sided_tradeable_probe") if isinstance(payload.get("one_sided_tradeable_probe"), dict) else None
            if probe is not None and probe.get("one_sided") is True:
                one_sided_tradeable_count += 1
                actionability_counts[str(probe.get("actionability") or "one_sided_book_side_aware_probe")] += 1
            reason = str(payload.get("reason") or "")
            if reason not in tracked_reasons:
                continue
            ticker = str(payload.get("market_ticker") or "unknown")
            reason_counts[reason] += 1
            actionability = str(payload.get("actionability") or reason)
            actionability_counts[actionability] += 1
            latest_at = _as_utc(event.created_at)
            row = market_rollups.setdefault(
                ticker,
                {
                    "market_ticker": ticker,
                    "count": 0,
                    "reason_counts": Counter(),
                    "latest_at": latest_at,
                    "latest_reason": reason,
                    "latest_summary": event.summary,
                },
            )
            row["count"] += 1
            row["reason_counts"][reason] += 1
            current_latest = row.get("latest_at")
            if latest_at is not None and (current_latest is None or latest_at > current_latest):
                row["latest_at"] = latest_at
                row["latest_reason"] = reason
                row["latest_summary"] = event.summary

        top_markets = []
        for row in market_rollups.values():
            top_markets.append(
                {
                    "market_ticker": row["market_ticker"],
                    "count": row["count"],
                    "reason_counts": dict(row["reason_counts"]),
                    "latest_reason": row["latest_reason"],
                    "latest_summary": row["latest_summary"],
                    "latest_at": _iso(row["latest_at"]),
                }
            )
        top_markets.sort(key=lambda row: (int(row["count"]), row["latest_at"] or ""), reverse=True)

        return {
            "pre_room_miss_count": sum(reason_counts.values()),
            "one_sided_book_count": int(reason_counts.get("one_sided_book", 0)),
            "no_taker_quote_count": int(reason_counts.get("no_taker_quote", 0)),
            "one_sided_tradeable_probe_count": one_sided_tradeable_count,
            "wide_spread_count": int(reason_counts.get("spread_too_wide", 0)),
            "invalid_spread_count": int(reason_counts.get("non_positive_spread", 0)),
            "missing_market_state_count": int(reason_counts.get("market_state_missing", 0)),
            "reason_counts": dict(reason_counts),
            "actionability_counts": dict(actionability_counts),
            "top_markets": top_markets[:20],
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
        mark_stale_threshold_seconds = max(
            int(self.settings.risk_stale_market_seconds),
            int(self.settings.daemon_reconcile_stale_kill_switch_seconds),
        )
        for position in positions:
            market_state = market_states.get(position.market_ticker)
            mark = _current_side_mark(position, market_state)
            current = mark["price"]
            cost = Decimal(position.average_price_dollars) * Decimal(position.count_fp)
            total_cost += cost
            observed_at = _as_utc(market_state.observed_at) if market_state is not None else None
            stale_seconds = None if observed_at is None else int((now - observed_at).total_seconds())
            mark_missing = current is None
            is_stale = stale_seconds is None or stale_seconds > mark_stale_threshold_seconds
            conservative_zero_mark = bool(mark["mark_conservative"]) and current == Decimal("0.0000")
            is_stale_or_missing = mark_missing or (is_stale and not conservative_zero_mark)
            if is_stale_or_missing:
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
                "mark_stale_threshold_seconds": mark_stale_threshold_seconds,
                "market_state_stale": is_stale,
                "mark_source": mark["mark_source"],
                "mark_conservative": bool(mark["mark_conservative"]),
                "stale_or_missing_market_state": is_stale_or_missing,
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
        suppressed_exit_clusters = [
            cluster
            for cluster in stop_loss.get("kill_switch_suppressed_clusters", [])
            if cluster.get("repeated") or cluster.get("persisted_past_cooldown")
        ]
        if suppressed_exit_clusters:
            open_tickers = {row["market_ticker"] for row in exposure["positions"]}
            exposed_clusters = [
                cluster for cluster in suppressed_exit_clusters if cluster["market_ticker"] in open_tickers
            ]
            if exposed_clusters:
                issues.append({
                    "severity": "critical",
                    "code": "risk_reducing_exit_suppressed_by_kill_switch",
                    "summary": "Risk-reducing stop-loss exits are suppressed while open exposure remains.",
                    "evidence": {
                        "clusters": exposed_clusters[:10],
                        "open_position_markets": sorted(open_tickers),
                    },
                })
            else:
                issues.append({
                    "severity": "medium",
                    "code": "historical_risk_reducing_exit_suppressed_by_kill_switch",
                    "summary": "Historical stop-loss exits were suppressed by kill switch, but no matching open exposure remains.",
                    "evidence": {
                        "clusters": suppressed_exit_clusters[:10],
                        "open_position_markets": sorted(open_tickers),
                    },
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
        recent_selected_without_ticket = int(
            signal_funnel.get("recent_selected_without_ticket_count", signal_funnel["selected_without_ticket_count"])
        )
        if recent_selected_without_ticket:
            issues.append({
                "severity": "high",
                "code": "selected_signal_without_trade_ticket",
                "summary": "Some candidate-selected signals did not produce a trade ticket for risk/execution review.",
                "evidence": {
                    "selected_without_ticket_count": signal_funnel["selected_without_ticket_count"],
                    "recent_selected_without_ticket_count": recent_selected_without_ticket,
                    "legacy_selected_without_ticket_count": signal_funnel.get("legacy_selected_without_ticket_count", 0),
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
    trigger = report.get("trigger_diagnostics", {})
    lifecycle = report.get("lifecycle", {})
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
        (
            "Auto-trigger pre-room misses: "
            f"{trigger.get('pre_room_miss_count', 0)} "
            f"(one-sided book: {trigger.get('one_sided_book_count', 0)}, "
            f"wide spread: {trigger.get('wide_spread_count', 0)})"
        ),
    ]
    if lifecycle.get("worst_buckets"):
        lines.extend(["", "Worst lifecycle buckets:"])
        for row in lifecycle["worst_buckets"][:5]:
            lines.append(
                f"- {row['bucket_key']}: net={row['lifecycle_net_pnl']} "
                f"fees={row['fees']} win_rate={row['bucket_win_rate']}"
            )
        lines.append("")
        lines.append("Top risk reasons:")
    else:
        lines.append("")
        lines.append("Top risk reasons:")
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
