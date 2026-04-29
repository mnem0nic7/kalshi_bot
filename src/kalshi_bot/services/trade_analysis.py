from __future__ import annotations

import csv
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import (
    Artifact,
    FillRecord,
    HistoricalMarketSnapshotRecord,
    HistoricalReplayRunRecord,
    HistoricalSettlementLabelRecord,
    HistoricalWeatherSnapshotRecord,
    MarketPriceHistory,
    MarketState,
    OrderRecord,
    RiskVerdictRecord,
    Room,
    Signal,
    TradeTicketRecord,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory


SCHEMA_VERSION = "trade-analysis-v1"
MODEL_CARD_VERSION = "trade-analysis-model-card-v1"
TRAINING_EXCLUDED_REASONS = {
    "missing_market_snapshot",
    "missing_weather_snapshot",
    "pending_settlement",
    "missing_settlement_label",
    "missing_side",
    "missing_ticket_price",
}
PENDING_EXCLUSION_REASONS = {"pending_settlement"}
MODEL_REQUIRED_FEATURES = {
    "edge_bps",
    "confidence",
    "ticket_yes_price_dollars",
    "spread_dollars",
    "market_stale_seconds",
    "weather_stale_seconds",
}
MODEL_OPTIONAL_IMPUTED_FEATURES = {"forecast_residual_f"}
POINT_IN_TIME_HISTORICAL_MARKET_SOURCES = {
    "checkpoint_captured_market_snapshot",
    "captured_market_snapshot",
    "reconstructed_market_checkpoint",
}
IN_CLAUSE_BATCH_SIZE = 10_000


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: Any | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _decimal_str(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _group_pnl_by_series(picked: list[tuple[float, int, dict[str, Any]]]) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = {}
    for _prediction, _label, row in picked:
        series = str(row.get("series_ticker") or "<unknown>")
        grouped.setdefault(series, []).append(float(row.get("gross_pnl_dollars") or 0.0))
    return grouped


def _stale_age_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "missing"
    if seconds <= 60:
        return "<=60s"
    if seconds <= 300:
        return "61-300s"
    if seconds <= 900:
        return "301-900s"
    if seconds <= 3600:
        return "901-3600s"
    return ">3600s"


def _counter_rows(rows: list[dict[str, Any]], key: str, *, limit: int = 20) -> list[dict[str, Any]]:
    return [
        {"value": value, "rows": count}
        for value, count in Counter(str(row.get(key) or "<unknown>") for row in rows).most_common(limit)
    ]


def _batches(values: list[str], size: int = IN_CLAUSE_BATCH_SIZE) -> list[list[str]]:
    batch_size = max(1, int(size))
    return [values[idx: idx + batch_size] for idx in range(0, len(values), batch_size)]


def _market_day_from_ticker(ticker: str) -> str | None:
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def _market_date_from_ticker(ticker: str) -> datetime | None:
    market_day = _market_day_from_ticker(ticker)
    if market_day is None:
        return None
    try:
        return datetime.strptime(market_day, "%y%b%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def _series_from_ticker(ticker: str) -> str | None:
    prefix = ticker.split("-")[0] if ticker else ""
    return prefix or None


def _side_cost_from_yes_price(side: str | None, yes_price: Decimal | None) -> Decimal | None:
    if side is None or yes_price is None:
        return None
    if side == "yes":
        return Decimal(yes_price)
    if side == "no":
        return Decimal("1") - Decimal(yes_price)
    return None


def _label_win_for_side(side: str | None, kalshi_result: str | None, settlement_value: Decimal | None) -> bool | None:
    if side not in {"yes", "no"}:
        return None
    result = (kalshi_result or "").lower()
    if result in {"yes", "no"}:
        return result == side
    if settlement_value is not None:
        yes_won = Decimal(settlement_value) >= Decimal("0.5")
        return yes_won if side == "yes" else not yes_won
    return None


def _gross_pnl_for_side(
    *,
    side: str | None,
    count: Decimal | None,
    buy_yes_price: Decimal | None,
    avg_fill_yes_price: Decimal | None,
    settlement_value: Decimal | None,
) -> Decimal | None:
    if side not in {"yes", "no"} or count is None:
        return None
    entry = _side_cost_from_yes_price(side, avg_fill_yes_price or buy_yes_price)
    if entry is None:
        return None
    if settlement_value is None:
        return None
    yes_value = Decimal(settlement_value)
    payout = yes_value if side == "yes" else Decimal("1") - yes_value
    return (payout - entry) * Decimal(count)


@dataclass(slots=True)
class _LifecycleLot:
    count: Decimal
    price: Decimal


def _gross_pnl_for_lifecycle(
    *,
    side: str | None,
    fills: list[FillRecord],
    buy_yes_price: Decimal | None,
    fallback_count: Decimal | None,
    settlement_value: Decimal | None,
) -> Decimal | None:
    if side not in {"yes", "no"}:
        return None
    lots: list[_LifecycleLot] = []
    gross = Decimal("0")
    saw_buy = False
    matched_exit = False

    for fill in sorted(fills, key=lambda item: (_as_utc(item.created_at) or datetime.min.replace(tzinfo=UTC), item.id)):
        if fill.side != side:
            continue
        price = _side_cost_from_yes_price(side, fill.yes_price_dollars)
        if price is None:
            continue
        count = Decimal(fill.count_fp)
        if fill.action == "buy":
            saw_buy = True
            lots.append(_LifecycleLot(count=count, price=price))
            continue
        if fill.action != "sell":
            continue
        remaining = count
        for lot in lots:
            if remaining <= 0:
                break
            if lot.count <= 0:
                continue
            matched = min(lot.count, remaining)
            gross += (price - lot.price) * matched
            lot.count -= matched
            remaining -= matched
            matched_exit = True

    unsettled_count = sum((lot.count for lot in lots if lot.count > 0), Decimal("0"))
    if unsettled_count and settlement_value is not None:
        yes_value = Decimal(settlement_value)
        payout = yes_value if side == "yes" else Decimal("1") - yes_value
        for lot in lots:
            if lot.count <= 0:
                continue
            gross += (payout - lot.price) * lot.count

    if saw_buy and (matched_exit or settlement_value is not None):
        return gross

    return _gross_pnl_for_side(
        side=side,
        count=fallback_count,
        buy_yes_price=buy_yes_price,
        avg_fill_yes_price=None,
        settlement_value=settlement_value,
    )


@dataclass(slots=True)
class TradeAnalysisDataset:
    rows: list[dict[str, Any]]
    summary: dict[str, Any]


class TradeAnalysisService:
    """Read-only decision dataset and model-selection analysis.

    The service materializes one row per decision room with as-of market/weather
    evidence. It never writes database state and never calls the exchange.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        weather_directory: WeatherMarketDirectory,
        trading_audit_service: Any | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.trading_audit_service = trading_audit_service

    async def build_dataset(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 180,
        now: datetime | None = None,
        limit: int | None = None,
    ) -> TradeAnalysisDataset:
        now = _as_utc(now) or _utc_now()
        cutoff = now - timedelta(days=days)
        async with self.session_factory() as session:
            rooms = await self._rooms(session, kalshi_env=kalshi_env, cutoff=cutoff, limit=limit)
            room_ids = [room.id for room in rooms]
            signals = await self._latest_by_room(
                session,
                Signal,
                room_ids,
                order_fields=(Signal.created_at, Signal.id),
            )
            tickets = await self._latest_by_room(
                session,
                TradeTicketRecord,
                room_ids,
                order_fields=(TradeTicketRecord.created_at, TradeTicketRecord.id),
            )
            ticket_ids = [ticket.id for ticket in tickets.values()]
            risks = await self._latest_risk_by_ticket(session, ticket_ids)
            orders = await self._orders(session, kalshi_env=kalshi_env, cutoff=cutoff - timedelta(days=2))
            fills = await self._fills(session, kalshi_env=kalshi_env, cutoff=cutoff - timedelta(days=2))
            settlements = await self._settlements(session)
            replay_by_room = await self._replay_by_room(session, room_ids)

            rows: list[dict[str, Any]] = []
            orders_by_ticket: dict[str, list[OrderRecord]] = {}
            orders_by_client: dict[str, list[OrderRecord]] = {}
            for order in orders:
                if order.trade_ticket_id:
                    orders_by_ticket.setdefault(str(order.trade_ticket_id), []).append(order)
                orders_by_client.setdefault(order.client_order_id, []).append(order)
            fills_by_order: dict[str, list[FillRecord]] = {}
            for fill in fills:
                if fill.order_id:
                    fills_by_order.setdefault(str(fill.order_id), []).append(fill)

            for room in rooms:
                signal = signals.get(room.id)
                if signal is None:
                    continue
                ticket = tickets.get(room.id)
                risk = risks.get(ticket.id) if ticket is not None else None
                related_orders = self._related_orders(ticket, orders_by_ticket, orders_by_client)
                related_orders = self._related_lifecycle_orders(ticket, related_orders, orders)
                related_fills = [
                    fill
                    for order in related_orders
                    for fill in fills_by_order.get(str(order.id), [])
                ]
                decision_ts = _as_utc(ticket.created_at if ticket is not None else signal.created_at) or _as_utc(room.created_at) or now
                market_snapshot = await self._market_snapshot(
                    session,
                    kalshi_env,
                    room.market_ticker,
                    decision_ts,
                    room_id=room.id,
                )
                weather_snapshot = await self._weather_snapshot(session, room.market_ticker, decision_ts)
                settlement = settlements.get(room.market_ticker)
                row = self._row(
                    room=room,
                    signal=signal,
                    ticket=ticket,
                    risk=risk,
                    orders=related_orders,
                    fills=related_fills,
                    market_snapshot=market_snapshot,
                    weather_snapshot=weather_snapshot,
                    settlement=settlement,
                    replay=replay_by_room.get(room.id),
                    decision_ts=decision_ts,
                    now=now,
                )
                rows.append(row)
        return TradeAnalysisDataset(rows=rows, summary=self._summary(rows, kalshi_env=kalshi_env, days=days, now=now))

    async def build_report(
        self,
        *,
        kalshi_env: str = "production",
        days: int = 180,
        now: datetime | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        dataset = await self.build_dataset(kalshi_env=kalshi_env, days=days, now=now, limit=limit)
        blockers = await self._promotion_blockers(kalshi_env=kalshi_env)
        return {
            **dataset.summary,
            "promotion_blockers": blockers,
            "top_exclusion_reasons": Counter(
                reason
                for row in dataset.rows
                for reason in row.get("exclusion_reasons", [])
            ).most_common(20),
            "top_exclusion_reasons_by_series": [
                {"series_ticker": series, "reason": reason, "rows": count}
                for (series, reason), count in Counter(
                    (row.get("series_ticker") or "<unknown>", reason)
                    for row in dataset.rows
                    for reason in row.get("exclusion_reasons", [])
                ).most_common(20)
            ],
            "pending_settlement_count": sum(
                1
                for row in dataset.rows
                if any(reason in PENDING_EXCLUSION_REASONS for reason in row.get("exclusion_reasons", []))
            ),
            "data_defect_count": sum(
                1
                for row in dataset.rows
                if any(reason not in PENDING_EXCLUSION_REASONS for reason in row.get("exclusion_reasons", []))
            ),
            "stale_market_snapshot_diagnostics": self._stale_market_snapshot_diagnostics(dataset.rows),
            "by_decision_status": dict(Counter(row.get("decision_status") for row in dataset.rows)),
            "by_series": dict(Counter(row.get("series_ticker") or "<unknown>" for row in dataset.rows)),
            "pnl": self._pnl_summary(dataset.rows),
            "read_only": True,
        }

    async def write_dataset(
        self,
        *,
        output: Path,
        kalshi_env: str = "production",
        days: int = 180,
        now: datetime | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        dataset = await self.build_dataset(kalshi_env=kalshi_env, days=days, now=now, limit=limit)
        actual_output, fmt = self._write_rows(output, dataset.rows)
        return {
            **dataset.summary,
            "output": str(actual_output),
            "requested_output": str(output),
            "format": fmt,
            "read_only": True,
        }

    async def model_eval(
        self,
        *,
        dataset_path: Path,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = _as_utc(now) or _utc_now()
        rows = self._read_rows(dataset_path)
        eligible = [row for row in rows if self._model_eligible(row)]
        eligible.sort(key=lambda row: (str(row.get("decision_ts") or ""), str(row.get("room_id") or "")))
        warnings: list[dict[str, Any]] = []
        if rows:
            envs = {str(row.get("kalshi_env") or "") for row in rows if row.get("kalshi_env")}
            if len(envs) == 1:
                warnings.extend(await self._promotion_blockers(kalshi_env=next(iter(envs))))

        split = self._chronological_split(eligible)
        train: list[dict[str, Any]] = []
        test: list[dict[str, Any]] = []
        metrics: dict[str, Any]
        if split is None:
            metrics = {"status": "insufficient_data", "reason": "need_at_least_20_eligible_rows_and_both_labels"}
        else:
            train, test = split
            model = self._fit_logistic(train)
            metrics = self._evaluate_model(model, train, test)

        return {
            "model_card_version": MODEL_CARD_VERSION,
            "generated_at": now.isoformat(),
            "dataset": {
                "path": str(dataset_path),
                "rows": len(rows),
                "eligible_rows": len(eligible),
                "excluded_rows": len(rows) - len(eligible),
                "schema_version": rows[0].get("schema_version") if rows else SCHEMA_VERSION,
            },
            "feature_diagnostics": self._feature_diagnostics(rows, eligible, train=train, test=test),
            "warnings": warnings,
            "promotion_blocked": any(w.get("severity") in {"critical", "high"} for w in warnings),
            "metrics": metrics,
            "read_only": True,
        }

    async def summary_for_auto_evolve(self, *, kalshi_env: str, days: int) -> dict[str, Any]:
        report = await self.build_report(kalshi_env=kalshi_env, days=days)
        return {
            "available": True,
            "schema_version": SCHEMA_VERSION,
            "window_days": days,
            "row_count": report["row_count"],
            "training_eligible_count": report["training_eligible_count"],
            "excluded_count": report["excluded_count"],
            "top_exclusion_reasons": report["top_exclusion_reasons"][:8],
            "pnl": report["pnl"],
            "promotion_blocked": bool(report["promotion_blockers"]),
            "promotion_blockers": report["promotion_blockers"][:10],
        }

    async def _rooms(
        self,
        session: AsyncSession,
        *,
        kalshi_env: str,
        cutoff: datetime,
        limit: int | None,
    ) -> list[Room]:
        stmt = (
            select(Room)
            .where(Room.kalshi_env == kalshi_env, Room.created_at >= cutoff)
            .order_by(Room.created_at.asc(), Room.id.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return list((await session.execute(stmt)).scalars())

    async def _latest_by_room(
        self,
        session: AsyncSession,
        model: Any,
        room_ids: list[str],
        *,
        order_fields: tuple[Any, Any],
    ) -> dict[str, Any]:
        if not room_ids:
            return {}
        records: list[Any] = []
        for batch in _batches(room_ids):
            result = await session.execute(select(model).where(model.room_id.in_(batch)))
            records.extend(result.scalars())
        records.sort(key=lambda r: (r.room_id, order_fields[0].__get__(r, model), order_fields[1].__get__(r, model)))
        latest: dict[str, Any] = {}
        for record in records:
            latest[record.room_id] = record
        return latest

    async def _latest_risk_by_ticket(self, session: AsyncSession, ticket_ids: list[str]) -> dict[str, RiskVerdictRecord]:
        if not ticket_ids:
            return {}
        records: list[RiskVerdictRecord] = []
        for batch in _batches(ticket_ids):
            result = await session.execute(select(RiskVerdictRecord).where(RiskVerdictRecord.ticket_id.in_(batch)))
            records.extend(result.scalars())
        records.sort(key=lambda r: (r.ticket_id, r.created_at, r.id))
        latest: dict[str, RiskVerdictRecord] = {}
        for record in records:
            latest[record.ticket_id] = record
        return latest

    async def _orders(
        self,
        session: AsyncSession,
        *,
        kalshi_env: str,
        cutoff: datetime,
    ) -> list[OrderRecord]:
        return list(
            (
                await session.execute(
                    select(OrderRecord)
                    .where(OrderRecord.kalshi_env == kalshi_env, OrderRecord.created_at >= cutoff)
                    .order_by(OrderRecord.created_at.asc(), OrderRecord.id.asc())
                )
            ).scalars()
        )

    async def _fills(
        self,
        session: AsyncSession,
        *,
        kalshi_env: str,
        cutoff: datetime,
    ) -> list[FillRecord]:
        return list(
            (
                await session.execute(
                    select(FillRecord)
                    .where(FillRecord.kalshi_env == kalshi_env, FillRecord.created_at >= cutoff)
                    .order_by(FillRecord.created_at.asc(), FillRecord.id.asc())
                )
            ).scalars()
        )

    async def _settlements(self, session: AsyncSession) -> dict[str, HistoricalSettlementLabelRecord]:
        rows = list((await session.execute(select(HistoricalSettlementLabelRecord))).scalars())
        return {row.market_ticker: row for row in rows}

    async def _replay_by_room(self, session: AsyncSession, room_ids: list[str]) -> dict[str, HistoricalReplayRunRecord]:
        if not room_ids:
            return {}
        rows: list[HistoricalReplayRunRecord] = []
        for batch in _batches(room_ids):
            result = await session.execute(select(HistoricalReplayRunRecord).where(HistoricalReplayRunRecord.room_id.in_(batch)))
            rows.extend(result.scalars())
        return {str(row.room_id): row for row in rows if row.room_id}

    async def _market_snapshot(
        self,
        session: AsyncSession,
        kalshi_env: str,
        market_ticker: str,
        decision_ts: datetime,
        *,
        room_id: str | None = None,
    ) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        if room_id is not None:
            artifact = (
                await session.execute(
                    select(Artifact)
                    .where(
                        Artifact.room_id == room_id,
                        Artifact.artifact_type == "market_snapshot",
                    )
                    .order_by(Artifact.updated_at.desc(), Artifact.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if artifact is not None:
                artifact_snapshot = self._market_snapshot_from_artifact(artifact)
                artifact_observed_at = (
                    _as_utc(artifact_snapshot.get("observed_at"))
                    if artifact_snapshot is not None
                    else None
                )
                if artifact_snapshot is not None and artifact_observed_at is not None and artifact_observed_at <= decision_ts:
                    return artifact_snapshot

        history = (
            await session.execute(
                select(MarketPriceHistory)
                .where(
                    MarketPriceHistory.kalshi_env == kalshi_env,
                    MarketPriceHistory.market_ticker == market_ticker,
                    MarketPriceHistory.observed_at <= decision_ts,
                )
                .order_by(MarketPriceHistory.observed_at.desc(), MarketPriceHistory.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if history is not None:
            candidates.append({
                "source": "market_price_history",
                "source_kind": "market_price_history",
                "snapshot_id": history.id,
                "observed_at": history.observed_at,
                "yes_bid_dollars": history.yes_bid_dollars,
                "yes_ask_dollars": history.yes_ask_dollars,
                "mid_dollars": history.mid_dollars,
                "last_trade_dollars": history.last_trade_dollars,
                "volume": history.volume,
            })
        state = (
            await session.execute(
                select(MarketState).where(
                    MarketState.kalshi_env == kalshi_env,
                    MarketState.market_ticker == market_ticker,
                    MarketState.observed_at <= decision_ts,
                )
            )
        ).scalar_one_or_none()
        if state is not None:
            mid = None
            if state.yes_bid_dollars is not None and state.yes_ask_dollars is not None:
                mid = (state.yes_bid_dollars + state.yes_ask_dollars) / Decimal("2")
            candidates.append({
                "source": "market_state",
                "source_kind": "market_state",
                "snapshot_id": f"{state.kalshi_env}:{state.market_ticker}",
                "observed_at": state.observed_at,
                "yes_bid_dollars": state.yes_bid_dollars,
                "yes_ask_dollars": state.yes_ask_dollars,
                "mid_dollars": mid,
                "last_trade_dollars": state.last_trade_dollars,
                "volume": None,
            })

        historical_cutoff = decision_ts - timedelta(
            seconds=float(getattr(self.settings, "historical_replay_market_stale_seconds", self.settings.risk_stale_market_seconds))
        )
        historical = (
            await session.execute(
                select(HistoricalMarketSnapshotRecord)
                .where(
                    HistoricalMarketSnapshotRecord.market_ticker == market_ticker,
                    HistoricalMarketSnapshotRecord.asof_ts <= decision_ts,
                    HistoricalMarketSnapshotRecord.asof_ts >= historical_cutoff,
                    HistoricalMarketSnapshotRecord.source_kind.in_(POINT_IN_TIME_HISTORICAL_MARKET_SOURCES),
                )
                .order_by(HistoricalMarketSnapshotRecord.asof_ts.desc(), HistoricalMarketSnapshotRecord.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if historical is not None:
            mid = None
            if historical.yes_bid_dollars is not None and historical.yes_ask_dollars is not None:
                mid = (historical.yes_bid_dollars + historical.yes_ask_dollars) / Decimal("2")
            candidates.append({
                "source": "historical_market_snapshots",
                "source_kind": historical.source_kind,
                "source_id": historical.source_id,
                "snapshot_id": historical.id,
                "observed_at": historical.asof_ts,
                "yes_bid_dollars": historical.yes_bid_dollars,
                "yes_ask_dollars": historical.yes_ask_dollars,
                "mid_dollars": mid,
                "last_trade_dollars": historical.last_price_dollars,
                "volume": None,
            })

        if not candidates:
            return None

        source_rank = {
            "room_market_snapshot": 4,
            "market_state": 3,
            "market_price_history": 2,
            "historical_market_snapshots": 1,
        }
        return max(
            candidates,
            key=lambda row: (
                _as_utc(row.get("observed_at")) or datetime.min.replace(tzinfo=UTC),
                source_rank.get(str(row.get("source")), 0),
            ),
        )

    @staticmethod
    def _market_snapshot_from_artifact(artifact: Artifact) -> dict[str, Any] | None:
        payload = artifact.payload if isinstance(artifact.payload, dict) else {}
        market = payload.get("market") if isinstance(payload.get("market"), dict) else payload
        if not isinstance(market, dict):
            return None
        meta = payload.get("_snapshot_meta") if isinstance(payload.get("_snapshot_meta"), dict) else {}
        observed_at = (
            _as_utc(market.get("observed_at"))
            or _as_utc(payload.get("observed_at"))
            or _as_utc(meta.get("observed_at"))
            or _as_utc(artifact.created_at)
        )
        bid = _decimal_or_none(market.get("yes_bid_dollars"))
        ask = _decimal_or_none(market.get("yes_ask_dollars"))
        mid = (bid + ask) / Decimal("2") if bid is not None and ask is not None else None
        return {
            "source": "room_market_snapshot",
            "source_kind": artifact.source,
            "source_id": artifact.id,
            "snapshot_id": artifact.id,
            "observed_at": observed_at,
            "yes_bid_dollars": bid,
            "yes_ask_dollars": ask,
            "mid_dollars": mid,
            "last_trade_dollars": _decimal_or_none(market.get("last_price_dollars")),
            "volume": _int_or_none(market.get("volume")),
        }

    async def _weather_snapshot(
        self,
        session: AsyncSession,
        market_ticker: str,
        decision_ts: datetime,
    ) -> dict[str, Any] | None:
        mapping = self.weather_directory.resolve_market_stub(market_ticker)
        station_id = mapping.station_id if mapping is not None else None
        if station_id is None:
            return None
        row = (
            await session.execute(
                select(HistoricalWeatherSnapshotRecord)
                .where(
                    HistoricalWeatherSnapshotRecord.station_id == station_id,
                    HistoricalWeatherSnapshotRecord.asof_ts <= decision_ts,
                )
                .order_by(HistoricalWeatherSnapshotRecord.asof_ts.desc(), HistoricalWeatherSnapshotRecord.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        return {
            "source": "historical_weather_snapshots",
            "snapshot_id": row.id,
            "observed_at": row.asof_ts,
            "station_id": row.station_id,
            "forecast_updated_ts": row.forecast_updated_ts,
            "observation_ts": row.observation_ts,
            "forecast_high_f": row.forecast_high_f,
            "current_temp_f": row.current_temp_f,
        }

    def _related_orders(
        self,
        ticket: TradeTicketRecord | None,
        orders_by_ticket: dict[str, list[OrderRecord]],
        orders_by_client: dict[str, list[OrderRecord]],
    ) -> list[OrderRecord]:
        if ticket is None:
            return []
        seen: set[str] = set()
        out: list[OrderRecord] = []
        for order in orders_by_ticket.get(ticket.id, []) + orders_by_client.get(ticket.client_order_id, []):
            if order.id in seen:
                continue
            seen.add(order.id)
            out.append(order)
        out.sort(key=lambda order: (order.created_at, order.id))
        return out

    def _related_lifecycle_orders(
        self,
        ticket: TradeTicketRecord | None,
        base_orders: list[OrderRecord],
        all_orders: list[OrderRecord],
    ) -> list[OrderRecord]:
        if ticket is None:
            return base_orders
        seen = {order.id for order in base_orders}
        out = list(base_orders)
        side = ticket.side if ticket.side in {"yes", "no"} else None
        strategy_code = ticket.strategy_code or next((order.strategy_code for order in base_orders if order.strategy_code), None)
        start_at = _as_utc(ticket.created_at)
        if side is None or start_at is None:
            return out

        for order in all_orders:
            if order.id in seen:
                continue
            if order.market_ticker != ticket.market_ticker:
                continue
            if order.side != side or order.action != "sell":
                continue
            if _as_utc(order.created_at) is None or _as_utc(order.created_at) < start_at:
                continue
            if order.trade_ticket_id and order.trade_ticket_id != ticket.id:
                continue
            if strategy_code is not None and order.strategy_code not in {strategy_code, None}:
                continue
            seen.add(order.id)
            out.append(order)

        out.sort(key=lambda order: (order.created_at, order.id))
        return out

    def _row(
        self,
        *,
        room: Room,
        signal: Signal,
        ticket: TradeTicketRecord | None,
        risk: RiskVerdictRecord | None,
        orders: list[OrderRecord],
        fills: list[FillRecord],
        market_snapshot: dict[str, Any] | None,
        weather_snapshot: dict[str, Any] | None,
        settlement: HistoricalSettlementLabelRecord | None,
        replay: HistoricalReplayRunRecord | None,
        decision_ts: datetime,
        now: datetime,
    ) -> dict[str, Any]:
        mapping = self.weather_directory.resolve_market_stub(room.market_ticker)
        signal_payload = signal.payload or {}
        eligibility = signal_payload.get("eligibility") or {}
        strategy_code = (
            ticket.strategy_code
            if ticket is not None and ticket.strategy_code is not None
            else next((order.strategy_code for order in orders if order.strategy_code), None)
        )
        order_statuses = [order.status for order in orders]
        ticket_price = Decimal(ticket.yes_price_dollars) if ticket is not None else None
        ticket_count = Decimal(ticket.count_fp) if ticket is not None else None
        settlement_value = settlement.settlement_value_dollars if settlement is not None else None
        kalshi_result = settlement.kalshi_result if settlement is not None else None
        side = ticket.side if ticket is not None else None
        entry_fills = [fill for fill in fills if fill.action == "buy" and (side is None or fill.side == side)]
        fill_count = sum(Decimal(fill.count_fp) for fill in entry_fills) if entry_fills else Decimal("0")
        fill_notional = sum((Decimal(fill.yes_price_dollars) * Decimal(fill.count_fp)) for fill in entry_fills) if entry_fills else Decimal("0")
        avg_fill_yes = (fill_notional / fill_count) if fill_count else None
        label_win = _label_win_for_side(side, kalshi_result, settlement_value)
        gross_pnl = _gross_pnl_for_lifecycle(
            side=side,
            buy_yes_price=ticket_price,
            fallback_count=fill_count if fill_count else ticket_count,
            fills=fills,
            settlement_value=settlement_value,
        )
        market_observed_at = _as_utc(market_snapshot.get("observed_at")) if market_snapshot else None
        weather_observed_at = _as_utc(weather_snapshot.get("observed_at")) if weather_snapshot else None
        stale_market_seconds = (
            (decision_ts - market_observed_at).total_seconds()
            if market_observed_at is not None
            else None
        )
        stale_weather_seconds = (
            (decision_ts - weather_observed_at).total_seconds()
            if weather_observed_at is not None
            else None
        )
        market_stale_threshold_seconds = float(self.settings.risk_stale_market_seconds)
        replay_stale_threshold_seconds = float(getattr(self.settings, "historical_replay_market_stale_seconds", market_stale_threshold_seconds))
        market_stale_overage_seconds = (
            stale_market_seconds - market_stale_threshold_seconds
            if stale_market_seconds is not None and stale_market_seconds > market_stale_threshold_seconds
            else None
        )
        yes_bid = market_snapshot.get("yes_bid_dollars") if market_snapshot else None
        yes_ask = market_snapshot.get("yes_ask_dollars") if market_snapshot else None
        spread = (Decimal(yes_ask) - Decimal(yes_bid)) if yes_bid is not None and yes_ask is not None else None
        fair = Decimal(signal.fair_yes_dollars)
        side_cost = _side_cost_from_yes_price(side, ticket_price)
        model_edge = (Decimal("1") - fair if side == "no" else fair) - side_cost if side_cost is not None else None
        decision_status = self._decision_status(ticket=ticket, risk=risk, orders=orders, fills=fills)
        exclusion_reasons = self._exclusion_reasons(
            market_ticker=room.market_ticker,
            market_snapshot=market_snapshot,
            weather_snapshot=weather_snapshot,
            settlement=settlement,
            side=side,
            ticket_price=ticket_price,
            stale_market_seconds=stale_market_seconds,
            stale_weather_seconds=stale_weather_seconds,
            strategy_code=strategy_code,
            decision_status=decision_status,
            now=now,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "kalshi_env": room.kalshi_env,
            "room_id": room.id,
            "market_ticker": room.market_ticker,
            "series_ticker": getattr(mapping, "series_ticker", None) or getattr(replay, "series_ticker", None) or _series_from_ticker(room.market_ticker),
            "station_id": getattr(mapping, "station_id", None) or (weather_snapshot or {}).get("station_id"),
            "market_day": getattr(replay, "local_market_day", None) or _market_day_from_ticker(room.market_ticker),
            "threshold_f": getattr(mapping, "threshold_f", None),
            "operator": getattr(mapping, "operator", None),
            "decision_ts": decision_ts.isoformat(),
            "room_created_at": _iso(room.created_at),
            "agent_pack_version": room.agent_pack_version,
            "room_origin": room.room_origin,
            "shadow_mode": room.shadow_mode,
            "strategy_code": strategy_code,
            "signal_id": signal.id,
            "fair_yes_dollars": _decimal_str(signal.fair_yes_dollars),
            "edge_bps": signal.edge_bps,
            "confidence": signal.confidence,
            "signal_summary": signal.summary,
            "signal_trade_regime": signal_payload.get("trade_regime"),
            "signal_candidate_outcome": (signal_payload.get("trade_selection") or {}).get("evaluation_outcome"),
            "eligibility_market_spread_bps": _int_or_none(eligibility.get("market_spread_bps")),
            "eligibility_remaining_payout_dollars": eligibility.get("remaining_payout_dollars"),
            "ticket_id": ticket.id if ticket is not None else None,
            "action": ticket.action if ticket is not None else None,
            "side": side,
            "ticket_status": ticket.status if ticket is not None else None,
            "ticket_yes_price_dollars": _decimal_str(ticket_price),
            "ticket_count_fp": _decimal_str(ticket_count),
            "time_in_force": ticket.time_in_force if ticket is not None else None,
            "risk_status": risk.status if risk is not None else None,
            "risk_reasons": risk.reasons if risk is not None else [],
            "approved_notional_dollars": _decimal_str(risk.approved_notional_dollars) if risk is not None else None,
            "decision_status": decision_status,
            "order_count": len(orders),
            "order_statuses": order_statuses,
            "filled_contracts": _decimal_str(fill_count),
            "avg_fill_yes_price_dollars": _decimal_str(avg_fill_yes),
            "fee_dollars": _decimal_str(self._fee_dollars(fills)),
            "slippage_dollars": _decimal_str((avg_fill_yes - ticket_price) if avg_fill_yes is not None and ticket_price is not None else None),
            "market_snapshot_source": (market_snapshot or {}).get("source"),
            "market_snapshot_source_kind": (market_snapshot or {}).get("source_kind") or (market_snapshot or {}).get("source"),
            "market_snapshot_source_id": (market_snapshot or {}).get("source_id"),
            "market_snapshot_id": (market_snapshot or {}).get("snapshot_id"),
            "market_observed_at": _iso(market_observed_at),
            "market_stale_seconds": stale_market_seconds,
            "market_stale_threshold_seconds": market_stale_threshold_seconds,
            "historical_replay_market_stale_threshold_seconds": replay_stale_threshold_seconds,
            "market_stale_overage_seconds": market_stale_overage_seconds,
            "market_snapshot_age_bucket": _stale_age_bucket(stale_market_seconds),
            "yes_bid_dollars": _decimal_str(yes_bid),
            "yes_ask_dollars": _decimal_str(yes_ask),
            "mid_dollars": _decimal_str((market_snapshot or {}).get("mid_dollars")),
            "spread_dollars": _decimal_str(spread),
            "last_trade_dollars": _decimal_str((market_snapshot or {}).get("last_trade_dollars")),
            "volume": (market_snapshot or {}).get("volume"),
            "weather_snapshot_source": (weather_snapshot or {}).get("source"),
            "weather_snapshot_id": (weather_snapshot or {}).get("snapshot_id"),
            "weather_observed_at": _iso(weather_observed_at),
            "weather_stale_seconds": stale_weather_seconds,
            "forecast_updated_ts": _iso((weather_snapshot or {}).get("forecast_updated_ts")),
            "observation_ts": _iso((weather_snapshot or {}).get("observation_ts")),
            "forecast_high_f": _decimal_str((weather_snapshot or {}).get("forecast_high_f")),
            "current_temp_f": _decimal_str((weather_snapshot or {}).get("current_temp_f")),
            "forecast_residual_f": self._forecast_residual(weather_snapshot, mapping),
            "kalshi_result": kalshi_result,
            "settlement_value_dollars": _decimal_str(settlement_value),
            "settlement_ts": _iso(settlement.settlement_ts) if settlement is not None else None,
            "label_win": label_win,
            "gross_pnl_dollars": _decimal_str(gross_pnl.quantize(Decimal("0.0001"))) if gross_pnl is not None else None,
            "model_edge_dollars": _decimal_str(model_edge.quantize(Decimal("0.0001"))) if model_edge is not None else None,
            "training_eligible": not any(reason in TRAINING_EXCLUDED_REASONS for reason in exclusion_reasons),
            "exclusion_reasons": exclusion_reasons,
            "generated_at": now.isoformat(),
        }

    def _decision_status(
        self,
        *,
        ticket: TradeTicketRecord | None,
        risk: RiskVerdictRecord | None,
        orders: list[OrderRecord],
        fills: list[FillRecord],
    ) -> str:
        if fills:
            return "filled"
        if orders:
            failed = {"failed", "rejected", "rejected_503", "order_id_missing", "lock_denied", "write_credentials_missing"}
            if any(str(order.status).lower() in failed for order in orders):
                return "order_failed"
            return "ordered_unfilled"
        if risk is not None and risk.status == "approved":
            return "approved_no_order"
        if risk is not None:
            return f"risk_{risk.status}"
        if ticket is not None:
            return f"ticket_{ticket.status}"
        return "signal_only"

    def _exclusion_reasons(
        self,
        *,
        market_ticker: str,
        market_snapshot: dict[str, Any] | None,
        weather_snapshot: dict[str, Any] | None,
        settlement: HistoricalSettlementLabelRecord | None,
        side: str | None,
        ticket_price: Decimal | None,
        stale_market_seconds: float | None,
        stale_weather_seconds: float | None,
        strategy_code: str | None,
        decision_status: str,
        now: datetime,
    ) -> list[str]:
        reasons: list[str] = []
        if market_snapshot is None:
            reasons.append("missing_market_snapshot")
        elif stale_market_seconds is not None and stale_market_seconds > self.settings.risk_stale_market_seconds:
            reasons.append("stale_market_snapshot")
        if weather_snapshot is None:
            reasons.append("missing_weather_snapshot")
        elif stale_weather_seconds is not None and stale_weather_seconds > self.settings.risk_stale_weather_seconds:
            reasons.append("stale_weather_snapshot")
        if settlement is None or (settlement.settlement_value_dollars is None and settlement.kalshi_result is None):
            reason = self._settlement_missing_reason(market_ticker=market_ticker, now=now)
            reasons.append(reason)
        expects_execution_fields = decision_status != "signal_only"
        if expects_execution_fields and side not in {"yes", "no"}:
            reasons.append("missing_side")
        if expects_execution_fields and ticket_price is None:
            reasons.append("missing_ticket_price")
        if expects_execution_fields and strategy_code is None:
            reasons.append("missing_strategy_attribution")
        return reasons

    @staticmethod
    def _settlement_missing_reason(*, market_ticker: str, now: datetime) -> str:
        market_date = _market_date_from_ticker(market_ticker)
        if market_date is None:
            return "missing_settlement_label"
        now_utc = _as_utc(now) or _utc_now()
        if market_date.date() >= (now_utc.date() - timedelta(days=2)):
            return "pending_settlement"
        return "missing_settlement_label"

    def _forecast_residual(self, weather_snapshot: dict[str, Any] | None, mapping: Any | None) -> float | None:
        if weather_snapshot is None or mapping is None or getattr(mapping, "threshold_f", None) is None:
            return None
        forecast = _float_or_none(weather_snapshot.get("forecast_high_f"))
        if forecast is None:
            return None
        return forecast - float(mapping.threshold_f)

    def _fee_dollars(self, fills: list[FillRecord]) -> Decimal | None:
        total = Decimal("0")
        found = False
        for fill in fills:
            raw = fill.raw or {}
            if not isinstance(raw, dict):
                continue
            value = raw.get("fee_cost") or raw.get("fee_dollars") or raw.get("fee")
            if value in (None, ""):
                continue
            try:
                total += Decimal(str(value))
                found = True
            except Exception:
                continue
        return total if found else None

    def _summary(self, rows: list[dict[str, Any]], *, kalshi_env: str, days: int, now: datetime) -> dict[str, Any]:
        eligible = [row for row in rows if row.get("training_eligible")]
        return {
            "schema_version": SCHEMA_VERSION,
            "kalshi_env": kalshi_env,
            "window_days": days,
            "window_end": now.isoformat(),
            "row_count": len(rows),
            "training_eligible_count": len(eligible),
            "excluded_count": len(rows) - len(eligible),
            "read_only": True,
        }

    def _pnl_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        values = [Decimal(str(row["gross_pnl_dollars"])) for row in rows if row.get("gross_pnl_dollars") is not None]
        total = sum(values, Decimal("0"))
        return {
            "scored_count": len(values),
            "gross_pnl_dollars": str(total.quantize(Decimal("0.0001"))),
            "avg_pnl_dollars": str((total / Decimal(len(values))).quantize(Decimal("0.0001"))) if values else None,
        }

    def _stale_market_snapshot_diagnostics(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        stale_rows = [row for row in rows if "stale_market_snapshot" in row.get("exclusion_reasons", [])]
        ages = sorted(
            value
            for row in stale_rows
            if (value := _float_or_none(row.get("market_stale_seconds"))) is not None
        )
        p95 = ages[min(len(ages) - 1, math.ceil(len(ages) * 0.95) - 1)] if ages else None
        return {
            "row_count": len(stale_rows),
            "max_market_stale_seconds": round(max(ages), 3) if ages else None,
            "p95_market_stale_seconds": round(p95, 3) if p95 is not None else None,
            "by_source": _counter_rows(stale_rows, "market_snapshot_source"),
            "by_source_kind": _counter_rows(stale_rows, "market_snapshot_source_kind"),
            "by_series": _counter_rows(stale_rows, "series_ticker"),
            "by_station_id": _counter_rows(stale_rows, "station_id"),
            "by_market_day": _counter_rows(stale_rows, "market_day"),
            "by_age_bucket": [
                {"value": bucket, "rows": count}
                for bucket, count in Counter(row.get("market_snapshot_age_bucket") or "missing" for row in stale_rows).most_common()
            ],
        }

    async def _promotion_blockers(self, *, kalshi_env: str) -> list[dict[str, Any]]:
        if self.trading_audit_service is None:
            return []
        try:
            report = await self.trading_audit_service.build_report(kalshi_env=kalshi_env, days=7, focus="money-safety")
        except Exception as exc:
            return [{"severity": "high", "code": "trading_audit_unavailable", "summary": str(exc)}]
        return [
            {
                "severity": issue.get("severity"),
                "code": issue.get("code"),
                "summary": issue.get("summary"),
            }
            for issue in report.get("issues", [])
            if str(issue.get("severity") or "").lower() in {"critical", "high"}
        ]

    def _write_rows(self, output: Path, rows: list[dict[str, Any]]) -> tuple[Path, str]:
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.suffix.lower() == ".parquet":
            try:
                import pyarrow as pa  # type: ignore
                import pyarrow.parquet as pq  # type: ignore

                pq.write_table(pa.Table.from_pylist(rows), output)
                return output, "parquet"
            except Exception:
                output = output.with_suffix(".jsonl")
        if output.suffix.lower() == ".csv":
            fieldnames = sorted({key for row in rows for key in row})
            with output.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: self._csv_value(row.get(key)) for key in fieldnames})
            return output, "csv"
        with output.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=str, sort_keys=True))
                handle.write("\n")
        return output, "jsonl"

    def _read_rows(self, path: Path) -> list[dict[str, Any]]:
        if path.suffix.lower() == ".parquet":
            try:
                import pyarrow.parquet as pq  # type: ignore

                return [dict(row) for row in pq.read_table(path).to_pylist()]
            except Exception as exc:
                raise ValueError(f"Unable to read parquet dataset {path}: {exc}") from exc
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8", newline="") as handle:
                return [dict(row) for row in csv.DictReader(handle)]
        with path.open("r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def _csv_value(self, value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True)
        return value

    def _model_eligible(self, row: dict[str, Any]) -> bool:
        if not row.get("training_eligible"):
            return False
        if row.get("label_win") in (None, ""):
            return False
        return all(self._feature(row, key) is not None for key in MODEL_REQUIRED_FEATURES)

    def _feature_diagnostics(
        self,
        rows: list[dict[str, Any]],
        eligible: list[dict[str, Any]],
        *,
        train: list[dict[str, Any]] | None = None,
        test: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        feature_names = self._feature_names()
        train = train or []
        test = test or []

        def counts_for(sample: list[dict[str, Any]]) -> dict[str, dict[str, float | int | None]]:
            result: dict[str, dict[str, float | int | None]] = {}
            for feature in feature_names:
                missing_count = sum(1 for row in sample if self._feature(row, feature) is None)
                result[feature] = {
                    "present_count": len(sample) - missing_count,
                    "missing_count": missing_count,
                    "missing_rate": round(missing_count / len(sample), 6) if sample else None,
                    "imputation_value": 0.0 if feature in MODEL_OPTIONAL_IMPUTED_FEATURES else None,
                }
            return result

        return {
            "required_features": sorted(MODEL_REQUIRED_FEATURES),
            "optional_imputed_features": sorted(MODEL_OPTIONAL_IMPUTED_FEATURES),
            "all_rows": counts_for(rows),
            "model_eligible_rows": counts_for(eligible),
            "train_rows": counts_for(train),
            "test_rows": counts_for(test),
        }

    def _feature_names(self) -> list[str]:
        return [
            "edge_bps",
            "confidence",
            "ticket_yes_price_dollars",
            "spread_dollars",
            "market_stale_seconds",
            "weather_stale_seconds",
            "forecast_residual_f",
        ]

    def _feature(self, row: dict[str, Any], key: str) -> float | None:
        return _float_or_none(row.get(key))

    def _features(self, row: dict[str, Any]) -> list[float]:
        values = [self._feature(row, key) or 0.0 for key in self._feature_names()]
        return [1.0, *values]

    def _label(self, row: dict[str, Any]) -> int:
        value = row.get("label_win")
        if isinstance(value, str):
            return 1 if value.lower() == "true" else 0
        return 1 if value else 0

    def _chronological_split(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
        if len(rows) < 20:
            return None
        labels = {self._label(row) for row in rows}
        if len(labels) < 2:
            return None
        split_idx = max(1, int(len(rows) * 0.7))
        train = rows[:split_idx]
        test = rows[split_idx:]
        if not test or len({self._label(row) for row in train}) < 2:
            return None
        return train, test

    def _fit_logistic(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        feature_count = len(self._feature_names()) + 1
        means = [0.0] * feature_count
        stds = [1.0] * feature_count
        matrix = [self._features(row) for row in rows]
        for idx in range(1, feature_count):
            vals = [x[idx] for x in matrix]
            means[idx] = sum(vals) / len(vals)
            var = sum((v - means[idx]) ** 2 for v in vals) / max(1, len(vals) - 1)
            stds[idx] = math.sqrt(var) or 1.0
        scaled = [[1.0, *[(x[idx] - means[idx]) / stds[idx] for idx in range(1, feature_count)]] for x in matrix]
        weights = [0.0] * feature_count
        lr = 0.05
        reg = 0.01
        for _ in range(600):
            grads = [0.0] * feature_count
            for x, row in zip(scaled, rows, strict=True):
                y = self._label(row)
                p = self._sigmoid(sum(w * v for w, v in zip(weights, x, strict=True)))
                for idx, value in enumerate(x):
                    grads[idx] += (p - y) * value
            for idx in range(feature_count):
                penalty = reg * weights[idx] if idx else 0.0
                weights[idx] -= lr * ((grads[idx] / len(rows)) + penalty)
        return {"weights": weights, "means": means, "stds": stds, "feature_names": ["intercept", *self._feature_names()]}

    def _predict(self, model: dict[str, Any], row: dict[str, Any]) -> float:
        raw = self._features(row)
        means = model["means"]
        stds = model["stds"]
        x = [1.0, *[(raw[idx] - means[idx]) / stds[idx] for idx in range(1, len(raw))]]
        return self._sigmoid(sum(w * v for w, v in zip(model["weights"], x, strict=True)))

    def _sigmoid(self, value: float) -> float:
        if value >= 35:
            return 1.0
        if value <= -35:
            return 0.0
        return 1.0 / (1.0 + math.exp(-value))

    def _evaluate_model(
        self,
        model: dict[str, Any],
        train: list[dict[str, Any]],
        test: list[dict[str, Any]],
    ) -> dict[str, Any]:
        predictions = [(self._predict(model, row), self._label(row), row) for row in test]
        brier = sum((p - y) ** 2 for p, y, _ in predictions) / len(predictions)
        eps = 1e-9
        log_loss = -sum(y * math.log(max(eps, p)) + (1 - y) * math.log(max(eps, 1 - p)) for p, y, _ in predictions) / len(predictions)
        picked = [(p, y, row) for p, y, row in predictions if p >= 0.5]
        pnl_values = [float(row.get("gross_pnl_dollars") or 0.0) for _, _, row in picked]
        picked_losers = [(p, y, row) for p, y, row in picked if float(row.get("gross_pnl_dollars") or 0.0) < 0]
        picked_losses = [float(row.get("gross_pnl_dollars") or 0.0) for _, _, row in picked_losers]
        picked_wins = [value for value in pnl_values if value > 0]
        worst_picked_rows = sorted(
            picked,
            key=lambda item: (float(item[2].get("gross_pnl_dollars") or 0.0), str(item[2].get("decision_ts") or "")),
        )[:10]
        return {
            "status": "ok",
            "model_type": "builtin_logistic",
            "features": model["feature_names"],
            "train_rows": len(train),
            "test_rows": len(test),
            "train_window": {"start": train[0].get("decision_ts"), "end": train[-1].get("decision_ts")},
            "test_window": {"start": test[0].get("decision_ts"), "end": test[-1].get("decision_ts")},
            "brier": round(brier, 6),
            "log_loss": round(log_loss, 6),
            "coverage": round(len(picked) / len(test), 6),
            "win_rate_at_50pct": round((sum(y for _, y, _ in picked) / len(picked)), 6) if picked else None,
            "expected_value_mean": round(sum((p - 0.5) for p, _, _ in predictions) / len(predictions), 6),
            "fill_adjusted_pnl_dollars": round(sum(pnl_values), 4),
            "max_drawdown_dollars": round(self._max_drawdown(pnl_values), 4),
            "picked_trade_diagnostics": {
                "picked_count": len(picked),
                "picked_winning_rows": len(picked_wins),
                "picked_losing_rows": len(picked_losses),
                "avg_picked_pnl_dollars": round(sum(pnl_values) / len(pnl_values), 4) if pnl_values else None,
                "worst_picked_pnl_dollars": round(min(pnl_values), 4) if pnl_values else None,
                "best_picked_pnl_dollars": round(max(pnl_values), 4) if pnl_values else None,
                "picked_feature_missingness": self._prediction_row_missingness(picked),
                "picked_loser_feature_missingness": self._prediction_row_missingness(picked_losers),
                "worst_picked_rows": [
                    self._picked_row_diagnostic(prediction=prediction, label=label, row=row)
                    for prediction, label, row in worst_picked_rows
                ],
                "by_series": [
                    {
                        "series_ticker": series,
                        "picked_count": len(values),
                        "pnl_dollars": round(sum(values), 4),
                    }
                    for series, values in sorted(
                        _group_pnl_by_series(picked).items(),
                        key=lambda item: (sum(item[1]), item[0]),
                    )
                ],
            },
            "calibration": self._calibration(predictions),
        }

    def _prediction_row_missingness(self, rows: list[tuple[float, int, dict[str, Any]]]) -> dict[str, int]:
        return {
            feature: sum(1 for _prediction, _label, row in rows if self._feature(row, feature) is None)
            for feature in self._feature_names()
        }

    def _picked_row_diagnostic(self, *, prediction: float, label: int, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "room_id": row.get("room_id"),
            "market_ticker": row.get("market_ticker"),
            "series_ticker": row.get("series_ticker") or "<unknown>",
            "decision_ts": row.get("decision_ts"),
            "prediction": round(prediction, 6),
            "label_win": bool(label),
            "gross_pnl_dollars": _float_or_none(row.get("gross_pnl_dollars")),
            "side": row.get("side"),
            "ticket_yes_price_dollars": _float_or_none(row.get("ticket_yes_price_dollars")),
            "forecast_residual_f": _float_or_none(row.get("forecast_residual_f")),
            "forecast_residual_f_imputed": self._feature(row, "forecast_residual_f") is None,
        }

    def _max_drawdown(self, values: list[float]) -> float:
        peak = 0.0
        equity = 0.0
        max_dd = 0.0
        for value in values:
            equity += value
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
        return max_dd

    def _calibration(self, predictions: list[tuple[float, int, dict[str, Any]]]) -> list[dict[str, Any]]:
        buckets: dict[str, list[tuple[float, int]]] = {}
        for p, y, _ in predictions:
            lo = min(9, int(p * 10)) / 10
            key = f"{lo:.1f}-{lo + 0.1:.1f}"
            buckets.setdefault(key, []).append((p, y))
        return [
            {
                "bucket": key,
                "count": len(values),
                "avg_prediction": round(sum(p for p, _ in values) / len(values), 6),
                "observed_rate": round(sum(y for _, y in values) / len(values), 6),
            }
            for key, values in sorted(buckets.items())
        ]


def format_trade_analysis_report(report: dict[str, Any]) -> str:
    lines = [
        "Trade Analysis Report",
        f"env={report['kalshi_env']} window={report['window_days']}d rows={report['row_count']}",
        "",
        f"Training eligible: {report['training_eligible_count']}  Excluded: {report['excluded_count']}",
        f"P&L rows: {report['pnl']['scored_count']}  Gross P&L: {report['pnl']['gross_pnl_dollars']}",
        "",
        "Top exclusion reasons:",
    ]
    for reason, count in report.get("top_exclusion_reasons", [])[:10]:
        lines.append(f"- {count}: {reason}")
    if report.get("promotion_blockers"):
        lines.extend(["", "Promotion blockers:"])
        for issue in report["promotion_blockers"][:10]:
            lines.append(f"- {str(issue.get('severity')).upper()} {issue.get('code')}: {issue.get('summary')}")
    return "\n".join(lines)
