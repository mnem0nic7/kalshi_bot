from __future__ import annotations

import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from sqlalchemy import Select, and_, bindparam, case, func, or_, select, update as sql_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer
from sqlalchemy.orm.attributes import set_committed_value

from kalshi_bot.config import get_settings
from kalshi_bot.core.enums import RiskStatus, RoomOrigin, RoomStage
from kalshi_bot.core.fixed_point import as_decimal
from kalshi_bot.core.signal_payload import capital_bucket_from_signal_payload as _capital_bucket_from_signal_payload
from kalshi_bot.core.schemas import (
    MemoryNotePayload,
    PortfolioBucketSnapshot,
    ResearchClaim,
    ResearchDossier,
    ResearchSourceCard,
    RoomCreate,
    RoomMessageCreate,
    TradeTicket,
)
from kalshi_bot.db.deployment_control_repository import DeploymentControlRepositoryMixin
from kalshi_bot.db.learning_repository import LearningRepositoryMixin
from kalshi_bot.db.strategy_repository import StrategyRepositoryMixin
from kalshi_bot.db.web_auth_repository import WebAuthRepositoryMixin
from kalshi_bot.db.models import (
    Artifact,
    Checkpoint,
    ClimatologyPriorRecord,
    CryptoDataQualityRunRecord,
    CryptoDecisionOutcomeRecord,
    CryptoExecutionExampleRecord,
    CryptoMarketCandlestickRecord,
    CryptoMarketSnapshotRecord,
    CryptoFundingRateRecord,
    CryptoModelArtifactRecord,
    CryptoOrderBookSnapshotRecord,
    CryptoSettlementBenchmarkWindowRecord,
    CryptoSpotOHLCRecord,
    CryptoTradeTickRecord,
    CryptoTrainingFeatureRowRecord,
    DecisionTraceRecord,
    FillRecord,
    ForecastSnapshotRecord,
    MarketPriceHistory,
    MarketState,
    MemoryEmbedding,
    MemoryNoteRecord,
    OpsEvent,
    OrderRecord,
    PositionRecord,
    ResearchClaimRecord,
    ResearchDossierRecord,
    ResearchRunRecord,
    ResearchSourceRecord,
    RawExchangeEvent,
    RawWeatherEvent,
    RiskVerdictRecord,
    Room,
    RoomCampaignRecord,
    RoomMessage,
    RoomResearchHealthRecord,
    RoomStrategyAuditRecord,
    Signal,
    SourceHealthLogRecord,
    TradeTicketRecord,
    WeatherBootstrapEventRecord,
    WeatherBootstrapHistoricalEvidenceRecord,
)
from kalshi_bot.services.fee_model import FeeEstimate, extract_kalshi_raw_fee_dollars

_GENERIC_CRYPTO_STRATEGY_CODES = {"CRYPTO_15M", "CRYPTO_1H"}
_CLIENT_ORDER_STRATEGY_PREFIXES = {
    "b15t20r:": "btc15m_touch20_rules",
    "eth15t20r:": "eth15m_touch20_rules",
    "sol15t20r:": "sol15m_touch20_rules",
    "xrp15t20r:": "xrp15m_touch20_rules",
    "bnb15t20r:": "bnb15m_touch20_rules",
    "doge15t20r:": "doge15m_touch20_rules",
    "hype15t20r:": "hype15m_touch20_rules",
    "btc1ht20r:": "btc1h_touch20_rules",
    "eth1ht20r:": "eth1h_touch20_rules",
    "sol1ht20r:": "sol1h_touch20_rules",
    "xrp1ht20r:": "xrp1h_touch20_rules",
    "bnb1ht20r:": "bnb1h_touch20_rules",
    "doge1ht20r:": "doge1h_touch20_rules",
    "hype1ht20r:": "hype1h_touch20_rules",
}


def _crypto_entry_quote_sql(prefix: str = "") -> str:
    field = f"{prefix}." if prefix else ""
    return f"""
    (
        (
            {field}yes_bid_dollars > 0
            AND {field}yes_bid_dollars < 1
            AND {field}yes_ask_dollars > 0
            AND {field}yes_ask_dollars < 1
            AND {field}yes_ask_dollars >= {field}yes_bid_dollars
        )
        OR (
            {field}no_bid_dollars > 0
            AND {field}no_bid_dollars < 1
            AND {field}no_ask_dollars > 0
            AND {field}no_ask_dollars < 1
            AND {field}no_ask_dollars >= {field}no_bid_dollars
        )
    )
    """


def _strategy_code_for_client_order_prefix(client_order_id: str | None) -> str | None:
    if not client_order_id:
        return None
    for prefix, strategy_code in _CLIENT_ORDER_STRATEGY_PREFIXES.items():
        if client_order_id.startswith(prefix):
            return strategy_code
    return None


def _quantize_money(value: Any) -> Decimal:
    return as_decimal(value).quantize(Decimal("0.0001"))


def _decimal_from_first_key(payload: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        raw = payload.get(key)
        if raw is None:
            continue
        try:
            return Decimal(str(raw))
        except (ArithmeticError, ValueError):
            continue
    return None


def _total_capital_dollars_from_balance_payload(balance_payload: dict[str, Any]) -> Decimal | None:
    cash_cents = _decimal_from_first_key(balance_payload, ("balance", "cash_balance", "cash"))
    portfolio_cents = _decimal_from_first_key(
        balance_payload,
        ("portfolio_value", "portfolioValue", "portfolio"),
    )
    if cash_cents is None and portfolio_cents is None:
        return None
    if cash_cents is None:
        return portfolio_cents / Decimal("100") if portfolio_cents is not None else None
    if portfolio_cents is None:
        return cash_cents / Decimal("100")
    # Kalshi payloads in this codebase have represented portfolio_value two ways:
    # when below cash, as positions market value; when above cash, as total equity.
    total_cents = portfolio_cents if portfolio_cents >= cash_cents else cash_cents + portfolio_cents
    return total_cents / Decimal("100")


_PENDING_BUY_ORDER_STATUSES = {"resting", "submitted", "accepted", "open", "pending"}
# asyncpg rejects any query with more than 32767 bind parameters. ``id.in_(ids)``
# expands to one bind param per id, so the second-stage snapshot lookup must be
# chunked well under that ceiling once the distinct-market count grows large.
_CRYPTO_ID_IN_CHUNK_SIZE = 30000
_CRYPTO_MARKET_TICKER_RE = re.compile(r"^KX[A-Z0-9]+(?:15M|1H)-")
_CRYPTO_MARKET_ID_RE = re.compile(r"^KX([A-Z0-9]+)(15M|1H)-")


def _crypto_frequency_duration_bounds(frequency: str | None) -> tuple[int, int] | None:
    frequency_key = str(frequency or "").strip().lower()
    return {"1h": (3000, 4200), "hourly": (3000, 4200)}.get(frequency_key)


def _crypto_snapshot_matches_frequency_duration(row: Any, frequency: str | None) -> bool:
    duration_bounds = _crypto_frequency_duration_bounds(frequency)
    if duration_bounds is None:
        return True
    open_time = getattr(row, "open_time", None)
    close_time = getattr(row, "close_time", None)
    if open_time is None or close_time is None:
        return True
    seconds = (close_time - open_time).total_seconds()
    return duration_bounds[0] <= seconds <= duration_bounds[1]


def _crypto_snapshot_duration_condition(frequency: str | None) -> Any | None:
    duration_bounds = _crypto_frequency_duration_bounds(frequency)
    if duration_bounds is None:
        return None
    duration_seconds = func.extract(
        "epoch",
        CryptoMarketSnapshotRecord.close_time - CryptoMarketSnapshotRecord.open_time,
    )
    return or_(
        CryptoMarketSnapshotRecord.open_time.is_(None),
        CryptoMarketSnapshotRecord.close_time.is_(None),
        duration_seconds.between(*duration_bounds),
    )


def _is_crypto_market_ticker(market_ticker: str | None) -> bool:
    return bool(_CRYPTO_MARKET_TICKER_RE.match(str(market_ticker or "").upper()))


def _crypto_market_identity(market_ticker: str | None) -> tuple[str | None, str | None]:
    match = _CRYPTO_MARKET_ID_RE.match(str(market_ticker or "").upper())
    if match is None:
        return None, None
    return match.group(1), "15m" if match.group(2) == "15M" else "1h"


def _crypto_price_band(price: Decimal) -> str:
    if price < Decimal("0.25"):
        return "0.00-0.25"
    if price < Decimal("0.50"):
        return "0.25-0.50"
    if price < Decimal("0.75"):
        return "0.50-0.75"
    return "0.75-1.00"


def _as_utc_datetime(value: datetime | None, default: datetime | None = None) -> datetime:
    resolved = value or default or datetime.min.replace(tzinfo=UTC)
    if resolved.tzinfo is None:
        return resolved.replace(tzinfo=UTC)
    return resolved.astimezone(UTC)


def _fill_created_at(fill: FillRecord, default: datetime | None = None) -> datetime:
    return _as_utc_datetime(fill.created_at, default=default)


def _contract_price_from_yes_price(side: str | None, yes_price_dollars: Decimal) -> Decimal:
    yes_price = _quantize_money(yes_price_dollars)
    return yes_price if str(side or "").lower() == "yes" else Decimal("1.0000") - yes_price


def _fill_fee_dollars_from_raw(raw: Any) -> Decimal:
    return _quantize_money(extract_kalshi_raw_fee_dollars(raw).amount_dollars)


def _fill_fee_estimate_from_raw(raw: Any, *, is_taker: bool | None = None) -> FeeEstimate:
    role = None if is_taker is None else ("taker" if is_taker else "maker")
    return extract_kalshi_raw_fee_dollars(raw, role=role)


def _fill_fee_for_count_from_raw(raw: Any, total_count_fp: Any, count_fp: Decimal) -> Decimal:
    total_count = as_decimal(total_count_fp)
    if total_count <= Decimal("0") or count_fp <= Decimal("0"):
        return Decimal("0")
    return _fill_fee_dollars_from_raw(raw) * count_fp / total_count


def _fill_fee_for_count(fill: FillRecord, count_fp: Decimal) -> Decimal:
    return _fill_fee_for_count_from_raw(fill.raw, fill.count_fp, count_fp)


def _raw_subaccount(raw: Any) -> int | None:
    if not isinstance(raw, dict):
        return None
    candidates: list[Any] = [
        raw.get("subaccount"),
        raw.get("sub_account"),
    ]
    for key in ("order", "payload", "request_payload"):
        nested = raw.get(key)
        if isinstance(nested, dict):
            candidates.extend([nested.get("subaccount"), nested.get("sub_account")])
    for value in candidates:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _order_matches_subaccount(order: OrderRecord, subaccount: int | None) -> bool:
    if subaccount is None:
        return True
    raw_subaccount = _raw_subaccount(order.raw)
    if raw_subaccount is None:
        return int(subaccount) == 0
    return raw_subaccount == int(subaccount)


def _order_notional_dollars(order: OrderRecord) -> Decimal:
    contract_price = _contract_price_from_yes_price(order.side, order.yes_price_dollars)
    return _quantize_money(contract_price * as_decimal(order.count_fp))


def _fill_fee_dollars(fill: FillRecord) -> Decimal:
    return _fill_fee_dollars_from_raw(fill.raw)


def _gross_pnl_for_settled_buy(
    *,
    side: str | None,
    action: str | None,
    yes_price_dollars: Any,
    count_fp: Any,
    settlement_result: str | None,
) -> Decimal | None:
    if action != "buy" or settlement_result not in {"win", "loss"}:
        return None
    contract_price = _contract_price_from_yes_price(side, as_decimal(yes_price_dollars))
    count = as_decimal(count_fp)
    won = settlement_result == "win"
    return ((Decimal("1.0000") if won else Decimal("0")) - contract_price) * count


def _settled_buy_fill_economics_from_values(
    *,
    side: str | None,
    action: str | None,
    yes_price_dollars: Any,
    count_fp: Any,
    settlement_result: str | None,
    raw: Any,
    is_taker: bool | None = None,
) -> dict[str, Decimal | str | bool] | None:
    gross = _gross_pnl_for_settled_buy(
        side=side,
        action=action,
        yes_price_dollars=yes_price_dollars,
        count_fp=count_fp,
        settlement_result=settlement_result,
    )
    if gross is None:
        return None
    fee = _fill_fee_estimate_from_raw(raw, is_taker=is_taker)
    fees = _quantize_money(fee.amount_dollars)
    return {
        "gross_pnl_dollars": _quantize_money(gross),
        "fees_dollars": fees,
        "net_pnl_dollars": _quantize_money(gross - fees),
        "fee_source": fee.fee_source,
        "fee_missing": fee.missing,
        "fee_estimated": fee.estimated,
    }


def _candidate_trade_ticket_client_order_ids(client_order_id: str) -> list[str]:
    candidates: list[str] = []

    def add(value: str) -> None:
        if value and value not in candidates:
            candidates.append(value)

    raw = str(client_order_id).strip()
    add(raw)
    quote_base, sep, quote_num = raw.rpartition("_q")
    if sep and quote_num.isdigit():
        add(quote_base)
    else:
        quote_base = raw
    for base in list(candidates):
        for suffix in (":maker", ":taker"):
            if base.endswith(suffix):
                add(base[: -len(suffix)])
    return candidates


def _settled_buy_fill_pnl_from_values(
    *,
    side: str | None,
    action: str | None,
    yes_price_dollars: Any,
    count_fp: Any,
    settlement_result: str | None,
    raw: Any,
    is_taker: bool | None = None,
) -> Decimal | None:
    economics = _settled_buy_fill_economics_from_values(
        side=side,
        action=action,
        yes_price_dollars=yes_price_dollars,
        count_fp=count_fp,
        settlement_result=settlement_result,
        raw=raw,
        is_taker=is_taker,
    )
    if economics is None:
        return None
    return economics["net_pnl_dollars"]  # type: ignore[return-value]


def _settled_buy_fill_pnl(fill: FillRecord) -> Decimal | None:
    return _settled_buy_fill_pnl_from_values(
        side=fill.side,
        action=fill.action,
        yes_price_dollars=fill.yes_price_dollars,
        count_fp=fill.count_fp,
        settlement_result=fill.settlement_result,
        raw=fill.raw,
        is_taker=fill.is_taker,
    )


def _raw_has_decision_lineage(raw: Any) -> bool:
    payload = raw if isinstance(raw, dict) else {}
    lineage = payload.get("decision_lineage")
    if not isinstance(lineage, dict):
        return False
    required_groups = (
        ("decision_edge_bps", "edge_bps", "expected_net_edge_bps"),
        ("decision_fair_yes", "fair_yes_dollars"),
        ("decision_price", "target_yes_price_dollars", "selected_price_dollars"),
        ("decision_time", "signal_created_at"),
    )
    return all(any(lineage.get(key) not in (None, "") for key in keys) for keys in required_groups)


def _decision_time_from_raw(raw: Any) -> datetime | None:
    payload = raw if isinstance(raw, dict) else {}
    lineage = payload.get("decision_lineage")
    if not isinstance(lineage, dict):
        return None
    value = lineage.get("decision_time") or lineage.get("signal_created_at")
    if isinstance(value, datetime):
        return _as_utc_datetime(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return _as_utc_datetime(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _fill_latency_bucket(latency_seconds: float | None) -> str:
    if latency_seconds is None:
        return "unknown"
    if latency_seconds <= 5:
        return "0-5s"
    if latency_seconds <= 30:
        return "5-30s"
    if latency_seconds <= 120:
        return "30-120s"
    if latency_seconds <= 300:
        return "120-300s"
    return "300s+"


def _snapshot_side_bid_dollars(snapshot: dict[str, Any], side: str) -> Decimal | None:
    normalized = str(side or "").lower()
    if normalized == "yes":
        value = snapshot.get("yes_bid_dollars")
        if value is not None:
            return _quantize_money(value)
        opposite_ask = snapshot.get("no_ask_dollars")
        return _quantize_money(Decimal("1.0000") - as_decimal(opposite_ask)) if opposite_ask is not None else None
    if normalized == "no":
        value = snapshot.get("no_bid_dollars")
        if value is not None:
            return _quantize_money(value)
        opposite_ask = snapshot.get("yes_ask_dollars")
        return _quantize_money(Decimal("1.0000") - as_decimal(opposite_ask)) if opposite_ask is not None else None
    return None


class PlatformRepository(DeploymentControlRepositoryMixin, WebAuthRepositoryMixin, StrategyRepositoryMixin, LearningRepositoryMixin):
    def __init__(self, session: AsyncSession, *, kalshi_env: str | None = None) -> None:
        self.session = session
        self.kalshi_env = kalshi_env if kalshi_env is not None else get_settings().kalshi_env

    def _resolved_kalshi_env(self, kalshi_env: str | None = None) -> str:
        env = (kalshi_env or self.kalshi_env or "demo").strip()
        return env or "demo"

    async def _apply_crypto_snapshot_index_query_guards(self, *, disable_bitmapscan: bool = True) -> None:
        from sqlalchemy import text as sql_text

        await self.session.execute(sql_text("SET LOCAL statement_timeout = '45s'"))
        await self.session.execute(sql_text("SET LOCAL enable_seqscan = off"))
        if disable_bitmapscan:
            await self.session.execute(sql_text("SET LOCAL enable_bitmapscan = off"))

    def _env_stream_name(self, prefix: str, *, kalshi_env: str | None = None, suffix: str | None = None) -> str:
        parts = [prefix, self._resolved_kalshi_env(kalshi_env)]
        if suffix:
            parts.append(suffix)
        return ":".join(parts)

    @staticmethod
    def _fill_pnl_metrics(all_fills: list[FillRecord]) -> dict[str, Any]:
        """Summarize buy-fill trade P&L from a bounded fill set."""
        buys: dict[tuple[str, str], list[FillRecord]] = {}
        sells: dict[tuple[str, str], list[FillRecord]] = {}
        for fill in all_fills:
            key = (fill.market_ticker, fill.side)
            if fill.action == "buy":
                buys.setdefault(key, []).append(fill)
            elif fill.action == "sell":
                sells.setdefault(key, []).append(fill)

        def _fill_time(fill: FillRecord) -> datetime:
            value = fill.created_at
            if value is None:
                return datetime.min.replace(tzinfo=UTC)
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)

        won = 0.0
        total = 0.0
        scored_contracts = 0.0
        unresolved_contracts = 0.0
        unresolved_trade_count = 0
        trade_pnls_with_time: list[tuple[datetime, float]] = []
        for key, buy_fills in buys.items():
            _ticker, side = key
            sell_fills = sells.get(key, [])
            avg_sell: float | None = None
            if sell_fills:
                sell_count = sum(float(s.count_fp) for s in sell_fills)
                if sell_count > 0:
                    avg_sell = (
                        sum(float(s.yes_price_dollars) * float(s.count_fp) for s in sell_fills)
                        / sell_count
                    )
            for buy_fill in buy_fills:
                count = float(buy_fill.count_fp)
                buy_px = float(buy_fill.yes_price_dollars)
                total += count
                pnl: float | None = None
                profitable = False
                if avg_sell is not None:
                    if side == "yes":
                        pnl = (avg_sell - buy_px) * count
                    else:
                        pnl = (buy_px - avg_sell) * count
                    profitable = pnl > 0
                elif buy_fill.settlement_result is not None:
                    won_leg = buy_fill.settlement_result == "win"
                    if side == "yes":
                        pnl = ((1.0 if won_leg else 0.0) - buy_px) * count
                    else:
                        pnl = ((1.0 if won_leg else 0.0) - (1.0 - buy_px)) * count
                    profitable = won_leg
                if profitable:
                    won += count
                if pnl is None:
                    unresolved_trade_count += 1
                    unresolved_contracts += count
                else:
                    scored_contracts += count
                    trade_pnls_with_time.append((_fill_time(buy_fill), pnl))

        ordered_pnls = [
            pnl for _created_at, pnl in sorted(trade_pnls_with_time, key=lambda item: item[0])
        ]
        trade_count = len(ordered_pnls)
        wins_pnl = [p for p in ordered_pnls if p > 0]
        losses_pnl = [p for p in ordered_pnls if p < 0]
        total_pnl = sum(ordered_pnls)
        avg_win_dollars = (sum(wins_pnl) / len(wins_pnl)) if wins_pnl else None
        avg_loss_dollars = (sum(losses_pnl) / len(losses_pnl)) if losses_pnl else None
        mean_return_dollars = (total_pnl / trade_count) if trade_count else None

        stdev_dollars: float | None = None
        sharpe_per_trade: float | None = None
        if trade_count >= 2 and mean_return_dollars is not None:
            variance = sum((p - mean_return_dollars) ** 2 for p in ordered_pnls) / trade_count
            stdev = variance ** 0.5
            stdev_dollars = stdev
            if stdev > 0:
                sharpe_per_trade = mean_return_dollars / stdev

        running_pnl = 0.0
        peak_pnl = 0.0
        max_drawdown = 0.0
        for pnl in ordered_pnls:
            running_pnl += pnl
            peak_pnl = max(peak_pnl, running_pnl)
            max_drawdown = max(max_drawdown, peak_pnl - running_pnl)

        return {
            "won_contracts": won,
            "total_contracts": total,
            "scored_contracts": scored_contracts,
            "unresolved_contracts": unresolved_contracts,
            "trade_count": trade_count,
            "win_count": len(wins_pnl),
            "loss_count": len(losses_pnl),
            "unresolved_trade_count": unresolved_trade_count,
            "avg_win_dollars": avg_win_dollars,
            "avg_loss_dollars": avg_loss_dollars,
            "total_pnl_dollars": total_pnl,
            "mean_return_dollars": mean_return_dollars,
            "max_drawdown_dollars": max_drawdown if trade_count else None,
            "stdev_dollars": stdev_dollars,
            "sharpe_per_trade": sharpe_per_trade,
        }

    async def create_room(
        self,
        room: RoomCreate,
        *,
        active_color: str,
        shadow_mode: bool,
        kill_switch_enabled: bool,
        kalshi_env: str,
        room_origin: str | None = None,
        agent_pack_version: str | None = None,
        evaluation_run_id: str | None = None,
        role_models: dict[str, Any] | None = None,
    ) -> Room:
        record = Room(
            name=room.name,
            market_ticker=room.market_ticker,
            room_origin=room_origin or (RoomOrigin.SHADOW.value if shadow_mode else RoomOrigin.LIVE.value),
            prompt=room.prompt,
            kalshi_env=kalshi_env,
            stage=RoomStage.TRIGGERED.value,
            active_color=active_color,
            shadow_mode=shadow_mode,
            kill_switch_enabled=kill_switch_enabled,
            agent_pack_version=agent_pack_version,
            evaluation_run_id=evaluation_run_id,
            role_models=role_models or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def save_room_campaign(
        self,
        *,
        room_id: str,
        campaign_id: str,
        trigger_source: str,
        city_bucket: str | None = None,
        market_regime_bucket: str | None = None,
        difficulty_bucket: str | None = None,
        outcome_bucket: str | None = None,
        dossier_artifact_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RoomCampaignRecord:
        record = await self.get_room_campaign(room_id)
        if record is None:
            record = RoomCampaignRecord(
                room_id=room_id,
                campaign_id=campaign_id,
                trigger_source=trigger_source,
                city_bucket=city_bucket,
                market_regime_bucket=market_regime_bucket,
                difficulty_bucket=difficulty_bucket,
                outcome_bucket=outcome_bucket,
                dossier_artifact_id=dossier_artifact_id,
                payload=payload or {},
            )
            self.session.add(record)
        else:
            record.campaign_id = campaign_id
            record.trigger_source = trigger_source
            record.city_bucket = city_bucket
            record.market_regime_bucket = market_regime_bucket
            record.difficulty_bucket = difficulty_bucket
            record.outcome_bucket = outcome_bucket
            record.dossier_artifact_id = dossier_artifact_id
            record.payload = payload or record.payload
        await self.session.flush()
        return record

    async def update_room_campaign(
        self,
        room_id: str,
        *,
        dossier_artifact_id: str | None = None,
        payload_updates: dict[str, Any] | None = None,
    ) -> RoomCampaignRecord | None:
        record = await self.get_room_campaign(room_id)
        if record is None:
            return None
        if dossier_artifact_id is not None:
            record.dossier_artifact_id = dossier_artifact_id
        if payload_updates:
            record.payload = {**(record.payload or {}), **payload_updates}
        await self.session.flush()
        return record

    async def get_room_campaign(self, room_id: str) -> RoomCampaignRecord | None:
        stmt = select(RoomCampaignRecord).where(RoomCampaignRecord.room_id == room_id).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_room_campaigns(
        self,
        *,
        limit: int = 200,
        campaign_id: str | None = None,
    ) -> list[RoomCampaignRecord]:
        stmt = select(RoomCampaignRecord)
        if campaign_id is not None:
            stmt = stmt.where(RoomCampaignRecord.campaign_id == campaign_id)
        result = await self.session.execute(stmt.order_by(RoomCampaignRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms(
        self,
        limit: int = 25,
        *,
        origins: list[str] | None = None,
    ) -> list[Room]:
        stmt = select(Room)
        if origins:
            stmt = stmt.where(Room.room_origin.in_(origins))
        result = await self.session.execute(stmt.order_by(Room.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms_for_export(
        self,
        *,
        limit: int = 100,
        market_ticker: str | None = None,
        include_non_complete: bool = False,
        origins: list[str] | None = None,
        updated_since: datetime | None = None,
    ) -> list[Room]:
        stmt = select(Room)
        if market_ticker is not None:
            stmt = stmt.where(Room.market_ticker == market_ticker)
        if origins:
            stmt = stmt.where(Room.room_origin.in_(origins))
        if updated_since is not None:
            stmt = stmt.where(Room.updated_at >= updated_since)
        if not include_non_complete:
            stmt = stmt.where(Room.stage == RoomStage.COMPLETE.value)
        result = await self.session.execute(stmt.order_by(Room.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_rooms_for_learning(
        self,
        *,
        since: datetime,
        limit: int = 500,
        pack_version: str | None = None,
        color: str | None = None,
        market_ticker: str | None = None,
        origins: list[str] | None = None,
        include_frozen_production_live: bool = False,
    ) -> list[Room]:
        stmt = (
            select(Room)
            .where(
                Room.stage == RoomStage.COMPLETE.value,
                Room.created_at >= since,
            )
            .order_by(Room.created_at.asc())
        )
        if origins:
            stmt = stmt.where(Room.room_origin.in_(origins))
        else:
            learning_room_filter = (Room.shadow_mode.is_(True)) | (Room.kalshi_env != "production")
            if include_frozen_production_live:
                learning_room_filter = learning_room_filter | (
                    (Room.kalshi_env == "production")
                    & (Room.room_origin == RoomOrigin.LIVE.value)
                    & (Room.shadow_mode.is_(False))
                )
            stmt = stmt.where(Room.room_origin.in_([RoomOrigin.SHADOW.value, RoomOrigin.LIVE.value]), learning_room_filter)
        if pack_version is not None:
            stmt = stmt.where(Room.agent_pack_version == pack_version)
        if color is not None:
            stmt = stmt.where(Room.active_color == color)
        if market_ticker is not None:
            stmt = stmt.where(Room.market_ticker == market_ticker)
        result = await self.session.execute(stmt.limit(limit))
        return list(result.scalars())

    async def count_active_rooms(
        self,
        *,
        color: str | None = None,
        kalshi_env: str | None = None,
        updated_within_seconds: int | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(Room).where(
            Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value])
        )
        if color is not None:
            stmt = stmt.where(Room.active_color == color)
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        if updated_within_seconds is not None:
            cutoff = datetime.now(UTC) - timedelta(seconds=updated_within_seconds)
            stmt = stmt.where(Room.updated_at >= cutoff)
        return int((await self.session.execute(stmt)).scalar_one())

    async def list_active_rooms(
        self,
        *,
        kalshi_env: str | None = None,
        updated_within_seconds: int | None = None,
        limit: int = 20,
    ) -> list[Room]:
        stmt = select(Room).where(
            Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value])
        )
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        if updated_within_seconds is not None:
            cutoff = datetime.now(UTC) - timedelta(seconds=updated_within_seconds)
            stmt = stmt.where(Room.updated_at >= cutoff)
        stmt = stmt.order_by(Room.updated_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def get_room(self, room_id: str) -> Room | None:
        return await self.session.get(Room, room_id)

    async def delete_room(self, room_id: str) -> bool:
        room = await self.get_room(room_id)
        if room is None:
            return False
        await self.session.delete(room)
        await self.session.flush()
        return True

    async def get_latest_active_room_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> Room | None:
        stmt = (
            select(Room)
            .where(Room.market_ticker == market_ticker, Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value]))
            .order_by(Room.updated_at.desc())
            .limit(1)
        )
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_room_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> Room | None:
        stmt = select(Room).where(Room.market_ticker == market_ticker).order_by(Room.updated_at.desc()).limit(1)
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def reap_orphaned_rooms(self, *, color: str, kalshi_env: str | None = None) -> list[str]:
        """Mark all non-terminal rooms for *color* as failed. Returns IDs reaped."""
        stmt = select(Room).where(
            Room.stage.not_in([RoomStage.COMPLETE.value, RoomStage.FAILED.value]),
            Room.active_color == color,
        )
        if kalshi_env is not None:
            stmt = stmt.where(Room.kalshi_env == kalshi_env)
        rooms = list((await self.session.execute(stmt)).scalars())
        now = datetime.now(UTC)
        for room in rooms:
            room.stage = RoomStage.FAILED.value
            room.updated_at = now
        await self.session.flush()
        return [room.id for room in rooms]

    async def update_room_stage(self, room_id: str, stage: RoomStage) -> None:
        room = await self.get_room(room_id)
        if room is not None:
            room.stage = stage.value
            room.updated_at = datetime.now(UTC)
            await self.session.flush()

    async def update_room_runtime(
        self,
        room_id: str,
        *,
        agent_pack_version: str | None = None,
        evaluation_run_id: str | None = None,
        role_models: dict[str, Any] | None = None,
    ) -> Room | None:
        room = await self.get_room(room_id)
        if room is None:
            return None
        if agent_pack_version is not None:
            room.agent_pack_version = agent_pack_version
        if evaluation_run_id is not None:
            room.evaluation_run_id = evaluation_run_id
        if role_models is not None:
            room.role_models = role_models
        room.updated_at = datetime.now(UTC)
        await self.session.flush()
        return room

    async def append_message(self, room_id: str, message: RoomMessageCreate) -> RoomMessage:
        sequence_query: Select[tuple[int]] = select(func.coalesce(func.max(RoomMessage.sequence), 0) + 1).where(
            RoomMessage.room_id == room_id
        )
        next_sequence = (await self.session.execute(sequence_query)).scalar_one()
        record = RoomMessage(
            room_id=room_id,
            role=message.role.value,
            kind=message.kind.value,
            stage=message.stage.value if message.stage else None,
            sequence=next_sequence,
            content=message.content,
            payload=message.payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_messages(self, room_id: str, after_sequence: int = 0) -> list[RoomMessage]:
        stmt = (
            select(RoomMessage)
            .where(RoomMessage.room_id == room_id, RoomMessage.sequence > after_sequence)
            .order_by(RoomMessage.sequence.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars())

    async def save_artifact(
        self,
        *,
        room_id: str,
        artifact_type: str,
        source: str,
        title: str,
        payload: dict[str, Any],
        message_id: str | None = None,
        url: str | None = None,
        external_id: str | None = None,
    ) -> Artifact:
        record = Artifact(
            room_id=room_id,
            message_id=message_id,
            artifact_type=artifact_type,
            source=source,
            title=title,
            url=url,
            external_id=external_id,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_artifacts(self, *, room_id: str, artifact_type: str | None = None, limit: int = 50) -> list[Artifact]:
        stmt = select(Artifact).where(Artifact.room_id == room_id)
        if artifact_type is not None:
            stmt = stmt.where(Artifact.artifact_type == artifact_type)
        result = await self.session.execute(stmt.order_by(Artifact.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def get_latest_artifact(self, *, room_id: str, artifact_type: str) -> Artifact | None:
        stmt = (
            select(Artifact)
            .where(Artifact.room_id == room_id, Artifact.artifact_type == artifact_type)
            .order_by(Artifact.updated_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def log_exchange_event(self, stream_name: str, event_type: str, payload: dict[str, Any], market_ticker: str | None = None) -> None:
        self.session.add(
            RawExchangeEvent(stream_name=stream_name, event_type=event_type, payload=payload, market_ticker=market_ticker)
        )
        await self.session.flush()

    async def log_weather_event(self, station_id: str, event_type: str, payload: dict[str, Any]) -> None:
        self.session.add(RawWeatherEvent(station_id=station_id, event_type=event_type, payload=payload))
        await self.session.flush()

    async def upsert_market_state(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
        snapshot: dict[str, Any],
        yes_bid_dollars: Decimal | None,
        yes_ask_dollars: Decimal | None,
        last_trade_dollars: Decimal | None,
    ) -> MarketState:
        observed_at = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "kalshi_env": env,
            "market_ticker": market_ticker,
            "source": "kalshi",
            "snapshot": snapshot,
            "yes_bid_dollars": yes_bid_dollars,
            "yes_ask_dollars": yes_ask_dollars,
            "last_trade_dollars": last_trade_dollars,
            "observed_at": observed_at,
            "created_at": observed_at,
            "updated_at": observed_at,
        }
        update_values = {
            "snapshot": snapshot,
            "yes_bid_dollars": yes_bid_dollars,
            "yes_ask_dollars": yes_ask_dollars,
            "last_trade_dollars": last_trade_dollars,
            "observed_at": observed_at,
            "updated_at": observed_at,
        }
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(MarketState).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(MarketState).values(**insert_values)
        else:
            record = await self.session.get(MarketState, (env, market_ticker))
            if record is None:
                record = MarketState(kalshi_env=env, market_ticker=market_ticker, snapshot={})
                self.session.add(record)
            record.snapshot = snapshot
            record.yes_bid_dollars = yes_bid_dollars
            record.yes_ask_dollars = yes_ask_dollars
            record.last_trade_dollars = last_trade_dollars
            record.observed_at = observed_at
            await self.session.flush()
            return record

        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[MarketState.kalshi_env, MarketState.market_ticker],
                set_=update_values,
            )
        )
        await self.session.flush()
        return await self.session.get(MarketState, (env, market_ticker))

    async def get_market_state(self, market_ticker: str, *, kalshi_env: str | None = None) -> MarketState | None:
        env = self._resolved_kalshi_env(kalshi_env)
        return await self.session.get(MarketState, (env, market_ticker))

    async def list_market_states(
        self,
        market_tickers: list[str],
        *,
        kalshi_env: str | None = None,
    ) -> list[MarketState]:
        if not market_tickers:
            return []
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(MarketState).where(
            MarketState.kalshi_env == env,
            MarketState.market_ticker.in_(market_tickers),
        )
        return list((await self.session.execute(stmt)).scalars())

    async def get_latest_signal_for_room(self, room_id: str) -> Signal | None:
        stmt = select(Signal).where(Signal.room_id == room_id).order_by(Signal.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_signal_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
        before: datetime | None = None,
        max_age_seconds: int | None = None,
    ) -> Signal | None:
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(Signal)
            .join(Room, Signal.room_id == Room.id)
            .where(Signal.market_ticker == market_ticker, Room.kalshi_env == env)
        )
        if before is not None:
            stmt = stmt.where(Signal.created_at <= before)
            if max_age_seconds is not None and max_age_seconds > 0:
                stmt = stmt.where(Signal.created_at >= before - timedelta(seconds=max_age_seconds))
        stmt = stmt.order_by(Signal.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def latest_signal_payloads_for_markets(
        self,
        *,
        market_tickers: list[str],
        kalshi_env: str,
    ) -> dict[str, dict[str, Any]]:
        if not market_tickers:
            return {}
        stmt = (
            select(Signal.market_ticker, Signal.payload, Signal.fair_yes_dollars, Signal.edge_bps, Signal.confidence)
            .join(Room, Signal.room_id == Room.id)
            .where(
                Signal.market_ticker.in_(market_tickers),
                Room.kalshi_env == kalshi_env,
            )
            .order_by(Signal.market_ticker.asc(), Signal.created_at.desc())
        )
        results = await self.session.execute(stmt)
        payloads: dict[str, dict[str, Any]] = {}
        for market_ticker, payload, fair_yes_dollars, edge_bps, confidence in results:
            if market_ticker not in payloads:
                payloads[str(market_ticker)] = {
                    "fair_yes_dollars": str(fair_yes_dollars),
                    "edge_bps": edge_bps,
                    "confidence": confidence,
                    **dict(payload or {}),
                }
        return payloads

    async def save_signal(
        self,
        *,
        room_id: str,
        market_ticker: str,
        fair_yes_dollars: Decimal,
        edge_bps: int,
        confidence: float,
        summary: str,
        payload: dict[str, Any],
    ) -> Signal:
        record = Signal(
            room_id=room_id,
            market_ticker=market_ticker,
            fair_yes_dollars=fair_yes_dollars,
            edge_bps=edge_bps,
            confidence=confidence,
            summary=summary,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_market_price_snapshot(
        self,
        *,
        market_ticker: str,
        kalshi_env: str | None = None,
        yes_bid_dollars: Decimal | None,
        yes_ask_dollars: Decimal | None,
        mid_dollars: Decimal | None,
        last_trade_dollars: Decimal | None,
        volume: int | None,
        observed_at: datetime,
    ) -> MarketPriceHistory:
        record = MarketPriceHistory(
            id=str(uuid4()),
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            market_ticker=market_ticker,
            yes_bid_dollars=yes_bid_dollars,
            yes_ask_dollars=yes_ask_dollars,
            mid_dollars=mid_dollars,
            last_trade_dollars=last_trade_dollars,
            volume=volume,
            observed_at=observed_at,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def fetch_recent_prices(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
        window: timedelta,
    ) -> list[MarketPriceHistory]:
        cutoff = datetime.now(UTC) - window
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(MarketPriceHistory)
            .where(
                MarketPriceHistory.kalshi_env == env,
                MarketPriceHistory.market_ticker == market_ticker,
                MarketPriceHistory.observed_at >= cutoff,
            )
            .order_by(MarketPriceHistory.observed_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def record_crypto_market_snapshot(
        self,
        *,
        series_ticker: str,
        market_ticker: str,
        asset_symbol: str,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        event_ticker: str | None = None,
        title: str | None = None,
        status: str | None = None,
        open_time: datetime | None = None,
        close_time: datetime | None = None,
        expected_expiration_time: datetime | None = None,
        target_price_dollars: Decimal | None = None,
        yes_bid_dollars: Decimal | None = None,
        yes_ask_dollars: Decimal | None = None,
        no_bid_dollars: Decimal | None = None,
        no_ask_dollars: Decimal | None = None,
        last_price_dollars: Decimal | None = None,
        volume: int | None = None,
        open_interest: int | None = None,
        settlement_result: str | None = None,
        observed_at: datetime | None = None,
        source_kind: str = "live",
        payload: dict[str, Any] | None = None,
    ) -> CryptoMarketSnapshotRecord:
        now = datetime.now(UTC)
        observed = observed_at or now
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "series_ticker": series_ticker,
            "market_ticker": market_ticker,
            "event_ticker": event_ticker,
            "asset_symbol": asset_symbol,
            "frequency": frequency,
            "title": title,
            "status": status,
            "open_time": open_time,
            "close_time": close_time,
            "expected_expiration_time": expected_expiration_time,
            "target_price_dollars": target_price_dollars,
            "yes_bid_dollars": yes_bid_dollars,
            "yes_ask_dollars": yes_ask_dollars,
            "no_bid_dollars": no_bid_dollars,
            "no_ask_dollars": no_ask_dollars,
            "last_price_dollars": last_price_dollars,
            "volume": volume,
            "open_interest": open_interest,
            "settlement_result": settlement_result,
            "observed_at": observed,
            "source_kind": source_kind,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoMarketSnapshotRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoMarketSnapshotRecord).values(**insert_values)
        else:
            record = CryptoMarketSnapshotRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record

        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoMarketSnapshotRecord.kalshi_env,
                    CryptoMarketSnapshotRecord.market_ticker,
                    CryptoMarketSnapshotRecord.observed_at,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoMarketSnapshotRecord).where(
                    CryptoMarketSnapshotRecord.kalshi_env == env,
                    CryptoMarketSnapshotRecord.market_ticker == market_ticker,
                    CryptoMarketSnapshotRecord.observed_at == observed,
                )
            )
        ).scalar_one()
        return result

    async def bulk_record_crypto_market_snapshots(
        self, snapshots: list[dict[str, Any]], *, kalshi_env: str | None = None
    ) -> int:
        if not snapshots:
            return 0

        now = datetime.now(UTC)
        insert_values: list[dict[str, Any]] = []
        for snapshot in snapshots:
            observed = snapshot.get("observed_at") or now
            env = self._resolved_kalshi_env(snapshot.get("kalshi_env") or kalshi_env)
            insert_values.append(
                {
                    "id": str(uuid4()),
                    "kalshi_env": env,
                    "series_ticker": snapshot["series_ticker"],
                    "market_ticker": snapshot["market_ticker"],
                    "event_ticker": snapshot.get("event_ticker"),
                    "asset_symbol": snapshot["asset_symbol"],
                    "frequency": snapshot.get("frequency") or "15m",
                    "title": snapshot.get("title"),
                    "status": snapshot.get("status"),
                    "open_time": snapshot.get("open_time"),
                    "close_time": snapshot.get("close_time"),
                    "expected_expiration_time": snapshot.get("expected_expiration_time"),
                    "target_price_dollars": snapshot.get("target_price_dollars"),
                    "yes_bid_dollars": snapshot.get("yes_bid_dollars"),
                    "yes_ask_dollars": snapshot.get("yes_ask_dollars"),
                    "no_bid_dollars": snapshot.get("no_bid_dollars"),
                    "no_ask_dollars": snapshot.get("no_ask_dollars"),
                    "last_price_dollars": snapshot.get("last_price_dollars"),
                    "volume": snapshot.get("volume"),
                    "open_interest": snapshot.get("open_interest"),
                    "settlement_result": snapshot.get("settlement_result"),
                    "observed_at": observed,
                    "source_kind": snapshot.get("source_kind") or "live",
                    "payload": snapshot.get("payload") or {},
                    "created_at": now,
                    "updated_at": now,
                }
            )

        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoMarketSnapshotRecord).values(insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoMarketSnapshotRecord).values(insert_values)
        else:
            self.session.add_all(CryptoMarketSnapshotRecord(**values) for values in insert_values)
            await self.session.flush()
            return len(insert_values)

        update_values = {
            key: getattr(stmt.excluded, key)
            for key in insert_values[0]
            if key not in {"id", "created_at"}
        }
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoMarketSnapshotRecord.kalshi_env,
                    CryptoMarketSnapshotRecord.market_ticker,
                    CryptoMarketSnapshotRecord.observed_at,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        return len(insert_values)

    async def list_crypto_market_snapshots(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        status: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        market_ticker: str | None = None,
        since: datetime | None = None,
        settled_only: bool = False,
        limit: int = 1000,
        match_frequency_duration: bool = False,
    ) -> list[CryptoMarketSnapshotRecord]:
        stmt = select(CryptoMarketSnapshotRecord).where(
            CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        bind = self.session.get_bind()
        python_duration_filter = False
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if match_frequency_duration:
            duration_condition = (
                _crypto_snapshot_duration_condition(frequency)
                if bind is not None and bind.dialect.name == "postgresql"
                else None
            )
            if duration_condition is not None:
                stmt = stmt.where(duration_condition)
            elif _crypto_frequency_duration_bounds(frequency) is not None:
                python_duration_filter = True
        if status is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.status == status)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if market_ticker is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.market_ticker == market_ticker)
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        if settled_only:
            stmt = stmt.where(CryptoMarketSnapshotRecord.settlement_result.isnot(None))
        query_limit = max(limit * 10, limit) if python_duration_filter else limit
        stmt = stmt.order_by(CryptoMarketSnapshotRecord.observed_at.desc()).limit(query_limit)
        rows = list((await self.session.execute(stmt)).scalars())
        if python_duration_filter:
            rows = [row for row in rows if _crypto_snapshot_matches_frequency_duration(row, frequency)][:limit]
        return rows

    async def list_crypto_live_tickers_needing_settlement_labels(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 200000,
        match_frequency_duration: bool = False,
    ) -> list[str]:
        stmt = select(CryptoMarketSnapshotRecord.market_ticker).where(
            CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoMarketSnapshotRecord.source_kind != "settled_backfill",
            CryptoMarketSnapshotRecord.settlement_result.is_(None),
        )
        bind = self.session.get_bind()
        python_duration_filter = False
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if match_frequency_duration:
            duration_condition = (
                _crypto_snapshot_duration_condition(frequency)
                if bind is not None and bind.dialect.name == "postgresql"
                else None
            )
            if duration_condition is not None:
                stmt = stmt.where(duration_condition)
            elif _crypto_frequency_duration_bounds(frequency) is not None:
                python_duration_filter = True
                stmt = select(
                    CryptoMarketSnapshotRecord.market_ticker,
                    CryptoMarketSnapshotRecord.open_time,
                    CryptoMarketSnapshotRecord.close_time,
                ).where(
                    CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                    CryptoMarketSnapshotRecord.source_kind != "settled_backfill",
                    CryptoMarketSnapshotRecord.settlement_result.is_(None),
                )
                if frequency is not None:
                    stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        query_limit = max(limit * 10, limit) if python_duration_filter else limit
        stmt = stmt.distinct().limit(query_limit)
        rows = (await self.session.execute(stmt)).fetchall()
        tickers: list[str] = []
        seen: set[str] = set()
        for row in rows:
            if python_duration_filter and not _crypto_snapshot_matches_frequency_duration(
                SimpleNamespace(open_time=row.open_time, close_time=row.close_time),
                frequency,
            ):
                continue
            market_ticker = str(row.market_ticker or "").strip()
            if not market_ticker or market_ticker in seen:
                continue
            seen.add(market_ticker)
            tickers.append(market_ticker)
            if len(tickers) >= limit:
                break
        return tickers

    async def list_latest_crypto_market_snapshots(
        self,
        *,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        limit: int = 200,
    ) -> list[CryptoMarketSnapshotRecord]:
        rows = await self.list_crypto_market_snapshots(
            frequency=frequency,
            kalshi_env=kalshi_env,
            asset_symbols=asset_symbols,
            limit=max(limit * 6, limit),
            match_frequency_duration=True,
        )
        latest: list[CryptoMarketSnapshotRecord] = []
        seen: set[str] = set()
        for row in rows:
            if row.market_ticker in seen:
                continue
            seen.add(row.market_ticker)
            latest.append(row)
            if len(latest) >= limit:
                break
        return latest

    async def get_latest_crypto_market_snapshot(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> CryptoMarketSnapshotRecord | None:
        stmt = (
            select(CryptoMarketSnapshotRecord)
            .where(
                CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CryptoMarketSnapshotRecord.market_ticker == market_ticker,
            )
            .order_by(CryptoMarketSnapshotRecord.observed_at.desc(), CryptoMarketSnapshotRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def upsert_crypto_market_candlestick(
        self,
        *,
        series_ticker: str,
        market_ticker: str,
        asset_symbol: str,
        end_period_ts: datetime,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        period_interval: int = 1,
        open_dollars: Decimal | None = None,
        high_dollars: Decimal | None = None,
        low_dollars: Decimal | None = None,
        close_dollars: Decimal | None = None,
        volume: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> CryptoMarketCandlestickRecord:
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "series_ticker": series_ticker,
            "market_ticker": market_ticker,
            "asset_symbol": asset_symbol,
            "frequency": frequency,
            "period_interval": period_interval,
            "end_period_ts": end_period_ts,
            "open_dollars": open_dollars,
            "high_dollars": high_dollars,
            "low_dollars": low_dollars,
            "close_dollars": close_dollars,
            "volume": volume,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoMarketCandlestickRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoMarketCandlestickRecord).values(**insert_values)
        else:
            record = CryptoMarketCandlestickRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoMarketCandlestickRecord.kalshi_env,
                    CryptoMarketCandlestickRecord.market_ticker,
                    CryptoMarketCandlestickRecord.period_interval,
                    CryptoMarketCandlestickRecord.end_period_ts,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoMarketCandlestickRecord).where(
                    CryptoMarketCandlestickRecord.kalshi_env == env,
                    CryptoMarketCandlestickRecord.market_ticker == market_ticker,
                    CryptoMarketCandlestickRecord.period_interval == period_interval,
                    CryptoMarketCandlestickRecord.end_period_ts == end_period_ts,
                )
            )
        ).scalar_one()
        return result

    async def list_crypto_market_candlesticks(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        market_ticker: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> list[CryptoMarketCandlestickRecord]:
        stmt = select(CryptoMarketCandlestickRecord).where(
            CryptoMarketCandlestickRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.frequency == frequency)
        if market_ticker is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.market_ticker == market_ticker)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketCandlestickRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.end_period_ts >= since)
        stmt = stmt.order_by(CryptoMarketCandlestickRecord.end_period_ts.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_crypto_settled_market_snapshots(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 1000,
        defer_payload: bool = False,
    ) -> list[CryptoMarketSnapshotRecord]:
        """Return snapshots belonging to markets that actually settled."""
        env = self._resolved_kalshi_env(kalshi_env)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        settled_markets = select(CryptoMarketSnapshotRecord.market_ticker).where(
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.settlement_result.in_(["yes", "no"]),
        )
        if frequency is not None:
            settled_markets = settled_markets.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if symbols:
            settled_markets = settled_markets.where(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        settled_markets = settled_markets.distinct().subquery()
        stmt = select(CryptoMarketSnapshotRecord).where(
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.market_ticker.in_(select(settled_markets.c.market_ticker)),
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if symbols:
            stmt = stmt.where(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        if defer_payload:
            stmt = stmt.options(defer(CryptoMarketSnapshotRecord.payload))
        stmt = stmt.order_by(CryptoMarketSnapshotRecord.observed_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def _load_crypto_snapshots_by_ids(
        self,
        ids: list,
        *,
        defer_payload: bool = False,
    ) -> list[CryptoMarketSnapshotRecord]:
        """Load snapshot rows by primary key, chunked under asyncpg's bind-param
        ceiling. A single ``id.in_(ids)`` exceeds 32767 parameters once the
        matched-market count grows large, which crashed the nightly retrain."""
        records: list[CryptoMarketSnapshotRecord] = []
        for start in range(0, len(ids), _CRYPTO_ID_IN_CHUNK_SIZE):
            chunk = ids[start : start + _CRYPTO_ID_IN_CHUNK_SIZE]
            stmt = select(CryptoMarketSnapshotRecord).where(
                CryptoMarketSnapshotRecord.id.in_(chunk)
            )
            if defer_payload:
                stmt = stmt.options(defer(CryptoMarketSnapshotRecord.payload))
            records.extend((await self.session.execute(stmt)).scalars())
        return records

    async def list_crypto_live_quote_snapshots(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 50000,
        defer_payload: bool = False,
    ) -> list[CryptoMarketSnapshotRecord]:
        """Return the latest real-bid/ask live snapshot for each settled market.

        Uses ``DISTINCT ON (market_ticker)`` to return exactly one row per market:
        the most recent live monitoring snapshot that has real bid/ask prices AND
        ``settlement_result`` set (propagated by settlement backfill).  These rows
        receive ``quote_source='snapshot_quotes'`` in the replay and are the source
        of ``strict_trade_eligible`` decision points.

        Keeps total row count at O(markets) rather than O(markets × snapshots),
        avoiding the connection overload that returning all migrated live rows causes.
        """
        from sqlalchemy import text as sql_text

        env = self._resolved_kalshi_env(kalshi_env)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]

        where_parts = [
            "kalshi_env = :env",
            "settlement_result IN ('yes', 'no')",
            "source_kind != 'settled_backfill'",
            "yes_bid_dollars IS NOT NULL",
            "yes_ask_dollars IS NOT NULL",
        ]
        params: dict = {"env": env, "limit": limit}
        if frequency is not None:
            where_parts.append("frequency = :frequency")
            params["frequency"] = frequency
        if symbols:
            where_parts.append("asset_symbol IN :symbols")
            params["symbols"] = symbols
        if since is not None:
            where_parts.append("observed_at >= :since")
            params["since"] = since
        duration_bounds = _crypto_frequency_duration_bounds(frequency)

        bind = self.session.get_bind()
        if bind is not None and bind.dialect.name != "postgresql":
            python_duration_filter = _crypto_frequency_duration_bounds(frequency) is not None
            conditions = [
                CryptoMarketSnapshotRecord.kalshi_env == env,
                CryptoMarketSnapshotRecord.settlement_result.in_(["yes", "no"]),
                CryptoMarketSnapshotRecord.source_kind != "settled_backfill",
                CryptoMarketSnapshotRecord.yes_bid_dollars.is_not(None),
                CryptoMarketSnapshotRecord.yes_ask_dollars.is_not(None),
            ]
            if frequency is not None:
                conditions.append(CryptoMarketSnapshotRecord.frequency == frequency)
            if symbols:
                conditions.append(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
            if since is not None:
                conditions.append(CryptoMarketSnapshotRecord.observed_at >= since)
            stmt = (
                select(CryptoMarketSnapshotRecord)
                .where(*conditions)
                .order_by(CryptoMarketSnapshotRecord.market_ticker, CryptoMarketSnapshotRecord.observed_at.desc())
                .limit(max(limit * 10, limit) if python_duration_filter else limit)
            )
            if defer_payload:
                stmt = stmt.options(defer(CryptoMarketSnapshotRecord.payload))
            records = list((await self.session.execute(stmt)).scalars())
            if python_duration_filter:
                records = [record for record in records if _crypto_snapshot_matches_frequency_duration(record, frequency)]
            latest: list[CryptoMarketSnapshotRecord] = []
            seen: set[str] = set()
            for record in records:
                if record.market_ticker in seen:
                    continue
                seen.add(record.market_ticker)
                latest.append(record)
            return latest

        where_clause = " AND ".join(where_parts)
        raw = sql_text(f"""
            WITH latest AS (
                SELECT DISTINCT ON (market_ticker) market_ticker, observed_at
                FROM crypto_market_snapshots
                WHERE {where_clause}
                ORDER BY market_ticker, observed_at DESC
                LIMIT :limit
            )
            SELECT snapshot.id
            FROM latest
            JOIN crypto_market_snapshots AS snapshot
              ON snapshot.kalshi_env = :env
             AND snapshot.market_ticker = latest.market_ticker
             AND snapshot.observed_at = latest.observed_at
        """)
        if symbols:
            raw = raw.bindparams(bindparam("symbols", expanding=True))
        rows = (await self.session.execute(raw, params)).fetchall()
        if not rows:
            return []
        ids = [r[0] for r in rows]
        records = await self._load_crypto_snapshots_by_ids(ids, defer_payload=defer_payload)
        if duration_bounds is not None:
            records = [
                record for record in records if _crypto_snapshot_matches_frequency_duration(record, frequency)
            ]
        return records

    async def list_crypto_settled_live_quote_path_snapshots(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 200000,
        defer_payload: bool = False,
        include_joined_fallback: bool = True,
        entry_min_seconds_to_close: int | None = None,
        entry_min_market_age_seconds: int | None = None,
        entry_qualified_market_limit: int | None = None,
    ) -> list[CryptoMarketSnapshotRecord]:
        """Return real bid/ask quote paths for settled markets.

        Unlike ``list_crypto_live_quote_snapshots``, this preserves every real
        quote row in the replay window so touch-target scans can observe the
        intra-market path before settlement.  Rows may either have a propagated
        settlement label or join to the immutable ``settled_backfill`` row for
        the same market.
        """
        env = self._resolved_kalshi_env(kalshi_env)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        row_limit = max(1, int(limit or 200000))
        bind = self.session.get_bind()
        frequency_key = str(frequency or "").strip().lower()
        duration_bounds = {"1h": (3000, 4200), "hourly": (3000, 4200)}.get(frequency_key)

        def _matches_requested_duration(row: Any) -> bool:
            if duration_bounds is None:
                return True
            open_time = getattr(row, "open_time", None)
            close_time = getattr(row, "close_time", None)
            if open_time is None or close_time is None:
                return True
            seconds = (close_time - open_time).total_seconds()
            return duration_bounds[0] <= seconds <= duration_bounds[1]

        def _merge_quote_path_rows(direct_rows: list[Any], fallback_rows: list[Any]) -> list[Any]:
            merged: list[Any] = []
            seen: set[Any] = set()
            for row in [*direct_rows, *fallback_rows]:
                if not _matches_requested_duration(row):
                    continue
                key = getattr(row, "id", None)
                if key is None:
                    key = (
                        getattr(row, "market_ticker", None),
                        getattr(row, "observed_at", None),
                        getattr(row, "source_kind", None),
                    )
                if key in seen:
                    continue
                seen.add(key)
                merged.append(row)
            merged.sort(
                key=lambda row: (
                    -getattr(row, "observed_at", datetime.min.replace(tzinfo=UTC)).timestamp(),
                    str(getattr(row, "market_ticker", "")),
                )
            )
            return merged[:row_limit]

        direct_conditions = [
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.source_kind != "settled_backfill",
            CryptoMarketSnapshotRecord.settlement_result.in_(["yes", "no"]),
            or_(
                and_(
                    CryptoMarketSnapshotRecord.yes_bid_dollars.is_not(None),
                    CryptoMarketSnapshotRecord.yes_ask_dollars.is_not(None),
                ),
                and_(
                    CryptoMarketSnapshotRecord.no_bid_dollars.is_not(None),
                    CryptoMarketSnapshotRecord.no_ask_dollars.is_not(None),
                ),
            ),
        ]
        if frequency is not None:
            direct_conditions.append(CryptoMarketSnapshotRecord.frequency == frequency)
        if symbols:
            direct_conditions.append(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            direct_conditions.append(CryptoMarketSnapshotRecord.observed_at >= since)
        if duration_bounds is not None and bind is not None and bind.dialect.name == "postgresql":
            duration_seconds = func.extract(
                "epoch",
                CryptoMarketSnapshotRecord.close_time - CryptoMarketSnapshotRecord.open_time,
            )
            direct_conditions.append(
                or_(
                    CryptoMarketSnapshotRecord.open_time.is_(None),
                    CryptoMarketSnapshotRecord.close_time.is_(None),
                    duration_seconds.between(*duration_bounds),
                )
            )
        entry_seconds = max(0, int(entry_min_seconds_to_close or 0))
        if entry_seconds > 0 and bind is not None and bind.dialect.name == "postgresql" and include_joined_fallback:
            from sqlalchemy import text as sql_text

            entry_market_limit = max(
                1,
                min(row_limit, int(entry_qualified_market_limit or max(200, row_limit // 20))),
            )
            min_market_age_seconds = max(0, int(entry_min_market_age_seconds or 0))
            label_where = [
                "label.kalshi_env = :env",
                "label.source_kind = 'settled_backfill'",
                "label.settlement_result IN ('yes', 'no')",
            ]
            entry_where = [
                "entry.kalshi_env = :env",
                "entry.source_kind <> 'settled_backfill'",
                _crypto_entry_quote_sql("entry"),
                "(entry.status IS NULL OR entry.status IN ('open', 'active'))",
                "entry.close_time IS NOT NULL",
                "entry.observed_at <= entry.close_time - (:entry_seconds * INTERVAL '1 second')",
            ]
            snapshot_where = [
                "snapshot.kalshi_env = :env",
                "snapshot.source_kind <> 'settled_backfill'",
                _crypto_entry_quote_sql("snapshot"),
                "snapshot.market_ticker = entry_markets.market_ticker",
            ]
            params: dict[str, Any] = {
                "env": env,
                "limit": row_limit,
                "entry_seconds": entry_seconds,
                "entry_market_limit": entry_market_limit,
                "label_candidate_limit": max(entry_market_limit, min(row_limit, entry_market_limit * 5)),
            }
            if min_market_age_seconds > 0:
                entry_where.append(
                    "(entry.open_time IS NULL OR entry.observed_at >= entry.open_time + (:entry_min_market_age_seconds * INTERVAL '1 second'))"
                )
                params["entry_min_market_age_seconds"] = min_market_age_seconds
            if frequency is not None:
                label_where.append("label.frequency = :frequency")
                entry_where.append("entry.frequency = :frequency")
                snapshot_where.append("snapshot.frequency = :frequency")
                params["frequency"] = frequency
            if symbols:
                label_where.append("label.asset_symbol IN :symbols")
                entry_where.append("entry.asset_symbol IN :symbols")
                snapshot_where.append("snapshot.asset_symbol IN :symbols")
                params["symbols"] = symbols
            if since is not None:
                label_where.append("label.observed_at >= :since")
                entry_where.append("entry.observed_at >= :since")
                snapshot_where.append("snapshot.observed_at >= :since")
                params["since"] = since
            if duration_bounds is not None:
                label_where.append(
                    """
                    (
                        label.open_time IS NULL
                        OR label.close_time IS NULL
                        OR EXTRACT(EPOCH FROM (label.close_time - label.open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                entry_where.append(
                    """
                    (
                        entry.open_time IS NULL
                        OR entry.close_time IS NULL
                        OR EXTRACT(EPOCH FROM (entry.close_time - entry.open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                snapshot_where.append(
                    """
                    (
                        snapshot.open_time IS NULL
                        OR snapshot.close_time IS NULL
                        OR EXTRACT(EPOCH FROM (snapshot.close_time - snapshot.open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                params["duration_min"] = duration_bounds[0]
                params["duration_max"] = duration_bounds[1]
            raw = sql_text(
                f"""
                WITH label_candidates AS (
                    SELECT
                           label.market_ticker,
                           label.settlement_result,
                           label.observed_at
                    FROM crypto_market_snapshots AS label
                    WHERE {" AND ".join(label_where)}
                    ORDER BY label.observed_at DESC, label.market_ticker
                    LIMIT :label_candidate_limit
                ),
                labels AS (
                    SELECT DISTINCT ON (market_ticker)
                           market_ticker,
                           settlement_result
                    FROM label_candidates
                    ORDER BY
                        market_ticker,
                        observed_at DESC
                ),
                entry_rows AS (
                    SELECT entry.market_ticker, entry.observed_at AS entry_observed
                    FROM crypto_market_snapshots AS entry
                    JOIN labels
                      ON labels.market_ticker = entry.market_ticker
                    WHERE {" AND ".join(entry_where)}
                    ORDER BY entry.observed_at DESC, entry.market_ticker
                    LIMIT :entry_market_limit
                ),
                entry_markets AS (
                    SELECT market_ticker, MAX(entry_observed) AS latest_entry_observed
                    FROM entry_rows
                    GROUP BY market_ticker
                    ORDER BY latest_entry_observed DESC, market_ticker
                    LIMIT :entry_market_limit
                )
                SELECT
                    snapshot.id,
                    snapshot.kalshi_env,
                    snapshot.series_ticker,
                    snapshot.market_ticker,
                    snapshot.event_ticker,
                    snapshot.asset_symbol,
                    snapshot.frequency,
                    snapshot.title,
                    snapshot.status,
                    snapshot.open_time,
                    snapshot.close_time,
                    snapshot.expected_expiration_time,
                    snapshot.target_price_dollars,
                    snapshot.yes_bid_dollars,
                    snapshot.yes_ask_dollars,
                    snapshot.no_bid_dollars,
                    snapshot.no_ask_dollars,
                    snapshot.last_price_dollars,
                    snapshot.volume,
                    snapshot.open_interest,
                    labels.settlement_result,
                    snapshot.observed_at,
                    snapshot.source_kind,
                    snapshot.created_at,
                    snapshot.updated_at
                FROM entry_markets
                JOIN labels
                  ON labels.market_ticker = entry_markets.market_ticker
                JOIN crypto_market_snapshots AS snapshot
                  ON snapshot.market_ticker = entry_markets.market_ticker
                WHERE {" AND ".join(snapshot_where)}
                ORDER BY entry_markets.latest_entry_observed DESC, snapshot.market_ticker, snapshot.observed_at
                LIMIT :limit
                """
            )
            if symbols:
                raw = raw.bindparams(bindparam("symbols", expanding=True))
            await self._apply_crypto_snapshot_index_query_guards()
            rows = (await self.session.execute(raw, params)).mappings().all()
            fallback_rows = [
                row
                for row in (SimpleNamespace(**dict(row)) for row in rows)
                if _matches_requested_duration(row)
            ]
            return fallback_rows[:row_limit]

        if entry_seconds > 0 and bind is not None and bind.dialect.name == "postgresql" and not include_joined_fallback:
            from sqlalchemy import text as sql_text

            entry_market_limit = max(
                1,
                min(row_limit, int(entry_qualified_market_limit or max(200, row_limit // 20))),
            )
            min_market_age_seconds = max(0, int(entry_min_market_age_seconds or 0))
            entry_where = [
                "kalshi_env = :env",
                "source_kind <> 'settled_backfill'",
                "settlement_result IN ('yes', 'no')",
                _crypto_entry_quote_sql(),
                "(status IS NULL OR status IN ('open', 'active'))",
                "close_time IS NOT NULL",
                "observed_at <= close_time - (:entry_seconds * INTERVAL '1 second')",
            ]
            snapshot_where = [
                "snapshot.kalshi_env = :env",
                "snapshot.source_kind <> 'settled_backfill'",
                "snapshot.settlement_result IN ('yes', 'no')",
                """
                (
                    (snapshot.yes_bid_dollars IS NOT NULL AND snapshot.yes_ask_dollars IS NOT NULL)
                    OR (snapshot.no_bid_dollars IS NOT NULL AND snapshot.no_ask_dollars IS NOT NULL)
                )
                """,
            ]
            params: dict[str, Any] = {
                "env": env,
                "limit": row_limit,
                "entry_seconds": entry_seconds,
                "entry_market_limit": entry_market_limit,
            }
            if min_market_age_seconds > 0:
                entry_where.append(
                    "(open_time IS NULL OR observed_at >= open_time + (:entry_min_market_age_seconds * INTERVAL '1 second'))"
                )
                params["entry_min_market_age_seconds"] = min_market_age_seconds
            if frequency is not None:
                entry_where.append("frequency = :frequency")
                snapshot_where.append("snapshot.frequency = :frequency")
                params["frequency"] = frequency
            if symbols:
                entry_where.append("asset_symbol IN :symbols")
                snapshot_where.append("snapshot.asset_symbol IN :symbols")
                params["symbols"] = symbols
            if since is not None:
                entry_where.append("observed_at >= :since")
                snapshot_where.append("snapshot.observed_at >= :since")
                params["since"] = since
            if duration_bounds is not None:
                entry_where.append(
                    """
                    (
                        open_time IS NULL
                        OR close_time IS NULL
                        OR EXTRACT(EPOCH FROM (close_time - open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                snapshot_where.append(
                    """
                    (
                        snapshot.open_time IS NULL
                        OR snapshot.close_time IS NULL
                        OR EXTRACT(EPOCH FROM (snapshot.close_time - snapshot.open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                params["duration_min"] = duration_bounds[0]
                params["duration_max"] = duration_bounds[1]
            raw = sql_text(
                f"""
                WITH entry_rows AS (
                    SELECT market_ticker, observed_at AS entry_observed
                    FROM crypto_market_snapshots
                    WHERE {" AND ".join(entry_where)}
                    ORDER BY observed_at DESC, market_ticker
                    LIMIT :entry_market_limit
                ),
                entry_markets AS (
                    SELECT market_ticker, MAX(entry_observed) AS latest_entry_observed
                    FROM entry_rows
                    GROUP BY market_ticker
                    ORDER BY latest_entry_observed DESC, market_ticker
                    LIMIT :entry_market_limit
                )
                SELECT
                    snapshot.id,
                    snapshot.kalshi_env,
                    snapshot.series_ticker,
                    snapshot.market_ticker,
                    snapshot.event_ticker,
                    snapshot.asset_symbol,
                    snapshot.frequency,
                    snapshot.title,
                    snapshot.status,
                    snapshot.open_time,
                    snapshot.close_time,
                    snapshot.expected_expiration_time,
                    snapshot.target_price_dollars,
                    snapshot.yes_bid_dollars,
                    snapshot.yes_ask_dollars,
                    snapshot.no_bid_dollars,
                    snapshot.no_ask_dollars,
                    snapshot.last_price_dollars,
                    snapshot.volume,
                    snapshot.open_interest,
                    snapshot.settlement_result,
                    snapshot.observed_at,
                    snapshot.source_kind,
                    snapshot.created_at,
                    snapshot.updated_at
                FROM entry_markets
                JOIN crypto_market_snapshots AS snapshot
                  ON snapshot.market_ticker = entry_markets.market_ticker
                WHERE {" AND ".join(snapshot_where)}
                ORDER BY entry_markets.latest_entry_observed DESC, snapshot.market_ticker, snapshot.observed_at
                LIMIT :limit
                """
            )
            if symbols:
                raw = raw.bindparams(bindparam("symbols", expanding=True))
            await self._apply_crypto_snapshot_index_query_guards(disable_bitmapscan=False)
            rows = (await self.session.execute(raw, params)).mappings().all()
            direct_rows = [
                row
                for row in (SimpleNamespace(**dict(row)) for row in rows)
                if _matches_requested_duration(row)
            ]
            direct_rows.sort(
                key=lambda row: (
                    -getattr(row, "observed_at", datetime.min.replace(tzinfo=UTC)).timestamp(),
                    str(getattr(row, "market_ticker", "")),
                )
            )
            return direct_rows[:row_limit]
        direct_stmt = (
            select(CryptoMarketSnapshotRecord)
            .where(*direct_conditions)
            .order_by(CryptoMarketSnapshotRecord.observed_at.desc(), CryptoMarketSnapshotRecord.market_ticker)
            .limit(row_limit)
        )
        if defer_payload:
            direct_stmt = direct_stmt.options(defer(CryptoMarketSnapshotRecord.payload))
        direct_rows = [
            row
            for row in (await self.session.execute(direct_stmt)).scalars()
            if _matches_requested_duration(row)
        ]
        if len(direct_rows) >= row_limit or not include_joined_fallback:
            return direct_rows

        if bind is not None and bind.dialect.name == "postgresql":
            from sqlalchemy import text as sql_text

            label_where = [
                "kalshi_env = :env",
                "source_kind = 'settled_backfill'",
                "settlement_result IN ('yes', 'no')",
            ]
            snapshot_where = [
                "snapshot.kalshi_env = :env",
                "snapshot.source_kind <> 'settled_backfill'",
                """
                (
                    (snapshot.yes_bid_dollars IS NOT NULL AND snapshot.yes_ask_dollars IS NOT NULL)
                    OR (snapshot.no_bid_dollars IS NOT NULL AND snapshot.no_ask_dollars IS NOT NULL)
                )
                """,
                "snapshot.market_ticker = labels.market_ticker",
            ]
            params: dict[str, Any] = {"env": env, "limit": row_limit}
            if frequency is not None:
                label_where.append("frequency = :frequency")
                snapshot_where.append("snapshot.frequency = :frequency")
                params["frequency"] = frequency
            if symbols:
                label_where.append("asset_symbol IN :symbols")
                snapshot_where.append("snapshot.asset_symbol IN :symbols")
                params["symbols"] = symbols
            if since is not None:
                label_where.append("observed_at >= :since")
                snapshot_where.append("snapshot.observed_at >= :since")
                params["since"] = since
            if duration_bounds is not None:
                label_where.append(
                    """
                    (
                        open_time IS NULL
                        OR close_time IS NULL
                        OR EXTRACT(EPOCH FROM (close_time - open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                snapshot_where.append(
                    """
                    (
                        snapshot.open_time IS NULL
                        OR snapshot.close_time IS NULL
                        OR EXTRACT(EPOCH FROM (snapshot.close_time - snapshot.open_time)) BETWEEN :duration_min AND :duration_max
                    )
                    """
                )
                params["duration_min"] = duration_bounds[0]
                params["duration_max"] = duration_bounds[1]
            raw = sql_text(
                f"""
                WITH labels AS (
                    SELECT DISTINCT ON (market_ticker)
                           market_ticker,
                           settlement_result
                    FROM crypto_market_snapshots
                    WHERE {" AND ".join(label_where)}
                    ORDER BY market_ticker, observed_at DESC
                )
                SELECT
                    snapshot.id,
                    snapshot.kalshi_env,
                    snapshot.series_ticker,
                    snapshot.market_ticker,
                    snapshot.event_ticker,
                    snapshot.asset_symbol,
                    snapshot.frequency,
                    snapshot.title,
                    snapshot.status,
                    snapshot.open_time,
                    snapshot.close_time,
                    snapshot.expected_expiration_time,
                    snapshot.target_price_dollars,
                    snapshot.yes_bid_dollars,
                    snapshot.yes_ask_dollars,
                    snapshot.no_bid_dollars,
                    snapshot.no_ask_dollars,
                    snapshot.last_price_dollars,
                    snapshot.volume,
                    snapshot.open_interest,
                    labels.settlement_result,
                    snapshot.observed_at,
                    snapshot.source_kind,
                    snapshot.created_at,
                    snapshot.updated_at
                FROM labels
                JOIN LATERAL (
                    SELECT
                        id,
                        kalshi_env,
                        series_ticker,
                        market_ticker,
                        event_ticker,
                        asset_symbol,
                        frequency,
                        title,
                        status,
                        open_time,
                        close_time,
                        expected_expiration_time,
                        target_price_dollars,
                        yes_bid_dollars,
                        yes_ask_dollars,
                        no_bid_dollars,
                        no_ask_dollars,
                        last_price_dollars,
                        volume,
                        open_interest,
                        observed_at,
                        source_kind,
                        created_at,
                        updated_at
                    FROM crypto_market_snapshots AS snapshot
                    WHERE {" AND ".join(snapshot_where)}
                    ORDER BY snapshot.observed_at DESC
                ) AS snapshot ON TRUE
                ORDER BY snapshot.observed_at DESC, snapshot.market_ticker
                LIMIT :limit
                """
            )
            if symbols:
                raw = raw.bindparams(bindparam("symbols", expanding=True))
            await self._apply_crypto_snapshot_index_query_guards()
            rows = (await self.session.execute(raw, params)).mappings().all()
            fallback_rows = [
                row
                for row in (SimpleNamespace(**dict(row)) for row in rows)
                if _matches_requested_duration(row)
            ]
            if direct_rows:
                return _merge_quote_path_rows(direct_rows, fallback_rows)
            return fallback_rows

        label_conditions = [
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.source_kind == "settled_backfill",
            CryptoMarketSnapshotRecord.settlement_result.in_(["yes", "no"]),
        ]
        if frequency is not None:
            label_conditions.append(CryptoMarketSnapshotRecord.frequency == frequency)
        if symbols:
            label_conditions.append(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            label_conditions.append(CryptoMarketSnapshotRecord.observed_at >= since)
        label_stmt = (
            select(CryptoMarketSnapshotRecord.market_ticker, CryptoMarketSnapshotRecord.settlement_result)
            .where(*label_conditions)
            .order_by(CryptoMarketSnapshotRecord.observed_at.desc())
        )
        label_map: dict[str, str] = {}
        for market_ticker, settlement_result in (await self.session.execute(label_stmt)).all():
            ticker = str(market_ticker or "")
            result = str(settlement_result or "").lower()
            if ticker and result in {"yes", "no"} and ticker not in label_map:
                label_map[ticker] = result
        if not label_map:
            return direct_rows
        conditions = [
            CryptoMarketSnapshotRecord.kalshi_env == env,
            CryptoMarketSnapshotRecord.source_kind != "settled_backfill",
            or_(
                and_(
                    CryptoMarketSnapshotRecord.yes_bid_dollars.is_not(None),
                    CryptoMarketSnapshotRecord.yes_ask_dollars.is_not(None),
                ),
                and_(
                    CryptoMarketSnapshotRecord.no_bid_dollars.is_not(None),
                    CryptoMarketSnapshotRecord.no_ask_dollars.is_not(None),
                ),
            ),
        ]
        if frequency is not None:
            conditions.append(CryptoMarketSnapshotRecord.frequency == frequency)
        if symbols:
            conditions.append(CryptoMarketSnapshotRecord.asset_symbol.in_(symbols))
        if since is not None:
            conditions.append(CryptoMarketSnapshotRecord.observed_at >= since)
        snapshots: list[CryptoMarketSnapshotRecord] = []
        tickers = list(label_map)
        chunk_size = 1
        for offset in range(0, len(tickers), chunk_size):
            chunk = tickers[offset : offset + chunk_size]
            stmt = (
                select(CryptoMarketSnapshotRecord)
                .where(*conditions, CryptoMarketSnapshotRecord.market_ticker.in_(chunk))
                .order_by(CryptoMarketSnapshotRecord.observed_at.desc(), CryptoMarketSnapshotRecord.market_ticker)
                .limit(row_limit)
            )
            if defer_payload:
                stmt = stmt.options(defer(CryptoMarketSnapshotRecord.payload))
            for snapshot in (await self.session.execute(stmt)).scalars():
                joined = label_map.get(snapshot.market_ticker)
                if str(snapshot.settlement_result or "").lower() not in {"yes", "no"} and joined in {"yes", "no"}:
                    set_committed_value(snapshot, "settlement_result", joined)
                snapshots.append(snapshot)
        if direct_rows:
            return _merge_quote_path_rows(direct_rows, snapshots)
        snapshots = [snapshot for snapshot in snapshots if _matches_requested_duration(snapshot)]
        snapshots.sort(key=lambda row: (row.observed_at, row.market_ticker), reverse=True)
        return snapshots[:row_limit]

    async def update_crypto_snapshot_settlement_result(
        self,
        *,
        market_ticker: str,
        settlement_result: str,
        kalshi_env: str | None = None,
        frequency: str | None = None,
        observed_since: datetime | None = None,
    ) -> int:
        return await self.update_crypto_snapshot_settlement_results(
            {market_ticker: settlement_result},
            kalshi_env=kalshi_env,
            frequency=frequency,
            observed_since=observed_since,
        )

    async def update_crypto_snapshot_settlement_results(
        self,
        settlements: dict[str, str],
        *,
        kalshi_env: str | None = None,
        frequency: str | None = None,
        observed_since: datetime | None = None,
        require_quote_path: bool = False,
    ) -> int:
        """Propagate a settled market's result to its live monitoring snapshots.

        The settlement backfill creates a separate ``settled_backfill`` record, but
        the earlier live monitoring snapshots retain ``settlement_result=NULL``.
        Updating them here makes ``list_crypto_settled_market_snapshots`` include
        those rows, giving the replay access to real bid/ask prices
        (``quote_source='snapshot_quotes'``) for strict-trade-eligible rows. When
        a nightly/preflight window is supplied, scope the propagation to that
        window so old historical rows do not dominate the training backfill.
        """
        rows = [
            (str(market_ticker).strip(), str(settlement_result).strip())
            for market_ticker, settlement_result in settlements.items()
            if str(market_ticker or "").strip() and str(settlement_result or "").strip() in {"yes", "no"}
        ]
        if not rows:
            return 0
        env = self._resolved_kalshi_env(kalshi_env)
        bind = self.session.get_bind()
        if bind is not None and bind.dialect.name == "postgresql":
            from sqlalchemy import text as sql_text

            filters = [
                "snapshot.kalshi_env = :kalshi_env",
                "snapshot.market_ticker = :market_ticker",
                "snapshot.source_kind != 'settled_backfill'",
                "snapshot.settlement_result IS NULL",
            ]
            params: dict[str, Any] = {"kalshi_env": env}
            if frequency is not None:
                filters.append("snapshot.frequency = :frequency")
                params["frequency"] = frequency
            if observed_since is not None:
                filters.append("snapshot.observed_at >= :observed_since")
                params["observed_since"] = observed_since
            if require_quote_path:
                filters.append(
                    """
                    (
                        (snapshot.yes_bid_dollars IS NOT NULL AND snapshot.yes_ask_dollars IS NOT NULL)
                        OR (snapshot.no_bid_dollars IS NOT NULL AND snapshot.no_ask_dollars IS NOT NULL)
                    )
                    """
                )
            await self._apply_crypto_snapshot_index_query_guards()
            stmt = sql_text(
                f"""
                UPDATE crypto_market_snapshots AS snapshot
                SET settlement_result = :settlement_result,
                    updated_at = now()
                WHERE {" AND ".join(filters)}
                """
            )
            total = 0
            for market_ticker, settlement_result in rows:
                result = await self.session.execute(
                    stmt,
                    {
                        **params,
                        "market_ticker": market_ticker,
                        "settlement_result": settlement_result,
                    },
                )
                total += int(result.rowcount or 0)
            return total
        total = 0
        for market_ticker, settlement_result in rows:
            stmt = (
                sql_update(CryptoMarketSnapshotRecord)
                .where(
                    CryptoMarketSnapshotRecord.market_ticker == market_ticker,
                    CryptoMarketSnapshotRecord.kalshi_env == env,
                    CryptoMarketSnapshotRecord.source_kind != "settled_backfill",
                    CryptoMarketSnapshotRecord.settlement_result.is_(None),
                )
                .values(settlement_result=settlement_result)
            )
            if frequency is not None:
                stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
            if observed_since is not None:
                stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= observed_since)
            if require_quote_path:
                stmt = stmt.where(
                    or_(
                        and_(
                            CryptoMarketSnapshotRecord.yes_bid_dollars.is_not(None),
                            CryptoMarketSnapshotRecord.yes_ask_dollars.is_not(None),
                        ),
                        and_(
                            CryptoMarketSnapshotRecord.no_bid_dollars.is_not(None),
                            CryptoMarketSnapshotRecord.no_ask_dollars.is_not(None),
                        ),
                    )
                )
            result = await self.session.execute(stmt.execution_options(synchronize_session=False))
            if result.rowcount is not None and result.rowcount > 0:
                total += result.rowcount
        return total

    async def list_crypto_snapshot_asset_symbols(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        since: datetime | None = None,
    ) -> list[str]:
        stmt = select(CryptoMarketSnapshotRecord.asset_symbol).distinct().where(
            CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.frequency == frequency)
        if since is not None:
            stmt = stmt.where(CryptoMarketSnapshotRecord.observed_at >= since)
        rows = (await self.session.execute(stmt)).scalars().all()
        return sorted({row for row in rows if row})

    async def list_latest_crypto_market_snapshots(
        self,
        *,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        limit: int = 200,
    ) -> list[CryptoMarketSnapshotRecord]:
        rows = await self.list_crypto_market_snapshots(
            frequency=frequency,
            kalshi_env=kalshi_env,
            asset_symbols=asset_symbols,
            limit=max(limit * 6, limit),
            match_frequency_duration=True,
        )
        latest: list[CryptoMarketSnapshotRecord] = []
        seen: set[str] = set()
        for row in rows:
            if row.market_ticker in seen:
                continue
            seen.add(row.market_ticker)
            latest.append(row)
            if len(latest) >= limit:
                break
        return latest

    async def get_latest_crypto_market_snapshot(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> CryptoMarketSnapshotRecord | None:
        stmt = (
            select(CryptoMarketSnapshotRecord)
            .where(
                CryptoMarketSnapshotRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CryptoMarketSnapshotRecord.market_ticker == market_ticker,
            )
            .order_by(CryptoMarketSnapshotRecord.observed_at.desc(), CryptoMarketSnapshotRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def upsert_crypto_market_candlestick(
        self,
        *,
        series_ticker: str,
        market_ticker: str,
        asset_symbol: str,
        end_period_ts: datetime,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        period_interval: int = 1,
        open_dollars: Decimal | None = None,
        high_dollars: Decimal | None = None,
        low_dollars: Decimal | None = None,
        close_dollars: Decimal | None = None,
        volume: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> CryptoMarketCandlestickRecord:
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "series_ticker": series_ticker,
            "market_ticker": market_ticker,
            "asset_symbol": asset_symbol,
            "frequency": frequency,
            "period_interval": period_interval,
            "end_period_ts": end_period_ts,
            "open_dollars": open_dollars,
            "high_dollars": high_dollars,
            "low_dollars": low_dollars,
            "close_dollars": close_dollars,
            "volume": volume,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoMarketCandlestickRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoMarketCandlestickRecord).values(**insert_values)
        else:
            record = CryptoMarketCandlestickRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoMarketCandlestickRecord.kalshi_env,
                    CryptoMarketCandlestickRecord.market_ticker,
                    CryptoMarketCandlestickRecord.period_interval,
                    CryptoMarketCandlestickRecord.end_period_ts,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoMarketCandlestickRecord).where(
                    CryptoMarketCandlestickRecord.kalshi_env == env,
                    CryptoMarketCandlestickRecord.market_ticker == market_ticker,
                    CryptoMarketCandlestickRecord.period_interval == period_interval,
                    CryptoMarketCandlestickRecord.end_period_ts == end_period_ts,
                )
            )
        ).scalar_one()
        return result

    async def list_crypto_market_candlesticks(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        market_ticker: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        limit: int = 1000,
        defer_payload: bool = False,
    ) -> list[CryptoMarketCandlestickRecord]:
        stmt = select(CryptoMarketCandlestickRecord).where(
            CryptoMarketCandlestickRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.frequency == frequency)
        if market_ticker is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.market_ticker == market_ticker)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoMarketCandlestickRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.end_period_ts >= since)
        if defer_payload:
            stmt = stmt.options(defer(CryptoMarketCandlestickRecord.payload))
        stmt = stmt.order_by(CryptoMarketCandlestickRecord.end_period_ts.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def map_crypto_candlestick_coverage(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        since: datetime | None = None,
    ) -> dict[str, datetime]:
        """Latest stored candle end time per market ticker, for crawl skip decisions."""
        stmt = select(
            CryptoMarketCandlestickRecord.market_ticker,
            func.max(CryptoMarketCandlestickRecord.end_period_ts),
        ).where(CryptoMarketCandlestickRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env))
        if frequency is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.frequency == frequency)
        if since is not None:
            stmt = stmt.where(CryptoMarketCandlestickRecord.end_period_ts >= since)
        stmt = stmt.group_by(CryptoMarketCandlestickRecord.market_ticker)
        return {ticker: end_ts for ticker, end_ts in (await self.session.execute(stmt)).all() if end_ts is not None}

    async def upsert_crypto_spot_ohlc(
        self,
        *,
        provider: str,
        asset_symbol: str,
        end_ts: datetime,
        frequency: str = "15m",
        kalshi_env: str | None = None,
        quote_currency: str = "USD",
        interval_seconds: int = 900,
        start_ts: datetime | None = None,
        open_dollars: Decimal | None = None,
        high_dollars: Decimal | None = None,
        low_dollars: Decimal | None = None,
        close_dollars: Decimal | None = None,
        volume: Decimal | None = None,
        observed_at: datetime | None = None,
        source_kind: str = "spot_ohlc",
        source_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> CryptoSpotOHLCRecord:
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": str(uuid4()),
            "kalshi_env": env,
            "provider": provider,
            "asset_symbol": asset_symbol,
            "quote_currency": quote_currency,
            "frequency": frequency,
            "interval_seconds": interval_seconds,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "open_dollars": open_dollars,
            "high_dollars": high_dollars,
            "low_dollars": low_dollars,
            "close_dollars": close_dollars,
            "volume": volume,
            "observed_at": observed_at or now,
            "source_kind": source_kind,
            "source_id": source_id,
            "payload": payload or {},
            "created_at": now,
            "updated_at": now,
        }
        update_values = {key: value for key, value in insert_values.items() if key not in {"id", "created_at"}}
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(CryptoSpotOHLCRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(CryptoSpotOHLCRecord).values(**insert_values)
        else:
            record = CryptoSpotOHLCRecord(**insert_values)
            self.session.add(record)
            await self.session.flush()
            return record
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoSpotOHLCRecord.kalshi_env,
                    CryptoSpotOHLCRecord.provider,
                    CryptoSpotOHLCRecord.asset_symbol,
                    CryptoSpotOHLCRecord.quote_currency,
                    CryptoSpotOHLCRecord.interval_seconds,
                    CryptoSpotOHLCRecord.end_ts,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(CryptoSpotOHLCRecord).where(
                    CryptoSpotOHLCRecord.kalshi_env == env,
                    CryptoSpotOHLCRecord.provider == provider,
                    CryptoSpotOHLCRecord.asset_symbol == asset_symbol,
                    CryptoSpotOHLCRecord.quote_currency == quote_currency,
                    CryptoSpotOHLCRecord.interval_seconds == interval_seconds,
                    CryptoSpotOHLCRecord.end_ts == end_ts,
                )
            )
        ).scalar_one()
        return result

    async def list_crypto_spot_ohlc(
        self,
        *,
        frequency: str | None = None,
        kalshi_env: str | None = None,
        provider: str | None = None,
        asset_symbol: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 1000,
        defer_payload: bool = False,
    ) -> list[CryptoSpotOHLCRecord]:
        stmt = select(CryptoSpotOHLCRecord).where(
            CryptoSpotOHLCRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if frequency is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.frequency == frequency)
        if provider is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.provider == provider)
        if asset_symbol is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.asset_symbol == asset_symbol)
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoSpotOHLCRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.end_ts >= since)
        if until is not None:
            stmt = stmt.where(CryptoSpotOHLCRecord.end_ts <= until)
        if defer_payload:
            stmt = stmt.options(defer(CryptoSpotOHLCRecord.payload))
        stmt = stmt.order_by(CryptoSpotOHLCRecord.end_ts.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    def _upsert_stmt_for(self, model: type[Any], values: dict[str, Any]) -> Any | None:
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            return pg_insert(model).values(**values)
        if dialect_name == "sqlite":
            return sqlite_insert(model).values(**values)
        return None

    async def upsert_crypto_order_book_snapshot(self, **values: Any) -> None:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        values["market_ticker"] = values.get("market_ticker") or ""
        update_values = {key: value for key, value in values.items() if key not in {"id", "created_at"}}
        stmt = self._upsert_stmt_for(CryptoOrderBookSnapshotRecord, values)
        if stmt is None:
            self.session.add(CryptoOrderBookSnapshotRecord(**values))
            await self.session.flush()
            return
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoOrderBookSnapshotRecord.kalshi_env,
                    CryptoOrderBookSnapshotRecord.provider,
                    CryptoOrderBookSnapshotRecord.asset_symbol,
                    CryptoOrderBookSnapshotRecord.frequency,
                    CryptoOrderBookSnapshotRecord.market_ticker,
                    CryptoOrderBookSnapshotRecord.observed_at,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()

    async def upsert_crypto_trade_tick(self, **values: Any) -> None:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        values["market_ticker"] = values.get("market_ticker") or ""
        values["source_id"] = values.get("source_id") or ""
        values["trade_id"] = values.get("trade_id") or ""
        update_values = {key: value for key, value in values.items() if key not in {"id", "created_at"}}
        stmt = self._upsert_stmt_for(CryptoTradeTickRecord, values)
        if stmt is None:
            self.session.add(CryptoTradeTickRecord(**values))
            await self.session.flush()
            return
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoTradeTickRecord.kalshi_env,
                    CryptoTradeTickRecord.provider,
                    CryptoTradeTickRecord.asset_symbol,
                    CryptoTradeTickRecord.source_id,
                    CryptoTradeTickRecord.trade_id,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()

    async def upsert_crypto_settlement_benchmark_window(self, **values: Any) -> None:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        update_values = {key: value for key, value in values.items() if key not in {"id", "created_at"}}
        stmt = self._upsert_stmt_for(CryptoSettlementBenchmarkWindowRecord, values)
        if stmt is None:
            self.session.add(CryptoSettlementBenchmarkWindowRecord(**values))
            await self.session.flush()
            return
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoSettlementBenchmarkWindowRecord.kalshi_env,
                    CryptoSettlementBenchmarkWindowRecord.market_ticker,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()

    async def upsert_crypto_training_feature_row(self, **values: Any) -> None:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        update_values = {key: value for key, value in values.items() if key not in {"id", "created_at"}}
        stmt = self._upsert_stmt_for(CryptoTrainingFeatureRowRecord, values)
        if stmt is None:
            self.session.add(CryptoTrainingFeatureRowRecord(**values))
            await self.session.flush()
            return
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoTrainingFeatureRowRecord.kalshi_env,
                    CryptoTrainingFeatureRowRecord.frequency,
                    CryptoTrainingFeatureRowRecord.row_id,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()

    async def bulk_upsert_crypto_training_feature_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        chunk_size: int = 2000,
    ) -> int:
        """Upsert many training feature rows per statement (idempotent on
        (kalshi_env, frequency, row_id)). Re-upserting an existing row_id
        refreshes its columns (e.g. a label that settled since first build)."""
        if not rows:
            return 0
        prepared: list[dict[str, Any]] = []
        for values in rows:
            payload = dict(values)
            payload["kalshi_env"] = self._resolved_kalshi_env(payload.get("kalshi_env"))
            payload.pop("id", None)
            payload.pop("created_at", None)
            prepared.append(payload)

        dialect = self.session.bind.dialect.name if self.session.bind is not None else ""
        written = 0
        conflict_keys = {"kalshi_env", "frequency", "row_id"}
        for start in range(0, len(prepared), chunk_size):
            chunk = prepared[start : start + chunk_size]
            if dialect == "postgresql":
                stmt = pg_insert(CryptoTrainingFeatureRowRecord).values(chunk)
            elif dialect == "sqlite":
                stmt = sqlite_insert(CryptoTrainingFeatureRowRecord).values(chunk)
            else:
                for payload in chunk:
                    self.session.add(CryptoTrainingFeatureRowRecord(**payload))
                written += len(chunk)
                continue
            # Assumes all dicts in a chunk share the same keys — true for in-house callers
            # that build rows from a uniform schema.
            update_cols = {
                col: getattr(stmt.excluded, col)
                for col in chunk[0].keys()
                if col not in conflict_keys
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    CryptoTrainingFeatureRowRecord.kalshi_env,
                    CryptoTrainingFeatureRowRecord.frequency,
                    CryptoTrainingFeatureRowRecord.row_id,
                ],
                set_=update_cols,
            )
            await self.session.execute(stmt)
            written += len(chunk)
        await self.session.flush()
        return written

    async def list_crypto_training_feature_rows(
        self,
        *,
        frequency: str,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 1000,
    ) -> list[CryptoTrainingFeatureRowRecord]:
        stmt = select(CryptoTrainingFeatureRowRecord).where(
            CryptoTrainingFeatureRowRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoTrainingFeatureRowRecord.frequency == frequency,
        )
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoTrainingFeatureRowRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoTrainingFeatureRowRecord.decision_time >= since)
        if until is not None:
            stmt = stmt.where(CryptoTrainingFeatureRowRecord.decision_time <= until)
        stmt = stmt.order_by(CryptoTrainingFeatureRowRecord.decision_time.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def get_crypto_training_feature_watermark(
        self,
        *,
        frequency: str,
        kalshi_env: str | None = None,
        feature_schema_version: str,
    ) -> datetime | None:
        """Max persisted decision_time for (env, frequency) at a given schema
        version, or None if no schema-matched rows exist (cold cache)."""
        stmt = select(func.max(CryptoTrainingFeatureRowRecord.decision_time)).where(
            CryptoTrainingFeatureRowRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoTrainingFeatureRowRecord.frequency == frequency,
            CryptoTrainingFeatureRowRecord.feature_schema_version == feature_schema_version,
        )
        value = (await self.session.execute(stmt)).scalar_one_or_none()
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    async def upsert_crypto_decision_outcome(self, **values: Any) -> None:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        update_values = {key: value for key, value in values.items() if key not in {"id", "created_at"}}
        stmt = self._upsert_stmt_for(CryptoDecisionOutcomeRecord, values)
        if stmt is None:
            self.session.add(CryptoDecisionOutcomeRecord(**values))
            await self.session.flush()
            return
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoDecisionOutcomeRecord.kalshi_env,
                    CryptoDecisionOutcomeRecord.market_ticker,
                    CryptoDecisionOutcomeRecord.decision_time,
                    CryptoDecisionOutcomeRecord.input_hash,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()

    async def count_crypto_decision_outcomes(
        self,
        *,
        frequency: str,
        kalshi_env: str | None = None,
        asset_symbols: list[str] | None = None,
        since: datetime | None = None,
    ) -> int:
        stmt = select(func.count()).select_from(CryptoDecisionOutcomeRecord).where(
            CryptoDecisionOutcomeRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            CryptoDecisionOutcomeRecord.frequency == frequency,
        )
        symbols = [symbol for symbol in (asset_symbols or []) if str(symbol or "").strip()]
        if symbols:
            stmt = stmt.where(CryptoDecisionOutcomeRecord.asset_symbol.in_(symbols))
        if since is not None:
            stmt = stmt.where(CryptoDecisionOutcomeRecord.decision_time >= since)
        return int((await self.session.execute(stmt)).scalar_one() or 0)

    async def list_recent_settled_crypto_outcomes(
        self,
        *,
        frequency: str,
        kalshi_env: str | None = None,
        asset_symbol: str,
        limit: int,
    ) -> list[CryptoDecisionOutcomeRecord]:
        stmt = (
            select(CryptoDecisionOutcomeRecord)
            .where(
                CryptoDecisionOutcomeRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CryptoDecisionOutcomeRecord.frequency == frequency,
                CryptoDecisionOutcomeRecord.asset_symbol == asset_symbol,
                CryptoDecisionOutcomeRecord.fill_count > 0,
                CryptoDecisionOutcomeRecord.settlement_result.is_not(None),
            )
            .order_by(CryptoDecisionOutcomeRecord.decision_time.desc())
            .limit(limit)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def record_crypto_data_quality_run(self, **values: Any) -> CryptoDataQualityRunRecord:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        record = CryptoDataQualityRunRecord(**values)
        self.session.add(record)
        await self.session.flush()
        return record

    async def upsert_crypto_execution_example(self, **values: Any) -> None:
        values["kalshi_env"] = self._resolved_kalshi_env(values.get("kalshi_env"))
        values["order_id"] = values.get("order_id") or ""
        update_values = {key: value for key, value in values.items() if key not in {"id", "created_at"}}
        stmt = self._upsert_stmt_for(CryptoExecutionExampleRecord, values)
        if stmt is None:
            self.session.add(CryptoExecutionExampleRecord(**values))
            await self.session.flush()
            return
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    CryptoExecutionExampleRecord.kalshi_env,
                    CryptoExecutionExampleRecord.market_ticker,
                    CryptoExecutionExampleRecord.decision_time,
                    CryptoExecutionExampleRecord.order_id,
                ],
                set_=update_values,
            )
        )
        await self.session.flush()

    async def upsert_crypto_funding_rate(
        self,
        *,
        provider: str,
        asset_symbol: str,
        quote_currency: str,
        settlement_ts: datetime,
        funding_rate: Any,
        realized_rate: Any,
        payload: dict[str, Any],
    ) -> CryptoFundingRateRecord:
        from decimal import Decimal
        import uuid

        insert_values = {
            "id": str(uuid.uuid4()),
            "provider": provider,
            "asset_symbol": asset_symbol,
            "quote_currency": quote_currency,
            "settlement_ts": settlement_ts,
            "funding_rate": Decimal(str(funding_rate)),
            "realized_rate": Decimal(str(realized_rate)),
            "payload": payload,
        }
        set_fields = {
            "realized_rate": insert_values["realized_rate"],
            "funding_rate": insert_values["funding_rate"],
            "payload": insert_values["payload"],
        }
        try:
            stmt = pg_insert(CryptoFundingRateRecord).values(**insert_values)
            stmt = stmt.on_conflict_do_update(constraint="uq_crypto_funding_rates_period", set_=set_fields)
            await self.session.execute(stmt)
            await self.session.flush()
        except Exception:
            stmt = sqlite_insert(CryptoFundingRateRecord).values(**insert_values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["provider", "asset_symbol", "settlement_ts"],
                set_=set_fields,
            )
            await self.session.execute(stmt)
            await self.session.flush()
        result = await self.session.execute(
            select(CryptoFundingRateRecord).where(
                CryptoFundingRateRecord.provider == provider,
                CryptoFundingRateRecord.asset_symbol == asset_symbol,
                CryptoFundingRateRecord.settlement_ts == settlement_ts,
            )
        )
        return result.scalars().first()  # type: ignore[return-value]

    async def list_crypto_funding_rates(
        self,
        asset_symbol: str,
        *,
        provider: str | None = None,
        before_ts: datetime | None = None,
        limit: int = 10,
    ) -> list[CryptoFundingRateRecord]:
        """Return settled funding rates newest-first, optionally filtered to before_ts."""
        stmt = select(CryptoFundingRateRecord).where(
            CryptoFundingRateRecord.asset_symbol == asset_symbol
        )
        if provider is not None:
            stmt = stmt.where(CryptoFundingRateRecord.provider == provider)
        if before_ts is not None:
            stmt = stmt.where(CryptoFundingRateRecord.settlement_ts < before_ts)
        stmt = stmt.order_by(CryptoFundingRateRecord.settlement_ts.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_crypto_funding_rates_bulk(
        self,
        *,
        asset_symbols: list[str] | None = None,
        limit: int = 50_000,
    ) -> list[CryptoFundingRateRecord]:
        """Return funding rates for multiple assets oldest-first, for bulk training use."""
        stmt = select(CryptoFundingRateRecord)
        if asset_symbols:
            stmt = stmt.where(CryptoFundingRateRecord.asset_symbol.in_(asset_symbols))
        stmt = stmt.order_by(CryptoFundingRateRecord.settlement_ts.asc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def record_crypto_model_artifact(
        self,
        *,
        frequency: str,
        artifact_type: str,
        version: str,
        status: str,
        sample_count: int,
        metrics: dict[str, Any],
        payload: dict[str, Any],
        kalshi_env: str | None = None,
        trained_at: datetime | None = None,
    ) -> CryptoModelArtifactRecord:
        record = CryptoModelArtifactRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            frequency=frequency,
            artifact_type=artifact_type,
            version=version,
            status=status,
            trained_at=trained_at,
            sample_count=sample_count,
            metrics=metrics,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_latest_crypto_model_artifact(
        self,
        *,
        frequency: str = "15m",
        artifact_type: str,
        kalshi_env: str | None = None,
    ) -> CryptoModelArtifactRecord | None:
        stmt = (
            select(CryptoModelArtifactRecord)
            .where(
                CryptoModelArtifactRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
                CryptoModelArtifactRecord.frequency == frequency,
                CryptoModelArtifactRecord.artifact_type == artifact_type,
            )
            .order_by(CryptoModelArtifactRecord.created_at.desc(), CryptoModelArtifactRecord.id.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_momentum_shadow_metrics(
        self,
        *,
        kalshi_env: str,
        window_hours: int = 24,
        veto_threshold_cents_per_min: float | None = None,
    ) -> dict[str, Any]:
        """Return shadow-mode outcome counts and slope/weight averages for the rolling window.

        Aggregates momentum_post_processor_outcome from signal payloads.
        avg_slope_cents_per_min includes all rows that have a slope stamped (any outcome).
        avg_weight is restricted to 'success' rows.
        veto_fraction is the share of 'success' rows where |slope| exceeds the threshold.
        """
        cutoff = datetime.now(UTC) - timedelta(hours=window_hours)
        stmt = (
            select(Signal.payload)
            .join(Room, Signal.room_id == Room.id)
            .where(
                Room.kalshi_env == kalshi_env,
                Signal.created_at >= cutoff,
            )
        )
        payloads = list((await self.session.execute(stmt)).scalars())

        by_outcome: dict[str, int] = {
            "success": 0,
            "calibration_missing": 0,
            "insufficient_points": 0,
            "price_history_error": 0,
            "unknown": 0,
        }
        slopes: list[float] = []
        weights: list[float] = []
        success_with_slope = 0
        veto_count = 0

        for payload in payloads:
            if not isinstance(payload, dict):
                by_outcome["unknown"] += 1
                continue
            outcome = str(payload.get("momentum_post_processor_outcome") or "unknown")
            if outcome not in by_outcome:
                outcome = "unknown"
            by_outcome[outcome] += 1

            raw_slope = payload.get("momentum_slope_cents_per_min")
            raw_weight = payload.get("momentum_weight")

            if raw_slope is not None:
                try:
                    slopes.append(float(raw_slope))
                except (TypeError, ValueError):
                    pass

            if outcome == "success" and raw_weight is not None:
                try:
                    weights.append(float(raw_weight))
                except (TypeError, ValueError):
                    pass

            if outcome == "success" and raw_slope is not None and veto_threshold_cents_per_min is not None:
                try:
                    success_with_slope += 1
                    if abs(float(raw_slope)) > veto_threshold_cents_per_min:
                        veto_count += 1
                except (TypeError, ValueError):
                    pass

        return {
            "window_hours": window_hours,
            "total": len(payloads),
            "by_outcome": by_outcome,
            "avg_slope_cents_per_min": (sum(slopes) / len(slopes)) if slopes else None,
            "avg_weight": (sum(weights) / len(weights)) if weights else None,
            "veto_fraction": (veto_count / success_with_slope) if success_with_slope > 0 else None,
        }

    async def purge_market_price_history(
        self,
        *,
        older_than: timedelta,
        kalshi_env: str | None = None,
    ) -> int:
        from sqlalchemy import delete as sa_delete
        cutoff = datetime.now(UTC) - older_than
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = sa_delete(MarketPriceHistory).where(
            MarketPriceHistory.kalshi_env == env,
            MarketPriceHistory.observed_at < cutoff,
        )
        result = await self.session.execute(stmt)
        return result.rowcount or 0

    async def vacuum_memory_notes(self, *, older_than_days: int) -> int:
        """Delete memory notes (and their cascade-linked embeddings) older than the retention window."""
        from sqlalchemy import delete as sa_delete
        cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
        stmt = sa_delete(MemoryNoteRecord).where(MemoryNoteRecord.created_at < cutoff)
        result = await self.session.execute(stmt)
        return result.rowcount or 0

    async def get_latest_trade_ticket_for_room(self, room_id: str) -> TradeTicketRecord | None:
        stmt = (
            select(TradeTicketRecord)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(TradeTicketRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_trade_ticket(
        self,
        room_id: str,
        ticket: TradeTicket,
        client_order_id: str,
        message_id: str | None = None,
        *,
        strategy_code: str | None = None,
    ) -> TradeTicketRecord:
        record = TradeTicketRecord(
            room_id=room_id,
            message_id=message_id,
            market_ticker=ticket.market_ticker,
            action=ticket.action.value,
            side=ticket.side.value,
            yes_price_dollars=ticket.yes_price_dollars,
            count_fp=ticket.count_fp,
            time_in_force=ticket.time_in_force,
            client_order_id=client_order_id,
            strategy_code=strategy_code,
            payload=ticket.model_dump(mode="json"),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def update_trade_ticket_status(self, ticket_id: str, status: str) -> TradeTicketRecord | None:
        record = await self.session.get(TradeTicketRecord, ticket_id)
        if record is None:
            return None
        record.status = status
        await self.session.flush()
        return record

    async def save_decision_trace(
        self,
        *,
        room_id: str | None,
        ticket_id: str | None,
        market_ticker: str,
        kalshi_env: str | None,
        decision_kind: str,
        path_version: str,
        source_snapshot_ids: dict[str, Any],
        input_hash: str,
        trace_hash: str,
        trace: dict[str, Any],
        decision_time: datetime | None = None,
        agent_pack_version: str | None = None,
        parameter_pack_version: str | None = None,
    ) -> DecisionTraceRecord:
        record = DecisionTraceRecord(
            room_id=room_id,
            ticket_id=ticket_id,
            market_ticker=market_ticker,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            decision_kind=decision_kind,
            decision_time=decision_time or datetime.now(UTC),
            path_version=path_version,
            agent_pack_version=agent_pack_version,
            parameter_pack_version=parameter_pack_version,
            source_snapshot_ids=source_snapshot_ids,
            input_hash=input_hash,
            trace_hash=trace_hash,
            trace=trace,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_decision_trace(self, decision_trace_id: str) -> DecisionTraceRecord | None:
        stmt = select(DecisionTraceRecord).where(DecisionTraceRecord.id == decision_trace_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_decision_trace_for_room(self, room_id: str) -> DecisionTraceRecord | None:
        stmt = (
            select(DecisionTraceRecord)
            .where(DecisionTraceRecord.room_id == room_id)
            .order_by(DecisionTraceRecord.created_at.desc(), DecisionTraceRecord.decision_time.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_decision_trace_for_market(
        self,
        market_ticker: str,
        *,
        kalshi_env: str | None = None,
    ) -> DecisionTraceRecord | None:
        stmt = (
            select(DecisionTraceRecord)
            .where(
                DecisionTraceRecord.market_ticker == market_ticker,
                DecisionTraceRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            )
            .order_by(DecisionTraceRecord.decision_time.desc(), DecisionTraceRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_forecast_snapshot(
        self,
        *,
        market_ticker: str,
        kalshi_env: str | None,
        source_members: dict[str, Any],
        fused_pdf: dict[str, Any],
        probability_output: dict[str, Any],
        source_set_used: list[str],
        fetched_at: datetime | None = None,
        parameter_pack_version: str | None = None,
    ) -> ForecastSnapshotRecord:
        record = ForecastSnapshotRecord(
            market_ticker=market_ticker,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            fetched_at=fetched_at or datetime.now(UTC),
            parameter_pack_version=parameter_pack_version,
            source_members=source_members,
            fused_pdf=fused_pdf,
            probability_output=probability_output,
            source_set_used=source_set_used,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_climatology_prior(
        self,
        *,
        station_id: str,
        day_of_year: int,
        bucket_low_f: float | None,
        bucket_high_f: float | None,
        series_ticker: str | None = None,
    ) -> ClimatologyPriorRecord | None:
        stmt = select(ClimatologyPriorRecord).where(
            ClimatologyPriorRecord.station_id == station_id,
            ClimatologyPriorRecord.day_of_year == day_of_year,
        )
        if series_ticker is not None:
            stmt = stmt.where(ClimatologyPriorRecord.series_ticker == series_ticker)
        if bucket_low_f is None:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_low_f.is_(None))
        else:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_low_f == bucket_low_f)
        if bucket_high_f is None:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_high_f.is_(None))
        else:
            stmt = stmt.where(ClimatologyPriorRecord.bucket_high_f == bucket_high_f)
        stmt = stmt.order_by(ClimatologyPriorRecord.updated_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_source_health_log(
        self,
        *,
        source: str,
        label: str,
        score: float,
        success_score: float,
        freshness_score: float,
        completeness_score: float,
        consistency_score: float,
        kalshi_env: str | None = None,
        market_ticker: str | None = None,
        station_id: str | None = None,
        observed_at: datetime | None = None,
        is_aggregate: bool = False,
        payload: dict[str, Any] | None = None,
    ) -> SourceHealthLogRecord:
        record = SourceHealthLogRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            source=source,
            is_aggregate=is_aggregate,
            market_ticker=market_ticker,
            station_id=station_id,
            observed_at=observed_at or datetime.now(UTC),
            label=label,
            score=score,
            success_score=success_score,
            freshness_score=freshness_score,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_recent_source_health_logs(
        self,
        *,
        kalshi_env: str | None = None,
        aggregate_only: bool | None = None,
        source: str | None = None,
        limit: int = 50,
    ) -> list[SourceHealthLogRecord]:
        stmt = select(SourceHealthLogRecord).where(
            SourceHealthLogRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if aggregate_only is not None:
            stmt = stmt.where(SourceHealthLogRecord.is_aggregate.is_(aggregate_only))
        if source is not None:
            stmt = stmt.where(SourceHealthLogRecord.source == source)
        stmt = stmt.order_by(SourceHealthLogRecord.observed_at.desc(), SourceHealthLogRecord.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars().all())

    async def save_weather_bootstrap_event(
        self,
        *,
        kalshi_env: str | None = None,
        market_ticker: str,
        event_type: str,
        status: str,
        series_ticker: str | None = None,
        local_market_day: str | None = None,
        bucket_key: str | None = None,
        policy_key: str | None = None,
        tier: str | None = None,
        side: str | None = None,
        confidence: float | None = None,
        edge_bps: int | None = None,
        size_factor: float | None = None,
        count_fp: Decimal | None = None,
        notional_dollars: Decimal | None = None,
        pnl_dollars: Decimal | None = None,
        evidence_weight: float = 1.0,
        source: str = "live",
        occurred_at: datetime | None = None,
        room_id: str | None = None,
        decision_trace_id: str | None = None,
        order_id: str | None = None,
        fill_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WeatherBootstrapEventRecord:
        record = WeatherBootstrapEventRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            bucket_key=bucket_key,
            policy_key=policy_key,
            tier=tier,
            event_type=event_type,
            status=status,
            side=side,
            confidence=confidence,
            edge_bps=edge_bps,
            size_factor=size_factor,
            count_fp=count_fp,
            notional_dollars=notional_dollars,
            pnl_dollars=pnl_dollars,
            evidence_weight=evidence_weight,
            source=source,
            occurred_at=occurred_at or datetime.now(UTC),
            room_id=room_id,
            decision_trace_id=decision_trace_id,
            order_id=order_id,
            fill_id=fill_id,
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_weather_bootstrap_events(
        self,
        *,
        kalshi_env: str | None = None,
        bucket_key: str | None = None,
        series_ticker: str | None = None,
        since: datetime | None = None,
        event_types: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 1000,
    ) -> list[WeatherBootstrapEventRecord]:
        stmt = select(WeatherBootstrapEventRecord).where(
            WeatherBootstrapEventRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if bucket_key:
            stmt = stmt.where(WeatherBootstrapEventRecord.bucket_key == bucket_key)
        if series_ticker:
            stmt = stmt.where(WeatherBootstrapEventRecord.series_ticker == series_ticker)
        if since is not None:
            stmt = stmt.where(WeatherBootstrapEventRecord.occurred_at >= since)
        if event_types:
            stmt = stmt.where(WeatherBootstrapEventRecord.event_type.in_(event_types))
        if statuses:
            stmt = stmt.where(WeatherBootstrapEventRecord.status.in_(statuses))
        stmt = stmt.order_by(WeatherBootstrapEventRecord.occurred_at.desc(), WeatherBootstrapEventRecord.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars().all())

    async def save_weather_bootstrap_historical_evidence(
        self,
        *,
        kalshi_env: str | None = None,
        market_ticker: str,
        replay_version: str,
        source_fingerprint: str,
        series_ticker: str | None = None,
        local_market_day: str | None = None,
        bucket_key: str | None = None,
        policy_key: str | None = None,
        tier: str | None = None,
        strict_replay: bool = True,
        side: str | None = None,
        confidence: float | None = None,
        edge_bps: int | None = None,
        count_fp: Decimal | None = None,
        notional_dollars: Decimal | None = None,
        pnl_dollars: Decimal | None = None,
        evidence_weight: float = 1.0,
        outcome: str | None = None,
        observed_at: datetime | None = None,
        payload: dict[str, Any] | None = None,
    ) -> WeatherBootstrapHistoricalEvidenceRecord:
        record = WeatherBootstrapHistoricalEvidenceRecord(
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            market_ticker=market_ticker,
            series_ticker=series_ticker,
            local_market_day=local_market_day,
            bucket_key=bucket_key,
            policy_key=policy_key,
            tier=tier,
            replay_version=replay_version,
            source_fingerprint=source_fingerprint,
            strict_replay=strict_replay,
            side=side,
            confidence=confidence,
            edge_bps=edge_bps,
            count_fp=count_fp,
            notional_dollars=notional_dollars,
            pnl_dollars=pnl_dollars,
            evidence_weight=evidence_weight,
            outcome=outcome,
            observed_at=observed_at or datetime.now(UTC),
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_weather_bootstrap_historical_evidence(
        self,
        *,
        kalshi_env: str | None = None,
        source_fingerprint: str,
        market_ticker: str,
        bucket_key: str | None = None,
        policy_key: str | None = None,
    ) -> WeatherBootstrapHistoricalEvidenceRecord | None:
        stmt = select(WeatherBootstrapHistoricalEvidenceRecord).where(
            WeatherBootstrapHistoricalEvidenceRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env),
            WeatherBootstrapHistoricalEvidenceRecord.source_fingerprint == source_fingerprint,
            WeatherBootstrapHistoricalEvidenceRecord.market_ticker == market_ticker,
            WeatherBootstrapHistoricalEvidenceRecord.policy_key == policy_key,
        )
        if bucket_key is None:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.bucket_key.is_(None))
        else:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.bucket_key == bucket_key)
        return (await self.session.execute(stmt.limit(1))).scalar_one_or_none()

    async def list_weather_bootstrap_historical_evidence(
        self,
        *,
        kalshi_env: str | None = None,
        bucket_key: str | None = None,
        series_ticker: str | None = None,
        strict_replay: bool | None = True,
        limit: int = 1000,
    ) -> list[WeatherBootstrapHistoricalEvidenceRecord]:
        stmt = select(WeatherBootstrapHistoricalEvidenceRecord).where(
            WeatherBootstrapHistoricalEvidenceRecord.kalshi_env == self._resolved_kalshi_env(kalshi_env)
        )
        if bucket_key:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.bucket_key == bucket_key)
        if series_ticker:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.series_ticker == series_ticker)
        if strict_replay is not None:
            stmt = stmt.where(WeatherBootstrapHistoricalEvidenceRecord.strict_replay.is_(strict_replay))
        stmt = stmt.order_by(
            WeatherBootstrapHistoricalEvidenceRecord.observed_at.desc(),
            WeatherBootstrapHistoricalEvidenceRecord.created_at.desc(),
        ).limit(limit)
        return list((await self.session.execute(stmt)).scalars().all())

    async def get_latest_risk_verdict_for_room(self, room_id: str) -> RiskVerdictRecord | None:
        stmt = (
            select(RiskVerdictRecord)
            .where(RiskVerdictRecord.room_id == room_id)
            .order_by(RiskVerdictRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def save_risk_verdict(
        self,
        *,
        room_id: str,
        ticket_id: str,
        status: RiskStatus,
        reasons: list[str],
        approved_notional_dollars: Decimal | None,
        approved_count_fp: Decimal | None,
        payload: dict[str, Any],
    ) -> RiskVerdictRecord:
        record = RiskVerdictRecord(
            room_id=room_id,
            ticket_id=ticket_id,
            status=status.value,
            reasons=reasons,
            approved_notional_dollars=approved_notional_dollars,
            approved_count_fp=approved_count_fp,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def save_order(
        self,
        *,
        ticket_id: str | None,
        client_order_id: str,
        market_ticker: str,
        status: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        kalshi_order_id: str | None = None,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> OrderRecord:
        return await self.upsert_order(
            ticket_id=ticket_id,
            client_order_id=client_order_id,
            market_ticker=market_ticker,
            status=status,
            side=side,
            action=action,
            yes_price_dollars=yes_price_dollars,
            count_fp=count_fp,
            raw=raw,
            kalshi_order_id=kalshi_order_id,
            kalshi_env=kalshi_env,
            strategy_code=strategy_code,
        )

    async def _resolve_strategy_code_for_order(
        self,
        *,
        strategy_code: str | None,
        ticket_id: str | None,
        client_order_id: str | None,
    ) -> str | None:
        """Strategy code flows from the ticket if caller didn't specify one."""
        prefix_strategy = _strategy_code_for_client_order_prefix(client_order_id)
        if prefix_strategy is not None and (strategy_code is None or strategy_code in _GENERIC_CRYPTO_STRATEGY_CODES):
            return prefix_strategy
        if strategy_code is not None:
            return strategy_code
        if ticket_id is not None:
            stmt = select(TradeTicketRecord.strategy_code).where(TradeTicketRecord.id == ticket_id)
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        if client_order_id is not None:
            stmt = select(TradeTicketRecord.strategy_code).where(
                TradeTicketRecord.client_order_id.in_(_candidate_trade_ticket_client_order_ids(client_order_id))
            )
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        return None

    async def _resolve_ticket_id_for_order(
        self,
        *,
        ticket_id: str | None,
        client_order_id: str | None,
    ) -> str | None:
        if ticket_id is not None:
            return ticket_id
        if not client_order_id:
            return None
        stmt = select(TradeTicketRecord.id).where(
            TradeTicketRecord.client_order_id.in_(_candidate_trade_ticket_client_order_ids(client_order_id))
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    @staticmethod
    def _ticket_status_rank(status: str | None) -> int:
        normalized = str(status or "").strip().lower()
        if normalized in {"filled", "executed"}:
            return 50
        if (
            normalized
            in {
                "blocked",
                "canceled",
                "cancelled",
                "expired",
                "lock_denied",
                "order_id_missing",
                "partially_filled_cancelled",
                "rejected",
                "requote_edge_lost",
                "unfilled_cancelled",
                "write_credentials_missing",
            }
            or normalized.startswith("rejected_")
        ):
            return 40
        if normalized in {"submitted", "resting", "accepted", "open", "pending"}:
            return 20
        if normalized == "approved":
            return 10
        if normalized == "proposed":
            return 0
        return 20

    async def _sync_trade_ticket_status_from_order(self, ticket_id: str | None, status: str) -> None:
        if ticket_id is None:
            return
        ticket = await self.session.get(TradeTicketRecord, ticket_id)
        if ticket is None:
            return
        if self._ticket_status_rank(status) >= self._ticket_status_rank(ticket.status):
            ticket.status = status
            await self.session.flush()

    async def upsert_order(
        self,
        *,
        client_order_id: str,
        market_ticker: str,
        status: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        ticket_id: str | None = None,
        kalshi_order_id: str | None = None,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> OrderRecord:
        from kalshi_bot.db.models import OrderRecord as _OR
        resolved_ticket_id = await self._resolve_ticket_id_for_order(
            ticket_id=ticket_id,
            client_order_id=client_order_id,
        )
        resolved_strategy = await self._resolve_strategy_code_for_order(
            strategy_code=strategy_code,
            ticket_id=resolved_ticket_id,
            client_order_id=client_order_id,
        )
        record_id = str(uuid4())
        now = datetime.now(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        insert_values = {
            "id": record_id,
            "trade_ticket_id": resolved_ticket_id,
            "client_order_id": client_order_id,
            "kalshi_env": env,
            "market_ticker": market_ticker,
            "status": status,
            "side": side,
            "action": action,
            "yes_price_dollars": yes_price_dollars,
            "count_fp": count_fp,
            "strategy_code": resolved_strategy,
            "raw": raw,
            "kalshi_order_id": kalshi_order_id,
            "created_at": now,
            "updated_at": now,
        }
        update_values = {
            "status": status,
            "market_ticker": market_ticker,
            "side": side,
            "action": action,
            "yes_price_dollars": yes_price_dollars,
            "count_fp": count_fp,
            "raw": raw,
            "updated_at": now,
        }
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(_OR).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(_OR).values(**insert_values)
        else:
            # fallback: SELECT then mutate
            existing = (
                await self.session.execute(
                    select(_OR).where(
                        _OR.kalshi_env == env,
                        _OR.client_order_id == client_order_id,
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                existing = _OR(**insert_values)
                self.session.add(existing)
            else:
                for k, v in update_values.items():
                    setattr(existing, k, v)
                if resolved_ticket_id and not existing.trade_ticket_id:
                    existing.trade_ticket_id = resolved_ticket_id
                if kalshi_order_id and not existing.kalshi_order_id:
                    existing.kalshi_order_id = kalshi_order_id
                if resolved_strategy and not existing.strategy_code:
                    existing.strategy_code = resolved_strategy
            await self.session.flush()
            await self._sync_trade_ticket_status_from_order(existing.trade_ticket_id, existing.status)
            return existing

        # COALESCE lets later, richer execution records repair placeholder rows
        # inserted first by websocket/reconcile without overwriting with NULL.
        coalesce_ticket_id = func.coalesce(stmt.excluded.trade_ticket_id, _OR.trade_ticket_id)
        coalesce_kalshi_id = func.coalesce(stmt.excluded.kalshi_order_id, _OR.kalshi_order_id)
        coalesce_strategy = func.coalesce(stmt.excluded.strategy_code, _OR.strategy_code)
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=["kalshi_env", "client_order_id"],
                set_={
                    **update_values,
                    "trade_ticket_id": coalesce_ticket_id,
                    "kalshi_order_id": coalesce_kalshi_id,
                    "strategy_code": coalesce_strategy,
                },
            )
        )
        await self.session.flush()
        result = (
            await self.session.execute(
                select(_OR).where(
                    _OR.kalshi_env == env,
                    _OR.client_order_id == client_order_id,
                )
            )
        ).scalar_one()
        await self._sync_trade_ticket_status_from_order(result.trade_ticket_id, result.status)
        return result

    async def list_orders_for_room(self, room_id: str) -> list[OrderRecord]:
        stmt = (
            select(OrderRecord)
            .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(OrderRecord.created_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_orders_for_markets(
        self,
        market_tickers: list[str],
        *,
        kalshi_env: str | None = None,
    ) -> list[OrderRecord]:
        if not market_tickers:
            return []
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(OrderRecord)
            .where(
                OrderRecord.kalshi_env == env,
                OrderRecord.market_ticker.in_(market_tickers),
            )
            .order_by(OrderRecord.created_at.desc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def save_fill(
        self,
        *,
        market_ticker: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        order_id: str | None = None,
        trade_id: str | None = None,
        is_taker: bool = True,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> FillRecord:
        env = self._resolved_kalshi_env(kalshi_env)
        raw_order_id = raw.get("order_id") if isinstance(raw, dict) else None
        resolved_order_id, resolved_strategy, resolved_side = await self._resolve_fill_links(
            strategy_code=strategy_code,
            order_id=order_id,
            kalshi_order_id=raw_order_id,
            kalshi_env=env,
            market_ticker=market_ticker,
            side=side,
            action=action,
        )
        economic_side = resolved_side or side
        fill_observed_at = datetime.now(UTC)
        decision_context = await self._resolve_fill_decision_context(
            order_id=resolved_order_id,
            market_ticker=market_ticker,
            kalshi_env=env,
            before=fill_observed_at,
        )
        decision_lineage = decision_context.pop("_decision_lineage", {})
        record = FillRecord(
            order_id=resolved_order_id,
            trade_id=trade_id,
            kalshi_env=env,
            market_ticker=market_ticker,
            side=economic_side,
            action=action,
            yes_price_dollars=yes_price_dollars,
            count_fp=count_fp,
            strategy_code=resolved_strategy,
            raw=self._raw_with_decision_lineage(raw, decision_lineage),
            is_taker=is_taker,
            **decision_context,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def _resolve_order_for_fill(
        self,
        *,
        order_id: str | None,
        kalshi_order_id: str | None,
        kalshi_env: str | None,
    ) -> OrderRecord | None:
        if order_id is not None:
            stmt = select(OrderRecord).where(OrderRecord.id == order_id)
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        if kalshi_order_id is not None and kalshi_env is not None:
            attribution_rank = case(
                (
                    OrderRecord.strategy_code.is_not(None)
                    & OrderRecord.trade_ticket_id.is_not(None),
                    0,
                ),
                (OrderRecord.strategy_code.is_not(None), 1),
                (OrderRecord.trade_ticket_id.is_not(None), 2),
                else_=3,
            )
            stmt = select(OrderRecord).where(
                OrderRecord.kalshi_env == kalshi_env,
                OrderRecord.kalshi_order_id == kalshi_order_id,
            ).order_by(attribution_rank, OrderRecord.updated_at.desc(), OrderRecord.created_at.desc()).limit(1)
            found = (await self.session.execute(stmt)).scalar_one_or_none()
            if found is not None:
                return found
        return None

    async def _latest_attributed_buy_fill(
        self,
        *,
        market_ticker: str,
        side: str,
        kalshi_env: str,
        before: datetime | None = None,
    ) -> FillRecord | None:
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == kalshi_env,
            FillRecord.market_ticker == market_ticker,
            FillRecord.side == side,
            FillRecord.action == "buy",
            FillRecord.strategy_code.is_not(None),
        )
        if before is not None:
            stmt = stmt.where(FillRecord.created_at <= before)
        stmt = stmt.order_by(FillRecord.created_at.desc()).limit(1)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_latest_fill_strategy_for_market_side(
        self,
        *,
        market_ticker: str,
        side: str,
        kalshi_env: str | None = None,
        before: datetime | None = None,
    ) -> str | None:
        found = await self._latest_attributed_buy_fill(
            market_ticker=market_ticker,
            side=side,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            before=before,
        )
        return found.strategy_code if found is not None else None

    async def _resolve_fill_links(
        self,
        *,
        strategy_code: str | None,
        order_id: str | None,
        kalshi_order_id: str | None,
        kalshi_env: str,
        market_ticker: str,
        side: str,
        action: str,
        before: datetime | None = None,
    ) -> tuple[str | None, str | None, str | None]:
        """Return (order_id, strategy_code, economic_side) for a fill using bounded evidence."""
        if strategy_code is not None:
            matched = await self._resolve_order_for_fill(
                order_id=order_id,
                kalshi_order_id=kalshi_order_id,
                kalshi_env=kalshi_env,
            )
            if matched is not None and matched.strategy_code and strategy_code in _GENERIC_CRYPTO_STRATEGY_CODES:
                return matched.id, matched.strategy_code, matched.side
            return (
                order_id or (matched.id if matched is not None else None),
                strategy_code,
                matched.side if matched is not None else None,
            )

        matched_order = await self._resolve_order_for_fill(
            order_id=order_id,
            kalshi_order_id=kalshi_order_id,
            kalshi_env=kalshi_env,
        )
        if matched_order is not None:
            matched_strategy = matched_order.strategy_code
            if matched_strategy is None:
                matched_strategy = await self._resolve_strategy_code_for_order(
                    strategy_code=None,
                    ticket_id=matched_order.trade_ticket_id,
                    client_order_id=matched_order.client_order_id,
                )
                if matched_strategy is not None:
                    matched_order.strategy_code = matched_strategy
            return matched_order.id, matched_strategy, matched_order.side

        if action == "sell":
            latest_buy = await self._latest_attributed_buy_fill(
                market_ticker=market_ticker,
                side=side,
                kalshi_env=kalshi_env,
                before=before,
            )
            if latest_buy is not None:
                return order_id, latest_buy.strategy_code, latest_buy.side

        return order_id, None, None

    @staticmethod
    def _decision_context_from_signal(signal: Signal, *, decision_price: Decimal | None = None) -> dict[str, Any]:
        context: dict[str, Any] = {
            "decision_edge_bps": signal.edge_bps,
            "decision_confidence": signal.confidence,
            "decision_spread_bps": None,
            "decision_fair_yes": signal.fair_yes_dollars,
            "decision_price": decision_price,
            "decision_ts": signal.created_at,
        }
        payload = signal.payload if isinstance(signal.payload, dict) else {}
        spread = payload.get("spread_bps")
        if spread in (None, ""):
            candidate = payload.get("candidate_trace")
            if isinstance(candidate, dict):
                spread = candidate.get("spread_bps")
        if spread not in (None, ""):
            try:
                context["decision_spread_bps"] = int(spread)
            except (TypeError, ValueError):
                context["decision_spread_bps"] = None
        return context

    @staticmethod
    def _apply_raw_decision_lineage(context: dict[str, Any], lineage: dict[str, Any]) -> None:
        field_map = {
            "decision_edge_bps": ("decision_edge_bps", "edge_bps", "expected_net_edge_bps"),
            "decision_confidence": ("decision_confidence", "confidence"),
            "decision_spread_bps": ("decision_spread_bps", "spread_bps"),
            "decision_fair_yes": ("decision_fair_yes", "fair_yes_dollars"),
            "decision_price": ("decision_price", "target_yes_price_dollars", "selected_price_dollars"),
            "decision_ts": ("decision_ts", "decision_time", "signal_created_at"),
        }
        for field, keys in field_map.items():
            if context.get(field) not in (None, ""):
                continue
            for key in keys:
                value = lineage.get(key)
                if value in (None, ""):
                    continue
                try:
                    if field in {"decision_edge_bps", "decision_spread_bps"}:
                        context[field] = int(value)
                    elif field == "decision_confidence":
                        context[field] = float(value)
                    elif field == "decision_ts":
                        context[field] = _as_utc_datetime(datetime.fromisoformat(str(value)))
                    else:
                        context[field] = _quantize_money(value)
                    break
                except Exception:
                    continue

    @staticmethod
    def _raw_with_decision_lineage(raw: dict[str, Any], lineage: dict[str, Any]) -> dict[str, Any]:
        if not lineage:
            return raw
        existing = raw.get("decision_lineage") if isinstance(raw, dict) else None
        merged = {**(existing if isinstance(existing, dict) else {}), **lineage}
        return {**raw, "decision_lineage": merged}

    def _lineage_from_context(
        self,
        context: dict[str, Any],
        *,
        source: str,
        order: OrderRecord | None = None,
        signal: Signal | None = None,
    ) -> dict[str, Any]:
        lineage = {
            "source": source,
            "decision_edge_bps": context.get("decision_edge_bps"),
            "decision_confidence": context.get("decision_confidence"),
            "decision_spread_bps": context.get("decision_spread_bps"),
            "decision_fair_yes": str(context["decision_fair_yes"]) if context.get("decision_fair_yes") is not None else None,
            "decision_price": str(context["decision_price"]) if context.get("decision_price") is not None else None,
            "decision_time": context["decision_ts"].isoformat() if context.get("decision_ts") is not None else None,
        }
        if order is not None:
            lineage.update(
                {
                    "order_id": order.id,
                    "kalshi_order_id": order.kalshi_order_id,
                    "client_order_id": order.client_order_id,
                    "trade_ticket_id": order.trade_ticket_id,
                    "order_strategy_code": order.strategy_code,
                }
            )
            order_raw = order.raw if isinstance(order.raw, dict) else {}
            raw_lineage = order_raw.get("decision_lineage")
            if isinstance(raw_lineage, dict):
                lineage.update(raw_lineage)
        if signal is not None:
            lineage.update({"signal_id": signal.id, "signal_room_id": signal.room_id})
            payload = signal.payload if isinstance(signal.payload, dict) else {}
            crypto_modeling = payload.get("crypto_modeling") if isinstance(payload.get("crypto_modeling"), dict) else {}
            candidate_trace = payload.get("candidate_trace") if isinstance(payload.get("candidate_trace"), dict) else {}
            lineage.update(
                {
                    "model_version": crypto_modeling.get("model_version") or candidate_trace.get("model_version"),
                    "backtest_version": crypto_modeling.get("backtest_version"),
                    "replay_gate_status": crypto_modeling.get("replay_gate_status"),
                    "selected_side": candidate_trace.get("selected_side") or payload.get("recommended_side"),
                    "bucket_key": candidate_trace.get("bucket_key"),
                    "candidate_status": candidate_trace.get("candidate_status"),
                }
            )
        return {key: value for key, value in lineage.items() if value not in (None, "")}

    async def _resolve_fill_decision_context(
        self,
        *,
        order_id: str | None,
        market_ticker: str | None = None,
        kalshi_env: str | None = None,
        before: datetime | None = None,
    ) -> dict[str, Any]:
        """Resolve decision-time lineage for a fill.

        Joins order_id -> OrderRecord -> trade_ticket_id -> TradeTicketRecord.room_id
        -> latest Signal for that room. Returns the six decision_* fields, all None
        when no order/ticket/signal can be resolved (never raises). The private
        ``_decision_lineage`` key is stripped before model persistence and copied
        into FillRecord.raw for richer audit/reporting.
        """
        context: dict[str, Any] = {
            "decision_edge_bps": None,
            "decision_confidence": None,
            "decision_spread_bps": None,
            "decision_fair_yes": None,
            "decision_price": None,
            "decision_ts": None,
        }
        order: OrderRecord | None = None
        signal: Signal | None = None
        lineage_source = "unresolved"
        if order_id is not None:
            order = (
                await self.session.execute(select(OrderRecord).where(OrderRecord.id == order_id))
            ).scalar_one_or_none()
        if order is not None:
            context["decision_price"] = order.yes_price_dollars
            order_raw = order.raw if isinstance(order.raw, dict) else {}
            raw_lineage = order_raw.get("decision_lineage")
            if isinstance(raw_lineage, dict):
                self._apply_raw_decision_lineage(context, raw_lineage)
                lineage_source = "order_raw_decision_lineage"
            if order.trade_ticket_id is not None:
                ticket = (
                    await self.session.execute(
                        select(TradeTicketRecord).where(TradeTicketRecord.id == order.trade_ticket_id)
                    )
                ).scalar_one_or_none()
                if ticket is not None:
                    signal = await self.get_latest_signal_for_room(ticket.room_id)
        if signal is None and market_ticker is not None:
            signal = await self.get_latest_signal_for_market(
                market_ticker,
                kalshi_env=kalshi_env,
                before=before,
                max_age_seconds=30 * 60,
            )
            if signal is not None and lineage_source == "unresolved":
                lineage_source = "latest_market_signal_fallback"
        if signal is not None:
            signal_context = self._decision_context_from_signal(signal, decision_price=context.get("decision_price"))
            for key, value in signal_context.items():
                if context.get(key) in (None, "") and value not in (None, ""):
                    context[key] = value
            if lineage_source == "unresolved":
                lineage_source = "ticket_signal"
        context["_decision_lineage"] = self._lineage_from_context(
            context,
            source=lineage_source,
            order=order,
            signal=signal,
        )
        return context

    async def upsert_fill(
        self,
        *,
        market_ticker: str,
        side: str,
        action: str,
        yes_price_dollars: Decimal,
        count_fp: Decimal,
        raw: dict[str, Any],
        order_id: str | None = None,
        trade_id: str | None = None,
        is_taker: bool = True,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> FillRecord:
        env = self._resolved_kalshi_env(kalshi_env)
        raw_order_id = raw.get("order_id") if isinstance(raw, dict) else None
        resolved_order_id, resolved_strategy, resolved_side = await self._resolve_fill_links(
            strategy_code=strategy_code,
            order_id=order_id,
            kalshi_order_id=raw_order_id,
            kalshi_env=env,
            market_ticker=market_ticker,
            side=side,
            action=action,
        )
        economic_side = resolved_side or side
        fill_observed_at = datetime.now(UTC)
        decision_context = await self._resolve_fill_decision_context(
            order_id=resolved_order_id,
            market_ticker=market_ticker,
            kalshi_env=env,
            before=fill_observed_at,
        )
        decision_lineage = decision_context.pop("_decision_lineage", {})
        raw_with_lineage = self._raw_with_decision_lineage(raw, decision_lineage)
        if trade_id is not None:
            insert_values = {
                "id": str(uuid4()),
                "order_id": resolved_order_id,
                "trade_id": trade_id,
                "kalshi_env": env,
                "market_ticker": market_ticker,
                "side": economic_side,
                "action": action,
                "yes_price_dollars": yes_price_dollars,
                "count_fp": count_fp,
                "strategy_code": resolved_strategy,
                "raw": raw_with_lineage,
                "is_taker": is_taker,
                "created_at": fill_observed_at,
                "updated_at": fill_observed_at,
                **decision_context,
            }
            dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
            if dialect_name == "postgresql":
                stmt = pg_insert(FillRecord).values(**insert_values)
            elif dialect_name == "sqlite":
                stmt = sqlite_insert(FillRecord).values(**insert_values)
            else:
                stmt = None
            if stmt is not None:
                excluded = stmt.excluded
                await self.session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[FillRecord.kalshi_env, FillRecord.trade_id],
                        set_={
                            "order_id": func.coalesce(excluded.order_id, FillRecord.order_id),
                            "market_ticker": excluded.market_ticker,
                            "side": excluded.side,
                            "action": excluded.action,
                            "yes_price_dollars": excluded.yes_price_dollars,
                            "count_fp": excluded.count_fp,
                            "strategy_code": func.coalesce(excluded.strategy_code, FillRecord.strategy_code),
                            "raw": excluded.raw,
                            "is_taker": excluded.is_taker,
                            "updated_at": fill_observed_at,
                            "decision_edge_bps": func.coalesce(excluded.decision_edge_bps, FillRecord.decision_edge_bps),
                            "decision_confidence": func.coalesce(excluded.decision_confidence, FillRecord.decision_confidence),
                            "decision_spread_bps": func.coalesce(excluded.decision_spread_bps, FillRecord.decision_spread_bps),
                            "decision_fair_yes": func.coalesce(excluded.decision_fair_yes, FillRecord.decision_fair_yes),
                            "decision_price": func.coalesce(excluded.decision_price, FillRecord.decision_price),
                            "decision_ts": func.coalesce(excluded.decision_ts, FillRecord.decision_ts),
                        },
                    )
                )
                await self.session.flush()
                stmt = select(FillRecord).where(
                    FillRecord.kalshi_env == env,
                    FillRecord.trade_id == trade_id,
                )
                return (await self.session.execute(stmt)).scalar_one()

        record: FillRecord | None = None
        if trade_id is not None:
            stmt = select(FillRecord).where(
                FillRecord.kalshi_env == env,
                FillRecord.trade_id == trade_id,
            )
            record = (await self.session.execute(stmt)).scalar_one_or_none()
        if record is None:
            record = FillRecord(
                order_id=resolved_order_id,
                trade_id=trade_id,
                kalshi_env=env,
                market_ticker=market_ticker,
                side=economic_side,
                action=action,
                yes_price_dollars=yes_price_dollars,
                count_fp=count_fp,
                strategy_code=resolved_strategy,
                raw=raw_with_lineage,
                is_taker=is_taker,
                **decision_context,
            )
            self.session.add(record)
        else:
            record.order_id = resolved_order_id or record.order_id
            record.market_ticker = market_ticker
            record.side = economic_side
            record.action = action
            record.yes_price_dollars = yes_price_dollars
            record.count_fp = count_fp
            if resolved_strategy and not record.strategy_code:
                record.strategy_code = resolved_strategy
            record.raw = raw_with_lineage
            record.is_taker = is_taker
            # Coalesce: only fill in decision_* that are still NULL (don't clobber).
            for field, value in decision_context.items():
                if value is not None and getattr(record, field) is None:
                    setattr(record, field, value)
        await self.session.flush()
        return record

    async def repair_fill_attribution(
        self,
        *,
        kalshi_env: str | None = None,
        days: int = 7,
        limit: int = 500,
        dry_run: bool = True,
        strategy_code: str | None = None,
        market_prefix: str | None = None,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        cutoff = datetime.now(UTC) - timedelta(days=max(1, int(days)))
        stmt = (
            select(FillRecord)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.created_at >= cutoff,
                or_(FillRecord.strategy_code.is_(None), FillRecord.strategy_code == "", FillRecord.order_id.is_(None)),
            )
            .order_by(FillRecord.created_at.desc(), FillRecord.id.desc())
            .limit(max(1, int(limit)))
        )
        if market_prefix:
            stmt = stmt.where(FillRecord.market_ticker.like(f"{market_prefix}%"))
        fills = list((await self.session.execute(stmt)).scalars())
        repaired: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for fill in fills:
            raw = fill.raw if isinstance(fill.raw, dict) else {}
            raw_order_id = raw.get("order_id")
            resolved_order_id, resolved_strategy, resolved_side = await self._resolve_fill_links(
                strategy_code=None,
                order_id=None if raw_order_id else fill.order_id,
                kalshi_order_id=raw_order_id,
                kalshi_env=env,
                market_ticker=fill.market_ticker,
                side=fill.side,
                action=fill.action,
                before=fill.created_at,
            )
            if strategy_code and resolved_strategy != strategy_code:
                skipped.append(
                    {
                        "fill_id": fill.id,
                        "trade_id": fill.trade_id,
                        "reason": "resolved_strategy_mismatch",
                        "resolved_strategy_code": resolved_strategy,
                    }
                )
                continue
            if not resolved_order_id and not resolved_strategy:
                skipped.append(
                    {
                        "fill_id": fill.id,
                        "trade_id": fill.trade_id,
                        "reason": "no_attributed_order_match",
                        "kalshi_order_id": raw_order_id,
                    }
                )
                continue
            change = {
                "fill_id": fill.id,
                "trade_id": fill.trade_id,
                "market_ticker": fill.market_ticker,
                "old_order_id": fill.order_id,
                "new_order_id": resolved_order_id or fill.order_id,
                "old_strategy_code": fill.strategy_code,
                "new_strategy_code": resolved_strategy or fill.strategy_code,
                "old_side": fill.side,
                "new_side": resolved_side or fill.side,
            }
            if (
                change["old_order_id"] == change["new_order_id"]
                and change["old_strategy_code"] == change["new_strategy_code"]
                and change["old_side"] == change["new_side"]
            ):
                skipped.append({**change, "reason": "no_change"})
                continue
            repaired.append(change)
            if not dry_run:
                fill.order_id = resolved_order_id or fill.order_id
                fill.strategy_code = resolved_strategy or fill.strategy_code
                fill.side = resolved_side or fill.side
        if not dry_run:
            await self.session.flush()
        return {
            "kalshi_env": env,
            "dry_run": dry_run,
            "days": days,
            "scanned": len(fills),
            "repairable": len(repaired),
            "skipped": len(skipped),
            "changes": repaired,
            "skipped_samples": skipped[:20],
        }

    async def list_fills_for_room(self, room_id: str) -> list[FillRecord]:
        stmt = (
            select(FillRecord)
            .join(OrderRecord, FillRecord.order_id == OrderRecord.id)
            .join(TradeTicketRecord, OrderRecord.trade_ticket_id == TradeTicketRecord.id)
            .where(TradeTicketRecord.room_id == room_id)
            .order_by(FillRecord.created_at.asc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_fills_for_markets(
        self,
        market_tickers: list[str],
        *,
        kalshi_env: str | None = None,
    ) -> list[FillRecord]:
        if not market_tickers:
            return []
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(FillRecord)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.market_ticker.in_(market_tickers),
            )
            .order_by(FillRecord.created_at.desc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def settle_fills(self, settlements: list[dict[str, Any]], *, kalshi_env: str | None = None) -> int:
        """Mark fills as win/loss based on settlement results. Returns number of fills updated."""
        settled = 0
        env = self._resolved_kalshi_env(kalshi_env)
        for s in settlements:
            ticker = s.get("ticker") or s.get("market_ticker")
            result = s.get("market_result")
            if not ticker or result not in ("yes", "no"):
                continue
            stmt = select(FillRecord).where(
                FillRecord.kalshi_env == env,
                FillRecord.market_ticker == ticker,
                FillRecord.settlement_result.is_(None),
            )
            fills = list((await self.session.execute(stmt)).scalars())
            for fill in fills:
                fill.settlement_result = "win" if fill.side == result else "loss"
                settled += 1
        if settled:
            await self.session.flush()
        return settled

    async def sync_weather_bootstrap_settlement_events(self, *, kalshi_env: str | None = None) -> int:
        """Mirror settled bootstrap-attributed fills into bootstrap evidence events."""
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(FillRecord, WeatherBootstrapEventRecord)
            .join(OrderRecord, FillRecord.order_id == OrderRecord.id)
            .join(WeatherBootstrapEventRecord, WeatherBootstrapEventRecord.order_id == OrderRecord.id)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.settlement_result.is_not(None),
                WeatherBootstrapEventRecord.kalshi_env == env,
                WeatherBootstrapEventRecord.event_type == "order",
            )
            .order_by(FillRecord.created_at.asc(), WeatherBootstrapEventRecord.created_at.asc())
        )
        rows = list((await self.session.execute(stmt)).all())
        created = 0
        seen_fill_ids: set[str] = set()
        for fill, event in rows:
            if fill.id in seen_fill_ids:
                continue
            seen_fill_ids.add(fill.id)
            existing_stmt = select(WeatherBootstrapEventRecord.id).where(
                WeatherBootstrapEventRecord.kalshi_env == env,
                WeatherBootstrapEventRecord.event_type == "settlement",
                WeatherBootstrapEventRecord.fill_id == fill.id,
            ).limit(1)
            existing = (await self.session.execute(existing_stmt)).scalar_one_or_none()
            if existing is not None:
                continue
            pnl = _settled_buy_fill_pnl(fill)
            if pnl is None:
                continue
            status = "settled_win" if fill.settlement_result == "win" else "settled_loss"
            await self.save_weather_bootstrap_event(
                kalshi_env=env,
                market_ticker=fill.market_ticker,
                series_ticker=event.series_ticker,
                local_market_day=event.local_market_day,
                bucket_key=event.bucket_key,
                policy_key=event.policy_key,
                tier=event.tier,
                event_type="settlement",
                status=status,
                side=fill.side,
                confidence=event.confidence,
                edge_bps=event.edge_bps,
                size_factor=event.size_factor,
                count_fp=fill.count_fp,
                notional_dollars=event.notional_dollars,
                pnl_dollars=pnl,
                evidence_weight=1.0,
                source="live_settlement",
                occurred_at=datetime.now(UTC),
                room_id=event.room_id,
                order_id=event.order_id,
                fill_id=fill.id,
                payload={
                    **dict(event.payload or {}),
                    "settlement_result": fill.settlement_result,
                    "fill_id": fill.id,
                    "trade_id": fill.trade_id,
                    "pnl_dollars": str(pnl) if pnl is not None else None,
                },
            )
            created += 1
        if created:
            await self.session.flush()
        return created

    async def get_fill_win_rate_30d(
        self,
        *,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> dict[str, Any]:
        """Return 30-day rolling realized P&L metrics.

        A position is a win if:
        - It was sold (stop-loss or manual exit) at a better price than entry, OR
        - It was held to settlement and the market resolved on our side.
        Settlement-based result is used only when no sell fill exists for the ticker+side.

        When ``strategy_code`` is provided, only fills attributed to that strategy
        are counted. Fills with a NULL ``strategy_code`` are excluded from filtered
        queries (treat as unknown-attribution).

        Returned keys:
        - ``won_contracts``, ``total_contracts``: legacy count-weighted win/loss totals.
        - ``trade_count``, ``win_count``, ``loss_count``: per-trade counts (each
          buy fill = one trade observation, regardless of contract count).
        - ``avg_win_dollars``, ``avg_loss_dollars``: mean P&L of winning / losing
          trades, each weighted by contract count inside the trade. None when
          there are no trades of that kind.
        - ``stdev_dollars``: population stdev of per-trade P&L (unweighted).
          None when fewer than two trades.
        - ``sharpe_per_trade``: mean(p&l) / stdev(p&l) over the sample (P2-1
          rolling Sharpe proxy). None when stdev is zero or fewer than two trades.
        """
        cutoff = datetime.now(UTC) - timedelta(days=30)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == env,
            FillRecord.created_at >= cutoff,
        )
        if strategy_code is not None:
            stmt = stmt.where(FillRecord.strategy_code == strategy_code)
        all_fills = list((await self.session.execute(stmt)).scalars())

        return self._fill_pnl_metrics(all_fills)

    async def get_session_fill_pnl_summary(
        self,
        *,
        kalshi_env: str | None = None,
        pacific_date: str | None = None,
    ) -> dict[str, Any]:
        """Return scored fill P&L metrics for one Pacific trading day."""
        import zoneinfo
        from datetime import date as date_cls
        from datetime import time

        pacific_zone = zoneinfo.ZoneInfo("America/Los_Angeles")
        local_date = (
            date_cls.fromisoformat(pacific_date)
            if pacific_date is not None
            else datetime.now(pacific_zone).date()
        )
        start_local = datetime.combine(local_date, time.min, tzinfo=pacific_zone)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(UTC)
        end_utc = end_local.astimezone(UTC)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(FillRecord)
            .where(
                FillRecord.kalshi_env == env,
                FillRecord.created_at >= start_utc,
                FillRecord.created_at < end_utc,
            )
            .order_by(FillRecord.created_at.asc())
        )
        all_fills = list((await self.session.execute(stmt)).scalars())
        metrics = self._fill_pnl_metrics(all_fills)
        metrics.update(
            {
                "date": local_date.isoformat(),
                "window_start": start_utc.isoformat(),
                "window_end": end_utc.isoformat(),
                "fill_count": len(all_fills),
                "buy_fill_count": sum(1 for fill in all_fills if fill.action == "buy"),
                "sell_fill_count": sum(1 for fill in all_fills if fill.action == "sell"),
            }
        )
        return metrics

    async def get_strategy_city_fill_metrics_since(
        self,
        *,
        series_ticker: str,
        strategy_name: str,
        since: datetime,
        kalshi_env: str | None = None,
        strategy_code: str | None = None,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        effective_strategy_code = strategy_code if strategy_code is not None else strategy_name
        stmt = select(FillRecord).where(
            FillRecord.kalshi_env == env,
            FillRecord.created_at >= since,
            FillRecord.market_ticker.like(f"{series_ticker}%"),
            FillRecord.strategy_code == effective_strategy_code,
        )
        all_fills = list((await self.session.execute(stmt)).scalars())
        buys: dict[tuple[str, str], list[FillRecord]] = {}
        sells: dict[tuple[str, str], list[FillRecord]] = {}
        for fill in all_fills:
            key = (fill.market_ticker, fill.side)
            if fill.action == "buy":
                buys.setdefault(key, []).append(fill)
            elif fill.action == "sell":
                sells.setdefault(key, []).append(fill)

        trade_pnls: list[float] = []
        for key, buy_fills in buys.items():
            _ticker, side = key
            sell_fills = sells.get(key, [])
            avg_sell: float | None = None
            if sell_fills:
                sell_count = sum(float(s.count_fp) for s in sell_fills)
                if sell_count > 0:
                    avg_sell = sum(float(s.yes_price_dollars) * float(s.count_fp) for s in sell_fills) / sell_count
            for buy_fill in buy_fills:
                count = float(buy_fill.count_fp)
                buy_px = float(buy_fill.yes_price_dollars)
                pnl: float | None = None
                if avg_sell is not None:
                    pnl = (avg_sell - buy_px) * count if side == "yes" else (buy_px - avg_sell) * count
                elif buy_fill.settlement_result is not None:
                    won_leg = buy_fill.settlement_result == "win"
                    pnl = ((1.0 if won_leg else 0.0) - buy_px) * count if side == "yes" else ((1.0 if won_leg else 0.0) - (1.0 - buy_px)) * count
                if pnl is not None:
                    trade_pnls.append(pnl)

        fill_count = len(trade_pnls)
        win_count = len([pnl for pnl in trade_pnls if pnl > 0])
        total_pnl = sum(trade_pnls)
        return {
            "series_ticker": series_ticker,
            "strategy_name": strategy_name,
            "resolved_live_fills": fill_count,
            "win_count": win_count,
            "win_rate": (win_count / fill_count) if fill_count else None,
            "realized_pnl": total_pnl,
        }

    async def get_daily_realized_pnl_dollars_by_strategy(
        self,
        *,
        strategy_code: str,
        kalshi_env: str | None = None,
        now: datetime | None = None,
    ) -> Decimal:
        """Realized daily P&L for one strategy in the last 24 hours.

        Sells inside the window are matched against prior BUY fills for the same
        ticker/side so exits from older positions keep their cost basis. If an
        entry cannot be found, the sell contributes fees only instead of being
        counted as pure profit.
        """
        reference_time = now or datetime.now(UTC)
        cutoff = reference_time - timedelta(hours=24)
        env = self._resolved_kalshi_env(kalshi_env)
        fill_columns = (
            FillRecord.id,
            FillRecord.market_ticker,
            FillRecord.side,
            FillRecord.action,
            FillRecord.yes_price_dollars,
            FillRecord.count_fp,
            FillRecord.raw,
            FillRecord.is_taker,
            FillRecord.settlement_result,
            FillRecord.created_at,
        )
        stmt = select(*fill_columns).where(
            FillRecord.kalshi_env == env,
            FillRecord.strategy_code == strategy_code,
            FillRecord.created_at >= cutoff,
            FillRecord.created_at <= reference_time,
        )
        fills = list((await self.session.execute(stmt)).mappings())
        pnl = Decimal("0")

        sell_fills = [fill for fill in fills if fill["action"] == "sell"]
        sell_keys = {(fill["market_ticker"], fill["side"]) for fill in sell_fills}
        buys_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {key: [] for key in sell_keys}
        if sell_keys:
            latest_sell_time = max(
                (_as_utc_datetime(sell["created_at"], default=reference_time) for sell in sell_fills),
                default=reference_time,
            )
            buy_stmt = select(*fill_columns).where(
                FillRecord.kalshi_env == env,
                FillRecord.strategy_code == strategy_code,
                FillRecord.action == "buy",
                FillRecord.created_at <= latest_sell_time,
            )
            for buy in (await self.session.execute(buy_stmt)).mappings():
                key = (buy["market_ticker"], buy["side"])
                if key in buys_by_key:
                    buys_by_key[key].append(dict(buy))
            for buys in buys_by_key.values():
                buys.sort(
                    key=lambda fill: (
                        _as_utc_datetime(fill["created_at"]),
                        str(fill["id"]),
                    )
                )

        buy_remaining: dict[str, Decimal] = {
            str(buy["id"]): as_decimal(buy["count_fp"])
            for buys in buys_by_key.values()
            for buy in buys
        }
        matched_buy_counts: dict[str, Decimal] = {}
        for sell in sorted(
            sell_fills,
            key=lambda fill: (
                _as_utc_datetime(fill["created_at"]),
                str(fill["id"]),
            ),
        ):
            sell_remaining = as_decimal(sell["count_fp"])
            if sell_remaining <= Decimal("0"):
                continue
            sell_contract_price = _contract_price_from_yes_price(
                sell["side"],
                as_decimal(sell["yes_price_dollars"]),
            )
            for buy in buys_by_key.get((sell["market_ticker"], sell["side"]), []):
                if (
                    buy["created_at"] is not None
                    and sell["created_at"] is not None
                    and _as_utc_datetime(buy["created_at"]) > _as_utc_datetime(sell["created_at"])
                ):
                    continue
                buy_id = str(buy["id"])
                available = buy_remaining.get(buy_id, Decimal("0"))
                if available <= Decimal("0"):
                    continue
                matched = min(available, sell_remaining)
                buy_contract_price = _contract_price_from_yes_price(
                    buy["side"],
                    as_decimal(buy["yes_price_dollars"]),
                )
                pnl += sell_contract_price * matched
                pnl -= buy_contract_price * matched
                pnl -= _fill_fee_for_count_from_raw(buy["raw"], buy["count_fp"], matched)
                pnl -= _fill_fee_for_count_from_raw(sell["raw"], sell["count_fp"], matched)
                buy_remaining[buy_id] = available - matched
                matched_buy_counts[buy_id] = matched_buy_counts.get(buy_id, Decimal("0")) + matched
                sell_remaining -= matched
                if sell_remaining <= Decimal("0"):
                    break
            if sell_remaining > Decimal("0"):
                pnl -= _fill_fee_for_count_from_raw(sell["raw"], sell["count_fp"], sell_remaining)

        for buy in fills:
            if buy["action"] != "buy" or buy["settlement_result"] not in {"win", "loss"}:
                continue
            remaining_count = as_decimal(buy["count_fp"]) - matched_buy_counts.get(str(buy["id"]), Decimal("0"))
            if remaining_count <= Decimal("0"):
                continue
            contract_price = _contract_price_from_yes_price(
                buy["side"],
                as_decimal(buy["yes_price_dollars"]),
            )
            cost_total = contract_price * remaining_count
            buy_fee = _fill_fee_for_count_from_raw(buy["raw"], buy["count_fp"], remaining_count)
            if buy["settlement_result"] == "win":
                pnl += remaining_count - cost_total - buy_fee
            elif buy["settlement_result"] == "loss":
                pnl -= cost_total + buy_fee

        return pnl.quantize(Decimal("0.01"))

    async def get_daily_realized_pnl_dollars_by_strategy_asset(
        self,
        *,
        strategy_code: str,
        asset_symbol: str,
        kalshi_env: str | None = None,
        now: datetime | None = None,
    ) -> Decimal:
        reference_time = now or datetime.now(UTC)
        cutoff = reference_time - timedelta(hours=24)
        env = self._resolved_kalshi_env(kalshi_env)
        asset = str(asset_symbol or "").upper()
        stmt = select(
            FillRecord.market_ticker,
            FillRecord.side,
            FillRecord.action,
            FillRecord.yes_price_dollars,
            FillRecord.count_fp,
            FillRecord.raw,
            FillRecord.is_taker,
            FillRecord.settlement_result,
        ).where(
            FillRecord.kalshi_env == env,
            FillRecord.strategy_code == strategy_code,
            FillRecord.action == "buy",
            FillRecord.settlement_result.in_(["win", "loss"]),
            FillRecord.created_at >= cutoff,
            FillRecord.created_at <= reference_time,
            FillRecord.market_ticker.like(f"KX{asset}%"),
        )
        pnl = Decimal("0")
        for fill in (await self.session.execute(stmt)).mappings():
            fill_asset, _frequency = _crypto_market_identity(fill["market_ticker"])
            if fill_asset != asset:
                continue
            fill_pnl = _settled_buy_fill_pnl_from_values(
                side=fill["side"],
                action=fill["action"],
                yes_price_dollars=fill["yes_price_dollars"],
                count_fp=fill["count_fp"],
                settlement_result=fill["settlement_result"],
                raw=fill["raw"],
                is_taker=fill["is_taker"],
            )
            if fill_pnl is not None:
                pnl += fill_pnl
        return pnl.quantize(Decimal("0.01"))

    async def get_crypto_live_pnl_cell_stats(
        self,
        *,
        kalshi_env: str | None,
        strategy_code: str,
        asset_symbol: str,
        frequency: str,
        side: str,
        contract_price_dollars: Decimal,
        liquidity: str,
        lookback_days: int,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        asset = str(asset_symbol or "").upper()
        normalized_frequency = "1h" if str(frequency).lower() in {"1h", "1hr", "hour"} else "15m"
        target_bucket = _crypto_price_band(_quantize_money(contract_price_dollars))
        cutoff = datetime.now(UTC) - timedelta(days=max(1, int(lookback_days)))
        stmt = select(
            FillRecord.market_ticker,
            FillRecord.side,
            FillRecord.action,
            FillRecord.yes_price_dollars,
            FillRecord.count_fp,
            FillRecord.raw,
            FillRecord.settlement_result,
            FillRecord.created_at,
            FillRecord.is_taker,
        ).where(
            FillRecord.kalshi_env == env,
            FillRecord.strategy_code == strategy_code,
            FillRecord.action == "buy",
            FillRecord.side == side,
            FillRecord.settlement_result.in_(["win", "loss"]),
            FillRecord.created_at >= cutoff,
            FillRecord.market_ticker.like(f"KX{asset}%"),
        )
        normalized_liquidity = str(liquidity or "any").strip().lower()
        if normalized_liquidity == "maker":
            stmt = stmt.where(FillRecord.is_taker.is_(False))
        elif normalized_liquidity == "taker":
            stmt = stmt.where(FillRecord.is_taker.is_(True))
        fills = list((await self.session.execute(stmt)).mappings())
        gross = Decimal("0")
        pnl = Decimal("0")
        fees = Decimal("0")
        missing_fee_count = 0
        estimated_fee_count = 0
        fee_sources: Counter[str] = Counter()
        contracts = Decimal("0")
        fill_count = 0
        wins = 0
        losses = 0
        latest_fill_at: datetime | None = None
        for fill in fills:
            fill_asset, fill_frequency = _crypto_market_identity(fill["market_ticker"])
            if fill_asset != asset or fill_frequency != normalized_frequency:
                continue
            contract_price = _contract_price_from_yes_price(
                fill["side"],
                as_decimal(fill["yes_price_dollars"]),
            )
            if _crypto_price_band(contract_price) != target_bucket:
                continue
            economics = _settled_buy_fill_economics_from_values(
                side=fill["side"],
                action=fill["action"],
                yes_price_dollars=fill["yes_price_dollars"],
                count_fp=fill["count_fp"],
                settlement_result=fill["settlement_result"],
                raw=fill["raw"],
                is_taker=fill["is_taker"],
            )
            if economics is None:
                continue
            fill_count += 1
            contracts += as_decimal(fill["count_fp"])
            gross += as_decimal(economics["gross_pnl_dollars"])
            fees += as_decimal(economics["fees_dollars"])
            pnl += as_decimal(economics["net_pnl_dollars"])
            fee_sources[str(economics["fee_source"])] += 1
            if economics["fee_missing"]:
                missing_fee_count += 1
            if economics["fee_estimated"]:
                estimated_fee_count += 1
            if fill["settlement_result"] == "win":
                wins += 1
            else:
                losses += 1
            if fill["created_at"] is not None:
                created_at = _as_utc_datetime(fill["created_at"])
                latest_fill_at = max(latest_fill_at or created_at, created_at)
        pnl_per_contract = pnl / contracts if contracts > Decimal("0") else Decimal("0")
        return {
            "kalshi_env": env,
            "strategy_code": strategy_code,
            "asset_symbol": asset,
            "frequency": normalized_frequency,
            "side": side,
            "liquidity": normalized_liquidity,
            "price_bucket": target_bucket,
            "lookback_days": int(lookback_days),
            "fill_count": fill_count,
            "contracts": str(contracts.quantize(Decimal("0.01"))),
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / fill_count) if fill_count else None,
            "gross_pnl_dollars": str(gross.quantize(Decimal("0.0001"))),
            "net_pnl_dollars": str(pnl.quantize(Decimal("0.0001"))),
            "fees_dollars": str(fees.quantize(Decimal("0.0001"))),
            "missing_fee_count": missing_fee_count,
            "estimated_fee_count": estimated_fee_count,
            "fee_sources": dict(sorted(fee_sources.items())),
            "pnl_per_contract_dollars": str(pnl_per_contract.quantize(Decimal("0.0001"))),
            "latest_fill_at": latest_fill_at.isoformat() if latest_fill_at is not None else None,
        }

    async def build_crypto_pnl_attribution_report(
        self,
        *,
        kalshi_env: str | None,
        days: int,
        frequency: str | None = None,
        asset_symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        cutoff = datetime.now(UTC) - timedelta(days=max(1, int(days)))
        assets = {str(asset).upper() for asset in (asset_symbols or []) if str(asset or "").strip()}
        normalized_frequency = None
        if frequency:
            normalized_frequency = "1h" if str(frequency).lower() in {"1h", "1hr", "hour"} else "15m"
        stmt = select(
            FillRecord.market_ticker,
            FillRecord.side,
            FillRecord.action,
            FillRecord.yes_price_dollars,
            FillRecord.count_fp,
            FillRecord.raw,
            FillRecord.settlement_result,
            FillRecord.is_taker,
        ).where(
            FillRecord.kalshi_env == env,
            FillRecord.action == "buy",
            FillRecord.settlement_result.in_(["win", "loss"]),
            FillRecord.created_at >= cutoff,
        )
        if normalized_frequency == "15m":
            stmt = stmt.where(FillRecord.strategy_code == "CRYPTO_15M")
        elif normalized_frequency == "1h":
            stmt = stmt.where(FillRecord.strategy_code == "CRYPTO_1H")
        else:
            stmt = stmt.where(FillRecord.strategy_code.in_(["CRYPTO_15M", "CRYPTO_1H"]))
        totals = {
            "fills": 0,
            "contracts": Decimal("0"),
            "gross_pnl": Decimal("0"),
            "net_pnl": Decimal("0"),
            "fees": Decimal("0"),
            "missing_decision_lineage": 0,
            "missing_fee_count": 0,
            "estimated_fee_count": 0,
            "fee_sources": Counter(),
        }
        by_cell: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        by_market: dict[str, dict[str, Any]] = {}

        def add_row(bucket: dict[str, Any], fill: dict[str, Any], economics: dict[str, Decimal | str | bool]) -> None:
            bucket["fills"] += 1
            bucket["contracts"] += as_decimal(fill["count_fp"])
            bucket["gross_pnl"] += as_decimal(economics["gross_pnl_dollars"])
            bucket["net_pnl"] += as_decimal(economics["net_pnl_dollars"])
            bucket["fees"] += as_decimal(economics["fees_dollars"])
            bucket["fee_sources"][str(economics["fee_source"])] += 1
            if economics["fee_missing"]:
                bucket["missing_fee_count"] += 1
            if economics["fee_estimated"]:
                bucket["estimated_fee_count"] += 1
            if fill["settlement_result"] == "win":
                bucket["wins"] += 1
            else:
                bucket["losses"] += 1

        for row in (await self.session.execute(stmt)).mappings():
            fill = dict(row)
            asset, fill_frequency = _crypto_market_identity(fill["market_ticker"])
            if asset is None or fill_frequency is None:
                continue
            if assets and asset not in assets:
                continue
            if normalized_frequency and fill_frequency != normalized_frequency:
                continue
            economics = _settled_buy_fill_economics_from_values(
                side=fill["side"],
                action=fill["action"],
                yes_price_dollars=fill["yes_price_dollars"],
                count_fp=fill["count_fp"],
                settlement_result=fill["settlement_result"],
                raw=fill["raw"],
                is_taker=fill["is_taker"],
            )
            if economics is None:
                continue
            side_price = _contract_price_from_yes_price(fill["side"], as_decimal(fill["yes_price_dollars"]))
            liquidity = "taker" if fill["is_taker"] else "maker"
            price_bucket = _crypto_price_band(side_price)
            side = str(fill["side"] or "").lower()
            key = (asset, fill_frequency, side, liquidity, price_bucket)
            cell = by_cell.setdefault(
                key,
                {
                    "asset_symbol": asset,
                    "frequency": fill_frequency,
                    "side": side,
                    "liquidity": liquidity,
                    "price_bucket": price_bucket,
                    "fills": 0,
                    "contracts": Decimal("0"),
                    "gross_pnl": Decimal("0"),
                    "net_pnl": Decimal("0"),
                    "fees": Decimal("0"),
                    "missing_fee_count": 0,
                    "estimated_fee_count": 0,
                    "fee_sources": Counter(),
                    "wins": 0,
                    "losses": 0,
                },
            )
            market = by_market.setdefault(
                fill["market_ticker"],
                {
                    "market_ticker": fill["market_ticker"],
                    "asset_symbol": asset,
                    "frequency": fill_frequency,
                    "fills": 0,
                    "contracts": Decimal("0"),
                    "gross_pnl": Decimal("0"),
                    "net_pnl": Decimal("0"),
                    "fees": Decimal("0"),
                    "missing_fee_count": 0,
                    "estimated_fee_count": 0,
                    "fee_sources": Counter(),
                    "wins": 0,
                    "losses": 0,
                },
            )
            add_row(cell, fill, economics)
            add_row(market, fill, economics)
            totals["fills"] += 1
            totals["contracts"] += as_decimal(fill["count_fp"])
            totals["gross_pnl"] += as_decimal(economics["gross_pnl_dollars"])
            totals["net_pnl"] += as_decimal(economics["net_pnl_dollars"])
            totals["fees"] += as_decimal(economics["fees_dollars"])
            totals["fee_sources"][str(economics["fee_source"])] += 1
            if economics["fee_missing"]:
                totals["missing_fee_count"] += 1
            if economics["fee_estimated"]:
                totals["estimated_fee_count"] += 1
            if not _raw_has_decision_lineage(fill["raw"]):
                totals["missing_decision_lineage"] += 1

        def finalize(row: dict[str, Any]) -> dict[str, Any]:
            contracts = row["contracts"]
            fills = int(row["fills"])
            win_rate = (row["wins"] / fills) if fills else None
            pnl_per_contract = row["net_pnl"] / contracts if contracts > Decimal("0") else Decimal("0")
            return {
                **{
                    key: value
                    for key, value in row.items()
                    if key not in {"contracts", "gross_pnl", "net_pnl", "fees", "fee_sources"}
                },
                "contracts": str(contracts.quantize(Decimal("0.01"))),
                "gross_pnl_dollars": str(row["gross_pnl"].quantize(Decimal("0.0001"))),
                "net_pnl_dollars": str(row["net_pnl"].quantize(Decimal("0.0001"))),
                "fees_dollars": str(row["fees"].quantize(Decimal("0.0001"))),
                "fee_sources": dict(sorted(row["fee_sources"].items())),
                "pnl_per_contract_dollars": str(pnl_per_contract.quantize(Decimal("0.0001"))),
                "win_rate": win_rate,
            }

        cells = [finalize(row) for row in by_cell.values()]
        cells.sort(key=lambda item: Decimal(str(item["net_pnl_dollars"])))
        worst_markets = [finalize(row) for row in by_market.values()]
        worst_markets.sort(key=lambda item: Decimal(str(item["net_pnl_dollars"])))
        return {
            "schema_version": "crypto-pnl-attribution-v1",
            "kalshi_env": env,
            "days": int(days),
            "frequency": normalized_frequency,
            "asset_symbols": sorted(assets),
            "primary_metric": "net_pnl_dollars",
            "win_rate_role": "diagnostic_only",
            "totals": {
                "fills": totals["fills"],
                "contracts": str(totals["contracts"].quantize(Decimal("0.01"))),
                "gross_pnl_dollars": str(totals["gross_pnl"].quantize(Decimal("0.0001"))),
                "net_pnl_dollars": str(totals["net_pnl"].quantize(Decimal("0.0001"))),
                "fees_dollars": str(totals["fees"].quantize(Decimal("0.0001"))),
                "missing_fee_count": totals["missing_fee_count"],
                "estimated_fee_count": totals["estimated_fee_count"],
                "fee_sources": dict(sorted(totals["fee_sources"].items())),
                "pnl_per_contract_dollars": str(
                    (
                        totals["net_pnl"] / totals["contracts"]
                        if totals["contracts"] > Decimal("0")
                        else Decimal("0")
                    ).quantize(Decimal("0.0001"))
                ),
                "missing_decision_lineage": totals["missing_decision_lineage"],
            },
            "cells": cells,
            "worst_cells": cells[:20],
            "worst_markets": worst_markets[:20],
        }

    async def build_crypto_maker_markout_report(
        self,
        *,
        kalshi_env: str | None,
        days: int,
        frequency: str | None = None,
        asset_symbols: list[str] | None = None,
        horizons_seconds: tuple[int, ...] = (60, 300, 900),
    ) -> dict[str, Any]:
        env = self._resolved_kalshi_env(kalshi_env)
        cutoff = datetime.now(UTC) - timedelta(days=max(1, int(days)))
        assets = {str(asset).upper() for asset in (asset_symbols or []) if str(asset or "").strip()}
        normalized_frequency = None
        if frequency:
            normalized_frequency = "1h" if str(frequency).lower() in {"1h", "1hr", "hour"} else "15m"
        horizons = tuple(sorted({max(1, int(value)) for value in horizons_seconds}))
        stmt = select(
            FillRecord.id,
            FillRecord.market_ticker,
            FillRecord.side,
            FillRecord.action,
            FillRecord.yes_price_dollars,
            FillRecord.count_fp,
            FillRecord.raw,
            FillRecord.settlement_result,
            FillRecord.created_at,
        ).where(
            FillRecord.kalshi_env == env,
            FillRecord.action == "buy",
            FillRecord.is_taker.is_(False),
            FillRecord.created_at >= cutoff,
        )
        if normalized_frequency == "15m":
            stmt = stmt.where(FillRecord.strategy_code == "CRYPTO_15M")
        elif normalized_frequency == "1h":
            stmt = stmt.where(FillRecord.strategy_code == "CRYPTO_1H")
        else:
            stmt = stmt.where(FillRecord.strategy_code.in_(["CRYPTO_15M", "CRYPTO_1H"]))
        raw_fills = [dict(row) for row in (await self.session.execute(stmt)).mappings()]
        fills: list[dict[str, Any]] = []
        for fill in raw_fills:
            asset, fill_frequency = _crypto_market_identity(fill["market_ticker"])
            if asset is None or fill_frequency is None:
                continue
            if assets and asset not in assets:
                continue
            if normalized_frequency and fill_frequency != normalized_frequency:
                continue
            fills.append({**fill, "asset_symbol": asset, "frequency": fill_frequency})
        if not fills:
            return {
                "schema_version": "crypto-maker-markout-v1",
                "kalshi_env": env,
                "days": int(days),
                "frequency": normalized_frequency,
                "asset_symbols": sorted(assets),
                "horizons_seconds": list(horizons),
                "shadow_only": True,
                "live_trading_change": False,
                "totals": {"fills": 0, "contracts": "0.00"},
                "cells": [],
            }

        tickers = sorted({str(fill["market_ticker"]) for fill in fills})
        min_fill_at = min(_as_utc_datetime(fill["created_at"]) for fill in fills)
        max_fill_at = max(_as_utc_datetime(fill["created_at"]) for fill in fills) + timedelta(seconds=max(horizons))
        snapshot_stmt = (
            select(
                CryptoMarketSnapshotRecord.market_ticker,
                CryptoMarketSnapshotRecord.observed_at,
                CryptoMarketSnapshotRecord.yes_bid_dollars,
                CryptoMarketSnapshotRecord.yes_ask_dollars,
                CryptoMarketSnapshotRecord.no_bid_dollars,
                CryptoMarketSnapshotRecord.no_ask_dollars,
            )
            .where(
                CryptoMarketSnapshotRecord.kalshi_env == env,
                CryptoMarketSnapshotRecord.market_ticker.in_(tickers),
                CryptoMarketSnapshotRecord.observed_at >= min_fill_at,
                CryptoMarketSnapshotRecord.observed_at <= max_fill_at,
            )
            .order_by(CryptoMarketSnapshotRecord.market_ticker, CryptoMarketSnapshotRecord.observed_at)
        )
        snapshots_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in (await self.session.execute(snapshot_stmt)).mappings():
            snapshots_by_ticker[str(row["market_ticker"])].append(dict(row))

        def first_snapshot_at_or_after(market_ticker: str, target: datetime) -> dict[str, Any] | None:
            for snapshot in snapshots_by_ticker.get(market_ticker, []):
                if _as_utc_datetime(snapshot["observed_at"]) >= target:
                    return snapshot
            return None

        totals = {
            "fills": 0,
            "contracts": Decimal("0"),
            "gross_pnl": Decimal("0"),
            "net_pnl": Decimal("0"),
            "fees": Decimal("0"),
            "missing_fee_count": 0,
            "missing_decision_lineage": 0,
            "markout": {str(seconds): {"count": 0, "sum": Decimal("0")} for seconds in horizons},
        }
        cells: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

        def make_bucket(fill: dict[str, Any], price_bucket: str, latency_bucket: str) -> dict[str, Any]:
            key = (fill["asset_symbol"], fill["frequency"], fill["side"], price_bucket, latency_bucket)
            return cells.setdefault(
                key,
                {
                    "asset_symbol": fill["asset_symbol"],
                    "frequency": fill["frequency"],
                    "side": fill["side"],
                    "price_bucket": price_bucket,
                    "fill_latency_bucket": latency_bucket,
                    "fills": 0,
                    "contracts": Decimal("0"),
                    "gross_pnl": Decimal("0"),
                    "net_pnl": Decimal("0"),
                    "fees": Decimal("0"),
                    "missing_fee_count": 0,
                    "missing_decision_lineage": 0,
                    "wins": 0,
                    "losses": 0,
                    "markout": {str(seconds): {"count": 0, "sum": Decimal("0")} for seconds in horizons},
                },
            )

        for fill in fills:
            fill_time = _as_utc_datetime(fill["created_at"])
            decision_time = _decision_time_from_raw(fill["raw"])
            latency_seconds = (fill_time - decision_time).total_seconds() if decision_time is not None else None
            if latency_seconds is not None and latency_seconds < 0:
                latency_seconds = None
            latency_bucket = _fill_latency_bucket(latency_seconds)
            side_price = _contract_price_from_yes_price(fill["side"], as_decimal(fill["yes_price_dollars"]))
            price_bucket = _crypto_price_band(side_price)
            bucket = make_bucket(fill, price_bucket, latency_bucket)
            economics = _settled_buy_fill_economics_from_values(
                side=fill["side"],
                action=fill["action"],
                yes_price_dollars=fill["yes_price_dollars"],
                count_fp=fill["count_fp"],
                settlement_result=fill["settlement_result"],
                raw=fill["raw"],
                is_taker=False,
            )
            contracts = as_decimal(fill["count_fp"])
            for target in (bucket, totals):
                target["fills"] += 1
                target["contracts"] += contracts
                if economics is not None:
                    target["gross_pnl"] += as_decimal(economics["gross_pnl_dollars"])
                    target["net_pnl"] += as_decimal(economics["net_pnl_dollars"])
                    target["fees"] += as_decimal(economics["fees_dollars"])
                    if economics["fee_missing"]:
                        target["missing_fee_count"] += 1
                if not _raw_has_decision_lineage(fill["raw"]):
                    target["missing_decision_lineage"] += 1
            if fill["settlement_result"] == "win":
                bucket["wins"] += 1
            elif fill["settlement_result"] == "loss":
                bucket["losses"] += 1
            for seconds in horizons:
                snapshot = first_snapshot_at_or_after(str(fill["market_ticker"]), fill_time + timedelta(seconds=seconds))
                if snapshot is None:
                    continue
                mark_price = _snapshot_side_bid_dollars(snapshot, str(fill["side"]))
                if mark_price is None:
                    continue
                markout = mark_price - side_price
                for target in (bucket, totals):
                    target["markout"][str(seconds)]["count"] += 1
                    target["markout"][str(seconds)]["sum"] += markout

        def finalize(row: dict[str, Any]) -> dict[str, Any]:
            contracts = row["contracts"]
            fills_count = int(row["fills"])
            markouts = {
                seconds: {
                    "count": value["count"],
                    "avg_dollars_per_contract": str(
                        (
                            value["sum"] / Decimal(value["count"])
                            if value["count"]
                            else Decimal("0")
                        ).quantize(Decimal("0.0001"))
                    )
                    if value["count"]
                    else None,
                }
                for seconds, value in row["markout"].items()
            }
            return {
                **{
                    key: value
                    for key, value in row.items()
                    if key not in {"contracts", "gross_pnl", "net_pnl", "fees", "markout"}
                },
                "contracts": str(contracts.quantize(Decimal("0.01"))),
                "gross_pnl_dollars": str(row["gross_pnl"].quantize(Decimal("0.0001"))),
                "fees_dollars": str(row["fees"].quantize(Decimal("0.0001"))),
                "net_pnl_dollars": str(row["net_pnl"].quantize(Decimal("0.0001"))),
                "pnl_per_contract_dollars": str(
                    (
                        row["net_pnl"] / contracts
                        if contracts > Decimal("0")
                        else Decimal("0")
                    ).quantize(Decimal("0.0001"))
                ),
                "win_rate": (row["wins"] / fills_count) if fills_count and "wins" in row else None,
                "markouts": markouts,
            }

        finalized_cells = [finalize(row) for row in cells.values()]
        finalized_cells.sort(key=lambda item: Decimal(str(item["net_pnl_dollars"])))
        return {
            "schema_version": "crypto-maker-markout-v1",
            "kalshi_env": env,
            "days": int(days),
            "frequency": normalized_frequency,
            "asset_symbols": sorted(assets),
            "horizons_seconds": list(horizons),
            "shadow_only": True,
            "live_trading_change": False,
            "totals": finalize(totals),
            "cells": finalized_cells,
            "worst_cells": finalized_cells[:20],
        }

    async def get_broken_book_rate_30d(self, *, kalshi_env: str | None = None) -> dict[str, Any]:
        cutoff = datetime.now(UTC) - timedelta(days=30)
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = (
            select(
                func.count().filter(
                    RoomStrategyAuditRecord.stand_down_reason == "book_effectively_broken"
                ).label("broken_count"),
                func.count().label("total_count"),
            )
            .join(Room, Room.id == RoomStrategyAuditRecord.room_id)
            .where(
                Room.kalshi_env == env,
                RoomStrategyAuditRecord.created_at >= cutoff,
            )
        )
        row = (await self.session.execute(stmt)).one()
        return {"broken_count": int(row.broken_count), "total_count": int(row.total_count)}

    async def get_position(
        self,
        market_ticker: str,
        subaccount: int = 0,
        *,
        kalshi_env: str | None = None,
        include_closed: bool = False,
    ) -> PositionRecord | None:
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(PositionRecord).where(
            PositionRecord.kalshi_env == env,
            PositionRecord.market_ticker == market_ticker,
            PositionRecord.subaccount == subaccount,
        )
        if not include_closed:
            stmt = stmt.where(PositionRecord.count_fp > 0)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_positions_for_ticker(
        self,
        market_ticker: str,
        subaccount: int = 0,
        *,
        kalshi_env: str | None = None,
        include_closed: bool = False,
    ) -> list[PositionRecord]:
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(PositionRecord).where(
            PositionRecord.kalshi_env == env,
            PositionRecord.market_ticker == market_ticker,
            PositionRecord.subaccount == subaccount,
        )
        if not include_closed:
            stmt = stmt.where(PositionRecord.count_fp > 0)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_pending_buy_count_fp(
        self,
        market_ticker: str,
        side: str,
        *,
        kalshi_env: str | None = None,
        subaccount: int | None = None,
    ) -> Decimal:
        """Sum count_fp of resting/submitted buy orders for this ticker+side (in-flight exposure)."""
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(OrderRecord).where(
            OrderRecord.kalshi_env == env,
            OrderRecord.market_ticker == market_ticker,
            OrderRecord.side == side,
            OrderRecord.action == "buy",
            OrderRecord.status.in_(sorted(_PENDING_BUY_ORDER_STATUSES)),
        )
        orders = list((await self.session.execute(stmt)).scalars())
        return sum(
            (as_decimal(order.count_fp) for order in orders if _order_matches_subaccount(order, subaccount)),
            Decimal("0"),
        )

    async def get_crypto_portfolio_position_notional_dollars(
        self,
        *,
        kalshi_env: str | None = None,
        subaccount: int | None = None,
    ) -> Decimal:
        """Return open notional for crypto positions in one env/subaccount."""
        positions = await self.list_positions(
            limit=5000,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            subaccount=subaccount,
        )
        return _quantize_money(
            sum(
                (
                    abs(as_decimal(position.count_fp)) * as_decimal(position.average_price_dollars)
                    for position in positions
                    if _is_crypto_market_ticker(position.market_ticker)
                ),
                Decimal("0"),
            )
        )

    async def get_crypto_portfolio_pending_buy_notional_dollars(
        self,
        *,
        kalshi_env: str | None = None,
        subaccount: int | None = None,
    ) -> Decimal:
        """Return notional for resting/submitted crypto BUY orders in one env/subaccount."""
        env = self._resolved_kalshi_env(kalshi_env)
        stmt = select(OrderRecord).where(
            OrderRecord.kalshi_env == env,
            OrderRecord.action == "buy",
            OrderRecord.status.in_(sorted(_PENDING_BUY_ORDER_STATUSES)),
        )
        orders = list((await self.session.execute(stmt)).scalars())
        return _quantize_money(
            sum(
                (
                    _order_notional_dollars(order)
                    for order in orders
                    if _is_crypto_market_ticker(order.market_ticker) and _order_matches_subaccount(order, subaccount)
                ),
                Decimal("0"),
            )
        )

    async def zero_settled_positions(
        self,
        *,
        kalshi_env: str,
        subaccount: int,
        live_tickers: set[str],
    ) -> int:
        """Zero out DB positions not present in the live Kalshi response (i.e. settled)."""
        stmt = select(PositionRecord).where(
            PositionRecord.kalshi_env == kalshi_env,
            PositionRecord.subaccount == subaccount,
            PositionRecord.count_fp != 0,
        )
        rows = list((await self.session.execute(stmt)).scalars())
        zeroed = 0
        for row in rows:
            if row.market_ticker not in live_tickers:
                row.count_fp = Decimal("0")
                zeroed += 1
        if zeroed:
            await self.session.flush()
        return zeroed

    async def upsert_position(
        self,
        *,
        market_ticker: str,
        subaccount: int,
        kalshi_env: str | None = None,
        side: str,
        count_fp: Decimal,
        average_price_dollars: Decimal,
        raw: dict[str, Any],
    ) -> PositionRecord:
        env = self._resolved_kalshi_env(kalshi_env)
        now = datetime.now(UTC)
        insert_values = {
            "id": str(uuid4()),
            "market_ticker": market_ticker,
            "subaccount": subaccount,
            "kalshi_env": env,
            "side": side,
            "count_fp": count_fp,
            "average_price_dollars": average_price_dollars,
            "raw": raw,
            "created_at": now,
            "updated_at": now,
        }
        update_values = {
            "side": side,
            "count_fp": count_fp,
            "average_price_dollars": average_price_dollars,
            "raw": raw,
            "updated_at": now,
        }
        dialect_name = self.session.bind.dialect.name if self.session.bind is not None else ""
        if dialect_name == "postgresql":
            stmt = pg_insert(PositionRecord).values(**insert_values)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(PositionRecord).values(**insert_values)
        else:
            stmt = None
        if stmt is not None:
            await self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["kalshi_env", "market_ticker", "subaccount"],
                    set_=update_values,
                )
            )
            await self.session.flush()
            return (
                await self.session.execute(
                    select(PositionRecord).where(
                        PositionRecord.kalshi_env == env,
                        PositionRecord.market_ticker == market_ticker,
                        PositionRecord.subaccount == subaccount,
                    )
                )
            ).scalar_one()

        existing = (
            await self.session.execute(
                select(PositionRecord).where(
                    PositionRecord.kalshi_env == env,
                    PositionRecord.market_ticker == market_ticker,
                    PositionRecord.subaccount == subaccount,
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = PositionRecord(**insert_values)
            self.session.add(existing)
        else:
            for key, value in update_values.items():
                setattr(existing, key, value)
        await self.session.flush()
        return existing

    async def log_ops_event(
        self,
        *,
        severity: str,
        summary: str,
        source: str,
        payload: dict[str, Any],
        room_id: str | None = None,
        kalshi_env: str | None = None,
    ) -> OpsEvent:
        record = OpsEvent(
            room_id=room_id,
            kalshi_env=self._resolved_kalshi_env(kalshi_env),
            severity=severity,
            summary=summary,
            source=source,
            payload=payload,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def create_research_run(self, *, market_ticker: str, trigger_reason: str, payload: dict[str, Any] | None = None) -> ResearchRunRecord:
        record = ResearchRunRecord(
            market_ticker=market_ticker,
            trigger_reason=trigger_reason,
            status="running",
            payload=payload or {},
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def complete_research_run(
        self,
        run_id: str,
        *,
        status: str,
        payload: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> ResearchRunRecord:
        record = await self.session.get(ResearchRunRecord, run_id)
        if record is None:
            raise KeyError(f"Research run {run_id} not found")
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.error_text = error_text
        if payload is not None:
            record.payload = payload
        await self.session.flush()
        return record

    async def save_research_sources(self, *, run_id: str, market_ticker: str, sources: list[ResearchSourceCard]) -> dict[str, ResearchSourceRecord]:
        created: dict[str, ResearchSourceRecord] = {}
        for source in sources:
            record = ResearchSourceRecord(
                research_run_id=run_id,
                market_ticker=market_ticker,
                source_key=source.source_key,
                source_class=source.source_class,
                trust_tier=source.trust_tier,
                publisher=source.publisher,
                title=source.title,
                url=source.url,
                snippet=source.snippet,
                retrieved_at=source.retrieved_at,
                payload=source.model_dump(mode="json"),
            )
            self.session.add(record)
            await self.session.flush()
            created[source.source_key] = record
        return created

    async def save_research_claims(
        self,
        *,
        run_id: str,
        market_ticker: str,
        claims: list[ResearchClaim],
        source_records: dict[str, ResearchSourceRecord],
    ) -> list[ResearchClaimRecord]:
        created: list[ResearchClaimRecord] = []
        for claim in claims:
            record = ResearchClaimRecord(
                research_run_id=run_id,
                research_source_id=source_records.get(claim.source_key).id if claim.source_key in source_records else None,
                market_ticker=market_ticker,
                source_key=claim.source_key,
                claim_text=claim.claim,
                stance=claim.stance,
                settlement_critical=claim.settlement_critical,
                freshness_seconds=claim.freshness_seconds,
                payload=claim.model_dump(mode="json"),
            )
            self.session.add(record)
            await self.session.flush()
            created.append(record)
        return created

    async def upsert_research_dossier(self, dossier: ResearchDossier) -> ResearchDossierRecord:
        record = await self.session.get(ResearchDossierRecord, dossier.market_ticker)
        if record is None:
            record = ResearchDossierRecord(market_ticker=dossier.market_ticker, payload={})
            self.session.add(record)
        record.status = dossier.status
        record.mode = dossier.mode
        record.confidence = dossier.summary.research_confidence
        record.source_count = len(dossier.sources)
        record.contradiction_count = dossier.contradiction_count
        record.unresolved_count = dossier.unresolved_count
        record.settlement_covered = dossier.settlement_covered
        record.last_run_id = dossier.last_run_id
        record.expires_at = dossier.freshness.expires_at
        record.payload = dossier.model_dump(mode="json")
        await self.session.flush()
        return record

    async def get_research_dossier(self, market_ticker: str) -> ResearchDossierRecord | None:
        return await self.session.get(ResearchDossierRecord, market_ticker)

    async def list_research_dossiers(self, limit: int = 100) -> list[ResearchDossierRecord]:
        result = await self.session.execute(select(ResearchDossierRecord).order_by(ResearchDossierRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_research_runs(
        self,
        *,
        market_ticker: str | None = None,
        status: str | None = None,
        limit: int = 20,
    ) -> list[ResearchRunRecord]:
        stmt = select(ResearchRunRecord)
        if market_ticker is not None:
            stmt = stmt.where(ResearchRunRecord.market_ticker == market_ticker)
        if status is not None:
            stmt = stmt.where(ResearchRunRecord.status == status)
        result = await self.session.execute(stmt.order_by(ResearchRunRecord.started_at.desc()).limit(limit))
        return list(result.scalars())

    async def list_research_sources(self, *, run_id: str) -> list[ResearchSourceRecord]:
        result = await self.session.execute(
            select(ResearchSourceRecord).where(ResearchSourceRecord.research_run_id == run_id).order_by(ResearchSourceRecord.retrieved_at.desc())
        )
        return list(result.scalars())

    async def list_research_claims(self, *, run_id: str) -> list[ResearchClaimRecord]:
        result = await self.session.execute(
            select(ResearchClaimRecord).where(ResearchClaimRecord.research_run_id == run_id).order_by(ResearchClaimRecord.created_at.asc())
        )
        return list(result.scalars())

    async def upsert_room_research_health(
        self,
        *,
        room_id: str,
        market_ticker: str,
        dossier_status: str,
        gate_passed: bool,
        valid_dossier: bool,
        good_for_training: bool,
        quality_score: float,
        citation_coverage_score: float,
        settlement_clarity_score: float,
        freshness_score: float,
        contradiction_count: int,
        structured_completeness_score: float,
        fair_value_score: float,
        dossier_artifact_id: str | None,
        payload: dict[str, Any],
    ) -> RoomResearchHealthRecord:
        record = await self.session.get(RoomResearchHealthRecord, room_id)
        if record is None:
            record = RoomResearchHealthRecord(room_id=room_id, market_ticker=market_ticker, payload={})
            self.session.add(record)
        record.market_ticker = market_ticker
        record.dossier_status = dossier_status
        record.gate_passed = gate_passed
        record.valid_dossier = valid_dossier
        record.good_for_training = good_for_training
        record.quality_score = quality_score
        record.citation_coverage_score = citation_coverage_score
        record.settlement_clarity_score = settlement_clarity_score
        record.freshness_score = freshness_score
        record.contradiction_count = contradiction_count
        record.structured_completeness_score = structured_completeness_score
        record.fair_value_score = fair_value_score
        record.dossier_artifact_id = dossier_artifact_id
        record.payload = payload
        await self.session.flush()
        return record

    async def get_room_research_health(self, room_id: str) -> RoomResearchHealthRecord | None:
        return await self.session.get(RoomResearchHealthRecord, room_id)

    async def list_room_research_health(
        self,
        *,
        limit: int = 200,
        good_for_training: bool | None = None,
    ) -> list[RoomResearchHealthRecord]:
        stmt = select(RoomResearchHealthRecord)
        if good_for_training is not None:
            stmt = stmt.where(RoomResearchHealthRecord.good_for_training.is_(good_for_training))
        result = await self.session.execute(stmt.order_by(RoomResearchHealthRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def upsert_room_strategy_audit(
        self,
        *,
        room_id: str,
        market_ticker: str,
        audit_source: str,
        audit_version: str,
        thesis_correctness: str,
        trade_quality: str,
        block_correctness: str,
        missed_stand_down: bool,
        stale_data_mismatch: bool,
        effective_freshness_agreement: bool,
        resolution_state: str | None,
        eligibility_passed: bool | None,
        stand_down_reason: str | None,
        trainable_default: bool,
        exclude_reason: str | None,
        quality_warnings: list[str],
        payload: dict[str, Any],
    ) -> RoomStrategyAuditRecord:
        record = await self.session.get(RoomStrategyAuditRecord, room_id)
        if record is None:
            record = RoomStrategyAuditRecord(room_id=room_id, market_ticker=market_ticker, payload={})
            self.session.add(record)
        record.market_ticker = market_ticker
        record.audit_source = audit_source
        record.audit_version = audit_version
        record.thesis_correctness = thesis_correctness
        record.trade_quality = trade_quality
        record.block_correctness = block_correctness
        record.missed_stand_down = missed_stand_down
        record.stale_data_mismatch = stale_data_mismatch
        record.effective_freshness_agreement = effective_freshness_agreement
        record.resolution_state = resolution_state
        record.eligibility_passed = eligibility_passed
        record.stand_down_reason = stand_down_reason
        record.trainable_default = trainable_default
        record.exclude_reason = exclude_reason
        record.quality_warnings = quality_warnings
        record.payload = payload
        await self.session.flush()
        return record

    async def get_room_strategy_audit(self, room_id: str) -> RoomStrategyAuditRecord | None:
        return await self.session.get(RoomStrategyAuditRecord, room_id)

    async def list_room_strategy_audits(
        self,
        *,
        limit: int = 200,
        since: datetime | None = None,
        market_ticker: str | None = None,
        audit_source: str | None = None,
        trainable_default: bool | None = None,
    ) -> list[RoomStrategyAuditRecord]:
        stmt = select(RoomStrategyAuditRecord)
        if since is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.updated_at >= since)
        if market_ticker is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.market_ticker == market_ticker)
        if audit_source is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.audit_source == audit_source)
        if trainable_default is not None:
            stmt = stmt.where(RoomStrategyAuditRecord.trainable_default.is_(trainable_default))
        result = await self.session.execute(stmt.order_by(RoomStrategyAuditRecord.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def save_memory_note(self, *, room_id: str | None, payload: MemoryNotePayload, embedding: list[float] | None, provider: str) -> MemoryNoteRecord:
        note = MemoryNoteRecord(
            room_id=room_id,
            title=payload.title,
            summary=payload.summary,
            tags=payload.tags,
            linked_message_ids=payload.linked_message_ids,
        )
        self.session.add(note)
        await self.session.flush()
        self.session.add(
            MemoryEmbedding(memory_note_id=note.id, provider=provider, embedding=embedding, payload={"tags": payload.tags})
        )
        await self.session.flush()
        return note

    async def list_recent_memory_notes(self, limit: int = 10) -> list[MemoryNoteRecord]:
        result = await self.session.execute(select(MemoryNoteRecord).order_by(MemoryNoteRecord.created_at.desc()).limit(limit))
        return list(result.scalars())

    async def get_latest_memory_note_for_room(self, room_id: str) -> MemoryNoteRecord | None:
        stmt = (
            select(MemoryNoteRecord)
            .where(MemoryNoteRecord.room_id == room_id)
            .order_by(MemoryNoteRecord.created_at.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_positions(
        self,
        limit: int = 50,
        kalshi_env: str | None = None,
        subaccount: int | None = None,
    ) -> list[PositionRecord]:
        stmt = select(PositionRecord).where(PositionRecord.count_fp != 0)
        if kalshi_env is not None:
            stmt = stmt.where(PositionRecord.kalshi_env == kalshi_env)
        if subaccount is not None:
            stmt = stmt.where(PositionRecord.subaccount == subaccount)
        stmt = stmt.order_by(PositionRecord.updated_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def portfolio_bucket_snapshot(
        self,
        *,
        kalshi_env: str,
        subaccount: int,
        total_capital_dollars: Decimal,
        safe_capital_reserve_ratio: float,
        risky_capital_max_ratio: float,
    ) -> PortfolioBucketSnapshot:
        positions = await self.list_positions(limit=5000, kalshi_env=kalshi_env, subaccount=subaccount)
        signal_payloads = await self.latest_signal_payloads_for_markets(
            market_tickers=[position.market_ticker for position in positions],
            kalshi_env=kalshi_env,
        )

        overall_used = Decimal("0.0000")
        safe_used = Decimal("0.0000")
        risky_used = Decimal("0.0000")
        for position in positions:
            notional = _quantize_money(abs(Decimal(str(position.count_fp))) * Decimal(str(position.average_price_dollars)))
            overall_used += notional
            bucket = _capital_bucket_from_signal_payload(signal_payloads.get(position.market_ticker))
            if bucket == "safe":
                safe_used += notional
            else:
                risky_used += notional

        total_capital = _quantize_money(total_capital_dollars)
        risky_limit = _quantize_money(total_capital * Decimal(str(risky_capital_max_ratio)))
        safe_reserve_target = _quantize_money(total_capital * Decimal(str(safe_capital_reserve_ratio)))
        overall_remaining = _quantize_money(max(Decimal("0.0000"), total_capital - overall_used))
        risky_remaining = _quantize_money(max(Decimal("0.0000"), min(overall_remaining, risky_limit - risky_used)))
        safe_remaining = overall_remaining

        return PortfolioBucketSnapshot(
            total_capital_dollars=total_capital,
            overall_used_dollars=overall_used,
            overall_remaining_dollars=overall_remaining,
            safe_used_dollars=safe_used,
            safe_remaining_dollars=safe_remaining,
            safe_reserve_target_dollars=safe_reserve_target,
            risky_used_dollars=risky_used,
            risky_limit_dollars=risky_limit,
            risky_remaining_dollars=risky_remaining,
            safe_capital_reserve_ratio=safe_capital_reserve_ratio,
            risky_capital_max_ratio=risky_capital_max_ratio,
        )

    async def list_ops_events(
        self,
        *,
        limit: int = 50,
        sources: list[str] | None = None,
        created_after: datetime | None = None,
        kalshi_env: str | None = None,
    ) -> list[OpsEvent]:
        stmt = select(OpsEvent)
        if kalshi_env is not None:
            stmt = stmt.where(OpsEvent.kalshi_env == self._resolved_kalshi_env(kalshi_env))
        if sources:
            stmt = stmt.where(OpsEvent.source.in_(sources))
        if created_after is not None:
            stmt = stmt.where(OpsEvent.updated_at >= created_after)
        result = await self.session.execute(stmt.order_by(OpsEvent.updated_at.desc()).limit(limit))
        return list(result.scalars())

    async def set_checkpoint(self, stream_name: str, cursor: str | None, payload: dict[str, Any]) -> Checkpoint:
        bind = self.session.get_bind()
        dialect_name = bind.dialect.name if bind is not None else ""
        now = datetime.now(UTC)
        insert_values = {
            "id": str(uuid4()),
            "stream_name": stream_name,
            "cursor": cursor,
            "payload": payload,
            "created_at": now,
            "updated_at": now,
        }
        if dialect_name in {"postgresql", "sqlite"}:
            insert_stmt = (pg_insert if dialect_name == "postgresql" else sqlite_insert)(Checkpoint).values(
                **insert_values
            )
            stmt = (
                insert_stmt.on_conflict_do_update(
                    index_elements=["stream_name"],
                    set_={
                        "cursor": insert_stmt.excluded.cursor,
                        "payload": insert_stmt.excluded.payload,
                        "updated_at": now,
                    },
                )
                .returning(Checkpoint.id)
            )
            checkpoint_id = (await self.session.execute(stmt)).scalar_one()
            checkpoint = await self.session.get(Checkpoint, checkpoint_id, populate_existing=True)
            if checkpoint is None:
                raise RuntimeError(f"checkpoint upsert returned missing id for {stream_name}")
            return checkpoint

        stmt = select(Checkpoint).where(Checkpoint.stream_name == stream_name)
        checkpoint = (await self.session.execute(stmt)).scalar_one_or_none()
        if checkpoint is None:
            checkpoint = Checkpoint(stream_name=stream_name, cursor=cursor, payload=payload)
            self.session.add(checkpoint)
        else:
            checkpoint.cursor = cursor
            checkpoint.payload = payload
        await self.session.flush()
        return checkpoint

    async def get_checkpoint(self, stream_name: str) -> Checkpoint | None:
        stmt = select(Checkpoint).where(Checkpoint.stream_name == stream_name)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_checkpoints(
        self,
        *,
        prefix: str | None = None,
        limit: int = 500,
    ) -> list[Checkpoint]:
        stmt = select(Checkpoint)
        if prefix is not None:
            stmt = stmt.where(Checkpoint.stream_name.like(f"{prefix}%"))
        stmt = stmt.order_by(Checkpoint.stream_name.asc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def get_total_capital_dollars(self, *, kalshi_env: str | None = None) -> Decimal | None:
        """Return total portfolio value (cash + positions) from the latest reconcile checkpoint."""
        checkpoint = await self.get_checkpoint(self._env_stream_name("reconcile", kalshi_env=kalshi_env))
        if checkpoint is None:
            return None
        balance_payload = dict((checkpoint.payload or {}).get("balance") or {})
        return _total_capital_dollars_from_balance_payload(balance_payload)

    @staticmethod
    def _pacific_today() -> str:
        """Return today's date string in Pacific Time (YYYY-MM-DD)."""
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")

    async def get_daily_portfolio_baseline_dollars(
        self,
        *,
        pacific_date: str | None = None,
        kalshi_env: str | None = None,
    ) -> Decimal | None:
        date = pacific_date or self._pacific_today()
        checkpoint = await self.get_checkpoint(self._env_stream_name("daily_portfolio", kalshi_env=kalshi_env, suffix=date))
        if checkpoint is None:
            return None
        raw = (checkpoint.payload or {}).get("total_capital_dollars")
        if raw is None:
            return None
        try:
            return Decimal(str(raw))
        except ArithmeticError:
            return None

    async def set_daily_portfolio_baseline_dollars(
        self,
        total_capital_dollars: Decimal,
        *,
        pacific_date: str | None = None,
        kalshi_env: str | None = None,
    ) -> None:
        date = pacific_date or self._pacific_today()
        await self.set_checkpoint(
            self._env_stream_name("daily_portfolio", kalshi_env=kalshi_env, suffix=date),
            cursor=None,
            payload={"total_capital_dollars": str(total_capital_dollars), "date": date},
        )

    async def get_daily_pnl_dollars(self, *, kalshi_env: str | None = None) -> Decimal | None:
        """Return today's P&L: current portfolio value minus start-of-day baseline (Pacific Time)."""
        current = await self.get_total_capital_dollars(kalshi_env=kalshi_env)
        baseline = await self.get_daily_portfolio_baseline_dollars(kalshi_env=kalshi_env)
        if current is None or baseline is None:
            return None
        return (current - baseline).quantize(Decimal("0.01"))

    async def list_exchange_events(
        self,
        *,
        stream_name: str | None = None,
        event_type: str | None = None,
        market_ticker: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 50,
    ) -> list[RawExchangeEvent]:
        stmt = select(RawExchangeEvent)
        if stream_name is not None:
            stmt = stmt.where(RawExchangeEvent.stream_name == stream_name)
        if event_type is not None:
            stmt = stmt.where(RawExchangeEvent.event_type == event_type)
        if market_ticker is not None:
            stmt = stmt.where(RawExchangeEvent.market_ticker == market_ticker)
        if created_after is not None:
            stmt = stmt.where(RawExchangeEvent.created_at >= created_after)
        if created_before is not None:
            stmt = stmt.where(RawExchangeEvent.created_at <= created_before)
        stmt = stmt.order_by(RawExchangeEvent.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_weather_events(
        self,
        *,
        station_id: str | None = None,
        event_type: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 200,
    ) -> list[RawWeatherEvent]:
        stmt = select(RawWeatherEvent)
        if station_id is not None:
            stmt = stmt.where(RawWeatherEvent.station_id == station_id)
        if event_type is not None:
            stmt = stmt.where(RawWeatherEvent.event_type == event_type)
        if created_after is not None:
            stmt = stmt.where(RawWeatherEvent.created_at >= created_after)
        if created_before is not None:
            stmt = stmt.where(RawWeatherEvent.created_at <= created_before)
        stmt = stmt.order_by(RawWeatherEvent.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())
