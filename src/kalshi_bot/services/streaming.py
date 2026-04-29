from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.core.fixed_point import as_decimal, quantize_count, quantize_price
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiWebSocketClient
from kalshi_bot.core.metrics import FEED_FRESHNESS_SECONDS

logger = logging.getLogger(__name__)


def _first_positive_count(*values: Any) -> str | None:
    for value in values:
        if value in (None, ""):
            continue
        if as_decimal(value) > 0:
            return str(value)
    return None


class SequenceGapError(RuntimeError):
    pass


@dataclass(slots=True)
class OrderBookState:
    market_ticker: str
    yes_levels: dict[Decimal, Decimal] = field(default_factory=dict)
    no_levels: dict[Decimal, Decimal] = field(default_factory=dict)
    seq: int | None = None

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any], seq: int | None) -> "OrderBookState":
        state = cls(market_ticker=snapshot["market_ticker"], seq=seq)
        for price, count in snapshot.get("yes_dollars_fp", []):
            state.yes_levels[quantize_price(price)] = quantize_count(count)
        for price, count in snapshot.get("no_dollars_fp", []):
            state.no_levels[quantize_price(price)] = quantize_count(count)
        return state

    def apply_delta(self, msg: dict[str, Any], seq: int | None) -> None:
        levels = self.yes_levels if msg["side"] == "yes" else self.no_levels
        price = quantize_price(msg["price_dollars"])
        next_size = levels.get(price, Decimal("0")) + Decimal(str(msg["delta_fp"]))
        if next_size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = quantize_count(next_size)
        self.seq = seq

    @property
    def best_yes_bid(self) -> Decimal | None:
        return max(self.yes_levels) if self.yes_levels else None

    @property
    def best_no_bid(self) -> Decimal | None:
        return max(self.no_levels) if self.no_levels else None

    @property
    def best_yes_ask(self) -> Decimal | None:
        if self.best_no_bid is None:
            return None
        return quantize_price(Decimal("1.0000") - self.best_no_bid)

    @property
    def best_no_ask(self) -> Decimal | None:
        if self.best_yes_bid is None:
            return None
        return quantize_price(Decimal("1.0000") - self.best_yes_bid)

    def snapshot_payload(self) -> dict[str, Any]:
        return {
            "market_ticker": self.market_ticker,
            "yes_dollars_fp": [[f"{price:.4f}", f"{count:.2f}"] for price, count in sorted(self.yes_levels.items())],
            "no_dollars_fp": [[f"{price:.4f}", f"{count:.2f}"] for price, count in sorted(self.no_levels.items())],
            "seq": self.seq,
        }


class MarketStreamService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        websocket_client: KalshiWebSocketClient,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.websocket_client = websocket_client
        self.orderbooks: dict[str, OrderBookState] = {}
        self.channel_names: dict[int, str] = {}
        self.sid_sequences: dict[int, int] = {}

    async def stream(
        self,
        *,
        market_tickers: list[str],
        include_private: bool = True,
        max_messages: int | None = None,
        on_market_update: Callable[[str], Awaitable[None]] | None = None,
    ) -> int:
        processed = 0
        pending_tasks: set[asyncio.Task] = set()
        backoff = 2.0
        while True:
            try:
                self._reset_connection_state()
                await self.websocket_client.connect()
                await self._subscribe(market_tickers, include_private=include_private)
                backoff = 2.0  # reset after successful connect + first message
                async for message in self.websocket_client.iter_messages():
                    async with self.session_factory() as session:
                        repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                        updated_market = await self.process_message(repo, message)
                        await session.commit()
                    if on_market_update is not None and updated_market is not None:
                        task = asyncio.create_task(on_market_update(updated_market))
                        pending_tasks.add(task)
                        task.add_done_callback(pending_tasks.discard)
                    processed += 1
                    if max_messages is not None and processed >= max_messages:
                        await self.websocket_client.close()
                        if pending_tasks:
                            await asyncio.gather(*list(pending_tasks), return_exceptions=True)
                        return processed
            except asyncio.CancelledError:
                await self.websocket_client.close()
                raise
            except Exception as exc:
                logger.exception("market stream loop failed")
                async with self.session_factory() as session:
                    repo = PlatformRepository(session, kalshi_env=self.settings.kalshi_env)
                    await self._record_stream_error(repo, exc)
                    await session.commit()
                await self.websocket_client.close()
                if max_messages is not None:
                    raise
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _subscribe(self, market_tickers: list[str], *, include_private: bool) -> None:
        await self.websocket_client.subscribe(["market_lifecycle_v2"])
        if market_tickers:
            await self.websocket_client.subscribe(["orderbook_delta"], market_tickers=market_tickers)
            if include_private:
                await self.websocket_client.subscribe(["user_orders"], market_tickers=market_tickers)
                await self.websocket_client.subscribe(["fill"], market_tickers=market_tickers)

    async def process_message(self, repo: PlatformRepository, message: dict[str, Any]) -> str | None:
        message_type = message.get("type", "unknown")
        sid = int(message.get("sid", 0) or 0)
        seq = message.get("seq")
        if message_type == "subscribed" and sid and message.get("msg", {}).get("channel"):
            self.channel_names[sid] = message["msg"]["channel"]
        await repo.log_exchange_event("ws", message_type, message, market_ticker=self._market_ticker_for_message(message))
        if sid and seq is not None:
            self._validate_sid_sequence(
                sid=sid,
                seq=int(seq),
                message_type=message_type,
                market_ticker=self._market_ticker_for_message(message),
            )
        if sid:
            await repo.set_checkpoint(
                self._checkpoint_name(sid),
                str(seq) if seq is not None else None,
                {"message_type": message_type, "message": message},
            )

        if message_type == "orderbook_snapshot":
            await self._handle_orderbook_snapshot(repo, message, sid=sid, seq=seq)
            return self._market_ticker_for_message(message)
        elif message_type == "orderbook_delta":
            await self._handle_orderbook_delta(repo, message, sid=sid, seq=seq)
            return self._market_ticker_for_message(message)
        elif message_type == "market_lifecycle_v2":
            await self._handle_market_lifecycle(repo, message)
            return self._market_ticker_for_message(message)
        elif message_type == "user_order":
            await self._handle_user_order(repo, message)
        elif message_type == "fill":
            await self._handle_fill(repo, message)
        elif message_type == "error":
            await repo.log_ops_event(
                severity="error",
                summary="Kalshi websocket returned an error message",
                source="stream",
                payload=message,
            )
        return None

    async def _record_stream_error(self, repo: PlatformRepository, exc: Exception) -> None:
        now = datetime.now(UTC)
        error_type = type(exc).__name__
        message = str(exc) or repr(exc)
        checkpoint_key = f"kalshi_ws_error:{self.settings.kalshi_env}:{self.settings.app_color}:{error_type}"
        checkpoint = await repo.get_checkpoint(checkpoint_key)
        existing = dict(checkpoint.payload or {}) if checkpoint is not None else {}
        occurrence_count = int(existing.get("occurrence_count") or 0) + 1
        first_seen_at = existing.get("first_seen_at") or now.isoformat()
        last_logged_at = self._parse_datetime(existing.get("last_logged_at"))
        cooldown = timedelta(seconds=max(0, int(self.settings.stream_error_log_cooldown_seconds)))
        should_log = last_logged_at is None or now - last_logged_at >= cooldown
        payload = {
            "error_type": error_type,
            "message": message,
            "first_seen_at": first_seen_at,
            "last_seen_at": now.isoformat(),
            "occurrence_count": occurrence_count,
            "last_logged_at": now.isoformat() if should_log else existing.get("last_logged_at"),
        }
        if should_log:
            await repo.log_ops_event(
                severity="error",
                summary=(
                    "Kalshi websocket stream error"
                    if occurrence_count == 1
                    else f"Kalshi websocket stream error repeated {occurrence_count} times"
                ),
                source="stream",
                payload=payload,
                kalshi_env=self.settings.kalshi_env,
            )
        await repo.set_checkpoint(
            checkpoint_key,
            cursor=None,
            payload=payload,
        )

    async def _handle_orderbook_snapshot(self, repo: PlatformRepository, message: dict[str, Any], *, sid: int, seq: int | None) -> None:
        msg = message["msg"]
        market_ticker = msg["market_ticker"]
        self.orderbooks[market_ticker] = OrderBookState.from_snapshot(msg, seq=seq)
        FEED_FRESHNESS_SECONDS.labels(feed="kalshi_ws").set(0)
        await self._persist_market_state(repo, market_ticker)

    async def _handle_orderbook_delta(self, repo: PlatformRepository, message: dict[str, Any], *, sid: int, seq: int | None) -> None:
        msg = message["msg"]
        market_ticker = msg["market_ticker"]
        state = self.orderbooks.get(market_ticker)
        if state is None:
            logger.warning("received orderbook delta before snapshot", extra={"market_ticker": market_ticker, "sid": sid, "seq": seq})
            await repo.log_ops_event(
                severity="warning",
                summary="Skipped orderbook delta before snapshot",
                source="stream",
                payload={"market_ticker": market_ticker, "sid": sid, "seq": seq, "message": msg},
            )
            return
        state.apply_delta(msg, seq=seq)
        FEED_FRESHNESS_SECONDS.labels(feed="kalshi_ws").set(0)
        await self._persist_market_state(repo, market_ticker)

    async def _handle_market_lifecycle(self, repo: PlatformRepository, message: dict[str, Any]) -> None:
        msg = message["msg"]
        market_ticker = msg.get("market_ticker")
        if market_ticker:
            await self._persist_market_state(repo, market_ticker, lifecycle=msg)

    async def _handle_user_order(self, repo: PlatformRepository, message: dict[str, Any]) -> None:
        msg = message["msg"]
        yes_price = msg.get("yes_price_dollars") or msg.get("price_dollars") or "0.5000"
        count = _first_positive_count(
            msg.get("remaining_count_fp"),
            msg.get("initial_count_fp"),
            msg.get("count_fp"),
        )
        if count is None:
            logger.warning(
                "skipping websocket user_order with non-positive count",
                extra={"order_id": msg.get("order_id"), "ticker": msg.get("ticker") or msg.get("market_ticker")},
            )
            return
        await repo.upsert_order(
            client_order_id=msg.get("client_order_id") or msg.get("order_id"),
            market_ticker=msg.get("ticker") or msg.get("market_ticker") or "unknown",
            status=msg.get("status", "unknown"),
            side=msg.get("side", "yes"),
            action=msg.get("action", "buy"),
            yes_price_dollars=quantize_price(yes_price),
            count_fp=quantize_count(count),
            raw=msg,
            kalshi_order_id=msg.get("order_id"),
            kalshi_env=self.settings.kalshi_env,
        )

    async def _handle_fill(self, repo: PlatformRepository, message: dict[str, Any]) -> None:
        msg = message["msg"]
        yes_price = msg.get("yes_price_dollars") or msg.get("price_dollars") or "0.5000"
        count = _first_positive_count(msg.get("count_fp"))
        if count is None:
            logger.warning(
                "skipping websocket fill with non-positive count",
                extra={"trade_id": msg.get("trade_id"), "ticker": msg.get("ticker") or msg.get("market_ticker")},
            )
            return
        await repo.upsert_fill(
            market_ticker=msg.get("market_ticker") or msg.get("ticker") or "unknown",
            side=msg.get("side", "yes"),
            action=msg.get("action", "buy"),
            yes_price_dollars=quantize_price(yes_price),
            count_fp=quantize_count(count),
            raw=msg,
            trade_id=msg.get("trade_id"),
            is_taker=bool(msg.get("is_taker", True)),
            kalshi_env=self.settings.kalshi_env,
        )

    async def _persist_market_state(
        self,
        repo: PlatformRepository,
        market_ticker: str,
        *,
        lifecycle: dict[str, Any] | None = None,
    ) -> None:
        state = self.orderbooks.get(market_ticker)
        snapshot: dict[str, Any] = {"market_ticker": market_ticker}
        if state is not None:
            snapshot["orderbook"] = state.snapshot_payload()
        if lifecycle is not None:
            snapshot["lifecycle"] = lifecycle
        await repo.upsert_market_state(
            market_ticker,
            snapshot=snapshot,
            kalshi_env=self.settings.kalshi_env,
            yes_bid_dollars=state.best_yes_bid if state is not None else None,
            yes_ask_dollars=state.best_yes_ask if state is not None else None,
            last_trade_dollars=None,
        )

    @staticmethod
    def _market_ticker_for_message(message: dict[str, Any]) -> str | None:
        msg = message.get("msg", {})
        return msg.get("market_ticker") or msg.get("ticker")

    def _checkpoint_name(self, sid: int) -> str:
        return f"kalshi_ws:{self.settings.kalshi_env}:{self.settings.app_color}:{sid}"

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

    def _reset_connection_state(self) -> None:
        self.orderbooks.clear()
        self.channel_names.clear()
        self.sid_sequences.clear()

    def _validate_sid_sequence(
        self,
        *,
        sid: int,
        seq: int,
        message_type: str,
        market_ticker: str | None,
    ) -> None:
        last_seq = self.sid_sequences.get(sid)
        if last_seq is not None and seq != last_seq + 1:
            raise SequenceGapError(
                f"Expected sid {sid} seq {last_seq + 1}, got {seq} for {message_type} {market_ticker or 'unknown'}"
            )
        self.sid_sequences[sid] = seq
