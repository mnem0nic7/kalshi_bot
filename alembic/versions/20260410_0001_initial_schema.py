"""initial schema

Revision ID: 20260410_0001
Revises:
Create Date: 2026-04-10 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector


revision = "20260410_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "rooms",
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=True),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("active_color", sa.String(length=16), nullable=False),
        sa.Column("shadow_mode", sa.Boolean(), nullable=False),
        sa.Column("kill_switch_enabled", sa.Boolean(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_rooms_market_ticker", "rooms", ["market_ticker"], unique=False)

    op.create_table(
        "room_messages",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("role", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("room_id", "sequence", name="uq_room_message_sequence"),
    )
    op.create_index("ix_room_messages_room_id", "room_messages", ["room_id"], unique=False)
    op.create_index("ix_room_messages_role", "room_messages", ["role"], unique=False)
    op.create_index("ix_room_messages_kind", "room_messages", ["kind"], unique=False)
    op.create_index("ix_room_messages_sequence", "room_messages", ["sequence"], unique=False)
    op.create_index("ix_room_messages_room_created", "room_messages", ["room_id", "created_at"], unique=False)

    op.create_table(
        "artifacts",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("message_id", sa.String(length=36), nullable=True),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("external_id", sa.String(length=255), nullable=True),
        sa.Column("fingerprint", sa.String(length=255), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["room_messages.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_artifacts_room_id", "artifacts", ["room_id"], unique=False)

    op.create_table(
        "raw_exchange_events",
        sa.Column("stream_name", sa.String(length=64), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_raw_exchange_events_stream_name", "raw_exchange_events", ["stream_name"], unique=False)
    op.create_index("ix_raw_exchange_events_market_ticker", "raw_exchange_events", ["market_ticker"], unique=False)
    op.create_index("ix_raw_exchange_events_created_at", "raw_exchange_events", ["created_at"], unique=False)
    op.create_index(
        "ix_raw_exchange_events_stream_created",
        "raw_exchange_events",
        ["stream_name", "created_at"],
        unique=False,
    )

    op.create_table(
        "raw_weather_events",
        sa.Column("station_id", sa.String(length=32), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_raw_weather_events_station_id", "raw_weather_events", ["station_id"], unique=False)
    op.create_index("ix_raw_weather_events_created_at", "raw_weather_events", ["created_at"], unique=False)

    op.create_table(
        "market_state",
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_trade_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("snapshot", sa.JSON(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("market_ticker"),
    )
    op.create_index("ix_market_state_observed_at", "market_state", ["observed_at"], unique=False)

    op.create_table(
        "signals",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("fair_yes_dollars", sa.Numeric(10, 4), nullable=False),
        sa.Column("edge_bps", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_signals_room_id", "signals", ["room_id"], unique=False)
    op.create_index("ix_signals_market_ticker", "signals", ["market_ticker"], unique=False)

    op.create_table(
        "trade_tickets",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("message_id", sa.String(length=36), nullable=True),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("yes_price_dollars", sa.Numeric(10, 4), nullable=False),
        sa.Column("count_fp", sa.Numeric(10, 2), nullable=False),
        sa.Column("time_in_force", sa.String(length=64), nullable=False),
        sa.Column("client_order_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["room_messages.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_order_id", name="uq_trade_tickets_client_order_id"),
    )
    op.create_index("ix_trade_tickets_room_id", "trade_tickets", ["room_id"], unique=False)
    op.create_index("ix_trade_tickets_market_ticker", "trade_tickets", ["market_ticker"], unique=False)

    op.create_table(
        "risk_verdicts",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("ticket_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reasons", sa.JSON(), nullable=False),
        sa.Column("approved_notional_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("approved_count_fp", sa.Numeric(10, 2), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticket_id"], ["trade_tickets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_risk_verdicts_room_id", "risk_verdicts", ["room_id"], unique=False)
    op.create_index("ix_risk_verdicts_ticket_id", "risk_verdicts", ["ticket_id"], unique=False)

    op.create_table(
        "orders",
        sa.Column("trade_ticket_id", sa.String(length=36), nullable=True),
        sa.Column("kalshi_order_id", sa.String(length=128), nullable=True),
        sa.Column("client_order_id", sa.String(length=64), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("yes_price_dollars", sa.Numeric(10, 4), nullable=False),
        sa.Column("count_fp", sa.Numeric(10, 2), nullable=False),
        sa.Column("raw", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["trade_ticket_id"], ["trade_tickets.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_order_id", name="uq_orders_client_order_id"),
    )
    op.create_index("ix_orders_kalshi_order_id", "orders", ["kalshi_order_id"], unique=False)
    op.create_index("ix_orders_market_ticker", "orders", ["market_ticker"], unique=False)

    op.create_table(
        "fills",
        sa.Column("order_id", sa.String(length=36), nullable=True),
        sa.Column("trade_id", sa.String(length=128), nullable=True),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("yes_price_dollars", sa.Numeric(10, 4), nullable=False),
        sa.Column("count_fp", sa.Numeric(10, 2), nullable=False),
        sa.Column("is_taker", sa.Boolean(), nullable=False),
        sa.Column("raw", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("trade_id", name="uq_fills_trade_id"),
    )
    op.create_index("ix_fills_order_id", "fills", ["order_id"], unique=False)
    op.create_index("ix_fills_market_ticker", "fills", ["market_ticker"], unique=False)

    op.create_table(
        "positions",
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("subaccount", sa.Integer(), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("count_fp", sa.Numeric(10, 2), nullable=False),
        sa.Column("average_price_dollars", sa.Numeric(10, 4), nullable=False),
        sa.Column("raw", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("market_ticker", "subaccount", name="uq_positions_market_subaccount"),
    )
    op.create_index("ix_positions_market_ticker", "positions", ["market_ticker"], unique=False)

    op.create_table(
        "ops_events",
        sa.Column("room_id", sa.String(length=36), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ops_events_room_id", "ops_events", ["room_id"], unique=False)

    op.create_table(
        "memory_notes",
        sa.Column("room_id", sa.String(length=36), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("linked_message_ids", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_memory_notes_room_id", "memory_notes", ["room_id"], unique=False)

    op.create_table(
        "memory_embeddings",
        sa.Column("memory_note_id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("embedding", Vector(dim=16), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["memory_note_id"], ["memory_notes.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_memory_embeddings_memory_note_id", "memory_embeddings", ["memory_note_id"], unique=True)

    op.create_table(
        "checkpoints",
        sa.Column("stream_name", sa.String(length=128), nullable=False),
        sa.Column("cursor", sa.String(length=255), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stream_name", name="uq_checkpoint_stream_name"),
    )

    op.create_table(
        "deployment_control",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("active_color", sa.String(length=16), nullable=False),
        sa.Column("kill_switch_enabled", sa.Boolean(), nullable=False),
        sa.Column("execution_lock_holder", sa.String(length=64), nullable=True),
        sa.Column("shadow_color", sa.String(length=16), nullable=True),
        sa.Column("notes", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    for table in [
        "deployment_control",
        "checkpoints",
        "memory_embeddings",
        "memory_notes",
        "ops_events",
        "positions",
        "fills",
        "orders",
        "risk_verdicts",
        "trade_tickets",
        "signals",
        "market_state",
        "raw_weather_events",
        "raw_exchange_events",
        "artifacts",
        "room_messages",
        "rooms",
    ]:
        op.drop_table(table)

