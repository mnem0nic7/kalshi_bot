"""Scope runtime state by environment.

Revision ID: 20260421_0016
Revises: 20260421_0015
Create Date: 2026-04-21
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260421_0016"
down_revision = "20260421_0015"
branch_labels = None
depends_on = None


def _backfill_orders_env(dialect_name: str) -> None:
    if dialect_name == "postgresql":
        op.execute(
            """
            UPDATE orders
            SET kalshi_env = rooms.kalshi_env
            FROM trade_tickets
            JOIN rooms ON rooms.id = trade_tickets.room_id
            WHERE orders.trade_ticket_id = trade_tickets.id
              AND (orders.kalshi_env IS NULL OR orders.kalshi_env = '')
            """
        )
        return
    if dialect_name == "sqlite":
        op.execute(
            """
            UPDATE orders
            SET kalshi_env = (
                SELECT rooms.kalshi_env
                FROM trade_tickets
                JOIN rooms ON rooms.id = trade_tickets.room_id
                WHERE trade_tickets.id = orders.trade_ticket_id
                LIMIT 1
            )
            WHERE trade_ticket_id IS NOT NULL
              AND (kalshi_env IS NULL OR kalshi_env = '')
            """
        )


def _backfill_fills_env(dialect_name: str) -> None:
    if dialect_name == "postgresql":
        op.execute(
            """
            UPDATE fills
            SET kalshi_env = orders.kalshi_env
            FROM orders
            WHERE fills.order_id = orders.id
              AND (fills.kalshi_env IS NULL OR fills.kalshi_env = '')
            """
        )
        return
    if dialect_name == "sqlite":
        op.execute(
            """
            UPDATE fills
            SET kalshi_env = (
                SELECT orders.kalshi_env
                FROM orders
                WHERE orders.id = fills.order_id
                LIMIT 1
            )
            WHERE order_id IS NOT NULL
              AND (kalshi_env IS NULL OR kalshi_env = '')
            """
        )


def _backfill_ops_events_env(dialect_name: str) -> None:
    if dialect_name == "postgresql":
        op.execute(
            """
            UPDATE ops_events
            SET kalshi_env = rooms.kalshi_env
            FROM rooms
            WHERE ops_events.room_id = rooms.id
              AND (ops_events.kalshi_env IS NULL OR ops_events.kalshi_env = '')
            """
        )
        op.execute(
            """
            UPDATE ops_events
            SET kalshi_env = NULLIF(payload ->> 'kalshi_env', '')
            WHERE (kalshi_env IS NULL OR kalshi_env = '')
              AND payload IS NOT NULL
            """
        )
        return
    if dialect_name == "sqlite":
        op.execute(
            """
            UPDATE ops_events
            SET kalshi_env = (
                SELECT rooms.kalshi_env
                FROM rooms
                WHERE rooms.id = ops_events.room_id
                LIMIT 1
            )
            WHERE room_id IS NOT NULL
              AND (kalshi_env IS NULL OR kalshi_env = '')
            """
        )


def _rename_deployment_control_default_row() -> None:
    op.execute(
        """
        UPDATE deployment_control
        SET id = 'demo'
        WHERE id = 'default'
          AND NOT EXISTS (
              SELECT 1 FROM deployment_control existing WHERE existing.id = 'demo'
          )
        """
    )
    op.execute("DELETE FROM deployment_control WHERE id = 'default'")


def _recreate_market_state() -> None:
    op.drop_index("ix_market_state_observed_at", table_name="market_state")
    op.drop_table("market_state")
    op.create_table(
        "market_state",
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_trade_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("snapshot", sa.JSON(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("kalshi_env", "market_ticker"),
    )
    op.create_index("ix_market_state_observed_at", "market_state", ["observed_at"], unique=False)


def _recreate_market_price_history() -> None:
    op.drop_index("ix_market_price_history_ticker_observed", table_name="market_price_history")
    op.drop_table("market_price_history")
    op.create_table(
        "market_price_history",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("mid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_trade_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_market_price_history_kalshi_env", "market_price_history", ["kalshi_env"], unique=False)
    op.create_index(
        "ix_market_price_history_ticker_observed",
        "market_price_history",
        ["market_ticker", "observed_at"],
        unique=False,
    )


def upgrade() -> None:
    bind = op.get_bind()
    dialect_name = bind.dialect.name

    _rename_deployment_control_default_row()

    op.execute("UPDATE positions SET kalshi_env = 'demo' WHERE kalshi_env IS NULL OR kalshi_env = ''")
    with op.batch_alter_table("positions") as batch_op:
        batch_op.alter_column("kalshi_env", existing_type=sa.String(length=16), nullable=False, server_default="demo")
        batch_op.create_index("ix_positions_kalshi_env", ["kalshi_env"], unique=False)
        batch_op.drop_constraint("uq_positions_market_subaccount", type_="unique")
        batch_op.create_unique_constraint(
            "uq_positions_env_market_subaccount",
            ["kalshi_env", "market_ticker", "subaccount"],
        )

    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("kalshi_env", sa.String(length=16), nullable=True))
        batch_op.create_index("ix_orders_kalshi_env", ["kalshi_env"], unique=False)
    _backfill_orders_env(dialect_name)
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_constraint("uq_orders_client_order_id", type_="unique")
        batch_op.create_unique_constraint(
            "uq_orders_env_client_order_id",
            ["kalshi_env", "client_order_id"],
        )

    with op.batch_alter_table("fills") as batch_op:
        batch_op.add_column(sa.Column("kalshi_env", sa.String(length=16), nullable=True))
        batch_op.create_index("ix_fills_kalshi_env", ["kalshi_env"], unique=False)
    _backfill_fills_env(dialect_name)
    with op.batch_alter_table("fills") as batch_op:
        batch_op.drop_constraint("uq_fills_trade_id", type_="unique")
        batch_op.create_unique_constraint("uq_fills_env_trade_id", ["kalshi_env", "trade_id"])

    with op.batch_alter_table("ops_events") as batch_op:
        batch_op.add_column(sa.Column("kalshi_env", sa.String(length=16), nullable=True))
        batch_op.create_index("ix_ops_events_kalshi_env", ["kalshi_env"], unique=False)
    _backfill_ops_events_env(dialect_name)

    _recreate_market_state()
    _recreate_market_price_history()


def downgrade() -> None:
    op.drop_index("ix_market_price_history_ticker_observed", table_name="market_price_history")
    op.drop_index("ix_market_price_history_kalshi_env", table_name="market_price_history")
    op.drop_table("market_price_history")
    op.create_table(
        "market_price_history",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("mid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_trade_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_market_price_history_ticker_observed",
        "market_price_history",
        ["market_ticker", "observed_at"],
        unique=False,
    )

    op.drop_index("ix_market_state_observed_at", table_name="market_state")
    op.drop_table("market_state")
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

    with op.batch_alter_table("ops_events") as batch_op:
        batch_op.drop_index("ix_ops_events_kalshi_env")
        batch_op.drop_column("kalshi_env")

    with op.batch_alter_table("fills") as batch_op:
        batch_op.drop_constraint("uq_fills_env_trade_id", type_="unique")
        batch_op.create_unique_constraint("uq_fills_trade_id", ["trade_id"])
        batch_op.drop_index("ix_fills_kalshi_env")
        batch_op.drop_column("kalshi_env")

    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_constraint("uq_orders_env_client_order_id", type_="unique")
        batch_op.create_unique_constraint("uq_orders_client_order_id", ["client_order_id"])
        batch_op.drop_index("ix_orders_kalshi_env")
        batch_op.drop_column("kalshi_env")

    with op.batch_alter_table("positions") as batch_op:
        batch_op.drop_constraint("uq_positions_env_market_subaccount", type_="unique")
        batch_op.create_unique_constraint("uq_positions_market_subaccount", ["market_ticker", "subaccount"])
        batch_op.drop_index("ix_positions_kalshi_env")
        batch_op.alter_column("kalshi_env", existing_type=sa.String(length=16), nullable=False, server_default="")

    op.execute(
        """
        UPDATE deployment_control
        SET id = 'default'
        WHERE id = 'demo'
          AND NOT EXISTS (
              SELECT 1 FROM deployment_control existing WHERE existing.id = 'default'
          )
        """
    )
    op.execute("DELETE FROM deployment_control WHERE id = 'demo'")
