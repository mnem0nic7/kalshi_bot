"""Add durable deterministic decision traces.

Revision ID: 20260427_0024
Revises: 20260425_0023
Create Date: 2026-04-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260427_0024"
down_revision = "20260425_0023"
branch_labels = None
depends_on = None


def _jsonb() -> sa.TypeEngine:
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "decision_traces",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("room_id", sa.String(length=36), nullable=True),
        sa.Column("ticket_id", sa.String(length=36), nullable=True),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("decision_kind", sa.String(length=32), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("path_version", sa.String(length=64), nullable=False),
        sa.Column("agent_pack_version", sa.String(length=128), nullable=True),
        sa.Column("parameter_pack_version", sa.String(length=128), nullable=True),
        sa.Column("source_snapshot_ids", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column("trace_hash", sa.String(length=64), nullable=False),
        sa.Column("trace", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticket_id"], ["trade_tickets.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_decision_traces_agent_pack_version", "decision_traces", ["agent_pack_version"])
    op.create_index("ix_decision_traces_decision_kind", "decision_traces", ["decision_kind"])
    op.create_index("ix_decision_traces_decision_time", "decision_traces", ["decision_time"])
    op.create_index("ix_decision_traces_input_hash", "decision_traces", ["input_hash"])
    op.create_index("ix_decision_traces_kalshi_env", "decision_traces", ["kalshi_env"])
    op.create_index("ix_decision_traces_market_env_created", "decision_traces", ["market_ticker", "kalshi_env", "created_at"])
    op.create_index("ix_decision_traces_market_ticker", "decision_traces", ["market_ticker"])
    op.create_index("ix_decision_traces_parameter_pack_version", "decision_traces", ["parameter_pack_version"])
    op.create_index("ix_decision_traces_path_version", "decision_traces", ["path_version"])
    op.create_index("ix_decision_traces_room_created", "decision_traces", ["room_id", "created_at"])
    op.create_index("ix_decision_traces_room_id", "decision_traces", ["room_id"])
    op.create_index("ix_decision_traces_ticket_id", "decision_traces", ["ticket_id"])
    op.create_index("ix_decision_traces_trace_hash", "decision_traces", ["trace_hash"])


def downgrade() -> None:
    op.drop_index("ix_decision_traces_trace_hash", table_name="decision_traces")
    op.drop_index("ix_decision_traces_ticket_id", table_name="decision_traces")
    op.drop_index("ix_decision_traces_room_id", table_name="decision_traces")
    op.drop_index("ix_decision_traces_room_created", table_name="decision_traces")
    op.drop_index("ix_decision_traces_path_version", table_name="decision_traces")
    op.drop_index("ix_decision_traces_parameter_pack_version", table_name="decision_traces")
    op.drop_index("ix_decision_traces_market_ticker", table_name="decision_traces")
    op.drop_index("ix_decision_traces_market_env_created", table_name="decision_traces")
    op.drop_index("ix_decision_traces_kalshi_env", table_name="decision_traces")
    op.drop_index("ix_decision_traces_input_hash", table_name="decision_traces")
    op.drop_index("ix_decision_traces_decision_time", table_name="decision_traces")
    op.drop_index("ix_decision_traces_decision_kind", table_name="decision_traces")
    op.drop_index("ix_decision_traces_agent_pack_version", table_name="decision_traces")
    op.drop_table("decision_traces")
