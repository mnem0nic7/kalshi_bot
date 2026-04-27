"""Add source health log table.

Revision ID: 20260427_0026
Revises: 20260427_0025
Create Date: 2026-04-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260427_0026"
down_revision = "20260427_0025"
branch_labels = None
depends_on = None


def _jsonb() -> sa.TypeEngine:
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "source_health_logs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False, server_default="demo"),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("is_aggregate", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("market_ticker", sa.String(length=128), nullable=True),
        sa.Column("station_id", sa.String(length=32), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("label", sa.String(length=16), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("success_score", sa.Float(), nullable=False),
        sa.Column("freshness_score", sa.Float(), nullable=False),
        sa.Column("completeness_score", sa.Float(), nullable=False),
        sa.Column("consistency_score", sa.Float(), nullable=False),
        sa.Column("payload", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_source_health_logs_is_aggregate", "source_health_logs", ["is_aggregate"])
    op.create_index("ix_source_health_logs_kalshi_env", "source_health_logs", ["kalshi_env"])
    op.create_index("ix_source_health_logs_label", "source_health_logs", ["label"])
    op.create_index("ix_source_health_logs_market_ticker", "source_health_logs", ["market_ticker"])
    op.create_index("ix_source_health_logs_observed_at", "source_health_logs", ["observed_at"])
    op.create_index("ix_source_health_logs_source", "source_health_logs", ["source"])
    op.create_index("ix_source_health_logs_station_id", "source_health_logs", ["station_id"])
    op.create_index(
        "ix_source_health_logs_env_source_observed",
        "source_health_logs",
        ["kalshi_env", "source", "observed_at"],
    )
    op.create_index(
        "ix_source_health_logs_env_aggregate_observed",
        "source_health_logs",
        ["kalshi_env", "is_aggregate", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_source_health_logs_env_aggregate_observed", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_env_source_observed", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_station_id", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_source", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_observed_at", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_market_ticker", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_label", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_kalshi_env", table_name="source_health_logs")
    op.drop_index("ix_source_health_logs_is_aggregate", table_name="source_health_logs")
    op.drop_table("source_health_logs")
