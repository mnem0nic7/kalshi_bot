"""Add weather empirical bootstrap evidence tables.

Revision ID: 20260511_0031
Revises: 20260504_0030
Create Date: 2026-05-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260511_0031"
down_revision = "20260504_0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "weather_bootstrap_events",
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("series_ticker", sa.String(length=64), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=True),
        sa.Column("bucket_key", sa.String(length=512), nullable=True),
        sa.Column("policy_key", sa.String(length=512), nullable=True),
        sa.Column("tier", sa.String(length=32), nullable=True),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("edge_bps", sa.Integer(), nullable=True),
        sa.Column("size_factor", sa.Float(), nullable=True),
        sa.Column("count_fp", sa.Numeric(10, 2), nullable=True),
        sa.Column("notional_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("pnl_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("evidence_weight", sa.Float(), nullable=False, server_default=sa.text("1.0")),
        sa.Column("source", sa.String(length=64), nullable=False, server_default="live"),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("room_id", sa.String(length=36), nullable=True),
        sa.Column("decision_trace_id", sa.String(length=36), nullable=True),
        sa.Column("order_id", sa.String(length=36), nullable=True),
        sa.Column("fill_id", sa.String(length=36), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["decision_trace_id"], ["decision_traces.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["fill_id"], ["fills.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    for name, columns in {
        "ix_weather_bootstrap_events_kalshi_env": ["kalshi_env"],
        "ix_weather_bootstrap_events_market_ticker": ["market_ticker"],
        "ix_weather_bootstrap_events_series_ticker": ["series_ticker"],
        "ix_weather_bootstrap_events_local_market_day": ["local_market_day"],
        "ix_weather_bootstrap_events_bucket_key": ["bucket_key"],
        "ix_weather_bootstrap_events_policy_key": ["policy_key"],
        "ix_weather_bootstrap_events_tier": ["tier"],
        "ix_weather_bootstrap_events_event_type": ["event_type"],
        "ix_weather_bootstrap_events_status": ["status"],
        "ix_weather_bootstrap_events_side": ["side"],
        "ix_weather_bootstrap_events_source": ["source"],
        "ix_weather_bootstrap_events_occurred_at": ["occurred_at"],
        "ix_weather_bootstrap_events_room_id": ["room_id"],
        "ix_weather_bootstrap_events_decision_trace_id": ["decision_trace_id"],
        "ix_weather_bootstrap_events_order_id": ["order_id"],
        "ix_weather_bootstrap_events_fill_id": ["fill_id"],
        "ix_weather_bootstrap_events_env_bucket_time": ["kalshi_env", "bucket_key", "occurred_at"],
        "ix_weather_bootstrap_events_env_series_day": ["kalshi_env", "series_ticker", "local_market_day"],
    }.items():
        op.create_index(name, "weather_bootstrap_events", columns, if_not_exists=True)

    op.create_table(
        "weather_bootstrap_historical_evidence",
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("series_ticker", sa.String(length=64), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=True),
        sa.Column("bucket_key", sa.String(length=512), nullable=True),
        sa.Column("policy_key", sa.String(length=512), nullable=True),
        sa.Column("tier", sa.String(length=32), nullable=True),
        sa.Column("replay_version", sa.String(length=128), nullable=False),
        sa.Column("source_fingerprint", sa.String(length=128), nullable=False),
        sa.Column("strict_replay", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("edge_bps", sa.Integer(), nullable=True),
        sa.Column("count_fp", sa.Numeric(10, 2), nullable=True),
        sa.Column("notional_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("pnl_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("evidence_weight", sa.Float(), nullable=False, server_default=sa.text("1.0")),
        sa.Column("outcome", sa.String(length=32), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "kalshi_env",
            "source_fingerprint",
            "market_ticker",
            "bucket_key",
            "policy_key",
            name="uq_weather_bootstrap_historical_source_market_bucket_policy",
        ),
        if_not_exists=True,
    )
    for name, columns in {
        "ix_weather_bootstrap_historical_evidence_kalshi_env": ["kalshi_env"],
        "ix_weather_bootstrap_historical_evidence_market_ticker": ["market_ticker"],
        "ix_weather_bootstrap_historical_evidence_series_ticker": ["series_ticker"],
        "ix_weather_bootstrap_historical_evidence_local_market_day": ["local_market_day"],
        "ix_weather_bootstrap_historical_evidence_bucket_key": ["bucket_key"],
        "ix_weather_bootstrap_historical_evidence_policy_key": ["policy_key"],
        "ix_weather_bootstrap_historical_evidence_tier": ["tier"],
        "ix_weather_bootstrap_historical_evidence_replay_version": ["replay_version"],
        "ix_weather_bootstrap_historical_evidence_source_fingerprint": ["source_fingerprint"],
        "ix_weather_bootstrap_historical_evidence_strict_replay": ["strict_replay"],
        "ix_weather_bootstrap_historical_evidence_side": ["side"],
        "ix_weather_bootstrap_historical_evidence_outcome": ["outcome"],
        "ix_weather_bootstrap_historical_evidence_observed_at": ["observed_at"],
        "ix_weather_bootstrap_hist_env_bucket_time": ["kalshi_env", "bucket_key", "observed_at"],
    }.items():
        op.create_index(name, "weather_bootstrap_historical_evidence", columns, if_not_exists=True)


def downgrade() -> None:
    for name in (
        "ix_weather_bootstrap_hist_env_bucket_time",
        "ix_weather_bootstrap_historical_evidence_observed_at",
        "ix_weather_bootstrap_historical_evidence_outcome",
        "ix_weather_bootstrap_historical_evidence_side",
        "ix_weather_bootstrap_historical_evidence_strict_replay",
        "ix_weather_bootstrap_historical_evidence_source_fingerprint",
        "ix_weather_bootstrap_historical_evidence_replay_version",
        "ix_weather_bootstrap_historical_evidence_tier",
        "ix_weather_bootstrap_historical_evidence_policy_key",
        "ix_weather_bootstrap_historical_evidence_bucket_key",
        "ix_weather_bootstrap_historical_evidence_local_market_day",
        "ix_weather_bootstrap_historical_evidence_series_ticker",
        "ix_weather_bootstrap_historical_evidence_market_ticker",
        "ix_weather_bootstrap_historical_evidence_kalshi_env",
    ):
        op.drop_index(name, table_name="weather_bootstrap_historical_evidence", if_exists=True)
    op.drop_table("weather_bootstrap_historical_evidence", if_exists=True)

    for name in (
        "ix_weather_bootstrap_events_env_series_day",
        "ix_weather_bootstrap_events_env_bucket_time",
        "ix_weather_bootstrap_events_fill_id",
        "ix_weather_bootstrap_events_order_id",
        "ix_weather_bootstrap_events_decision_trace_id",
        "ix_weather_bootstrap_events_room_id",
        "ix_weather_bootstrap_events_occurred_at",
        "ix_weather_bootstrap_events_source",
        "ix_weather_bootstrap_events_side",
        "ix_weather_bootstrap_events_status",
        "ix_weather_bootstrap_events_event_type",
        "ix_weather_bootstrap_events_tier",
        "ix_weather_bootstrap_events_policy_key",
        "ix_weather_bootstrap_events_bucket_key",
        "ix_weather_bootstrap_events_local_market_day",
        "ix_weather_bootstrap_events_series_ticker",
        "ix_weather_bootstrap_events_market_ticker",
        "ix_weather_bootstrap_events_kalshi_env",
    ):
        op.drop_index(name, table_name="weather_bootstrap_events", if_exists=True)
    op.drop_table("weather_bootstrap_events", if_exists=True)
