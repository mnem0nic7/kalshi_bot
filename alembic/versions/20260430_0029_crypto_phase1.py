"""Add crypto market data and model artifacts.

Revision ID: 20260430_0029
Revises: 20260430_0028
Create Date: 2026-04-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260430_0029"
down_revision = "20260430_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crypto_market_snapshots",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("series_ticker", sa.String(length=64), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("event_ticker", sa.String(length=128), nullable=True),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=True),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "expected_expiration_time", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("target_price_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("no_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("no_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_price_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column("open_interest", sa.Integer(), nullable=True),
        sa.Column("settlement_result", sa.String(length=16), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "kalshi_env",
            "market_ticker",
            "observed_at",
            name="uq_crypto_market_snapshot_observed",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_kalshi_env",
        "crypto_market_snapshots",
        ["kalshi_env"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_series_ticker",
        "crypto_market_snapshots",
        ["series_ticker"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_market_ticker",
        "crypto_market_snapshots",
        ["market_ticker"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_event_ticker",
        "crypto_market_snapshots",
        ["event_ticker"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_asset_symbol",
        "crypto_market_snapshots",
        ["asset_symbol"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_frequency",
        "crypto_market_snapshots",
        ["frequency"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_status",
        "crypto_market_snapshots",
        ["status"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_close_time",
        "crypto_market_snapshots",
        ["close_time"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_settlement_result",
        "crypto_market_snapshots",
        ["settlement_result"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_observed_at",
        "crypto_market_snapshots",
        ["observed_at"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_frequency_status",
        "crypto_market_snapshots",
        ["frequency", "status"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_snapshots_market_observed",
        "crypto_market_snapshots",
        ["market_ticker", "observed_at"],
        if_not_exists=True,
    )

    op.create_table(
        "crypto_market_candlesticks",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("series_ticker", sa.String(length=64), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("period_interval", sa.Integer(), nullable=False),
        sa.Column("end_period_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("high_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("low_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("close_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "kalshi_env",
            "market_ticker",
            "period_interval",
            "end_period_ts",
            name="uq_crypto_candle_period",
        ),
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_candlesticks_kalshi_env",
        "crypto_market_candlesticks",
        ["kalshi_env"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_candlesticks_series_ticker",
        "crypto_market_candlesticks",
        ["series_ticker"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_candlesticks_market_ticker",
        "crypto_market_candlesticks",
        ["market_ticker"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_candlesticks_asset_symbol",
        "crypto_market_candlesticks",
        ["asset_symbol"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_candlesticks_frequency",
        "crypto_market_candlesticks",
        ["frequency"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_market_candlesticks_end_period_ts",
        "crypto_market_candlesticks",
        ["end_period_ts"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_candles_market_period",
        "crypto_market_candlesticks",
        ["market_ticker", "end_period_ts"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_candles_series_period",
        "crypto_market_candlesticks",
        ["series_ticker", "end_period_ts"],
        if_not_exists=True,
    )

    op.create_table(
        "crypto_model_artifacts",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("trained_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_model_artifacts_kalshi_env",
        "crypto_model_artifacts",
        ["kalshi_env"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_model_artifacts_frequency",
        "crypto_model_artifacts",
        ["frequency"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_model_artifacts_artifact_type",
        "crypto_model_artifacts",
        ["artifact_type"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_model_artifacts_version",
        "crypto_model_artifacts",
        ["version"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_model_artifacts_status",
        "crypto_model_artifacts",
        ["status"],
        if_not_exists=True,
    )
    op.create_index(
        "ix_crypto_model_artifacts_frequency_type",
        "crypto_model_artifacts",
        ["frequency", "artifact_type", "created_at"],
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_crypto_model_artifacts_frequency_type",
        table_name="crypto_model_artifacts",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_model_artifacts_status",
        table_name="crypto_model_artifacts",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_model_artifacts_version",
        table_name="crypto_model_artifacts",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_model_artifacts_artifact_type",
        table_name="crypto_model_artifacts",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_model_artifacts_frequency",
        table_name="crypto_model_artifacts",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_model_artifacts_kalshi_env",
        table_name="crypto_model_artifacts",
        if_exists=True,
    )
    op.drop_table("crypto_model_artifacts", if_exists=True)

    op.drop_index(
        "ix_crypto_candles_series_period",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_candles_market_period",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_candlesticks_end_period_ts",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_candlesticks_frequency",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_candlesticks_asset_symbol",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_candlesticks_market_ticker",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_candlesticks_series_ticker",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_candlesticks_kalshi_env",
        table_name="crypto_market_candlesticks",
        if_exists=True,
    )
    op.drop_table("crypto_market_candlesticks", if_exists=True)

    op.drop_index(
        "ix_crypto_market_snapshots_market_observed",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_frequency_status",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_observed_at",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_settlement_result",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_close_time",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_status",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_frequency",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_asset_symbol",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_event_ticker",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_market_ticker",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_series_ticker",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_index(
        "ix_crypto_market_snapshots_kalshi_env",
        table_name="crypto_market_snapshots",
        if_exists=True,
    )
    op.drop_table("crypto_market_snapshots", if_exists=True)
