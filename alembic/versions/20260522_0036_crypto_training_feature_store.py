"""Add crypto training feature-store tables.

Revision ID: 20260522_0036
Revises: 20260522_0035
Create Date: 2026-05-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260522_0036"
down_revision = "20260522_0035"
branch_labels = None
depends_on = None


def _json_default() -> sa.TextClause:
    return sa.text("'{}'")


def upgrade() -> None:
    op.create_table(
        "crypto_order_book_snapshots",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=128), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("best_bid_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("best_ask_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("mid_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("spread_bps", sa.Integer(), nullable=True),
        sa.Column("bid_depth", sa.Numeric(24, 8), nullable=True),
        sa.Column("ask_depth", sa.Numeric(24, 8), nullable=True),
        sa.Column("depth_imbalance", sa.Numeric(12, 8), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "kalshi_env",
            "provider",
            "asset_symbol",
            "frequency",
            "market_ticker",
            "observed_at",
            name="uq_crypto_order_book_snapshot_observed",
        ),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_order_books_asset_observed", "crypto_order_book_snapshots", ["asset_symbol", "observed_at"], if_not_exists=True)
    op.create_index("ix_crypto_order_books_market_observed", "crypto_order_book_snapshots", ["market_ticker", "observed_at"], if_not_exists=True)

    op.create_table(
        "crypto_trade_ticks",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("trade_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("price_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("size", sa.Numeric(24, 8), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kalshi_env", "provider", "asset_symbol", "source_id", "trade_id", name="uq_crypto_trade_tick_source_trade"),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_trade_ticks_asset_observed", "crypto_trade_ticks", ["asset_symbol", "observed_at"], if_not_exists=True)
    op.create_index("ix_crypto_trade_ticks_market_observed", "crypto_trade_ticks", ["market_ticker", "observed_at"], if_not_exists=True)

    op.create_table(
        "crypto_settlement_benchmark_windows",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("target_price_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("window_start_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_end_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("open_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("high_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("low_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("close_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("twap_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("vwap_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kalshi_env", "market_ticker", name="uq_crypto_settlement_window_market"),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_settlement_windows_asset_close", "crypto_settlement_benchmark_windows", ["asset_symbol", "window_end_ts"], if_not_exists=True)

    op.create_table(
        "crypto_decision_outcomes",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decision_kind", sa.String(length=32), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column("trace_hash", sa.String(length=64), nullable=True),
        sa.Column("model_version", sa.String(length=128), nullable=True),
        sa.Column("prediction_yes", sa.Numeric(10, 8), nullable=True),
        sa.Column("selected_side", sa.String(length=16), nullable=True),
        sa.Column("selected_price_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("selected_count_fp", sa.Numeric(10, 2), nullable=True),
        sa.Column("gate_status", sa.String(length=32), nullable=True),
        sa.Column("settlement_result", sa.String(length=16), nullable=True),
        sa.Column("simulated_pnl_dollars", sa.Numeric(12, 4), nullable=True),
        sa.Column("realized_pnl_dollars", sa.Numeric(12, 4), nullable=True),
        sa.Column("fill_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source_snapshot_ids", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kalshi_env", "market_ticker", "decision_time", "input_hash", name="uq_crypto_decision_outcome_input"),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_decision_outcomes_asset_time", "crypto_decision_outcomes", ["asset_symbol", "decision_time"], if_not_exists=True)
    op.create_index("ix_crypto_decision_outcomes_freq_time", "crypto_decision_outcomes", ["frequency", "decision_time"], if_not_exists=True)

    op.create_table(
        "crypto_training_feature_rows",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("row_id", sa.String(length=255), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("settlement_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("label_yes", sa.Integer(), nullable=True),
        sa.Column("strict_trade_eligible", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("feature_schema_version", sa.String(length=64), nullable=False),
        sa.Column("feature_hash", sa.String(length=64), nullable=False),
        sa.Column("source_build_id", sa.String(length=64), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kalshi_env", "frequency", "row_id", name="uq_crypto_training_feature_row"),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_training_feature_rows_asset_time", "crypto_training_feature_rows", ["asset_symbol", "decision_time"], if_not_exists=True)
    op.create_index("ix_crypto_training_feature_rows_freq_time", "crypto_training_feature_rows", ["frequency", "decision_time"], if_not_exists=True)

    op.create_table(
        "crypto_data_quality_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("run_kind", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_build_id", sa.String(length=64), nullable=False),
        sa.Column("window_start_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_end_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rows_materialized", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("strict_trade_eligible_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("spot_coverage_pct", sa.Float(), nullable=False, server_default="0"),
        sa.Column("decision_outcome_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metrics", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_data_quality_runs_asset_created", "crypto_data_quality_runs", ["asset_symbol", "created_at"], if_not_exists=True)
    op.create_index("ix_crypto_data_quality_runs_freq_created", "crypto_data_quality_runs", ["frequency", "created_at"], if_not_exists=True)

    op.create_table(
        "crypto_execution_examples",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("order_id", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("fill_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("requested_price_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("average_fill_price_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("requested_count_fp", sa.Numeric(10, 2), nullable=True),
        sa.Column("filled_count_fp", sa.Numeric(10, 2), nullable=True),
        sa.Column("slippage_bps", sa.Integer(), nullable=True),
        sa.Column("realized_pnl_dollars", sa.Numeric(12, 4), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=_json_default()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kalshi_env", "market_ticker", "decision_time", "order_id", name="uq_crypto_execution_example_order"),
        if_not_exists=True,
    )
    op.create_index("ix_crypto_execution_examples_asset_time", "crypto_execution_examples", ["asset_symbol", "decision_time"], if_not_exists=True)


def downgrade() -> None:
    for index_name, table_name in [
        ("ix_crypto_execution_examples_asset_time", "crypto_execution_examples"),
        ("ix_crypto_data_quality_runs_freq_created", "crypto_data_quality_runs"),
        ("ix_crypto_data_quality_runs_asset_created", "crypto_data_quality_runs"),
        ("ix_crypto_training_feature_rows_freq_time", "crypto_training_feature_rows"),
        ("ix_crypto_training_feature_rows_asset_time", "crypto_training_feature_rows"),
        ("ix_crypto_decision_outcomes_freq_time", "crypto_decision_outcomes"),
        ("ix_crypto_decision_outcomes_asset_time", "crypto_decision_outcomes"),
        ("ix_crypto_settlement_windows_asset_close", "crypto_settlement_benchmark_windows"),
        ("ix_crypto_trade_ticks_market_observed", "crypto_trade_ticks"),
        ("ix_crypto_trade_ticks_asset_observed", "crypto_trade_ticks"),
        ("ix_crypto_order_books_market_observed", "crypto_order_book_snapshots"),
        ("ix_crypto_order_books_asset_observed", "crypto_order_book_snapshots"),
    ]:
        op.drop_index(index_name, table_name=table_name, if_exists=True)
    for table_name in [
        "crypto_execution_examples",
        "crypto_data_quality_runs",
        "crypto_training_feature_rows",
        "crypto_decision_outcomes",
        "crypto_settlement_benchmark_windows",
        "crypto_trade_ticks",
        "crypto_order_book_snapshots",
    ]:
        op.drop_table(table_name, if_exists=True)
