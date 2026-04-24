"""Add decision corpus build and row tables.

Revision ID: 20260424_0022
Revises: 20260423_0021
Create Date: 2026-04-24
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260424_0022"
down_revision = "20260423_0021"
branch_labels = None
depends_on = None


SUPPORT_STATUS_VALUES = ("supported", "exploratory", "insufficient")
SUPPORT_LEVEL_VALUES = (
    "L1_station_season_lead_regime",
    "L2_station_season_lead",
    "L3_station_season",
    "L4_season_lead",
    "L5_global",
)
SOURCE_PROVENANCE_VALUES = (
    "historical_replay_full_checkpoint",
    "historical_replay_partial_checkpoint",
    "historical_replay_late_only",
    "historical_replay_external_forecast_repair",
    "historical_replay_unknown",
)


def _in_clause(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.create_table(
        "decision_corpus_builds",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("git_sha", sa.String(length=64), nullable=True),
        sa.Column("source", sa.JSON(), nullable=False),
        sa.Column("filters", sa.JSON(), nullable=False),
        sa.Column("date_from", sa.Date(), nullable=False),
        sa.Column("date_to", sa.Date(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("parent_build_id", sa.String(length=36), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["parent_build_id"], ["decision_corpus_builds.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_decision_corpus_builds_version", "decision_corpus_builds", ["version"])
    op.create_index("ix_decision_corpus_builds_status", "decision_corpus_builds", ["status"])
    op.create_index("ix_decision_corpus_builds_date_from", "decision_corpus_builds", ["date_from"])
    op.create_index("ix_decision_corpus_builds_date_to", "decision_corpus_builds", ["date_to"])
    op.create_index("ix_decision_corpus_builds_parent_build_id", "decision_corpus_builds", ["parent_build_id"])
    op.create_index("ix_decision_corpus_builds_finished_at", "decision_corpus_builds", ["finished_at"])

    op.create_table(
        "decision_corpus_rows",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("corpus_build_id", sa.String(length=36), nullable=False),
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("series_ticker", sa.String(length=128), nullable=True),
        sa.Column("station_id", sa.String(length=32), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=False),
        sa.Column("checkpoint_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("deployment_color", sa.String(length=16), nullable=True),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("policy_version", sa.String(length=128), nullable=False),
        sa.Column("source_asof_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quote_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quote_captured_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("time_to_settlement_at_checkpoint_minutes", sa.Integer(), nullable=True),
        sa.Column("fair_yes_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("edge_bps", sa.Integer(), nullable=True),
        sa.Column("recommended_side", sa.String(length=16), nullable=True),
        sa.Column("target_yes_price_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("eligibility_status", sa.String(length=32), nullable=True),
        sa.Column("stand_down_reason", sa.String(length=64), nullable=True),
        sa.Column("trade_regime", sa.String(length=64), nullable=True),
        sa.Column("liquidity_regime", sa.String(length=64), nullable=True),
        sa.Column("support_status", sa.String(length=32), nullable=False),
        sa.Column("support_level", sa.String(length=64), nullable=False),
        sa.Column("support_n", sa.Integer(), nullable=False),
        sa.Column("support_market_days", sa.Integer(), nullable=False),
        sa.Column("support_recency_days", sa.Integer(), nullable=True),
        sa.Column("backoff_path", sa.JSON(), nullable=False),
        sa.Column("settlement_result", sa.String(length=16), nullable=True),
        sa.Column("settlement_value_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("pnl_counterfactual_target_frictionless", sa.Numeric(12, 6), nullable=True),
        sa.Column("pnl_counterfactual_target_with_fees", sa.Numeric(12, 6), nullable=True),
        sa.Column("pnl_model_fair_frictionless", sa.Numeric(12, 6), nullable=True),
        sa.Column("pnl_executed_realized", sa.Numeric(12, 6), nullable=True),
        sa.Column("fee_counterfactual_dollars", sa.Numeric(12, 6), nullable=True),
        sa.Column("counterfactual_count", sa.Numeric(10, 2), nullable=True),
        sa.Column("executed_count", sa.Numeric(10, 2), nullable=True),
        sa.Column("fee_model_version", sa.String(length=64), nullable=True),
        sa.Column("source_provenance", sa.String(length=64), nullable=False),
        sa.Column("source_details", sa.JSON(), nullable=False),
        sa.Column("signal_payload", sa.JSON(), nullable=False),
        sa.Column("quote_snapshot", sa.JSON(), nullable=False),
        sa.Column("settlement_payload", sa.JSON(), nullable=False),
        sa.Column("diagnostics", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(f"support_status IN ({_in_clause(SUPPORT_STATUS_VALUES)})", name="ck_decision_corpus_support_status"),
        sa.CheckConstraint(f"support_level IN ({_in_clause(SUPPORT_LEVEL_VALUES)})", name="ck_decision_corpus_support_level"),
        sa.CheckConstraint(f"source_provenance IN ({_in_clause(SOURCE_PROVENANCE_VALUES)})", name="ck_decision_corpus_source_provenance"),
        sa.ForeignKeyConstraint(["corpus_build_id"], ["decision_corpus_builds.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "corpus_build_id",
            "room_id",
            "market_ticker",
            "checkpoint_ts",
            "policy_version",
            "model_version",
            name="uq_decision_corpus_row_identity",
        ),
    )
    for column in (
        "corpus_build_id",
        "room_id",
        "market_ticker",
        "series_ticker",
        "station_id",
        "local_market_day",
        "checkpoint_ts",
        "kalshi_env",
        "model_version",
        "policy_version",
        "source_asof_ts",
        "quote_observed_at",
        "quote_captured_at",
        "recommended_side",
        "eligibility_status",
        "stand_down_reason",
        "trade_regime",
        "liquidity_regime",
        "support_status",
        "support_level",
        "settlement_result",
        "fee_model_version",
        "source_provenance",
        "created_at",
    ):
        op.create_index(f"ix_decision_corpus_rows_{column}", "decision_corpus_rows", [column])
    op.create_index(
        "ix_decision_corpus_rows_day_env_policy",
        "decision_corpus_rows",
        ["local_market_day", "kalshi_env", "policy_version"],
    )
    op.create_index(
        "ix_decision_corpus_rows_series_day",
        "decision_corpus_rows",
        ["series_ticker", "local_market_day"],
    )


def downgrade() -> None:
    op.drop_index("ix_decision_corpus_rows_series_day", table_name="decision_corpus_rows")
    op.drop_index("ix_decision_corpus_rows_day_env_policy", table_name="decision_corpus_rows")
    for column in (
        "created_at",
        "source_provenance",
        "fee_model_version",
        "settlement_result",
        "support_level",
        "support_status",
        "liquidity_regime",
        "trade_regime",
        "stand_down_reason",
        "eligibility_status",
        "recommended_side",
        "quote_captured_at",
        "quote_observed_at",
        "source_asof_ts",
        "policy_version",
        "model_version",
        "kalshi_env",
        "checkpoint_ts",
        "local_market_day",
        "station_id",
        "series_ticker",
        "market_ticker",
        "room_id",
        "corpus_build_id",
    ):
        op.drop_index(f"ix_decision_corpus_rows_{column}", table_name="decision_corpus_rows")
    op.drop_table("decision_corpus_rows")
    op.drop_index("ix_decision_corpus_builds_finished_at", table_name="decision_corpus_builds")
    op.drop_index("ix_decision_corpus_builds_parent_build_id", table_name="decision_corpus_builds")
    op.drop_index("ix_decision_corpus_builds_date_to", table_name="decision_corpus_builds")
    op.drop_index("ix_decision_corpus_builds_date_from", table_name="decision_corpus_builds")
    op.drop_index("ix_decision_corpus_builds_status", table_name="decision_corpus_builds")
    op.drop_index("ix_decision_corpus_builds_version", table_name="decision_corpus_builds")
    op.drop_table("decision_corpus_builds")
