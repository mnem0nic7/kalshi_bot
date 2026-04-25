"""Add auto-evolve promotion and city assignment history tables.

Revision ID: 20260425_0023
Revises: 20260424_0022
Create Date: 2026-04-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260425_0023"
down_revision = "20260424_0022"
branch_labels = None
depends_on = None


def _jsonb() -> sa.TypeEngine:
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.alter_column("trade_tickets", "strategy_code", existing_type=sa.String(length=16), type_=sa.String(length=64))
    op.alter_column("orders", "strategy_code", existing_type=sa.String(length=16), type_=sa.String(length=64))
    op.alter_column("fills", "strategy_code", existing_type=sa.String(length=16), type_=sa.String(length=64))
    op.add_column("strategy_results", sa.Column("corpus_build_id", sa.String(length=32), nullable=True))
    op.create_foreign_key(
        "fk_strategy_results_corpus_build_id",
        "strategy_results",
        "decision_corpus_builds",
        ["corpus_build_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_strategy_results_corpus_build_id", "strategy_results", ["corpus_build_id"])
    op.add_column(
        "city_strategy_assignments",
        sa.Column("kalshi_env", sa.String(length=16), nullable=False, server_default="demo"),
    )
    op.drop_constraint("city_strategy_assignments_pkey", "city_strategy_assignments", type_="primary")
    op.create_primary_key(
        "city_strategy_assignments_pkey",
        "city_strategy_assignments",
        ["kalshi_env", "series_ticker"],
    )
    op.add_column("city_strategy_assignments", sa.Column("evidence_corpus_build_id", sa.String(length=32), nullable=True))
    op.add_column("city_strategy_assignments", sa.Column("evidence_run_at", sa.DateTime(timezone=True), nullable=True))
    op.create_foreign_key(
        "fk_city_strategy_assignments_evidence_corpus_build_id",
        "city_strategy_assignments",
        "decision_corpus_builds",
        ["evidence_corpus_build_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_city_strategy_assignments_evidence_corpus_build_id",
        "city_strategy_assignments",
        ["evidence_corpus_build_id"],
    )

    op.create_table(
        "strategy_promotions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("promoted_strategy_name", sa.String(length=64), nullable=False),
        sa.Column("promoted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("previous_city_assignments", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("new_city_assignments", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("baseline_metrics", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("rollback_metrics", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("promotion_details", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("rollback_details", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("rollback_skipped_cities", _jsonb(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("watchdog_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("watchdog_extended_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("watchdog_status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("watchdog_extended_reason", sa.String(length=64), nullable=True),
        sa.Column("watchdog_extended_detail", sa.Text(), nullable=True),
        sa.Column("watchdog_last_eval_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("watchdog_last_eval_reason", sa.String(length=128), nullable=True),
        sa.Column("rollback_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rollback_trigger", sa.String(length=128), nullable=True),
        sa.Column("trigger_source", sa.String(length=64), nullable=True),
        sa.Column("resolution_data", _jsonb(), nullable=True),
        sa.Column("secondary_sync_status", sa.String(length=32), nullable=False, server_default="not_applicable"),
        sa.Column("secondary_sync_error", sa.Text(), nullable=True),
        sa.Column("secondary_sync_resolution", _jsonb(), nullable=True),
        sa.Column("secondary_rollback_status", sa.String(length=32), nullable=False, server_default="not_applicable"),
        sa.Column("secondary_rollback_error", sa.Text(), nullable=True),
        sa.Column("secondary_rollback_resolution", _jsonb(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "watchdog_status IN ('pending', 'extended', 'passed', 'rolled_back', 'insufficient_data')",
            name="ck_strategy_promotions_watchdog_status",
        ),
        sa.CheckConstraint(
            "secondary_sync_status IN ('pending', 'failed', 'synced', 'ignored_by_operator', 'not_applicable')",
            name="ck_strategy_promotions_secondary_sync_status",
        ),
        sa.CheckConstraint(
            "secondary_rollback_status IN ('pending', 'failed', 'synced', 'ignored_by_operator', 'not_applicable')",
            name="ck_strategy_promotions_secondary_rollback_status",
        ),
    )
    op.create_index("ix_strategy_promotions_env_status_due", "strategy_promotions", ["kalshi_env", "watchdog_status", "watchdog_due_at"])
    op.create_index("ix_strategy_promotions_strategy", "strategy_promotions", ["promoted_strategy_name"])

    op.create_table(
        "city_assignment_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("series_ticker", sa.String(length=64), nullable=False),
        sa.Column("previous_strategy", sa.String(length=64), nullable=True),
        sa.Column("new_strategy", sa.String(length=64), nullable=True),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("promotion_id", sa.Integer(), nullable=True),
        sa.Column("metadata", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["promotion_id"], ["strategy_promotions.id"], ondelete="SET NULL"),
        sa.CheckConstraint(
            "event_type IN ('auto_evolve_assign', 'manual_assign', 'manual_override', 'rollback_restore', 'rollback_delete')",
            name="ck_city_assignment_events_event_type",
        ),
    )
    op.create_index(
        "ix_city_assignment_events_env_city_created",
        "city_assignment_events",
        ["kalshi_env", "series_ticker", "created_at"],
    )
    op.create_index("ix_city_assignment_events_promotion", "city_assignment_events", ["promotion_id"])


def downgrade() -> None:
    op.drop_index("ix_city_assignment_events_promotion", table_name="city_assignment_events")
    op.drop_index("ix_city_assignment_events_env_city_created", table_name="city_assignment_events")
    op.drop_table("city_assignment_events")
    op.drop_index("ix_strategy_promotions_strategy", table_name="strategy_promotions")
    op.drop_index("ix_strategy_promotions_env_status_due", table_name="strategy_promotions")
    op.drop_table("strategy_promotions")
    op.drop_index("ix_city_strategy_assignments_evidence_corpus_build_id", table_name="city_strategy_assignments")
    op.drop_constraint(
        "fk_city_strategy_assignments_evidence_corpus_build_id",
        "city_strategy_assignments",
        type_="foreignkey",
    )
    op.drop_column("city_strategy_assignments", "evidence_run_at")
    op.drop_column("city_strategy_assignments", "evidence_corpus_build_id")
    op.drop_constraint("city_strategy_assignments_pkey", "city_strategy_assignments", type_="primary")
    op.create_primary_key(
        "city_strategy_assignments_pkey",
        "city_strategy_assignments",
        ["series_ticker"],
    )
    op.drop_column("city_strategy_assignments", "kalshi_env")
    op.drop_index("ix_strategy_results_corpus_build_id", table_name="strategy_results")
    op.drop_constraint("fk_strategy_results_corpus_build_id", "strategy_results", type_="foreignkey")
    op.drop_column("strategy_results", "corpus_build_id")
    op.alter_column("fills", "strategy_code", existing_type=sa.String(length=64), type_=sa.String(length=16))
    op.alter_column("orders", "strategy_code", existing_type=sa.String(length=64), type_=sa.String(length=16))
    op.alter_column("trade_tickets", "strategy_code", existing_type=sa.String(length=64), type_=sa.String(length=16))
