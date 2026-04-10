"""research dossier schema

Revision ID: 20260410_0002
Revises: 20260410_0001
Create Date: 2026-04-10 00:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260410_0002"
down_revision = "20260410_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "research_dossiers",
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("source_count", sa.Integer(), nullable=False),
        sa.Column("contradiction_count", sa.Integer(), nullable=False),
        sa.Column("unresolved_count", sa.Integer(), nullable=False),
        sa.Column("settlement_covered", sa.Boolean(), nullable=False),
        sa.Column("last_run_id", sa.String(length=36), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("market_ticker"),
    )
    op.create_index("ix_research_dossiers_last_run_id", "research_dossiers", ["last_run_id"], unique=False)
    op.create_index("ix_research_dossiers_expires_at", "research_dossiers", ["expires_at"], unique=False)

    op.create_table(
        "research_runs",
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("trigger_reason", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_research_runs_market_ticker", "research_runs", ["market_ticker"], unique=False)
    op.create_index("ix_research_runs_status", "research_runs", ["status"], unique=False)
    op.create_index("ix_research_runs_started_at", "research_runs", ["started_at"], unique=False)

    op.create_table(
        "research_sources",
        sa.Column("research_run_id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("source_key", sa.String(length=255), nullable=False),
        sa.Column("source_class", sa.String(length=64), nullable=False),
        sa.Column("trust_tier", sa.String(length=32), nullable=False),
        sa.Column("publisher", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("snippet", sa.Text(), nullable=False),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["research_run_id"], ["research_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_research_sources_research_run_id", "research_sources", ["research_run_id"], unique=False)
    op.create_index("ix_research_sources_market_ticker", "research_sources", ["market_ticker"], unique=False)
    op.create_index("ix_research_sources_source_key", "research_sources", ["source_key"], unique=False)
    op.create_index("ix_research_sources_retrieved_at", "research_sources", ["retrieved_at"], unique=False)

    op.create_table(
        "research_claims",
        sa.Column("research_run_id", sa.String(length=36), nullable=False),
        sa.Column("research_source_id", sa.String(length=36), nullable=True),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("source_key", sa.String(length=255), nullable=False),
        sa.Column("claim_text", sa.Text(), nullable=False),
        sa.Column("stance", sa.String(length=32), nullable=False),
        sa.Column("settlement_critical", sa.Boolean(), nullable=False),
        sa.Column("freshness_seconds", sa.Integer(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["research_run_id"], ["research_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["research_source_id"], ["research_sources.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_research_claims_research_run_id", "research_claims", ["research_run_id"], unique=False)
    op.create_index("ix_research_claims_research_source_id", "research_claims", ["research_source_id"], unique=False)
    op.create_index("ix_research_claims_market_ticker", "research_claims", ["market_ticker"], unique=False)
    op.create_index("ix_research_claims_source_key", "research_claims", ["source_key"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_research_claims_source_key", table_name="research_claims")
    op.drop_index("ix_research_claims_market_ticker", table_name="research_claims")
    op.drop_index("ix_research_claims_research_source_id", table_name="research_claims")
    op.drop_index("ix_research_claims_research_run_id", table_name="research_claims")
    op.drop_table("research_claims")

    op.drop_index("ix_research_sources_retrieved_at", table_name="research_sources")
    op.drop_index("ix_research_sources_source_key", table_name="research_sources")
    op.drop_index("ix_research_sources_market_ticker", table_name="research_sources")
    op.drop_index("ix_research_sources_research_run_id", table_name="research_sources")
    op.drop_table("research_sources")

    op.drop_index("ix_research_runs_started_at", table_name="research_runs")
    op.drop_index("ix_research_runs_status", table_name="research_runs")
    op.drop_index("ix_research_runs_market_ticker", table_name="research_runs")
    op.drop_table("research_runs")

    op.drop_index("ix_research_dossiers_expires_at", table_name="research_dossiers")
    op.drop_index("ix_research_dossiers_last_run_id", table_name="research_dossiers")
    op.drop_table("research_dossiers")
