"""Add deterministic parameter pack table.

Revision ID: 20260427_0027
Revises: 20260427_0026
Create Date: 2026-04-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260427_0027"
down_revision = "20260427_0026"
branch_labels = None
depends_on = None


def _jsonb() -> sa.TypeEngine:
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "parameter_packs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="candidate"),
        sa.Column("parent_version", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False, server_default="builtin"),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("pack_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("holdout_report", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version", name="uq_parameter_packs_version"),
    )
    op.create_index("ix_parameter_packs_pack_hash", "parameter_packs", ["pack_hash"])
    op.create_index("ix_parameter_packs_parent_version", "parameter_packs", ["parent_version"])
    op.create_index("ix_parameter_packs_status", "parameter_packs", ["status"])
    op.create_index("ix_parameter_packs_version", "parameter_packs", ["version"])


def downgrade() -> None:
    op.drop_index("ix_parameter_packs_version", table_name="parameter_packs")
    op.drop_index("ix_parameter_packs_status", table_name="parameter_packs")
    op.drop_index("ix_parameter_packs_parent_version", table_name="parameter_packs")
    op.drop_index("ix_parameter_packs_pack_hash", table_name="parameter_packs")
    op.drop_table("parameter_packs")
