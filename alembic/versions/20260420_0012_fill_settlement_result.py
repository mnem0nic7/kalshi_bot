"""Add settlement_result to fills

Revision ID: 20260420_0012
Revises: 20260418_0011
Create Date: 2026-04-20
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260420_0012"
down_revision = "20260418_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("fills", sa.Column("settlement_result", sa.String(8), nullable=True))


def downgrade() -> None:
    op.drop_column("fills", "settlement_result")
