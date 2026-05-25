"""merge crypto funding and training migration heads

Revision ID: 20260525_0038
Revises: 20260516_0032, 20260523_0037
Create Date: 2026-05-25 20:34:00.000000
"""

from __future__ import annotations

from alembic import op  # noqa: F401
import sqlalchemy as sa  # noqa: F401


revision = "20260525_0038"
down_revision = ("20260516_0032", "20260523_0037")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
