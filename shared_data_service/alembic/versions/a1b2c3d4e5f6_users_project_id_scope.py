"""users.project_id — nested/scoped routing

Adds the scope column a user belongs to (see the /{project_id}/user routes
in app/modules/shared/routes.py). Nullable: rows created via the top-level
/users route (and any pre-existing rows) have no project.

Revision ID: a1b2c3d4e5f6
Revises: 234e3011d09a
Create Date: 2026-07-20 00:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "234e3011d09a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users", sa.Column("project_id", sa.Uuid(), nullable=True)
    )
    op.create_index(
        op.f("ix_users_project_id"), "users", ["project_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_users_project_id"), table_name="users")
    op.drop_column("users", "project_id")
