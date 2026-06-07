"""Add extraction profile PDF input mode.

Revision ID: 2026_06_07_0030
Revises: 2026_06_04_0029
Create Date: 2026-06-07 12:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_07_0030"
down_revision: str | None = "2026_06_04_0029"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _assert_downgrade_safe() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("LOCK TABLE extraction_profiles IN ACCESS EXCLUSIVE MODE"))

    custom_mode_row_exists = bind.execute(
        sa.text(
            """
            SELECT 1
            FROM extraction_profiles
            WHERE pdf_input_mode <> 'auto'
            LIMIT 1
            """
        )
    ).scalar()
    if custom_mode_row_exists is not None:
        raise RuntimeError(
            "Cannot downgrade: persisted non-auto extraction_profiles.pdf_input_mode rows present"
        )


def upgrade() -> None:
    op.add_column(
        "extraction_profiles",
        sa.Column(
            "pdf_input_mode",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'auto'"),
            comment="PDF input mode selection",
        ),
    )
    op.create_check_constraint(
        "ck_extraction_profiles_pdf_input_mode_valid",
        "extraction_profiles",
        "pdf_input_mode IN ('auto', 'vector', 'raster')",
    )


def downgrade() -> None:
    _assert_downgrade_safe()
    op.drop_constraint(
        "ck_extraction_profiles_pdf_input_mode_valid",
        "extraction_profiles",
        type_="check",
    )
    op.drop_column("extraction_profiles", "pdf_input_mode")
