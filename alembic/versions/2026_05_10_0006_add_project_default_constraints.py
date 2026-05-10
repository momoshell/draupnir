"""Add project default currency and unit-system constraints.

Legacy rows may still contain free-form defaults from before these checks
existed. Canonicalize recognized values and clear everything else to NULL
before adding the constraints so the migration can succeed on existing data.

Revision ID: 2026_05_10_0006
Revises: 2026_05_05_0005
Create Date: 2026-05-10 07:45:00.000000

"""
from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_10_0006"
down_revision: str | None = "2026_05_05_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply migration changes."""
    op.execute(
        """
        UPDATE projects
        SET default_unit_system = CASE
            WHEN lower(btrim(default_unit_system)) IN ('metric', 'imperial')
                THEN lower(btrim(default_unit_system))
            ELSE NULL
        END
        WHERE default_unit_system IS NOT NULL
          AND (
              default_unit_system <> lower(btrim(default_unit_system))
              OR lower(btrim(default_unit_system)) NOT IN ('metric', 'imperial')
          )
        """
    )
    op.execute(
        """
        UPDATE projects
        SET default_currency = CASE
            WHEN upper(btrim(default_currency)) ~ '^[A-Z]{3}$'
                THEN upper(btrim(default_currency))
            ELSE NULL
        END
        WHERE default_currency IS NOT NULL
          AND (
              default_currency <> upper(btrim(default_currency))
              OR upper(btrim(default_currency)) !~ '^[A-Z]{3}$'
          )
        """
    )
    op.create_check_constraint(
        "ck_projects_default_unit_system",
        "projects",
        "default_unit_system IS NULL OR default_unit_system IN ('metric', 'imperial')",
    )
    op.create_check_constraint(
        "ck_projects_default_currency",
        "projects",
        "default_currency IS NULL OR default_currency ~ '^[A-Z]{3}$'",
    )


def downgrade() -> None:
    """Revert migration changes."""
    op.drop_constraint("ck_projects_default_currency", "projects", type_="check")
    op.drop_constraint("ck_projects_default_unit_system", "projects", type_="check")
