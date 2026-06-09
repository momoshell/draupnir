"""Allow MAX_ATTEMPTS_EXCEEDED in the jobs error_code check constraint.

The worker now fails jobs that exhaust their retry budget with the new
``MAX_ATTEMPTS_EXCEEDED`` error code. The ``ck_jobs_error_code_valid`` check
constraint enumerates the allowed codes, so it must be widened to accept it.

Revision ID: 2026_06_08_0032
Revises: 2026_06_08_0031
Create Date: 2026-06-08 21:45:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_06_08_0032"
down_revision: str | None = "2026_06_08_0031"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_ERROR_CODES_WITH_MAX_ATTEMPTS = (
    "NOT_FOUND",
    "INVALID_CURSOR",
    "VALIDATION_ERROR",
    "INPUT_INVALID",
    "INPUT_UNSUPPORTED_FORMAT",
    "ADAPTER_UNAVAILABLE",
    "ADAPTER_TIMEOUT",
    "ADAPTER_FAILED",
    "STORAGE_FAILED",
    "DB_CONFLICT",
    "IDEMPOTENCY_CONFLICT",
    "REVISION_CONFLICT",
    "NORMALIZED_ENTITIES_NOT_MATERIALIZED",
    "JOB_CANCELLED",
    "MAX_ATTEMPTS_EXCEEDED",
    "INTERNAL_ERROR",
)
_ERROR_CODES_WITHOUT_MAX_ATTEMPTS = tuple(
    code for code in _ERROR_CODES_WITH_MAX_ATTEMPTS if code != "MAX_ATTEMPTS_EXCEEDED"
)


def _error_code_check(codes: Sequence[str]) -> str:
    rendered = ", ".join(f"'{code}'" for code in codes)
    return f"error_code IS NULL OR error_code IN ({rendered})"


def upgrade() -> None:
    op.drop_constraint("ck_jobs_error_code_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_error_code_valid",
        "jobs",
        _error_code_check(_ERROR_CODES_WITH_MAX_ATTEMPTS),
    )


def downgrade() -> None:
    op.drop_constraint("ck_jobs_error_code_valid", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_error_code_valid",
        "jobs",
        _error_code_check(_ERROR_CODES_WITHOUT_MAX_ATTEMPTS),
    )
