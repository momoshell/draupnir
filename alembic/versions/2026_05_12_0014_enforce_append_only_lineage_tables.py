"""enforce append-only lineage tables

Revision ID: 2026_05_12_0014
Revises: 2026_05_11_0013
Create Date: 2026-05-12 20:30:00
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NamedTuple

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2026_05_12_0014"
down_revision: str | None = "2026_05_11_0013"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

APPEND_ONLY_SQLSTATE = "55000"
_ROW_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_row"
_TRUNCATE_GUARD_FUNCTION_NAME = "enforce_append_only_lineage_truncate"
_ROW_TRIGGER_NAME = "trg_append_only_row_guard"
_TRUNCATE_TRIGGER_NAME = "trg_append_only_truncate_guard"


class _AppendOnlyTableSpec(NamedTuple):
    table_name: str
    allowlisted_update_columns: tuple[str, ...]


_PROTECTED_TABLE_SPECS: tuple[_AppendOnlyTableSpec, ...] = (
    _AppendOnlyTableSpec("files", ("deleted_at",)),
    _AppendOnlyTableSpec("extraction_profiles", ()),
    _AppendOnlyTableSpec("adapter_run_outputs", ()),
    _AppendOnlyTableSpec("drawing_revisions", ()),
    _AppendOnlyTableSpec("validation_reports", ()),
    _AppendOnlyTableSpec("generated_artifacts", ("deleted_at",)),
    _AppendOnlyTableSpec("job_events", ()),
)


def _create_guard_functions() -> None:
    """Create shared trigger functions for append-only enforcement."""

    op.execute(
        sa.text(
            f"""
            CREATE FUNCTION {_ROW_GUARD_FUNCTION_NAME}()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            DECLARE
                allowlisted_columns text[] := COALESCE(TG_ARGV, ARRAY[]::text[]);
                json_column record;
                sanitized_old jsonb;
                sanitized_new jsonb;
                old_deleted_at jsonb;
                new_deleted_at jsonb;
                old_json_text text;
                new_json_text text;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '{APPEND_ONLY_SQLSTATE}',
                        MESSAGE = format(
                            'append-only trigger blocked %s on %I',
                            TG_OP,
                            TG_TABLE_NAME
                        ),
                        DETAIL = 'Protected lineage/history tables are append-only.';
                END IF;

                FOR json_column IN
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = TG_TABLE_SCHEMA
                      AND table_name = TG_TABLE_NAME
                      AND data_type = 'json'
                      AND NOT (column_name = ANY(allowlisted_columns))
                LOOP
                    EXECUTE format(
                        'SELECT ($1).%1$I::text, ($2).%1$I::text',
                        json_column.column_name
                    )
                    INTO old_json_text, new_json_text
                    USING OLD, NEW;

                    IF old_json_text IS DISTINCT FROM new_json_text THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '{APPEND_ONLY_SQLSTATE}',
                            MESSAGE = format(
                                'append-only trigger blocked %s on %I',
                                TG_OP,
                                TG_TABLE_NAME
                            ),
                            DETAIL = (
                                'Protected JSON payload columns are append-only and cannot be '
                                'rewritten after insert.'
                            );
                    END IF;
                END LOOP;

                sanitized_old := to_jsonb(OLD) - allowlisted_columns;
                sanitized_new := to_jsonb(NEW) - allowlisted_columns;

                IF sanitized_old IS DISTINCT FROM sanitized_new THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '{APPEND_ONLY_SQLSTATE}',
                        MESSAGE = format(
                            'append-only trigger blocked %s on %I',
                            TG_OP,
                            TG_TABLE_NAME
                        ),
                        DETAIL = (
                            'Only allowlisted soft-delete markers may change on '
                            'protected lineage/history tables.'
                        );
                END IF;

                IF 'deleted_at' = ANY(allowlisted_columns) THEN
                    old_deleted_at := to_jsonb(OLD) -> 'deleted_at';
                    new_deleted_at := to_jsonb(NEW) -> 'deleted_at';

                    IF old_deleted_at = new_deleted_at THEN
                        RETURN NEW;
                    END IF;

                    IF old_deleted_at = 'null'::jsonb AND new_deleted_at <> 'null'::jsonb THEN
                        RETURN NEW;
                    END IF;

                    RAISE EXCEPTION USING
                        ERRCODE = '{APPEND_ONLY_SQLSTATE}',
                        MESSAGE = format(
                            'append-only trigger blocked %s on %I',
                            TG_OP,
                            TG_TABLE_NAME
                        ),
                        DETAIL = (
                            'deleted_at is write-once and may only change from NULL '
                            'to non-NULL.'
                        );
                END IF;

                RETURN NEW;
            END;
            $$
            """
        )
    )

    op.execute(
        sa.text(
            f"""
            CREATE FUNCTION {_TRUNCATE_GUARD_FUNCTION_NAME}()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                RAISE EXCEPTION USING
                    ERRCODE = '{APPEND_ONLY_SQLSTATE}',
                    MESSAGE = format(
                        'append-only trigger blocked %s on %I',
                        TG_OP,
                        TG_TABLE_NAME
                    ),
                    DETAIL = (
                        'Protected lineage/history tables are append-only and '
                        'cannot be truncated.'
                    );
            END;
            $$
            """
        )
    )


def _create_table_triggers() -> None:
    """Attach row and truncate append-only guards to protected tables."""

    for spec in _PROTECTED_TABLE_SPECS:
        allowlisted_columns = ", ".join(
            f"'{column_name}'" for column_name in spec.allowlisted_update_columns
        )
        function_arguments = f"({allowlisted_columns})" if allowlisted_columns else "()"

        op.execute(
            sa.text(
                f"""
                CREATE TRIGGER {_ROW_TRIGGER_NAME}
                BEFORE UPDATE OR DELETE ON "{spec.table_name}"
                FOR EACH ROW
                EXECUTE FUNCTION {_ROW_GUARD_FUNCTION_NAME}{function_arguments}
                """
            )
        )
        op.execute(
            sa.text(
                f"""
                CREATE TRIGGER {_TRUNCATE_TRIGGER_NAME}
                BEFORE TRUNCATE ON "{spec.table_name}"
                FOR EACH STATEMENT
                EXECUTE FUNCTION {_TRUNCATE_GUARD_FUNCTION_NAME}()
                """
            )
        )


def _drop_table_triggers() -> None:
    """Remove append-only triggers from protected tables."""

    for spec in _PROTECTED_TABLE_SPECS:
        op.execute(
            sa.text(
                f'DROP TRIGGER IF EXISTS {_TRUNCATE_TRIGGER_NAME} ON "{spec.table_name}"'
            )
        )
        op.execute(
            sa.text(f'DROP TRIGGER IF EXISTS {_ROW_TRIGGER_NAME} ON "{spec.table_name}"')
        )


def _drop_guard_functions() -> None:
    """Drop shared append-only enforcement functions."""

    op.execute(sa.text(f"DROP FUNCTION IF EXISTS {_TRUNCATE_GUARD_FUNCTION_NAME}()"))
    op.execute(sa.text(f"DROP FUNCTION IF EXISTS {_ROW_GUARD_FUNCTION_NAME}()"))


def _assert_protected_tables_empty_for_downgrade() -> None:
    """Refuse to remove append-only protections while protected rows exist."""

    bind = op.get_bind()
    non_empty_tables: list[str] = []

    for spec in _PROTECTED_TABLE_SPECS:
        has_rows = bind.execute(
            sa.select(sa.literal(1)).select_from(sa.table(spec.table_name)).limit(1)
        ).first()
        if has_rows is not None:
            non_empty_tables.append(spec.table_name)

    if non_empty_tables:
        joined_tables = ", ".join(non_empty_tables)
        raise RuntimeError(
            "Refusing to downgrade migration 2026_05_12_0014: removing append-only "
            "protections while protected tables contain rows can permit destructive "
            "lineage/history edits. Empty the following tables before retrying: "
            f"{joined_tables}."
        )


def upgrade() -> None:
    """Protect lineage/history tables from destructive mutations."""

    _create_guard_functions()
    _create_table_triggers()


def downgrade() -> None:
    """Remove append-only lineage/history protection from empty databases only."""

    _assert_protected_tables_empty_for_downgrade()
    _drop_table_triggers()
    _drop_guard_functions()
