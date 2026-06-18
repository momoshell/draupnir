"""Integration tests for immutable export job input persistence contracts."""

from __future__ import annotations

import importlib.util
import uuid
from collections.abc import AsyncGenerator, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.models.estimate_version import EstimateVersion
from app.models.export_job_input import ExportJobInput
from app.models.job import Job, JobStatus, JobType
from tests import test_quantity_takeoff_persistence as quantity_takeoff_persistence
from tests.conftest import requires_database

pytest_plugins = ("tests.test_quantity_takeoff_persistence",)
pytestmark = [pytest.mark.asyncio, requires_database]

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "2026_06_04_0029_add_revised_dxf_export_kind.py"
)


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


def _normalize_ondelete(raw_foreign_key: Mapping[str, Any]) -> str:
    options = raw_foreign_key.get("options")
    if isinstance(options, dict):
        return str(options.get("ondelete") or "").upper()
    return ""


def _normalize_constraint_sqltext(sqltext: str) -> str:
    normalized = sqltext
    for pg_cast in (
        "::boolean",
        "::character varying",
        "::text",
    ):
        normalized = normalized.replace(pg_cast, "")
    normalized = normalized.replace('"', " ")
    normalized = normalized.replace("(", " ").replace(")", " ")
    return " ".join(normalized.split())


def _inspect_schema(sync_connection: sa.Connection) -> dict[str, Any]:
    inspector = sa.inspect(sync_connection)
    tables = ("jobs", "export_job_inputs")

    triggers = tuple(
        sync_connection.execute(
            sa.text(
                """
                SELECT tgname
                FROM pg_trigger
                WHERE tgrelid = 'export_job_inputs'::regclass
                AND NOT tgisinternal
                ORDER BY tgname
                """
            )
        )
        .scalars()
        .all()
    )

    return {
        "columns": {
            table_name: {
                str(column["name"]): {"nullable": bool(column["nullable"])}
                for column in inspector.get_columns(table_name)
            }
            for table_name in tables
        },
        "primary_keys": {
            table_name: tuple(inspector.get_pk_constraint(table_name)["constrained_columns"])
            for table_name in tables
        },
        "check_constraints": {
            table_name: {
                str(constraint["name"]): _normalize_constraint_sqltext(str(constraint["sqltext"]))
                for constraint in inspector.get_check_constraints(table_name)
            }
            for table_name in tables
        },
        "foreign_keys": {
            (
                tuple(foreign_key["constrained_columns"]),
                str(foreign_key["referred_table"]),
                tuple(foreign_key["referred_columns"]),
            ): _normalize_ondelete(foreign_key)
            for foreign_key in inspector.get_foreign_keys("export_job_inputs")
        },
        "indexes": {
            tuple(index["column_names"]): str(index["name"])
            for index in inspector.get_indexes("export_job_inputs")
        },
        "triggers": triggers,
    }


async def _load_schema() -> dict[str, Any]:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        connection = await session.connection()
        return await connection.run_sync(_inspect_schema)


async def _assert_commit_fails(
    session: AsyncSession,
    obj: object,
    expected_substring: str | tuple[str, ...],
) -> None:
    session.add(obj)
    expected_substrings = (
        [expected_substring] if isinstance(expected_substring, str) else list(expected_substring)
    )

    try:
        await session.commit()
    except (DBAPIError, IntegrityError) as exc:
        await session.rollback()
        constraint_name = getattr(getattr(exc, "orig", None), "constraint_name", None)
        if constraint_name in expected_substrings:
            return

        message = "\n".join(
            part
            for part in (
                str(exc),
                repr(exc),
                str(getattr(exc, "orig", "")),
                repr(getattr(exc, "orig", "")),
            )
            if part
        ).lower()
        assert any(substring.lower() in message for substring in expected_substrings)
    else:
        pytest.fail(f"Expected database error containing {expected_substrings!r}")


async def _assert_append_only_sql_mutation_fails(
    session: AsyncSession,
    statement: str,
    *,
    operation: str,
    table_name: str,
    params: dict[str, object] | None = None,
) -> None:
    try:
        await session.execute(text(statement), params or {})
        await session.commit()
    except (DBAPIError, IntegrityError) as exc:
        await session.rollback()
        message = "\n".join(
            part
            for part in (
                str(exc),
                repr(exc),
                str(getattr(exc, "orig", "")),
                repr(getattr(exc, "orig", "")),
            )
            if part
        ).lower()
        assert f"append-only trigger blocked {operation.lower()} on {table_name}" in message
    else:
        pytest.fail(f"Expected append-only mutation failure for {operation} on {table_name}")


def _load_migration_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "migration_2026_06_04_0029_add_revised_dxf_export_kind",
        _MIGRATION_PATH,
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run_downgrade_guard(db_session: AsyncSession) -> None:
    migration = _load_migration_module()
    connection = await db_session.connection()

    def _run_guard(sync_connection: sa.Connection) -> None:
        migration_context = MigrationContext.configure(sync_connection)
        migration.op = Operations(migration_context)
        migration._assert_downgrade_safe()

    await connection.run_sync(_run_guard)


async def _create_export_job(
    db_session: AsyncSession,
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    *,
    base_revision_id: uuid.UUID | None = None,
    extraction_profile_id: uuid.UUID | None = None,
) -> uuid.UUID:
    job_id = uuid.uuid4()
    db_session.add(
        Job(
            id=job_id,
            project_id=seed.project_id,
            file_id=seed.file_id,
            extraction_profile_id=extraction_profile_id,
            base_revision_id=(
                base_revision_id if base_revision_id is not None else seed.drawing_revision_id
            ),
            parent_job_id=seed.quantity_job_id,
            job_type=JobType.EXPORT.value,
            status=JobStatus.PENDING.value,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
    )
    await db_session.flush()
    return job_id


async def _create_estimate_version(
    db_session: AsyncSession,
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
) -> uuid.UUID:
    estimate_job_id = uuid.uuid4()
    db_session.add(
        Job(
            id=estimate_job_id,
            project_id=seed.project_id,
            file_id=seed.file_id,
            extraction_profile_id=None,
            base_revision_id=seed.drawing_revision_id,
            parent_job_id=seed.quantity_job_id,
            job_type=JobType.ESTIMATE.value,
            status=JobStatus.SUCCEEDED.value,
            enqueue_status="published",
            enqueue_attempts=0,
            cancel_requested=False,
        )
    )
    await db_session.flush()

    estimate_version_id = uuid.uuid4()
    db_session.add(
        EstimateVersion(
            id=estimate_version_id,
            project_id=seed.project_id,
            source_file_id=seed.file_id,
            drawing_revision_id=seed.drawing_revision_id,
            quantity_takeoff_id=seed.quantity_takeoff_id,
            source_job_id=estimate_job_id,
            currency="GBP",
            subtotal_amount=Decimal("100.00"),
            tax_amount=Decimal("20.00"),
            total_amount=Decimal("120.00"),
        )
    )
    await db_session.flush()
    return estimate_version_id


def _build_export_input(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    *,
    source_job_id: uuid.UUID,
    export_kind: str,
    export_format: str,
    media_type: str,
    quantity_takeoff_id: uuid.UUID | None,
    estimate_version_id: uuid.UUID | None,
    source_job_type: str = JobType.EXPORT.value,
    options_json: object = cast(object, {"include_headers": True}),
) -> ExportJobInput:
    return ExportJobInput(
        source_job_id=source_job_id,
        project_id=seed.project_id,
        source_file_id=seed.file_id,
        drawing_revision_id=seed.drawing_revision_id,
        source_job_type=source_job_type,
        export_kind=export_kind,
        export_format=export_format,
        media_type=media_type,
        options_json=cast(dict[str, Any], options_json),
        quantity_takeoff_id=quantity_takeoff_id,
        estimate_version_id=estimate_version_id,
    )


async def test_export_job_input_schema_matches_contract() -> None:
    schema = await _load_schema()
    columns = schema["columns"]
    primary_keys = schema["primary_keys"]
    check_constraints = schema["check_constraints"]
    foreign_keys = schema["foreign_keys"]
    indexes = schema["indexes"]
    triggers = schema["triggers"]

    assert "export" in check_constraints["jobs"]["ck_jobs_job_type_valid"]
    assert "export" in check_constraints["jobs"]["ck_jobs_revision_scoped_base_revision_required"]
    assert (
        "export"
        in check_constraints["jobs"]["ck_jobs_revision_scoped_extraction_profile_forbidden"]
    )

    for required_column in (
        "source_job_id",
        "project_id",
        "source_file_id",
        "drawing_revision_id",
        "source_job_type",
        "export_kind",
        "export_format",
        "media_type",
        "options_json",
        "created_at",
    ):
        assert columns["export_job_inputs"][required_column]["nullable"] is False

    for nullable_column in (
        "quantity_takeoff_id",
        "estimate_version_id",
    ):
        assert columns["export_job_inputs"][nullable_column]["nullable"] is True

    assert primary_keys["export_job_inputs"] == ("source_job_id",)

    for constraint_name in (
        "ck_export_job_inputs_source_job_type_export",
        "ck_export_job_inputs_export_kind_valid",
        "ck_export_job_inputs_export_format_valid",
        "ck_export_job_inputs_media_type_valid",
        "ck_export_job_inputs_options_json_object",
        "ck_export_job_inputs_kind_format_media_type_matrix",
        "ck_export_job_inputs_quantity_lineage",
        "ck_export_job_inputs_estimate_lineage",
    ):
        assert constraint_name in check_constraints["export_job_inputs"]

    assert (
        "revised_dxf"
        in check_constraints["export_job_inputs"]["ck_export_job_inputs_export_kind_valid"]
    )
    assert (
        "dxf" in check_constraints["export_job_inputs"]["ck_export_job_inputs_export_format_valid"]
    )
    assert (
        "application/dxf"
        in check_constraints["export_job_inputs"]["ck_export_job_inputs_media_type_valid"]
    )
    assert (
        "revised_dxf"
        in check_constraints["export_job_inputs"][
            "ck_export_job_inputs_kind_format_media_type_matrix"
        ]
    )

    assert (
        (
            "source_job_id",
            "project_id",
            "source_file_id",
            "drawing_revision_id",
            "source_job_type",
        ),
        "jobs",
        ("id", "project_id", "file_id", "base_revision_id", "job_type"),
    ) in foreign_keys
    assert (
        (
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
        ),
        "quantity_takeoffs",
        ("id", "project_id", "drawing_revision_id"),
    ) in foreign_keys
    assert (
        (
            "estimate_version_id",
            "project_id",
            "drawing_revision_id",
            "quantity_takeoff_id",
        ),
        "estimate_versions",
        ("id", "project_id", "drawing_revision_id", "quantity_takeoff_id"),
    ) in foreign_keys
    assert all(ondelete == "RESTRICT" for ondelete in foreign_keys.values())

    for index_columns in (
        ("project_id",),
        ("source_file_id",),
        ("drawing_revision_id",),
        ("quantity_takeoff_id",),
        ("estimate_version_id",),
    ):
        assert index_columns in indexes

    assert "trg_append_only_row_guard" in triggers
    assert "trg_append_only_truncate_guard" in triggers


async def test_export_job_input_persists_valid_revision_quantity_and_estimate_inputs(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    estimate_version_id = await _create_estimate_version(db_session, seed)

    revision_job_id = await _create_export_job(db_session, seed)
    revised_dxf_job_id = await _create_export_job(db_session, seed)
    quantity_job_id = await _create_export_job(db_session, seed)
    estimate_csv_job_id = await _create_export_job(db_session, seed)
    estimate_pdf_job_id = await _create_export_job(db_session, seed)

    db_session.add_all(
        [
            _build_export_input(
                seed,
                source_job_id=revision_job_id,
                export_kind="revision_json",
                export_format="json",
                media_type="application/json",
                quantity_takeoff_id=None,
                estimate_version_id=None,
            ),
            _build_export_input(
                seed,
                source_job_id=revised_dxf_job_id,
                export_kind="revised_dxf",
                export_format="dxf",
                media_type="application/dxf",
                quantity_takeoff_id=None,
                estimate_version_id=None,
            ),
            _build_export_input(
                seed,
                source_job_id=quantity_job_id,
                export_kind="quantity_csv",
                export_format="csv",
                media_type="text/csv",
                quantity_takeoff_id=seed.quantity_takeoff_id,
                estimate_version_id=None,
            ),
            _build_export_input(
                seed,
                source_job_id=estimate_csv_job_id,
                export_kind="estimate_csv",
                export_format="csv",
                media_type="text/csv",
                quantity_takeoff_id=seed.quantity_takeoff_id,
                estimate_version_id=estimate_version_id,
            ),
            _build_export_input(
                seed,
                source_job_id=estimate_pdf_job_id,
                export_kind="estimate_pdf",
                export_format="pdf",
                media_type="application/pdf",
                quantity_takeoff_id=seed.quantity_takeoff_id,
                estimate_version_id=estimate_version_id,
            ),
        ]
    )
    await db_session.commit()

    persisted = (
        (
            await db_session.execute(
                select(ExportJobInput).order_by(ExportJobInput.source_job_id.asc())
            )
        )
        .scalars()
        .all()
    )
    assert len(persisted) == 5
    by_job_id = {row.source_job_id: row for row in persisted}

    assert by_job_id[revision_job_id].export_kind == "revision_json"
    assert by_job_id[revision_job_id].export_format == "json"
    assert by_job_id[revision_job_id].media_type == "application/json"
    assert by_job_id[revision_job_id].quantity_takeoff_id is None
    assert by_job_id[revision_job_id].estimate_version_id is None

    assert by_job_id[revised_dxf_job_id].export_kind == "revised_dxf"
    assert by_job_id[revised_dxf_job_id].export_format == "dxf"
    assert by_job_id[revised_dxf_job_id].media_type == "application/dxf"
    assert by_job_id[revised_dxf_job_id].quantity_takeoff_id is None
    assert by_job_id[revised_dxf_job_id].estimate_version_id is None

    assert by_job_id[quantity_job_id].export_kind == "quantity_csv"
    assert by_job_id[quantity_job_id].export_format == "csv"
    assert by_job_id[quantity_job_id].media_type == "text/csv"
    assert by_job_id[quantity_job_id].quantity_takeoff_id == seed.quantity_takeoff_id
    assert by_job_id[quantity_job_id].estimate_version_id is None

    assert by_job_id[estimate_csv_job_id].export_kind == "estimate_csv"
    assert by_job_id[estimate_csv_job_id].estimate_version_id == estimate_version_id

    assert by_job_id[estimate_pdf_job_id].export_kind == "estimate_pdf"
    assert by_job_id[estimate_pdf_job_id].estimate_version_id == estimate_version_id


async def test_export_job_input_contract_rejects_ambiguous_and_invalid_lineage_inputs(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    estimate_version_id = await _create_estimate_version(db_session, seed)

    await _assert_commit_fails(
        db_session,
        Job(
            id=uuid.uuid4(),
            project_id=seed.project_id,
            file_id=seed.file_id,
            job_type=JobType.EXPORT.value,
            status=JobStatus.PENDING.value,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        ),
        "ck_jobs_revision_scoped_base_revision_required",
    )

    await _assert_commit_fails(
        db_session,
        Job(
            id=uuid.uuid4(),
            project_id=seed.project_id,
            file_id=seed.file_id,
            extraction_profile_id=seed.extraction_profile_id,
            base_revision_id=seed.drawing_revision_id,
            job_type=JobType.EXPORT.value,
            status=JobStatus.PENDING.value,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        ),
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
    )

    bad_job_ids = [await _create_export_job(db_session, seed) for _ in range(11)]
    await db_session.commit()

    invalid_cases: list[tuple[dict[str, object], str | tuple[str, ...]]] = [
        (
            {
                "source_job_type": JobType.ESTIMATE.value,
                "export_kind": "revision_json",
                "export_format": "json",
                "media_type": "application/json",
                "quantity_takeoff_id": None,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_source_job_type_export",
        ),
        (
            {
                "export_kind": "revised_dxf",
                "export_format": "json",
                "media_type": "application/json",
                "quantity_takeoff_id": None,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_kind_format_media_type_matrix",
        ),
        (
            {
                "export_kind": "quantity_csv",
                "export_format": "pdf",
                "media_type": "application/pdf",
                "quantity_takeoff_id": seed.quantity_takeoff_id,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_kind_format_media_type_matrix",
        ),
        (
            {
                "export_kind": "revised_dxf",
                "export_format": "dxf",
                "media_type": "application/dxf",
                "quantity_takeoff_id": seed.quantity_takeoff_id,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_quantity_lineage",
        ),
        (
            {
                "export_kind": "revision_json",
                "export_format": "json",
                "media_type": "application/json",
                "quantity_takeoff_id": seed.quantity_takeoff_id,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_quantity_lineage",
        ),
        (
            {
                "export_kind": "quantity_csv",
                "export_format": "csv",
                "media_type": "text/csv",
                "quantity_takeoff_id": None,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_quantity_lineage",
        ),
        (
            {
                "export_kind": "quantity_csv",
                "export_format": "csv",
                "media_type": "text/csv",
                "quantity_takeoff_id": seed.quantity_takeoff_id,
                "estimate_version_id": estimate_version_id,
            },
            "ck_export_job_inputs_estimate_lineage",
        ),
        (
            {
                "export_kind": "estimate_csv",
                "export_format": "csv",
                "media_type": "text/csv",
                "quantity_takeoff_id": seed.quantity_takeoff_id,
                "estimate_version_id": None,
            },
            "ck_export_job_inputs_estimate_lineage",
        ),
        (
            {
                "export_kind": "estimate_pdf",
                "export_format": "pdf",
                "media_type": "application/pdf",
                "quantity_takeoff_id": uuid.uuid4(),
                "estimate_version_id": estimate_version_id,
            },
            # A bogus quantity_takeoff_id breaks both the gate-free takeoff lineage FK
            # and the estimate-version lineage FK (which also keys on quantity_takeoff_id);
            # Postgres may report either.
            (
                "fk_export_job_inputs_takeoff_lineage",
                "fk_export_job_inputs_estimate_version_lineage",
            ),
        ),
        (
            {
                "export_kind": "estimate_pdf",
                "export_format": "pdf",
                "media_type": "application/pdf",
                "quantity_takeoff_id": seed.quantity_takeoff_id,
                "estimate_version_id": uuid.uuid4(),
            },
            "fk_export_job_inputs_estimate_version_lineage",
        ),
        (
            {
                "export_kind": "revision_json",
                "export_format": "json",
                "media_type": "application/json",
                "quantity_takeoff_id": None,
                "estimate_version_id": None,
                "source_file_id": uuid.uuid4(),
            },
            (
                "fk_export_job_inputs_source_job_lineage_jobs",
                "fk_export_job_inputs_source_file_id_project_id_files",
            ),
        ),
    ]

    for job_id, (overrides, expected_substring) in zip(bad_job_ids, invalid_cases, strict=True):
        payload: dict[str, object] = {
            "source_job_id": job_id,
            "export_kind": "revision_json",
            "export_format": "json",
            "media_type": "application/json",
            "quantity_takeoff_id": None,
            "estimate_version_id": None,
            "source_job_type": JobType.EXPORT.value,
            "options_json": {"include_headers": True},
        }
        payload.update(overrides)

        await _assert_commit_fails(
            db_session,
            ExportJobInput(
                source_job_id=cast(uuid.UUID, payload["source_job_id"]),
                project_id=seed.project_id,
                source_file_id=cast(uuid.UUID, payload.get("source_file_id", seed.file_id)),
                drawing_revision_id=seed.drawing_revision_id,
                source_job_type=cast(str, payload["source_job_type"]),
                export_kind=cast(str, payload["export_kind"]),
                export_format=cast(str, payload["export_format"]),
                media_type=cast(str, payload["media_type"]),
                options_json=cast(dict[str, Any], payload["options_json"]),
                quantity_takeoff_id=cast(uuid.UUID | None, payload["quantity_takeoff_id"]),
                estimate_version_id=cast(uuid.UUID | None, payload["estimate_version_id"]),
            ),
            expected_substring,
        )


async def test_export_job_inputs_are_append_only_and_revised_dxf_downgrade_guarded(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    await _run_downgrade_guard(db_session)

    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    export_job_without_rows = await _create_export_job(db_session, seed)
    await db_session.commit()

    await _run_downgrade_guard(db_session)

    estimate_version_id = await _create_estimate_version(db_session, seed)
    row_job_id = await _create_export_job(db_session, seed)
    db_session.add(
        _build_export_input(
            seed,
            source_job_id=row_job_id,
            export_kind="estimate_csv",
            export_format="csv",
            media_type="text/csv",
            quantity_takeoff_id=seed.quantity_takeoff_id,
            estimate_version_id=estimate_version_id,
        )
    )
    await db_session.commit()

    await _run_downgrade_guard(db_session)

    await _assert_append_only_sql_mutation_fails(
        db_session,
        "UPDATE export_job_inputs SET export_format = 'pdf' WHERE source_job_id = :job_id",
        operation="UPDATE",
        table_name="export_job_inputs",
        params={"job_id": row_job_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        "DELETE FROM export_job_inputs WHERE source_job_id = :job_id",
        operation="DELETE",
        table_name="export_job_inputs",
        params={"job_id": row_job_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        "TRUNCATE export_job_inputs",
        operation="TRUNCATE",
        table_name="export_job_inputs",
    )

    _ = export_job_without_rows
    revised_dxf_job_id = await _create_export_job(db_session, seed)
    db_session.add(
        _build_export_input(
            seed,
            source_job_id=revised_dxf_job_id,
            export_kind="revised_dxf",
            export_format="dxf",
            media_type="application/dxf",
            quantity_takeoff_id=None,
            estimate_version_id=None,
        )
    )
    await db_session.commit()

    with pytest.raises(RuntimeError, match="persisted revised_dxf export_job_inputs rows present"):
        await _run_downgrade_guard(db_session)
