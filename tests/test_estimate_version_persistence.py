"""Integration tests for append-only estimate version persistence."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy import CheckConstraint, Table, UniqueConstraint, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.models.estimate_version import EstimateVersion
from app.models.job import Job, JobStatus, JobType
from app.models.quantity_takeoff import QuantityTakeoff
from tests import test_quantity_takeoff_persistence as quantity_takeoff_persistence
from tests.conftest import requires_database

pytest_plugins = ("tests.test_quantity_takeoff_persistence",)
pytestmark = [pytest.mark.asyncio, requires_database]
_ESTIMATE_VERSION_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "2026_05_17_0020_add_estimate_versions.py"
)


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
        "::numeric",
        "::text",
    ):
        normalized = normalized.replace(pg_cast, "")
    normalized = normalized.replace('"', " ")
    normalized = normalized.replace("(", " ").replace(")", " ")
    return " ".join(normalized.split())


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


def _inspect_estimate_version_schema(sync_connection: sa.Connection) -> dict[str, Any]:
    inspector = sa.inspect(sync_connection)
    tables = ("jobs", "quantity_takeoffs", "estimate_versions")
    return {
        "columns": {
            table_name: {
                str(column["name"]): {"nullable": bool(column["nullable"])}
                for column in inspector.get_columns(table_name)
            }
            for table_name in tables
        },
        "unique_constraints": {
            table_name: {
                tuple(constraint["column_names"]): str(constraint["name"])
                for constraint in inspector.get_unique_constraints(table_name)
            }
            for table_name in tables
        },
        "indexes": {
            table_name: {
                tuple(index["column_names"]): str(index["name"])
                for index in inspector.get_indexes(table_name)
            }
            for table_name in tables
        },
        "check_constraints": {
            table_name: {
                str(constraint["name"]): str(constraint["sqltext"])
                for constraint in inspector.get_check_constraints(table_name)
            }
            for table_name in ("estimate_versions",)
        },
        "foreign_keys": {
            table_name: {
                (
                    tuple(foreign_key["constrained_columns"]),
                    str(foreign_key["referred_table"]),
                    tuple(foreign_key["referred_columns"]),
                ): _normalize_ondelete(foreign_key)
                for foreign_key in inspector.get_foreign_keys(table_name)
            }
            for table_name in ("estimate_versions",)
        },
    }


async def _load_estimate_version_schema() -> dict[str, Any]:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        connection = await session.connection()
        return await connection.run_sync(_inspect_estimate_version_schema)


async def _create_estimate_source_job(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
) -> uuid.UUID:
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    job_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            Job(
                id=job_id,
                project_id=seed.project_id,
                file_id=seed.file_id,
                extraction_profile_id=seed.extraction_profile_id,
                base_revision_id=seed.drawing_revision_id,
                job_type=JobType.REPROCESS.value,
                status=JobStatus.SUCCEEDED.value,
                enqueue_status="published",
                enqueue_attempts=0,
                cancel_requested=False,
            )
        )
        await session.commit()

    return job_id


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
        assert f"append-only trigger blocked {operation.lower()} on estimate_versions" in message
    else:
        pytest.fail(f"Expected append-only mutation failure for {operation} on estimate_versions")


def _build_estimate_version(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    *,
    source_job_id: uuid.UUID,
    quantity_takeoff_id: uuid.UUID,
    drawing_revision_id: uuid.UUID | None = None,
    quantity_gate: str = "allowed",
    trusted_totals: bool = True,
    currency: str = "GBP",
    subtotal_amount: Decimal = Decimal("100.00"),
    tax_amount: Decimal = Decimal("20.00"),
    total_amount: Decimal = Decimal("120.00"),
) -> EstimateVersion:
    return EstimateVersion(
        id=uuid.uuid4(),
        project_id=seed.project_id,
        source_file_id=seed.file_id,
        drawing_revision_id=drawing_revision_id or seed.drawing_revision_id,
        quantity_takeoff_id=quantity_takeoff_id,
        source_job_id=source_job_id,
        quantity_gate=quantity_gate,
        trusted_totals=trusted_totals,
        currency=currency,
        subtotal_amount=subtotal_amount,
        tax_amount=tax_amount,
        total_amount=total_amount,
    )


async def test_estimate_version_schema_matches_contract() -> None:
    schema = await _load_estimate_version_schema()
    columns = schema["columns"]
    unique_constraints = schema["unique_constraints"]
    indexes = schema["indexes"]
    check_constraints = schema["check_constraints"]["estimate_versions"]
    foreign_keys = schema["foreign_keys"]["estimate_versions"]

    assert (
        "id",
        "project_id",
        "drawing_revision_id",
        "quantity_gate",
        "trusted_totals",
    ) in unique_constraints["quantity_takeoffs"]
    assert (
        "id",
        "project_id",
        "file_id",
        "base_revision_id",
        "job_type",
    ) in unique_constraints["jobs"]

    estimate_columns = columns["estimate_versions"]
    for required_column in (
        "project_id",
        "source_file_id",
        "drawing_revision_id",
        "quantity_takeoff_id",
        "source_job_id",
        "quantity_gate",
        "trusted_totals",
        "currency",
        "subtotal_amount",
        "tax_amount",
        "total_amount",
        "created_at",
    ):
        assert required_column in estimate_columns
        assert estimate_columns[required_column]["nullable"] is False

    assert ("source_job_id",) in unique_constraints["estimate_versions"]
    assert ("id", "project_id", "drawing_revision_id") in unique_constraints["estimate_versions"]
    assert ("project_id",) in indexes["estimate_versions"]
    assert ("source_file_id",) in indexes["estimate_versions"]
    assert ("quantity_takeoff_id",) in indexes["estimate_versions"]
    assert ("drawing_revision_id",) in indexes["estimate_versions"]

    for constraint_name in (
        "ck_estimate_versions_currency_gbp",
        "ck_estimate_versions_subtotal_amount_nonnegative",
        "ck_estimate_versions_tax_amount_nonnegative",
        "ck_estimate_versions_total_amount_nonnegative",
    ):
        assert constraint_name in check_constraints
    # Path B 3: estimate versions are no longer gated to allowed + trusted takeoffs.
    for constraint_name in (
        "ck_estimate_versions_quantity_gate_allowed",
        "ck_estimate_versions_trusted_totals_true",
    ):
        assert constraint_name not in check_constraints

    currency_sql = _normalize_constraint_sqltext(
        check_constraints["ck_estimate_versions_currency_gbp"]
    ).lower()
    assert "currency = 'gbp'" in currency_sql

    assert foreign_keys[(("project_id",), "projects", ("id",))] == "RESTRICT"
    assert (
        foreign_keys[(("source_file_id", "project_id"), "files", ("id", "project_id"))]
        == "RESTRICT"
    )
    assert (
        foreign_keys[
            (
                ("drawing_revision_id", "project_id", "source_file_id"),
                "drawing_revisions",
                ("id", "project_id", "source_file_id"),
            )
        ]
        == "RESTRICT"
    )
    assert (
        foreign_keys[
            (
                (
                    "quantity_takeoff_id",
                    "project_id",
                    "drawing_revision_id",
                    "quantity_gate",
                    "trusted_totals",
                ),
                "quantity_takeoffs",
                (
                    "id",
                    "project_id",
                    "drawing_revision_id",
                    "quantity_gate",
                    "trusted_totals",
                ),
            )
        ]
        == "RESTRICT"
    )
    assert (
        foreign_keys[
            (
                ("source_job_id", "project_id", "source_file_id"),
                "jobs",
                ("id", "project_id", "file_id"),
            )
        ]
        == "RESTRICT"
    )

    quantity_takeoff_table = cast(Table, QuantityTakeoff.__table__)
    quantity_takeoff_model_uniques = {
        tuple(constraint.columns.keys()): constraint.name
        for constraint in quantity_takeoff_table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert (
        "id",
        "project_id",
        "drawing_revision_id",
        "quantity_gate",
        "trusted_totals",
    ) in quantity_takeoff_model_uniques

    estimate_version_table = cast(Table, EstimateVersion.__table__)
    estimate_version_model_checks = {
        str(constraint.name): str(constraint.sqltext)
        for constraint in estimate_version_table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    for column_name, constraint_name in (
        ("subtotal_amount", "ck_estimate_versions_subtotal_amount_nonnegative"),
        ("tax_amount", "ck_estimate_versions_tax_amount_nonnegative"),
        ("total_amount", "ck_estimate_versions_total_amount_nonnegative"),
    ):
        sqltext = estimate_version_model_checks[constraint_name]
        assert f"{column_name}::text <> 'NaN'" in sqltext
        assert f"{column_name} >= 0::numeric" in sqltext

    migration_sql = _ESTIMATE_VERSION_MIGRATION_PATH.read_text(encoding="utf-8")
    for column_name in ("subtotal_amount", "tax_amount", "total_amount"):
        assert f"\"{column_name}::text <> 'NaN' AND {column_name} >= 0::numeric\"" in migration_sql


async def test_estimate_version_persists_allowed_trusted_header(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)

    estimate_version = _build_estimate_version(
        seed,
        source_job_id=source_job_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
    )
    db_session.add(estimate_version)
    await db_session.commit()

    persisted = await db_session.scalar(
        select(EstimateVersion).where(EstimateVersion.id == estimate_version.id)
    )
    assert persisted is not None
    assert persisted.quantity_gate == "allowed"
    assert persisted.trusted_totals is True
    assert persisted.currency == "GBP"
    assert persisted.subtotal_amount == Decimal("100.00")
    assert persisted.tax_amount == Decimal("20.00")
    assert persisted.total_amount == Decimal("120.00")


async def test_estimate_version_rejects_takeoff_without_allowed_trusted_contract(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)
    provisional_takeoff_id = await quantity_takeoff_persistence._create_quantity_takeoff(
        seed,
        review_state="provisional",
        validation_status="valid_with_warnings",
        quantity_gate="allowed_provisional",
        trusted_totals=False,
    )

    await _assert_commit_fails(
        db_session,
        _build_estimate_version(
            seed,
            source_job_id=source_job_id,
            quantity_takeoff_id=provisional_takeoff_id,
            quantity_gate="allowed",
            trusted_totals=True,
        ),
        "fk_estimate_versions_takeoff_contract",
    )


async def test_estimate_version_rejects_revision_mismatch(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)
    mismatched_revision_id = await quantity_takeoff_persistence._create_additional_drawing_revision(
        seed
    )

    await _assert_commit_fails(
        db_session,
        _build_estimate_version(
            seed,
            source_job_id=source_job_id,
            quantity_takeoff_id=seed.quantity_takeoff_id,
            drawing_revision_id=mismatched_revision_id,
        ),
        "fk_estimate_versions_takeoff_contract",
    )


async def test_estimate_version_rejects_invalid_currency(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)

    await _assert_commit_fails(
        db_session,
        _build_estimate_version(
            seed,
            source_job_id=source_job_id,
            quantity_takeoff_id=seed.quantity_takeoff_id,
            currency="USD",
        ),
        "ck_estimate_versions_currency_gbp",
    )


@pytest.mark.parametrize(
    ("field_name", "field_value", "constraint_name"),
    (
        ("subtotal_amount", Decimal("-0.01"), "ck_estimate_versions_subtotal_amount_nonnegative"),
        ("tax_amount", Decimal("-0.01"), "ck_estimate_versions_tax_amount_nonnegative"),
        ("total_amount", Decimal("-0.01"), "ck_estimate_versions_total_amount_nonnegative"),
    ),
)
async def test_estimate_version_rejects_negative_amounts(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
    field_name: str,
    field_value: Decimal,
    constraint_name: str,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)
    estimate_version = _build_estimate_version(
        seed,
        source_job_id=source_job_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
    )
    setattr(estimate_version, field_name, field_value)

    await _assert_commit_fails(
        db_session,
        estimate_version,
        constraint_name,
    )


@pytest.mark.parametrize(
    "field_name",
    (
        "subtotal_amount",
        "tax_amount",
        "total_amount",
    ),
)
async def test_estimate_version_rejects_nan_amounts(
    async_client: httpx.AsyncClient,
    field_name: str,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)
    estimate_version = _build_estimate_version(
        seed,
        source_job_id=source_job_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
    )
    with pytest.raises(ValueError, match=f"{field_name} must not be NaN"):
        setattr(estimate_version, field_name, Decimal("NaN"))


async def test_estimate_version_rejects_duplicate_source_job_id(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)

    first_estimate_version = _build_estimate_version(
        seed,
        source_job_id=source_job_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
    )
    db_session.add(first_estimate_version)
    await db_session.commit()

    await _assert_commit_fails(
        db_session,
        _build_estimate_version(
            seed,
            source_job_id=source_job_id,
            quantity_takeoff_id=seed.quantity_takeoff_id,
            subtotal_amount=Decimal("101.00"),
            total_amount=Decimal("121.00"),
        ),
        (
            "uq_estimate_versions_source_job_id",
            "estimate_versions_source_job_id_key",
            "source_job_id",
        ),
    )


async def test_estimate_versions_reject_append_only_mutations(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    source_job_id = await _create_estimate_source_job(seed)

    estimate_version = _build_estimate_version(
        seed,
        source_job_id=source_job_id,
        quantity_takeoff_id=seed.quantity_takeoff_id,
    )
    estimate_version_id = estimate_version.id
    db_session.add(estimate_version)
    await db_session.commit()

    await _assert_append_only_sql_mutation_fails(
        db_session,
        'UPDATE "estimate_versions" SET total_amount = total_amount + 1 WHERE id = :row_id',
        operation="UPDATE",
        params={"row_id": estimate_version_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        'DELETE FROM "estimate_versions" WHERE id = :row_id',
        operation="DELETE",
        params={"row_id": estimate_version_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        'TRUNCATE TABLE "estimate_versions" CASCADE',
        operation="TRUNCATE",
    )
