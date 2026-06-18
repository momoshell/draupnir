"""Integration tests for immutable estimate job input persistence contracts."""

from __future__ import annotations

import importlib.util
import uuid
from collections.abc import AsyncGenerator, Mapping
from datetime import date
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
import app.jobs.worker as worker_module
from app.models.estimate_job_input import EstimateJobInput, EstimateJobInputCatalogRef
from app.models.job import Job, JobStatus, JobType
from tests import test_estimate_snapshot_item_persistence as snapshot_item_persistence
from tests import test_quantity_takeoff_persistence as quantity_takeoff_persistence
from tests.conftest import requires_database

pytest_plugins = ("tests.test_quantity_takeoff_persistence",)
pytestmark = [pytest.mark.asyncio, requires_database]

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "2026_05_18_0022_add_estimate_job_input_contract.py"
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
    tables = ("jobs", "estimate_job_inputs", "estimate_job_input_catalog_refs")
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
            table_name: {
                (
                    tuple(foreign_key["constrained_columns"]),
                    str(foreign_key["referred_table"]),
                    tuple(foreign_key["referred_columns"]),
                ): _normalize_ondelete(foreign_key)
                for foreign_key in inspector.get_foreign_keys(table_name)
            }
            for table_name in ("estimate_job_inputs", "estimate_job_input_catalog_refs")
        },
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
    params: dict[str, object],
) -> None:
    try:
        await session.execute(text(statement), params)
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
        "migration_2026_05_18_0022_add_estimate_job_input_contract",
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
        migration._assert_estimate_job_input_tables_empty()

    await connection.run_sync(_run_guard)


async def _create_estimate_job_input(
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
            status=JobStatus.PENDING.value,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        )
    )
    await db_session.flush()
    db_session.add(
        EstimateJobInput(
            estimate_job_id=estimate_job_id,
            project_id=seed.project_id,
            source_file_id=seed.file_id,
            drawing_revision_id=seed.drawing_revision_id,
            quantity_takeoff_id=seed.quantity_takeoff_id,
            source_job_type=JobType.ESTIMATE.value,
            currency="GBP",
            pricing_effective_date=date(2026, 5, 18),
            pricing_mode="explicit",
            assumptions_json={"waste_factor": "10%"},
        )
    )
    await db_session.flush()
    return estimate_job_id


def _build_estimate_job_input(
    seed: quantity_takeoff_persistence._QuantityPersistenceSeed,
    *,
    estimate_job_id: uuid.UUID,
    source_job_type: str = JobType.ESTIMATE.value,
    quantity_takeoff_id: uuid.UUID | None = None,
    currency: str = "GBP",
    pricing_mode: str = "explicit",
    assumptions_json: object = cast(object, {"waste_factor": "10%"}),
) -> EstimateJobInput:
    return EstimateJobInput(
        estimate_job_id=estimate_job_id,
        project_id=seed.project_id,
        source_file_id=seed.file_id,
        drawing_revision_id=seed.drawing_revision_id,
        quantity_takeoff_id=quantity_takeoff_id or seed.quantity_takeoff_id,
        source_job_type=source_job_type,
        currency=currency,
        pricing_effective_date=date(2026, 5, 18),
        pricing_mode=pricing_mode,
        assumptions_json=cast(dict[str, Any], assumptions_json),
    )


def _build_catalog_ref(
    *,
    estimate_job_id: uuid.UUID,
    ref_type: str,
    selection_key: str,
    ref_order: int,
    catalog_id: uuid.UUID,
    catalog_checksum_sha256: str,
    selection_context_json: object = cast(object, {"source": "test"}),
) -> EstimateJobInputCatalogRef:
    return EstimateJobInputCatalogRef(
        estimate_job_id=estimate_job_id,
        ref_type=ref_type,
        selection_key=selection_key,
        ref_order=ref_order,
        rate_catalog_entry_id=catalog_id if ref_type == "rate" else None,
        material_catalog_entry_id=catalog_id if ref_type == "material" else None,
        formula_definition_id=catalog_id if ref_type == "formula" else None,
        catalog_checksum_sha256=catalog_checksum_sha256,
        selection_context_json=cast(dict[str, Any], selection_context_json),
    )


async def test_estimate_job_input_schema_matches_contract() -> None:
    schema = await _load_schema()
    columns = schema["columns"]
    unique_constraints = schema["unique_constraints"]
    primary_keys = schema["primary_keys"]
    check_constraints = schema["check_constraints"]
    foreign_keys = schema["foreign_keys"]

    assert "estimate" in check_constraints["jobs"]["ck_jobs_job_type_valid"]
    assert "estimate" in check_constraints["jobs"]["ck_jobs_revision_scoped_base_revision_required"]
    assert (
        "estimate"
        in check_constraints["jobs"]["ck_jobs_revision_scoped_extraction_profile_forbidden"]
    )

    for required_column in (
        "estimate_job_id",
        "project_id",
        "source_file_id",
        "drawing_revision_id",
        "quantity_takeoff_id",
        "source_job_type",
        "currency",
        "pricing_effective_date",
        "pricing_mode",
        "assumptions_json",
        "created_at",
    ):
        assert columns["estimate_job_inputs"][required_column]["nullable"] is False
    for required_column in (
        "estimate_job_id",
        "ref_type",
        "selection_key",
        "ref_order",
        "catalog_checksum_sha256",
        "selection_context_json",
        "created_at",
    ):
        assert columns["estimate_job_input_catalog_refs"][required_column]["nullable"] is False

    assert ("estimate_job_id", "project_id", "drawing_revision_id") in unique_constraints[
        "estimate_job_inputs"
    ]
    assert primary_keys["estimate_job_input_catalog_refs"] == (
        "estimate_job_id",
        "ref_type",
        "selection_key",
    )
    assert ("estimate_job_id", "ref_order") in unique_constraints["estimate_job_input_catalog_refs"]

    for constraint_name in (
        "ck_estimate_job_inputs_source_job_type_estimate",
        "ck_estimate_job_inputs_currency_gbp",
        "ck_estimate_job_inputs_pricing_mode_valid",
        "ck_estimate_job_inputs_assumptions_json_object",
    ):
        assert constraint_name in check_constraints["estimate_job_inputs"]
    # Path B 3: estimate creation is no longer gated to allowed + trusted takeoffs.
    for constraint_name in (
        "ck_estimate_job_inputs_quantity_gate_allowed",
        "ck_estimate_job_inputs_trusted_totals_true",
    ):
        assert constraint_name not in check_constraints["estimate_job_inputs"]
    for constraint_name in (
        "ck_estimate_job_input_catalog_refs_selection_key_nonblank",
        "ck_estimate_job_input_catalog_refs_ref_order_nonnegative",
        "ck_estimate_job_input_catalog_refs_ref_type_valid",
        "ck_estimate_job_input_catalog_refs_typed_catalog_ref_match",
        "ck_estimate_job_input_catalog_refs_checksum_sha256_format",
        "ck_est_job_input_catalog_refs_context_json_object",
    ):
        assert constraint_name in check_constraints["estimate_job_input_catalog_refs"]

    assert (
        (
            "estimate_job_id",
            "project_id",
            "source_file_id",
            "drawing_revision_id",
            "source_job_type",
        ),
        "jobs",
        ("id", "project_id", "file_id", "base_revision_id", "job_type"),
    ) in foreign_keys["estimate_job_inputs"]
    assert (
        (
            "quantity_takeoff_id",
            "project_id",
            "drawing_revision_id",
        ),
        "quantity_takeoffs",
        ("id", "project_id", "drawing_revision_id"),
    ) in foreign_keys["estimate_job_inputs"]
    for constrained_column, referred_table in (
        ("rate_catalog_entry_id", "rate_catalog_entries"),
        ("material_catalog_entry_id", "material_catalog_entries"),
        ("formula_definition_id", "formula_definitions"),
    ):
        assert ((constrained_column,), referred_table, ("id",)) in foreign_keys[
            "estimate_job_input_catalog_refs"
        ]


async def test_estimate_job_input_persists_lineage_catalog_refs_and_worker_boundary(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    rate, material, formula = await snapshot_item_persistence._create_catalog_rows(
        db_session,
        seed.project_id,
    )
    estimate_job_id = await _create_estimate_job_input(db_session, seed)
    db_session.add_all(
        [
            _build_catalog_ref(
                estimate_job_id=estimate_job_id,
                ref_type="rate",
                selection_key="labour:installer",
                ref_order=0,
                catalog_id=rate.id,
                catalog_checksum_sha256=rate.checksum_sha256,
            ),
            _build_catalog_ref(
                estimate_job_id=estimate_job_id,
                ref_type="material",
                selection_key="cable:standard",
                ref_order=1,
                catalog_id=material.id,
                catalog_checksum_sha256=material.checksum_sha256,
            ),
            _build_catalog_ref(
                estimate_job_id=estimate_job_id,
                ref_type="formula",
                selection_key="waste-factor:v1",
                ref_order=2,
                catalog_id=formula.id,
                catalog_checksum_sha256=formula.checksum_sha256,
            ),
        ]
    )

    await db_session.commit()

    persisted_input = await db_session.get(EstimateJobInput, estimate_job_id)
    assert persisted_input is not None
    assert persisted_input.project_id == seed.project_id
    assert persisted_input.source_file_id == seed.file_id
    assert persisted_input.drawing_revision_id == seed.drawing_revision_id
    assert persisted_input.quantity_takeoff_id == seed.quantity_takeoff_id
    assert persisted_input.source_job_type == JobType.ESTIMATE.value
    assert persisted_input.currency == "GBP"
    assert persisted_input.pricing_effective_date == date(2026, 5, 18)
    assert persisted_input.pricing_mode == "explicit"
    assert persisted_input.assumptions_json == {"waste_factor": "10%"}

    refs = (
        (
            await db_session.execute(
                select(EstimateJobInputCatalogRef)
                .where(EstimateJobInputCatalogRef.estimate_job_id == estimate_job_id)
                .order_by(EstimateJobInputCatalogRef.ref_order.asc())
            )
        )
        .scalars()
        .all()
    )
    assert [(ref.ref_type, ref.selection_key, ref.ref_order) for ref in refs] == [
        ("rate", "labour:installer", 0),
        ("material", "cable:standard", 1),
        ("formula", "waste-factor:v1", 2),
    ]

    publisher = worker_module.get_job_enqueue_publisher(JobType.ESTIMATE)
    assert worker_module.is_recoverable_enqueue_job_type(JobType.ESTIMATE) is True
    assert publisher is not None


async def test_estimate_job_contract_rejects_ambiguous_job_inputs(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)

    await _assert_commit_fails(
        db_session,
        Job(
            id=uuid.uuid4(),
            project_id=seed.project_id,
            file_id=seed.file_id,
            job_type=JobType.ESTIMATE.value,
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
            job_type=JobType.ESTIMATE.value,
            status=JobStatus.PENDING.value,
            enqueue_status="pending",
            enqueue_attempts=0,
            cancel_requested=False,
        ),
        "ck_jobs_revision_scoped_extraction_profile_forbidden",
    )

    reprocess_job_id = uuid.uuid4()
    db_session.add(
        Job(
            id=reprocess_job_id,
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
    await db_session.commit()

    for kwargs, expected_substring in (
        (
            {"source_job_type": JobType.REPROCESS.value},
            "ck_estimate_job_inputs_source_job_type_estimate",
        ),
        # Path B 3: quantity_gate / trusted_totals are no longer gate-constrained here.
        ({"currency": "USD"}, "ck_estimate_job_inputs_currency_gbp"),
        ({"pricing_mode": "retry_timestamp"}, "ck_estimate_job_inputs_pricing_mode_valid"),
        ({"assumptions_json": []}, "ck_estimate_job_inputs_assumptions_json_object"),
    ):
        await _assert_commit_fails(
            db_session,
            _build_estimate_job_input(seed, estimate_job_id=reprocess_job_id, **kwargs),
            expected_substring,
        )


async def test_estimate_catalog_refs_reject_ambiguous_or_nondeterministic_inputs(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    rate, _, _ = await snapshot_item_persistence._create_catalog_rows(db_session, seed.project_id)
    rate_id = rate.id
    rate_checksum = rate.checksum_sha256
    estimate_job_id = await _create_estimate_job_input(db_session, seed)
    await db_session.commit()

    for ref, expected_substring in (
        (
            _build_catalog_ref(
                estimate_job_id=estimate_job_id,
                ref_type="rate",
                selection_key="   ",
                ref_order=0,
                catalog_id=rate_id,
                catalog_checksum_sha256=rate_checksum,
            ),
            "ck_estimate_job_input_catalog_refs_selection_key_nonblank",
        ),
        (
            EstimateJobInputCatalogRef(
                estimate_job_id=estimate_job_id,
                ref_type="rate",
                selection_key="labour:installer",
                ref_order=0,
                rate_catalog_entry_id=rate_id,
                material_catalog_entry_id=rate_id,
                formula_definition_id=None,
                catalog_checksum_sha256=rate_checksum,
                selection_context_json={"source": "test"},
            ),
            "ck_estimate_job_input_catalog_refs_typed_catalog_ref_match",
        ),
        (
            _build_catalog_ref(
                estimate_job_id=estimate_job_id,
                ref_type="rate",
                selection_key="labour:installer",
                ref_order=0,
                catalog_id=rate_id,
                catalog_checksum_sha256="A" * 64,
            ),
            "ck_estimate_job_input_catalog_refs_checksum_sha256_format",
        ),
        (
            _build_catalog_ref(
                estimate_job_id=estimate_job_id,
                ref_type="rate",
                selection_key="labour:installer",
                ref_order=0,
                catalog_id=rate_id,
                catalog_checksum_sha256=rate_checksum,
                selection_context_json=[],
            ),
            "ck_est_job_input_catalog_refs_context_json_object",
        ),
    ):
        await _assert_commit_fails(db_session, ref, expected_substring)

    db_session.add(
        _build_catalog_ref(
            estimate_job_id=estimate_job_id,
            ref_type="rate",
            selection_key="labour:installer",
            ref_order=0,
            catalog_id=rate_id,
            catalog_checksum_sha256=rate_checksum,
        )
    )
    await db_session.commit()

    await _assert_commit_fails(
        db_session,
        _build_catalog_ref(
            estimate_job_id=estimate_job_id,
            ref_type="rate",
            selection_key="labour:installer",
            ref_order=1,
            catalog_id=rate_id,
            catalog_checksum_sha256=rate_checksum,
        ),
        "uq_estimate_job_input_catalog_refs_job_ref_type_selection_key",
    )
    await _assert_commit_fails(
        db_session,
        _build_catalog_ref(
            estimate_job_id=estimate_job_id,
            ref_type="rate",
            selection_key="labour:second",
            ref_order=0,
            catalog_id=rate_id,
            catalog_checksum_sha256=rate_checksum,
        ),
        "uq_estimate_job_input_catalog_refs_estimate_job_id_ref_order",
    )


async def test_estimate_job_inputs_are_append_only_and_downgrade_guarded(
    async_client: httpx.AsyncClient,
    db_session: AsyncSession,
) -> None:
    await _run_downgrade_guard(db_session)

    seed = await quantity_takeoff_persistence._seed_quantity_lineage(async_client)
    rate, _, _ = await snapshot_item_persistence._create_catalog_rows(db_session, seed.project_id)
    estimate_job_id = await _create_estimate_job_input(db_session, seed)
    db_session.add(
        _build_catalog_ref(
            estimate_job_id=estimate_job_id,
            ref_type="rate",
            selection_key="labour:installer",
            ref_order=0,
            catalog_id=rate.id,
            catalog_checksum_sha256=rate.checksum_sha256,
        )
    )
    await db_session.commit()

    await _assert_append_only_sql_mutation_fails(
        db_session,
        "UPDATE estimate_job_inputs SET currency = 'USD' WHERE estimate_job_id = :job_id",
        operation="UPDATE",
        table_name="estimate_job_inputs",
        params={"job_id": estimate_job_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        "DELETE FROM estimate_job_input_catalog_refs WHERE estimate_job_id = :job_id",
        operation="DELETE",
        table_name="estimate_job_input_catalog_refs",
        params={"job_id": estimate_job_id},
    )

    with pytest.raises(RuntimeError, match="estimate_job_input_catalog_refs contains 1 row"):
        await _run_downgrade_guard(db_session)
