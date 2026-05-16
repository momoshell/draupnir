"""Database persistence tests for estimation catalog constraints."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Callable, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.estimation_catalog import (
    EstimationFormula,
    EstimationFormulaSupersession,
    EstimationMaterial,
    EstimationMaterialSupersession,
    EstimationRate,
    EstimationRateSupersession,
)
from tests.conftest import requires_database

pytestmark = [pytest.mark.asyncio, requires_database]

CatalogBuilder = Callable[..., Any]
CATALOG_TABLES: tuple[str, ...] = (
    "formula_definition_supersessions",
    "material_catalog_entry_supersessions",
    "rate_catalog_entry_supersessions",
    "formula_definitions",
    "material_catalog_entries",
    "rate_catalog_entries",
)


async def _cleanup_catalog_tables() -> None:
    """Remove persisted catalog rows and related projects between tests."""

    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        try:
            for table_name in CATALOG_TABLES:
                await session.execute(text(f'ALTER TABLE "{table_name}" DISABLE TRIGGER ALL'))

            await session.execute(
                text(
                    "TRUNCATE TABLE "
                    "formula_definition_supersessions, "
                    "material_catalog_entry_supersessions, "
                    "rate_catalog_entry_supersessions, "
                    "formula_definitions, "
                    "material_catalog_entries, "
                    "rate_catalog_entries"
                )
            )

            for table_name in CATALOG_TABLES:
                await session.execute(text(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL'))

            await session.commit()
        except Exception:
            await session.rollback()
            raise


@pytest_asyncio.fixture
async def cleanup_estimation_catalog() -> AsyncGenerator[None, None]:
    """Clear catalog tables after each test, including append-only tables."""

    await _cleanup_catalog_tables()
    yield
    await _cleanup_catalog_tables()


@pytest_asyncio.fixture
async def db_session(cleanup_estimation_catalog: None) -> AsyncGenerator[AsyncSession, None]:
    """Provide a clean async session for catalog persistence tests."""

    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


def _checksum(seed: str) -> str:
    return (seed.encode("utf-8").hex() + ("0" * 64))[:64]


async def _create_project(session: AsyncSession) -> uuid.UUID:
    column_rows = (
        await session.execute(
            text(
                """
                SELECT column_name, data_type, is_nullable, column_default, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'projects'
                ORDER BY ordinal_position
                """
            )
        )
    ).all()

    values: dict[str, object] = {}
    for column_name, data_type, is_nullable, column_default, udt_name in column_rows:
        if column_default is not None or is_nullable == "YES":
            continue
        values[column_name] = await _project_column_value(
            session,
            column_name=column_name,
            data_type=data_type,
            udt_name=udt_name,
        )

    if values:
        columns = ", ".join(f'"{column_name}"' for column_name in values)
        params = ", ".join(f":{column_name}" for column_name in values)
        insert_sql = f"INSERT INTO projects ({columns}) VALUES ({params}) RETURNING id"
        result = await session.execute(text(insert_sql), values)
    else:
        result = await session.execute(text("INSERT INTO projects DEFAULT VALUES RETURNING id"))

    project_id = cast(uuid.UUID, result.scalar_one())
    await session.commit()
    return project_id


async def _project_column_value(
    session: AsyncSession,
    *,
    column_name: str,
    data_type: str,
    udt_name: str | None,
) -> object:
    normalized_type = data_type.lower()

    if column_name == "id" or normalized_type == "uuid":
        return uuid.uuid4()
    if normalized_type in {"character varying", "character", "text"}:
        return f"test-{column_name}-{uuid.uuid4().hex[:8]}"
    if normalized_type in {"smallint", "integer", "bigint"}:
        return 1
    if normalized_type in {"numeric", "real", "double precision"}:
        return Decimal("1")
    if normalized_type == "boolean":
        return False
    if normalized_type == "date":
        return date(2026, 1, 1)
    if normalized_type.startswith("timestamp"):
        return datetime.now(UTC)
    if normalized_type in {"json", "jsonb"}:
        return {}
    if normalized_type == "array":
        return []
    if normalized_type == "user-defined" and udt_name is not None:
        enum_value = await session.scalar(
            text(
                """
                SELECT enumlabel
                FROM pg_type
                JOIN pg_enum ON pg_type.oid = pg_enum.enumtypid
                WHERE typname = :udt_name
                ORDER BY enumsortorder
                LIMIT 1
                """
            ),
            {"udt_name": udt_name},
        )
        if enum_value is not None:
            return enum_value

    raise AssertionError(
        f"Unsupported required projects column {column_name!r} with type {data_type!r}"
    )


async def _persist(session: AsyncSession, *objects: object) -> None:
    session.add_all(list(objects))
    await session.commit()


async def _assert_commit_fails(
    session: AsyncSession,
    objects: object | Sequence[object],
    expected_substring: str | Sequence[str],
) -> None:
    pending_objects = [objects] if not isinstance(objects, Sequence) else list(objects)
    session.add_all(pending_objects)
    expected_substrings = (
        [expected_substring]
        if isinstance(expected_substring, str)
        else list(expected_substring)
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
    table_name: str,
    operation: str,
    params: dict[str, object] | None = None,
    expected_substring: str | Sequence[str] | None = None,
) -> None:
    expected_substrings = (
        [expected_substring]
        if isinstance(expected_substring, str)
        else list(expected_substring)
        if expected_substring is not None
        else [f"append-only trigger blocked {operation} on {table_name}"]
    )

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
        assert any(substring.lower() in message for substring in expected_substrings)
    else:
        pytest.fail(
            "Expected append-only mutation failure for "
            f"{operation} on {table_name}"
        )


def _build_rate(**overrides: Any) -> EstimationRate:
    payload: dict[str, Any] = {
        "scope_type": "global",
        "project_id": None,
        "rate_key": "labour.install",
        "source": "tests",
        "metadata_json": {"fixture": True},
        "name": "Labour install",
        "item_type": "labour",
        "per_unit": "m2",
        "currency": "GBP",
        "amount": Decimal("10.000000"),
        "effective_from": date(2026, 1, 1),
        "effective_to": date(2026, 2, 1),
        "checksum_sha256": _checksum("rate-default"),
    }
    payload.update(overrides)
    return EstimationRate(**payload)


def _build_material(**overrides: Any) -> EstimationMaterial:
    payload: dict[str, Any] = {
        "scope_type": "global",
        "project_id": None,
        "material_key": "material.board",
        "source": "tests",
        "metadata_json": {"fixture": True},
        "name": "Board",
        "unit": "sheet",
        "currency": "GBP",
        "unit_cost": Decimal("12.500000"),
        "effective_from": date(2026, 1, 1),
        "effective_to": date(2026, 2, 1),
        "checksum_sha256": _checksum("material-default"),
    }
    payload.update(overrides)
    return EstimationMaterial(**payload)


def _build_formula(**overrides: Any) -> EstimationFormula:
    payload: dict[str, Any] = {
        "scope_type": "global",
        "project_id": None,
        "formula_id": "formula.paint",
        "version": 1,
        "name": "Paint formula",
        "dsl_version": "1.0",
        "output_key": "estimate.total",
        "output_contract_json": {"type": "money"},
        "declared_inputs_json": [{"key": "quantity"}],
        "expression_json": {"op": "sum", "args": []},
        "rounding_json": {},
        "checksum_sha256": _checksum("formula-default"),
    }
    payload.update(overrides)
    return EstimationFormula(**payload)


async def test_rate_catalog_global_overlap_rejects_adjacent_windows_allowed(
    db_session: AsyncSession,
) -> None:
    await _persist(db_session, _build_rate())

    await _assert_commit_fails(
        db_session,
        _build_rate(
            effective_from=date(2026, 1, 15),
            effective_to=date(2026, 2, 15),
            checksum_sha256=_checksum("rate-overlap"),
        ),
        "ex_rate_catalog_entries_global_no_overlap",
    )

    await _persist(
        db_session,
        _build_rate(
            effective_from=date(2026, 2, 1),
            effective_to=date(2026, 3, 1),
            checksum_sha256=_checksum("rate-adjacent"),
        ),
    )

    assert await db_session.scalar(select(func.count()).select_from(EstimationRate)) == 2


async def test_material_catalog_project_overlap_rejects_adjacent_windows_allowed(
    db_session: AsyncSession,
) -> None:
    project_id = await _create_project(db_session)

    await _persist(
        db_session,
        _build_material(scope_type="project", project_id=project_id),
    )

    await _assert_commit_fails(
        db_session,
        _build_material(
            scope_type="project",
            project_id=project_id,
            effective_from=date(2026, 1, 15),
            effective_to=date(2026, 2, 15),
            checksum_sha256=_checksum("material-overlap"),
        ),
        "ex_material_catalog_entries_project_no_overlap",
    )

    await _persist(
        db_session,
        _build_material(
            scope_type="project",
            project_id=project_id,
            effective_from=date(2026, 2, 1),
            effective_to=date(2026, 3, 1),
            checksum_sha256=_checksum("material-adjacent"),
        ),
    )

    assert await db_session.scalar(select(func.count()).select_from(EstimationMaterial)) == 2


@pytest.mark.parametrize(
    ("builder", "scope_constraint", "scope_project_constraint", "checksum_constraint"),
    [
        pytest.param(
            _build_rate,
            "ck_rate_catalog_entries_scope_type",
            "ck_rate_catalog_entries_scope_project_id",
            "ck_rate_catalog_entries_checksum_sha256",
            id="rate",
        ),
        pytest.param(
            _build_material,
            "ck_material_catalog_entries_scope_type",
            "ck_material_catalog_entries_scope_project_id",
            "ck_material_catalog_entries_checksum_sha256",
            id="material",
        ),
        pytest.param(
            _build_formula,
            "ck_formula_definitions_scope_type",
            "ck_formula_definitions_scope_project_id",
            "ck_formula_definitions_checksum_sha256",
            id="formula",
        ),
    ],
)
async def test_catalog_rows_reject_invalid_scope_project_and_checksum(
    db_session: AsyncSession,
    builder: CatalogBuilder,
    scope_constraint: str,
    scope_project_constraint: str,
    checksum_constraint: str,
) -> None:
    project_id = await _create_project(db_session)

    await _assert_commit_fails(
        db_session,
        builder(scope_type="invalid", checksum_sha256=_checksum(f"{scope_constraint}-scope")),
        (scope_constraint, scope_project_constraint),
    )
    await _assert_commit_fails(
        db_session,
        builder(
            scope_type="global",
            project_id=project_id,
            checksum_sha256=_checksum(f"{scope_project_constraint}-scope-project"),
        ),
        scope_project_constraint,
    )
    await _assert_commit_fails(
        db_session,
        builder(
            scope_type="project",
            project_id=uuid.uuid4(),
            checksum_sha256=_checksum(f"{scope_project_constraint}-foreign-key"),
        ),
        "foreign key constraint",
    )
    await _assert_commit_fails(
        db_session,
        builder(checksum_sha256="not-a-valid-checksum"),
        checksum_constraint,
    )


async def test_formula_definitions_reject_duplicate_version_and_checksum_per_scope(
    db_session: AsyncSession,
) -> None:
    await _persist(
        db_session,
        _build_formula(
            formula_id="formula.global",
            version=1,
            checksum_sha256=_checksum("formula-global-v1"),
        ),
    )

    await _assert_commit_fails(
        db_session,
        _build_formula(
            formula_id="formula.global",
            version=1,
            checksum_sha256=_checksum("formula-global-v1-dup-version"),
        ),
        "uq_formula_definitions_global_version",
    )
    await _assert_commit_fails(
        db_session,
        _build_formula(
            formula_id="formula.global",
            version=2,
            checksum_sha256=_checksum("formula-global-v1"),
        ),
        "uq_formula_definitions_global_checksum",
    )

    project_id = await _create_project(db_session)
    await _persist(
        db_session,
        _build_formula(
            scope_type="project",
            project_id=project_id,
            formula_id="formula.project",
            version=1,
            checksum_sha256=_checksum("formula-project-v1"),
        ),
    )

    await _assert_commit_fails(
        db_session,
        _build_formula(
            scope_type="project",
            project_id=project_id,
            formula_id="formula.project",
            version=1,
            checksum_sha256=_checksum("formula-project-v1-dup-version"),
        ),
        "uq_formula_definitions_project_version",
    )
    await _assert_commit_fails(
        db_session,
        _build_formula(
            scope_type="project",
            project_id=project_id,
            formula_id="formula.project",
            version=2,
            checksum_sha256=_checksum("formula-project-v1"),
        ),
        "uq_formula_definitions_project_checksum",
    )


@pytest.mark.parametrize(
    (
        "builder",
        "same_row_overrides",
        "predecessor_overrides",
        "successor_overrides",
        "supersession_type",
        "predecessor_attr",
        "successor_attr",
        "same_row_message",
        "mismatched_key_message",
    ),
    [
        pytest.param(
            _build_rate,
            {
                "rate_key": "labour.same-row",
                "effective_from": date(2026, 4, 1),
                "effective_to": date(2026, 5, 1),
                "checksum_sha256": _checksum("rate-same-row"),
            },
            {
                "rate_key": "labour.base",
                "effective_from": date(2026, 6, 1),
                "effective_to": date(2026, 7, 1),
                "checksum_sha256": _checksum("rate-predecessor"),
            },
            {
                "rate_key": "labour.other",
                "effective_from": date(2026, 6, 1),
                "effective_to": date(2026, 7, 1),
                "checksum_sha256": _checksum("rate-successor"),
            },
            EstimationRateSupersession,
            "predecessor_rate_id",
            "successor_rate_id",
            "ck_rate_catalog_entry_supersessions_distinct_nodes",
            "rate supersession must preserve rate key and scope",
            id="rate",
        ),
        pytest.param(
            _build_material,
            {
                "material_key": "material.same-row",
                "effective_from": date(2026, 4, 1),
                "effective_to": date(2026, 5, 1),
                "checksum_sha256": _checksum("material-same-row"),
            },
            {
                "material_key": "material.base",
                "effective_from": date(2026, 6, 1),
                "effective_to": date(2026, 7, 1),
                "checksum_sha256": _checksum("material-predecessor"),
            },
            {
                "material_key": "material.other",
                "effective_from": date(2026, 6, 1),
                "effective_to": date(2026, 7, 1),
                "checksum_sha256": _checksum("material-successor"),
            },
            EstimationMaterialSupersession,
            "predecessor_material_id",
            "successor_material_id",
            "ck_material_catalog_entry_supersessions_distinct_nodes",
            "material supersession must preserve material key and scope",
            id="material",
        ),
        pytest.param(
            _build_formula,
            {
                "formula_id": "formula.same-row",
                "version": 1,
                "checksum_sha256": _checksum("formula-same-row"),
            },
            {
                "formula_id": "formula.base",
                "version": 1,
                "checksum_sha256": _checksum("formula-predecessor"),
            },
            {
                "formula_id": "formula.other",
                "version": 1,
                "checksum_sha256": _checksum("formula-successor"),
            },
            EstimationFormulaSupersession,
            "predecessor_formula_id",
            "successor_formula_id",
            "ck_formula_definition_supersessions_distinct_nodes",
            "formula supersession must preserve formula key and scope",
            id="formula",
        ),
    ],
)
async def test_supersessions_reject_same_row_and_mismatched_key_pairs(
    db_session: AsyncSession,
    builder: CatalogBuilder,
    same_row_overrides: dict[str, Any],
    predecessor_overrides: dict[str, Any],
    successor_overrides: dict[str, Any],
    supersession_type: type[Any],
    predecessor_attr: str,
    successor_attr: str,
    same_row_message: str,
    mismatched_key_message: str,
) -> None:
    same_row = builder(**same_row_overrides)
    predecessor = builder(**predecessor_overrides)
    successor = builder(**successor_overrides)

    await _persist(db_session, same_row)

    await _assert_commit_fails(
        db_session,
        supersession_type(
            **{
                predecessor_attr: same_row.id,
                successor_attr: same_row.id,
            }
        ),
        same_row_message,
    )

    await _persist(db_session, predecessor, successor)

    await _assert_commit_fails(
        db_session,
        supersession_type(
            **{
                predecessor_attr: predecessor.id,
                successor_attr: successor.id,
            }
        ),
        mismatched_key_message,
    )


@pytest.mark.parametrize(
    ("builder", "table_name", "supersession_table_name"),
    [
        pytest.param(
            _build_rate,
            "rate_catalog_entries",
            "rate_catalog_entry_supersessions",
            id="rate",
        ),
        pytest.param(
            _build_material,
            "material_catalog_entries",
            "material_catalog_entry_supersessions",
            id="material",
        ),
        pytest.param(
            _build_formula,
            "formula_definitions",
            "formula_definition_supersessions",
            id="formula",
        ),
    ],
)
async def test_catalog_tables_reject_append_only_mutations(
    db_session: AsyncSession,
    builder: CatalogBuilder,
    table_name: str,
    supersession_table_name: str,
) -> None:
    row = builder(checksum_sha256=_checksum(f"{table_name}-append-only"))
    await _persist(db_session, row)
    row_id = row.id

    await _assert_append_only_sql_mutation_fails(
        db_session,
        f"UPDATE \"{table_name}\" SET created_at = created_at + INTERVAL '1 second' "
        "WHERE id = :row_id",
        table_name=table_name,
        operation="UPDATE",
        params={"row_id": row_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        f'DELETE FROM "{table_name}" WHERE id = :row_id',
        table_name=table_name,
        operation="DELETE",
        params={"row_id": row_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        f'TRUNCATE TABLE "{table_name}", "{supersession_table_name}"',
        table_name=table_name,
        operation="TRUNCATE",
        expected_substring=(
            f"append-only trigger blocked TRUNCATE on {table_name}",
            f"append-only trigger blocked TRUNCATE on {supersession_table_name}",
        ),
    )


@pytest.mark.parametrize(
    (
        "builder",
        "predecessor_overrides",
        "successor_overrides",
        "supersession_type",
        "predecessor_attr",
        "successor_attr",
        "table_name",
    ),
    [
        pytest.param(
            _build_rate,
            {
                "rate_key": "labour.append-only",
                "effective_from": date(2026, 8, 1),
                "effective_to": date(2026, 9, 1),
                "checksum_sha256": _checksum("rate-append-only-predecessor"),
            },
            {
                "rate_key": "labour.append-only",
                "effective_from": date(2026, 9, 1),
                "effective_to": date(2026, 10, 1),
                "checksum_sha256": _checksum("rate-append-only-successor"),
            },
            EstimationRateSupersession,
            "predecessor_rate_id",
            "successor_rate_id",
            "rate_catalog_entry_supersessions",
            id="rate",
        ),
        pytest.param(
            _build_material,
            {
                "material_key": "material.append-only",
                "effective_from": date(2026, 8, 1),
                "effective_to": date(2026, 9, 1),
                "checksum_sha256": _checksum("material-append-only-predecessor"),
            },
            {
                "material_key": "material.append-only",
                "effective_from": date(2026, 9, 1),
                "effective_to": date(2026, 10, 1),
                "checksum_sha256": _checksum("material-append-only-successor"),
            },
            EstimationMaterialSupersession,
            "predecessor_material_id",
            "successor_material_id",
            "material_catalog_entry_supersessions",
            id="material",
        ),
        pytest.param(
            _build_formula,
            {
                "formula_id": "formula.append-only",
                "version": 1,
                "checksum_sha256": _checksum("formula-append-only-predecessor"),
            },
            {
                "formula_id": "formula.append-only",
                "version": 2,
                "checksum_sha256": _checksum("formula-append-only-successor"),
            },
            EstimationFormulaSupersession,
            "predecessor_formula_id",
            "successor_formula_id",
            "formula_definition_supersessions",
            id="formula",
        ),
    ],
)
async def test_supersession_tables_reject_append_only_mutations(
    db_session: AsyncSession,
    builder: CatalogBuilder,
    predecessor_overrides: dict[str, Any],
    successor_overrides: dict[str, Any],
    supersession_type: type[Any],
    predecessor_attr: str,
    successor_attr: str,
    table_name: str,
) -> None:
    predecessor = builder(**predecessor_overrides)
    successor = builder(**successor_overrides)
    await _persist(db_session, predecessor, successor)

    supersession = supersession_type(
        **{
            predecessor_attr: predecessor.id,
            successor_attr: successor.id,
        }
    )
    await _persist(db_session, supersession)
    supersession_id = supersession.id

    await _assert_append_only_sql_mutation_fails(
        db_session,
        f"UPDATE \"{table_name}\" SET created_at = created_at + INTERVAL '1 second' "
        "WHERE id = :row_id",
        table_name=table_name,
        operation="UPDATE",
        params={"row_id": supersession_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        f'DELETE FROM "{table_name}" WHERE id = :row_id',
        table_name=table_name,
        operation="DELETE",
        params={"row_id": supersession_id},
    )
    await _assert_append_only_sql_mutation_fails(
        db_session,
        f'TRUNCATE TABLE "{table_name}"',
        table_name=table_name,
        operation="TRUNCATE",
    )
