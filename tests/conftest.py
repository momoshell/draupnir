"""Shared fixtures for integration tests."""

import asyncio
import os
import re
import shutil
import subprocess
from collections.abc import AsyncGenerator, Callable, Generator, Mapping, MutableMapping
from pathlib import Path
from typing import NamedTuple
from urllib.parse import parse_qsl, quote, unquote, urlparse, urlunparse

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_LOCAL_DATABASE_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_XDIST_WORKER_ID_RE = re.compile(r"^gw[0-9]+$")
_XDIST_BLOCKED_QUERY_PARAMS = frozenset(
    {
        "host",
        "hostaddr",
        "port",
        "database",
        "dbname",
        "user",
        "password",
        "service",
    }
)
_POSTGRES_IDENTIFIER_MAX_BYTES = 63


class XdistDatabaseConfig(NamedTuple):
    """Derived database URLs for one pytest-xdist worker process."""

    worker_id: str
    worker_database_name: str
    worker_database_url: str
    maintenance_database_url: str


def _xdist_worker_id(environ: Mapping[str, str]) -> str | None:
    """Return the active xdist worker id, excluding serial/master collection."""

    worker_id = environ.get("PYTEST_XDIST_WORKER")
    if worker_id is None or worker_id == "master":
        return None
    return worker_id


def _xdist_testrun_uid(environ: Mapping[str, str]) -> str | None:
    """Return the xdist test-run uid when present."""

    testrun_uid = environ.get("PYTEST_XDIST_TESTRUNUID")
    if testrun_uid is None:
        return None
    normalized = testrun_uid.strip()
    return normalized or None


def _derive_database_identity(database_url: str) -> str:
    """Return a sanitized host/database identity for error messages."""

    parsed = urlparse(database_url)
    host = parsed.hostname or "<unknown-host>"
    database_name = unquote(parsed.path.lstrip("/")) or "<unknown-database>"
    if parsed.port is None:
        return f"{host}/{database_name}"
    return f"{host}:{parsed.port}/{database_name}"


def _derive_xdist_database_config(
    database_url: str,
    worker_id: str,
    *,
    testrun_uid: str | None = None,
) -> XdistDatabaseConfig:
    """Derive a safe physical database URL for one xdist worker."""

    parsed = urlparse(database_url)
    if not parsed.scheme.startswith("postgresql"):
        raise ValueError("DATABASE_URL must use a PostgreSQL URL for xdist DB isolation.")
    if parsed.hostname not in _LOCAL_DATABASE_HOSTS:
        raise ValueError(
            "DATABASE_URL must point to localhost, 127.0.0.1, or ::1 for xdist DB isolation."
        )

    unsafe_query_params = sorted(
        {
            key.casefold()
            for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
            if key.casefold() in _XDIST_BLOCKED_QUERY_PARAMS
        }
    )
    if unsafe_query_params:
        raise ValueError(
            "DATABASE_URL contains unsupported query parameter(s) for xdist DB isolation: "
            + ", ".join(unsafe_query_params)
        )

    database_name = unquote(parsed.path.lstrip("/"))
    if not database_name.endswith("_test"):
        raise ValueError(
            "DATABASE_URL must use a dedicated test database name ending in '_test' for "
            "xdist DB isolation."
        )
    if _XDIST_WORKER_ID_RE.fullmatch(worker_id) is None:
        raise ValueError(f"unsupported pytest-xdist worker id for DB isolation: {worker_id!r}")

    worker_suffix = worker_id if testrun_uid is None else f"{testrun_uid}_{worker_id}"
    worker_database_name = f"{database_name}_{worker_suffix}"
    if len(worker_database_name.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES:
        raise ValueError(
            "Derived worker database name exceeds PostgreSQL identifier limit "
            f"({_POSTGRES_IDENTIFIER_MAX_BYTES} bytes): {worker_database_name!r}"
        )

    worker_database_url = urlunparse(parsed._replace(path=f"/{quote(worker_database_name)}"))
    maintenance_database_url = urlunparse(parsed._replace(path="/postgres"))
    return XdistDatabaseConfig(
        worker_id=worker_id,
        worker_database_name=worker_database_name,
        worker_database_url=worker_database_url,
        maintenance_database_url=maintenance_database_url,
    )


def _asyncpg_dsn(database_url: str) -> str:
    """Return an asyncpg-compatible DSN from the SQLAlchemy async URL."""

    parsed = urlparse(database_url)
    scheme = parsed.scheme.split("+", maxsplit=1)[0]
    return urlunparse(parsed._replace(scheme=scheme))


def _quote_postgres_identifier(value: str) -> str:
    """Quote a PostgreSQL identifier for CREATE DATABASE."""

    return '"' + value.replace('"', '""') + '"'


async def _create_xdist_worker_database_async(config: XdistDatabaseConfig) -> None:
    """Create the worker database if it does not already exist."""

    import asyncpg  # type: ignore[import-untyped]

    connection = await asyncpg.connect(_asyncpg_dsn(config.maintenance_database_url))
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            config.worker_database_name,
        )
        if exists is None:
            await connection.execute(
                f"CREATE DATABASE {_quote_postgres_identifier(config.worker_database_name)}"
            )
    finally:
        await connection.close()


def _create_xdist_worker_database(config: XdistDatabaseConfig) -> None:
    """Synchronously ensure the worker database exists before app imports."""

    asyncio.run(_create_xdist_worker_database_async(config))


def _run_xdist_worker_migrations(
    worker_database_url: str,
    environ: Mapping[str, str],
) -> None:
    """Run Alembic migrations against the isolated worker database."""

    migration_env = {**environ, "DATABASE_URL": worker_database_url}
    result = subprocess.run(
        ["uv", "run", "alembic", "upgrade", "head"],
        check=False,
        env=migration_env,
    )
    if result.returncode != 0:
        worker_database_identity = _derive_database_identity(worker_database_url)
        raise pytest.UsageError(
            "pytest-xdist worker database migration failed for "
            f"{worker_database_identity!r} with exit code {result.returncode}."
        )


def _configure_xdist_worker_database(
    environ: MutableMapping[str, str] = os.environ,
    *,
    create_database: Callable[[XdistDatabaseConfig], None] = _create_xdist_worker_database,
    run_migrations: Callable[[str, Mapping[str, str]], None] = _run_xdist_worker_migrations,
) -> str | None:
    """Configure this xdist worker process to use its own physical database."""

    worker_id = _xdist_worker_id(environ)
    testrun_uid = _xdist_testrun_uid(environ)
    base_database_url = environ.get("DATABASE_URL")
    if worker_id is None or not base_database_url:
        return None

    try:
        config = _derive_xdist_database_config(
            base_database_url,
            worker_id,
            testrun_uid=testrun_uid,
        )
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc

    create_database(config)
    environ["DATABASE_URL"] = config.worker_database_url
    run_migrations(config.worker_database_url, environ)
    return config.worker_database_url


_configure_xdist_worker_database()

import app.api.v1.files as files_api  # noqa: E402
import app.jobs.worker as worker_module  # noqa: E402
from app.core.config import settings  # noqa: E402
from app.db.session import get_db  # noqa: E402
from app.main import app as fastapi_app  # noqa: E402
from app.storage.dependencies import _get_default_storage  # noqa: E402

APPEND_ONLY_PROTECTED_TABLES: tuple[str, ...] = (
    "files",
    "extraction_profiles",
    "adapter_run_outputs",
    "cad_change_sets",
    "cad_change_operations",
    "cad_change_set_validation_results",
    "drawing_revisions",
    "validation_reports",
    "generated_artifacts",
    "job_events",
    "estimate_versions",
    "estimate_job_inputs",
    "export_job_inputs",
    "estimate_job_input_catalog_refs",
    "estimate_snapshot_entries",
    "estimate_items",
    "quantity_takeoffs",
    "quantity_items",
    "revision_entity_manifests",
    "revision_layouts",
    "revision_layers",
    "revision_blocks",
    "revision_entities",
)
_PROJECT_TRUNCATE_CASCADE_CATALOG_TABLES: tuple[str, ...] = (
    "formula_definition_supersessions",
    "material_catalog_entry_supersessions",
    "rate_catalog_entry_supersessions",
    "formula_definitions",
    "material_catalog_entries",
    "rate_catalog_entries",
)
_PROJECT_TRUNCATE_CASCADE_APPEND_ONLY_TABLES: tuple[str, ...] = (
    *APPEND_ONLY_PROTECTED_TABLES,
    *_PROJECT_TRUNCATE_CASCADE_CATALOG_TABLES,
)
APPEND_ONLY_ROW_TRIGGER_NAME = "trg_append_only_row_guard"
APPEND_ONLY_TRUNCATE_TRIGGER_NAME = "trg_append_only_truncate_guard"
_LEGACY_APPEND_ONLY_TRIGGER_NAMES: tuple[str, ...] = (
    APPEND_ONLY_ROW_TRIGGER_NAME,
    APPEND_ONLY_TRUNCATE_TRIGGER_NAME,
)
_TEST_ONLY_CLEANUP_TABLES: tuple[str, ...] = ("idempotency_keys",)

_DATABASE_REQUIRED_REASON = "DATABASE_URL not set - skipping database tests"

_DB_LANE_MARKERS: frozenset[str] = frozenset(
    {
        "db_api",
        "db_worker",
        "db_estimation_export",
        "db_lineage",
        "db_migration",
    }
)

_DB_LANE_BY_TEST_FILE_KEY: dict[str, str] = {
    "projects": "db_api",
    "files": "db_api",
    "idempotency": "db_api",
    "extraction_profiles": "db_api",
    "quantity_takeoff_api": "db_api",
    "estimate_read_api": "db_api",
    "export_create_api": "db_api",
    "changeset_api": "db_api",
    "revision_materialization_api": "db_api",
    "validation_report_api": "db_api",
    "revision_quantity_estimate_flow": "db_api",
    "smoke": "db_api",
    "jobs": "db_worker",
    "jobs_api": "db_worker",
    "jobs_worker_ingest": "db_worker",
    "jobs_worker_lifecycle": "db_worker",
    "jobs_worker_quantity": "db_worker",
    "jobs_worker_estimate": "db_worker",
    "jobs_worker_exports": "db_worker",
    "ingest_output_persistence": "db_worker",
    "csv_exports": "db_estimation_export",
    "revision_json_export": "db_estimation_export",
    "estimate_pdf_export": "db_estimation_export",
    "estimate_engine_persistence_payloads": "db_estimation_export",
    "estimate_job_input_contract": "db_estimation_export",
    "estimate_snapshot_item_persistence": "db_estimation_export",
    "estimate_version_persistence": "db_estimation_export",
    "estimation_catalog_api": "db_estimation_export",
    "estimation_catalog_persistence": "db_estimation_export",
    "estimation_catalog_resolver": "db_estimation_export",
    "export_job_input_contract": "db_estimation_export",
    "quantity_takeoff_persistence": "db_estimation_export",
    "append_only_lineage_tables": "db_lineage",
    "changeset_persistence": "db_lineage",
    "lineage_delete_restrictions": "db_lineage",
    "db": "db_migration",
    "jobs_base_revision_migration": "db_migration",
    "jobs_revision_scoped_contract_migration": "db_migration",
}

# Marker for tests that require a running database.
requires_database = pytest.mark.integration


def _item_test_file_key(item: pytest.Item) -> str:
    """Return normalized `tests/test_*.py` key for lane assignment."""

    item_path = getattr(item, "path", None)
    path = Path(str(item_path)) if item_path is not None else Path(str(item.fspath))
    return path.stem.removeprefix("test_")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Assign DB lanes for integration tests and skip when DB URL is missing."""

    has_database_url = bool(os.environ.get("DATABASE_URL"))
    skip_database = pytest.mark.skip(reason=_DATABASE_REQUIRED_REASON)
    lane_errors: list[str] = []

    for item in items:
        is_integration = "integration" in item.keywords
        if not is_integration:
            continue

        if "compose_smoke" not in item.keywords:
            file_key = _item_test_file_key(item)
            expected_lane = _DB_LANE_BY_TEST_FILE_KEY.get(file_key)
            if expected_lane is None:
                lane_errors.append(
                    f"{item.nodeid}: missing DB lane mapping for test file key '{file_key}'"
                )
            else:
                item.add_marker(getattr(pytest.mark, expected_lane))

            assigned_lanes = sorted(
                marker for marker in _DB_LANE_MARKERS if marker in item.keywords
            )
            if len(assigned_lanes) != 1:
                lane_errors.append(
                    f"{item.nodeid}: expected exactly one DB lane marker; found {assigned_lanes}"
                )

        if not has_database_url:
            item.add_marker(skip_database)

    if lane_errors:
        details = "\n - ".join(lane_errors)
        raise pytest.UsageError(
            "DB lane marker assignment failed for integration collection."
            f"\n - {details}\n"
            "Update _DB_LANE_BY_TEST_FILE_KEY in tests/conftest.py."
        )


@pytest_asyncio.fixture(autouse=True)
async def init_database_resources() -> AsyncGenerator[None, None]:
    """Initialize and close DB resources on each test loop."""
    if not os.environ.get("DATABASE_URL"):
        yield
        return

    import app.db.session as session_module

    session_module.engine, session_module.AsyncSessionLocal = session_module._init_db_resources()

    yield

    await session_module.close_db()


@pytest.fixture(autouse=True)
def isolate_upload_storage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    """Use a per-test temporary upload root and clean up only that root."""
    upload_root = (tmp_path / "uploads").resolve()
    _get_default_storage.cache_clear()
    monkeypatch.setattr(settings, "upload_storage_root", str(upload_root))
    _get_default_storage.cache_clear()

    yield

    _get_default_storage.cache_clear()
    if upload_root.exists():
        shutil.rmtree(upload_root)


@pytest.fixture
def app() -> FastAPI:
    """Provide the FastAPI application instance for testing."""
    return fastapi_app


async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
    """Override for get_db dependency that uses the test session."""
    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    session = session_maker()
    try:
        yield session
    finally:
        await session.close()


async def _append_only_trigger_exists(
    session: AsyncSession,
    *,
    table_name: str,
    trigger_name: str,
) -> bool:
    """Return whether a named append-only trigger exists on a table."""

    result = await session.execute(
        text(
            """
            SELECT 1
            FROM pg_trigger
            WHERE tgrelid = to_regclass(:table_name)
              AND tgname = :trigger_name
            """
        ),
        {"table_name": table_name, "trigger_name": trigger_name},
    )
    return result.scalar_one_or_none() is not None


async def _load_existing_append_only_triggers(
    session: AsyncSession,
) -> list[tuple[str, str]]:
    """Return append-only triggers currently installed in the test database."""

    existing_triggers: list[tuple[str, str]] = []
    for table_name in _PROJECT_TRUNCATE_CASCADE_APPEND_ONLY_TABLES:
        trigger_names = await session.scalars(
            text(
                """
                SELECT tgname
                FROM pg_trigger
                WHERE tgrelid = to_regclass(:table_name)
                  AND tgname LIKE '%append_only%'
                ORDER BY tgname
                """
            ),
            {"table_name": table_name},
        )
        existing_triggers.extend((table_name, trigger_name) for trigger_name in trigger_names)

    return existing_triggers


async def _set_append_only_triggers_enabled(
    session: AsyncSession,
    *,
    triggers: list[tuple[str, str]],
    enabled: bool,
) -> None:
    """Enable or disable named append-only triggers for test-only cleanup."""

    action = "ENABLE" if enabled else "DISABLE"
    for table_name, trigger_name in triggers:
        await session.execute(text(f'ALTER TABLE "{table_name}" {action} TRIGGER {trigger_name}'))


async def _assert_append_only_triggers_enabled(
    session: AsyncSession,
    *,
    triggers: list[tuple[str, str]],
) -> None:
    """Assert named append-only triggers are fully re-enabled."""

    for table_name, trigger_name in triggers:
        state = await session.execute(
            text(
                """
                SELECT tgenabled
                FROM pg_trigger
                WHERE tgrelid = to_regclass(:table_name)
                  AND tgname = :trigger_name
                """
            ),
            {"table_name": table_name, "trigger_name": trigger_name},
        )
        raw_state = state.scalar_one()
        normalized_state = raw_state.decode() if isinstance(raw_state, bytes) else raw_state
        assert normalized_state == "O"


async def _truncate_test_only_cleanup_tables(session: AsyncSession) -> None:
    """Truncate test-only standalone tables that do not cascade from projects."""

    table_names = ", ".join(f'"{table_name}"' for table_name in _TEST_ONLY_CLEANUP_TABLES)
    await session.execute(text(f"TRUNCATE TABLE {table_names}"))


async def truncate_projects_cascade_for_cleanup() -> None:
    """Hard-clean test data, preserving append-only trigger handling for projects."""

    if not os.environ.get("DATABASE_URL"):
        return

    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        existing_triggers: list[tuple[str, str]] = []
        triggers_disabled = False
        try:
            try:
                existing_triggers = await _load_existing_append_only_triggers(session)
                if existing_triggers:
                    await _set_append_only_triggers_enabled(
                        session,
                        triggers=existing_triggers,
                        enabled=False,
                    )
                    triggers_disabled = True
                await _truncate_test_only_cleanup_tables(session)
                await session.execute(text("TRUNCATE TABLE projects CASCADE"))
            finally:
                if triggers_disabled:
                    await _set_append_only_triggers_enabled(
                        session,
                        triggers=existing_triggers,
                        enabled=True,
                    )
                    await _assert_append_only_triggers_enabled(session, triggers=existing_triggers)
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@pytest_asyncio.fixture
async def async_client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an async HTTP client for testing with DB dependency override."""
    # Override the get_db dependency to use test database
    app.dependency_overrides[get_db] = _override_get_db

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    # Clean up dependency override
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def cleanup_projects() -> AsyncGenerator[None, None]:
    """Clean test DB state before and after each test when using DATABASE_URL."""
    if not os.environ.get("DATABASE_URL"):
        yield
        return

    await truncate_projects_cascade_for_cleanup()

    yield

    await truncate_projects_cascade_for_cleanup()


@pytest.fixture
def enqueued_job_ids(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Capture enqueue calls without requiring a live broker."""
    recorded_job_ids: list[str] = []

    def _fake_enqueue(job_id: object) -> None:
        recorded_job_ids.append(str(job_id))

    monkeypatch.setattr(files_api, "enqueue_ingest_job", _fake_enqueue)
    monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_enqueue)
    return recorded_job_ids
