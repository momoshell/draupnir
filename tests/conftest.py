"""Shared fixtures for integration tests."""

import os
import shutil
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import app.api.v1.files as files_api
import app.jobs.worker as worker_module
from app.core.config import settings
from app.db.session import get_db
from app.main import app as fastapi_app
from app.storage.dependencies import _get_default_storage

APPEND_ONLY_PROTECTED_TABLES: tuple[str, ...] = (
    "files",
    "extraction_profiles",
    "adapter_run_outputs",
    "drawing_revisions",
    "validation_reports",
    "generated_artifacts",
    "job_events",
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

# Marker for tests that require a running database
requires_database = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set - skipping database tests",
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
        await session.execute(
            text(f'ALTER TABLE "{table_name}" {action} TRIGGER {trigger_name}')
        )


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
