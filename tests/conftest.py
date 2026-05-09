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
    """Truncate projects table after each test to ensure clean DB state."""
    if not os.environ.get("DATABASE_URL"):
        yield
        return

    import app.db.session as session_module

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    yield

    async with session_maker() as session:
        await session.execute(text("TRUNCATE TABLE projects CASCADE"))
        await session.commit()


@pytest.fixture
def enqueued_job_ids(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Capture enqueue calls without requiring a live broker."""
    recorded_job_ids: list[str] = []

    def _fake_enqueue(job_id: object) -> None:
        recorded_job_ids.append(str(job_id))

    monkeypatch.setattr(files_api, "enqueue_ingest_job", _fake_enqueue)
    monkeypatch.setattr(worker_module, "enqueue_ingest_job", _fake_enqueue)
    return recorded_job_ids
