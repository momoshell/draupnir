"""Shared fixtures for integration tests."""

import os
from collections.abc import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.main import app as fastapi_app

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
    """Truncate projects table after each test to ensure clean state."""
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
