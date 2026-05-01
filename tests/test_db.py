"""Tests for database connectivity and migrations."""

import asyncio
import os
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text

from app.core.config import settings
from app.db.session import close_db, get_engine, get_session_maker, init_db

# Marker for tests that require a running database
requires_database = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set - skipping database tests",
)


@pytest.fixture(autouse=True)
def setup_teardown() -> Generator[None, None, None]:
    """Reset database state before and after each test."""
    # Re-initialize database resources before each test
    import app.db.session as session_module

    session_module.engine, session_module.AsyncSessionLocal = session_module._init_db_resources()
    yield
    # Cleanup
    asyncio.run(close_db())


class TestDatabaseSession:
    """Test database session management."""

    def test_get_engine_returns_none_without_url(self) -> None:
        """Should return None when DATABASE_URL is not set."""
        import app.db.session as session_module

        # Test _init_db_resources directly with patched settings
        with patch.object(settings, "database_url", None):
            eng, session_maker = session_module._init_db_resources()
            assert eng is None
            assert session_maker is None

    @requires_database
    @pytest.mark.asyncio
    async def test_get_engine_creates_engine_with_url(self) -> None:
        """Should create engine when DATABASE_URL is set."""
        engine = get_engine()
        assert engine is not None

    @requires_database
    @pytest.mark.asyncio
    async def test_get_session_maker_returns_maker(self) -> None:
        """Should return session maker when engine is available."""
        maker = get_session_maker()
        assert maker is not None

    @requires_database
    @pytest.mark.asyncio
    async def test_init_db_connects_successfully(self) -> None:
        """Should verify database connectivity."""
        # Should not raise
        await init_db()

    @requires_database
    @pytest.mark.asyncio
    async def test_session_can_execute_query(self) -> None:
        """Should be able to execute queries through session."""
        maker = get_session_maker()
        assert maker is not None

        async with maker() as session:
            result = await session.execute(text("SELECT 1"))
            row = result.first()
            assert row is not None
            assert row[0] == 1

    @requires_database
    @pytest.mark.asyncio
    async def test_close_db_disposes_engine(self) -> None:
        """Should properly dispose of engine resources."""
        # Create engine
        engine = get_engine()
        maker = get_session_maker()
        assert engine is not None
        assert maker is not None

        # Close it
        await close_db()

        # Verify state is reset
        import app.db.session as session_module

        assert session_module.engine is None
        assert session_module.AsyncSessionLocal is None


class TestAlembicMigrations:
    """Test Alembic migration commands."""

    @requires_database
    @pytest.mark.asyncio
    async def test_alembic_upgrade_head(self) -> None:
        """Should successfully run alembic upgrade head."""
        # Run alembic upgrade in subprocess to avoid event loop issues
        result = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "head",
            cwd=str(Path(__file__).parent.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _stdout, stderr = await result.communicate()

        assert result.returncode == 0, f"Alembic upgrade failed: {stderr.decode()}"

    @requires_database
    @pytest.mark.asyncio
    async def test_alembic_downgrade_base(self) -> None:
        """Should successfully run alembic downgrade base."""
        # First upgrade
        upgrade_result = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "head",
            cwd=str(Path(__file__).parent.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _stdout, upgrade_stderr = await upgrade_result.communicate()
        assert upgrade_result.returncode == 0, f"Alembic upgrade failed: {upgrade_stderr.decode()}"

        # Then downgrade
        result = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "downgrade",
            "base",
            cwd=str(Path(__file__).parent.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _stdout, stderr = await result.communicate()

        assert result.returncode == 0, f"Alembic downgrade failed: {stderr.decode()}"

        # Restore schema for subsequent tests that require migrated tables.
        reupgrade_result = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "head",
            cwd=str(Path(__file__).parent.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _stdout, reupgrade_stderr = await reupgrade_result.communicate()
        assert reupgrade_result.returncode == 0, (
            f"Alembic re-upgrade failed after downgrade test: {reupgrade_stderr.decode()}"
        )

    @requires_database
    @pytest.mark.asyncio
    async def test_alembic_current(self) -> None:
        """Should show current migration state."""
        result = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "current",
            cwd=str(Path(__file__).parent.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _stdout, stderr = await result.communicate()

        # Current should succeed (may show no revisions if downgraded)
        assert result.returncode == 0, f"Alembic current failed: {stderr.decode()}"


class TestDatabaseDependency:
    """Test FastAPI database dependency."""

    @requires_database
    @pytest.mark.asyncio
    async def test_get_db_yields_session(self) -> None:
        """Should yield a valid async session."""
        from app.db.session import get_db

        gen = get_db()
        session = await gen.__anext__()

        # Session should be usable
        result = await session.execute(text("SELECT 1"))
        row = result.first()
        assert row is not None
        assert row[0] == 1

        # Cleanup
        await gen.aclose()

    @requires_database
    @pytest.mark.asyncio
    async def test_get_db_closes_session_on_error(self) -> None:
        """Should close session properly when generator is closed."""
        from app.db.session import get_db

        gen = get_db()
        session = await gen.__anext__()

        # Session should be an AsyncSession
        assert session is not None

        # Cleanup should work without errors
        await gen.aclose()
