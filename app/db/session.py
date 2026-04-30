"""Database session management and FastAPI dependency injection."""

from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Engine and session maker are created at module import time
# If database_url is not set, these will be None
engine: AsyncEngine | None = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None


def _init_db_resources() -> tuple[AsyncEngine | None, async_sessionmaker[AsyncSession] | None]:
    """Initialize database engine and session maker.

    Returns:
        Tuple of (engine, session_maker), both None if DATABASE_URL is not set.
    """
    if not settings.database_url:
        logger.warning("database_url_not_configured", msg="DATABASE_URL is not set")
        return None, None

    engine = create_async_engine(
        settings.database_url,
        echo=settings.debug,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )

    logger.info(
        "database_engine_created",
        url=settings.database_url.split("@")[-1] if "@" in settings.database_url else "configured",
    )

    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    return engine, session_maker


# Initialize at module import time
engine, AsyncSessionLocal = _init_db_resources()


def get_engine() -> AsyncEngine | None:
    """Get the async database engine.

    Returns:
        AsyncEngine: The SQLAlchemy async engine, or None if DATABASE_URL is not set.
    """
    return engine


def get_session_maker() -> async_sessionmaker[AsyncSession] | None:
    """Get the async session maker.

    Returns:
        async_sessionmaker: The session maker, or None if engine is not available.
    """
    return AsyncSessionLocal


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database sessions.

    Yields:
        AsyncSession: An async database session.

    Example:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db)):
            ...
    """
    session_maker = get_session_maker()

    if session_maker is None:
        logger.warning("database_session_unavailable", msg="Cannot create database session")
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    session = session_maker()

    try:
        yield session
    finally:
        await session.close()


async def init_db() -> None:
    """Initialize database connection and verify connectivity.

    Raises:
        RuntimeError: If database connection fails.
    """
    engine = get_engine()

    if engine is None:
        logger.warning("database_initialization_skipped", msg="DATABASE_URL not configured")
        return

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("database_connection_verified")
    except Exception as e:
        logger.error("database_connection_failed", error=str(e))
        raise RuntimeError(f"Failed to connect to database: {e}") from e


async def close_db() -> None:
    """Close the database engine and cleanup resources."""
    global engine, AsyncSessionLocal

    if engine is not None:
        await engine.dispose()
        logger.info("database_engine_disposed")
        engine = None
        AsyncSessionLocal = None
