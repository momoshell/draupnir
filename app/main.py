"""FastAPI application factory."""

from fastapi import FastAPI

from app.api.v1 import health_router
from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.core.middleware import RequestIdMiddleware

logger = get_logger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    # Configure structured logging first
    configure_logging(
        service_name=settings.service_name,
        log_level=settings.log_level,
    )

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
    )

    # Add middleware
    app.add_middleware(RequestIdMiddleware)

    app.include_router(health_router, prefix=settings.api_prefix)

    logger.info("app_started", version=settings.app_version)

    return app


app = create_app()
