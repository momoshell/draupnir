"""FastAPI application factory."""

from fastapi import FastAPI

from app.api.v1 import health_router
from app.core.config import settings


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
    )

    app.include_router(health_router, prefix=settings.api_prefix)

    return app


app = create_app()
