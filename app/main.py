"""FastAPI application factory."""

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.v1 import files_router, health_router, jobs_router, project_router, system_router
from app.core.config import settings
from app.core.exceptions import custom_http_exception_handler, request_validation_exception_handler
from app.core.logging import configure_logging, get_logger
from app.core.middleware import ContentLengthLimitMiddleware, RequestIdMiddleware

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

    app.add_exception_handler(StarletteHTTPException, custom_http_exception_handler)
    app.add_exception_handler(RequestValidationError, request_validation_exception_handler)

    # Add middleware
    app.add_middleware(ContentLengthLimitMiddleware)
    app.add_middleware(RequestIdMiddleware)

    app.include_router(health_router, prefix=settings.api_prefix)
    app.include_router(system_router, prefix=settings.api_prefix)
    app.include_router(project_router, prefix=f"{settings.api_prefix}/projects")
    app.include_router(files_router, prefix=settings.api_prefix)
    app.include_router(jobs_router, prefix=settings.api_prefix)

    logger.info("app_started", version=settings.app_version)

    return app


app = create_app()
