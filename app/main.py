"""FastAPI application factory."""

from typing import Any

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.v1 import (
    estimation_router,
    files_router,
    health_router,
    jobs_router,
    project_router,
    revisions_router,
    system_router,
)
from app.core.config import settings
from app.core.exceptions import (
    APIErrorResponse,
    custom_http_exception_handler,
    request_validation_exception_handler,
)
from app.core.logging import configure_logging, get_logger
from app.core.middleware import ContentLengthLimitMiddleware, RequestIdMiddleware

logger = get_logger(__name__)

_API_DESCRIPTION = (
    "Draupnir extracts canonical, deterministic feature data from CAD/PDF drawings "
    "(entities, layers, devices, rooms, quantities, estimates) and exposes it for "
    "downstream systems. Responses report what was extracted plus an honest "
    "extraction-coverage signal; the API does not make opaque trust judgements. "
    "Mutations are asynchronous (jobs) and idempotent; lists are cursor-paginated; "
    "errors use a standard `{error: {code, message, details}}` envelope."
)

# OpenAPI tag groups (display order + descriptions); keep in sync with the tags
# applied at the router includes (main.py + app/api/v1/revisions.py).
_OPENAPI_TAGS = [
    {"name": "System", "description": "Health and runtime-capability discovery."},
    {"name": "Projects", "description": "Projects and project-scoped file upload."},
    {"name": "Files", "description": "Source files and their revisions/artifacts."},
    {"name": "Jobs", "description": "Async job status, events, retry and cancellation."},
    {"name": "Revisions", "description": "Drawing revisions (lineage metadata)."},
    {"name": "Adapter Outputs", "description": "Raw adapter run outputs per revision."},
    {"name": "Entities", "description": "Materialized canonical entities, layers and blocks."},
    {"name": "Devices", "description": "Tagged devices and the legend-anchored device schedule."},
    {"name": "Rooms", "description": "Interpreted room polygons and containment."},
    {
        "name": "Service Takeoff",
        "description": "Routed-service quantity takeoff (length per service+size, per room).",
    },
    {"name": "Validation", "description": "Validation reports and extraction coverage."},
    {"name": "Changesets", "description": "Canonical edit changesets: create, validate, apply."},
    {"name": "Quantities", "description": "Quantity takeoffs and their items."},
    {"name": "Estimates", "description": "Estimate versions, items and snapshot entries."},
    {"name": "Exports", "description": "Generated DXF / JSON / CSV / PDF exports."},
    {"name": "Estimation Catalog", "description": "Rate / material / formula catalog inputs."},
    {"name": "Artifacts", "description": "Generated artifact metadata and downloads."},
]


# Standardized error responses documented on every data endpoint, so consumers and
# generated clients (incl. the MCP tool layer) see the `{error: {code, message, details}}`
# envelope + the ErrorCode taxonomy. FastAPI auto-documents 422 for request validation.
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": APIErrorResponse, "description": "Invalid request (bad input or cursor)."},
    404: {"model": APIErrorResponse, "description": "Resource not found."},
    409: {
        "model": APIErrorResponse,
        "description": "Conflict (idempotency key reuse or concurrent revision change).",
    },
}


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
        description=_API_DESCRIPTION,
        openapi_tags=_OPENAPI_TAGS,
        contact={"name": "Draupnir", "url": "https://github.com/momoshell/draupnir"},
        license_info={"name": "Proprietary"},
        # Stable, clean operationIds derived from the (unique) route-handler names,
        # so generated SDKs / the MCP tool layer get readable, drift-resistant names.
        generate_unique_id_function=lambda route: route.name,
    )

    app.add_exception_handler(StarletteHTTPException, custom_http_exception_handler)
    app.add_exception_handler(RequestValidationError, request_validation_exception_handler)

    # Add middleware
    app.add_middleware(ContentLengthLimitMiddleware)
    app.add_middleware(RequestIdMiddleware)

    app.include_router(health_router, prefix=settings.api_prefix, tags=["System"])
    app.include_router(system_router, prefix=settings.api_prefix, tags=["System"])
    app.include_router(
        project_router,
        prefix=f"{settings.api_prefix}/projects",
        tags=["Projects"],
        responses=_ERROR_RESPONSES,
    )
    app.include_router(
        files_router, prefix=settings.api_prefix, tags=["Files"], responses=_ERROR_RESPONSES
    )
    app.include_router(
        jobs_router, prefix=settings.api_prefix, tags=["Jobs"], responses=_ERROR_RESPONSES
    )
    # revisions_router applies its own per-domain tags at the sub-router level.
    app.include_router(revisions_router, prefix=settings.api_prefix, responses=_ERROR_RESPONSES)
    app.include_router(
        estimation_router,
        prefix=settings.api_prefix,
        tags=["Estimation Catalog"],
        responses=_ERROR_RESPONSES,
    )

    logger.info("app_started", version=settings.app_version)

    return app


app = create_app()
