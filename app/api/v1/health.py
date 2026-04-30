"""Health check endpoint."""

from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: Literal["ok"]
    version: str | None = None


health_router = APIRouter()


@health_router.get("/health", response_model=HealthResponse)
async def get_health() -> HealthResponse:
    """Return service health status and optionally version."""
    version = settings.app_version if settings.expose_version_in_health else None
    logger.info("health_check_requested")
    return HealthResponse(status="ok", version=version)
