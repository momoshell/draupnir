"""API v1 routers."""

from app.api.v1.files import files_router
from app.api.v1.health import health_router
from app.api.v1.projects import project_router

__all__ = ["files_router", "health_router", "project_router"]
