"""API v1 routers."""

from app.api.v1.files import files_router
from app.api.v1.health import health_router
from app.api.v1.jobs import jobs_router
from app.api.v1.projects import project_router
from app.api.v1.system import system_router

__all__ = ["files_router", "health_router", "jobs_router", "project_router", "system_router"]
