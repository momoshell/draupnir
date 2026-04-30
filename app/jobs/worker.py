"""Celery worker application."""

from celery import Celery  # type: ignore[import-untyped]

from app.core.config import settings

celery_app = Celery(
    "draupnir",
    broker=settings.broker_url,
    backend="redis://localhost:6379/0",  # placeholder for now
)

# Auto-discover tasks from the jobs module
celery_app.autodiscover_tasks(["app.jobs"], force=True)


# Example task (remove once real tasks exist)
@celery_app.task  # type: ignore[untyped-decorator]
def health_check_task() -> str:
    """Simple health check task for testing worker connectivity."""
    return "ok"
