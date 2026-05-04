"""Storage dependency providers."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from app.core.config import settings
from app.storage.base import Storage
from app.storage.local import LocalFilesystemStorage


@lru_cache(maxsize=1)
def _get_default_storage() -> Storage:
    """Return the default local storage backend."""
    return LocalFilesystemStorage(Path(settings.storage_local_root).resolve())


def get_storage() -> Storage:
    """FastAPI dependency for storage backends."""
    return _get_default_storage()
