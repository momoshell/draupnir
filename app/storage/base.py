"""Storage protocol and shared storage data models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

StoragePayload = bytes | Path


@dataclass(frozen=True, slots=True)
class StoredObjectMeta:
    """Metadata for a stored object."""

    key: str
    storage_uri: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Stored object bytes and metadata."""

    meta: StoredObjectMeta
    body: bytes


class Storage(Protocol):
    """Abstract storage backend contract."""

    async def put(
        self,
        key: str,
        data: StoragePayload,
        *,
        immutable: bool = False,
    ) -> StoredObjectMeta:
        """Persist an object at a server-derived key without overwriting existing keys."""

    async def get(self, key: str) -> StoredObject:
        """Read an object by key."""

    async def stat(self, key: str) -> StoredObjectMeta:
        """Read object metadata without loading full bytes."""

    async def exists(self, key: str) -> bool:
        """Check whether an object exists."""

    async def delete(self, key: str) -> None:
        """Delete an object by key."""

    async def presign(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in_seconds: int = 3600,
    ) -> str | None:
        """Return a presigned URL when supported."""
