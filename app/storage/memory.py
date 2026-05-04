"""In-memory storage backend for tests and local seams."""

from __future__ import annotations

from pathlib import Path

from app.storage.base import StoragePayload, StoredObject, StoredObjectMeta


class MemoryStorage:
    """Simple in-memory storage backend."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, bool]] = {}

    async def put(
        self,
        key: str,
        data: StoragePayload,
        *,
        immutable: bool = False,
    ) -> StoredObjectMeta:
        """Store bytes at a key without overwriting existing keys."""
        body = data if isinstance(data, bytes) else Path(data).read_bytes()
        existing = self._objects.get(key)
        if existing is not None:
            raise FileExistsError(key)

        self._objects[key] = (body, immutable)
        return StoredObjectMeta(
            key=key,
            storage_uri=f"memory://{key}",
            size_bytes=len(body),
        )

    async def get(self, key: str) -> StoredObject:
        """Return object bytes and metadata."""
        body, _ = self._objects[key]
        return StoredObject(
            meta=StoredObjectMeta(
                key=key,
                storage_uri=f"memory://{key}",
                size_bytes=len(body),
            ),
            body=body,
        )

    async def stat(self, key: str) -> StoredObjectMeta:
        """Return metadata for a stored object."""
        body, _ = self._objects[key]
        return StoredObjectMeta(
            key=key,
            storage_uri=f"memory://{key}",
            size_bytes=len(body),
        )

    async def exists(self, key: str) -> bool:
        """Check whether a key exists."""
        return key in self._objects

    async def delete(self, key: str) -> None:
        """Delete a key if present."""
        existing = self._objects.get(key)
        if existing is None:
            return
        if existing[1]:
            raise PermissionError(key)

        self._objects.pop(key, None)

    async def presign(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in_seconds: int = 3600,
    ) -> str | None:
        """Stub presign support for interface parity."""
        _ = (key, method, expires_in_seconds)
        return None
