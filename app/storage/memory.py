"""In-memory storage backend for tests and local seams."""

from __future__ import annotations

import hashlib
from pathlib import Path

from app.storage.base import (
    StorageChecksumMismatchError,
    StorageHealthReport,
    StoragePayload,
    StoredObject,
    StoredObjectMeta,
)

_MEMORY_READ_CHUNK_SIZE_BYTES = 1024 * 1024


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
        body = data if isinstance(data, bytes) else self._read_path_bytes(Path(data))
        checksum_sha256 = hashlib.sha256(body).hexdigest()
        existing = self._objects.get(key)
        if existing is not None:
            raise FileExistsError(key)

        self._objects[key] = (body, immutable)
        return StoredObjectMeta(
            key=key,
            storage_uri=f"memory://{key}",
            size_bytes=len(body),
            checksum_sha256=checksum_sha256,
        )

    async def get(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObject:
        """Return object bytes and metadata."""
        body, _ = self._objects[key]
        checksum_sha256 = hashlib.sha256(body).hexdigest()
        self._verify_checksum(key, expected_checksum_sha256, checksum_sha256)
        return StoredObject(
            meta=StoredObjectMeta(
                key=key,
                storage_uri=f"memory://{key}",
                size_bytes=len(body),
                checksum_sha256=checksum_sha256,
            ),
            body=body,
        )

    async def stat(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObjectMeta:
        """Return metadata for a stored object."""
        body, _ = self._objects[key]
        checksum_sha256 = hashlib.sha256(body).hexdigest()
        self._verify_checksum(key, expected_checksum_sha256, checksum_sha256)
        return StoredObjectMeta(
            key=key,
            storage_uri=f"memory://{key}",
            size_bytes=len(body),
            checksum_sha256=checksum_sha256,
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

    async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
        """Delete a just-written object before immutable upload lineage is committed."""
        expected_storage_uri = f"memory://{key}"
        if storage_uri != expected_storage_uri:
            raise ValueError("Storage URI does not match key for failed-put cleanup.")

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

    async def healthcheck(self) -> StorageHealthReport:
        """Report that in-memory storage is available."""
        return StorageHealthReport(ok=True, details={"backend": "memory", "reachable": True})

    def _read_path_bytes(self, path: Path) -> bytes:
        body = bytearray()
        with path.open("rb") as stream:
            while chunk := stream.read(_MEMORY_READ_CHUNK_SIZE_BYTES):
                body.extend(chunk)
        return bytes(body)

    def _verify_checksum(
        self,
        key: str,
        expected_checksum_sha256: str | None,
        actual_checksum_sha256: str,
    ) -> None:
        if (
            expected_checksum_sha256 is not None
            and actual_checksum_sha256 != expected_checksum_sha256
        ):
            raise StorageChecksumMismatchError(
                key=key,
                expected=expected_checksum_sha256,
                actual=actual_checksum_sha256,
            )
