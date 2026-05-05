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
    checksum_sha256: str = ""


class StorageChecksumMismatchError(ValueError):
    """Raised when stored bytes do not match an expected checksum."""

    def __init__(self, *, key: str, expected: str, actual: str) -> None:
        self.key = key
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Checksum mismatch for storage key '{key}': expected {expected}, got {actual}."
        )


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

    async def get(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObject:
        """Read an object by key and optionally verify its checksum."""

    async def stat(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObjectMeta:
        """Read object metadata and optionally verify its checksum."""

    async def exists(self, key: str) -> bool:
        """Check whether an object exists."""

    async def delete(self, key: str) -> None:
        """Delete an object by key."""

    async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
        """Delete a just-written object before database lineage is committed."""

    async def presign(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in_seconds: int = 3600,
    ) -> str | None:
        """Return a presigned URL when supported."""
