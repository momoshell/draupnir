"""Local filesystem storage backend."""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from app.storage.base import (
    StorageChecksumMismatchError,
    StoragePayload,
    StoredObject,
    StoredObjectMeta,
)

_COPY_CHUNK_SIZE_BYTES = 1024 * 1024


class LocalFilesystemStorage:
    """Persist objects beneath a configured local root."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    async def put(
        self,
        key: str,
        data: StoragePayload,
        *,
        immutable: bool = False,
    ) -> StoredObjectMeta:
        """Persist an object without overwriting an existing key, regardless of immutability."""
        return await asyncio.to_thread(self._put_sync, key, data, immutable)

    async def get(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObject:
        """Load a stored object and optionally verify its checksum."""
        return await asyncio.to_thread(self._get_sync, key, expected_checksum_sha256)

    async def stat(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObjectMeta:
        """Return metadata for a stored object and optionally verify its checksum."""
        return await asyncio.to_thread(self._stat_sync, key, expected_checksum_sha256)

    async def exists(self, key: str) -> bool:
        """Check whether a stored object exists."""
        return await asyncio.to_thread(self._exists_sync, key)

    async def delete(self, key: str) -> None:
        """Delete a stored object if present."""
        await asyncio.to_thread(self._delete_sync, key)

    async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
        """Delete a just-written object before immutable upload lineage is committed."""
        await asyncio.to_thread(self._delete_failed_put_sync, key, storage_uri)

    async def presign(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in_seconds: int = 3600,
    ) -> str | None:
        """Stub presign support for local storage."""
        _ = (key, method, expires_in_seconds)
        return None

    def _put_sync(self, key: str, data: StoragePayload, immutable: bool) -> StoredObjectMeta:
        final_path = self._path_for_key(key)
        self._ensure_private_directory(final_path.parent, include_parents_until=self.root)
        temp_path = final_path.parent / f".{final_path.name}.{uuid.uuid4().hex}.tmp"

        final_path_created = False
        try:
            size_bytes, checksum_sha256 = self._write_temp_file(temp_path, data)
            temp_path.chmod(self._target_mode(immutable))
            os.link(temp_path, final_path)
            final_path_created = True
            self._fsync_directory(final_path.parent)
            return StoredObjectMeta(
                key=key,
                storage_uri=f"file://{final_path}",
                size_bytes=size_bytes,
                checksum_sha256=checksum_sha256,
            )
        except BaseException:
            if final_path_created:
                self._cleanup_uploaded_path(final_path)
            raise
        finally:
            self._cleanup_uploaded_path(temp_path)
            if final_path_created:
                self._fsync_directory(final_path.parent)

    def _get_sync(
        self,
        key: str,
        expected_checksum_sha256: str | None,
    ) -> StoredObject:
        path = self._path_for_key(key)
        body, checksum_sha256 = self._read_body_with_checksum(path)
        self._verify_checksum(key, expected_checksum_sha256, checksum_sha256)
        return StoredObject(
            meta=StoredObjectMeta(
                key=key,
                storage_uri=f"file://{path}",
                size_bytes=len(body),
                checksum_sha256=checksum_sha256,
            ),
            body=body,
        )

    def _stat_sync(
        self,
        key: str,
        expected_checksum_sha256: str | None,
    ) -> StoredObjectMeta:
        path = self._path_for_key(key)
        size_bytes, checksum_sha256 = self._checksum_path(path)
        self._verify_checksum(key, expected_checksum_sha256, checksum_sha256)
        return StoredObjectMeta(
            key=key,
            storage_uri=f"file://{path}",
            size_bytes=size_bytes,
            checksum_sha256=checksum_sha256,
        )

    def _delete_sync(self, key: str) -> None:
        path = self._path_for_key(key)
        if path.exists() and self._is_immutable_path(path):
            raise PermissionError(key)
        self._cleanup_uploaded_path(path)

    def _delete_failed_put_sync(self, key: str, storage_uri: str) -> None:
        path = self._path_for_key(key)
        expected_storage_uri = f"file://{path}"
        if storage_uri != expected_storage_uri:
            raise ValueError("Storage URI does not match key for failed-put cleanup.")
        self._cleanup_uploaded_path(path)

    def _exists_sync(self, key: str) -> bool:
        return self._path_for_key(key).exists()

    def _write_temp_file(self, temp_path: Path, data: StoragePayload) -> tuple[int, str]:
        with temp_path.open("xb") as stream:
            temp_path.chmod(0o600)
            if isinstance(data, bytes):
                stream.write(data)
                size_bytes = len(data)
                checksum_sha256 = hashlib.sha256(data).hexdigest()
            else:
                with Path(data).open("rb") as source_stream:
                    size_bytes, checksum_sha256 = self._copy_stream(source_stream, stream)
            stream.flush()
            os.fsync(stream.fileno())
        return size_bytes, checksum_sha256

    def _copy_stream(
        self,
        source_stream: BinaryIO,
        destination_stream: BinaryIO,
    ) -> tuple[int, str]:
        checksum_builder = hashlib.sha256()
        size_bytes = 0
        while chunk := source_stream.read(_COPY_CHUNK_SIZE_BYTES):
            destination_stream.write(chunk)
            checksum_builder.update(chunk)
            size_bytes += len(chunk)
        return size_bytes, checksum_builder.hexdigest()

    def _read_body_with_checksum(self, path: Path) -> tuple[bytes, str]:
        checksum_builder = hashlib.sha256()
        body = bytearray()
        with path.open("rb") as stream:
            while chunk := stream.read(_COPY_CHUNK_SIZE_BYTES):
                body.extend(chunk)
                checksum_builder.update(chunk)
        return bytes(body), checksum_builder.hexdigest()

    def _checksum_path(self, path: Path) -> tuple[int, str]:
        checksum_builder = hashlib.sha256()
        size_bytes = 0
        with path.open("rb") as stream:
            while chunk := stream.read(_COPY_CHUNK_SIZE_BYTES):
                checksum_builder.update(chunk)
                size_bytes += len(chunk)
        return size_bytes, checksum_builder.hexdigest()

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

    def _fsync_directory(self, path: Path) -> None:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _target_mode(self, immutable: bool) -> int:
        return 0o444 if immutable else 0o600

    def _path_for_key(self, key: str) -> Path:
        if key == "":
            raise ValueError("Storage key must not be empty.")

        key_path = PurePosixPath(key)
        if key_path.is_absolute():
            raise ValueError("Storage key must be relative.")
        if key_path == PurePosixPath("."):
            raise ValueError("Storage key must not resolve to the storage root.")
        if any(part == ".." for part in key_path.parts):
            raise ValueError("Storage key must not contain parent traversal segments.")

        path = self.root.joinpath(*key_path.parts).resolve()
        if path == self.root:
            raise ValueError("Storage key must not resolve to the storage root.")

        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("Storage key must resolve under the storage root.") from exc

        return path

    def _cleanup_uploaded_path(self, storage_path: Path) -> None:
        with suppress(OSError):
            storage_path.unlink(missing_ok=True)

        current = storage_path.parent
        while current != self.root and self.root in current.parents:
            with suppress(OSError):
                current.rmdir()
            current = current.parent

    def _is_immutable_path(self, path: Path) -> bool:
        return (path.stat().st_mode & 0o200) == 0

    def _ensure_private_directory(
        self,
        path: Path,
        *,
        include_parents_until: Path | None = None,
    ) -> None:
        path.mkdir(parents=True, exist_ok=True)
        targets = [path]
        if include_parents_until is not None:
            parent = path.parent
            while include_parents_until in parent.parents or parent == include_parents_until:
                targets.append(parent)
                if parent == include_parents_until:
                    break
                parent = parent.parent

        for target in targets:
            target.chmod(0o700)


LocalStorage = LocalFilesystemStorage
