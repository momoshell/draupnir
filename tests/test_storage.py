"""Unit tests for storage backends."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import BinaryIO, Literal, cast

import pytest

from app.storage import (
    LocalFilesystemStorage,
    LocalStorage,
    MemoryStorage,
)
from app.storage.base import (
    StorageChecksumMismatchError,
    StorageReadError,
    StorageWriteError,
)


@pytest.mark.asyncio
async def test_memory_storage_round_trip_with_bytes() -> None:
    """Memory storage should persist and return bytes plus metadata."""
    storage = MemoryStorage()

    meta = await storage.put("originals/file-1/checksum-1", b"payload", immutable=True)
    stored = await storage.get("originals/file-1/checksum-1")

    assert meta.key == "originals/file-1/checksum-1"
    assert meta.storage_uri == "memory://originals/file-1/checksum-1"
    assert meta.size_bytes == 7
    assert meta.checksum_sha256 == hashlib.sha256(b"payload").hexdigest()
    assert stored.meta == meta
    assert stored.body == b"payload"


@pytest.mark.asyncio
async def test_memory_storage_round_trip_with_path(tmp_path: Path) -> None:
    """Memory storage should accept staged file paths."""
    storage = MemoryStorage()
    staged_path = tmp_path / "upload.part"
    staged_path.write_bytes(b"from-path")

    meta = await storage.put("originals/file-2/checksum-2", staged_path, immutable=True)

    assert meta.size_bytes == len(b"from-path")
    assert meta.checksum_sha256 == hashlib.sha256(b"from-path").hexdigest()
    assert await storage.exists("originals/file-2/checksum-2") is True
    assert (await storage.get("originals/file-2/checksum-2")).body == b"from-path"


@pytest.mark.asyncio
async def test_memory_storage_stat_and_presign_stub() -> None:
    """Memory storage should support metadata checks and presign parity."""
    storage = MemoryStorage()
    key = "originals/file-3/checksum-3"
    await storage.put(key, b"abc", immutable=True)

    stat = await storage.stat(key)

    assert stat.size_bytes == 3
    assert stat.checksum_sha256 == hashlib.sha256(b"abc").hexdigest()
    assert await storage.presign(key) is None


@pytest.mark.asyncio
async def test_memory_storage_copy_to_path_round_trip(tmp_path: Path) -> None:
    """Memory storage should stream a stored object into a destination path."""
    storage = MemoryStorage()
    key = "originals/file-copy/checksum-copy"
    payload = b"copy-payload"
    await storage.put(key, payload, immutable=True)
    destination = tmp_path / "staged.bin"

    meta = await storage.copy_to_path(
        key,
        destination,
        expected_checksum_sha256=hashlib.sha256(payload).hexdigest(),
    )

    assert destination.read_bytes() == payload
    assert meta.key == key
    assert meta.storage_uri == f"memory://{key}"
    assert meta.size_bytes == len(payload)
    assert meta.checksum_sha256 == hashlib.sha256(payload).hexdigest()


@pytest.mark.asyncio
async def test_memory_storage_healthcheck_reports_backend_details() -> None:
    """Memory storage should expose health through the shared abstraction."""
    storage = MemoryStorage()

    report = await storage.healthcheck()

    assert report.ok is True
    assert report.details == {"backend": "memory", "reachable": True}


@pytest.mark.asyncio
async def test_memory_storage_rejects_overwrite_for_mutable_keys() -> None:
    """Memory storage should refuse overwrite operations for mutable keys."""
    storage = MemoryStorage()
    key = "scratch/file-3"
    original_payload = b"abc"
    await storage.put(key, original_payload, immutable=False)

    with pytest.raises(FileExistsError):
        await storage.put(key, b"def", immutable=False)

    assert await storage.exists(key) is True
    assert (await storage.get(key)).body == original_payload


@pytest.mark.asyncio
async def test_memory_storage_rejects_overwrite_and_delete_for_immutable_keys() -> None:
    """Memory storage should refuse immutable overwrite/delete operations."""
    storage = MemoryStorage()
    key = "originals/file-3/checksum-3"
    original_payload = b"abc"
    await storage.put(key, original_payload, immutable=True)

    with pytest.raises(FileExistsError):
        await storage.put(key, b"def", immutable=True)

    with pytest.raises(PermissionError):
        await storage.delete(key)

    assert await storage.exists(key) is True
    assert (await storage.get(key)).body == original_payload


@pytest.mark.asyncio
async def test_memory_storage_allows_failed_put_cleanup_for_immutable_keys() -> None:
    """Memory storage should allow pre-commit cleanup for immutable writes."""
    storage = MemoryStorage()
    key = "originals/file-4/checksum-4"
    meta = await storage.put(key, b"abc", immutable=True)

    await storage.delete_failed_put(key, storage_uri=meta.storage_uri)

    assert await storage.exists(key) is False


@pytest.mark.asyncio
async def test_memory_storage_allows_delete_for_mutable_keys() -> None:
    """Memory storage should allow deleting mutable objects."""
    storage = MemoryStorage()
    key = "scratch/file-3"
    await storage.put(key, b"abc", immutable=False)

    await storage.delete(key)

    assert await storage.exists(key) is False


@pytest.mark.asyncio
async def test_local_storage_round_trip_and_cleanup(tmp_path: Path) -> None:
    """Local storage should persist server-derived keys and support cleanup."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "originals/file-4/checksum-4"

    meta = await storage.put(key, b"payload", immutable=True)
    stored = await storage.get(key)

    assert meta.key == key
    assert meta.storage_uri == f"file://{(tmp_path / key).resolve()}"
    assert meta.size_bytes == 7
    assert meta.checksum_sha256 == hashlib.sha256(b"payload").hexdigest()
    assert stored.meta == meta
    assert stored.body == b"payload"
    assert await storage.exists(key) is True
    assert await storage.presign(key) is None
    assert ((tmp_path / key).stat().st_mode & 0o777) == 0o444
    assert ((tmp_path / "originals").stat().st_mode & 0o777) == 0o700
    assert (tmp_path.stat().st_mode & 0o777) == 0o700

    with pytest.raises(PermissionError):
        await storage.delete(key)

    assert await storage.exists(key) is True
    assert list((tmp_path / "originals" / "file-4").glob("*.tmp")) == []


@pytest.mark.asyncio
async def test_local_storage_copy_to_path_round_trip(tmp_path: Path) -> None:
    """Local storage should stream a stored object into a destination path."""
    storage = LocalFilesystemStorage(tmp_path / "storage")
    key = "originals/file-copy/checksum-copy"
    payload = b"copy-payload"
    meta = await storage.put(key, payload, immutable=True)
    destination = tmp_path / "staging" / "staged.bin"
    destination.parent.mkdir()

    copied = await storage.copy_to_path(
        key,
        destination,
        expected_checksum_sha256=meta.checksum_sha256,
    )

    assert destination.read_bytes() == payload
    assert copied == meta


@pytest.mark.asyncio
async def test_local_storage_copy_to_path_classifies_source_read_oserror_and_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local storage should classify source read failures and remove partial outputs."""
    storage = LocalFilesystemStorage(tmp_path / "storage")
    key = "originals/file-copy/checksum-copy"
    payload = b"copy-payload"
    await storage.put(key, payload, immutable=True)
    destination = tmp_path / "staging" / "staged.bin"
    destination.parent.mkdir()

    class _BrokenSourceStream:
        def __enter__(self) -> _BrokenSourceStream:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> Literal[False]:
            return False

        def read(self, _size: int) -> bytes:
            raise OSError("source read failed")

        def close(self) -> None:
            return None

    def _open_copy_source(_source_path: Path, _key: str) -> BinaryIO:
        return cast(BinaryIO, _BrokenSourceStream())

    monkeypatch.setattr(storage, "_open_copy_source", _open_copy_source)

    with pytest.raises(StorageReadError):
        await storage.copy_to_path(key, destination)

    assert destination.exists() is False


@pytest.mark.asyncio
async def test_local_storage_copy_to_path_classifies_destination_write_oserror_and_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local storage should classify destination write failures and remove partial outputs."""
    storage = LocalFilesystemStorage(tmp_path / "storage")
    key = "originals/file-copy/checksum-copy"
    payload = b"copy-payload"
    await storage.put(key, payload, immutable=True)
    destination = tmp_path / "staging" / "staged.bin"
    destination.parent.mkdir()

    class _BrokenDestinationStream:
        def __init__(self, stream: BinaryIO) -> None:
            self._stream = stream

        def __enter__(self) -> _BrokenDestinationStream:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> Literal[False]:
            self._stream.close()
            return False

        def write(self, _chunk: bytes) -> int:
            raise OSError("destination write failed")

        def flush(self) -> None:
            self._stream.flush()

        def fileno(self) -> int:
            return self._stream.fileno()

        def close(self) -> None:
            self._stream.close()

    def _open_copy_destination(_destination: Path, _key: str) -> BinaryIO:
        return cast(BinaryIO, _BrokenDestinationStream(destination.open("xb")))

    monkeypatch.setattr(storage, "_open_copy_destination", _open_copy_destination)

    with pytest.raises(StorageWriteError):
        await storage.copy_to_path(key, destination)

    assert destination.exists() is False


@pytest.mark.asyncio
async def test_local_storage_healthcheck_reports_root_details_without_creating_root(
    tmp_path: Path,
) -> None:
    """Local storage health should describe the configured root without side effects."""
    root = tmp_path / "storage-root"
    storage = LocalFilesystemStorage(root)

    report = await storage.healthcheck()

    assert report.ok is True
    assert report.details == {
        "backend": "local_filesystem",
        "reachable": True,
        "root_configured": True,
        "root_exists": False,
    }
    assert "root" not in report.details
    assert "nearest_existing_ancestor" not in report.details
    assert root.exists() is False


@pytest.mark.asyncio
async def test_local_storage_allows_failed_put_cleanup_for_immutable_keys(tmp_path: Path) -> None:
    """Local storage should allow pre-commit cleanup for immutable writes."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "originals/file-cleanup/checksum-cleanup"
    meta = await storage.put(key, b"payload", immutable=True)

    await storage.delete_failed_put(key, storage_uri=meta.storage_uri)

    assert await storage.exists(key) is False
    assert not (tmp_path / "originals").exists()


@pytest.mark.asyncio
async def test_local_storage_rejects_overwrite_for_immutable_keys(tmp_path: Path) -> None:
    """Local storage should refuse replacing an immutable object."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "originals/file-5/checksum-5"
    original_payload = b"payload"
    await storage.put(key, original_payload, immutable=True)

    with pytest.raises(FileExistsError):
        await storage.put(key, b"replacement", immutable=True)

    assert await storage.exists(key) is True
    assert (await storage.get(key)).body == original_payload


@pytest.mark.asyncio
async def test_local_storage_rejects_overwrite_for_mutable_keys(tmp_path: Path) -> None:
    """Local storage should refuse replacing a mutable object."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "scratch/file-7"
    original_payload = b"payload"
    await storage.put(key, original_payload, immutable=False)

    with pytest.raises(FileExistsError):
        await storage.put(key, b"replacement", immutable=False)

    assert await storage.exists(key) is True
    assert (await storage.get(key)).body == original_payload


@pytest.mark.asyncio
async def test_local_storage_allows_delete_for_mutable_keys(tmp_path: Path) -> None:
    """Local storage should allow deleting mutable objects."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "scratch/file-6"
    await storage.put(key, b"payload", immutable=False)

    assert ((tmp_path / key).stat().st_mode & 0o777) == 0o600

    await storage.delete(key)

    assert await storage.exists(key) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key",
    [
        "",
        ".",
        "/absolute/path",
        "../escape",
        "originals/../escape",
        "originals/..",
    ],
)
async def test_local_storage_rejects_unsafe_keys_before_path_resolution(
    tmp_path: Path,
    key: str,
) -> None:
    """Local storage should reject unsafe keys without mutating the storage root."""
    storage = LocalFilesystemStorage(tmp_path)
    root_mode = tmp_path.stat().st_mode & 0o777

    with pytest.raises(ValueError):
        await storage.put(key, b"payload", immutable=True)

    with pytest.raises(ValueError):
        await storage.exists(key)

    with pytest.raises(ValueError):
        await storage.delete(key)

    assert list(tmp_path.iterdir()) == []
    assert (tmp_path.stat().st_mode & 0o777) == root_mode


def test_local_storage_alias_remains_available() -> None:
    """Legacy LocalStorage import should resolve to the canonical class."""
    assert LocalStorage is LocalFilesystemStorage


@pytest.mark.asyncio
async def test_local_storage_artifact_keys_are_immutable_per_key(tmp_path: Path) -> None:
    """Artifact storage should refuse overwriting the same storage key."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "artifacts/artifact-1/report.pdf"
    payload = b"artifact-bytes"

    await storage.put(key, payload, immutable=True)

    with pytest.raises(FileExistsError):
        await storage.put(key, payload, immutable=True)

    assert (await storage.get(key)).body == payload


@pytest.mark.asyncio
async def test_local_storage_allows_same_artifact_name_under_new_artifact_id(
    tmp_path: Path,
) -> None:
    """Artifact storage should allow identical payloads under different artifact ids."""
    storage = LocalFilesystemStorage(tmp_path)
    first_key = "artifacts/artifact-1/report.pdf"
    second_key = "artifacts/artifact-2/report.pdf"
    payload = b"artifact-bytes"

    first = await storage.put(first_key, payload, immutable=True)
    second = await storage.put(second_key, payload, immutable=True)

    assert first.key == first_key
    assert second.key == second_key
    assert (await storage.get(first_key)).body == payload
    assert (await storage.get(second_key)).body == payload


@pytest.mark.asyncio
async def test_local_storage_put_from_path_returns_checksum_metadata(tmp_path: Path) -> None:
    """Local storage should hash staged path payloads while copying."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "originals/file-8/checksum-8"
    staged_path = tmp_path / "upload.part"
    payload = b"from-path"
    staged_path.write_bytes(payload)

    meta = await storage.put(key, staged_path, immutable=True)
    stat = await storage.stat(key, expected_checksum_sha256=hashlib.sha256(payload).hexdigest())

    assert meta.size_bytes == len(payload)
    assert meta.checksum_sha256 == hashlib.sha256(payload).hexdigest()
    assert stat == meta


@pytest.mark.asyncio
async def test_local_storage_detects_checksum_tampering_on_get_and_stat(tmp_path: Path) -> None:
    """Local storage should detect checksum mismatches for tampered files."""
    storage = LocalFilesystemStorage(tmp_path)
    key = "originals/file-9/checksum-9"
    original_payload = b"payload"
    meta = await storage.put(key, original_payload, immutable=True)
    stored_path = tmp_path / key
    stored_path.chmod(0o600)
    stored_path.write_bytes(b"tampered")

    with pytest.raises(StorageChecksumMismatchError):
        await storage.get(key, expected_checksum_sha256=meta.checksum_sha256)

    with pytest.raises(StorageChecksumMismatchError):
        await storage.stat(key, expected_checksum_sha256=meta.checksum_sha256)


@pytest.mark.asyncio
@pytest.mark.parametrize("storage_kind", ["memory", "local"])
async def test_storage_copy_to_path_removes_partial_destination_on_checksum_mismatch(
    tmp_path: Path,
    storage_kind: str,
) -> None:
    """Streaming copy should clean up partial destinations on checksum mismatch."""
    storage: MemoryStorage | LocalFilesystemStorage
    if storage_kind == "memory":
        storage = MemoryStorage()
    else:
        storage = LocalFilesystemStorage(tmp_path / "storage")

    key = "originals/file-copy/checksum-copy"
    payload = b"copy-payload"
    await storage.put(key, payload, immutable=True)
    destination = tmp_path / f"{storage_kind}-checksum.bin"

    with pytest.raises(StorageChecksumMismatchError):
        await storage.copy_to_path(
            key,
            destination,
            expected_checksum_sha256=hashlib.sha256(b"different").hexdigest(),
        )

    assert destination.exists() is False


@pytest.mark.asyncio
@pytest.mark.parametrize("storage_kind", ["memory", "local"])
async def test_storage_copy_to_path_rejects_existing_destination_without_overwrite(
    tmp_path: Path,
    storage_kind: str,
) -> None:
    """Streaming copy should fail when the destination already exists."""
    storage: MemoryStorage | LocalFilesystemStorage
    if storage_kind == "memory":
        storage = MemoryStorage()
    else:
        storage = LocalFilesystemStorage(tmp_path / "storage")

    key = "originals/file-copy/checksum-copy"
    payload = b"copy-payload"
    await storage.put(key, payload, immutable=True)
    destination = tmp_path / f"{storage_kind}-exists.bin"
    destination.write_bytes(b"existing")

    with pytest.raises(FileExistsError):
        await storage.copy_to_path(key, destination)

    assert destination.read_bytes() == b"existing"
