"""Unit tests for storage backends."""

from pathlib import Path

import pytest

from app.storage import LocalFilesystemStorage, LocalStorage, MemoryStorage


@pytest.mark.asyncio
async def test_memory_storage_round_trip_with_bytes() -> None:
    """Memory storage should persist and return bytes plus metadata."""
    storage = MemoryStorage()

    meta = await storage.put("originals/file-1/checksum-1", b"payload", immutable=True)
    stored = await storage.get("originals/file-1/checksum-1")

    assert meta.key == "originals/file-1/checksum-1"
    assert meta.storage_uri == "memory://originals/file-1/checksum-1"
    assert meta.size_bytes == 7
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
    assert await storage.exists("originals/file-2/checksum-2") is True
    assert (await storage.get("originals/file-2/checksum-2")).body == b"from-path"


@pytest.mark.asyncio
async def test_memory_storage_stat_and_presign_stub() -> None:
    """Memory storage should support metadata checks and presign parity."""
    storage = MemoryStorage()
    key = "originals/file-3/checksum-3"
    await storage.put(key, b"abc", immutable=True)

    assert (await storage.stat(key)).size_bytes == 3
    assert await storage.presign(key) is None


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
