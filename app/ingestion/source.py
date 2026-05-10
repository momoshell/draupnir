"""Source materialization helpers for ingestion runners."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

from app.ingestion.contracts import AdapterSource, InputFamily, UploadFormat
from app.storage import Storage, get_storage
from app.storage.base import (
    StorageChecksumMismatchError,
    StorageReadError,
    StorageWriteError,
)
from app.storage.keys import build_original_storage_key


@dataclass(frozen=True, slots=True)
class OriginalSourceMaterialization:
    """Immutable source metadata required to stage an original upload."""

    file_id: UUID
    checksum_sha256: str
    upload_format: UploadFormat
    input_family: InputFamily
    media_type: str | None
    original_name: str | None = None


class OriginalSourceReadError(Exception):
    """Sanitized original-source read failure."""

    def __init__(self, *, storage_key: str, reason: str) -> None:
        super().__init__("Failed to read original source from storage.")
        self.storage_key = storage_key
        self.reason = reason


class OriginalSourceStageError(Exception):
    """Sanitized original-source staging failure."""

    def __init__(self, *, reason: str) -> None:
        super().__init__("Failed to stage original source.")
        self.reason = reason


@asynccontextmanager
async def materialize_original_source(
    source: OriginalSourceMaterialization,
    *,
    storage: Storage | None = None,
    temp_root: Path | None = None,
) -> AsyncIterator[AdapterSource]:
    """Fetch an immutable original and stage it into an attempt-local tempdir."""
    resolved_storage = storage or get_storage()
    storage_key = build_original_storage_key(source.file_id, source.checksum_sha256)
    temp_dir_root = str(temp_root) if temp_root is not None else None
    with TemporaryDirectory(prefix="ingestion-source-", dir=temp_dir_root) as temp_dir:
        file_path = Path(temp_dir) / _materialized_name(source)
        try:
            await resolved_storage.copy_to_path(
                storage_key,
                file_path,
                expected_checksum_sha256=source.checksum_sha256,
            )
        except StorageChecksumMismatchError as exc:
            raise OriginalSourceReadError(
                storage_key=storage_key,
                reason="checksum_mismatch",
            ) from exc
        except (FileNotFoundError, KeyError) as exc:
            raise OriginalSourceReadError(
                storage_key=storage_key,
                reason="not_found",
            ) from exc
        except StorageReadError as exc:
            raise OriginalSourceReadError(
                storage_key=storage_key,
                reason="read_failed",
            ) from exc
        except StorageWriteError as exc:
            raise OriginalSourceStageError(reason="stage_failed") from exc
        except OSError as exc:
            raise OriginalSourceStageError(reason="stage_failed") from exc
        yield AdapterSource(
            file_path=file_path,
            upload_format=source.upload_format,
            input_family=source.input_family,
            media_type=source.media_type,
            original_name=source.original_name,
        )


def _materialized_name(source: OriginalSourceMaterialization) -> str:
    return f"source.{source.upload_format.value}"
