"""Shared helpers for immutable storage object keys."""

from uuid import UUID


def build_original_storage_key(file_id: UUID, checksum: str) -> str:
    """Build the immutable storage key for an uploaded source file."""
    return f"originals/{file_id}/{checksum}"
