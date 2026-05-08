"""Shared helpers for immutable storage object keys."""

from pathlib import PureWindowsPath
from uuid import UUID


def build_original_storage_key(file_id: UUID, checksum: str) -> str:
    """Build the immutable storage key for an uploaded source file."""
    return f"originals/{file_id}/{checksum}"


def build_generated_artifact_storage_key(artifact_id: UUID, filename: str) -> str:
    """Build the immutable storage key for a generated artifact payload."""
    return f"artifacts/{artifact_id}/{_normalize_artifact_filename(filename)}"


def _normalize_artifact_filename(filename: str) -> str:
    stripped = filename.strip()
    if not stripped:
        raise ValueError("Artifact filename must not be empty.")

    candidate_path = PureWindowsPath(stripped)
    candidate = candidate_path.name.strip()
    if candidate in {"", ".", ".."}:
        raise ValueError("Artifact filename must resolve to a single file name.")
    if candidate_path.parent != PureWindowsPath("."):
        raise ValueError("Artifact filename must not contain path segments.")

    return candidate
