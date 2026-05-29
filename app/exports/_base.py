"""Shared export renderer helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

type JSONScalarNormalizer = Callable[[object], tuple[bool, JSONValue]]
type JSONMappingKeyNormalizer = Callable[[object], str]
type JSONUnsupportedValueFactory = Callable[[object], Exception]


@dataclass(frozen=True, slots=True)
class ExportArtifact:
    """Shared rendered export artifact metadata."""

    content_bytes: bytes
    checksum_sha256: str
    size_bytes: int
    media_type: str
    generator_name: str
    generator_version: str


@dataclass(frozen=True, slots=True)
class ExportArtifactWithOptions(ExportArtifact):
    """Shared rendered export artifact metadata with normalized options."""

    options: dict[str, JSONValue]


def build_artifact[T: ExportArtifact](
    artifact_type: type[T],
    *,
    content_bytes: bytes,
    media_type: str,
    generator_name: str,
    generator_version: str,
    options: dict[str, JSONValue] | None = None,
) -> T:
    """Build a typed export artifact with shared checksum and size metadata."""

    artifact = ExportArtifact(
        content_bytes=content_bytes,
        checksum_sha256=hashlib.sha256(content_bytes).hexdigest(),
        size_bytes=len(content_bytes),
        media_type=media_type,
        generator_name=generator_name,
        generator_version=generator_version,
    )

    if issubclass(artifact_type, ExportArtifactWithOptions):
        if options is None:
            raise TypeError("Options are required for export artifacts with options.")
        return artifact_type(
            content_bytes=artifact.content_bytes,
            checksum_sha256=artifact.checksum_sha256,
            size_bytes=artifact.size_bytes,
            media_type=artifact.media_type,
            generator_name=artifact.generator_name,
            generator_version=artifact.generator_version,
            options=options,
        )

    if options is not None:
        raise TypeError("Options are only supported for export artifacts with options.")

    return artifact_type(
        content_bytes=artifact.content_bytes,
        checksum_sha256=artifact.checksum_sha256,
        size_bytes=artifact.size_bytes,
        media_type=artifact.media_type,
        generator_name=artifact.generator_name,
        generator_version=artifact.generator_version,
    )


def canonical_json_bytes(
    payload: object,
    *,
    normalize_value: Callable[[object], JSONValue],
) -> bytes:
    """Serialize payload to canonical UTF-8 JSON bytes."""

    return canonical_json_text(normalize_value(payload)).encode("utf-8")


def canonical_json_text(value: JSONValue) -> str:
    """Serialize normalized JSON to canonical text."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def normalize_json_value_tree(
    value: object,
    *,
    normalize_scalar: JSONScalarNormalizer,
    normalize_mapping_key: JSONMappingKeyNormalizer,
    unsupported_value: JSONUnsupportedValueFactory,
) -> JSONValue:
    """Normalize JSON-compatible trees with module-specific scalar rules."""

    handled, normalized = normalize_scalar(value)
    if handled:
        return normalized
    if isinstance(value, list | tuple):
        return [
            normalize_json_value_tree(
                item,
                normalize_scalar=normalize_scalar,
                normalize_mapping_key=normalize_mapping_key,
                unsupported_value=unsupported_value,
            )
            for item in value
        ]
    if isinstance(value, Mapping):
        return {
            normalize_mapping_key(key): normalize_json_value_tree(
                item,
                normalize_scalar=normalize_scalar,
                normalize_mapping_key=normalize_mapping_key,
                unsupported_value=unsupported_value,
            )
            for key, item in value.items()
        }
    raise unsupported_value(value)


def normalize_datetime(value: datetime) -> str:
    """Normalize datetimes to UTC ISO-8601 with Z suffix."""

    normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return normalized.isoformat().replace("+00:00", "Z")
