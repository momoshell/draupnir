from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from app.ingestion.canonical.hashing import normalize_sha256_hex

ENTITY_PROVENANCE_ALLOWED_ORIGINS: Final[frozenset[str]] = frozenset(
    {
        "source_direct",
        "adapter_normalized",
        "inferred",
        "user_created",
        "agent_proposed",
        "generated_export",
    }
)

ENTITY_PROVENANCE_REQUIRED_KEYS: Final[tuple[str, ...]] = (
    "origin",
    "adapter",
    "source_ref",
    "source_identity",
    "source_hash",
    "extraction_path",
    "notes",
)

_LEGACY_ROOT_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "adapter": ("adapter_key", "adapter_name", "adapter_id", "adapter_info", "adapter_meta"),
    "source_ref": ("source_reference", "source_locator", "source_entity_ref"),
    "source_identity": (
        "source_id",
        "entity_identity",
        "source_handle",
        "dxf_handle",
        "source_entity_handle",
        "native_handle",
    ),
    "source_hash": ("normalized_source_hash", "record_hash"),
    "extraction_path": ("path", "extraction"),
    "notes": ("debug_notes", "provenance_notes"),
}
_LEGACY_NESTED_ALIASES: Final[dict[str, tuple[tuple[str, ...], ...]]] = {
    "adapter": (("adapter", "key"), ("adapter", "name"), ("adapter", "id")),
    "source_ref": (("source", "ref"), ("source", "reference")),
    "source_identity": (("source", "identity"), ("source", "id")),
    "source_hash": (("source", "hash"), ("source", "sha256")),
}
_NULLABLE_SOURCE_FIELDS: Final[frozenset[str]] = frozenset(
    {"source_ref", "source_identity", "source_hash"}
)
_EXTRA_KEY_PREFIX: Final[str] = "extra:"
_LENIENT_DEFAULT_ORIGIN: Final[str] = "adapter_normalized"

type EntityProvenanceExtra = dict[str, Any]
type EntityProvenanceValue = str | None | dict[str, Any] | list[Any]
type EntityProvenance = dict[str, EntityProvenanceValue]


class EntityProvenanceError(ValueError):
    """Raised when entity provenance cannot be validated or canonicalized."""


def build_entity_provenance(
    *,
    origin: str,
    adapter: str | Mapping[str, Any],
    extraction_path: str | Mapping[str, Any] | list[Any],
    notes: str | Mapping[str, Any] | list[Any],
    source_ref: str | None = None,
    source_identity: str | None = None,
    source_hash: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> EntityProvenance:
    provenance: EntityProvenance = {
        "origin": _validate_origin(origin),
        "adapter": _require_mapping_or_string("adapter", adapter),
        "source_ref": _require_optional_string("source_ref", source_ref),
        "source_identity": _require_optional_string("source_identity", source_identity),
        "source_hash": _normalize_source_hash(source_hash, field_name="source_hash"),
        "extraction_path": _require_non_null("extraction_path", extraction_path),
        "notes": _require_non_null("notes", notes),
    }
    if extra:
        provenance["extra"] = _normalize_extra(extra)
    return provenance


def canonicalize_entity_provenance(payload: Mapping[str, Any]) -> EntityProvenance:
    canonical: EntityProvenance = {}
    for key in ENTITY_PROVENANCE_REQUIRED_KEYS:
        value = _resolve_canonical_value(payload, key)
        if key == "origin":
            canonical[key] = _LENIENT_DEFAULT_ORIGIN if value is None else _validate_origin(value)
        elif key == "adapter":
            canonical[key] = {} if value is None else _require_mapping_or_string(key, value)
        elif key == "source_hash":
            canonical[key] = _normalize_source_hash(value, field_name=key)
        elif key in _NULLABLE_SOURCE_FIELDS:
            canonical[key] = _require_optional_string(key, value)
        else:
            canonical[key] = [] if value is None else _require_non_null(key, value)

    extra = _resolve_extra(payload)
    if extra is not None:
        canonical["extra"] = extra
    return canonical


def validate_entity_provenance(payload: Mapping[str, Any]) -> EntityProvenance:
    missing_keys = [key for key in ENTITY_PROVENANCE_REQUIRED_KEYS if key not in payload]
    if missing_keys:
        if len(missing_keys) == 1:
            raise EntityProvenanceError(
                f"entity provenance missing required key: {missing_keys[0]}"
            )
        raise EntityProvenanceError(
            f"entity provenance missing required keys: {', '.join(missing_keys)}"
        )

    canonical: EntityProvenance = {}
    for key in ENTITY_PROVENANCE_REQUIRED_KEYS:
        value = payload[key]
        if key == "origin":
            canonical[key] = _validate_origin(value)
        elif key == "adapter":
            canonical[key] = _require_mapping_or_string(key, value)
        elif key == "source_hash":
            canonical[key] = _normalize_source_hash(value, field_name=key)
        elif key in _NULLABLE_SOURCE_FIELDS:
            canonical[key] = _require_optional_string(key, value)
        else:
            canonical[key] = _require_non_null(key, value)

    if "extra" in payload:
        canonical["extra"] = _normalize_extra(payload["extra"])
    return canonical


def _resolve_canonical_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]

    for alias in _LEGACY_ROOT_ALIASES.get(key, ()):
        if alias in payload:
            return payload[alias]

    for path in _LEGACY_NESTED_ALIASES.get(key, ()):
        nested_value = _dig(payload, path)
        if nested_value is not None:
            return nested_value

    return None


def _resolve_extra(payload: Mapping[str, Any]) -> EntityProvenanceExtra | None:
    if "extra" in payload:
        return _normalize_extra(payload["extra"])

    legacy_extra = {
        key.removeprefix(_EXTRA_KEY_PREFIX): value
        for key, value in payload.items()
        if key.startswith(_EXTRA_KEY_PREFIX) and value is not None
    }
    if legacy_extra:
        return _normalize_extra(legacy_extra)

    return None


def _normalize_extra(extra: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _normalize_extra_mapping(extra)
    native = normalized.get("native")
    if native is not None and not isinstance(native, dict):
        raise EntityProvenanceError("entity provenance extra.native must be a mapping")
    legacy_aliases = normalized.get("legacy_aliases")
    if legacy_aliases is not None and not isinstance(legacy_aliases, dict):
        raise EntityProvenanceError("entity provenance extra.legacy_aliases must be a mapping")
    return normalized


def _normalize_extra_mapping(extra_value: Any) -> dict[str, Any]:
    if extra_value is None:
        raise EntityProvenanceError("entity provenance extra must be non-null")
    if not isinstance(extra_value, Mapping):
        raise EntityProvenanceError("entity provenance extra must be a mapping")

    normalized: dict[str, Any] = {}
    for key, value in extra_value.items():
        if not key:
            raise EntityProvenanceError("entity provenance extra keys must be non-empty")
        if value is None:
            raise EntityProvenanceError(f"entity provenance extra value must be non-null: {key}")
        if isinstance(value, Mapping):
            normalized[key] = dict(value)
        elif isinstance(value, list):
            normalized[key] = list(value)
        else:
            normalized[key] = value
    return normalized


def _dig(payload: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _validate_origin(origin: Any) -> str:
    if not isinstance(origin, str):
        raise EntityProvenanceError("entity provenance origin must be a string")
    if origin not in ENTITY_PROVENANCE_ALLOWED_ORIGINS:
        allowed = ", ".join(sorted(ENTITY_PROVENANCE_ALLOWED_ORIGINS))
        raise EntityProvenanceError(f"entity provenance origin must be one of: {allowed}")
    return origin


def _require_mapping_or_string(field_name: str, value: Any) -> str | dict[str, Any]:
    if value is None:
        raise EntityProvenanceError(f"entity provenance {field_name} must be non-null")
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return dict(value)
    raise EntityProvenanceError(f"entity provenance {field_name} must be a string or mapping")


def _require_optional_string(field_name: str, value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise EntityProvenanceError(f"entity provenance {field_name} must be a string or null")
    return value


def _require_non_null(field_name: str, value: Any) -> Any:
    if value is None:
        raise EntityProvenanceError(f"entity provenance {field_name} must be non-null")
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, list):
        return list(value)
    return value


def _normalize_source_hash(value: Any, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise EntityProvenanceError(
            f"entity provenance {field_name} must be a SHA-256 string or null"
        )
    normalized = value.strip().lower()
    try:
        return normalize_sha256_hex(normalized, allow_prefix=True)
    except ValueError as exc:
        raise EntityProvenanceError(
            f"entity provenance {field_name} must be a raw 64-character SHA-256 hex string"
        ) from exc
