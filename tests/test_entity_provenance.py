from __future__ import annotations

from typing import Any, Literal, TypedDict, cast

import pytest

from app.ingestion.canonical import (
    ENTITY_PROVENANCE_ALLOWED_ORIGINS,
    ENTITY_PROVENANCE_REQUIRED_KEYS,
    EntityProvenanceError,
    build_entity_provenance,
    canonicalize_entity_provenance,
    validate_entity_provenance,
)
from app.ingestion.contracts import JSONValue


def test_build_entity_provenance_returns_required_keys_and_namespaced_extra() -> None:
    provenance = build_entity_provenance(
        origin="adapter_normalized",
        adapter={"key": "dwg"},
        source_ref="layer:wall",
        source_identity="entity-1",
        source_hash="sha256:ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
        extraction_path=["page", 1, "entity", 2],
        notes={"detail": "normalized"},
        extra={
            "native": {"dwg": {"record_hash": "legacy"}},
            "legacy_aliases": {"record_hash": "legacy"},
        },
    )

    assert tuple(provenance)[:7] == ENTITY_PROVENANCE_REQUIRED_KEYS
    assert set(ENTITY_PROVENANCE_REQUIRED_KEYS).issubset(provenance)
    assert provenance["source_hash"] == (
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    )
    assert provenance["extra"] == {
        "native": {"dwg": {"record_hash": "legacy"}},
        "legacy_aliases": {"record_hash": "legacy"},
    }


@pytest.mark.parametrize(
    ("field_name", "value"),
    [("adapter", None), ("extraction_path", None), ("notes", None)],
)
def test_build_entity_provenance_rejects_non_nullable_required_fields(
    field_name: Literal["adapter", "extraction_path", "notes"], value: None
) -> None:
    class BuildEntityProvenanceKwargs(TypedDict):
        origin: str
        adapter: str
        source_ref: str | None
        source_identity: str | None
        source_hash: str | None
        extraction_path: dict[str, JSONValue] | list[object] | str
        notes: dict[str, JSONValue] | list[object] | str

    kwargs: BuildEntityProvenanceKwargs = {
        "origin": "adapter_normalized",
        "adapter": "dwg",
        "source_ref": None,
        "source_identity": None,
        "source_hash": None,
        "extraction_path": {"step": 1},
        "notes": ["ok"],
    }
    invalid_kwargs: dict[str, object] = dict(kwargs)
    invalid_kwargs[field_name] = value

    with pytest.raises(EntityProvenanceError, match=field_name):
        build_entity_provenance(**cast(Any, invalid_kwargs))


def test_build_entity_provenance_rejects_invalid_origin() -> None:
    with pytest.raises(EntityProvenanceError, match="must be one of"):
        build_entity_provenance(
            origin="legacy",
            adapter="dwg",
            extraction_path="root.entities[0]",
            notes="note",
        )


def test_allowed_origins_is_exposed() -> None:
    assert sorted(ENTITY_PROVENANCE_ALLOWED_ORIGINS) == [
        "adapter_normalized",
        "agent_proposed",
        "generated_export",
        "inferred",
        "source_direct",
        "user_created",
    ]


def test_canonicalize_entity_provenance_supports_legacy_aliases() -> None:
    payload = {
        "adapter": {"key": "pdf_vector"},
        "source_entity_ref": "page:1",
        "source_handle": "shape-7",
        "record_hash": "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
        "path": {"page": 1, "shape": 7},
        "debug_notes": {"warning": "fallback"},
        "extra": {
            "native": {"pdf_vector": {"adapter_record_hash": "sha256:..."}},
            "legacy_aliases": {"record_hash": "record_hash"},
        },
    }

    canonical = canonicalize_entity_provenance(payload)

    assert canonical == {
        "origin": "adapter_normalized",
        "adapter": {"key": "pdf_vector"},
        "source_ref": "page:1",
        "source_identity": "shape-7",
        "source_hash": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "extraction_path": {"page": 1, "shape": 7},
        "notes": {"warning": "fallback"},
        "extra": {
            "native": {"pdf_vector": {"adapter_record_hash": "sha256:..."}},
            "legacy_aliases": {"record_hash": "record_hash"},
        },
    }


def test_canonicalize_entity_provenance_defaults_missing_required_fields() -> None:
    canonical = canonicalize_entity_provenance({})

    assert canonical == {
        "origin": "adapter_normalized",
        "adapter": {},
        "source_ref": None,
        "source_identity": None,
        "source_hash": None,
        "extraction_path": [],
        "notes": [],
    }


def test_canonicalize_entity_provenance_allows_nullable_source_fields() -> None:
    canonical = canonicalize_entity_provenance(
        {
            "origin": "source_direct",
            "adapter": "ifc",
            "extraction_path": "entities[2]",
            "notes": {"detail": "raw"},
        }
    )

    assert canonical["source_ref"] is None
    assert canonical["source_identity"] is None
    assert canonical["source_hash"] is None


def test_canonicalize_entity_provenance_uses_normalized_source_hash_alias() -> None:
    canonical = canonicalize_entity_provenance(
        {
            "origin": "source_direct",
            "adapter": "dxf",
            "normalized_source_hash": (
                "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"
            ),
            "extraction_path": ["modelspace", 0],
            "notes": "debug",
        }
    )

    assert canonical["source_hash"] == (
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    )


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        ("source_id", "entity-1"),
        ("source_handle", "A1"),
        ("dxf_handle", "B2"),
        ("source_entity_handle", "C3"),
        ("native_handle", "D4"),
    ],
)
def test_canonicalize_entity_provenance_supports_identity_aliases(
    alias: str, expected: str
) -> None:
    canonical = canonicalize_entity_provenance(
        {
            "adapter": "dxf",
            alias: expected,
        }
    )

    assert canonical["origin"] == "adapter_normalized"
    assert canonical["source_identity"] == expected
    assert canonical["extraction_path"] == []
    assert canonical["notes"] == []


def test_canonicalize_entity_provenance_preserves_top_level_extra_mapping() -> None:
    canonical = canonicalize_entity_provenance(
        {
            "origin": "generated_export",
            "adapter": {"key": "revision_json_export"},
            "source_ref": None,
            "source_identity": None,
            "source_hash": None,
            "extraction_path": {"export": "revision-json"},
            "notes": {"detail": "derived"},
            "extra": {
                "native": {"revision_json_export": {"artifact_id": "abc"}},
                "legacy_aliases": {"artifact_id": "artifact_id"},
            },
        }
    )

    assert canonical["extra"] == {
        "native": {"revision_json_export": {"artifact_id": "abc"}},
        "legacy_aliases": {"artifact_id": "artifact_id"},
    }


def test_validate_entity_provenance_rejects_missing_required_key() -> None:
    with pytest.raises(EntityProvenanceError, match="missing required key: notes"):
        validate_entity_provenance(
            {
                "origin": "source_direct",
                "adapter": "dxf",
                "source_ref": None,
                "source_identity": None,
                "source_hash": None,
                "extraction_path": "entities[0]",
            }
        )


def test_validate_entity_provenance_requires_top_level_required_keys() -> None:
    with pytest.raises(EntityProvenanceError, match="missing required key: source_ref"):
        validate_entity_provenance(
            {
                "origin": "adapter_normalized",
                "adapter": {"key": "pdf_vector"},
                "source_entity_ref": "page:1",
                "source_identity": "shape-7",
                "source_hash": None,
                "extraction_path": {"page": 1, "shape": 7},
                "notes": {"warning": "fallback"},
            }
        )


def test_validate_entity_provenance_rejects_invalid_origin() -> None:
    with pytest.raises(EntityProvenanceError, match="must be one of"):
        validate_entity_provenance(
            {
                "origin": "bad-origin",
                "adapter": "dxf",
                "source_ref": None,
                "source_identity": None,
                "source_hash": None,
                "extraction_path": "entities[0]",
                "notes": "note",
            }
        )


def test_validate_entity_provenance_rejects_null_adapter() -> None:
    with pytest.raises(EntityProvenanceError, match="adapter"):
        validate_entity_provenance(
            {
                "origin": "source_direct",
                "adapter": None,
                "source_ref": None,
                "source_identity": None,
                "source_hash": None,
                "extraction_path": [],
                "notes": [],
            }
        )


def test_validate_entity_provenance_rejects_invalid_hash() -> None:
    with pytest.raises(EntityProvenanceError, match="64-character SHA-256"):
        validate_entity_provenance(
            {
                "origin": "source_direct",
                "adapter": "dxf",
                "source_ref": None,
                "source_identity": None,
                "source_hash": "not-a-hash",
                "extraction_path": "entities[0]",
                "notes": "note",
            }
        )


def test_validate_entity_provenance_normalizes_prefixed_hash_with_whitespace() -> None:
    canonical = validate_entity_provenance(
        {
            "origin": "source_direct",
            "adapter": "dxf",
            "source_ref": None,
            "source_identity": None,
            "source_hash": (
                "  sha256:ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789\n"
            ),
            "extraction_path": "entities[0]",
            "notes": "note",
        }
    )

    assert canonical["source_hash"] == (
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    )
