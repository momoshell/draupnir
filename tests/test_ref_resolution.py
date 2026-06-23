"""Unit tests for the shared canonical ref-resolver (#535)."""

from typing import Any

from app.ingestion.ref_resolution import (
    BLOCK_IDENTITY_KEYS,
    COLLECTION_REF_FALLBACK_KEYS,
    LAYER_IDENTITY_KEYS,
    collection_identity,
    collection_identity_index,
    collection_ref,
    first_ref,
    ref_set,
    string_ref,
)


def test_string_ref_normalizes_and_drops_empty() -> None:
    assert string_ref("  A-WALL  ") == "A-WALL"
    assert string_ref("") is None
    assert string_ref("   ") is None
    assert string_ref(None) is None
    assert string_ref(42) == "42"


def test_first_ref_follows_key_precedence() -> None:
    payload = {"layer_ref": "primary", "layer": "legacy"}
    assert first_ref(payload, ("layer_ref", "layer")) == "primary"
    assert first_ref({"layer": "legacy"}, ("layer_ref", "layer")) == "legacy"
    assert first_ref({}, ("layer_ref", "layer")) is None


def test_collection_ref_prefers_typed_key_then_name_ref_id() -> None:
    assert collection_ref({"layer_ref": "X", "name": "Y"}, "layer_ref") == "X"
    # name precedes ref/id in the fallback order.
    assert collection_ref({"ref": "r", "name": "Y"}, "layer_ref") == "Y"
    assert collection_ref({"ref": "r", "id": "i"}, "layer_ref") == "r"
    assert collection_ref({"id": "i"}, "layer_ref") == "i"
    assert collection_ref({}, "layer_ref") is None


def test_collection_fallback_keys_constant() -> None:
    assert COLLECTION_REF_FALLBACK_KEYS == ("name", "ref", "id")


def test_ref_set_collects_all_identifiers_without_precedence() -> None:
    # Unlike first_ref, ref_set returns EVERY non-empty ref among the keys.
    assert ref_set({"layer_ref": "L", "name": "A-WALL", "ref": "L"}, LAYER_IDENTITY_KEYS) == {
        "L",
        "A-WALL",
    }
    assert ref_set({}, LAYER_IDENTITY_KEYS) == set()


def test_collection_identity_is_full_candidate_set() -> None:
    # #539: a layer is identified by ALL its candidate refs, not the single precedence-winner.
    assert collection_identity({"ref": "L", "name": "A-WALL"}, LAYER_IDENTITY_KEYS) == {
        "L",
        "A-WALL",
    }
    # Block identity uses the block-specific alias set.
    assert collection_identity({"block_ref": "B", "block_name": "DOOR"}, BLOCK_IDENTITY_KEYS) == {
        "B",
        "DOOR",
    }


def test_collection_identity_index_unions_all_entries() -> None:
    layers: list[Any] = [
        {"ref": "L1", "name": "A-WALL"},
        {"layer_ref": "L2"},
        "not-a-mapping",  # skipped defensively
    ]
    assert collection_identity_index(layers, LAYER_IDENTITY_KEYS) == {"L1", "A-WALL", "L2"}
    assert collection_identity_index([], LAYER_IDENTITY_KEYS) == set()
