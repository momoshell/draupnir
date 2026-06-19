"""Unit tests for the shared canonical ref-resolver (#535)."""

from app.ingestion.ref_resolution import (
    COLLECTION_REF_FALLBACK_KEYS,
    collection_ref,
    first_ref,
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
