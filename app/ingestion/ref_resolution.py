"""Shared resolution of canonical payload references.

Single source of truth for how a stable reference string is read from a canonical
payload (which keys, in what precedence). Used by materialization (to assign and
link refs) and by reconciliation (to detect orphaned references), so the two never
drift in what counts as "the same" layer / block / entity.

This is the pure *candidate* resolution only — materialization layers its own
uniqueness allocation / synthesis on top for row identity.
"""

from collections.abc import Mapping
from typing import Any

# Declared collection entries (layouts / layers / blocks) carry their identity under
# the typed ref key, falling back to these in precedence order.
COLLECTION_REF_FALLBACK_KEYS = ("name", "ref", "id")

# An entity references its layer / block via the contract key, with a legacy alias.
ENTITY_LAYER_KEYS = ("layer_ref", "layer")
ENTITY_BLOCK_KEYS = ("block_ref", "block")
# Candidate keys that identify an entity (for matching parent references).
ENTITY_ID_KEYS = ("entity_id", "id", "source_identity")
ENTITY_PARENT_KEYS = ("parent_entity_ref", "parent_id")


def string_ref(value: Any) -> str | None:
    """Normalize a persisted ref string extracted from canonical payloads."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def first_ref(payload: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    """Return the first non-empty string ref among ``keys`` (in precedence order)."""
    for key in keys:
        ref = string_ref(payload.get(key))
        if ref is not None:
            return ref
    return None


def collection_ref(payload: Mapping[str, Any], explicit_key: str) -> str | None:
    """Resolve a declared collection entry's ref (typed key, then name/ref/id)."""
    return first_ref(payload, (explicit_key, *COLLECTION_REF_FALLBACK_KEYS))
