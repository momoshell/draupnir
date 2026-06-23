"""Shared resolution of canonical payload references.

Single source of truth for how a stable reference string is read from a canonical
payload (which keys, in what precedence). Used by materialization (to assign and
link refs) and by reconciliation (to detect orphaned references), so the two never
drift in what counts as "the same" layer / block / entity.

This is the pure *candidate* resolution only — materialization layers its own
uniqueness allocation / synthesis on top for row identity.
"""

from collections.abc import Mapping, Sequence
from typing import Any

# Declared collection entries (layouts / layers / blocks) carry their identity under
# the typed ref key, falling back to these in precedence order. Used for *assignment*
# (picking the single canonical ref a materialized row is stored under).
COLLECTION_REF_FALLBACK_KEYS = ("name", "ref", "id")

# An entity references its layer / block via the contract key, with legacy + name aliases.
ENTITY_LAYER_KEYS = ("layer_ref", "layer", "layer_name")
ENTITY_BLOCK_KEYS = ("block_ref", "block", "block_name")
# Candidate keys that identify an entity (for matching parent references).
ENTITY_ID_KEYS = ("entity_id", "id", "source_identity")
ENTITY_PARENT_KEYS = ("parent_entity_ref", "parent_id")

# A declared collection entry can be *matched* by ANY of its identifying refs, not just the
# single precedence-winner used for assignment (#539). These are the full identity key sets:
# the typed ref key + every alias an entry or an entity-side reference may use. Matching is
# set-membership so e.g. a layer ``{"ref": "L", "name": "A-WALL"}`` is the same layer whether an
# entity references it as ``layer_ref="L"`` or ``layer="A-WALL"``.
LAYER_IDENTITY_KEYS = ("layer_ref", "name", "layer_name", "ref", "id")
BLOCK_IDENTITY_KEYS = ("block_ref", "name", "block_name", "ref", "id")


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
    """Resolve a declared collection entry's single canonical ref (typed key, then name/ref/id).

    This is the *assignment* ref — the one a materialized row is stored under. For *matching*
    (does an entity reference this entry?) use :func:`collection_identity` instead, which returns
    the full candidate set rather than one precedence-winner (#539).
    """
    return first_ref(payload, (explicit_key, *COLLECTION_REF_FALLBACK_KEYS))


def ref_set(payload: Mapping[str, Any], keys: tuple[str, ...]) -> set[str]:
    """Return the set of all non-empty string refs among ``keys`` (no precedence)."""
    return {ref for key in keys if (ref := string_ref(payload.get(key))) is not None}


def collection_identity(payload: Mapping[str, Any], identity_keys: tuple[str, ...]) -> set[str]:
    """Every ref a declared collection entry can be matched by (#539).

    The single authoritative matching rule: an entity references this entry iff the entity's
    resolved ref is in this set. Pass :data:`LAYER_IDENTITY_KEYS` / :data:`BLOCK_IDENTITY_KEYS`.
    """
    return ref_set(payload, identity_keys)


def collection_identity_index(
    entries: Sequence[Mapping[str, Any]],
    identity_keys: tuple[str, ...],
) -> set[str]:
    """Union of every declared entry's identity refs — the set an entity ref must hit to link."""
    index: set[str] = set()
    for entry in entries:
        if isinstance(entry, Mapping):
            index |= collection_identity(entry, identity_keys)
    return index
