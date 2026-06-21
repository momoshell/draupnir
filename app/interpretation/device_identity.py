"""Device identity resolution (Phase 2, epic #545).

Consumes the :class:`~app.interpretation.legend_dictionary.LegendDictionary` built in Phase 1
and emits a typed :class:`DeviceIdentity` for every placed block instance (:class:`Device`).

Classification is deterministic and order-independent. Every instance is emitted — nothing is
dropped. The classification precedence (first match wins):

1. ``legend_exemplar`` — layer_ref contains the legend match token (landmine guard: keyed on
   layer, NEVER on block_ref).
2. ``device``          — legend-resolvable: normalized tag in by_abbreviation() OR block_ref in
   by_symbol_family() (ADR-002: legend membership is the primary device signal).
3. ``architecture``    — block_ref case-insensitively matches any ``ARCHITECTURE_FAMILY_PATTERNS``.
4. ``device``          — any tag present (tagged but not legend-resolvable — still a candidate).
5. ``annotation``      — obvious annotation families.
6. ``unknown``         — else.

Resolution priority for kind=device (first match wins, basis recorded):

1. tag.text.strip().upper() in by_abbreviation()  → resolved, basis=tag_abbreviation.
2. block_ref in by_symbol_family()                → resolved, basis=symbol_family.
3. tag present but unmatched                      → unresolved, basis=unresolved_tag.
4. else                                           → unknown, basis=none.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports. Unit-testable with fakes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.devices import Device
from app.interpretation.legend_dictionary import LegendDictionary, LegendEntry

# ---------------------------------------------------------------------------
# Rule version — bump whenever the rule table or precedence order changes.
# ---------------------------------------------------------------------------

#: Consumers can record this to know which classification logic produced an output.
RULE_VERSION: str = "2"

# ---------------------------------------------------------------------------
# Kind constants
# ---------------------------------------------------------------------------

KIND_DEVICE: str = "device"
KIND_ARCHITECTURE: str = "architecture"
KIND_ANNOTATION: str = "annotation"
KIND_LEGEND_EXEMPLAR: str = "legend_exemplar"
KIND_UNKNOWN: str = "unknown"

# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

STATUS_RESOLVED: str = "resolved"
STATUS_UNRESOLVED: str = "unresolved"
STATUS_UNKNOWN: str = "unknown"

# ---------------------------------------------------------------------------
# Basis constants
# ---------------------------------------------------------------------------

BASIS_TAG_ABBREVIATION: str = "tag_abbreviation"
BASIS_SYMBOL_FAMILY: str = "symbol_family"
BASIS_UNRESOLVED_TAG: str = "unresolved_tag"
BASIS_NONE: str = "none"

# ---------------------------------------------------------------------------
# Architecture family patterns — case-insensitive substring match on block_ref.
# Extend here; bump RULE_VERSION when patterns change.
# ---------------------------------------------------------------------------

ARCHITECTURE_FAMILY_PATTERNS: tuple[str, ...] = (
    "Room Tag",
    "Room Name",
    "Grid",
    "Mullion",
    "Door",
    "Gate",
    "Window",
    "Curtain",
    "Placeholder",  # MPA_Placeholder / MPA_StructuralPlaceholder (RC columns)
    "Column",
    "Louvre",
    "Hardscape",
    "_rvt",  # linked Revit model master/container block (nesting root) — never a device
)

# ---------------------------------------------------------------------------
# Annotation family patterns — case-insensitive substring match on block_ref.
# Kept minimal; only obvious annotation block families.
# ---------------------------------------------------------------------------

_ANNOTATION_FAMILY_PATTERNS: tuple[str, ...] = (
    "North Arrow",
    "Scale Bar",
    "Detail Marker",
    "Section Marker",
    "Elevation Marker",
    "Revision Cloud",
    "Title Block",
    "Titleblock",  # RUK-Annotation-TitleblockA0 (no space variant)
    "Annotation",  # generic annotation/drawing-number blocks
)


# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeviceIdentity:
    """Resolved identity for one placed block instance.

    ``confidence=None`` means face-value / legend-exact match (NOT low confidence).
    Conflicts are surfaced via ``competing_type_names``, not via confidence.
    """

    entity_id: str
    kind: str  # KIND_*
    status: str  # STATUS_* (meaningful for kind=device)
    type_name: str | None
    abbreviation: str | None  # raw tag preserved when unresolved
    description: str | None
    basis: str  # BASIS_* or "architecture:<pattern>"
    source_layers: tuple[str, ...]
    confidence: float | None  # None = not-scored / face-value
    competing_type_names: tuple[str, ...]  # carried from the resolved LegendEntry; () when none


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def classify_instance_kind(
    device: Device,
    legend: LegendDictionary,
    *,
    legend_layer_match: str = "LEGEND",
) -> str:
    """Classify a device instance into a coarse kind (deterministic, first match wins).

    Precedence (legend ICON membership is the strong device signal; a mere nearby TAG must
    not promote an architectural family — mullions/columns/grids carry stray tags on real
    drawings, #545 real-data finding):
    1. legend_exemplar — layer_ref contains legend_layer_match (case-insensitive).
    2. device (legend icon) — block_ref in by_symbol_family() (a legend-defined device family).
    3. architecture    — block_ref matches any ARCHITECTURE_FAMILY_PATTERNS (beats proximity tags).
    4. annotation      — block_ref matches any _ANNOTATION_FAMILY_PATTERNS (beats device-by-tag,
       so a tagged titleblock/drawing-number block isn't promoted by a stray nearby tag).
    5. device (legend key) — normalized tag.text in by_abbreviation() (a legend-key abbreviation).
    6. device (tagged) — any tag present (non-architecture, non-annotation, not legend-resolvable).
    7. unknown         — else.
    """
    # 1. Legend exemplar guard — keyed ONLY on layer_ref, never on block_ref.
    if device.layer_ref is not None and legend_layer_match.lower() in device.layer_ref.lower():
        return KIND_LEGEND_EXEMPLAR

    block_ref = device.block_ref or ""
    block_ref_lower = block_ref.lower()
    by_abbr = legend.by_abbreviation()
    by_family = legend.by_symbol_family()

    # 2. Device — legend ICON family. Strong identity: the family IS a legend-defined device
    # family. by_symbol_family carries ONLY legend families (the route scopes Source A to the
    # legend), so this never matches architecture.
    if block_ref and block_ref in by_family:
        return KIND_DEVICE

    # 3. Architecture — family pattern. Beats a nearby/legend-key TAG: a Rectangular Mullion
    # with a stray "H"/"AHU" tag is still a mullion, not a device.
    for pattern in ARCHITECTURE_FAMILY_PATTERNS:
        if pattern.lower() in block_ref_lower:
            return KIND_ARCHITECTURE

    # 4. Annotation — block_ref substring match. Beats device-by-tag so a tagged titleblock /
    #    drawing-number block is not promoted to a device by a stray nearby tag.
    for pattern in _ANNOTATION_FAMILY_PATTERNS:
        if pattern.lower() in block_ref_lower:
            return KIND_ANNOTATION

    # 5. Device — legend KEY abbreviation (normalized strip+upper for the uppercase keys).
    if device.tag is not None and device.tag.text.strip().upper() in by_abbr:
        return KIND_DEVICE

    # 6. Device — any tag present (non-architecture, non-annotation, not legend-resolvable).
    if device.tag is not None:
        return KIND_DEVICE

    # 7. Fallthrough.
    return KIND_UNKNOWN


def resolve_device_identities(
    devices: Sequence[Device],
    legend: LegendDictionary,
    *,
    legend_layer_match: str = "LEGEND",
) -> list[DeviceIdentity]:
    """Classify and resolve every device instance into a :class:`DeviceIdentity`.

    Every instance is emitted — nothing is dropped. Resolution is deterministic and
    order-independent (the legend lookup is key-equality, not insertion-order sensitive).
    """
    by_abbr = legend.by_abbreviation()
    by_family = legend.by_symbol_family()

    result: list[DeviceIdentity] = []
    for device in devices:
        identity = _resolve_one(device, legend, by_abbr, by_family, legend_layer_match)
        result.append(identity)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_one(
    device: Device,
    legend: LegendDictionary,
    by_abbr: dict[str, LegendEntry],
    by_family: dict[str, LegendEntry],
    legend_layer_match: str,
) -> DeviceIdentity:
    """Resolve a single device instance to a :class:`DeviceIdentity`."""
    kind = classify_instance_kind(device, legend, legend_layer_match=legend_layer_match)

    if kind == KIND_DEVICE:
        return _resolve_device(device, by_abbr, by_family)

    # Non-device kinds: emit with a labeled identity, no type resolution.
    source_layers = _source_layers(device)
    if kind == KIND_ARCHITECTURE:
        block_ref_lower = (device.block_ref or "").lower()
        matched_pattern = next(
            (p for p in ARCHITECTURE_FAMILY_PATTERNS if p.lower() in block_ref_lower),
            "",
        )
        return DeviceIdentity(
            entity_id=device.entity_id,
            kind=kind,
            status=STATUS_UNKNOWN,
            type_name=None,
            abbreviation=None,
            description=None,
            basis=f"architecture:{matched_pattern}",
            source_layers=source_layers,
            confidence=None,
            competing_type_names=(),
        )

    return DeviceIdentity(
        entity_id=device.entity_id,
        kind=kind,
        status=STATUS_UNKNOWN,
        type_name=None,
        abbreviation=None,
        description=None,
        basis=BASIS_NONE,
        source_layers=source_layers,
        confidence=None,
        competing_type_names=(),
    )


def _resolve_device(
    device: Device,
    by_abbr: dict[str, LegendEntry],
    by_family: dict[str, LegendEntry],
) -> DeviceIdentity:
    """Apply the resolution priority for kind=device.

    Priority is ICON-FAMILY-FIRST (real-data #545 finding): the legend symbol family is the
    authoritative graphic identity of a placed device, so it wins the type. A nearby tag is a
    weaker signal — on real drawings a stray ``AHU``/``H`` tag sits near many unrelated blocks,
    so resolving type from the tag first mistyped every icon device as its neighbour's tag.
    The tag is the fallback type only when the family is not a legend family.
    """
    # Priority 1 — block_ref in by_symbol_family (the legend ICON family — authoritative type).
    block_ref = device.block_ref or ""
    is_legend_family = bool(block_ref) and block_ref in by_family
    if is_legend_family and by_family[block_ref].type_name is not None:
        entry = by_family[block_ref]
        layers = _source_layers(device)
        return DeviceIdentity(
            entity_id=device.entity_id,
            kind=KIND_DEVICE,
            status=STATUS_RESOLVED,
            type_name=entry.type_name,
            abbreviation=entry.abbreviation,
            description=entry.description,
            basis=BASIS_SYMBOL_FAMILY,
            source_layers=layers,
            confidence=None,
            competing_type_names=entry.competing_type_names,
        )

    # Placeholder legend family (in by_symbol_family but type_name=None, e.g. Revit "Family - ___"):
    # it IS a device (a legend family) but the block carries no human name. We do NOT borrow a type
    # from a nearby tag — on real drawings those tags are mis-associated noise (#590). Stay
    # UNRESOLVED, preserving the raw tag (if any) as the abbreviation. Never guess.
    if is_legend_family:
        layers = _source_layers(device, include_tag_layer=device.tag is not None)
        return DeviceIdentity(
            entity_id=device.entity_id,
            kind=KIND_DEVICE,
            status=STATUS_UNRESOLVED,
            type_name=None,
            abbreviation=device.tag.text if device.tag is not None else None,
            description=None,
            basis=BASIS_UNRESOLVED_TAG if device.tag is not None else BASIS_NONE,
            source_layers=layers,
            confidence=None,
            competing_type_names=(),
        )

    # Priority 2 — tag.text in by_abbreviation (normalized: strip + upper for lookup only).
    normalized_tag = device.tag.text.strip().upper() if device.tag is not None else None
    if normalized_tag is not None and normalized_tag in by_abbr:
        entry = by_abbr[normalized_tag]
        layers = _source_layers(device, include_tag_layer=True)
        return DeviceIdentity(
            entity_id=device.entity_id,
            kind=KIND_DEVICE,
            status=STATUS_RESOLVED,
            type_name=entry.type_name,
            abbreviation=entry.abbreviation,
            description=entry.description,
            basis=BASIS_TAG_ABBREVIATION,
            source_layers=layers,
            confidence=None,
            competing_type_names=entry.competing_type_names,
        )

    # Priority 3 — tag present but unmatched.
    if device.tag is not None:
        layers = _source_layers(device, include_tag_layer=True)
        return DeviceIdentity(
            entity_id=device.entity_id,
            kind=KIND_DEVICE,
            status=STATUS_UNRESOLVED,
            type_name=None,
            abbreviation=device.tag.text,  # raw tag preserved; NEVER fabricate a type
            description=None,
            basis=BASIS_UNRESOLVED_TAG,
            source_layers=layers,
            confidence=None,
            competing_type_names=(),
        )

    # Priority 4 — no tag, no family match.
    layers = _source_layers(device)
    return DeviceIdentity(
        entity_id=device.entity_id,
        kind=KIND_DEVICE,
        status=STATUS_UNKNOWN,
        type_name=None,
        abbreviation=None,
        description=None,
        basis=BASIS_NONE,
        source_layers=layers,
        confidence=None,
        competing_type_names=(),
    )


def _source_layers(device: Device, *, include_tag_layer: bool = False) -> tuple[str, ...]:
    """Collect deduped, deterministic source layers for a device (and optionally its tag)."""
    seen: list[str] = []
    if device.layer_ref is not None and device.layer_ref not in seen:
        seen.append(device.layer_ref)
    if (
        include_tag_layer
        and device.tag is not None
        and device.tag.layer_ref is not None
        and device.tag.layer_ref not in seen
    ):
        seen.append(device.tag.layer_ref)
    return tuple(seen)
