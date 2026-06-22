"""Routed-run grouping coordinator (issue #606, Phase 1 / be-02).

A thin pure coordinator that groups routed linework (line/polyline/arc) by
``(layer_ref, colour_key)`` and resolves each group's discipline against a
:class:`~app.interpretation.service_legend.ServiceLegend`.

Descending authority / graceful-degradation shape:

1. Filter entities to routed types (``ROUTED_ENTITY_TYPES``).
2. Group by ``(layer_ref, colour_key(color))`` — exactly ONE :class:`RunGroup` per
   distinct key; None values are valid key parts (honest, never dropped).
3. Resolve via ``legend.by_colour().get(colour_key)``:
   - match  → ``status=RESOLVED``, discipline, ``basis=BASIS_LEGEND_COLOUR``.
   - no match (or colour_key is None) → ``status=UNKNOWN``, raw values preserved,
     ``basis=BASIS_UNRESOLVED``.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports. Unit-testable with fakes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from app.interpretation.service_legend import ServiceLegend, colour_key

# ---------------------------------------------------------------------------
# Constants — frozen interface contract
# ---------------------------------------------------------------------------

ROUTED_ENTITY_TYPES: tuple[str, ...] = ("line", "polyline", "arc")
STATUS_RESOLVED: str = "resolved"
STATUS_UNKNOWN: str = "unknown"
BASIS_LEGEND_COLOUR: str = "legend_colour"
BASIS_UNRESOLVED: str = "unresolved"

# ---------------------------------------------------------------------------
# Input dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RoutedEntity:
    """One routed linework entity from the drawing.

    ``color`` mirrors the resolved ``style.color`` mapping produced by the adapters
    (``{index, rgb, by_layer, by_block}``).  ``geometry`` is carried opaquely for
    Phase 3 measurement; it is NOT consumed here.
    """

    entity_id: str
    entity_type: str  # filtered against ROUTED_ENTITY_TYPES
    layer_ref: str | None
    color: Mapping[str, Any] | None  # resolved style.color {index,rgb,by_layer,by_block}
    geometry: Mapping[str, Any] | None  # carried opaquely for P3; NOT used in P1


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunGroup:
    """One discipline-resolved (or honestly unresolved) run group.

    ``entity_ids`` lists members in input order; Phase 3 will measure them.
    ``confidence=None`` means not-scored / face-value (mirroring the rooms convention);
    it additionally signals a conflicted legend match when ``competing_disciplines`` is
    non-empty.
    """

    layer_ref: str | None
    colour_key: str | None
    colour_index: int | None
    colour_rgb: str | None
    status: str  # STATUS_RESOLVED | STATUS_UNKNOWN
    discipline: str | None
    basis: str  # BASIS_LEGEND_COLOUR | BASIS_UNRESOLVED
    source_layers: tuple[str, ...]
    confidence: float | None  # None = face-value / conflicted-legend marker
    competing_disciplines: tuple[str, ...]
    entity_ids: tuple[str, ...]  # members in input order (P3 measures these)


@dataclass(frozen=True, slots=True)
class RoutedRunResult:
    """Immutable result of routed-run identification for one revision."""

    groups: tuple[RunGroup, ...]  # deterministic order


# ---------------------------------------------------------------------------
# Public coordinator
# ---------------------------------------------------------------------------


def identify_routed_runs(
    entities: Sequence[RoutedEntity], legend: ServiceLegend
) -> RoutedRunResult:
    """Group routed linework by ``(layer_ref, colour_key)`` and resolve disciplines.

    Pure over :class:`RoutedEntity` and :class:`ServiceLegend` — no DB, no geometry
    processing.  Never raises on degenerate or empty input.

    The result is deterministic: groups are sorted by ``(layer_ref or "", colour_key
    or "")``, invariant under any permutation of ``entities``.
    """
    if not entities:
        return RoutedRunResult(groups=())

    # Step 1 — filter to routed entity types.
    routed = [e for e in entities if e.entity_type in ROUTED_ENTITY_TYPES]
    if not routed:
        return RoutedRunResult(groups=())

    # Step 2 — group by (layer_ref, colour_key); preserve input order within each group.
    # Use a list to keep insertion order for deterministic entity_ids.
    groups_map: dict[tuple[str | None, str | None], list[RoutedEntity]] = {}
    for entity in routed:
        ck = colour_key(entity.color)
        key: tuple[str | None, str | None] = (entity.layer_ref, ck)
        if key not in groups_map:
            groups_map[key] = []
        groups_map[key].append(entity)

    # Step 3 — resolve each group against the legend and build RunGroup objects.
    colour_index_map = legend.by_colour()
    run_groups: list[RunGroup] = []

    for (layer_ref, ck), members in groups_map.items():
        # Extract colour index/rgb from the first member's color mapping.
        first_color = members[0].color
        raw_index: int | None = None
        raw_rgb: str | None = None
        if first_color is not None:
            idx_val = first_color.get("index")
            rgb_val = first_color.get("rgb")
            try:
                raw_index = int(str(idx_val)) if idx_val is not None else None
            except (ValueError, TypeError):
                raw_index = None
            raw_rgb = str(rgb_val).lower() if rgb_val is not None else None

        source_layers: tuple[str, ...] = (layer_ref,) if layer_ref is not None else ()
        entity_ids: tuple[str, ...] = tuple(e.entity_id for e in members)

        # Resolve via legend.
        entry = colour_index_map.get(ck) if ck is not None else None

        if entry is not None:
            run_groups.append(
                RunGroup(
                    layer_ref=layer_ref,
                    colour_key=ck,
                    colour_index=raw_index,
                    colour_rgb=raw_rgb,
                    status=STATUS_RESOLVED,
                    discipline=entry.discipline,
                    basis=BASIS_LEGEND_COLOUR,
                    source_layers=source_layers,
                    confidence=None,
                    competing_disciplines=entry.competing_disciplines,
                    entity_ids=entity_ids,
                )
            )
        else:
            run_groups.append(
                RunGroup(
                    layer_ref=layer_ref,
                    colour_key=ck,
                    colour_index=raw_index,
                    colour_rgb=raw_rgb,
                    status=STATUS_UNKNOWN,
                    discipline=None,
                    basis=BASIS_UNRESOLVED,
                    source_layers=source_layers,
                    confidence=None,
                    competing_disciplines=(),
                    entity_ids=entity_ids,
                )
            )

    # Step 4 — deterministic sort: (layer_ref or "", colour_key or "").
    run_groups.sort(key=lambda g: (g.layer_ref or "", g.colour_key or ""))

    return RoutedRunResult(groups=tuple(run_groups))
