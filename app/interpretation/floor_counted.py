"""Per-room device count fusion — COUNTED (device) contribution (issue #719, Phase R-E).

Pure module: stdlib + app.interpretation.{devices,device_identity,room_fusion} ONLY.
No cv2, skimage, DB, or FastAPI.

``bucket_counted_devices`` takes a sequence of per-member (source_revision_id, GridTransform,
TypedDevice sequence) tuples, transforms each device anchor into the reference frame, classifies
it against the ``RoomRegistry``, and returns a ``CountedFusionResult`` summarising:

* Per-(type, room) counts  (``counts_by_room``).
* The full device→room assignment map  (``assignments``).
* Counts of excluded non-device kinds  (``excluded_kinds``).

Determinism
-----------
Members are processed in ``source_revision_id`` ascending order (caller must pre-sort OR pass
already-sorted; ``bucket_counted_devices`` sorts internally).  Within a member devices are
processed in input order.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.device_identity import (
    KIND_ANNOTATION,
    KIND_ARCHITECTURE,
    KIND_LEGEND_EXEMPLAR,
)
from app.interpretation.devices import TypedDevice
from app.interpretation.grid_registration import GridTransform
from app.interpretation.room_fusion import RoomRegistry

# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CountedDeviceAssignment:
    """The resolved room assignment for one counted device instance.

    This is the per-device record consumed by R-F (#720) to build the device→room map.
    Fields are locked — downstream specs depend on them.

    ``room_number`` and ``room_id`` are ``None`` when the device is unassigned (outside
    every polygon and Voronoi envelope) or has no position (``position is None``).
    ``needs_review`` is propagated from ``RoomAssignment`` or set ``True`` when position is
    missing (honest — never silently dropped).
    """

    device_id: str
    type_name: str
    source_revision_id: str
    room_number: str | None
    room_id: str | None
    boundary_basis: str | None
    confidence: float | None
    needs_review: bool


@dataclass(frozen=True, slots=True)
class CountedFusionResult:
    """Result of fusing one floor's counted devices across all registered members.

    ``assignments`` — one entry per counted device (KIND_DEVICE, incl. "unresolved").
    ``counts_by_room`` — nested dict: room_number (or None) → {type_name: count}.
    ``excluded_kinds`` — tally of devices excluded from counting (architecture / annotation /
      legend_exemplar), keyed by their bucketed label.
    """

    assignments: tuple[CountedDeviceAssignment, ...]
    counts_by_room: dict[str | None, dict[str, int]]
    excluded_kinds: dict[str, int]

    def by_device_id(self) -> dict[str, CountedDeviceAssignment]:
        """Return a ``device_id → CountedDeviceAssignment`` lookup dict."""
        return {a.device_id: a for a in self.assignments}


# ---------------------------------------------------------------------------
# Exclusion-kind labels (stable; used as keys in excluded_kinds)
# ---------------------------------------------------------------------------

_EXCLUDED_KIND_LABEL: dict[str, str] = {
    KIND_ARCHITECTURE: "architecture",
    KIND_ANNOTATION: "annotation",
    KIND_LEGEND_EXEMPLAR: "legend_exemplar",
}

# Any TypedDevice whose type_name is "architecture" is also excluded (belt-and-suspenders —
# resolve_typed_devices already sets kind=KIND_ARCHITECTURE for those, but the label check
# covers both paths).
_EXCLUDED_TYPE_NAMES: frozenset[str] = frozenset({"architecture"})


# ---------------------------------------------------------------------------
# Pure fusion function
# ---------------------------------------------------------------------------


def bucket_counted_devices(
    members: Sequence[tuple[str, GridTransform, Sequence[TypedDevice]]],
    registry: RoomRegistry,
) -> CountedFusionResult:
    """Classify every counted device into a room and aggregate counts.

    Parameters
    ----------
    members:
        Sequence of ``(source_revision_id, transform, typed_devices)`` tuples.
        Members are sorted internally by ``source_revision_id`` for determinism.
    registry:
        Frozen ``RoomRegistry`` used for point-in-polygon + Voronoi classification.

    Returns
    -------
    CountedFusionResult
        ``assignments`` is in deterministic order (by member revision_id, then device
        input order).  ``counts_by_room`` is keyed by ``room_number`` (``None`` for
        unassigned).  ``excluded_kinds`` tallies KIND_ARCHITECTURE / KIND_ANNOTATION /
        KIND_LEGEND_EXEMPLAR devices that are excluded from the counting denominator.
    """
    # Deterministic member order.
    sorted_members = sorted(members, key=lambda m: m[0])

    assignments: list[CountedDeviceAssignment] = []
    counts_by_room: dict[str | None, dict[str, int]] = {}
    excluded_kinds: dict[str, int] = {}

    for source_revision_id, transform, typed_devices in sorted_members:
        for td in typed_devices:
            # --- Exclusion gate ---
            excluded_label = _EXCLUDED_KIND_LABEL.get(td.kind)
            if excluded_label is None and td.type_name in _EXCLUDED_TYPE_NAMES:
                # architecture type_name without matching kind (shouldn't happen in practice,
                # but guard it so the rule is complete).
                excluded_label = "architecture"
            if excluded_label is not None:
                excluded_kinds[excluded_label] = excluded_kinds.get(excluded_label, 0) + 1
                continue

            # --- No-position path (honest; never dropped) ---
            if td.position is None:
                assignments.append(
                    CountedDeviceAssignment(
                        device_id=td.device_id,
                        type_name=td.type_name,
                        source_revision_id=source_revision_id,
                        room_number=None,
                        room_id=None,
                        boundary_basis=None,
                        confidence=None,
                        needs_review=True,
                    )
                )
                _increment_counts(counts_by_room, room_number=None, type_name=td.type_name)
                continue

            # --- Apply transform → classify ---
            world_x, world_y = transform.apply((td.position["x"], td.position["y"]))
            assignment = registry.classify((world_x, world_y))

            assignments.append(
                CountedDeviceAssignment(
                    device_id=td.device_id,
                    type_name=td.type_name,
                    source_revision_id=source_revision_id,
                    room_number=assignment.room_number,
                    room_id=assignment.room_id,
                    boundary_basis=assignment.boundary_basis,
                    confidence=assignment.confidence,
                    needs_review=assignment.needs_review,
                )
            )
            _increment_counts(
                counts_by_room, room_number=assignment.room_number, type_name=td.type_name
            )

    return CountedFusionResult(
        assignments=tuple(assignments),
        counts_by_room=counts_by_room,
        excluded_kinds=excluded_kinds,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _increment_counts(
    counts_by_room: dict[str | None, dict[str, int]],
    *,
    room_number: str | None,
    type_name: str,
) -> None:
    """Increment the count for ``(room_number, type_name)`` in-place."""
    room_bucket = counts_by_room.setdefault(room_number, {})
    room_bucket[type_name] = room_bucket.get(type_name, 0) + 1
