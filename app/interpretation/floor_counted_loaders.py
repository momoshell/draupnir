"""DB seam for the COUNTED (device) floor fusion (issue #719, Phase R-E).

Single entry point: ``load_counted_fusion`` fetches typed devices per registered member,
applies per-member isolation, and delegates to the pure ``bucket_counted_devices``.

The ``RoomRegistry`` is passed in by the caller — this module does NOT build it (separation
of concerns: the registry may be shared across fusion kinds R-E / R-F / R-G).

``app.api`` imports are lazy (inside function bodies) to avoid an interpretation→api cycle
(#705 class pattern).  ``load_typed_devices`` lives in ``app.interpretation.devices`` (no
api dependency) so it is imported at module level — this makes it patchable by tests.
"""

from __future__ import annotations

import logging
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.devices import DEFAULT_TAG_MAX_DISTANCE_M, TypedDevice, load_typed_devices
from app.interpretation.floor_counted import CountedFusionResult, bucket_counted_devices
from app.interpretation.floor_registration import FloorRegistration
from app.interpretation.grid_registration import GridTransform
from app.interpretation.room_fusion import RoomRegistry

_log = logging.getLogger(__name__)

_MAX_NESTING_DEPTH = 8


async def load_counted_fusion(
    db: AsyncSession,
    registration: FloorRegistration,
    registry: RoomRegistry,
    *,
    scope: Literal["sheet", "modelspace"] = "sheet",
    max_depth: int = _MAX_NESTING_DEPTH,
    device_layer: list[str] | None = None,
    tag_layer: list[str] | None = None,
    max_tag_distance: float | None = DEFAULT_TAG_MAX_DISTANCE_M,
) -> CountedFusionResult:
    """Load typed devices for each registered member and return the fused result.

    Per-member isolation: if loading a member's devices raises, that member is skipped
    (logged at WARNING) and other members are unaffected.  An empty-members input or all
    members failing returns an empty ``CountedFusionResult`` without raising.

    Parameters
    ----------
    db:
        Async DB session (read-only usage).
    registration:
        Floor registration result from ``compose_floor_registration`` / the registration
        loader.  Only ``members_ok`` are processed; ``members_failed`` are ignored.
    registry:
        Frozen ``RoomRegistry`` for point classification (built externally and shared).
    scope:
        ``"sheet"`` (default) restricts the device walk to the printed sheet;
        ``"modelspace"`` walks the full modelspace.
    max_depth:
        Maximum block-nesting depth for the device walk (default 8).
    device_layer:
        Optional layer-ref filter for the device walk (forwarded to ``load_typed_devices``).
    tag_layer:
        Optional layer-ref filter for tag association (forwarded to ``load_typed_devices``).
    max_tag_distance:
        Max distance for tag association (forwarded to ``load_typed_devices``).
        Defaults to ``DEFAULT_TAG_MAX_DISTANCE_M`` (2.0 m cap, #768).
    """
    collected: list[tuple[str, GridTransform, list[TypedDevice]]] = []

    for member in registration.members_ok:
        revision_id_str = str(member.revision_id)
        try:
            typed = await load_typed_devices(
                db,
                member.revision_id,
                scope=scope,
                max_depth=max_depth,
                device_layers=device_layer,
                tag_layer=tag_layer,
                max_tag_distance=max_tag_distance,
            )
        except Exception:
            _log.warning(
                "floor_counted: skipping member %s — load_typed_devices raised",
                revision_id_str,
                exc_info=True,
            )
            continue

        collected.append((revision_id_str, member.transform, typed))

    # bucket_counted_devices sorts by source_revision_id internally.
    return bucket_counted_devices(collected, registry)
