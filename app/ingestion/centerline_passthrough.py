"""Passthrough centerline producer — cv2/skimage/shapely-free.

Emits one :class:`~app.ingestion.centerline_contract.Centerline` per
:class:`~app.interpretation.routed_runs.RunGroup` using the same geometry-sum
rule as the service-takeoff coordinator.  The passthrough producer carries
``producer_kind="passthrough"`` and is the LP1 (C0) implementation; real
raster/DWG producers are introduced in #640/#641.

No heavy vision imports.  Safe to use on the read path.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from app.ingestion.centerline_contract import (
    CURRENT_ALGO_VERSION,
    Centerline,
    CenterlineGeometry,
    entity_group_drawn_length,
)
from app.interpretation.routed_runs import RunGroup

# SHA-256 of "{}" — fixed sentinel for the passthrough producer (no raster params).
_PASSTHROUGH_RASTER_PARAMS_HASH: str = hashlib.sha256(b"{}").hexdigest()


def passthrough_centerlines(
    run_groups: Sequence[RunGroup],
    geometry_by_entity_id: Mapping[str, Mapping[str, Any]],
) -> list[Centerline]:
    """Produce one :class:`Centerline` per group using drawn-entity lengths.

    Parameters
    ----------
    run_groups:
        P1 RunGroup objects.  Each group defines a (layer_ref, colour_key) key
        and the set of member entity IDs whose drawn length is summed.
    geometry_by_entity_id:
        Geometry dict keyed by entity ID.  Missing entity IDs contribute 0 to
        the length sum (tolerant).

    Returns
    -------
    list[Centerline]
        One ``Centerline`` per group, ordered the same as ``run_groups``.
        The ``geometry.polylines`` field is empty for the passthrough producer —
        skeletal tracing is deferred to LP2+ producers.
    """
    results: list[Centerline] = []
    for group in run_groups:
        entity_ids: tuple[str, ...] = tuple(group.entity_ids)
        length_du = entity_group_drawn_length(entity_ids, geometry_by_entity_id)
        results.append(
            Centerline(
                layer_ref=group.layer_ref,
                colour_key=group.colour_key,
                geometry=CenterlineGeometry(polylines=(), length_du=length_du),
                entity_count=len(entity_ids),
                algo_version=CURRENT_ALGO_VERSION,
                raster_params_hash=_PASSTHROUGH_RASTER_PARAMS_HASH,
                producer_kind="passthrough",
            )
        )
    return results
