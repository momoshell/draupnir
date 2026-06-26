"""DB seam for the fused per-room measured takeoff coordinator (issue #718, Phase R-D).

Loads persisted centerline geometry for each registered member revision, transforms
vertices into the reference frame, resolves service identities via the standard
identity chain, then delegates to
:func:`~app.interpretation.floor_measured.bucket_measured_for_member`.

Per-member isolation: a load error on any single member is caught; that member is
skipped and other members are unaffected.

app.api imports are LAZY (inside function bodies) to prevent the interpretation→api
import cycle that crashed the celery worker before #705.
"""

from __future__ import annotations

import logging
from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.floor_measured import (
    SERVICE_UNKNOWN,
    FusedMeasuredItem,
    FusedMeasuredResult,
    _item_sort_key,
    bucket_measured_for_member,
)
from app.interpretation.floor_registration import FloorRegistration
from app.interpretation.room_fusion import RoomRegistry
from app.interpretation.routed_runs import identify_routed_runs
from app.interpretation.run_service_identity import fuse_run_service_identities
from app.interpretation.service_takeoff_loaders import (
    load_measured_geometry,
    load_service_takeoff_inputs,
)

logger = logging.getLogger(__name__)


async def load_fused_measured(
    db: AsyncSession,
    *,
    registration: FloorRegistration,
    registry: RoomRegistry,
    scope: Literal["sheet", "modelspace"] = "sheet",
    radius: float = 5.0,
) -> FusedMeasuredResult:
    """Load and bucket persisted centerline geometry for all registered members.

    For each member in ``registration.members_ok``:

    1. Load persisted centerline polylines via ``load_measured_geometry``.
    2. Transform every vertex via ``member.transform.apply((x, y))`` into the
       reference coordinate frame.
    3. Resolve service identities for the member revision via the standard
       identity chain (``load_service_takeoff_inputs`` → ``identify_routed_runs``
       → ``fuse_run_service_identities``).
    4. Call ``bucket_measured_for_member`` and accumulate results.

    A load error on any single member is caught and that member is skipped;
    other members continue to be processed (per-member isolation).

    Parameters
    ----------
    db:
        Active async session.
    registration:
        Multi-member floor co-registration result from Phase R-A/B.
        Only ``members_ok`` are processed; ``members_failed`` are silently skipped.
    registry:
        Frozen :class:`~app.interpretation.room_fusion.RoomRegistry` (Phase R-C).
    scope:
        ``"sheet"`` (default) restricts to on-sheet entities; ``"modelspace"``
        includes all entities.
    radius:
        Tag-to-run association radius in metres (mirrors service-takeoff default).

    Returns
    -------
    FusedMeasuredResult
        All items from all members fused, sorted deterministically.
        ``unscaled=True`` when any item was unscaled.
    """
    exclude_off_sheet = scope == "sheet"

    all_assigned: list[FusedMeasuredItem] = []
    all_unassigned: list[FusedMeasuredItem] = []
    has_unscaled = False

    for member in registration.members_ok:
        revision_id: UUID = member.revision_id
        role: str = member.role

        try:
            # 1. Load persisted centerline geometry ({} if none — never raises).
            geom = await load_measured_geometry(db, revision_id)
            if not geom:
                continue

            # 2. Transform every vertex into the reference frame.
            transformed: dict[
                tuple[str | None, str | None],
                tuple[tuple[tuple[float, float], ...], ...],
            ] = {}
            for group_key, polylines in geom.items():
                tx_polylines: list[tuple[tuple[float, float], ...]] = []
                for pl in polylines:
                    tx_pl = tuple(member.transform.apply(pt) for pt in pl)
                    tx_polylines.append(tx_pl)
                transformed[group_key] = tuple(tx_polylines)

            # 3. Resolve service identities via the standard identity chain.
            inputs = await load_service_takeoff_inputs(
                db,
                revision_id,
                exclude_off_sheet=exclude_off_sheet,
            )
            runs = identify_routed_runs(inputs.routed_entities, inputs.legend).groups
            identities = fuse_run_service_identities(
                runs,
                inputs.geometry_by_entity_id,
                inputs.tag_placements,
                radius=radius,
            ).identities

            # Collapse identities to {group_key: ((service, size_raw, size_kind), ...)}
            services_by_group: dict[
                tuple[str | None, str | None],
                tuple[tuple[str, str | None, str | None], ...],
            ] = {}
            for ident in identities:
                key = (ident.layer_ref, ident.colour_key)
                if ident.services:
                    services_by_group[key] = tuple(
                        (ss.service, ss.size.raw, ss.size.kind) for ss in ident.services
                    )
                else:
                    services_by_group[key] = ((SERVICE_UNKNOWN, None, None),)

            # 4. Bucket and accumulate.
            scale = inputs.scale
            result = bucket_measured_for_member(
                member_revision_id=str(revision_id),
                role=role,
                polylines_by_group=transformed,
                services_by_group=services_by_group,
                registry=registry,
                scale=scale,
            )
            all_assigned.extend(result.items)
            all_unassigned.extend(result.unassigned)
            if result.unscaled:
                has_unscaled = True

        except Exception:
            # Per-member isolation: skip this member; others continue unaffected.
            logger.warning(
                "load_fused_measured: skipping member %s (role=%s) after error",
                member.revision_id,
                member.role,
                exc_info=True,
            )
            continue

    # Deterministic sort across all members.
    all_assigned.sort(key=_item_sort_key)
    all_unassigned.sort(key=_item_sort_key)

    return FusedMeasuredResult(
        items=tuple(all_assigned),
        unassigned=tuple(all_unassigned),
        unscaled=has_unscaled,
    )
