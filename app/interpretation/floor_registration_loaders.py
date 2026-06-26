"""DB seam for N-member floor co-registration (issue #714).

Loads DrawingRevision rows to validate project membership, loads grid fiducials for
each revision via :func:`load_grid_fiducials`, then delegates pure computation to
:func:`compose_floor_registration`.

All ORM access lives here; the pure composer in :mod:`app.interpretation.floor_registration`
has no DB/ORM imports.

Hard failures (raise ValueError)
---------------------------------
- Any revision id not found in the database.
- Members spanning more than one project.
- Reference revision has no resolvable grid fiducials.

Soft failures (FailedMember in result)
----------------------------------------
- A non-reference member with no resolvable grid fiducials.
- A non-reference member whose fiducial load raises (malformed geometry_json, DB hiccup, etc.)
  — degraded to empty-fiducials so the composer routes it to members_failed with reason
  ``load_error``.
- A member whose transform quality != "good", |rotation_rad| > rot_tol_rad, or
  |scale - 1| > scale_tol.
"""

from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.interpretation.floor_registration import (
    FailedMember,
    FloorMember,
    FloorRegistration,
    compose_floor_registration,
)
from app.interpretation.grid_registration import GridFiducial
from app.interpretation.grid_registration_loaders import load_grid_fiducials
from app.models.drawing_revision import DrawingRevision

_log = logging.getLogger(__name__)


async def load_floor_registration(
    db: AsyncSession,
    *,
    reference_revision_id: UUID,
    members: list[FloorMember],
    reference_role: str,
    rot_tol_rad: float = 0.02,
    scale_tol: float = 0.02,
    grid_layer_tokens: tuple[str, ...] = ("z030g", "grid"),
    label_radius_m: float = 0.5,
) -> FloorRegistration:
    """Load revisions, co-register members INTO the reference frame, and return results.

    Parameters
    ----------
    db:
        Async SQLAlchemy session.
    reference_revision_id:
        UUID of the reference revision — defines the registration frame.
    members:
        List of :class:`FloorMember` for each non-reference revision to register.
        May be empty (reference-only output).
    reference_role:
        Caller-defined label for the reference revision (e.g. "lighting").
    rot_tol_rad:
        Maximum tolerated rotation (radians) for the fail-closed gate.
    scale_tol:
        Maximum tolerated scale deviation from 1.0 for the fail-closed gate.
    grid_layer_tokens:
        Layer name tokens forwarded to :func:`load_grid_fiducials`.
    label_radius_m:
        Label-proximity radius forwarded to :func:`load_grid_fiducials`.

    Returns
    -------
    FloorRegistration

    Raises
    ------
    ValueError
        If any revision id is not found, members span >1 project, or the reference
        revision has no resolvable grid fiducials.
    """
    # Collect all revision ids to query in one shot.
    all_revision_ids: list[UUID] = [reference_revision_id, *(m.revision_id for m in members)]

    rev_rows = (
        await db.execute(
            select(DrawingRevision.id, DrawingRevision.project_id).where(
                DrawingRevision.id.in_(all_revision_ids)
            )
        )
    ).all()

    project_by_rev: dict[UUID, UUID] = {row.id: row.project_id for row in rev_rows}

    # --- Hard failure: any revision not found ---
    missing = [str(rid) for rid in all_revision_ids if rid not in project_by_rev]
    if missing:
        raise ValueError(f"DrawingRevision(s) not found: {missing}")

    # --- Hard failure: members span more than one project ---
    projects = set(project_by_rev.values())
    if len(projects) > 1:
        raise ValueError(
            f"Revisions belong to different projects: "
            f"{', '.join(str(p) for p in sorted(projects, key=str))}"
        )

    # --- Load reference fiducials ---
    reference_fiducials = await load_grid_fiducials(
        db,
        reference_revision_id,
        grid_layer_tokens=grid_layer_tokens,
        label_radius_m=label_radius_m,
    )
    # Hard failure: reference has no fiducials — checked by compose_floor_registration.

    # --- Load member fiducials (per-member isolation: load errors are soft failures) ---
    # If load_grid_fiducials raises for one member (malformed geometry_json, decode error,
    # transient DB hiccup), we must NOT propagate the exception and abort the whole
    # registration — other members are unaffected.  Degrade to empty fiducials so the
    # composer routes this member to members_failed with reason "load_error".
    pre_failed: list[FailedMember] = []
    members_with_fiducials: list[tuple[FloorMember, list[GridFiducial]]] = []
    for member in members:
        try:
            fiducials = await load_grid_fiducials(
                db,
                member.revision_id,
                grid_layer_tokens=grid_layer_tokens,
                label_radius_m=label_radius_m,
            )
            members_with_fiducials.append((member, fiducials))
        except Exception as exc:
            _log.warning(
                "load_grid_fiducials raised for member %s (role=%s); degrading to soft failure: %s",
                member.revision_id,
                member.role,
                exc,
            )
            pre_failed.append(
                FailedMember(
                    revision_id=member.revision_id,
                    role=member.role,
                    reason=f"load_error: {type(exc).__name__}",
                )
            )

    # --- Delegate to pure composer ---
    result = compose_floor_registration(
        reference_revision_id=reference_revision_id,
        reference_role=reference_role,
        reference_fiducials=reference_fiducials,
        members=members_with_fiducials,
        rot_tol_rad=rot_tol_rad,
        scale_tol=scale_tol,
    )

    if not pre_failed:
        return result

    # Merge pre_failed into result.members_failed (sorted by revision_id str ascending).
    merged_failed = sorted(
        list(result.members_failed) + pre_failed,
        key=lambda m: str(m.revision_id),
    )
    return FloorRegistration(
        reference_revision_id=result.reference_revision_id,
        reference_role=result.reference_role,
        members_ok=result.members_ok,
        members_failed=tuple(merged_failed),
    )
