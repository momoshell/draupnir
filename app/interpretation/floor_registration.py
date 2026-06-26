"""N-member floor co-registration coordinator (issue #714).

Accepts a reference revision and any number of member revisions, co-registers each
member INTO the reference frame via shared grid fiducials, and returns a
:class:`FloorRegistration` summary with per-member success/failure details.

Pure module: stdlib + app.interpretation.grid_registration only.
No DB/ORM/FastAPI/numpy/cv2/shapely imports.

Architecture
------------
- The caller (a DB seam loader) fetches fiducials for each revision and passes
  them here.  All DB access is isolated in ``floor_registration_loaders``.
- Registration orientation: ``estimate_grid_transform(member, reference)`` —
  each member is registered INTO the reference frame (source=member, target=reference).
- The reference revision is always included in ``members_ok`` with an identity
  transform produced by ``estimate_grid_transform(reference_fiducials, reference_fiducials)``.
  This gives an honest quality signal rather than forging a hardcoded "good".
- Fail-closed gate per member: ``quality != "good"`` or ``|rotation_rad| > rot_tol_rad``
  or ``|scale - 1| > scale_tol``.  The scale clause is belt-and-suspenders: a bad scale
  already prevents ``quality == "good"``, but an explicit check mirrors the cable coordinator's
  documented defence against ``GridTransform.apply()`` being translation-only (it ignores scale
  so a mismatch would silently mis-place every vertex and produce confident-wrong results).
  A failed member is appended to ``members_failed``; other members are unaffected.
- HARD failures (raise ValueError): any revision id not found in DB (enforced by
  the loader); members spanning >1 project; reference with no resolvable fiducials.
- SOFT failure: a non-reference member with no fiducials is dropped to members_failed.
- Determinism: ``members_ok`` and ``members_failed`` are sorted by revision_id (str) ascending.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from app.interpretation.grid_registration import (
    GridFiducial,
    GridTransform,
    estimate_grid_transform,
)

# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FloorMember:
    """Input descriptor for one floor member revision."""

    revision_id: UUID
    role: str  # caller-defined label, e.g. "containment", "power"


@dataclass(frozen=True, slots=True)
class RegisteredMember:
    """A member revision that registered successfully."""

    revision_id: UUID
    role: str
    transform: GridTransform
    audit: dict[str, object]


@dataclass(frozen=True, slots=True)
class FailedMember:
    """A member revision that failed the registration gate."""

    revision_id: UUID
    role: str
    reason: str


@dataclass(frozen=True, slots=True)
class FloorRegistration:
    """Result of a multi-member floor co-registration.

    Notes on reference quality
    --------------------------
    The reference revision's :class:`RegisteredMember` entry in ``members_ok`` is
    produced by registering the reference onto itself via
    ``estimate_grid_transform(reference_fiducials, reference_fiducials)``.  Its
    ``transform.quality`` therefore reflects the *richness of the reference's own
    fiducial set* (how many grid bubbles were resolved), NOT registration success.
    With fewer than 3 fiducials the reference entry may read ``quality="degraded"``
    even though it defines the frame.  Downstream consumers iterating ``members_ok``
    and trusting ``quality=="good"`` should treat the reference entry separately
    or be aware of this distinction.
    """

    reference_revision_id: UUID
    reference_role: str
    members_ok: tuple[RegisteredMember, ...]
    members_failed: tuple[FailedMember, ...]


# ---------------------------------------------------------------------------
# Composer
# ---------------------------------------------------------------------------


def _build_audit(
    transform: GridTransform,
    *,
    revision_id: UUID,
    role: str,
    reference_revision_id: UUID,
) -> dict[str, object]:
    """Build the per-member audit dict with the exact required keys."""
    return {
        "dx": transform.dx,
        "dy": transform.dy,
        "rotation_rad": transform.rotation_rad,
        "scale": transform.scale,
        "matched_count": transform.matched_count,
        "matched_labels": list(transform.matched_labels),
        "median_residual_m": transform.median_residual_m,
        "max_residual_m": transform.max_residual_m,
        "quality": transform.quality,
        "revision_id": str(revision_id),
        "role": role,
        "reference_revision_id": str(reference_revision_id),
    }


def compose_floor_registration(
    *,
    reference_revision_id: UUID,
    reference_role: str,
    reference_fiducials: list[GridFiducial],
    members: list[tuple[FloorMember, list[GridFiducial]]],
    rot_tol_rad: float = 0.02,
    scale_tol: float = 0.02,
) -> FloorRegistration:
    """Compose a :class:`FloorRegistration` from pre-loaded fiducials.

    Parameters
    ----------
    reference_revision_id:
        UUID of the reference revision that defines the registration frame.
    reference_role:
        Caller-defined role label for the reference revision.
    reference_fiducials:
        Fiducials loaded for the reference revision.  Must be non-empty (hard failure
        enforced by the caller/loader).
    members:
        Sequence of (FloorMember, fiducials) pairs — MAY be empty (reference-only output).
        The reference revision must NOT appear in this list; it is added automatically.
    rot_tol_rad:
        Maximum tolerated rotation (radians) for a member to be classified as OK.
    scale_tol:
        Maximum tolerated scale deviation from 1.0 for a member to be classified as OK.
        Belt-and-suspenders defence: ``GridTransform.apply()`` is translation-only so a
        scale mismatch would silently mis-place every vertex and produce confident-wrong
        results.  A bad scale already prevents ``quality=="good"``, but this explicit
        clause mirrors the cable coordinator's documented guard.

    Returns
    -------
    FloorRegistration
        ``members_ok`` and ``members_failed`` are sorted by revision_id (str) ascending
        for determinism regardless of input order.

    Raises
    ------
    ValueError
        If ``reference_fiducials`` is empty (hard failure — caller must ensure non-empty).
    """
    if not reference_fiducials:
        raise ValueError(
            f"Reference revision {reference_revision_id} has no resolvable grid fiducials"
        )

    members_ok: list[RegisteredMember] = []
    members_failed: list[FailedMember] = []

    # --- Register reference onto itself (honest quality) ---
    ref_transform = estimate_grid_transform(reference_fiducials, reference_fiducials)
    ref_audit = _build_audit(
        ref_transform,
        revision_id=reference_revision_id,
        role=reference_role,
        reference_revision_id=reference_revision_id,
    )
    members_ok.append(
        RegisteredMember(
            revision_id=reference_revision_id,
            role=reference_role,
            transform=ref_transform,
            audit=ref_audit,
        )
    )

    # --- Register each non-reference member ---
    for member, fiducials in members:
        # Soft failure: member has no fiducials → dropped, not raised.
        if not fiducials:
            members_failed.append(
                FailedMember(
                    revision_id=member.revision_id,
                    role=member.role,
                    reason="no_fiducials",
                )
            )
            continue

        transform = estimate_grid_transform(fiducials, reference_fiducials)

        registration_failed = (
            (transform.quality != "good")
            or (abs(transform.rotation_rad) > rot_tol_rad)
            or (abs(transform.scale - 1.0) > scale_tol)
        )

        if registration_failed:
            members_failed.append(
                FailedMember(
                    revision_id=member.revision_id,
                    role=member.role,
                    reason=f"quality={transform.quality} rotation_rad={transform.rotation_rad:.6f}",
                )
            )
        else:
            audit = _build_audit(
                transform,
                revision_id=member.revision_id,
                role=member.role,
                reference_revision_id=reference_revision_id,
            )
            members_ok.append(
                RegisteredMember(
                    revision_id=member.revision_id,
                    role=member.role,
                    transform=transform,
                    audit=audit,
                )
            )

    # Deterministic sort by revision_id str ascending.
    members_ok.sort(key=lambda m: str(m.revision_id))
    members_failed.sort(key=lambda m: str(m.revision_id))

    return FloorRegistration(
        reference_revision_id=reference_revision_id,
        reference_role=reference_role,
        members_ok=tuple(members_ok),
        members_failed=tuple(members_failed),
    )
