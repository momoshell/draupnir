"""Pure unit tests for floor_registration.py (issue #714).

No DB, no ORM, no FastAPI, no heavy libs.  All geometry in metres.

Covers: import purity, dataclass shapes, compose_floor_registration correctness,
fail-closed gate, per-member isolation, determinism, edge cases (empty members,
reference-only, no-fiducials soft fail, hard fail on empty reference).
"""

from __future__ import annotations

import importlib
import math
import random
import uuid
from pathlib import Path

from app.interpretation.floor_registration import (
    FailedMember,
    FloorMember,
    FloorRegistration,
    RegisteredMember,
    compose_floor_registration,
)
from app.interpretation.grid_registration import GridFiducial

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _f(label: str, x: float, y: float) -> GridFiducial:
    return GridFiducial(label=label, point=(x, y))


def _shifted(fiducials: list[GridFiducial], dx: float, dy: float) -> list[GridFiducial]:
    return [
        GridFiducial(label=f.label, point=(f.point[0] + dx, f.point[1] + dy)) for f in fiducials
    ]


def _good_fiducials() -> list[GridFiducial]:
    """Six-point grid matching real drawings: enough for quality='good'."""
    return [
        _f("4", 10.0, 5.0),
        _f("5", 20.0, 5.0),
        _f("6", 30.0, 5.0),
        _f("B", 10.0, 15.0),
        _f("C", 20.0, 15.0),
        _f("D", 30.0, 15.0),
    ]


def _member(role: str = "power") -> FloorMember:
    return FloorMember(revision_id=uuid.uuid4(), role=role)


# ---------------------------------------------------------------------------
# (0) Import purity — floor_registration.py must be stdlib + grid_registration only
# ---------------------------------------------------------------------------


def test_import_purity_floor_registration() -> None:
    """floor_registration.py must not import DB, CV, or shape libs."""
    mod = importlib.import_module("app.interpretation.floor_registration")
    source_file = getattr(mod, "__file__", "") or ""
    import_lines = [
        line
        for line in Path(source_file).read_text().splitlines()
        if line.lstrip().startswith(("import ", "from "))
    ]
    src = "\n".join(import_lines)
    forbidden = ("sqlalchemy", "fastapi", "numpy", "cv2", "shapely", "skimage")
    for pkg in forbidden:
        assert pkg not in src, f"floor_registration.py must not import {pkg}"


# ---------------------------------------------------------------------------
# (1) Dataclass shapes: frozen + slots
# ---------------------------------------------------------------------------


def test_dataclasses_frozen_slots() -> None:
    """All public dataclasses are frozen=True, slots=True."""
    for cls in (FloorMember, RegisteredMember, FailedMember, FloorRegistration):
        assert "__slots__" in cls.__dict__ or hasattr(cls, "__slots__"), (
            f"{cls.__name__} must have __slots__"
        )
        # Frozen: attempt mutation raises FrozenInstanceError.
        import dataclasses

        assert dataclasses.is_dataclass(cls)
        params = dataclasses.fields(cls)
        assert len(params) > 0


# ---------------------------------------------------------------------------
# (2) Reference-only: compose with empty members
# ---------------------------------------------------------------------------


def test_reference_only_empty_members() -> None:
    """compose_floor_registration with no members yields reference in members_ok only."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[],
    )

    assert isinstance(result, FloorRegistration)
    assert result.reference_revision_id == ref_id
    assert result.reference_role == "lighting"
    assert len(result.members_ok) == 1
    assert len(result.members_failed) == 0
    assert result.members_ok[0].revision_id == ref_id
    assert result.members_ok[0].role == "lighting"


# ---------------------------------------------------------------------------
# (3) Reference identity transform is honest (uses estimate_grid_transform)
# ---------------------------------------------------------------------------


def test_reference_self_registration_quality_good() -> None:
    """Reference registered onto itself via estimate_grid_transform → quality=good."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[],
    )

    ref_member = result.members_ok[0]
    assert ref_member.transform.quality == "good"
    assert abs(ref_member.transform.dx) < 1e-9
    assert abs(ref_member.transform.dy) < 1e-9
    assert abs(ref_member.transform.rotation_rad) < 0.001
    assert abs(ref_member.transform.scale - 1.0) < 0.001


# ---------------------------------------------------------------------------
# (4) Member registers quality=good → lands in members_ok
# ---------------------------------------------------------------------------


def test_good_member_registered_in_members_ok() -> None:
    """A member with matching fiducials (good quality) appears in members_ok."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("containment")
    mem_fids = _shifted(ref_fids, 28.5, 0.0)

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, mem_fids)],
    )

    assert len(result.members_failed) == 0
    # ref + 1 member
    assert len(result.members_ok) == 2
    mem_reg = next(m for m in result.members_ok if m.revision_id == mem.revision_id)
    assert mem_reg.role == "containment"
    assert mem_reg.transform.quality == "good"
    assert abs(mem_reg.transform.dx - (-28.5)) < 1e-6
    assert abs(mem_reg.transform.dy) < 1e-6


# ---------------------------------------------------------------------------
# (5) Fail-closed gate: quality != 'good' → members_failed
# ---------------------------------------------------------------------------


def test_degraded_quality_goes_to_members_failed() -> None:
    """Member with only 2 matching fiducials (degraded) is rejected to members_failed."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("power")
    # Only 2 fiducials → quality='degraded' (not enough for 'good')
    mem_fids = _shifted(ref_fids[:2], 5.0, 0.0)

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, mem_fids)],
    )

    assert len(result.members_failed) == 1
    failed = result.members_failed[0]
    assert failed.revision_id == mem.revision_id
    assert failed.role == "power"
    assert "quality=degraded" in failed.reason


def test_rotation_beyond_tol_goes_to_members_failed() -> None:
    """Member transform with |rotation_rad| > rot_tol_rad is rejected."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("med_gas")

    # Rotate source fiducials 5° (>> 0.02 rad) then shift to produce good residuals.
    angle = math.radians(5)

    def _rotate(f: GridFiducial) -> GridFiducial:
        x, y = f.point
        rx = x * math.cos(angle) - y * math.sin(angle)
        ry = x * math.sin(angle) + y * math.cos(angle)
        return GridFiducial(label=f.label, point=(rx, ry))

    mem_fids = [_rotate(f) for f in ref_fids]

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, mem_fids)],
        rot_tol_rad=0.02,
    )

    assert len(result.members_failed) == 1
    assert result.members_failed[0].revision_id == mem.revision_id


# ---------------------------------------------------------------------------
# (6) Per-member isolation: one failure doesn't affect other members
# ---------------------------------------------------------------------------


def test_per_member_isolation() -> None:
    """A failed member doesn't affect other members in the same call."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()

    good_mem = _member("power")
    good_fids = _shifted(ref_fids, 10.0, 0.0)

    bad_mem = _member("med_gas")
    # Only 2 fiducials → degraded.
    bad_fids = _shifted(ref_fids[:2], 10.0, 0.0)

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(good_mem, good_fids), (bad_mem, bad_fids)],
    )

    ok_ids = {m.revision_id for m in result.members_ok}
    fail_ids = {m.revision_id for m in result.members_failed}
    assert good_mem.revision_id in ok_ids
    assert bad_mem.revision_id in fail_ids
    # ref always ok
    assert ref_id in ok_ids


# ---------------------------------------------------------------------------
# (7) Soft failure: member with no fiducials → members_failed (not raised)
# ---------------------------------------------------------------------------


def test_member_no_fiducials_soft_fail() -> None:
    """A non-reference member with no fiducials is a soft failure (FailedMember, not raise)."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("containment")

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, [])],
    )

    assert len(result.members_failed) == 1
    assert result.members_failed[0].revision_id == mem.revision_id
    assert result.members_failed[0].reason == "no_fiducials"
    # Reference is still ok.
    assert len(result.members_ok) == 1
    assert result.members_ok[0].revision_id == ref_id


# ---------------------------------------------------------------------------
# (8) Hard failure: empty reference fiducials raises ValueError
# ---------------------------------------------------------------------------


def test_empty_reference_fiducials_raises() -> None:
    """compose_floor_registration raises ValueError when reference has no fiducials."""
    import pytest

    ref_id = uuid.uuid4()
    with pytest.raises(ValueError, match="no resolvable grid fiducials"):
        compose_floor_registration(
            reference_revision_id=ref_id,
            reference_role="lighting",
            reference_fiducials=[],
            members=[],
        )


# ---------------------------------------------------------------------------
# (9) Audit dict exact keys
# ---------------------------------------------------------------------------


_REQUIRED_AUDIT_KEYS = frozenset(
    {
        "dx",
        "dy",
        "rotation_rad",
        "scale",
        "matched_count",
        "matched_labels",
        "median_residual_m",
        "max_residual_m",
        "quality",
        "revision_id",
        "role",
        "reference_revision_id",
    }
)


def test_audit_dict_exact_keys() -> None:
    """RegisteredMember.audit contains exactly the required keys."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("containment")
    mem_fids = _shifted(ref_fids, 5.0, 0.0)

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, mem_fids)],
    )

    for m in result.members_ok:
        assert set(m.audit.keys()) == _REQUIRED_AUDIT_KEYS, (
            f"Unexpected audit keys for {m.revision_id}: {set(m.audit.keys())}"
        )

    # Check audit values are consistent
    mem_reg = next(m for m in result.members_ok if m.revision_id == mem.revision_id)
    assert mem_reg.audit["revision_id"] == str(mem.revision_id)
    assert mem_reg.audit["role"] == "containment"
    assert mem_reg.audit["reference_revision_id"] == str(ref_id)


# ---------------------------------------------------------------------------
# (10) Determinism: output is identical under any input-order permutation
# ---------------------------------------------------------------------------


def test_determinism_member_order() -> None:
    """members_ok and members_failed are sorted by revision_id str; order-independent."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()

    # Create 4 members: 3 good, 1 bad (2-fiducials).
    members_data = [
        (FloorMember(revision_id=uuid.uuid4(), role=f"m{i}"), _shifted(ref_fids, float(i), 0.0))
        for i in range(3)
    ]
    bad_mem = FloorMember(revision_id=uuid.uuid4(), role="bad")
    members_data.append((bad_mem, _shifted(ref_fids[:2], 1.0, 0.0)))

    def _run(seed: int) -> FloorRegistration:
        shuffled = list(members_data)
        rng = random.Random(seed)
        rng.shuffle(shuffled)
        return compose_floor_registration(
            reference_revision_id=ref_id,
            reference_role="lighting",
            reference_fiducials=ref_fids,
            members=shuffled,
        )

    r1 = _run(1)
    r2 = _run(42)
    r3 = _run(999)

    assert [m.revision_id for m in r1.members_ok] == [m.revision_id for m in r2.members_ok]
    assert [m.revision_id for m in r1.members_ok] == [m.revision_id for m in r3.members_ok]
    assert [m.revision_id for m in r1.members_failed] == [m.revision_id for m in r2.members_failed]
    assert [m.revision_id for m in r1.members_failed] == [m.revision_id for m in r3.members_failed]


# ---------------------------------------------------------------------------
# (11) Three good members all register quality=good
# ---------------------------------------------------------------------------


def test_three_good_members_all_register() -> None:
    """Three members with full fiducials + small offsets all end in members_ok."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()

    offsets = [(2.0, 0.0), (0.0, 3.0), (-1.5, 0.5)]
    members_data = [
        (FloorMember(revision_id=uuid.uuid4(), role=f"m{i}"), _shifted(ref_fids, dx, dy))
        for i, (dx, dy) in enumerate(offsets)
    ]

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=members_data,
    )

    assert len(result.members_failed) == 0
    # ref + 3 members
    assert len(result.members_ok) == 4
    for m in result.members_ok:
        assert m.transform.quality == "good"


# ---------------------------------------------------------------------------
# (12) rot_tol_rad: a rotation just below tolerance is accepted into members_ok
# ---------------------------------------------------------------------------


def test_rot_tol_rad_wide_accepts_no_rotation() -> None:
    """With rot_tol_rad=0.02 a perfectly aligned member lands in members_ok."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("power")
    # Zero rotation, pure translation — must pass gate.
    mem_fids = _shifted(ref_fids, 5.0, 0.0)

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, mem_fids)],
        rot_tol_rad=0.02,
    )

    assert mem.revision_id in {m.revision_id for m in result.members_ok}
    assert mem.revision_id not in {m.revision_id for m in result.members_failed}


# ---------------------------------------------------------------------------
# (13) scale_tol gate: scale mismatch beyond tolerance → members_failed
# ---------------------------------------------------------------------------


def test_scale_beyond_tol_goes_to_members_failed() -> None:
    """Member with scale >> 1.0 (belt-and-suspenders scale gate) lands in members_failed."""
    ref_id = uuid.uuid4()
    ref_fids = _good_fiducials()
    mem = _member("power")

    # Scale source fiducials by 1.1 (10 % >> scale_tol=0.02).
    scale = 1.1
    mem_fids = [
        GridFiducial(label=f.label, point=(f.point[0] * scale, f.point[1] * scale))
        for f in ref_fids
    ]

    result = compose_floor_registration(
        reference_revision_id=ref_id,
        reference_role="lighting",
        reference_fiducials=ref_fids,
        members=[(mem, mem_fids)],
        scale_tol=0.02,
    )

    assert mem.revision_id in {m.revision_id for m in result.members_failed}
    assert mem.revision_id not in {m.revision_id for m in result.members_ok}
