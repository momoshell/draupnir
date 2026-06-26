"""DB-seam integration tests for floor_registration_loaders (issue #714).

Lane: db_worker

Architecture
------------
- Error-path tests (project_mismatch, missing_revision, reference_without_fiducials)
  use a mocked AsyncSession so they run without needing fully-ingested DrawingRevision
  rows (creating DrawingRevision requires a full ingest harness with many FK deps).
- The smoke test queries the real app data (project 49140cff) and is SKIPPED when
  that project/revisions are not present — CI without that data never fails.

Error paths tested
------------------
1. test_project_mismatch_raises: revisions belong to different projects → ValueError.
2. test_missing_revision_raises: unknown revision id → ValueError.
3. test_reference_without_fiducials_raises: reference exists but has no grid entities → ValueError.

Smoke test (real data, guarded)
---------------------------------
test_welbeck_level0_floor_registration_smoke: project 49140cff, 3 members registered
quality=good, matched=8, rotation_rad=0.0, dy=0.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from app.interpretation.floor_registration import FloorMember, FloorRegistration
from app.interpretation.floor_registration_loaders import load_floor_registration

# ---------------------------------------------------------------------------
# Real project/revision UUIDs for the guarded smoke test.
# ---------------------------------------------------------------------------

_WELBECK_PROJECT_ID = UUID("49140cff-7368-4b2c-9627-991283553d81")
_WELBECK_REF_REVISION_ID = UUID("95015ea4-b4d0-46a6-b676-e3fc7f622a7f")  # E-630003 lighting
_WELBECK_MEMBERS = [
    FloorMember(
        revision_id=UUID("b29fcd01-3eb0-41ad-9f0c-012925d47824"),
        role="containment",
    ),
    FloorMember(
        revision_id=UUID("bd2ac5ee-7de4-448c-b24e-cf0d294aaef0"),
        role="power",
    ),
    FloorMember(
        revision_id=UUID("1cc72d6e-7723-4189-8a02-a336956a275b"),
        role="med_gas",
    ),
]


# ---------------------------------------------------------------------------
# Helpers for mocking AsyncSession
# ---------------------------------------------------------------------------


def _make_mock_db(rev_rows: list[SimpleNamespace]) -> AsyncMock:
    """Return a mock AsyncSession whose execute().all() yields rev_rows."""
    db = AsyncMock()
    execute_result = MagicMock()
    execute_result.all.return_value = rev_rows
    db.execute.return_value = execute_result
    return db


# ---------------------------------------------------------------------------
# Error-path tests (mocked DB — no real revisions needed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_project_mismatch_raises() -> None:
    """Members spanning >1 project raise ValueError."""
    ref_id = uuid.uuid4()
    mem_id = uuid.uuid4()
    proj_a = uuid.uuid4()
    proj_b = uuid.uuid4()

    # Two revisions, different project_ids.
    rev_rows = [
        SimpleNamespace(id=ref_id, project_id=proj_a),
        SimpleNamespace(id=mem_id, project_id=proj_b),
    ]
    db = _make_mock_db(rev_rows)

    with pytest.raises(ValueError, match="different projects"):
        await load_floor_registration(
            db,
            reference_revision_id=ref_id,
            members=[FloorMember(revision_id=mem_id, role="power")],
            reference_role="lighting",
        )


@pytest.mark.asyncio
async def test_missing_revision_raises() -> None:
    """An unknown revision id raises ValueError mentioning the missing id."""
    ref_id = uuid.uuid4()
    unknown_id = uuid.uuid4()

    # Only ref returned; unknown_id absent.
    rev_rows = [SimpleNamespace(id=ref_id, project_id=uuid.uuid4())]
    db = _make_mock_db(rev_rows)

    with pytest.raises(ValueError, match="not found"):
        await load_floor_registration(
            db,
            reference_revision_id=ref_id,
            members=[FloorMember(revision_id=unknown_id, role="power")],
            reference_role="lighting",
        )


@pytest.mark.asyncio
async def test_member_load_error_is_soft_failure() -> None:
    """load_grid_fiducials raising for ONE member must not abort the whole registration.

    The failing member lands in members_failed with reason starting 'load_error';
    other members that loaded successfully are unaffected and appear in members_ok.

    Regression guard for the per-member isolation guarantee in the loader.
    """
    from app.interpretation.grid_registration import GridFiducial

    ref_id = uuid.uuid4()
    good_mem_id = uuid.uuid4()
    bad_mem_id = uuid.uuid4()
    proj_id = uuid.uuid4()

    rev_rows = [
        SimpleNamespace(id=ref_id, project_id=proj_id),
        SimpleNamespace(id=good_mem_id, project_id=proj_id),
        SimpleNamespace(id=bad_mem_id, project_id=proj_id),
    ]
    db = _make_mock_db(rev_rows)

    # Six good fiducials for ref and good_mem; bad_mem's loader raises.
    _good_fids = [GridFiducial(label=str(i), point=(float(i * 10), 0.0)) for i in range(6)]

    call_count = 0

    async def _fake_load_grid_fiducials(
        session: object,
        revision_id: UUID,
        **kwargs: object,
    ) -> list[GridFiducial]:
        nonlocal call_count
        call_count += 1
        if revision_id == bad_mem_id:
            raise RuntimeError("simulated DB decode error")
        return list(_good_fids)

    with patch(
        "app.interpretation.floor_registration_loaders.load_grid_fiducials",
        new=_fake_load_grid_fiducials,
    ):
        result = await load_floor_registration(
            db,
            reference_revision_id=ref_id,
            members=[
                FloorMember(revision_id=good_mem_id, role="power"),
                FloorMember(revision_id=bad_mem_id, role="containment"),
            ],
            reference_role="lighting",
        )

    # bad_mem must be in members_failed with load_error reason.
    failed_ids = {m.revision_id for m in result.members_failed}
    assert bad_mem_id in failed_ids
    bad_failed = next(m for m in result.members_failed if m.revision_id == bad_mem_id)
    assert bad_failed.reason.startswith("load_error")

    # good_mem (quality=good, same fiducials as ref → identity) must be in members_ok.
    ok_ids = {m.revision_id for m in result.members_ok}
    assert good_mem_id in ok_ids
    assert ref_id in ok_ids


@pytest.mark.asyncio
async def test_reference_without_fiducials_raises() -> None:
    """Reference revision with no grid entities (empty fiducials) raises ValueError."""
    ref_id = uuid.uuid4()
    proj_id = uuid.uuid4()

    rev_rows = [SimpleNamespace(id=ref_id, project_id=proj_id)]
    db = _make_mock_db(rev_rows)

    # Patch load_grid_fiducials to return empty list for reference.
    with (
        patch(
            "app.interpretation.floor_registration_loaders.load_grid_fiducials",
            new=AsyncMock(return_value=[]),
        ),
        pytest.raises(ValueError, match="no resolvable grid fiducials"),
    ):
        await load_floor_registration(
            db,
            reference_revision_id=ref_id,
            members=[],
            reference_role="lighting",
        )


# ---------------------------------------------------------------------------
# Guarded smoke test — real data (skipped when project not present in DB)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.db_worker
@pytest.mark.asyncio
async def test_welbeck_level0_floor_registration_smoke() -> None:
    """E-630003 lighting as reference; E-610003, E-620003, M-540003 as members.

    All three should register quality=good, matched=8, rotation_rad≈0, dy=0.
    Skipped when the project or revisions are absent from the database.
    """
    import os

    from sqlalchemy import select

    import app.db.session as session_module
    from app.models.drawing_revision import DrawingRevision

    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — skipping smoke test")

    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        pytest.skip("DB not configured — skipping smoke test")

    async with session_maker() as db:
        # Check all required revisions are present.
        all_ids = [_WELBECK_REF_REVISION_ID, *(m.revision_id for m in _WELBECK_MEMBERS)]
        rows = (
            await db.execute(select(DrawingRevision.id).where(DrawingRevision.id.in_(all_ids)))
        ).all()
        found = {row.id for row in rows}
        missing = [rid for rid in all_ids if rid not in found]
        if missing:
            pytest.skip(
                f"Welbeck revisions not present in DB (missing: {missing}) — skipping smoke test"
            )

        result = await load_floor_registration(
            db,
            reference_revision_id=_WELBECK_REF_REVISION_ID,
            members=_WELBECK_MEMBERS,
            reference_role="lighting",
        )

    assert isinstance(result, FloorRegistration)

    # All 3 members + reference should be in members_ok.
    assert len(result.members_failed) == 0, (
        f"Unexpected failures: {[(m.revision_id, m.reason) for m in result.members_failed]}"
    )
    assert len(result.members_ok) == 4  # ref + 3

    for m in result.members_ok:
        audit = m.audit
        assert audit["quality"] == "good", (
            f"Member {m.revision_id} (role={m.role}) quality={audit['quality']}"
        )
        assert audit["matched_count"] == 8, (
            f"Member {m.revision_id} matched_count={audit['matched_count']}"
        )
        assert abs(audit["rotation_rad"]) < 0.01, (  # type: ignore[arg-type]
            f"Member {m.revision_id} rotation_rad={audit['rotation_rad']}"
        )
        assert abs(audit["dy"]) < 0.01, f"Member {m.revision_id} dy={audit['dy']}"  # type: ignore[arg-type]
