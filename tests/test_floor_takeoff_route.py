"""DB-seam + guarded real-data smoke tests for the /floors/takeoff route (issue #721, Phase R-G).

DB-seam tests mock the assembler at the boundary; the real-data smoke test is guarded
behind ``DRAUPNIR_REALDATA_SMOKE=1`` and requires a live DB with the Phase R test data.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI

_SKIP_UNLESS_REALDATA = pytest.mark.skipif(
    os.environ.get("DRAUPNIR_REALDATA_SMOKE") != "1",
    reason="Set DRAUPNIR_REALDATA_SMOKE=1 to run real-data smoke tests",
)

_REF_REV = UUID("aaaa0000-0000-0000-0000-000000000001")
_MEM_REV = UUID("bbbb0000-0000-0000-0000-000000000002")


async def _dummy_db() -> AsyncGenerator[None, None]:
    """A no-op get_db override: the input-validation 422s fire before any DB access,
    so the session is never touched. Without this, get_db raises 500 in the unit lane
    (no DATABASE_URL configured) and masks the 422 the test asserts."""
    yield None


def _make_app() -> FastAPI:
    """Build the app with get_db overridden so pure-validation 422s don't 500 on a
    missing DB session (these tests assert input validation, not DB behaviour)."""
    from app.db.session import get_db
    from app.main import create_app

    app = create_app()
    app.dependency_overrides[get_db] = _dummy_db
    return app


# ---------------------------------------------------------------------------
# Fail-loud / parse tests (pure logic — no DB, no assembler call)
# ---------------------------------------------------------------------------


def test_parse_member_exported() -> None:
    """parse_member is importable and parses correctly (no DB needed)."""
    from app.interpretation.floor_takeoff_loaders import parse_member

    rid, role = parse_member(f"{_REF_REV}:lighting")
    assert rid == _REF_REV
    assert role == "lighting"


def test_parse_member_bad_token() -> None:
    from app.interpretation.floor_takeoff_loaders import parse_member

    with pytest.raises(ValueError, match="colon-separated"):
        parse_member("no-colon-token")


# ---------------------------------------------------------------------------
# Route-level tests with mocked _get_active_revision_manifest_or_409 and assembler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_route_422_reference_not_in_member_list() -> None:
    """HTTP 422 when reference_revision_id is not among the member UUIDs."""
    from fastapi.testclient import TestClient

    app = _make_app()
    client = TestClient(app)

    other_rev = uuid4()
    another_rev = uuid4()
    # reference_revision_id is NOT in member list
    response = client.get(
        "/v1/floors/takeoff",
        params={
            "reference_revision_id": str(_REF_REV),
            "member": [f"{other_rev}:containment", f"{another_rev}:power"],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_route_422_fewer_than_2_members() -> None:
    """HTTP 422 when fewer than 2 distinct member UUIDs are supplied."""
    from fastapi.testclient import TestClient

    app = _make_app()
    client = TestClient(app)

    # Only 1 distinct member (the reference itself)
    response = client.get(
        "/v1/floors/takeoff",
        params={
            "reference_revision_id": str(_REF_REV),
            "member": [f"{_REF_REV}:lighting"],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_route_422_invalid_member_token() -> None:
    """HTTP 422 when a member token does not contain a colon."""
    from fastapi.testclient import TestClient

    app = _make_app()
    client = TestClient(app)

    response = client.get(
        "/v1/floors/takeoff",
        params={
            "reference_revision_id": str(_REF_REV),
            "member": ["not-a-valid-token", f"{_MEM_REV}:containment"],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_route_422_containment_not_in_member_list() -> None:
    """HTTP 422 when containment_revision_id is supplied but not listed as a member.

    Without this guard the cable-pair quality gate short-circuits (containment_member
    is None → None != "good" is False) and silently returns 200 with unreliable
    home-run data.
    """
    from fastapi.testclient import TestClient

    app = _make_app()
    client = TestClient(app)

    containment_rev = uuid4()
    # reference + one other member, but containment_revision_id is a DIFFERENT uuid
    # that is NOT listed in the member tokens.
    response = client.get(
        "/v1/floors/takeoff",
        params={
            "reference_revision_id": str(_REF_REV),
            "member": [f"{_REF_REV}:lighting", f"{_MEM_REV}:power"],
            "containment_revision_id": str(containment_rev),
        },
    )
    assert response.status_code == 422
    # Custom exception handler wraps HTTPException.detail in error.message.
    body = response.json()
    message = body.get("error", {}).get("message", body.get("detail", ""))
    assert "containment_revision_id" in message


# ---------------------------------------------------------------------------
# Real-data smoke test (guarded)
# ---------------------------------------------------------------------------

_LIGHTING_REVISION_ID = "95015ea4-b4d0-46a6-b676-e3fc7f622a7f"
_CONTAINMENT_REVISION_ID = "b29fcd01-3eb0-41ad-9f0c-012925d47824"
_POWER_REVISION_ID = "bd2ac5ee-7de4-448c-b24e-cf0d294aaef0"
_MEDGAS_REVISION_ID = "1cc72d6e-7723-4189-8a02-a336956a275b"


@_SKIP_UNLESS_REALDATA
@pytest.mark.asyncio
async def test_floor_takeoff_realdata_gate() -> None:
    """R-H real-data fusion gate (#722): the full Welbeck Level-0 floor end-to-end.

    Requires a live API (DRAUPNIR_API_URL) with project 49140cff ingested + centerlines
    materialized (GET service-takeoff per member first). Encodes the PASS invariants from
    docs/phase-r-gate-results.md. Skipped unless DRAUPNIR_REALDATA_SMOKE=1.
    """
    import httpx

    base_url = os.environ.get("DRAUPNIR_API_URL", "http://localhost:8000")
    async with httpx.AsyncClient(base_url=base_url, timeout=120.0) as client:
        response = await client.get(
            "/v1/floors/takeoff",
            params={
                "reference_revision_id": _LIGHTING_REVISION_ID,
                "member": [
                    f"{_LIGHTING_REVISION_ID}:lighting",
                    f"{_CONTAINMENT_REVISION_ID}:containment",
                    f"{_POWER_REVISION_ID}:power",
                    f"{_MEDGAS_REVISION_ID}:med-gas",
                ],
                "containment_revision_id": _CONTAINMENT_REVISION_ID,
                "strategy": "auto",
                "voronoi_fallback": "true",
            },
        )

    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    body = response.json()

    # --- Registration: all four members register cleanly ---
    assert body["reference_revision_id"] == _LIGHTING_REVISION_ID
    assert body["reference_role"] == "lighting"
    assert body["summary"]["members_registered"] == 4, body["summary"]
    assert body["members_failed"] == []
    assert all(m["quality"] == "good" for m in body["members"]), body["members"]

    # --- Three kinds present and DISTINCT (no cross-kind summing) ---
    assert isinstance(body["rooms"], list) and body["rooms"], "expected >=1 room with quantities"
    assert body["estimated_meta"] is not None
    assert body["estimated_meta"]["quantity_kind"] == "estimated"
    rel = body["estimated_meta"]["reliability"]
    assert rel["device_drop_m"] == "reliable"
    assert rel["in_plan_length_m"] == "schematic_provisional"
    # in-plan cable is reported at floor level, NOT per-room
    assert isinstance(body["estimated_circuits"], list) and body["estimated_circuits"]
    for rb in body["rooms"]:
        est = rb.get("estimated")
        if est is not None:
            assert "in_plan_length_m" not in est  # floor-level only

    # --- MEASURED conservation vs each member's single-revision service-takeoff ---
    def _measured_total(role: str) -> float:
        blocks = [*body["rooms"], body["unassigned"]]
        return sum(
            (i.get("real_length_m") or 0.0)
            for rb in blocks
            for i in rb["measured"]
            if i["role"] == role
        )

    async with httpx.AsyncClient(base_url=base_url, timeout=120.0) as client:
        for role, rev in (
            ("containment", _CONTAINMENT_REVISION_ID),
            ("med-gas", _MEDGAS_REVISION_ID),
        ):
            st = (await client.get(f"/v1/revisions/{rev}/service-takeoff")).json()
            single = sum((ln.get("real_length_m") or 0.0) for ln in (st.get("items") or []))
            fused = _measured_total(role)
            assert single > 0.0, f"{role}: single-revision total should be positive"
            assert abs(fused - single) <= 0.03 * single, (
                f"{role}: fused {fused:.1f} m vs single-rev {single:.1f} m exceeds 3% tolerance"
            )

    # --- no_anchor_fraction + no_anchor_status provisional gate (issue #735) ---
    naf = body["summary"]["no_anchor_fraction"]
    assert isinstance(naf, (int, float)) and 0.0 <= naf <= 1.0
    nas = body["summary"]["no_anchor_status"]
    assert nas in ("ok", "elevated", "critical"), f"unexpected no_anchor_status: {nas!r}"
    # Status must be consistent with the fraction and the documented provisional bands.
    if naf <= 0.50:
        assert nas == "ok", f"fraction {naf} should be 'ok', got {nas!r}"
    elif naf <= 0.95:
        assert nas == "elevated", f"fraction {naf} should be 'elevated', got {nas!r}"
    else:
        assert nas == "critical", f"fraction {naf} should be 'critical', got {nas!r}"
