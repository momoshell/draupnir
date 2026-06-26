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


@_SKIP_UNLESS_REALDATA
@pytest.mark.asyncio
async def test_floor_takeoff_realdata_smoke() -> None:
    """Smoke test against the Phase R canonical test data (requires live DB + REALDATA env)."""
    import httpx

    base_url = os.environ.get("DRAUPNIR_API_URL", "http://localhost:8000")
    async with httpx.AsyncClient(base_url=base_url, timeout=60.0) as client:
        response = await client.get(
            "/v1/floors/takeoff",
            params={
                "reference_revision_id": _LIGHTING_REVISION_ID,
                "member": [
                    f"{_LIGHTING_REVISION_ID}:lighting",
                    f"{_CONTAINMENT_REVISION_ID}:containment",
                ],
                "strategy": "auto",
                "voronoi_fallback": "true",
            },
        )

    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    body = response.json()
    assert body["reference_revision_id"] == _LIGHTING_REVISION_ID
    assert body["reference_role"] == "lighting"
    assert isinstance(body["rooms"], list)
    assert isinstance(body["summary"], dict)
    assert body["summary"]["members_registered"] >= 1
    # Estimated meta must be present
    assert body["estimated_meta"] is not None
    assert body["estimated_meta"]["quantity_kind"] == "estimated"
