"""Route-level tests for GET /revisions/{id}/cable-estimate (issue #698b).

Mirrors test_revision_service_takeoff_api.py: the DB loaders are monkeypatched
with in-memory fixtures so these run without a database.

Scenarios:
1. Revision with a spline + device INSERT (no containment) → 200, quantity_kind="estimated",
   home_run_status="no_containment_sheet" for all circuits, device_drop/in_plan present.
2. containment_revision_id pointing to a non-existent revision → 404.
3. Invalid scope → 422 (FastAPI query validation).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

import app.api.v1.revision_routes.cable_estimate as cable_estimate_route
from app.db.session import get_db
from app.interpretation.cable_topology import DeviceFootprint, SplineInput

REVISION_ID = uuid.uuid4()


# ---------------------------------------------------------------------------
# Synthetic topology: one panel + one luminaire connected by one spline
# ---------------------------------------------------------------------------

_PANEL_BLOCK_REF = "Pr_60_70_22_22_DB-TST1 - Standard Distribution Board"
_LUM_BLOCK_REF = "Luminaire"

_SPLINES = [
    SplineInput(
        entity_id="sp-1",
        vertices=((0.0, 0.0), (5.0, 0.0)),
        closed=False,
    )
]

_DEVICES = [
    DeviceFootprint(
        entity_id="panel-1",
        block_ref=_PANEL_BLOCK_REF,
        kind="device",
        bbox=(-0.1, -0.1, 0.1, 0.1),
        position=(0.0, 0.0),
    ),
    DeviceFootprint(
        entity_id="lum-1",
        block_ref=_LUM_BLOCK_REF,
        kind="device",
        bbox=(4.9, -0.1, 5.1, 0.1),
        position=(5.0, 0.0),
    ),
]


def _manifest(revision_id: uuid.UUID | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        source_file_id=uuid.uuid4(),
        extraction_profile_id=None,
        source_job_id=uuid.uuid4(),
        drawing_revision_id=revision_id or REVISION_ID,
        adapter_run_output_id=None,
        canonical_entity_schema_version="1",
        counts_json={"layouts": 1, "layers": 1, "blocks": 1, "entities": 3},
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cable_estimate_app(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App with loaders patched: one spline + one panel + one luminaire, no containment."""

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest(revision_id)

    async def _fake_splines(
        db: Any, revision_id: uuid.UUID, *, exclude_off_sheet: bool = True
    ) -> list[SplineInput]:
        return list(_SPLINES)

    async def _fake_devices(
        db: Any, revision_id: uuid.UUID, *, exclude_off_sheet: bool = False
    ) -> list[DeviceFootprint]:
        return list(_DEVICES)

    monkeypatch.setattr(
        cable_estimate_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(cable_estimate_route, "load_spline_inputs", _fake_splines)
    monkeypatch.setattr(cable_estimate_route, "load_device_footprints", _fake_devices)

    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def cable_estimate_app_404(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App where the primary revision is found but any secondary revision raises 404."""
    from fastapi import HTTPException

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    _primary_id = REVISION_ID
    call_count: list[int] = [0]

    async def _fake_manifest_404_on_second(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        call_count[0] += 1
        if call_count[0] == 1:
            # First call = primary revision — succeed.
            return _manifest(revision_id)
        # Second call = containment revision — not found.
        raise HTTPException(status_code=404, detail="Revision not found")

    async def _fake_splines(
        db: Any, revision_id: uuid.UUID, *, exclude_off_sheet: bool = True
    ) -> list[SplineInput]:
        return []

    async def _fake_devices(
        db: Any, revision_id: uuid.UUID, *, exclude_off_sheet: bool = False
    ) -> list[DeviceFootprint]:
        return []

    monkeypatch.setattr(
        cable_estimate_route,
        "_get_active_revision_manifest_or_409",
        _fake_manifest_404_on_second,
    )
    monkeypatch.setattr(cable_estimate_route, "load_spline_inputs", _fake_splines)
    monkeypatch.setattr(cable_estimate_route, "load_device_footprints", _fake_devices)

    yield app
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_estimate(application: FastAPI, query: str = "") -> httpx.Response:
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(f"/v1/revisions/{REVISION_ID}/cable-estimate{query}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_cable_estimate_no_containment_returns_200(
    cable_estimate_app: FastAPI,
) -> None:
    """GET without containment_revision_id returns 200 with quantity_kind='estimated'."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    assert body["quantity_kind"] == "estimated"


async def test_cable_estimate_no_containment_has_no_containment_sheet_status(
    cable_estimate_app: FastAPI,
) -> None:
    """Without containment_revision_id all circuits have home_run_status='no_containment_sheet'."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()

    circuits = body["per_circuit"]
    assert len(circuits) >= 1, f"Expected at least one circuit; got: {circuits}"

    for circuit in circuits:
        assert circuit["home_run_status"] == "no_containment_sheet", (
            f"Expected no_containment_sheet, got {circuit['home_run_status']}"
        )
        assert circuit["home_run_m"] is None


async def test_cable_estimate_no_containment_device_drop_and_in_plan_present(
    cable_estimate_app: FastAPI,
) -> None:
    """device_drop_m and in_plan_length_m are present and non-negative."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()

    # The luminaire contributes a 2.0 m device drop (from default_estimate_params).
    assert body["total_device_drop_m"] >= 0.0
    assert body["total_in_plan_length_m"] >= 0.0
    # The spline from (0,0) to (5,0) is 5 m in-plan length.
    assert body["total_in_plan_length_m"] == pytest.approx(5.0, abs=0.01)


async def test_cable_estimate_no_containment_total_home_run_zero(
    cable_estimate_app: FastAPI,
) -> None:
    """Without containment, total_home_run_m=0.0."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    assert body["total_home_run_m"] == pytest.approx(0.0)


async def test_cable_estimate_no_containment_containment_revision_id_echoed_null(
    cable_estimate_app: FastAPI,
) -> None:
    """containment_revision_id is echoed as null in the response when not supplied."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    assert body["containment_revision_id"] is None


async def test_cable_estimate_no_containment_suppressed_counts_correct(
    cable_estimate_app: FastAPI,
) -> None:
    """home_run_suppressed_counts has no_containment_sheet equal to circuit count."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()

    n_circuits = len(body["per_circuit"])
    counts = body["home_run_suppressed_counts"]
    assert counts["no_containment_sheet"] == n_circuits


async def test_cable_estimate_registration_audit_null_without_containment(
    cable_estimate_app: FastAPI,
) -> None:
    """registration_audit is null when containment_revision_id is not supplied."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    assert body["registration_audit"] is None


async def test_cable_estimate_spare_fraction_in_response(
    cable_estimate_app: FastAPI,
) -> None:
    """spare_fraction is present and equals the default (0.10)."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    assert body["spare_fraction"] == pytest.approx(0.10)


async def test_cable_estimate_grand_with_spare_consistent(
    cable_estimate_app: FastAPI,
) -> None:
    """grand_with_spare_m ≈ grand_base_m * 1.10 (default spare)."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    assert body["grand_with_spare_m"] == pytest.approx(
        body["grand_base_m"] * (1.0 + body["spare_fraction"]), abs=1e-6
    )


async def test_cable_estimate_nonexistent_containment_returns_404(
    cable_estimate_app_404: FastAPI,
) -> None:
    """A containment_revision_id pointing to a non-existent revision → 404."""
    nonexistent = uuid.uuid4()
    transport = ASGITransport(app=cable_estimate_app_404)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/v1/revisions/{REVISION_ID}/cable-estimate?containment_revision_id={nonexistent}"
        )
    assert response.status_code == 404


async def test_cable_estimate_invalid_scope_returns_422(
    cable_estimate_app: FastAPI,
) -> None:
    """An invalid scope value is rejected with 422 by FastAPI's query validation."""
    response = await _get_estimate(cable_estimate_app, "?scope=bogus")
    assert response.status_code == 422


async def test_cable_estimate_params_stamp_present(
    cable_estimate_app: FastAPI,
) -> None:
    """params_stamp is a dict containing 'vertical_drops' and 'spare' keys."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    stamp = body["params_stamp"]
    assert isinstance(stamp, dict)
    assert "vertical_drops" in stamp
    assert "spare" in stamp


async def test_cable_estimate_reliability_keys_present(
    cable_estimate_app: FastAPI,
) -> None:
    """reliability dict contains the expected term keys."""
    response = await _get_estimate(cable_estimate_app)
    assert response.status_code == 200
    body = response.json()
    rel = body["reliability"]
    assert "device_drop_m" in rel
    assert "in_plan_length_m" in rel
    assert "home_run_m" in rel
    assert "combined" in rel
