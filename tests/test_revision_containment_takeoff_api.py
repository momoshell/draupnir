"""Route-level tests for GET /revisions/{id}/containment-takeoff (issue #756, Phase 752c).

Uses the _make_app() / dependency-override pattern from test_floor_takeoff_route.py so
tests run without a database. The assembler (assemble_containment_takeoff) is monkeypatched
at the route module boundary.

Covers:
1. 200 happy path — per_type results map through, shape is correct.
2. 404 on a missing/invalid revision.
3. Honest-absent passthrough — containment_type=None serializes as null.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

import app.api.v1.revision_routes.containment_takeoff as containment_takeoff_route
from app.db.session import get_db
from app.interpretation.containment_takeoff import (
    ContainmentAttributionResult,
    ContainmentTypeLength,
)

REVISION_ID = uuid.uuid4()
MISSING_ID = uuid.uuid4()


async def _dummy_db() -> AsyncGenerator[None, None]:
    """No-op get_db override so tests that don't hit the DB don't 500."""
    yield None


def _make_app() -> FastAPI:
    """Build the full app with get_db overridden for pure-validation / no-DB tests."""
    from app.main import create_app

    application = create_app()
    application.dependency_overrides[get_db] = _dummy_db
    return application


# ---------------------------------------------------------------------------
# Synthetic ContainmentAttributionResult factories
# ---------------------------------------------------------------------------


def _result_with_types() -> ContainmentAttributionResult:
    """Two real containment types plus the honest-absent (None) bucket."""
    return ContainmentAttributionResult(
        per_type=(
            ContainmentTypeLength(
                containment_type="cable-tray",
                length_m=12.5,
                member_colour_keys=("idx:1",),
                member_pattern_names=("ANSI31",),
            ),
            ContainmentTypeLength(
                containment_type="conduit",
                length_m=4.0,
                member_colour_keys=("idx:2",),
                member_pattern_names=("SOLID",),
            ),
            ContainmentTypeLength(
                containment_type=None,  # honest-absent
                length_m=1.5,
                member_colour_keys=("idx:3",),
                member_pattern_names=("",),
            ),
        ),
        shared_length_m=0.25,
        total_length_m=18.25,
        centerline_segment_count=42,
    )


def _result_empty() -> ContainmentAttributionResult:
    return ContainmentAttributionResult(
        per_type=(),
        shared_length_m=0.0,
        total_length_m=0.0,
        centerline_segment_count=0,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_takeoff(
    application: FastAPI,
    revision_id: UUID = REVISION_ID,
    query: str = "",
) -> httpx.Response:
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(f"/v1/revisions/{revision_id}/containment-takeoff{query}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_containment_takeoff_200_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """200 with per_type entries correctly mapped from ContainmentAttributionResult."""
    application = _make_app()
    application.dependency_overrides[get_db] = _dummy_db

    from types import SimpleNamespace

    def _fake_manifest_fn(revision_id: UUID, db: Any) -> Any:
        return SimpleNamespace(
            id=uuid.uuid4(),
            project_id=uuid.uuid4(),
            source_file_id=uuid.uuid4(),
        )

    async def _fake_manifest(revision_id: UUID, db: Any) -> Any:
        return _fake_manifest_fn(revision_id, db)

    async def _fake_assemble(db: Any, revision_id: UUID, **_: Any) -> ContainmentAttributionResult:
        return _result_with_types()

    monkeypatch.setattr(
        containment_takeoff_route,
        "_get_active_revision_manifest_or_409",
        _fake_manifest,
    )
    monkeypatch.setattr(
        containment_takeoff_route,
        "assemble_containment_takeoff",
        _fake_assemble,
    )

    response = await _get_takeoff(application)
    assert response.status_code == 200

    body = response.json()

    # Top-level fields must be present (extra='forbid' on model).
    assert set(body.keys()) == {
        "per_type",
        "shared_length_m",
        "total_length_m",
        "centerline_segment_count",
    }

    assert body["shared_length_m"] == pytest.approx(0.25)
    assert body["total_length_m"] == pytest.approx(18.25)
    assert body["centerline_segment_count"] == 42

    per_type = body["per_type"]
    assert len(per_type) == 3

    cable_tray = next(e for e in per_type if e["containment_type"] == "cable-tray")
    assert cable_tray["length_m"] == pytest.approx(12.5)
    assert cable_tray["member_colour_keys"] == ["idx:1"]
    assert cable_tray["member_pattern_names"] == ["ANSI31"]

    conduit = next(e for e in per_type if e["containment_type"] == "conduit")
    assert conduit["length_m"] == pytest.approx(4.0)

    # Honest-absent: containment_type is null.
    absent = next(e for e in per_type if e["containment_type"] is None)
    assert absent["length_m"] == pytest.approx(1.5)


async def test_containment_takeoff_honest_absent_is_null(monkeypatch: pytest.MonkeyPatch) -> None:
    """ContainmentTypeLength(containment_type=None) serializes to containment_type=null."""
    application = _make_app()
    application.dependency_overrides[get_db] = _dummy_db

    from types import SimpleNamespace

    async def _fake_manifest(revision_id: UUID, db: Any) -> Any:
        return SimpleNamespace(
            id=uuid.uuid4(), project_id=uuid.uuid4(), source_file_id=uuid.uuid4()
        )

    async def _fake_assemble(db: Any, revision_id: UUID, **_: Any) -> ContainmentAttributionResult:
        return ContainmentAttributionResult(
            per_type=(
                ContainmentTypeLength(
                    containment_type=None,
                    length_m=3.0,
                    member_colour_keys=("idx:5",),
                    member_pattern_names=("",),
                ),
            ),
            shared_length_m=0.0,
            total_length_m=3.0,
            centerline_segment_count=5,
        )

    monkeypatch.setattr(
        containment_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(containment_takeoff_route, "assemble_containment_takeoff", _fake_assemble)

    response = await _get_takeoff(application)
    assert response.status_code == 200

    body = response.json()
    assert len(body["per_type"]) == 1
    entry = body["per_type"][0]
    assert entry["containment_type"] is None
    assert entry["length_m"] == pytest.approx(3.0)


async def test_containment_takeoff_404_missing_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing revision causes the route to return 404."""
    application = _make_app()
    application.dependency_overrides[get_db] = _dummy_db

    from app.core.exceptions import raise_not_found

    async def _raise_not_found(revision_id: UUID, db: Any) -> Any:
        raise_not_found("Drawing revision", str(revision_id))

    monkeypatch.setattr(
        containment_takeoff_route,
        "_get_active_revision_manifest_or_409",
        _raise_not_found,
    )

    response = await _get_takeoff(application, revision_id=MISSING_ID)
    assert response.status_code == 404


async def test_containment_takeoff_empty_revision_200(monkeypatch: pytest.MonkeyPatch) -> None:
    """A revision with no containment data returns 200 with empty per_type and zero lengths."""
    application = _make_app()
    application.dependency_overrides[get_db] = _dummy_db

    from types import SimpleNamespace

    async def _fake_manifest(revision_id: UUID, db: Any) -> Any:
        return SimpleNamespace(
            id=uuid.uuid4(), project_id=uuid.uuid4(), source_file_id=uuid.uuid4()
        )

    async def _fake_assemble(db: Any, revision_id: UUID, **_: Any) -> ContainmentAttributionResult:
        return _result_empty()

    monkeypatch.setattr(
        containment_takeoff_route, "_get_active_revision_manifest_or_409", _fake_manifest
    )
    monkeypatch.setattr(containment_takeoff_route, "assemble_containment_takeoff", _fake_assemble)

    response = await _get_takeoff(application)
    assert response.status_code == 200

    body = response.json()
    assert body["per_type"] == []
    assert body["shared_length_m"] == pytest.approx(0.0)
    assert body["total_length_m"] == pytest.approx(0.0)
    assert body["centerline_segment_count"] == 0
