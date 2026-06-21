"""Route-level tests for the /devices endpoint.

These exercise the full request → response path (routing, loaders seam, the
interpretation pipeline, and schema serialization) with DB loaders overridden by
in-memory fixtures, so they run without a database. Pattern mirrors test_rooms_api.py.

Fixture design notes
--------------------
Source A (block families) is derived from ALL enumerated device block_refs. This means any
device with a non-None block_ref lands in ``by_symbol_family``, which causes step 2 of
``classify_instance_kind`` to fire and return KIND_DEVICE before the architecture pattern
check at step 3. Architecture kind therefore only fires for block_refs that are NOT in the
enumerated set (e.g., when ``device_layer`` filters exclude architectural INSERTs). The
tests reflect this actual pipeline behavior: the SD and door devices below are all
classified as KIND_DEVICE (legend-resolvable via symbol_family), and the exemplars on the
``%LEGEND%`` layer are classified as KIND_LEGEND_EXEMPLAR.

Empty-legend fixture
--------------------
``empty_legend_devices_app`` overrides all loaders with no prose and no tag candidates so the
legend is empty. Every device must come back as unresolved or unknown — the endpoint must
not crash and all items must still carry a semantics block.

Pagination fixture
------------------
``paginated_devices_app`` injects a larger set of devices so that the default page size is
exceeded and a second page is reachable. The test verifies that semantics survive pagination
and that legend/schedule_by_type are revision-wide (not truncated to the page window).
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

import app.api.v1.revision_routes.devices as devices_route
from app.db.session import get_db
from app.interpretation.devices import Device, _TagCandidate

REVISION_ID = uuid.uuid4()


def _manifest() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        source_file_id=uuid.uuid4(),
        extraction_profile_id=None,
        source_job_id=uuid.uuid4(),
        drawing_revision_id=REVISION_ID,
        adapter_run_output_id=None,
        canonical_entity_schema_version="1",
        counts_json={"layouts": 1, "layers": 3, "blocks": 5, "entities": 20},
        created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# DWG-shaped fixture:
#   - 3 placed devices on Z000 layer with SD block_ref (smoke detectors)
#   - 2 exemplar inserts on a LEGEND layer (legend_exemplar kind, excluded from
#     device-count buckets but kept in items for kind=all)
#   - 1 device with a different block_ref (not SD) to exercise type diversity
# ---------------------------------------------------------------------------

_SD_DEVICES = [
    Device(
        entity_id=f"sd-{i}",
        sequence_index=i,
        depth=0,
        block_ref="SD",
        layer_ref="Z000",
        position={"x": float(i * 10), "y": 0.0},
        tag=None,
    )
    for i in range(3)
]

_LEGEND_EXEMPLARS = [
    Device(
        entity_id=f"legend-ex-{i}",
        sequence_index=10 + i,
        depth=0,
        block_ref="SD",
        # Layer name contains LEGEND → kind=legend_exemplar.
        layer_ref="Z010T-LEGEND",
        position={"x": 100.0 + i * 5, "y": 100.0},
        tag=None,
    )
    for i in range(2)
]

# A FAP device (fire alarm point) — different family, tagged.
_FAP_DEVICE = Device(
    entity_id="fap-1",
    sequence_index=20,
    depth=0,
    block_ref="FAP",
    layer_ref="Z000",
    position={"x": 200.0, "y": 0.0},
    tag=None,
)

_ALL_DEVICES = [*_SD_DEVICES, *_LEGEND_EXEMPLARS, _FAP_DEVICE]

# Tag candidates: one SD tag on a tag layer (near sd-0 at origin).
_TAG_CANDIDATES = [
    _TagCandidate(entity_id="tag-sd", text="SD", layer_ref="Z000-DEVICE-TAG", x=0.0, y=0.0),
    _TagCandidate(entity_id="tag-fap", text="FAP", layer_ref="Z000-DEVICE-TAG", x=200.0, y=0.0),
]

# Prose legend text simulating a schedule block on a LEGEND layer.
_LEGEND_PROSE_TEXTS = ["SD DENOTES SMOKE DETECTOR", "FAP DENOTES FIRE ALARM POINT"]


@pytest.fixture
def devices_app(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App with device loaders patched to in-memory fixtures (no DB)."""

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return list(_ALL_DEVICES)

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **_: Any) -> list[_TagCandidate]:
        return list(_TAG_CANDIDATES)

    async def _fake_legend_texts(db: Any, revision_id: uuid.UUID, **_: Any) -> list[str]:
        return list(_LEGEND_PROSE_TEXTS)

    monkeypatch.setattr(devices_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(devices_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(devices_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(devices_route, "load_legend_text_candidates", _fake_legend_texts)

    yield app
    app.dependency_overrides.clear()


async def _get_devices(devices_app: FastAPI, query: str = "") -> httpx.Response:
    transport = ASGITransport(app=devices_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(f"/v1/revisions/{REVISION_ID}/devices{query}")


async def test_devices_endpoint_returns_200_with_semantics(devices_app: FastAPI) -> None:
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    body = response.json()

    # All items present (kind=all default).
    assert len(body["items"]) == len(_ALL_DEVICES)

    # Every item has a semantics block.
    for item in body["items"]:
        sem = item["semantics"]
        assert "kind" in sem
        assert "status" in sem
        assert "basis" in sem
        assert isinstance(sem["source_layers"], list)
        assert isinstance(sem["competing_type_names"], list)

    # association.total_devices matches raw device count.
    assert body["association"]["total_devices"] == len(_ALL_DEVICES)

    # schedule (block_ref based) is unchanged.
    assert len(body["schedule"]) > 0


async def test_devices_legend_summary_populated(devices_app: FastAPI) -> None:
    """The legend block must have legend_size > 0 when prose + family inputs are present."""
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    body = response.json()

    legend = body["legend"]
    assert "legend_size" in legend
    assert "sources" in legend
    assert "resolved_count" in legend
    assert "unresolved_count" in legend
    # SD and FAP block families + prose lines → at least two entries.
    assert legend["legend_size"] >= 2


async def test_devices_schedule_by_type_excludes_exemplars_and_is_small(
    devices_app: FastAPI,
) -> None:
    """schedule_by_type denominattor EXCLUDES legend_exemplar; count is a small handful."""
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    body = response.json()

    sbt = body["schedule_by_type"]
    # Total across all buckets must be <= total_devices (exemplars excluded from denominator).
    total_in_buckets = sum(entry["count"] for entry in sbt)
    assert total_in_buckets <= body["association"]["total_devices"]
    # 3 SD devices + 1 FAP = 4 (exemplars excluded) → a handful, NOT ~113.
    assert total_in_buckets < 20

    # Exemplars (2 items) must NOT appear as a separate bucket.
    type_names = {e["type_name"] for e in sbt}
    assert "legend_exemplar" not in type_names

    # SD should be the most common type.
    sd_entry = next((e for e in sbt if e["type_name"] == "SD"), None)
    assert sd_entry is not None
    assert sd_entry["count"] == 3


async def test_devices_kind_filter_device_returns_only_devices(devices_app: FastAPI) -> None:
    response = await _get_devices(devices_app, "?kind=device")
    assert response.status_code == 200
    body = response.json()

    for item in body["items"]:
        assert item["semantics"]["kind"] == "device"


async def test_devices_kind_filter_architecture_returns_subset(devices_app: FastAPI) -> None:
    """kind=architecture returns only architecture items (may be empty if none in fixture)."""
    response = await _get_devices(devices_app, "?kind=architecture")
    assert response.status_code == 200
    body = response.json()

    for item in body["items"]:
        assert item["semantics"]["kind"] == "architecture"


async def test_devices_kind_filter_invalid_returns_422(devices_app: FastAPI) -> None:
    response = await _get_devices(devices_app, "?kind=badvalue")
    assert response.status_code == 422


async def test_devices_kind_echo_in_response(devices_app: FastAPI) -> None:
    response = await _get_devices(devices_app, "?kind=device")
    assert response.status_code == 200
    assert response.json()["kind"] == "device"


async def test_devices_kind_default_is_all(devices_app: FastAPI) -> None:
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    assert response.json()["kind"] == "all"


@pytest.fixture
def devices_capture_scope(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[FastAPI, dict[str, Any]]]:
    """Like ``devices_app`` but records the ``exclude_off_sheet`` kwarg each loader receives,
    so the #588 ``scope`` route-param wiring can be asserted without a DB."""
    seen: dict[str, Any] = {}

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **kw: Any) -> list[Device]:
        seen["devices"] = kw.get("exclude_off_sheet")
        return list(_ALL_DEVICES)

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **kw: Any) -> list[_TagCandidate]:
        seen["tags"] = kw.get("exclude_off_sheet")
        return list(_TAG_CANDIDATES)

    async def _fake_legend_texts(db: Any, revision_id: uuid.UUID, **_: Any) -> list[str]:
        return list(_LEGEND_PROSE_TEXTS)

    monkeypatch.setattr(devices_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(devices_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(devices_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(devices_route, "load_legend_text_candidates", _fake_legend_texts)
    yield app, seen
    app.dependency_overrides.clear()


async def test_devices_scope_defaults_to_printed_sheet(
    devices_capture_scope: tuple[FastAPI, dict[str, Any]],
) -> None:
    """Default scope (#588) excludes off-sheet for BOTH the device walk and tag association."""
    application, seen = devices_capture_scope
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/devices")
    assert response.status_code == 200
    assert seen["devices"] is True
    assert seen["tags"] is True


async def test_devices_scope_modelspace_keeps_full_walk(
    devices_capture_scope: tuple[FastAPI, dict[str, Any]],
) -> None:
    application, seen = devices_capture_scope
    transport = ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/revisions/{REVISION_ID}/devices?scope=modelspace")
    assert response.status_code == 200
    assert seen["devices"] is False
    assert seen["tags"] is False


async def test_devices_scope_invalid_returns_422(devices_app: FastAPI) -> None:
    response = await _get_devices(devices_app, "?scope=bogus")
    assert response.status_code == 422


async def test_devices_semantics_kind_values_are_valid(devices_app: FastAPI) -> None:
    """Every semantics.kind must be one of the KIND_* constants."""
    from app.interpretation.device_identity import (
        KIND_ANNOTATION,
        KIND_ARCHITECTURE,
        KIND_DEVICE,
        KIND_LEGEND_EXEMPLAR,
        KIND_UNKNOWN,
        STATUS_RESOLVED,
        STATUS_UNKNOWN,
        STATUS_UNRESOLVED,
    )

    valid_kinds = {
        KIND_DEVICE,
        KIND_ARCHITECTURE,
        KIND_ANNOTATION,
        KIND_LEGEND_EXEMPLAR,
        KIND_UNKNOWN,
    }
    valid_statuses = {STATUS_RESOLVED, STATUS_UNRESOLVED, STATUS_UNKNOWN}

    response = await _get_devices(devices_app)
    assert response.status_code == 200
    for item in response.json()["items"]:
        sem = item["semantics"]
        assert sem["kind"] in valid_kinds, f"unexpected kind: {sem['kind']}"
        assert sem["status"] in valid_statuses, f"unexpected status: {sem['status']}"


async def test_devices_legend_exemplars_present_in_all_items(devices_app: FastAPI) -> None:
    """kind=all includes legend_exemplar items; kind=device excludes them."""
    response_all = await _get_devices(devices_app)
    body_all = response_all.json()
    exemplar_ids_all = {
        item["entity_id"]
        for item in body_all["items"]
        if item["semantics"]["kind"] == "legend_exemplar"
    }
    assert len(exemplar_ids_all) == len(_LEGEND_EXEMPLARS)

    response_device = await _get_devices(devices_app, "?kind=device")
    body_device = response_device.json()
    exemplar_ids_device = {
        item["entity_id"]
        for item in body_device["items"]
        if item["semantics"]["kind"] == "legend_exemplar"
    }
    assert len(exemplar_ids_device) == 0


async def test_devices_resolved_device_has_type_name(devices_app: FastAPI) -> None:
    """SD devices resolved via tag abbreviation should carry type_name from the legend."""
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    body = response.json()

    # sd-0 is nearest to the SD tag candidate at origin; it gets tagged → resolved.
    sd0 = next((item for item in body["items"] if item["entity_id"] == "sd-0"), None)
    assert sd0 is not None
    sem = sd0["semantics"]
    assert sem["kind"] == "device"
    # Status is resolved because SD tag text hits by_abbreviation (from Source C tag token).
    assert sem["status"] == "resolved"
    # type_name comes from Source A or C (SD is both a family and a tag token).
    assert sem["type_name"] == "SD"


# ---------------------------------------------------------------------------
# schedule (block_ref) exact counts
# ---------------------------------------------------------------------------


async def test_devices_block_ref_schedule_counts(devices_app: FastAPI) -> None:
    """The raw block_ref schedule counts ALL inserted instances including exemplars.

    Raw walk: 3 SD placed + 2 SD exemplars + 1 FAP placed = SD:5, FAP:1.
    The schedule is a raw count, not filtered by kind.
    """
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    body = response.json()

    schedule = {entry["block_ref"]: entry["count"] for entry in body["schedule"]}
    # Exemplars share block_ref="SD" with placed devices; all 5 appear in the raw count.
    assert schedule.get("SD") == 5
    # FAP device contributes 1 raw count.
    assert schedule.get("FAP") == 1


# ---------------------------------------------------------------------------
# schedule_by_type includes FAP bucket
# ---------------------------------------------------------------------------


async def test_devices_schedule_by_type_includes_fap(devices_app: FastAPI) -> None:
    """FAP device must also appear in schedule_by_type, not only SD."""
    response = await _get_devices(devices_app)
    assert response.status_code == 200
    body = response.json()

    type_names = {entry["type_name"]: entry["count"] for entry in body["schedule_by_type"]}
    # 1 FAP device (not an exemplar) must appear in the denominator bucket.
    assert "FAP" in type_names
    assert type_names["FAP"] == 1


# ---------------------------------------------------------------------------
# legend and schedule_by_type are revision-wide regardless of kind filter
# ---------------------------------------------------------------------------


async def test_devices_legend_and_schedule_by_type_are_revision_wide(
    devices_app: FastAPI,
) -> None:
    """Applying kind=device must NOT shrink legend_size or schedule_by_type totals.

    legend and schedule_by_type are built from the full device set before pagination
    and kind-filtering; they must be identical for kind=all and kind=device.
    """
    response_all = await _get_devices(devices_app)
    response_device = await _get_devices(devices_app, "?kind=device")

    assert response_all.status_code == 200
    assert response_device.status_code == 200

    body_all = response_all.json()
    body_device = response_device.json()

    # legend must be identical regardless of kind filter.
    assert body_device["legend"]["legend_size"] == body_all["legend"]["legend_size"]
    assert body_device["legend"]["resolved_count"] == body_all["legend"]["resolved_count"]

    # schedule_by_type totals must be identical.
    total_all = sum(e["count"] for e in body_all["schedule_by_type"])
    total_device = sum(e["count"] for e in body_device["schedule_by_type"])
    assert total_all == total_device

    # But the returned items DO differ: kind=device has fewer items than kind=all.
    assert len(body_device["items"]) < len(body_all["items"])


# ---------------------------------------------------------------------------
# Empty legend — degraded-path fixture
# ---------------------------------------------------------------------------

_BARE_DEVICES = [
    Device(
        entity_id=f"bare-{i}",
        sequence_index=i,
        depth=0,
        block_ref=None,  # No block_ref → cannot land in by_symbol_family.
        layer_ref="Z000",
        position={"x": float(i), "y": 0.0},
        tag=None,
    )
    for i in range(3)
]


@pytest.fixture
def empty_legend_devices_app(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App variant with no legend prose and no tag candidates.

    Every device has block_ref=None so it does not self-register in the legend via
    Source A. With empty Source B (prose) and Source C (tags), the legend is completely
    empty. All devices must come back unresolved or unknown.
    """

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return list(_BARE_DEVICES)

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **_: Any) -> list[_TagCandidate]:
        return []

    async def _fake_legend_texts(db: Any, revision_id: uuid.UUID, **_: Any) -> list[str]:
        return []

    monkeypatch.setattr(devices_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(devices_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(devices_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(devices_route, "load_legend_text_candidates", _fake_legend_texts)

    yield app
    app.dependency_overrides.clear()


async def test_devices_empty_legend_does_not_crash(empty_legend_devices_app: FastAPI) -> None:
    """Empty legend → endpoint must return 200, all items have semantics, legend_size=0."""
    response = await _get_devices(empty_legend_devices_app)
    assert response.status_code == 200
    body = response.json()

    assert body["legend"]["legend_size"] == 0
    assert len(body["items"]) == len(_BARE_DEVICES)

    # Every item must still carry a full semantics block.
    for item in body["items"]:
        sem = item["semantics"]
        assert "kind" in sem
        assert "status" in sem
        assert "basis" in sem
        assert isinstance(sem["source_layers"], list)

    # With empty legend and no tags, devices land in unknown/unresolved (not resolved).
    resolved_items = [i for i in body["items"] if i["semantics"]["status"] == "resolved"]
    assert len(resolved_items) == 0


async def test_devices_empty_legend_association_total_devices_unchanged(
    empty_legend_devices_app: FastAPI,
) -> None:
    """association.total_devices reflects the raw walk count even with an empty legend."""
    response = await _get_devices(empty_legend_devices_app)
    assert response.status_code == 200
    body = response.json()

    assert body["association"]["total_devices"] == len(_BARE_DEVICES)


# ---------------------------------------------------------------------------
# Pagination — semantics and revision-wide aggregates survive across pages
# ---------------------------------------------------------------------------

# Build enough devices to exceed the default page size (50).
_PAGED_SD_DEVICES = [
    Device(
        entity_id=f"paged-sd-{i}",
        sequence_index=i,
        depth=0,
        block_ref="SD",
        layer_ref="Z000",
        position={"x": float(i * 10), "y": 0.0},
        tag=None,
    )
    for i in range(60)  # 60 devices → page 1 has 50, page 2 has 10.
]

_PAGED_LEGEND_EXEMPLARS = [
    Device(
        entity_id=f"paged-legend-ex-{i}",
        sequence_index=100 + i,
        depth=0,
        block_ref="SD",
        layer_ref="Z010T-LEGEND",
        position={"x": 500.0 + i * 5, "y": 500.0},
        tag=None,
    )
    for i in range(2)
]

_PAGED_ALL_DEVICES = [*_PAGED_SD_DEVICES, *_PAGED_LEGEND_EXEMPLARS]


@pytest.fixture
def paginated_devices_app(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    """App variant with 62 devices (60 placed SD + 2 exemplars) to test pagination."""

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = _no_db

    async def _fake_manifest(revision_id: uuid.UUID, db: Any) -> SimpleNamespace:
        return _manifest()

    async def _fake_devices(db: Any, revision_id: uuid.UUID, **_: Any) -> list[Device]:
        return list(_PAGED_ALL_DEVICES)

    async def _fake_tags(db: Any, revision_id: uuid.UUID, **_: Any) -> list[_TagCandidate]:
        return []

    async def _fake_legend_texts(db: Any, revision_id: uuid.UUID, **_: Any) -> list[str]:
        return ["SD DENOTES SMOKE DETECTOR"]

    monkeypatch.setattr(devices_route, "_get_active_revision_manifest_or_409", _fake_manifest)
    monkeypatch.setattr(devices_route, "enumerate_devices", _fake_devices)
    monkeypatch.setattr(devices_route, "load_tag_candidates", _fake_tags)
    monkeypatch.setattr(devices_route, "load_legend_text_candidates", _fake_legend_texts)

    yield app
    app.dependency_overrides.clear()


async def test_devices_pagination_page2_has_semantics(paginated_devices_app: FastAPI) -> None:
    """Page 2 items must also carry a semantics block — identity is not lost after page 1."""
    # Fetch page 1 to get the cursor.
    response_p1 = await _get_devices(paginated_devices_app)
    assert response_p1.status_code == 200
    body_p1 = response_p1.json()
    assert body_p1["next_cursor"] is not None, "Expected more than one page"

    cursor = body_p1["next_cursor"]
    transport = ASGITransport(app=paginated_devices_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response_p2 = await client.get(f"/v1/revisions/{REVISION_ID}/devices?cursor={cursor}")

    assert response_p2.status_code == 200
    body_p2 = response_p2.json()

    # Page 2 must have items.
    assert len(body_p2["items"]) > 0

    # Every item on page 2 must carry a complete semantics block.
    for item in body_p2["items"]:
        sem = item["semantics"]
        assert "kind" in sem
        assert "status" in sem
        assert "basis" in sem
        assert isinstance(sem["source_layers"], list)
        assert isinstance(sem["competing_type_names"], list)


async def test_devices_pagination_schedule_by_type_is_revision_wide(
    paginated_devices_app: FastAPI,
) -> None:
    """schedule_by_type on page 1 must reflect ALL 60 placed devices, not just the 50 on page 1.

    The two exemplars are excluded from the denominator bucket but count is still
    revision-wide (all 60 placed, not just 50).
    """
    response = await _get_devices(paginated_devices_app)
    assert response.status_code == 200
    body = response.json()

    sbt = {entry["type_name"]: entry["count"] for entry in body["schedule_by_type"]}
    # 60 placed SD devices — all 60 must be in the SD bucket (not just the first 50 on the page).
    assert sbt.get("SD") == 60
    # Exemplars must not appear as a bucket.
    assert "legend_exemplar" not in sbt


async def test_devices_pagination_legend_is_revision_wide(
    paginated_devices_app: FastAPI,
) -> None:
    """legend on page 1 summarises the FULL device set, not just the page window."""
    response = await _get_devices(paginated_devices_app)
    assert response.status_code == 200
    body = response.json()

    # association.total_devices must be the full 62 (60 + 2 exemplars), not just 50.
    assert body["association"]["total_devices"] == len(_PAGED_ALL_DEVICES)
    # legend must have at least one entry (SD from prose).
    assert body["legend"]["legend_size"] >= 1
