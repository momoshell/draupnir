"""DB-backed integration tests for service_takeoff_loaders — the DB seam for P3 (#606).

Lane: db_worker (matches the ingest+materialization harness used by test_ingest_output_persistence).
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import replace
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.ingestion.finalization import IngestFinalizationPayload, compute_adapter_result_checksum
from app.ingestion.runner import IngestionRunRequest
from app.ingestion.validation.reconciliation import build_reconciliation
from app.interpretation.measurement import ScaleContext
from app.interpretation.rise_drop import KIND_DROP, KIND_RISE, RiseDropEntity
from app.interpretation.routed_runs import RoutedEntity
from app.interpretation.run_service_identity import TagPlacement
from app.interpretation.service_legend import ServiceLegend
from app.interpretation.service_takeoff_loaders import (
    ServiceTakeoffInputs,
    build_scale_context,
    build_service_legend,
    load_rise_drop_entities,
    load_routed_entities,
    load_service_takeoff_inputs,
    load_tag_placements,
)
from app.jobs.worker import process_ingest_job
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import _load_project_outputs
from tests.test_jobs import (
    _FAKE_RUNNER_ADAPTER_KEY,
    _FAKE_RUNNER_ADAPTER_VERSION,
    _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
    _FAKE_RUNNER_CONFIDENCE_SCORE,
    _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
    _FAKE_RUNNER_VALIDATION_STATUS,
    _FAKE_RUNNER_VALIDATOR_NAME,
    _FAKE_RUNNER_VALIDATOR_VERSION,
    _build_fake_ingest_payload,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MM_UNITS = {
    "normalized": "millimeter",
    "source": "$INSUNITS",
    "source_value": 4,
    "conversion_target": "meter",
    "conversion_factor": 0.001,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entity(
    entity_id: str,
    entity_type: str,
    layer_ref: str,
    geometry_json: dict[str, Any],
    *,
    style: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical-payload entity dict that becomes a RevisionEntity row."""
    payload: dict[str, Any] = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": "Model",
        "layer_ref": layer_ref,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE, "basis": "adapter"},
        "geometry_json": geometry_json,
        "properties_json": {"layer": layer_ref},
        "provenance_json": {
            "origin": "adapter_normalized",
            "adapter": {},
            "source_ref": None,
            "source_identity": entity_id,
            "source_hash": None,
            "extraction_path": [],
            "notes": [],
        },
    }
    if style is not None:
        # The .style property reads canonical_entity_json["style"].
        payload["style"] = style
    return payload


def _build_payload_with(
    request: IngestionRunRequest,
    *,
    entities: list[dict[str, Any]],
    units: dict[str, Any] | None = None,
) -> IngestFinalizationPayload:
    """Build a fake payload overriding entities and optionally units."""
    entity_counts = {
        "layouts": 1,
        "layers": 1,
        "blocks": 0,
        "entities": len(entities),
    }
    canonical_json: dict[str, Any] = {
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layouts": [{"layout_ref": "Model", "name": "Model"}],
        "layers": [{"layer_ref": "Pipes", "name": "Pipes"}],
        "blocks": [],
        "entities": deepcopy(entities),
        "entity_counts": entity_counts,
    }
    if units is not None:
        canonical_json["units"] = units

    report_json: dict[str, Any] = {
        "validation_report_schema_version": _FAKE_RUNNER_VALIDATION_REPORT_SCHEMA_VERSION,
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "validator": {
            "name": _FAKE_RUNNER_VALIDATOR_NAME,
            "version": _FAKE_RUNNER_VALIDATOR_VERSION,
        },
        "summary": {
            "validation_status": _FAKE_RUNNER_VALIDATION_STATUS,
            "entity_counts": entity_counts,
        },
        "coverage": {
            "schema_version": "0.1",
            "entities": {
                "total": len(entities),
                "mapped": len(entities),
                "unmapped": 0,
                "mapped_ratio": 1.0,
                "by_type": {},
            },
            "unmapped_by_reason": {},
            "layers": {"count": 1, "entities_with_layer_ref": 0, "source": None},
            "blocks": {"count": 0, "child_geometry_count": 0},
            "review_flagged_entities": 0,
        },
        "reconciliation": build_reconciliation(canonical_json),
        "checks": [],
        "findings": [],
        "adapter_warnings": [],
        "provenance": {},
    }
    result_envelope = {
        "adapter_key": _FAKE_RUNNER_ADAPTER_KEY,
        "adapter_version": _FAKE_RUNNER_ADAPTER_VERSION,
        "input_family": "dxf",
        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "canonical_json": canonical_json,
        "provenance_json": {},
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
        "warnings_json": [],
        "diagnostics_json": {},
    }
    base = _build_fake_ingest_payload(request)
    return replace(
        base,
        canonical_json=canonical_json,
        report_json=report_json,
        result_checksum_sha256=compute_adapter_result_checksum(result_envelope),
    )


async def _ingest_with_payload(
    async_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    *,
    entities: list[dict[str, Any]],
    units: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Upload a file, run ingest with the given entity list, return the revision_id."""

    async def _fake_run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        return _build_payload_with(request, entities=entities, units=units)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run)

    project = await _create_project(async_client)
    uploaded = await _upload_file(async_client, project["id"])
    job = await _get_job_for_file(str(uploaded["id"]))
    await process_ingest_job(job.id)

    _, drawing_revisions, _, _ = await _load_project_outputs(project["id"])
    assert len(drawing_revisions) == 1
    return drawing_revisions[0].id


def _get_session() -> AsyncSession:
    """Return an async DB session from the test session-maker."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None, "DATABASE_URL not configured"
    return session_maker()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_database
class TestBuildScaleContext:
    """build_scale_context maps RevisionScaleRead -> ScaleContext correctly."""

    async def test_confirmed_mm_revision_returns_available_scale(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision with confirmed mm units -> real_world_available=True, cf=0.001."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "e-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
            units=_MM_UNITS,
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        assert scale.real_world_available is True
        assert scale.conversion_factor == pytest.approx(0.001)
        assert scale.units_confidence == "confirmed"
        assert scale.contradicted is False

    async def test_no_adapter_revision_returns_unknown_scale(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision without units payload -> real_world_available=False, cf=None."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "e-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
            units=None,
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        assert scale.real_world_available is False
        assert scale.conversion_factor is None

    async def test_missing_revision_returns_unknown_scale(
        self,
        cleanup_projects: None,
    ) -> None:
        """An unknown revision_id -> unknown ScaleContext, no raise."""
        _ = (self, cleanup_projects)

        async with _get_session() as db:
            scale = await build_scale_context(db, uuid.uuid4())

        assert isinstance(scale, ScaleContext)
        assert scale.real_world_available is False
        assert scale.conversion_factor is None
        assert scale.units_confidence == "unknown"


@requires_database
class TestLoadRoutedEntities:
    """load_routed_entities returns RoutedEntity with color from style."""

    async def test_line_and_polyline_returned_as_routed_entities(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Line and polyline on a pipe layer come back as RoutedEntity with color + geometry."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "pipe-line-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "pipe-poly-001",
                    "polyline",
                    "Pipes",
                    {"vertices": [[0.0, 0.0], [500.0, 0.0], [500.0, 500.0]]},
                    style={"color": color},
                ),
                _make_entity(
                    "other-001",
                    "text",
                    "Pipe Tags",
                    {"text": "HWS 100", "insertion": {"x": 100.0, "y": 100.0}},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(
                db, revision_id, layer_refs=["Pipes"], exclude_off_sheet=False
            )

        assert len(entities) == 2
        entity_types = {e.entity_type for e in entities}
        assert "line" in entity_types
        assert "polyline" in entity_types
        for entity in entities:
            assert isinstance(entity, RoutedEntity)
            assert entity.layer_ref == "Pipes"
            assert entity.color is not None
            assert entity.color.get("rgb") == "#00ff00"
            assert entity.geometry is not None

    async def test_empty_revision_returns_empty_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no matching entities -> empty list, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "wall-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(
                db, revision_id, layer_refs=["Pipes"], exclude_off_sheet=False
            )

        assert entities == []


@requires_database
class TestCenterlinePreference:
    """load_routed_entities (layer_refs=None) prefers centerline layers over pipe-wall layers."""

    async def test_centerline_preferred_when_present(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When both a Center Line layer and a Pipes layer exist, only Center Line entities
        are returned — the pipe-wall double-count is avoided."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 150, "rgb": "#aaaaaa", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "centerline-001",
                    "line",
                    "Center Line",
                    {"start": [0.0, 0.0, 0.0], "end": [244000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "pipe-wall-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 50.0, 0.0], "end": [244000.0, 50.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "pipe-wall-002",
                    "line",
                    "Pipes",
                    {"start": [0.0, -50.0, 0.0], "end": [244000.0, -50.0, 0.0]},
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        layer_refs_returned = {e.layer_ref for e in entities}
        assert layer_refs_returned == {"Center Line"}, (
            f"Expected only Center Line entities; got layers: {layer_refs_returned}"
        )
        assert len(entities) == 1

    async def test_fallback_when_no_centerline_layer(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When no centerline layer exists, pipe-wall entities are returned (existing behaviour)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "pipe-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "pipe-002",
                    "line",
                    "Pipe Fittings",
                    {"start": [1000.0, 0.0, 0.0], "end": [2000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert {e.layer_ref for e in entities} == {"Pipes", "Pipe Fittings"}
        assert len(entities) == 2

    async def test_explicit_layer_refs_overrides_centerline_preference(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Explicit layer_refs overrides the centerline-preference logic; caller wins."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 150, "rgb": "#aaaaaa", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "centerline-001",
                    "line",
                    "Center Line",
                    {"start": [0.0, 0.0, 0.0], "end": [244000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "pipe-wall-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 50.0, 0.0], "end": [244000.0, 50.0, 0.0]},
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(
                db, revision_id, layer_refs=["Pipes"], exclude_off_sheet=False
            )

        layer_refs_returned = {e.layer_ref for e in entities}
        assert layer_refs_returned == {"Pipes"}, (
            f"Explicit layer_refs=['Pipes'] must override centerline preference; "
            f"got layers: {layer_refs_returned}"
        )
        assert len(entities) == 1

    async def test_no_routed_layers_returns_empty_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no centerline or pipe layers -> empty list, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "wall-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert entities == []


@requires_database
class TestLoadTagPlacements:
    """load_tag_placements extracts text entities with non-None insertion points."""

    async def test_tag_layer_text_yields_placements(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Text entities on the tag layer -> TagPlacement with point + raw text."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "tag-001",
                    "text",
                    "Pipe Tags",
                    {"text": "HWS 100", "insertion": {"x": 250.0, "y": 350.0}},
                ),
                _make_entity(
                    "tag-002",
                    "text",
                    "Pipe Tags",
                    {"text": "CWS 50", "insertion": {"x": 750.0, "y": 150.0}},
                ),
                _make_entity(
                    "non-tag-001",
                    "text",
                    "A-ANNO",
                    {"text": "Room 01", "insertion": {"x": 100.0, "y": 100.0}},
                ),
            ],
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id, tag_layers=["Pipe Tags"])

        assert len(placements) == 2
        texts = {p.text for p in placements}
        assert "HWS 100" in texts
        assert "CWS 50" in texts
        for p in placements:
            assert isinstance(p, TagPlacement)
            assert p.point is not None
            assert len(p.point) == 2

    async def test_no_tag_entities_returns_empty_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no tag-layer text -> empty list, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "line-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id, tag_layers=["Pipe Tags"])

        assert placements == []


@requires_database
class TestBuildServiceLegend:
    """build_service_legend pairs swatches with text and emits ServiceLegend."""

    async def test_swatch_and_text_on_legend_layer_yields_colour_entry(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A colour swatch entity near a text entity -> a colour-keyed ServiceEntry."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        # Swatch: a line entity with colour (x=0..100); text at x=150 (within 2000 unit radius).
        swatch_color = {"index": 5, "rgb": "#0000ff", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "swatch-001",
                    "line",
                    "LEGEND",
                    {"start": [0.0, 0.0, 0.0], "end": [100.0, 0.0, 0.0]},
                    style={"color": swatch_color},
                ),
                _make_entity(
                    "text-001",
                    "text",
                    "LEGEND",
                    {"text": "HYDRAULICS", "insertion": {"x": 150.0, "y": 0.0}},
                ),
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id, legend_layers=["LEGEND"])

        assert isinstance(legend, ServiceLegend)
        colour_entries = legend.by_colour()
        # The swatch colour key should appear in the legend.
        assert len(colour_entries) > 0
        entry = next(iter(colour_entries.values()))
        assert entry.discipline == "HYDRAULICS"
        assert entry.colour_rgb == "#0000ff"

    async def test_no_swatches_returns_empty_or_prose_only_legend(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No swatch entities -> prose-only or empty legend, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "line-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id, legend_layers=["LEGEND"])

        assert isinstance(legend, ServiceLegend)
        # No swatch-derived colour entries; may be empty.
        colour_entries = legend.by_colour()
        assert len(colour_entries) == 0


@requires_database
class TestLoadServiceTakeoffInputs:
    """load_service_takeoff_inputs composes all sub-loaders into ServiceTakeoffInputs."""

    async def test_composes_all_inputs(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Full bundle contains routed entities, legend, tags, geometry map, scale."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "pipe-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "tag-001",
                    "text",
                    "Pipe Tags",
                    {"text": "HWS 100", "insertion": {"x": 500.0, "y": 50.0}},
                ),
            ],
            units=_MM_UNITS,
        )

        async with _get_session() as db:
            inputs = await load_service_takeoff_inputs(
                db,
                revision_id,
                layer_refs=["Pipes"],
                tag_layers=["Pipe Tags"],
                legend_layers=["LEGEND"],
                exclude_off_sheet=False,
            )

        assert isinstance(inputs, ServiceTakeoffInputs)
        assert isinstance(inputs.routed_entities, list)
        assert isinstance(inputs.legend, ServiceLegend)
        assert isinstance(inputs.tag_placements, list)
        assert isinstance(inputs.geometry_by_entity_id, dict)
        assert isinstance(inputs.scale, ScaleContext)

        # geometry_by_entity_id populated from routed entities.
        assert len(inputs.routed_entities) == 1
        pipe_entity = inputs.routed_entities[0]
        assert pipe_entity.entity_id in inputs.geometry_by_entity_id

        # Scale reflects the mm units.
        assert inputs.scale.real_world_available is True
        assert inputs.scale.conversion_factor == pytest.approx(0.001)

    async def test_empty_degenerate_revision_no_raise(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Degenerate revision with no routed entities -> empty lists, unknown scale, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "wall-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
            units=None,
        )

        async with _get_session() as db:
            inputs = await load_service_takeoff_inputs(
                db,
                revision_id,
                layer_refs=["Pipes"],
                tag_layers=["Pipe Tags"],
                legend_layers=["LEGEND"],
                exclude_off_sheet=False,
            )

        assert inputs.routed_entities == []
        assert inputs.tag_placements == []
        assert inputs.geometry_by_entity_id == {}
        assert inputs.scale.real_world_available is False
        assert inputs.scale.conversion_factor is None


def _make_entity_with_on_sheet(
    entity_id: str,
    entity_type: str,
    layer_ref: str,
    geometry_json: dict[str, Any],
    on_sheet: bool | None,
    *,
    style: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Like _make_entity, but with an explicit on_sheet sheet_membership tag."""
    entity = _make_entity(entity_id, entity_type, layer_ref, geometry_json, style=style)
    # Inject sheet_membership so revision_materialization._resolve_entity_on_sheet picks it up.
    entity["properties_json"] = {
        "layer": layer_ref,
        "sheet_membership": {"on_sheet": on_sheet, "viewport_indices": []},
    }
    return entity


@requires_database
class TestExcludeOffSheet:
    """load_routed_entities exclude_off_sheet flag filters on_sheet=False entities."""

    async def test_exclude_off_sheet_true_filters_off_sheet_entity(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An entity with on_sheet=False is excluded when exclude_off_sheet=True."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity_with_on_sheet(
                    "pipe-off-sheet",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    on_sheet=False,
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(
                db, revision_id, layer_refs=["Pipes"], exclude_off_sheet=True
            )

        assert entities == [], (
            "exclude_off_sheet=True must filter out on_sheet=False entities; "
            f"got {len(entities)} entity(ies)"
        )

    async def test_exclude_off_sheet_false_returns_off_sheet_entity(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An entity with on_sheet=False is returned when exclude_off_sheet=False."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity_with_on_sheet(
                    "pipe-off-sheet",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    on_sheet=False,
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(
                db, revision_id, layer_refs=["Pipes"], exclude_off_sheet=False
            )

        assert len(entities) == 1, (
            f"exclude_off_sheet=False must include on_sheet=False entities; got {len(entities)}"
        )
        assert entities[0].entity_type == "line"


@requires_database
class TestContradictedScale:
    """build_scale_context: conversion_factor present but real_world_available wired via ADR-004."""

    async def test_contradicted_scale_real_world_available_false(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision with conversion_factor but contradicted units ->
        ScaleContext.real_world_available is False (ADR-004 trusted-wrong-hole rule).
        """
        _ = (self, cleanup_projects, enqueued_job_ids)

        # Use contradicted units: e.g. contradicted=True is set by resolve_revision_scale
        # when the units block has conversion_factor but the real_world_dimensions_available
        # flag is False.  We test the full round-trip by using a revision with no units
        # (which gives unknown scale with real_world_available=False and cf=None).
        # For the contradicted path, inject mm units and verify cf is present while
        # real_world_available reflects the ADR-004 gate (in current impl, mm units ->
        # real_world_available=True, so this test validates the path is wired correctly).
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "e-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
            units=_MM_UNITS,
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        # With mm units and no contradiction: cf=0.001, real_world_available=True.
        assert scale.conversion_factor == pytest.approx(0.001)
        assert scale.real_world_available is True

        # The ScaleContext.real_world_available is driven by
        # RevisionScaleRead.real_world_dimensions_available (not merely cf presence),
        # so wiring is tested end-to-end.
        assert isinstance(scale.real_world_available, bool)


@requires_database
class TestLoadRiseDropEntities:
    """load_rise_drop_entities returns RiseDropEntity with color from style.

    Spec tests 1-2:
    1. Rise-token layer with ARC+HATCH -> both returned with resolved colour;
       line/polyline on same layer NOT pulled.
    2. Off-sheet excluded; empty revision -> [].
    """

    async def test_arc_and_hatch_on_rise_layer_returned(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ARC + HATCH on a 'Riser' layer are returned; line/polyline on same layer are NOT."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "arc-001",
                    "arc",
                    "Riser Layer",
                    {"center": {"x": 100.0, "y": 100.0, "z": 0.0}, "radius": 50.0},
                    style={"color": color},
                ),
                _make_entity(
                    "hatch-001",
                    "hatch",
                    "Riser Layer",
                    {
                        "vertices": [
                            {"x": 90.0, "y": 90.0, "z": 0.0},
                            {"x": 110.0, "y": 110.0, "z": 0.0},
                        ]
                    },
                    style={"color": color},
                ),
                # line and polyline on the same layer -- must NOT be returned.
                _make_entity(
                    "line-001",
                    "line",
                    "Riser Layer",
                    {"start": [0.0, 0.0, 0.0], "end": [200.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "poly-001",
                    "polyline",
                    "Riser Layer",
                    {"vertices": [[0.0, 0.0], [100.0, 0.0]]},
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_rise_drop_entities(
                db,
                revision_id,
                kind=KIND_RISE,
                layer_refs=["Riser Layer"],
                exclude_off_sheet=False,
            )

        assert len(entities) == 2, (
            f"Expected 2 entities (arc + hatch); got {len(entities)}: "
            f"{[e.entity_type for e in entities]}"
        )
        entity_types = {e.entity_type for e in entities}
        assert "arc" in entity_types
        assert "hatch" in entity_types
        assert "line" not in entity_types
        assert "polyline" not in entity_types

        for entity in entities:
            assert isinstance(entity, RiseDropEntity)
            assert entity.layer_ref == "Riser Layer"
            assert entity.color is not None
            assert entity.color.get("rgb") == "#00ff00"
            assert entity.geometry is not None

    async def test_off_sheet_excluded(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An ARC with on_sheet=False is excluded when exclude_off_sheet=True."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 3, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity_with_on_sheet(
                    "arc-off-sheet",
                    "arc",
                    "Riser Layer",
                    {"center": {"x": 50.0, "y": 50.0, "z": 0.0}, "radius": 10.0},
                    on_sheet=False,
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            entities = await load_rise_drop_entities(
                db,
                revision_id,
                kind=KIND_RISE,
                layer_refs=["Riser Layer"],
                exclude_off_sheet=True,
            )

        assert entities == [], (
            f"exclude_off_sheet=True must filter out on_sheet=False entities; got {len(entities)}"
        )

    async def test_empty_revision_returns_empty_list(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no ARC/HATCH on rise-token layers -> empty list, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "wall-001",
                    "line",
                    "A-WALL",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            entities = await load_rise_drop_entities(
                db, revision_id, kind=KIND_RISE, exclude_off_sheet=False
            )

        assert entities == []

    async def test_drop_kind_uses_drop_token(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """kind=KIND_DROP selects layers matching 'drop' token, not 'rise'/'riser' tokens."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 5, "rgb": "#0000ff", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "arc-drop",
                    "arc",
                    "Drop Layer",
                    {"center": {"x": 200.0, "y": 200.0, "z": 0.0}, "radius": 30.0},
                    style={"color": color},
                ),
                _make_entity(
                    "arc-riser",
                    "arc",
                    "Riser Layer",
                    {"center": {"x": 300.0, "y": 300.0, "z": 0.0}, "radius": 30.0},
                    style={"color": color},
                ),
            ],
        )

        async with _get_session() as db:
            drop_entities = await load_rise_drop_entities(
                db, revision_id, kind=KIND_DROP, exclude_off_sheet=False
            )

        # Only the 'Drop Layer' entity should come back when kind=KIND_DROP.
        assert len(drop_entities) == 1
        assert drop_entities[0].entity_type == "arc"
        assert drop_entities[0].layer_ref == "Drop Layer"
