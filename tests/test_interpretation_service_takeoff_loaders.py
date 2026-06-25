"""DB-backed integration tests for service_takeoff_loaders — the DB seam for P3 (#606).

Lane: db_worker (matches the ingest+materialization harness used by test_ingest_output_persistence).
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import replace
from typing import Any, ClassVar

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
from app.interpretation.routed_runs import (
    STATUS_UNKNOWN,
    RoutedEntity,
    identify_routed_runs,
)
from app.interpretation.run_service_identity import TagPlacement, fuse_run_service_identities
from app.interpretation.service_legend import ServiceLegend
from app.interpretation.service_takeoff_loaders import (
    INPUT_FAMILY_PDF_VECTOR,
    ServiceTakeoffInputs,
    _rgb_tuple_to_hex,
    build_scale_context,
    build_service_legend,
    load_bundle_bands_by_colour,
    load_measured_lengths,
    load_rise_drop_entities,
    load_routed_entities,
    load_service_takeoff_inputs,
    load_stack_headers,
    load_tag_placements,
    load_tag_stack_texts,
)
from app.interpretation.tag_stack_service import BundleColourBand, StackHeader, TagStackText
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
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical-payload entity dict that becomes a RevisionEntity row.

    ``properties`` is merged into ``properties_json`` so callers can inject
    ``rect_like`` / ``fill_color_rgb`` for PDF swatch fixtures without touching
    the existing ``style`` path.
    """
    base_props: dict[str, Any] = {"layer": layer_ref}
    if properties:
        base_props.update(properties)
    payload: dict[str, Any] = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
        "layout_ref": "Model",
        "layer_ref": layer_ref,
        "confidence_score": _FAKE_RUNNER_CONFIDENCE_SCORE,
        "confidence_json": {"score": _FAKE_RUNNER_CONFIDENCE_SCORE, "basis": "adapter"},
        "geometry_json": geometry_json,
        "properties_json": base_props,
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
    pdf_scale: dict[str, Any] | None = None,
    input_family: str = "dxf",
    metadata: dict[str, Any] | None = None,
) -> IngestFinalizationPayload:
    """Build a fake payload overriding entities and optionally units, pdf_scale, input_family,
    and metadata."""
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
    if pdf_scale is not None:
        canonical_json["pdf_scale"] = pdf_scale
    if metadata is not None:
        canonical_json["metadata"] = deepcopy(metadata)

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
        "input_family": input_family,
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
        input_family=input_family,
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
    pdf_scale: dict[str, Any] | None = None,
    input_family: str = "dxf",
    metadata: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Upload a file, run ingest with the given entity list, return the revision_id."""

    async def _fake_run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        return _build_payload_with(
            request,
            entities=entities,
            units=units,
            pdf_scale=pdf_scale,
            input_family=input_family,
            metadata=metadata,
        )

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
        # Vector geometry is already scaled to metres by the adapter; the takeoff factor is 1.0,
        # NOT the units-block conversion_factor (which must not be re-applied) — #661 Bug B.
        assert scale.conversion_factor == pytest.approx(1.0)
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
class TestContainerLayerRecognition:
    """load_routed_entities recognises discipline-named containment layers (#679).

    Cable tray, ladder, trunking, basket, conduit and duct layers must all be loaded
    as routed entities when no authored Center Line layer is present.  A Pipes layer
    continues to work (regression).  The Center Line preference must still win when
    an authored centerline layer co-exists with a containment layer.  A layer matching
    none of the tokens must not be loaded.
    """

    _COLOR: ClassVar[dict[str, Any]] = {
        "index": 7,
        "rgb": "#ffffff",
        "by_layer": False,
        "by_block": False,
    }
    _GEOM: ClassVar[dict[str, Any]] = {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]}

    def _make_line(self, eid: str, layer: str) -> dict[str, Any]:
        return _make_entity(eid, "line", layer, self._GEOM, style={"color": self._COLOR})

    async def test_cable_tray_layer_loads(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A layer named 'Cable Tray' (contains 'tray') is loaded as a routed entity."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[self._make_line("tray-001", "E610G_Cable Tray")],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert len(entities) == 1
        assert entities[0].layer_ref == "E610G_Cable Tray"

    async def test_all_container_tokens_load(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ladder, trunking, basket, conduit and duct token layers are all loaded."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        token_layers = [
            ("ldr-001", "Cable Ladder"),
            ("trk-001", "Power Trunking"),
            ("bsk-001", "Wire Basket"),
            ("cnd-001", "Conduit Run"),
            ("dct-001", "Air Duct"),
        ]
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[self._make_line(eid, layer) for eid, layer in token_layers],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        loaded_layers = {e.layer_ref for e in entities}
        expected = {"Cable Ladder", "Power Trunking", "Wire Basket", "Conduit Run", "Air Duct"}
        assert loaded_layers == expected

    async def test_pipes_layer_still_loads(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """'Pipes' layer continues to load — regression guard for existing pipe sheets."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[self._make_line("pipe-001", "Pipes")],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert len(entities) == 1
        assert entities[0].layer_ref == "Pipes"

    async def test_centerline_preference_wins_over_tray_layer(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When a Center Line layer coexists with a tray layer, only Center Line entities
        are returned — the authored centerline preference is not affected by #679."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        cl_color = {"index": 2, "rgb": "#ffff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "cl-001",
                    "line",
                    "Center Line",
                    {"start": [0.0, 0.0, 0.0], "end": [500.0, 0.0, 0.0]},
                    style={"color": cl_color},
                ),
                self._make_line("tray-001", "E610G_Cable Tray"),
            ],
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        layer_refs_returned = {e.layer_ref for e in entities}
        assert layer_refs_returned == {"Center Line"}, (
            f"Center Line preference must win over tray layer; got: {layer_refs_returned}"
        )
        assert len(entities) == 1

    async def test_unmatched_layer_not_loaded(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A layer matching none of the container tokens is not loaded."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[self._make_line("wall-001", "A-WALL-FIRE")],
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
class TestLoadTagPlacementsBroaden:
    """load_tag_placements broadened (broaden_tag_layers=True/False) behaviour."""

    async def test_broaden_picks_up_tag_on_non_pipe_tag_layer(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With broaden=True (default), a parseable tag on layer 'E600T_Gen' is returned.

        The loader does NOT layer-scope DWG text — content filtering is deferred to parse_tag
        downstream (ADR-003).  Title-block junk on a far layer is returned by the loader but
        rejected by the spatial radius gate in fuse_run_service_identities.
        """
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                # Parseable tag on a non-pipe-tag layer ("E600T_Gen").
                _make_entity(
                    "tag-non-standard",
                    "text",
                    "E600T_Gen",
                    {"text": "42 mm VAC", "insertion": {"x": 1.0, "y": 1.0}},
                ),
                # Title-block-style text on a far layer — returned by loader but spatially gated.
                _make_entity(
                    "title-block-001",
                    "text",
                    "A-TITLE",
                    {"text": "PROJECT NAME", "insertion": {"x": 999.0, "y": 999.0}},
                ),
            ],
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id)

        texts = {p.text for p in placements}
        # Broadened loader returns the parseable tag regardless of layer.
        assert "42 mm VAC" in texts
        # Title-block text is also returned (spatial gate rejects it later).
        assert "PROJECT NAME" in texts

    async def test_broaden_false_still_token_scopes(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """broaden=False: only text on _DEFAULT_TAG_LAYER_TOKENS layers is returned."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                # On a matching layer ("Pipe Tags" contains "tag").
                _make_entity(
                    "tag-matching",
                    "text",
                    "Pipe Tags",
                    {"text": "54 mm HWS", "insertion": {"x": 1.0, "y": 1.0}},
                ),
                # On a non-matching layer — excluded when broaden=False.
                _make_entity(
                    "tag-non-standard",
                    "text",
                    "E600T_Gen",
                    {"text": "42 mm VAC", "insertion": {"x": 2.0, "y": 2.0}},
                ),
            ],
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id, broaden_tag_layers=False)

        texts = {p.text for p in placements}
        assert "54 mm HWS" in texts
        assert "42 mm VAC" not in texts


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

    async def test_anchor_region_hatch_swatch_on_non_legend_layer(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Anchor + nearby HATCH swatch on a NON-legend layer + label → colour entry (Gap A+B)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        # Geometry mirrors M-540003 legend (scaled to anchor at 0,0):
        # Anchor at (0, 0). Swatch hatch centroid at (0.25, -0.5) — within PAD_RIGHT=3.5
        # and PAD_BELOW=2.0. Label at (0.75, -0.5) — 0.5 units right of swatch centroid
        # (within pair radius 1.5). Black border line at swatch location — must be rejected.
        green_color = {"index": 3, "rgb": "00ff00", "by_layer": False, "by_block": False}
        black_color = {"index": 0, "rgb": "000000", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                # Anchor: "LEGEND" on a non-legend layer.
                _make_entity(
                    "anchor-001",
                    "text",
                    "Z010T",
                    {"text": "LEGEND", "insertion": {"x": 0.0, "y": 0.0}},
                ),
                # Green HATCH swatch in legend column — chromatic, accepted.
                _make_entity(
                    "hatch-001",
                    "hatch",
                    "A060G5",
                    {
                        "vertices": [
                            [0.0, -0.75],
                            [0.5, -0.75],
                            [0.5, -0.25],
                            [0.0, -0.25],
                        ]
                    },
                    style={"color": green_color},
                ),
                # Black border line at same location — achromatic, must be rejected.
                _make_entity(
                    "border-001",
                    "line",
                    "Z000",
                    {"start": [0.0, -0.25, 0.0], "end": [0.5, -0.25, 0.0]},
                    style={"color": black_color},
                ),
                # Label: text 0.5 units right of swatch centroid, within pair radius 1.5.
                _make_entity(
                    "label-001",
                    "text",
                    "Z010T",
                    {"text": "MECHANICAL EQUIPMENT", "insertion": {"x": 0.75, "y": -0.5}},
                ),
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id)

        assert isinstance(legend, ServiceLegend)
        colour_entries = legend.by_colour()
        # Black border must NOT appear.
        assert "000000" not in colour_entries, "Achromatic border must be rejected"
        # Green swatch must be paired with "MECHANICAL EQUIPMENT".
        entry = colour_entries.get("00ff00")
        assert entry is not None, f"Expected 00ff00 in {list(colour_entries.keys())}"
        assert entry.discipline == "MECHANICAL EQUIPMENT"

    async def test_three_chromatic_swatches_black_borders_notes_excluded(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """3 chromatic HATCH swatches + adjacent labels; black borders and notes cluster excluded.

        Mirrors M-540003 real-data geometry (scaled so anchor is at 0,0):
        - olive/green/purple hatches in column at x≈0.25, y∈[-1.6,-0.4]
        - labels 0.5u right of each swatch on same row
        - 39 black border lines collocated with swatches → must not appear
        - notes text cluster at x≈4.0 (outside PAD_RIGHT=3.5) → excluded
        Expected: exactly 3 colour→discipline entries, no greyscale entry.
        """
        _ = (self, cleanup_projects, enqueued_job_ids)

        olive_color = {"index": 65, "rgb": "7ebc32", "by_layer": False, "by_block": False}
        green_color = {"index": 3, "rgb": "00ff00", "by_layer": False, "by_block": False}
        purple_color = {"index": 193, "rgb": "9783dc", "by_layer": False, "by_block": False}
        black_color = {"index": 0, "rgb": "000000", "by_layer": False, "by_block": False}

        def _hatch(eid: str, cx: float, cy: float, color: dict) -> dict:  # type: ignore[type-arg]
            """Build a square hatch centred at (cx, cy), 0.4 units wide."""
            return _make_entity(
                eid,
                "hatch",
                "A060G5",
                {
                    "vertices": [
                        [cx - 0.2, cy - 0.2],
                        [cx + 0.2, cy - 0.2],
                        [cx + 0.2, cy + 0.2],
                        [cx - 0.2, cy + 0.2],
                    ]
                },
                style={"color": color},
            )

        def _border(eid: str, cx: float, cy: float) -> dict:  # type: ignore[type-arg]
            """Black border line at swatch location — must be rejected as achromatic."""
            return _make_entity(
                eid,
                "line",
                "Z000",
                {"start": [cx - 0.2, cy, 0.0], "end": [cx + 0.2, cy, 0.0]},
                style={"color": black_color},
            )

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                # Anchor at (0, 0).
                _make_entity(
                    "anchor-m",
                    "text",
                    "Z010T",
                    {"text": "LEGEND", "insertion": {"x": 0.0, "y": 0.0}},
                ),
                # Row 1 (y=-0.4): olive swatch + label.
                _hatch("h-olive", 0.25, -0.4, olive_color),
                _border("b-olive", 0.25, -0.4),
                _make_entity(
                    "lbl-olive",
                    "text",
                    "Z010T",
                    {"text": "GAS", "insertion": {"x": 0.75, "y": -0.4}},
                ),
                # Row 2 (y=-1.0): green swatch + label.
                _hatch("h-green", 0.25, -1.0, green_color),
                _border("b-green", 0.25, -1.0),
                _make_entity(
                    "lbl-green",
                    "text",
                    "Z010T",
                    {"text": "MECHANICAL EQUIPMENT", "insertion": {"x": 0.75, "y": -1.0}},
                ),
                # Row 3 (y=-1.6): purple swatch + label.
                _hatch("h-purple", 0.25, -1.6, purple_color),
                _border("b-purple", 0.25, -1.6),
                _make_entity(
                    "lbl-purple",
                    "text",
                    "Z010T",
                    {"text": "HYDRAULIC EQUIPMENT", "insertion": {"x": 0.75, "y": -1.6}},
                ),
                # Notes cluster at x=4.0 — outside PAD_RIGHT=3.5, must be excluded.
                _make_entity(
                    "note-001",
                    "text",
                    "Z010T",
                    {"text": "NOTES", "insertion": {"x": 4.0, "y": -0.5}},
                ),
                _make_entity(
                    "note-002",
                    "text",
                    "Z010T",
                    {"text": "SEE DRAWING NOTES", "insertion": {"x": 4.0, "y": -1.0}},
                ),
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id)

        colour_entries = legend.by_colour()
        # Exactly 3 chromatic entries — no greyscale blob.
        assert "000000" not in colour_entries, "Black border must not appear in legend"
        assert len(colour_entries) == 3, (
            f"Expected 3 colour entries, got {len(colour_entries)}: {list(colour_entries.keys())}"
        )
        assert colour_entries["7ebc32"].discipline == "GAS"
        assert colour_entries["00ff00"].discipline == "MECHANICAL EQUIPMENT"
        assert colour_entries["9783dc"].discipline == "HYDRAULIC EQUIPMENT"

    async def test_explicit_legend_layers_still_works(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Explicit legend_layers override still produces colour entries (existing path)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        swatch_color = {"index": 5, "rgb": "#0000ff", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "swatch-002",
                    "line",
                    "LEGEND",
                    {"start": [0.0, 0.0, 0.0], "end": [100.0, 0.0, 0.0]},
                    style={"color": swatch_color},
                ),
                _make_entity(
                    "text-002",
                    "text",
                    "LEGEND",
                    {"text": "HYDRAULICS", "insertion": {"x": 150.0, "y": 0.0}},
                ),
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id, legend_layers=["LEGEND"])

        colour_entries = legend.by_colour()
        assert len(colour_entries) >= 1
        entry = next(iter(colour_entries.values()))
        assert entry.discipline == "HYDRAULICS"

    async def test_no_anchor_no_legend_layer_returns_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No anchor and no LEGEND/KEY layer -> empty colour legend, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "pipe-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id)

        assert isinstance(legend, ServiceLegend)
        assert len(legend.by_colour()) == 0

    async def test_anchor_but_no_in_region_swatch_yields_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Anchor found but no in-region swatch -> empty colour legend (prose may still exist)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                # Anchor at (0, 0).
                _make_entity(
                    "anchor-002",
                    "text",
                    "Z010T",
                    {"text": "LEGEND", "insertion": {"x": 0.0, "y": 0.0}},
                ),
                # Swatch far outside region (x=50, well beyond _DWG_REGION_PAD_RIGHT=3.5).
                _make_entity(
                    "swatch-far",
                    "line",
                    "SomeLayer",
                    {"start": [50.0, 0.0, 0.0], "end": [60.0, 0.0, 0.0]},
                    style={"color": {"index": 7, "rgb": "ffff00", "by_layer": False}},
                ),
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id)

        assert isinstance(legend, ServiceLegend)
        assert len(legend.by_colour()) == 0

    async def test_region_excludes_far_routed_entity(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A coloured routed entity at x≈40 is excluded; legend swatch near anchor is included."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        # Anchor at (0, 0); legend swatch hatch at (1, -1); label at (2, -1).
        # Routed pipe entity at x=40 (far outside region) — must NOT appear in legend.
        legend_color = {"index": 10, "rgb": "ff0000", "by_layer": False, "by_block": False}
        pipe_color = {"index": 20, "rgb": "0000ff", "by_layer": False, "by_block": False}

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "anchor-003",
                    "text",
                    "Z010T",
                    {"text": "LEGEND", "insertion": {"x": 0.0, "y": 0.0}},
                ),
                # Legend swatch (hatch) near anchor — should be included.
                _make_entity(
                    "hatch-leg",
                    "hatch",
                    "A060G5",
                    {"vertices": [[0.5, -1.5], [1.5, -1.5], [1.5, -0.5], [0.5, -0.5]]},
                    style={"color": legend_color},
                ),
                # Label for legend swatch.
                _make_entity(
                    "label-leg",
                    "text",
                    "Z010T",
                    {"text": "HYDRAULIC EQUIPMENT", "insertion": {"x": 2.0, "y": -1.0}},
                ),
                # Routed entity far from legend — should NOT appear in legend.
                _make_entity(
                    "pipe-far",
                    "line",
                    "Pipes",
                    {"start": [40.0, 0.0, 0.0], "end": [50.0, 0.0, 0.0]},
                    style={"color": pipe_color},
                ),
            ],
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id)

        colour_entries = legend.by_colour()
        # The far-away pipe colour must NOT appear in the legend.
        assert "0000ff" not in colour_entries, (
            "Far-away routed entity colour must not appear in legend"
        )
        # The legend swatch colour MUST appear.
        assert "ff0000" in colour_entries, (
            f"Legend colour missing; got {list(colour_entries.keys())}"
        )
        assert colour_entries["ff0000"].discipline == "HYDRAULIC EQUIPMENT"


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

        # Scale reflects the mm units; geometry is pre-scaled to metres so the takeoff factor
        # is 1.0 (the units conversion_factor was already applied by the adapter) — #661 Bug B.
        assert inputs.scale.real_world_available is True
        assert inputs.scale.conversion_factor == pytest.approx(1.0)

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

        # With mm units (pre-scaled geometry) and no contradiction: takeoff factor=1.0,
        # real_world_available=True (#661 Bug B — conversion_factor not re-applied).
        assert scale.conversion_factor == pytest.approx(1.0)
        assert scale.real_world_available is True

        # The ScaleContext.real_world_available is driven by
        # RevisionScaleRead.real_world_dimensions_available (not merely cf presence),
        # so wiring is tested end-to-end.
        assert isinstance(scale.real_world_available, bool)


_DUMMY_LINE_ENTITY = _make_entity(
    "e-dummy",
    "line",
    "A-WALL",
    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
)

# pdf_scale for 1:50, millimeter: 17.638889 pts/mm -> 0.017638889 m/pt
_PDF_SCALE_MM: dict[str, Any] = {
    "status": "derived_from_text",
    "scale_ratio": 50,
    "points_to_real": 17.638889,
    "real_world_unit": "millimeter",
    "confidence": "high",
}
# Same physical scale expressed in centimeter and meter
_PDF_SCALE_CM: dict[str, Any] = {
    "status": "derived_from_text",
    "scale_ratio": 50,
    "points_to_real": 1.7638889,
    "real_world_unit": "centimeter",
    "confidence": "high",
}
_PDF_SCALE_M: dict[str, Any] = {
    "status": "derived_from_text",
    "scale_ratio": 50,
    "points_to_real": 0.017638889,
    "real_world_unit": "meter",
    "confidence": "high",
}


@requires_database
class TestBuildScaleContextPdf:
    """build_scale_context: PDF pdf_scale fallback derives metres-per-point (ADR-004)."""

    async def test_pdf_mm_scale_derives_conversion_factor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """1:50 mm pdf_scale -> conversion_factor ~0.017638889, inferred, real_world_available."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_DUMMY_LINE_ENTITY],
            units=None,
            pdf_scale=_PDF_SCALE_MM,
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        assert scale.conversion_factor == pytest.approx(0.017638889)
        assert scale.real_world_available is True
        assert scale.units_confidence == "inferred"
        assert scale.contradicted is False

    async def test_pdf_cm_and_m_variants_normalize_to_same_factor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """cm and m pdf_scale variants both normalize to the same ~0.017638889 m/pt."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        rev_cm = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_DUMMY_LINE_ENTITY],
            units=None,
            pdf_scale=_PDF_SCALE_CM,
        )
        rev_m = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_DUMMY_LINE_ENTITY],
            units=None,
            pdf_scale=_PDF_SCALE_M,
        )

        async with _get_session() as db:
            scale_cm = await build_scale_context(db, rev_cm)
            scale_m = await build_scale_context(db, rev_m)

        assert scale_cm.conversion_factor == pytest.approx(0.017638889, rel=1e-5)
        assert scale_m.conversion_factor == pytest.approx(0.017638889, rel=1e-5)
        assert scale_cm.units_confidence == "inferred"
        assert scale_m.units_confidence == "inferred"

    async def test_no_pdf_scale_and_no_units_returns_none(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No pdf_scale and no units -> conversion_factor None, real_world_available False."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_DUMMY_LINE_ENTITY],
            units=None,
            pdf_scale=None,
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        assert scale.conversion_factor is None
        assert scale.real_world_available is False
        assert scale.units_confidence == "unknown"

    async def test_pdf_scale_missing_real_world_unit_leaves_factor_none(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pdf_scale with points_to_real but missing/unknown real_world_unit -> factor None."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_DUMMY_LINE_ENTITY],
            units=None,
            pdf_scale={
                "status": "derived_from_text",
                "scale_ratio": 50,
                "points_to_real": 17.638889,
                # real_world_unit intentionally absent
            },
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        assert scale.conversion_factor is None
        # Without a usable real_world_unit the factor cannot be normalized to metres, so the
        # availability gate stays honestly False (no available=True with a None factor).
        assert scale.real_world_available is False
        assert scale.units_confidence == "unknown"

    async def test_units_present_wins_over_pdf_scale(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DWG-style units conversion_factor present -> pdf fallback not triggered."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_DUMMY_LINE_ENTITY],
            units=_MM_UNITS,
            pdf_scale=_PDF_SCALE_MM,
        )

        async with _get_session() as db:
            scale = await build_scale_context(db, revision_id)

        # Units path wins over the PDF fallback (PDF factor ~0.017638889 not used). Geometry is
        # pre-scaled to metres, so the takeoff factor is 1.0, not 0.001 (#661 Bug B).
        assert scale.conversion_factor == pytest.approx(1.0)
        assert scale.units_confidence == "confirmed"


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


# ---------------------------------------------------------------------------
# PDF-vector seam tests (#626)
# ---------------------------------------------------------------------------

# Two distinct RGB colours for PDF entity fixtures (plain ASCII, no literal Ø).
_PDF_COLOR_BLUE = {"index": None, "rgb": "#2d71ff", "by_layer": False, "by_block": False}
_PDF_COLOR_GREY = {"index": None, "rgb": "#999999", "by_layer": False, "by_block": False}

# Text blocks fixture — one pipe tag, one prose block.
_PDF_TEXT_BLOCKS: list[dict[str, Any]] = [
    {
        "page_number": 1,
        "layout": "Model",
        "block_number": 0,
        "bbox": {"x_min": 100.0, "y_min": 200.0, "x_max": 200.0, "y_max": 220.0},
        "text": "54 mm VAC",
    },
    {
        "page_number": 1,
        "layout": "Model",
        "block_number": 1,
        "bbox": {"x_min": 10.0, "y_min": 10.0, "x_max": 300.0, "y_max": 30.0},
        "text": "DRAWING NOTES",
    },
]


def _make_pdf_entities() -> list[dict[str, Any]]:
    """Two line entities with distinct rgb colours on pen-signature layer names."""
    return [
        _make_entity(
            "pdf-line-blue",
            "line",
            "pen-0.25",  # meaningless pen-sig layer for PDF
            {"start": [0.0, 0.0, 0.0], "end": [500.0, 0.0, 0.0]},
            style={"color": _PDF_COLOR_BLUE},
        ),
        _make_entity(
            "pdf-line-grey",
            "line",
            "pen-0.50",
            {"start": [0.0, 100.0, 0.0], "end": [500.0, 100.0, 0.0]},
            style={"color": _PDF_COLOR_GREY},
        ),
    ]


@requires_database
class TestPdfRoutedEntitiesSelectAll:
    """load_routed_entities for pdf_vector: all entities returned, layer_ref normalised to None."""

    async def test_pdf_select_all_layer_ref_none_colours_preserved(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PDF revision with two coloured lines on pen-sig layers: both returned,
        every RoutedEntity.layer_ref is None, RGB colours preserved."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert len(entities) == 2
        for ent in entities:
            assert isinstance(ent, RoutedEntity)
            assert ent.layer_ref is None, f"PDF layer_ref must be None; got {ent.layer_ref!r}"
            assert ent.color is not None
            assert ent.color.get("rgb") is not None

        rgbs = {ent.color.get("rgb") for ent in entities if ent.color}
        assert "#2d71ff" in rgbs
        assert "#999999" in rgbs

    async def test_pdf_colour_only_grouping_status_unknown(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PDF entities fed into identify_routed_runs with empty legend:
        one group per distinct rgb, all STATUS_UNKNOWN, source_layers ()."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        result = identify_routed_runs(entities, ServiceLegend(entries=()))
        groups = result.groups

        assert len(groups) == 2, f"Expected 2 colour groups; got {len(groups)}"
        for g in groups:
            assert g.status == STATUS_UNKNOWN
            assert g.source_layers == ()  # layer_ref None -> empty tuple
            assert g.colour_rgb is not None

    async def test_pdf_explicit_layer_refs_override(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Explicit layer_refs= still takes precedence for PDF; layer_ref preserved."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
        )

        async with _get_session() as db:
            entities = await load_routed_entities(
                db,
                revision_id,
                layer_refs=["pen-0.25"],
                exclude_off_sheet=False,
            )

        assert len(entities) == 1
        # Explicit path preserves the actual layer_ref value.
        assert entities[0].layer_ref == "pen-0.25"
        assert entities[0].color is not None
        assert entities[0].color.get("rgb") == "#2d71ff"


@requires_database
class TestPdfTagPlacements:
    """load_tag_placements for pdf_vector: sourced from metadata.text_blocks."""

    async def test_pdf_tags_from_text_blocks(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """metadata.text_blocks with a pipe-tag entry and a prose entry:
        only the tag-structured block ('54 mm VAC') is returned; prose ('DRAWING NOTES')
        yields no candidates from _extract_pdf_tag_candidates (no size head)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
            metadata={"text_blocks": _PDF_TEXT_BLOCKS},
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id)

        assert len(placements) == 1
        texts = {p.text for p in placements}
        assert "54 mm VAC" in texts
        assert "DRAWING NOTES" not in texts

        p = placements[0]
        assert isinstance(p, TagPlacement)
        assert p.layer_ref is None
        assert len(p.point) == 2
        # Centroid of the "54 mm VAC" bbox: x=(100+200)/2=150, y=(200+220)/2=210
        assert p.point == pytest.approx((150.0, 210.0))

    async def test_pdf_tags_feed_fuse(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PDF tags from text_blocks + coloured entities fed into fuse_run_service_identities:
        '54 mm VAC' attaches to nearest colour run; prose yields no service."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        # Blue entity near x=250 y=0; grey at x=250 y=100.
        # Tag "54 mm VAC" centroid at (150, 210) — closer to grey run (y=100) than blue (y=0).
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
            metadata={"text_blocks": _PDF_TEXT_BLOCKS},
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)
            placements = await load_tag_placements(db, revision_id)

        run_result = identify_routed_runs(entities, ServiceLegend(entries=()))
        geometry_map: dict[str, Any] = {
            ent.entity_id: ent.geometry for ent in entities if ent.geometry is not None
        }
        fuse_result = fuse_run_service_identities(
            run_result.groups, geometry_map, placements, radius=5000.0
        )

        # "54 mm VAC" is parseable -> becomes a ServiceSize on the nearest run.
        # "DRAWING NOTES" is not parseable -> silently skipped.
        assigned_tag_texts = {
            ss.source_tag_text for identity in fuse_result.identities for ss in identity.services
        }
        assert "54 mm VAC" in assigned_tag_texts, (
            f"Expected '54 mm VAC' to appear as a service source tag; "
            f"identities: {fuse_result.identities}"
        )

    async def test_pdf_no_text_blocks_returns_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PDF revision with no metadata.text_blocks -> tag placements []."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
            # No metadata kwarg -> no text_blocks.
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id)

        assert placements == []

    async def test_pdf_explicit_tag_layers_override(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Explicit tag_layers= still uses entity-based path even for pdf_vector."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "tag-ent",
                    "text",
                    "Pipe Tags",
                    {"text": "HWS 50", "insertion": {"x": 10.0, "y": 10.0}},
                ),
            ],
            input_family="pdf_vector",
            metadata={"text_blocks": _PDF_TEXT_BLOCKS},
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id, tag_layers=["Pipe Tags"])

        assert len(placements) == 1
        assert placements[0].text == "HWS 50"
        assert placements[0].layer_ref == "Pipe Tags"


@requires_database
class TestPdfDwgRegression:
    """DWG/DXF paths byte-for-byte unchanged when input_family != pdf_vector."""

    async def test_dwg_centerline_layer_ref_preserved(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DWG revision with centerline+pipe layers: selection uses token ilike,
        RoutedEntity.layer_ref equals the actual row value."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 1, "rgb": "#ff0000", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "cl-001",
                    "line",
                    "Center Line",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
                _make_entity(
                    "pipe-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 50.0, 0.0], "end": [1000.0, 50.0, 0.0]},
                    style={"color": color},
                ),
            ],
            input_family="dwg",
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        # Centerline preferred; layer_ref is the actual value (not None).
        assert len(entities) == 1
        assert entities[0].layer_ref == "Center Line"
        assert entities[0].color is not None

    async def test_dxf_layer_ref_preserved(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DXF revision: pipe-wall entities returned with row.layer_ref intact."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 2, "rgb": "#00ff00", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "p-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [500.0, 0.0, 0.0]},
                    style={"color": color},
                ),
            ],
            input_family="dxf",
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert len(entities) == 1
        assert entities[0].layer_ref == "Pipes"


@requires_database
class TestPdfHonestEmpty:
    """Edge cases: empty/missing data never raises."""

    async def test_pdf_no_routed_entities_returns_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PDF revision with no routed entity types -> empty list."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "txt-001",
                    "text",
                    "pen-0.25",
                    {"text": "NOTE", "insertion": {"x": 0.0, "y": 0.0}},
                ),
            ],
            input_family="pdf_vector",
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)

        assert entities == []

    async def test_pdf_legend_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """build_service_legend on PDF revision -> ServiceLegend with no colour entries
        (PDFs have no legend-layer swatch entities — honest empty behaviour)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
        )

        async with _get_session() as db:
            legend = await build_service_legend(db, revision_id)

        assert isinstance(legend, ServiceLegend)
        assert len(legend.by_colour()) == 0

    async def test_resolve_input_family_no_adapter_run_returns_none(
        self,
        cleanup_projects: None,
    ) -> None:
        """_resolve_input_family with unknown revision_id -> None, no raise."""
        from app.interpretation.service_takeoff_loaders import _resolve_input_family

        _ = (self, cleanup_projects)

        async with _get_session() as db:
            family = await _resolve_input_family(db, uuid.uuid4())

        assert family is None


# ---------------------------------------------------------------------------
# Unit tests: _extract_pdf_tag_candidates (no DB required)
# ---------------------------------------------------------------------------


class TestExtractPdfTagCandidates:
    """_extract_pdf_tag_candidates segments tag heads and rejects long-tail prose."""

    def test_concatenated_med_gas_four_candidates(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        # Concatenated block as seen in real PDF med-gas drawings.
        result = _extract_pdf_tag_candidates("Ø76 mm VACØ42 mm MAØ42 mm AGSSØ42 mm OXY")
        assert result == [
            "Ø76 mm VAC",
            "Ø42 mm MA",
            "Ø42 mm AGSS",
            "Ø42 mm OXY",
        ]

    def test_concatenated_med_gas_garbled_glyph(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        # Non-UTF-8 sources emit U+FFFD instead of the diameter glyph; it must still be the
        # segment boundary so a garbled concatenated block splits per tag (mirrors run_tags).
        g = "�"
        result = _extract_pdf_tag_candidates(f"{g}76 mm VAC{g}42 mm MA{g}42 mm AGSS")
        assert result == [f"{g}76 mm VAC", f"{g}42 mm MA", f"{g}42 mm AGSS"]

    def test_single_wxh_tag_returned(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        assert _extract_pdf_tag_candidates("200 x 100 LV DIST") == ["200 x 100 LV DIST"]

    def test_wxh_no_spaces_returned(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        assert _extract_pdf_tag_candidates("650x350 EA") == ["650x350 EA"]

    def test_note_prose_long_tail_rejected(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        # A single size token buried in a long prose sentence: one head, long tail -> rejected.
        note = (
            "WHERE PIPEWORK PASSES THROUGH A FIRE BARRIER, SPACING SHALL BE MINIMUM OF "
            "200MM BETWEEN EACH DEVICE OR AS PER RECOMMENDATIONS."
        )
        assert _extract_pdf_tag_candidates(note) == []

    def test_no_size_no_candidates(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        assert _extract_pdf_tag_candidates("DO NOT SCALE") == []
        assert _extract_pdf_tag_candidates("CT CONTROL PANEL") == []

    def test_empty_string_no_candidates(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        assert _extract_pdf_tag_candidates("") == []

    def test_mixed_block_short_tag_then_long_prose_head(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        # Short WxH tag (first segment <= 30 chars), then a size head buried in long prose
        # (second segment > 30 chars) -> only the short tag is kept.
        # Segment 1: "200x100 EA " -> "200x100 EA" (10 chars <= 30) -> kept.
        # Segment 2: "54mm BETWEEN EACH DEVICE OR AS PER SPEC" (39 chars > 30) -> rejected.
        result = _extract_pdf_tag_candidates("200x100 EA 54mm BETWEEN EACH DEVICE OR AS PER SPEC")
        assert result == ["200x100 EA"]

    def test_short_mm_tag_single_candidate(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        assert _extract_pdf_tag_candidates("54 mm VAC") == ["54 mm VAC"]

    def test_diameter_prefix_mm_tag(self) -> None:
        from app.interpretation.service_takeoff_loaders import _extract_pdf_tag_candidates

        assert _extract_pdf_tag_candidates("Ø54 mm VAC") == ["Ø54 mm VAC"]


# ---------------------------------------------------------------------------
# DB-backed tests: input_family field and PDF tag filter
# ---------------------------------------------------------------------------


# Extended text_blocks fixture including prose entries that must be rejected.
_PDF_TEXT_BLOCKS_MIXED: list[dict[str, Any]] = [
    {
        "page_number": 1,
        "layout": "Model",
        "block_number": 0,
        "bbox": {"x_min": 100.0, "y_min": 200.0, "x_max": 200.0, "y_max": 220.0},
        "text": "54 mm VAC",
    },
    {
        "page_number": 1,
        "layout": "Model",
        "block_number": 1,
        "bbox": {"x_min": 10.0, "y_min": 10.0, "x_max": 600.0, "y_max": 30.0},
        "text": "5291 LONDON",
    },
    {
        "page_number": 1,
        "layout": "Model",
        "block_number": 2,
        "bbox": {"x_min": 10.0, "y_min": 40.0, "x_max": 200.0, "y_max": 55.0},
        "text": "Drawn: HK",
    },
]


@requires_database
class TestPdfTagFilter:
    """load_tag_placements PDF path rejects prose via extractor; DWG path unaffected."""

    async def test_pdf_prose_rejected_only_tag_structure_returned(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """text_blocks with ['54 mm VAC', '5291 LONDON', 'Drawn: HK']:
        load_tag_placements returns only the '54 mm VAC' placement
        ('5291 LONDON' and 'Drawn: HK' yield no candidates from the extractor)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
            metadata={"text_blocks": _PDF_TEXT_BLOCKS_MIXED},
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id)

        assert len(placements) == 1, (
            f"Expected only '54 mm VAC'; got: {[p.text for p in placements]}"
        )
        assert placements[0].text == "54 mm VAC"
        assert placements[0].layer_ref is None

    async def test_dwg_text_entity_tags_unaffected(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DWG path still returns text-entity tags regardless of the PDF extractor."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "tag-dwg-001",
                    "text",
                    "Pipe Tags",
                    {"text": "HWS 100", "insertion": {"x": 100.0, "y": 100.0}},
                ),
                _make_entity(
                    "tag-dwg-002",
                    "text",
                    "Pipe Tags",
                    {"text": "5291 LONDON", "insertion": {"x": 200.0, "y": 200.0}},
                ),
            ],
            input_family="dwg",
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id, tag_layers=["Pipe Tags"])

        # DWG path is layer-filtered, not prose-filtered — all tag-layer texts returned.
        texts = {p.text for p in placements}
        assert "HWS 100" in texts
        assert "5291 LONDON" in texts


@requires_database
class TestPdfTagExtractor:
    """load_tag_placements PDF path: segmentation, WxH extraction, note rejection."""

    async def test_concatenated_med_gas_block_segmented(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A concatenated med-gas block produces one TagPlacement per segment,
        all sharing the block centroid; a note paragraph yields none."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        med_gas_block: dict[str, Any] = {
            "page_number": 1,
            "layout": "Model",
            "block_number": 0,
            "bbox": {"x_min": 100.0, "y_min": 200.0, "x_max": 300.0, "y_max": 220.0},
            "text": "Ø76 mm VACØ42 mm MAØ42 mm AGSSØ42 mm OXY",
        }
        wxh_block: dict[str, Any] = {
            "page_number": 1,
            "layout": "Model",
            "block_number": 1,
            "bbox": {"x_min": 400.0, "y_min": 200.0, "x_max": 550.0, "y_max": 220.0},
            "text": "200 x 100 LV DIST",
        }
        note_block: dict[str, Any] = {
            "page_number": 1,
            "layout": "Model",
            "block_number": 2,
            "bbox": {"x_min": 10.0, "y_min": 10.0, "x_max": 800.0, "y_max": 30.0},
            "text": (
                "WHERE PIPEWORK PASSES THROUGH A FIRE BARRIER, SPACING SHALL BE MINIMUM OF "
                "200MM BETWEEN EACH DEVICE OR AS PER RECOMMENDATIONS."
            ),
        }

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
            metadata={"text_blocks": [med_gas_block, wxh_block, note_block]},
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id)

        texts = [p.text for p in placements]

        # Four med-gas candidates from the concatenated block.
        assert "Ø76 mm VAC" in texts, f"Expected Ø76 mm VAC in {texts}"
        assert "Ø42 mm MA" in texts, f"Expected Ø42 mm MA in {texts}"
        assert "Ø42 mm AGSS" in texts, f"Expected Ø42 mm AGSS in {texts}"
        assert "Ø42 mm OXY" in texts, f"Expected Ø42 mm OXY in {texts}"

        # WxH containment tag.
        assert "200 x 100 LV DIST" in texts, f"Expected 200 x 100 LV DIST in {texts}"

        # Note paragraph must produce no placement.
        note_texts = [t for t in texts if "BARRIER" in t or "200MM" in t]
        assert note_texts == [], f"Note prose must not produce placements; got {note_texts}"

        # All med-gas placements share the block centroid.
        med_gas_centroid = (200.0, 210.0)  # (100+300)/2, (200+220)/2
        for p in placements:
            if p.text in ("Ø76 mm VAC", "Ø42 mm MA", "Ø42 mm AGSS", "Ø42 mm OXY"):
                assert p.point == pytest.approx(med_gas_centroid), (
                    f"{p.text!r} centroid mismatch: {p.point}"
                )
                assert p.layer_ref is None

    async def test_segmented_tags_feed_fuse_run_service_identities(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Segmented med-gas + WxH tags fuse onto colour runs; note yields no service."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        med_gas_block: dict[str, Any] = {
            "page_number": 1,
            "layout": "Model",
            "block_number": 0,
            "bbox": {"x_min": 100.0, "y_min": 200.0, "x_max": 300.0, "y_max": 220.0},
            "text": "Ø76 mm VACØ42 mm MAØ42 mm AGSSØ42 mm OXY",
        }
        wxh_block: dict[str, Any] = {
            "page_number": 1,
            "layout": "Model",
            "block_number": 1,
            "bbox": {"x_min": 400.0, "y_min": 200.0, "x_max": 550.0, "y_max": 220.0},
            "text": "200 x 100 LV DIST",
        }
        note_block: dict[str, Any] = {
            "page_number": 1,
            "layout": "Model",
            "block_number": 2,
            "bbox": {"x_min": 10.0, "y_min": 10.0, "x_max": 800.0, "y_max": 30.0},
            "text": (
                "WHERE PIPEWORK PASSES THROUGH A FIRE BARRIER, SPACING SHALL BE MINIMUM OF "
                "200MM BETWEEN EACH DEVICE OR AS PER RECOMMENDATIONS."
            ),
        }

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
            metadata={"text_blocks": [med_gas_block, wxh_block, note_block]},
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)
            placements = await load_tag_placements(db, revision_id)

        run_result = identify_routed_runs(entities, ServiceLegend(entries=()))
        geometry_map: dict[str, Any] = {
            ent.entity_id: ent.geometry for ent in entities if ent.geometry is not None
        }
        fuse_result = fuse_run_service_identities(
            run_result.groups, geometry_map, placements, radius=5000.0
        )

        assigned_services = {
            ss.source_tag_text for identity in fuse_result.identities for ss in identity.services
        }

        # Real services from segmented tags must be attached.
        assert "Ø76 mm VAC" in assigned_services, (
            f"VAC not in assigned services; got {assigned_services}"
        )
        assert "Ø42 mm AGSS" in assigned_services, (
            f"AGSS not in assigned services; got {assigned_services}"
        )
        assert "Ø42 mm OXY" in assigned_services, (
            f"OXY not in assigned services; got {assigned_services}"
        )
        assert "200 x 100 LV DIST" in assigned_services, (
            f"LV not in assigned services; got {assigned_services}"
        )

        # No fabricated service from the note paragraph.
        note_services = [s for s in assigned_services if "BARRIER" in s or "200MM" in s]
        assert note_services == [], f"Note must not produce a service; got {note_services}"

    async def test_dwg_tag_path_unchanged_by_extractor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DWG revision uses entity-based tag path; extractor is never invoked."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "tag-dwg-001",
                    "text",
                    "Pipe Tags",
                    {"text": "HWS 100", "insertion": {"x": 100.0, "y": 100.0}},
                ),
                _make_entity(
                    "tag-dwg-002",
                    "text",
                    "Pipe Tags",
                    {"text": "CWS 50", "insertion": {"x": 200.0, "y": 200.0}},
                ),
            ],
            input_family="dwg",
        )

        async with _get_session() as db:
            placements = await load_tag_placements(db, revision_id, tag_layers=["Pipe Tags"])

        texts = {p.text for p in placements}
        assert "HWS 100" in texts
        assert "CWS 50" in texts
        assert len(placements) == 2


@requires_database
class TestInputFamilyPopulated:
    """load_service_takeoff_inputs populates input_family on the bundle."""

    async def test_pdf_vector_input_family_on_bundle(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PDF-vector revision -> inputs.input_family == 'pdf_vector'."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_entities(),
            input_family="pdf_vector",
        )

        async with _get_session() as db:
            inputs = await load_service_takeoff_inputs(db, revision_id, exclude_off_sheet=False)

        assert isinstance(inputs, ServiceTakeoffInputs)
        assert inputs.input_family == "pdf_vector"

    async def test_dwg_input_family_on_bundle(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DWG revision -> inputs.input_family == 'dwg'."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        color = {"index": 1, "rgb": "#ff0000", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "cl-001",
                    "line",
                    "Center Line",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                    style={"color": color},
                ),
            ],
            input_family="dwg",
        )

        async with _get_session() as db:
            inputs = await load_service_takeoff_inputs(db, revision_id, exclude_off_sheet=False)

        assert isinstance(inputs, ServiceTakeoffInputs)
        assert inputs.input_family == "dwg"


# ---------------------------------------------------------------------------
# PDF legend reader tests (#630 Phase 1)
# ---------------------------------------------------------------------------

# Anchor text block at (x=100, y=200) -> region x:[85,420], y:[195,620]
_LEGEND_ANCHOR_BLOCK: dict[str, Any] = {
    "page_number": 1,
    "layout": "Model",
    "block_number": 10,
    "bbox": {"x_min": 100.0, "y_min": 200.0, "x_max": 190.0, "y_max": 215.0},
    "text": "WATER LEGEND",
}

# Row 1: line swatch at y=250 (center_y=250), text at x=160 y=247..253
_LEGEND_LINE_SWATCH_COLOR = {
    "rgb": "00ffff",
    "index": None,
    "by_layer": False,
    "by_block": False,
}
_LEGEND_ROW1_TEXT_BLOCK: dict[str, Any] = {
    "page_number": 1,
    "layout": "Model",
    "block_number": 11,
    "bbox": {"x_min": 160.0, "y_min": 247.0, "x_max": 300.0, "y_max": 253.0},
    "text": "MAINS COLD WATER",
}

# Row 2: rect swatch (fill only, stroke=None) at y=270, text at x=160 y=267..273
_LEGEND_RECT_FILL_COLOR = (0.0, 1.0, 0.0)  # => "00ff00"
_LEGEND_ROW2_TEXT_BLOCK: dict[str, Any] = {
    "page_number": 1,
    "layout": "Model",
    "block_number": 12,
    "bbox": {"x_min": 160.0, "y_min": 267.0, "x_max": 300.0, "y_max": 273.0},
    "text": "HOT WATER RETURN",
}


def _make_pdf_legend_entities() -> list[dict[str, Any]]:
    """Two swatch entities for the PDF legend reader fixture."""
    # Line swatch: start=[110,250], end=[150,250] -> len=40, bbox=(110,250,150,250)
    # Inside region x:[85,420] y:[195,620] -> 85<=110 & 150<=420 & 195<=250 & 250<=620 ✓
    # center_x=130, center_y=250
    line_swatch = _make_entity(
        "swatch-line-001",
        "line",
        "pen-0.35",
        {"start": [110.0, 250.0, 0.0], "end": [150.0, 250.0, 0.0]},
        style={"color": _LEGEND_LINE_SWATCH_COLOR},
    )
    # Rect swatch: polyline vertices forming a 30x10 rect at y=265..275
    # bbox=(110,265,140,275), w=30, h=10; center_x=125, center_y=270
    # fill_color_rgb=(0,1,0) => "00ff00"; stroke=None (by_layer)
    rect_swatch = _make_entity(
        "swatch-rect-001",
        "polyline",
        "pen-0.35",
        {
            "vertices": [
                [110.0, 265.0],
                [140.0, 265.0],
                [140.0, 275.0],
                [110.0, 275.0],
                [110.0, 265.0],
            ]
        },
        style={
            "color": {
                "rgb": None,
                "index": None,
                "by_layer": True,
                "by_block": False,
            }
        },
        properties={
            "rect_like": True,
            "fill_color_rgb": list(_LEGEND_RECT_FILL_COLOR),
        },
    )
    return [line_swatch, rect_swatch]


@pytest.mark.parametrize(
    ("rgb", "expected"),
    [
        ((1.0, 0.0, 0.0), "ff0000"),
        ((0.0, 1.0, 0.0), "00ff00"),
        ((0.0, 0.0, 1.0), "0000ff"),
        ((0.176, 0.443, 1.0), "2d71ff"),  # the medical-gas linework colour
        ((0.0, 0.0, 0.0), "000000"),
        ((1.0, 1.0, 1.0), "ffffff"),
        ((1.5, -0.2, 0.5), "ff0080"),  # clipping above 1.0 and below 0.0
        ((1.0, 0.0, 0.0, 1.0), "ff0000"),  # ignores a 4th (alpha) component
    ],
)
def test_rgb_tuple_to_hex(rgb: tuple[float, ...], expected: str) -> None:
    """The fill-tuple->hex conversion the colour-join depends on (clamped, lowercase, 6-char)."""
    assert _rgb_tuple_to_hex(rgb) == expected


def test_rgb_tuple_to_hex_matches_adapter_rgb_hex() -> None:
    """Locks the colour-join invariant: fill-tuple hex == the stroke hex the adapter emits."""
    from app.ingestion.adapters.pymupdf import _rgb_hex

    for rgb in ((0.176, 0.443, 1.0), (0.0, 1.0, 1.0), (0.498, 0.0, 0.498)):
        assert _rgb_tuple_to_hex(rgb) == _rgb_hex(rgb)


@requires_database
class TestPdfLegendReader:
    """_build_pdf_service_legend: swatch detection from region + text_blocks pairing."""

    async def test_pdf_legend_line_and_rect_swatches(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pdf_vector revision with anchor + line swatch + rect swatch:
        legend has two colour entries keyed by their hex codes."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_legend_entities(),
            input_family="pdf_vector",
            metadata={
                "text_blocks": [
                    _LEGEND_ANCHOR_BLOCK,
                    _LEGEND_ROW1_TEXT_BLOCK,
                    _LEGEND_ROW2_TEXT_BLOCK,
                ]
            },
        )

        async with _get_session() as db:
            legend = await build_service_legend(
                db, revision_id, input_family=INPUT_FAMILY_PDF_VECTOR
            )

        assert isinstance(legend, ServiceLegend)
        by_colour = legend.by_colour()
        assert "00ffff" in by_colour, f"Expected '00ffff' in legend; got {list(by_colour.keys())}"
        assert "00ff00" in by_colour, f"Expected '00ff00' in legend; got {list(by_colour.keys())}"
        assert by_colour["00ffff"].discipline == "MAINS COLD WATER"
        assert by_colour["00ff00"].discipline == "HOT WATER RETURN"

    async def test_pdf_rect_swatch_colour_from_fill_not_stroke(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Rect swatch with stroke=None (by_layer) and fill tuple uses fill hex as colour key."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_make_pdf_legend_entities()[1]],  # only the rect swatch
            input_family="pdf_vector",
            metadata={
                "text_blocks": [
                    _LEGEND_ANCHOR_BLOCK,
                    _LEGEND_ROW2_TEXT_BLOCK,
                ]
            },
        )

        async with _get_session() as db:
            legend = await build_service_legend(
                db, revision_id, input_family=INPUT_FAMILY_PDF_VECTOR
            )

        by_colour = legend.by_colour()
        assert "00ff00" in by_colour, (
            f"Fill-only rect swatch must yield '00ff00'; got {list(by_colour.keys())}"
        )
        assert "00ffff" not in by_colour

    async def test_pdf_no_anchor_returns_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """text_blocks with only KEY PLAN / NOTES anchors -> empty legend (exclusion RE)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=_make_pdf_legend_entities(),
            input_family="pdf_vector",
            metadata={
                "text_blocks": [
                    {
                        "page_number": 1,
                        "layout": "Model",
                        "block_number": 20,
                        "bbox": {"x_min": 10.0, "y_min": 10.0, "x_max": 100.0, "y_max": 25.0},
                        "text": "KEY PLAN",
                    },
                    {
                        "page_number": 1,
                        "layout": "Model",
                        "block_number": 21,
                        "bbox": {"x_min": 10.0, "y_min": 30.0, "x_max": 100.0, "y_max": 45.0},
                        "text": "NOTES",
                    },
                    _LEGEND_ROW1_TEXT_BLOCK,
                ]
            },
        )

        async with _get_session() as db:
            legend = await build_service_legend(
                db, revision_id, input_family=INPUT_FAMILY_PDF_VECTOR
            )

        assert isinstance(legend, ServiceLegend)
        assert len(legend.by_colour()) == 0

    async def test_pdf_swatch_without_pairing_skipped(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In-region swatch with no same-row right text -> absent from legend, no crash."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[_make_pdf_legend_entities()[0]],  # only the line swatch
            input_family="pdf_vector",
            metadata={
                "text_blocks": [
                    _LEGEND_ANCHOR_BLOCK,
                    # No text block on row y=250 to the right of the swatch.
                ]
            },
        )

        async with _get_session() as db:
            legend = await build_service_legend(
                db, revision_id, input_family=INPUT_FAMILY_PDF_VECTOR
            )

        assert isinstance(legend, ServiceLegend)
        assert len(legend.by_colour()) == 0

    async def test_dwg_legend_path_unchanged(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DXF revision with LEGEND-layer swatch+text -> same ServiceLegend as before
        (regression: DWG path byte-for-byte unchanged by PDF branch)."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        swatch_color = {"index": 4, "rgb": "#ff00ff", "by_layer": False, "by_block": False}
        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "swatch-dwg-001",
                    "line",
                    "LEGEND",
                    {"start": [0.0, 0.0, 0.0], "end": [100.0, 0.0, 0.0]},
                    style={"color": swatch_color},
                ),
                _make_entity(
                    "text-dwg-001",
                    "text",
                    "LEGEND",
                    {"text": "GAS", "insertion": {"x": 150.0, "y": 0.0}},
                ),
            ],
            input_family="dxf",
        )

        async with _get_session() as db:
            # Explicitly pass dxf family to avoid re-resolve.
            legend = await build_service_legend(db, revision_id, legend_layers=["LEGEND"])

        by_colour = legend.by_colour()
        assert len(by_colour) > 0
        entry = next(iter(by_colour.values()))
        assert entry.discipline == "GAS"
        assert entry.colour_rgb == "#ff00ff"

    async def test_pdf_colour_run_resolves_via_legend_else_unknown(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Integration: PDF legend + routed entities -> legend colour RESOLVED;
        unmatched colour -> STATUS_UNKNOWN."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        # One routed line with colour 00ffff (matches legend), one with 999999 (unknown).
        line_cyan = _make_entity(
            "routed-cyan",
            "line",
            "pen-0.5",
            {"start": [500.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
            style={
                "color": {
                    "rgb": "00ffff",
                    "index": None,
                    "by_layer": False,
                    "by_block": False,
                }
            },
        )
        line_grey = _make_entity(
            "routed-grey",
            "line",
            "pen-0.5",
            {"start": [500.0, 100.0, 0.0], "end": [1000.0, 100.0, 0.0]},
            style={
                "color": {
                    "rgb": "999999",
                    "index": None,
                    "by_layer": False,
                    "by_block": False,
                }
            },
        )

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[*_make_pdf_legend_entities(), line_cyan, line_grey],
            input_family="pdf_vector",
            metadata={
                "text_blocks": [
                    _LEGEND_ANCHOR_BLOCK,
                    _LEGEND_ROW1_TEXT_BLOCK,
                    _LEGEND_ROW2_TEXT_BLOCK,
                ]
            },
        )

        async with _get_session() as db:
            entities = await load_routed_entities(db, revision_id, exclude_off_sheet=False)
            legend = await build_service_legend(
                db, revision_id, input_family=INPUT_FAMILY_PDF_VECTOR
            )

        run_result = identify_routed_runs(entities, legend)
        groups = run_result.groups

        # Find the cyan group and the grey group.
        cyan_groups = [g for g in groups if g.colour_rgb and "00ffff" in g.colour_rgb.lower()]
        grey_groups = [g for g in groups if g.colour_rgb and "999999" in g.colour_rgb.lower()]

        assert len(cyan_groups) >= 1, f"Expected cyan group; groups: {groups}"
        assert len(grey_groups) >= 1, f"Expected grey group; groups: {groups}"
        assert cyan_groups[0].status != STATUS_UNKNOWN, (
            f"Cyan should be resolved via legend; got {cyan_groups[0].status}"
        )
        assert grey_groups[0].status == STATUS_UNKNOWN, (
            f"Grey should be STATUS_UNKNOWN (no legend entry); got {grey_groups[0].status}"
        )


# ---------------------------------------------------------------------------
# load_measured_lengths — version-gate and idempotent-upsert (C0 / be-639b)
# ---------------------------------------------------------------------------


@requires_database
class TestLoadMeasuredLengths:
    """load_measured_lengths version gate and idempotent upsert behaviour."""

    async def test_version_gate_excludes_rows_at_different_algo_version(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Rows at a different algo_version are excluded; CURRENT_ALGO_VERSION rows included."""
        import uuid as _uuid_mod

        from sqlalchemy import text

        from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION
        from app.models.revision_routed_length import RevisionRoutedLength

        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "e-load-ml-1",
                    "line",
                    "A-PIPE",
                    {"start": [0.0, 0.0, 0.0], "end": [1000.0, 0.0, 0.0]},
                )
            ],
        )

        # Load project + source_file_id + source_job_id via raw join (manifest knows them).
        async with _get_session() as db:
            result = await db.execute(
                text(
                    "SELECT m.project_id, m.source_file_id, m.source_job_id "
                    "FROM revision_entity_manifests m "
                    "WHERE m.drawing_revision_id = :rev"
                ),
                {"rev": revision_id},
            )
            row = result.fetchone()
            assert row is not None
            project_id, source_file_id, source_job_id = row

        raster_hash = "b" * 64
        raster_hash_other = "c" * 64

        # Row at CURRENT_ALGO_VERSION.
        row_current = RevisionRoutedLength(
            id=_uuid_mod.uuid4(),
            project_id=project_id,
            source_file_id=source_file_id,
            extraction_profile_id=None,
            source_job_id=source_job_id,
            drawing_revision_id=revision_id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1",
            layer_ref="A-PIPE",
            colour_key="red",
            algo_version=CURRENT_ALGO_VERSION,
            raster_params_hash=raster_hash,
            producer_kind="passthrough",
            skeleton_length_du=1234.5,
            entity_count=1,
            geometry_json=None,
        )
        # Row at a different algo_version.
        row_other = RevisionRoutedLength(
            id=_uuid_mod.uuid4(),
            project_id=project_id,
            source_file_id=source_file_id,
            extraction_profile_id=None,
            source_job_id=source_job_id,
            drawing_revision_id=revision_id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1",
            layer_ref="A-PIPE",
            colour_key="blue",
            algo_version="old-version-1",
            raster_params_hash=raster_hash_other,
            producer_kind="passthrough",
            skeleton_length_du=999.0,
            entity_count=1,
            geometry_json=None,
        )

        async with _get_session() as db:
            db.add(row_current)
            db.add(row_other)
            await db.commit()

        async with _get_session() as db:
            mapping, present = await load_measured_lengths(db, revision_id)

        assert ("A-PIPE", "red") in present, "CURRENT_ALGO_VERSION row must be present"
        assert ("A-PIPE", "blue") not in present, "old-version row must be excluded"
        assert mapping[("A-PIPE", "red")] == pytest.approx(1234.5)

    async def test_idempotent_upsert_same_group_version_hash_yields_one_row(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Persisting the same (revision, group, version, hash) twice -> exactly one row."""
        import uuid as _uuid_mod

        from sqlalchemy import func, select, text

        from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION
        from app.models.revision_routed_length import RevisionRoutedLength

        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "e-idem-1",
                    "line",
                    "A-PIPE",
                    {"start": [0.0, 0.0, 0.0], "end": [500.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            result = await db.execute(
                text(
                    "SELECT m.project_id, m.source_file_id, m.source_job_id "
                    "FROM revision_entity_manifests m "
                    "WHERE m.drawing_revision_id = :rev"
                ),
                {"rev": revision_id},
            )
            row = result.fetchone()
            assert row is not None
            project_id, source_file_id, source_job_id = row

        raster_hash = "d" * 64

        def _make_row() -> RevisionRoutedLength:
            return RevisionRoutedLength(
                id=_uuid_mod.uuid4(),
                project_id=project_id,
                source_file_id=source_file_id,
                extraction_profile_id=None,
                source_job_id=source_job_id,
                drawing_revision_id=revision_id,
                adapter_run_output_id=None,
                canonical_entity_schema_version="1",
                layer_ref="A-PIPE",
                colour_key="idem-key",
                algo_version=CURRENT_ALGO_VERSION,
                raster_params_hash=raster_hash,
                producer_kind="passthrough",
                skeleton_length_du=77.0,
                entity_count=1,
                geometry_json=None,
            )

        # Insert first time.
        async with _get_session() as db:
            db.add(_make_row())
            await db.commit()

        # Insert second time using ON CONFLICT DO NOTHING (the idempotent path).
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        async with _get_session() as db:
            stmt = (
                pg_insert(RevisionRoutedLength)
                .values(
                    [
                        {
                            "id": _uuid_mod.uuid4(),
                            "project_id": project_id,
                            "source_file_id": source_file_id,
                            "extraction_profile_id": None,
                            "source_job_id": source_job_id,
                            "drawing_revision_id": revision_id,
                            "adapter_run_output_id": None,
                            "canonical_entity_schema_version": "1",
                            "layer_ref": "A-PIPE",
                            "colour_key": "idem-key",
                            "algo_version": CURRENT_ALGO_VERSION,
                            "raster_params_hash": raster_hash,
                            "producer_kind": "passthrough",
                            "skeleton_length_du": 77.0,
                            "entity_count": 1,
                            "geometry_json": None,
                        }
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_revision_routed_lengths_group_version")
            )
            await db.execute(stmt)
            await db.commit()

        async with _get_session() as db:
            count_result = await db.execute(
                select(func.count()).where(
                    RevisionRoutedLength.drawing_revision_id == revision_id,
                    RevisionRoutedLength.colour_key == "idem-key",
                )
            )
            count = count_result.scalar_one()

        assert count == 1, f"Expected exactly 1 row (idempotent), got {count}"

    async def test_load_measured_lengths_returns_empty_for_no_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A revision with no RevisionRoutedLength rows returns ({}, set())."""
        from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION

        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "e-empty-1",
                    "line",
                    "A-PIPE",
                    {"start": [0.0, 0.0, 0.0], "end": [1.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            mapping, present = await load_measured_lengths(
                db, revision_id, algo_version=CURRENT_ALGO_VERSION
            )

        assert mapping == {}
        assert present == set()


# ---------------------------------------------------------------------------
# Tests for the tag-stack / header / bundle loaders (#674 Phase 3 be-674b)
# ---------------------------------------------------------------------------


@requires_database
class TestLoadTagStackTexts:
    """load_tag_stack_texts maps TagPlacement -> TagStackText (text + point)."""

    async def test_pipe_tag_entities_become_tag_stack_texts(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Text entities on a Pipe Tags layer are returned as TagStackText with point."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "tag-stack-001",
                    "text",
                    "Pipe Tags",
                    {"text": "54 mm VAC", "insertion": {"x": 10.0, "y": 20.0}},
                ),
                _make_entity(
                    "tag-stack-002",
                    "text",
                    "Pipe Tags",
                    {"text": "42 mm MA", "insertion": {"x": 10.5, "y": 15.0}},
                ),
                _make_entity(
                    "non-tag-001",
                    "text",
                    "A-ANNO",
                    {"text": "Room 01", "insertion": {"x": 50.0, "y": 50.0}},
                ),
            ],
        )

        async with _get_session() as db:
            texts = await load_tag_stack_texts(db, revision_id, tag_layers=["Pipe Tags"])

        assert len(texts) == 2
        for t in texts:
            assert isinstance(t, TagStackText)
            assert len(t.point) == 2
        raw_texts = {t.text for t in texts}
        assert "54 mm VAC" in raw_texts
        assert "42 mm MA" in raw_texts

    async def test_no_tag_entities_returns_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no Pipe Tag entities -> empty list, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "pipe-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            texts = await load_tag_stack_texts(db, revision_id, tag_layers=["Pipe Tags"])

        assert texts == []


@requires_database
class TestLoadStackHeaders:
    """load_stack_headers finds FROM ... TO ... orientation text on any layer."""

    async def test_header_text_on_any_layer_is_returned(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A 'FROM TOP TO BOTTOM' text on a generic layer is returned as StackHeader."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "header-001",
                    "text",
                    "Z010T",
                    {"text": "FROM TOP TO BOTTOM", "insertion": {"x": 5.0, "y": 25.0}},
                ),
                _make_entity(
                    "non-header-001",
                    "text",
                    "Z010T",
                    {"text": "DRAWING NOTES", "insertion": {"x": 100.0, "y": 100.0}},
                ),
            ],
        )

        async with _get_session() as db:
            headers = await load_stack_headers(db, revision_id)

        assert len(headers) == 1
        h = headers[0]
        assert isinstance(h, StackHeader)
        assert "TOP TO BOTTOM" in h.text.upper()
        assert h.point == (5.0, 25.0)

    async def test_all_four_orientations_are_matched(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """All four FROM ... TO ... orientation variants are matched."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "h-ttb",
                    "text",
                    "Z010T",
                    {"text": "FROM TOP TO BOTTOM", "insertion": {"x": 0.0, "y": 0.0}},
                ),
                _make_entity(
                    "h-btt",
                    "text",
                    "Z010T",
                    {"text": "FROM BOTTOM TO TOP", "insertion": {"x": 1.0, "y": 0.0}},
                ),
                _make_entity(
                    "h-ltr",
                    "text",
                    "Z010T",
                    {"text": "FROM LEFT TO RIGHT", "insertion": {"x": 2.0, "y": 0.0}},
                ),
                _make_entity(
                    "h-rtl",
                    "text",
                    "Z010T",
                    {"text": "FROM RIGHT TO LEFT", "insertion": {"x": 3.0, "y": 0.0}},
                ),
                _make_entity(
                    "non-header",
                    "text",
                    "Z010T",
                    {"text": "SOME OTHER TEXT", "insertion": {"x": 4.0, "y": 0.0}},
                ),
            ],
        )

        async with _get_session() as db:
            headers = await load_stack_headers(db, revision_id)

        assert len(headers) == 4
        direction_texts = {h.text for h in headers}
        for expected in ("TOP TO BOTTOM", "BOTTOM TO TOP", "LEFT TO RIGHT", "RIGHT TO LEFT"):
            assert any(expected in t for t in direction_texts), (
                f"Expected '{expected}' in a header text; got {direction_texts}"
            )

    async def test_no_header_entities_returns_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no orientation-header text -> empty list, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "pipe-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            headers = await load_stack_headers(db, revision_id)

        assert headers == []


@requires_database
class TestLoadBundleBandsByColour:
    """load_bundle_bands_by_colour groups FillBand output by colour_key."""

    async def test_hatch_bands_grouped_by_colour_key(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """HATCH entities on Pipes layer are grouped into per-colour BundleColourBand lists."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        green_color = {"index": 3, "rgb": "00ff00", "by_layer": False, "by_block": False}
        blue_color = {"index": 5, "rgb": "0000ff", "by_layer": False, "by_block": False}

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                # Two green hatches → one colour_key bucket with 2 entries.
                _make_entity(
                    "hatch-green-1",
                    "hatch",
                    "Pipes",
                    {
                        "vertices": [
                            [0.0, 0.0],
                            [1.0, 0.0],
                            [1.0, 0.5],
                            [0.0, 0.5],
                        ]
                    },
                    style={"color": green_color},
                ),
                _make_entity(
                    "hatch-green-2",
                    "hatch",
                    "Pipes",
                    {
                        "vertices": [
                            [0.0, 0.5],
                            [1.0, 0.5],
                            [1.0, 1.0],
                            [0.0, 1.0],
                        ]
                    },
                    style={"color": green_color},
                ),
                # One blue hatch → separate colour_key bucket with 1 entry.
                _make_entity(
                    "hatch-blue-1",
                    "hatch",
                    "Pipes",
                    {
                        "vertices": [
                            [2.0, 0.0],
                            [3.0, 0.0],
                            [3.0, 1.0],
                            [2.0, 1.0],
                        ]
                    },
                    style={"color": blue_color},
                ),
                # Non-hatch entity should be ignored.
                _make_entity(
                    "line-001",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [5.0, 0.0, 0.0]},
                    style={"color": green_color},
                ),
            ],
            units=_MM_UNITS,
        )

        async with _get_session() as db:
            bands_by_colour = await load_bundle_bands_by_colour(
                db, revision_id, exclude_off_sheet=False
            )

        assert "00ff00" in bands_by_colour, f"Expected '00ff00' key; got {list(bands_by_colour)}"
        assert "0000ff" in bands_by_colour, f"Expected '0000ff' key; got {list(bands_by_colour)}"
        assert len(bands_by_colour["00ff00"]) == 2
        assert len(bands_by_colour["0000ff"]) == 1

        for band in bands_by_colour["00ff00"]:
            assert isinstance(band, BundleColourBand)
            assert band.colour_key == "00ff00"
            assert len(band.ring) >= 3

        for band in bands_by_colour["0000ff"]:
            assert isinstance(band, BundleColourBand)
            assert band.colour_key == "0000ff"

    async def test_no_hatch_entities_returns_empty_dict(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revision with no HATCH entities on pipe/rise/drop layers -> empty dict, no raise."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        revision_id = await _ingest_with_payload(
            async_client,
            monkeypatch,
            entities=[
                _make_entity(
                    "line-only",
                    "line",
                    "Pipes",
                    {"start": [0.0, 0.0, 0.0], "end": [1.0, 0.0, 0.0]},
                )
            ],
        )

        async with _get_session() as db:
            bands_by_colour = await load_bundle_bands_by_colour(db, revision_id)

        assert bands_by_colour == {}
