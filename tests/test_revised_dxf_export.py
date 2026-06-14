"""Integration tests for deterministic revised DXF export rendering."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.exports import revised_dxf as revised_dxf_module
from app.exports.revised_dxf import (
    REVISED_DXF_EXPORT_GENERATOR_NAME,
    REVISED_DXF_EXPORT_GENERATOR_VERSION,
    REVISED_DXF_EXPORT_MEDIA_TYPE,
    RevisedDxfExportError,
    RevisedDxfExportResult,
    _load_real_world_scale,
    render_revised_dxf_export,
)
from app.ingestion.contracts import (
    AdapterAvailability,
    AdapterStatus,
    AvailabilityReason,
)
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import CadChangeSet
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File
from app.models.job import Job, JobStatus, JobType
from app.models.project import Project
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)
from tests.conftest import requires_database

pytestmark = [pytest.mark.asyncio, requires_database]

_FIXTURE_UUID_NAMESPACE = uuid.UUID("b0fbf345-22f4-4107-9982-72587ad61be8")
_EXPECTED_REVISED_DXF_CHECKSUM = "dd76eb82953872c35d8bdfafc702dfc285fa758f397b3f0abec664c0bb81418d"


@dataclass(frozen=True, slots=True)
class SeededRevisionFixture:
    project: Project
    source_file: File
    extraction_profile: ExtractionProfile
    source_job: Job
    adapter_output: AdapterRunOutput
    base_revision: DrawingRevision
    revision: DrawingRevision
    manifest: RevisionEntityManifest | None


class _UnavailableAdapter:
    def probe(self) -> AdapterAvailability:
        return AdapterAvailability(
            status=AdapterStatus.UNAVAILABLE,
            availability_reason=AvailabilityReason.PROBE_FAILED,
            details={"dependency": "ezdxf"},
        )

    async def export(self, request: object, options: object) -> object:
        del request, options
        raise AssertionError("export should not be called when the adapter is unavailable")


class _AvailableAdapter:
    def probe(self) -> AdapterAvailability:
        return AdapterAvailability(status=AdapterStatus.AVAILABLE)


class _AdapterContractError(RuntimeError):
    code: ClassVar[str] = "EXPORT_FAILED"
    details: ClassVar[dict[str, str]] = {"stage": "export"}


class _ContractFailingAdapter(_AvailableAdapter):
    async def export(self, request: object, options: object) -> object:
        del request, options
        raise _AdapterContractError("adapter contract failed")


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


async def test_render_revised_dxf_export_is_deterministic_and_returns_artifact_metadata(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(
        db_session,
        layers=(
            {"layer_ref": "Layer-B", "sequence_index": 1, "payload": {"name": "Layer-B"}},
            {"layer_ref": "Layer-A", "sequence_index": 0, "payload": {"name": "Layer-A"}},
        ),
        entities=(
            {
                "entity_id": "polyline-b",
                "entity_type": "lwpolyline",
                "entity_schema_version": "1.0",
                "sequence_index": 1,
                "layout_ref": "Model",
                "layer_ref": "Layer-B",
                "geometry": {
                    "points": [
                        {"x": 0.0, "y": 0.0},
                        {"x": 1.5, "y": 0.0},
                        {"x": 1.5, "y": 1.0},
                    ],
                    "units": {"normalized": "m"},
                },
                "properties": {"closed": False},
                "canonical_entity": {
                    "entity_type": "lwpolyline",
                    "geometry": {
                        "points": [
                            {"x": 0.0, "y": 0.0},
                            {"x": 1.5, "y": 0.0},
                            {"x": 1.5, "y": 1.0},
                        ],
                        "units": {"normalized": "m"},
                    },
                    "properties": {"closed": False},
                },
            },
            {
                "entity_id": "line-a",
                "entity_type": "line",
                "entity_schema_version": "1.0",
                "sequence_index": 0,
                "layout_ref": "Model",
                "layer_ref": "Layer-A",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0},
                    "end": {"x": 2.0, "y": 0.0},
                    "units": {"normalized": "m"},
                },
                "properties": {},
                "canonical_entity": {
                    "entity_type": "line",
                    "geometry": {
                        "start": {"x": 0.0, "y": 0.0},
                        "end": {"x": 2.0, "y": 0.0},
                        "units": {"normalized": "m"},
                    },
                    "properties": {},
                },
            },
        ),
    )
    options = {
        "exported_at": datetime(2026, 6, 2, 18, 45, tzinfo=UTC),
        "label": "deterministic",
    }

    first = await render_revised_dxf_export(db_session, seeded.revision.id, options=options)
    second = await render_revised_dxf_export(db_session, seeded.revision.id, options=options)

    content_text = first.content_bytes.decode("utf-8")

    assert first == second
    assert first == RevisedDxfExportResult(
        content_bytes=first.content_bytes,
        checksum_sha256=_EXPECTED_REVISED_DXF_CHECKSUM,
        size_bytes=len(first.content_bytes),
        media_type=REVISED_DXF_EXPORT_MEDIA_TYPE,
        generator_name=REVISED_DXF_EXPORT_GENERATOR_NAME,
        generator_version=REVISED_DXF_EXPORT_GENERATOR_VERSION,
        options={
            "exported_at": "2026-06-02T18:45:00Z",
            "label": "deterministic",
        },
    )
    assert first.media_type == REVISED_DXF_EXPORT_MEDIA_TYPE
    assert first.checksum_sha256 == _EXPECTED_REVISED_DXF_CHECKSUM
    assert "LINE" in content_text
    assert "LWPOLYLINE" in content_text
    assert content_text.index("Layer-A") < content_text.index("Layer-B")
    assert content_text.index("LINE") < content_text.index("LWPOLYLINE")


async def test_render_revised_dxf_export_raises_for_missing_manifest(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(db_session, with_manifest=False)

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.revision.id)

    assert exc_info.value.code == "MANIFEST_NOT_FOUND"
    assert exc_info.value.details == {"revision_id": str(seeded.revision.id)}


async def test_render_revised_dxf_export_rejects_non_changeset_origin_revision(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(db_session)

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.base_revision.id)

    assert exc_info.value.code == "INPUT_INVALID"
    assert exc_info.value.details == {
        "revision_id": str(seeded.base_revision.id),
        "revision_kind": "ingest",
    }


async def test_load_real_world_scale_reads_confident_pdf_scale(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(
        db_session,
        adapter_metadata={
            "pdf_scale": {
                "real_world_units": True,
                "real_world_unit": "millimeter",
                "points_to_real": 17.638889,
            }
        },
    )

    scale = await _load_real_world_scale(db_session, seeded.base_revision)
    assert scale is not None
    assert scale.unit == "millimeter"
    assert scale.points_to_real == 17.638889

    # The changeset revision has no adapter run output -> no real-world scale.
    assert await _load_real_world_scale(db_session, seeded.revision) is None


async def test_load_real_world_scale_none_when_scale_unconfirmed(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(
        db_session,
        adapter_metadata={"pdf_scale": {"real_world_units": False}},
    )

    assert await _load_real_world_scale(db_session, seeded.base_revision) is None


async def test_render_dxf_export_allows_base_revision_when_changeset_not_required(
    db_session: AsyncSession,
) -> None:
    # Base-revision DXF export (#411) renders any active revision: the changeset-origin guard
    # is bypassed. The fixture seeds materialization rows only for the changeset revision, so a
    # base render gets past the guard to MANIFEST_NOT_FOUND rather than rejecting on INPUT_INVALID.
    seeded = await _seed_revision_fixture(db_session)

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(
            db_session, seeded.base_revision.id, require_changeset_origin=False
        )

    assert exc_info.value.code != "INPUT_INVALID"
    assert exc_info.value.code == "MANIFEST_NOT_FOUND"


async def test_render_revised_dxf_export_raises_for_missing_materialization_rows(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(
        db_session,
        manifest_counts={"layouts": 1, "layers": 1, "blocks": 0, "entities": 1},
        layouts=({"layout_ref": "Model", "sequence_index": 0, "payload": {"name": "Model"}},),
        layers=({"layer_ref": "Layer-A", "sequence_index": 0, "payload": {"name": "Layer-A"}},),
        entities=(),
    )

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.revision.id)

    assert exc_info.value.code == "MATERIALIZATION_MISSING"
    assert exc_info.value.details == {
        "revision_id": str(seeded.revision.id),
        "component": "entities",
        "expected_count": 1,
        "actual_count": 0,
    }


async def test_render_revised_dxf_export_raises_for_unavailable_adapter(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seeded = await _seed_revision_fixture(db_session)

    def _load_unavailable_adapter(descriptor: object) -> _UnavailableAdapter:
        del descriptor
        return _UnavailableAdapter()

    monkeypatch.setattr(revised_dxf_module, "load_export_adapter", _load_unavailable_adapter)

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.revision.id)

    assert exc_info.value.code == "ADAPTER_UNAVAILABLE"
    assert exc_info.value.details == {
        "adapter_key": "ezdxf_writer",
        "output_format": "revised_dxf",
        "status": "unavailable",
        "availability_reason": "probe_failed",
        "license_state": "not_required",
        "issues": [],
        "observed": [],
        "details": {"dependency": "ezdxf"},
    }


async def test_render_revised_dxf_export_raises_for_adapter_load_failure(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seeded = await _seed_revision_fixture(db_session)

    def _raise_load_failure(descriptor: object) -> object:
        del descriptor
        raise RuntimeError("boom")

    monkeypatch.setattr(revised_dxf_module, "load_export_adapter", _raise_load_failure)

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.revision.id)

    assert exc_info.value.code == "ADAPTER_LOAD_FAILED"
    assert exc_info.value.details == {
        "adapter_key": "ezdxf_writer",
        "output_format": "revised_dxf",
        "error_type": "RuntimeError",
    }


async def test_render_revised_dxf_export_maps_unexpected_adapter_contract_failure(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seeded = await _seed_revision_fixture(db_session)

    def _load_contract_failing_adapter(descriptor: object) -> _ContractFailingAdapter:
        del descriptor
        return _ContractFailingAdapter()

    monkeypatch.setattr(revised_dxf_module, "load_export_adapter", _load_contract_failing_adapter)

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.revision.id)

    assert exc_info.value.code == "ADAPTER_FAILED"
    assert exc_info.value.details == {
        "adapter_key": "ezdxf_writer",
        "output_format": "revised_dxf",
        "stage": "export",
        "original_error_code": "EXPORT_FAILED",
    }


async def test_render_revised_dxf_export_wraps_unsupported_writer_payload(
    db_session: AsyncSession,
) -> None:
    # Blocks + INSERTs are now supported; a genuinely unsupported entity type (spline) still
    # surfaces from the writer and is wrapped as a RevisedDxfExportError.
    seeded = await _seed_revision_fixture(
        db_session,
        entities=(
            {
                "entity_id": "spline-a",
                "entity_type": "spline",
                "entity_schema_version": "1.0",
                "sequence_index": 0,
                "layout_ref": "Model",
                "layer_ref": "Layer-A",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0},
                    "end": {"x": 2.0, "y": 0.0},
                    "units": {"normalized": "m"},
                },
                "properties": {},
                "canonical_entity": {
                    "entity_type": "spline",
                    "geometry": {
                        "start": {"x": 0.0, "y": 0.0},
                        "end": {"x": 2.0, "y": 0.0},
                        "units": {"normalized": "m"},
                    },
                    "properties": {},
                },
            },
        ),
    )

    with pytest.raises(RevisedDxfExportError) as exc_info:
        await render_revised_dxf_export(db_session, seeded.revision.id)

    assert exc_info.value.code == "UNSUPPORTED_ENTITY_TYPE"
    assert exc_info.value.details["adapter_key"] == "ezdxf_writer"
    assert exc_info.value.details["output_format"] == "revised_dxf"
    assert exc_info.value.details["entity_type"] == "spline"


async def _seed_revision_fixture(
    db_session: AsyncSession,
    *,
    with_manifest: bool = True,
    manifest_counts: Mapping[str, int] | None = None,
    adapter_metadata: Mapping[str, Any] | None = None,
    layouts: tuple[Mapping[str, Any], ...] = (
        {"layout_ref": "Model", "sequence_index": 0, "payload": {"name": "Model"}},
    ),
    layers: tuple[Mapping[str, Any], ...] = (
        {"layer_ref": "Layer-A", "sequence_index": 0, "payload": {"name": "Layer-A"}},
    ),
    blocks: tuple[Mapping[str, Any], ...] = (),
    entities: tuple[Mapping[str, Any], ...] = (
        {
            "entity_id": "line-a",
            "entity_type": "line",
            "entity_schema_version": "1.0",
            "sequence_index": 0,
            "layout_ref": "Model",
            "layer_ref": "Layer-A",
            "geometry": {
                "start": {"x": 0.0, "y": 0.0},
                "end": {"x": 1.0, "y": 0.0},
                "units": {"normalized": "m"},
            },
            "properties": {},
            "canonical_entity": {
                "entity_type": "line",
                "geometry": {
                    "start": {"x": 0.0, "y": 0.0},
                    "end": {"x": 1.0, "y": 0.0},
                    "units": {"normalized": "m"},
                },
                "properties": {},
            },
        },
    ),
) -> SeededRevisionFixture:
    project = Project(
        id=_fixture_uuid("project"),
        name="Revised DXF Export Project",
        description="Fixture project",
    )
    source_file = File(
        id=_fixture_uuid("source_file"),
        project_id=project.id,
        original_filename="fixture.dxf",
        media_type="application/dxf",
        detected_format="dxf",
        storage_uri="file:///tmp/fixture.dxf",
        size_bytes=128,
        checksum_sha256="a" * 64,
        immutable=True,
        created_at=datetime(2026, 6, 2, 18, 0, tzinfo=UTC),
    )
    extraction_profile = ExtractionProfile(
        id=_fixture_uuid("extraction_profile"),
        project_id=project.id,
        profile_version="1.0",
        layout_mode="all",
        xref_handling="embed",
        block_handling="expand",
        text_extraction=True,
        dimension_extraction=True,
        confidence_threshold=0.6,
        created_at=datetime(2026, 6, 2, 18, 1, tzinfo=UTC),
    )
    source_job = Job(
        id=_fixture_uuid("source_job"),
        project_id=project.id,
        file_id=source_file.id,
        extraction_profile_id=extraction_profile.id,
        base_revision_id=None,
        parent_job_id=None,
        job_type=JobType.INGEST.value,
        status=JobStatus.SUCCEEDED.value,
        attempts=1,
        max_attempts=3,
        enqueue_status="published",
        enqueue_attempts=1,
        cancel_requested=False,
        created_at=datetime(2026, 6, 2, 18, 2, tzinfo=UTC),
        finished_at=datetime(2026, 6, 2, 18, 8, tzinfo=UTC),
    )
    adapter_output = AdapterRunOutput(
        id=_fixture_uuid("adapter_output"),
        project_id=project.id,
        source_file_id=source_file.id,
        extraction_profile_id=extraction_profile.id,
        source_job_id=source_job.id,
        adapter_key="tests.fake",
        adapter_version="1.0",
        input_family="dxf",
        canonical_entity_schema_version="1.0",
        canonical_json={
            "entities": [],
            "layers": [],
            "layouts": [],
            "blocks": [],
            **({"metadata": dict(adapter_metadata)} if adapter_metadata is not None else {}),
        },
        provenance_json={"adapter": {"key": "tests.fake", "version": "1.0"}},
        confidence_json={"score": 0.875},
        confidence_score=0.875,
        warnings_json=[],
        diagnostics_json={"duration_ms": 3},
        result_checksum_sha256="b" * 64,
        created_at=datetime(2026, 6, 2, 18, 4, tzinfo=UTC),
    )
    base_revision = DrawingRevision(
        id=_fixture_uuid("base_revision"),
        project_id=project.id,
        source_file_id=source_file.id,
        extraction_profile_id=extraction_profile.id,
        source_job_id=source_job.id,
        adapter_run_output_id=adapter_output.id,
        predecessor_revision_id=None,
        revision_sequence=1,
        revision_kind="ingest",
        review_state="approved",
        canonical_entity_schema_version="1.0",
        confidence_score=0.875,
        created_at=datetime(2026, 6, 2, 18, 5, tzinfo=UTC),
    )
    changeset_source_job = Job(
        id=_fixture_uuid("changeset_source_job"),
        project_id=project.id,
        file_id=source_file.id,
        extraction_profile_id=None,
        base_revision_id=base_revision.id,
        parent_job_id=source_job.id,
        job_type=JobType.CHANGESET_APPLY.value,
        status=JobStatus.SUCCEEDED.value,
        attempts=1,
        max_attempts=3,
        enqueue_status="published",
        enqueue_attempts=1,
        cancel_requested=False,
        created_at=datetime(2026, 6, 2, 18, 8, tzinfo=UTC),
        finished_at=datetime(2026, 6, 2, 18, 9, tzinfo=UTC),
    )
    changeset = CadChangeSet(
        id=_fixture_uuid("changeset"),
        project_id=project.id,
        base_revision_id=base_revision.id,
        status="applied",
        created_by="test",
        created_at=datetime(2026, 6, 2, 18, 9, tzinfo=UTC),
    )
    revision = DrawingRevision(
        id=_fixture_uuid("revision"),
        project_id=project.id,
        source_file_id=source_file.id,
        extraction_profile_id=None,
        source_job_id=changeset_source_job.id,
        adapter_run_output_id=None,
        changeset_id=changeset.id,
        predecessor_revision_id=base_revision.id,
        revision_sequence=2,
        revision_kind="changeset",
        review_state="approved",
        canonical_entity_schema_version="1.0",
        confidence_score=0.875,
        created_at=datetime(2026, 6, 2, 18, 10, tzinfo=UTC),
    )

    db_session.add(project)
    await db_session.flush()
    db_session.add(source_file)
    db_session.add(extraction_profile)
    await db_session.flush()
    db_session.add(source_job)
    await db_session.flush()
    db_session.add(adapter_output)
    await db_session.flush()
    db_session.add(base_revision)
    await db_session.flush()
    db_session.add(changeset_source_job)
    db_session.add(changeset)
    await db_session.flush()
    db_session.add(revision)
    await db_session.flush()

    manifest: RevisionEntityManifest | None = None
    if with_manifest:
        counts = {
            "layouts": len(layouts),
            "layers": len(layers),
            "blocks": len(blocks),
            "entities": len(entities),
        }
        if manifest_counts is not None:
            counts.update({key: int(value) for key, value in manifest_counts.items()})
        manifest = RevisionEntityManifest(
            id=_fixture_uuid("manifest"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=changeset_source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1.0",
            counts_json=counts,
            created_at=datetime(2026, 6, 2, 18, 11, tzinfo=UTC),
        )
        db_session.add(manifest)
        await db_session.flush()

    created_layouts: list[RevisionLayout] = []
    for layout_index, spec in enumerate(layouts):
        created_layout = RevisionLayout(
            id=_fixture_uuid(f"layout:{layout_index}"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=changeset_source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            payload_json=dict(spec["payload"]),
            layout_ref=str(spec["layout_ref"]),
            created_at=datetime(2026, 6, 2, 18, 11, tzinfo=UTC),
        )
        db_session.add(created_layout)
        created_layouts.append(created_layout)
    await db_session.flush()

    created_layers: list[RevisionLayer] = []
    for layer_index, spec in enumerate(layers):
        created_layer = RevisionLayer(
            id=_fixture_uuid(f"layer:{layer_index}"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=changeset_source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            payload_json=dict(spec["payload"]),
            layer_ref=str(spec["layer_ref"]),
            created_at=datetime(2026, 6, 2, 18, 11, tzinfo=UTC),
        )
        db_session.add(created_layer)
        created_layers.append(created_layer)
    await db_session.flush()

    created_blocks: list[RevisionBlock] = []
    for block_index, spec in enumerate(blocks):
        created_block = RevisionBlock(
            id=_fixture_uuid(f"block:{block_index}"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=changeset_source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            payload_json=dict(spec["payload"]),
            block_ref=str(spec["block_ref"]),
            created_at=datetime(2026, 6, 2, 18, 11, tzinfo=UTC),
        )
        db_session.add(created_block)
        created_blocks.append(created_block)
    await db_session.flush()

    layout_by_ref = {layout.layout_ref: layout for layout in created_layouts}
    layer_by_ref = {layer.layer_ref: layer for layer in created_layers}
    block_by_ref = {block.block_ref: block for block in created_blocks}

    for entity_index, spec in enumerate(entities):
        layout_ref = _optional_str(spec.get("layout_ref"))
        layer_ref = _optional_str(spec.get("layer_ref"))
        block_ref = _optional_str(spec.get("block_ref"))
        entity = RevisionEntity(
            id=_fixture_uuid(f"entity:{entity_index}"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=None,
            source_job_id=changeset_source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=None,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            entity_id=str(spec["entity_id"]),
            entity_type=str(spec["entity_type"]),
            entity_schema_version=str(spec["entity_schema_version"]),
            parent_entity_ref=None,
            confidence_score=1.0,
            confidence_json={},
            geometry_json=dict(spec["geometry"]),
            properties_json=dict(spec["properties"]),
            provenance_json={"origin": "adapter_normalized"},
            canonical_entity_json=_optional_dict(spec.get("canonical_entity")),
            layout_ref=layout_ref,
            layer_ref=layer_ref,
            block_ref=block_ref,
            source_identity=None,
            source_hash=None,
            layout_id=None if layout_ref is None else layout_by_ref[layout_ref].id,
            layer_id=None if layer_ref is None else layer_by_ref[layer_ref].id,
            block_id=None if block_ref is None else block_by_ref[block_ref].id,
            parent_entity_row_id=None,
            created_at=datetime(2026, 6, 2, 18, 12, tzinfo=UTC),
        )
        db_session.add(entity)
    await db_session.commit()

    return SeededRevisionFixture(
        project=project,
        source_file=source_file,
        extraction_profile=extraction_profile,
        source_job=source_job,
        adapter_output=adapter_output,
        base_revision=base_revision,
        revision=revision,
        manifest=manifest,
    )


def _optional_dict(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    assert isinstance(value, dict)
    return dict(value)


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _fixture_uuid(name: str) -> uuid.UUID:
    return uuid.uuid5(_FIXTURE_UUID_NAMESPACE, f"revised_dxf_export:{name}")
