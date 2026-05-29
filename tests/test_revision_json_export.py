"""Integration tests for deterministic revision JSON export rendering."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

import app.db.session as session_module
from app.exports.revision_json import (
    REVISION_JSON_EXPORT_GENERATOR_NAME,
    REVISION_JSON_EXPORT_GENERATOR_VERSION,
    REVISION_JSON_EXPORT_MEDIA_TYPE,
    REVISION_JSON_EXPORT_SCHEMA_VERSION,
    RevisionJsonExportError,
    RevisionJsonExportResult,
    render_revision_json_export,
)
from app.models.adapter_run_output import AdapterRunOutput
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

_FIXTURE_UUID_NAMESPACE = uuid.UUID("a6f84dd8-3f4c-4f81-a4f9-2e8a14f7df70")
_EXPECTED_REVISION_JSON_CHECKSUM = (
    "5090d3538da96662cef7629a7536507b39b3c3ce2a5d34a524e2f5ae10c60056"
)


@dataclass(frozen=True, slots=True)
class SeededRevisionFixture:
    project: Project
    source_file: File
    extraction_profile: ExtractionProfile
    source_job: Job
    adapter_output: AdapterRunOutput
    revision: DrawingRevision
    manifest: RevisionEntityManifest | None


@pytest_asyncio.fixture
async def db_session(cleanup_projects: None) -> AsyncGenerator[AsyncSession, None]:
    _ = cleanup_projects
    session_maker = session_module.AsyncSessionLocal
    if session_maker is None:
        raise RuntimeError("Database is not configured. Set DATABASE_URL environment variable.")

    async with session_maker() as session:
        yield session
        await session.rollback()


async def test_render_revision_json_export_is_deterministic_and_canonical(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(
        db_session,
        layouts=(
            {"layout_ref": "Layout-B", "sequence_index": 1, "payload": {"z": 2, "a": 1}},
            {
                "layout_ref": "Layout-A",
                "sequence_index": 0,
                "payload": {"nested": {"z": 2, "a": 1}, "title": "Résumé"},
            },
        ),
        layers=(
            {"layer_ref": "Layer-B", "sequence_index": 1, "payload": {"z": 4, "a": 3}},
            {"layer_ref": "Layer-A", "sequence_index": 0, "payload": {"y": [2, 1]}},
        ),
        blocks=(
            {
                "block_ref": "Block-A",
                "sequence_index": 0,
                "payload": {"nested": {"m": 2, "a": 1}, "name": "Door"},
            },
        ),
        entities=(
            {
                "entity_id": "entity-b",
                "entity_type": "line",
                "entity_schema_version": "1.1",
                "sequence_index": 1,
                "parent_entity_ref": None,
                "layout_ref": "Layout-B",
                "layer_ref": "Layer-B",
                "block_ref": None,
                "source_identity": "src-b",
                "source_hash": "b" * 64,
                "confidence_score": 0.9,
                "confidence": {"z": 2, "a": 1},
                "geometry": {"z": [3, 2], "a": [1, 0]},
                "properties": {"z": 9, "a": 8},
                "provenance": {"origin": "adapter_normalized", "notes": ["β", "\u03b1"]},
                "canonical_entity": {
                    "entity_type": "line",
                    "geometry": {"z": [3, 2], "a": [1, 0]},
                },
            },
            {
                "entity_id": "entity-a",
                "entity_type": "insert",
                "entity_schema_version": "1.0",
                "sequence_index": 0,
                "parent_entity_ref": None,
                "layout_ref": "Layout-A",
                "layer_ref": "Layer-A",
                "block_ref": "Block-A",
                "source_identity": "src-a",
                "source_hash": None,
                "confidence_score": 0.75,
                "confidence": {"status": "ok", "a": 1},
                "geometry": {"coords": [{"y": 2, "x": 1}]},
                "properties": {"text": "αβ", "name": "Door"},
                "provenance": {"adapter": {"version": 1, "key": "fake"}},
                "canonical_entity": None,
            },
        ),
    )
    options = {
        "zeta": [{"b": 2, "a": 1}],
        "alpha": "β",
        "exported_at": datetime(2026, 5, 27, 16, 30, tzinfo=UTC),
    }

    assert seeded.manifest is not None

    first = await render_revision_json_export(db_session, seeded.revision.id, options=options)
    second = await render_revision_json_export(db_session, seeded.revision.id, options=options)

    expected_payload = {
        "schema_version": REVISION_JSON_EXPORT_SCHEMA_VERSION,
        "generator": {
            "name": REVISION_JSON_EXPORT_GENERATOR_NAME,
            "version": REVISION_JSON_EXPORT_GENERATOR_VERSION,
            "options": {
                "zeta": [{"b": 2, "a": 1}],
                "alpha": "β",
                "exported_at": "2026-05-27T16:30:00Z",
            },
        },
        "revision": {
            "id": str(seeded.revision.id),
            "project_id": str(seeded.revision.project_id),
            "source_file_id": str(seeded.revision.source_file_id),
            "extraction_profile_id": str(seeded.revision.extraction_profile_id),
            "source_job_id": str(seeded.revision.source_job_id),
            "adapter_run_output_id": str(seeded.revision.adapter_run_output_id),
            "predecessor_revision_id": None,
            "revision_sequence": 1,
            "revision_kind": "ingest",
            "review_state": "approved",
            "canonical_entity_schema_version": "1.0",
            "confidence_score": 0.875,
            "created_at": "2026-05-27T16:05:00Z",
        },
        "manifest": {
            "id": str(seeded.manifest.id),
            "counts": {"layouts": 2, "layers": 2, "blocks": 1, "entities": 2},
            "canonical_entity_schema_version": "1.0",
            "created_at": "2026-05-27T16:06:00Z",
        },
        "layouts": [
            {
                "layout_ref": "Layout-A",
                "sequence_index": 0,
                "payload": {"nested": {"z": 2, "a": 1}, "title": "Résumé"},
            },
            {"layout_ref": "Layout-B", "sequence_index": 1, "payload": {"z": 2, "a": 1}},
        ],
        "layers": [
            {"layer_ref": "Layer-A", "sequence_index": 0, "payload": {"y": [2, 1]}},
            {"layer_ref": "Layer-B", "sequence_index": 1, "payload": {"z": 4, "a": 3}},
        ],
        "blocks": [
            {
                "block_ref": "Block-A",
                "sequence_index": 0,
                "payload": {"nested": {"m": 2, "a": 1}, "name": "Door"},
            }
        ],
        "entities": [
            {
                "entity_id": "entity-a",
                "entity_type": "insert",
                "entity_schema_version": "1.0",
                "sequence_index": 0,
                "parent_entity_ref": None,
                "layout_ref": "Layout-A",
                "layer_ref": "Layer-A",
                "block_ref": "Block-A",
                "source_identity": "src-a",
                "source_hash": None,
                "confidence_score": 0.75,
                "confidence": {"status": "ok", "a": 1},
                "geometry": {"coords": [{"y": 2, "x": 1}]},
                "properties": {"text": "αβ", "name": "Door"},
                "provenance": {"adapter": {"version": 1, "key": "fake"}},
                "canonical_entity": None,
            },
            {
                "entity_id": "entity-b",
                "entity_type": "line",
                "entity_schema_version": "1.1",
                "sequence_index": 1,
                "parent_entity_ref": None,
                "layout_ref": "Layout-B",
                "layer_ref": "Layer-B",
                "block_ref": None,
                "source_identity": "src-b",
                "source_hash": "b" * 64,
                "confidence_score": 0.9,
                "confidence": {"z": 2, "a": 1},
                "geometry": {"z": [3, 2], "a": [1, 0]},
                "properties": {"z": 9, "a": 8},
                "provenance": {"origin": "adapter_normalized", "notes": ["β", "\u03b1"]},
                "canonical_entity": {
                    "entity_type": "line",
                    "geometry": {"z": [3, 2], "a": [1, 0]},
                },
            },
        ],
    }
    expected_bytes = _canonical_json_bytes(expected_payload)

    assert first == second
    assert first.checksum_sha256 == _EXPECTED_REVISION_JSON_CHECKSUM
    assert first == RevisionJsonExportResult(
        content_bytes=expected_bytes,
        checksum_sha256=_EXPECTED_REVISION_JSON_CHECKSUM,
        size_bytes=len(expected_bytes),
        media_type=REVISION_JSON_EXPORT_MEDIA_TYPE,
        generator_name=REVISION_JSON_EXPORT_GENERATOR_NAME,
        generator_version=REVISION_JSON_EXPORT_GENERATOR_VERSION,
        options={
            "zeta": [{"b": 2, "a": 1}],
            "alpha": "β",
            "exported_at": "2026-05-27T16:30:00Z",
        },
    )
    assert json.loads(first.content_bytes) == expected_payload
    assert b'"nested":{"a":1,"z":2}' in first.content_bytes
    assert b'"geometry":{"a":[1,0],"z":[3,2]}' in first.content_bytes
    assert b'"canonical_entity":null' in first.content_bytes


async def test_render_revision_json_export_orders_materialization_rows_by_sequence_index(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(
        db_session,
        layouts=(
            {"layout_ref": "Layout-3", "sequence_index": 2, "payload": {"n": 3}},
            {"layout_ref": "Layout-1", "sequence_index": 0, "payload": {"n": 1}},
            {"layout_ref": "Layout-2", "sequence_index": 1, "payload": {"n": 2}},
        ),
        layers=(
            {"layer_ref": "Layer-2", "sequence_index": 1, "payload": {"n": 2}},
            {"layer_ref": "Layer-1", "sequence_index": 0, "payload": {"n": 1}},
        ),
        blocks=(
            {"block_ref": "Block-2", "sequence_index": 1, "payload": {"n": 2}},
            {"block_ref": "Block-1", "sequence_index": 0, "payload": {"n": 1}},
        ),
        entities=(
            {
                "entity_id": "entity-2",
                "entity_type": "circle",
                "entity_schema_version": "1.0",
                "sequence_index": 1,
                "parent_entity_ref": None,
                "layout_ref": "Layout-2",
                "layer_ref": "Layer-2",
                "block_ref": "Block-2",
                "source_identity": "src-2",
                "source_hash": None,
                "confidence_score": 0.6,
                "confidence": {},
                "geometry": {"n": 2},
                "properties": {},
                "provenance": {},
                "canonical_entity": None,
            },
            {
                "entity_id": "entity-1",
                "entity_type": "line",
                "entity_schema_version": "1.0",
                "sequence_index": 0,
                "parent_entity_ref": None,
                "layout_ref": "Layout-1",
                "layer_ref": "Layer-1",
                "block_ref": "Block-1",
                "source_identity": "src-1",
                "source_hash": None,
                "confidence_score": 0.7,
                "confidence": {},
                "geometry": {"n": 1},
                "properties": {},
                "provenance": {},
                "canonical_entity": None,
            },
            {
                "entity_id": "entity-3",
                "entity_type": "arc",
                "entity_schema_version": "1.0",
                "sequence_index": 2,
                "parent_entity_ref": None,
                "layout_ref": "Layout-3",
                "layer_ref": "Layer-2",
                "block_ref": None,
                "source_identity": "src-3",
                "source_hash": None,
                "confidence_score": 0.8,
                "confidence": {},
                "geometry": {"n": 3},
                "properties": {},
                "provenance": {},
                "canonical_entity": None,
            },
        ),
    )

    result = await render_revision_json_export(db_session, seeded.revision.id)
    payload = json.loads(result.content_bytes)

    assert [item["layout_ref"] for item in payload["layouts"]] == [
        "Layout-1",
        "Layout-2",
        "Layout-3",
    ]
    assert [item["layer_ref"] for item in payload["layers"]] == ["Layer-1", "Layer-2"]
    assert [item["block_ref"] for item in payload["blocks"]] == ["Block-1", "Block-2"]
    assert [item["entity_id"] for item in payload["entities"]] == [
        "entity-1",
        "entity-2",
        "entity-3",
    ]


async def test_render_revision_json_export_raises_for_missing_revision(
    db_session: AsyncSession,
) -> None:
    with pytest.raises(RevisionJsonExportError, match="Drawing revision"):
        await render_revision_json_export(db_session, uuid.uuid4())


async def test_render_revision_json_export_raises_for_missing_manifest(
    db_session: AsyncSession,
) -> None:
    seeded = await _seed_revision_fixture(db_session, with_manifest=False)

    with pytest.raises(RevisionJsonExportError, match="Revision entity manifest"):
        await render_revision_json_export(db_session, seeded.revision.id)


async def _seed_revision_fixture(
    db_session: AsyncSession,
    *,
    with_manifest: bool = True,
    layouts: tuple[Mapping[str, Any], ...] = (),
    layers: tuple[Mapping[str, Any], ...] = (),
    blocks: tuple[Mapping[str, Any], ...] = (),
    entities: tuple[Mapping[str, Any], ...] = (),
) -> SeededRevisionFixture:
    project = Project(
        id=_fixture_uuid("project"),
        name="Revision Export Project",
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
        created_at=datetime(2026, 5, 27, 16, 0, tzinfo=UTC),
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
        created_at=datetime(2026, 5, 27, 16, 1, tzinfo=UTC),
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
        created_at=datetime(2026, 5, 27, 16, 2, tzinfo=UTC),
        finished_at=datetime(2026, 5, 27, 16, 8, tzinfo=UTC),
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
        canonical_json={"entities": [], "layers": [], "layouts": [], "blocks": []},
        provenance_json={"adapter": {"key": "tests.fake", "version": "1.0"}},
        confidence_json={"score": 0.875},
        confidence_score=0.875,
        warnings_json=[],
        diagnostics_json={"duration_ms": 3},
        result_checksum_sha256="b" * 64,
        created_at=datetime(2026, 5, 27, 16, 4, tzinfo=UTC),
    )
    revision = DrawingRevision(
        id=_fixture_uuid("revision"),
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
        created_at=datetime(2026, 5, 27, 16, 5, tzinfo=UTC),
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
    db_session.add(revision)
    await db_session.flush()

    manifest: RevisionEntityManifest | None = None
    if with_manifest:
        manifest = RevisionEntityManifest(
            id=_fixture_uuid("manifest"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1.0",
            counts_json={
                "layouts": len(layouts),
                "layers": len(layers),
                "blocks": len(blocks),
                "entities": len(entities),
            },
            created_at=datetime(2026, 5, 27, 16, 6, tzinfo=UTC),
        )
        db_session.add(manifest)
        await db_session.flush()

    created_layouts: list[RevisionLayout] = []
    for layout_index, spec in enumerate(layouts):
        created_layout = RevisionLayout(
            id=_fixture_uuid(f"layout:{layout_index}"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            payload_json=dict(spec["payload"]),
            layout_ref=str(spec["layout_ref"]),
            created_at=datetime(2026, 5, 27, 16, 6, tzinfo=UTC),
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
            extraction_profile_id=extraction_profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            payload_json=dict(spec["payload"]),
            layer_ref=str(spec["layer_ref"]),
            created_at=datetime(2026, 5, 27, 16, 6, tzinfo=UTC),
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
            extraction_profile_id=extraction_profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            payload_json=dict(spec["payload"]),
            block_ref=str(spec["block_ref"]),
            created_at=datetime(2026, 5, 27, 16, 6, tzinfo=UTC),
        )
        db_session.add(created_block)
        created_blocks.append(created_block)
    await db_session.flush()

    layout_by_ref = {layout.layout_ref: layout for layout in created_layouts}
    layer_by_ref = {layer.layer_ref: layer for layer in created_layers}
    block_by_ref = {block.block_ref: block for block in created_blocks}
    entity_by_ref: dict[str, RevisionEntity] = {}

    for entity_index, spec in enumerate(entities):
        parent_entity_ref_raw = spec.get("parent_entity_ref")
        parent_entity_ref = None if parent_entity_ref_raw is None else str(parent_entity_ref_raw)
        layout_ref_raw = spec.get("layout_ref")
        layout_ref = None if layout_ref_raw is None else str(layout_ref_raw)
        layer_ref_raw = spec.get("layer_ref")
        layer_ref = None if layer_ref_raw is None else str(layer_ref_raw)
        block_ref_raw = spec.get("block_ref")
        block_ref = None if block_ref_raw is None else str(block_ref_raw)
        entity = RevisionEntity(
            id=_fixture_uuid(f"entity:{entity_index}"),
            project_id=project.id,
            source_file_id=source_file.id,
            extraction_profile_id=extraction_profile.id,
            source_job_id=source_job.id,
            drawing_revision_id=revision.id,
            adapter_run_output_id=adapter_output.id,
            canonical_entity_schema_version="1.0",
            sequence_index=int(spec["sequence_index"]),
            entity_id=str(spec["entity_id"]),
            entity_type=str(spec["entity_type"]),
            entity_schema_version=str(spec["entity_schema_version"]),
            parent_entity_ref=parent_entity_ref,
            confidence_score=float(spec["confidence_score"]),
            confidence_json=dict(spec["confidence"]),
            geometry_json=dict(spec["geometry"]),
            properties_json=dict(spec["properties"]),
            provenance_json=dict(spec["provenance"]),
            canonical_entity_json=_optional_dict(spec.get("canonical_entity")),
            layout_ref=layout_ref,
            layer_ref=layer_ref,
            block_ref=block_ref,
            source_identity=_optional_str(spec.get("source_identity")),
            source_hash=_optional_str(spec.get("source_hash")),
            layout_id=None if layout_ref is None else layout_by_ref[layout_ref].id,
            layer_id=None if layer_ref is None else layer_by_ref[layer_ref].id,
            block_id=None if block_ref is None else block_by_ref[block_ref].id,
            parent_entity_row_id=(
                None if parent_entity_ref is None else entity_by_ref[parent_entity_ref].id
            ),
            created_at=datetime(2026, 5, 27, 16, 7, tzinfo=UTC),
        )
        db_session.add(entity)
        entity_by_ref[entity.entity_id] = entity
    await db_session.commit()

    return SeededRevisionFixture(
        project=project,
        source_file=source_file,
        extraction_profile=extraction_profile,
        source_job=source_job,
        adapter_output=adapter_output,
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


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return serialized.encode("utf-8")


def _fixture_uuid(name: str) -> uuid.UUID:
    return uuid.uuid5(_FIXTURE_UUID_NAMESPACE, f"revision_json_export:{name}")
