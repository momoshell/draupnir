"""Integration tests for revision-scoped materialization read APIs."""

import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from app.models.adapter_run_output import AdapterRunOutput
from app.models.drawing_revision import DrawingRevision
from tests.conftest import requires_database
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _load_project_materialization,
    _load_project_outputs,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import (
    _FAKE_RUNNER_ADAPTER_KEY,
    _FAKE_RUNNER_ADAPTER_VERSION,
    _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
    _FAKE_RUNNER_CONFIDENCE_SCORE,
    _FAKE_RUNNER_REVIEW_STATE,
    _build_fake_ingest_payload,
    _create_project,
    _get_job_for_file,
    _upload_file,
)


@pytest.fixture(autouse=True)
def fake_ingestion_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> list[IngestionRunRequest]:
    """Patch worker ingestion with a deterministic fake runner payload."""
    recorded_requests: list[IngestionRunRequest] = []

    async def _fake_run_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
        recorded_requests.append(request)
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)
    return recorded_requests


def _parse_timestamp(value: str) -> datetime:
    """Parse API timestamps while accepting UTC Z suffix serialization."""

    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _item_ids(items: list[dict[str, Any]]) -> list[str]:
    """Return identifiers from a JSON list response."""

    return [item["id"] for item in items]


@requires_database
class TestRevisionMaterializationApi:
    """Tests for revision-scoped materialization listing APIs."""

    async def test_materialization_endpoints_return_manifest_metadata_and_curated_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Layout/layer/block/entity routes should expose revision-scoped materialized rows."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_materialized_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layouts=[
                    {"layout_ref": "Model", "name": "Model"},
                    {"layout_ref": "Paper", "name": "Paper"},
                ],
                layers=[
                    {"layer_ref": "A-WALL", "name": "A-WALL"},
                    {"layer_ref": "A-DOOR", "name": "A-DOOR"},
                ],
                blocks=[{"block_ref": "DOOR-1", "name": "DOOR-1"}],
                entities=[
                    _build_contract_entity(
                        entity_id="entity-parent",
                        entity_type="insert",
                        layout_ref="Model",
                        layer_ref="A-DOOR",
                        block_ref="DOOR-1",
                        source_id="entity-source-parent",
                    ),
                    _build_contract_entity(
                        entity_id="entity-child",
                        entity_type="line",
                        layout_ref="Model",
                        layer_ref="A-WALL",
                        parent_entity_ref="entity-parent",
                        source_id="entity-source-child",
                        source_hash="a" * 64,
                    ),
                    _build_contract_entity(
                        entity_id="entity-paper",
                        entity_type="circle",
                        layout_ref="Paper",
                        layer_ref="A-WALL",
                        source_id="entity-source-paper",
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        manifests, layouts, layers, blocks, entities = await _load_project_materialization(
            project["id"]
        )
        drawing_revision = drawing_revisions[0]
        manifest = manifests[0]
        layout = layouts[0]
        layer = layers[0]
        block = blocks[0]
        parent_entity, child_entity, paper_entity = entities

        layouts_response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/layouts")
        layers_response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/layers")
        blocks_response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/blocks")
        entities_response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/entities")

        assert layouts_response.status_code == 200
        assert layers_response.status_code == 200
        assert blocks_response.status_code == 200
        assert entities_response.status_code == 200

        expected_counts = {"layouts": 2, "layers": 2, "blocks": 1, "entities": 3}

        for body in (
            layouts_response.json(),
            layers_response.json(),
            blocks_response.json(),
            entities_response.json(),
        ):
            assert body["manifest"]["id"] == str(manifest.id)
            assert body["manifest"]["project_id"] == str(manifest.project_id)
            assert body["manifest"]["source_file_id"] == str(manifest.source_file_id)
            assert body["manifest"]["extraction_profile_id"] == str(manifest.extraction_profile_id)
            assert body["manifest"]["source_job_id"] == str(manifest.source_job_id)
            assert body["manifest"]["drawing_revision_id"] == str(manifest.drawing_revision_id)
            assert body["manifest"]["adapter_run_output_id"] == str(manifest.adapter_run_output_id)
            assert (
                body["manifest"]["canonical_entity_schema_version"]
                == manifest.canonical_entity_schema_version
            )
            assert body["manifest"]["counts"] == expected_counts
            assert _parse_timestamp(body["manifest"]["created_at"]) == (
                manifest.created_at.astimezone(UTC)
            )
            assert body["counts"] == expected_counts

        layouts_body = layouts_response.json()
        assert _item_ids(layouts_body["items"]) == [str(item.id) for item in layouts]
        assert layouts_body["next_cursor"] is None
        assert layouts_body["items"][0] == {
            "id": str(layout.id),
            "project_id": str(layout.project_id),
            "source_file_id": str(layout.source_file_id),
            "extraction_profile_id": str(layout.extraction_profile_id),
            "source_job_id": str(layout.source_job_id),
            "drawing_revision_id": str(layout.drawing_revision_id),
            "adapter_run_output_id": str(layout.adapter_run_output_id),
            "canonical_entity_schema_version": layout.canonical_entity_schema_version,
            "sequence_index": layout.sequence_index,
            "layout_ref": layout.layout_ref,
            "payload": layout.payload_json,
            "created_at": layout.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
        assert "payload_json" not in layouts_body["items"][0]

        layers_body = layers_response.json()
        assert _item_ids(layers_body["items"]) == [str(item.id) for item in layers]
        assert layers_body["next_cursor"] is None
        assert layers_body["items"][0]["layer_ref"] == layer.layer_ref
        assert layers_body["items"][0]["payload"] == layer.payload_json
        assert "payload_json" not in layers_body["items"][0]

        blocks_body = blocks_response.json()
        assert _item_ids(blocks_body["items"]) == [str(item.id) for item in blocks]
        assert blocks_body["next_cursor"] is None
        assert blocks_body["items"][0]["block_ref"] == block.block_ref
        assert blocks_body["items"][0]["payload"] == block.payload_json
        assert "payload_json" not in blocks_body["items"][0]

        entities_body = entities_response.json()
        assert _item_ids(entities_body["items"]) == [str(item.id) for item in entities]
        assert entities_body["next_cursor"] is None

        parent_item, child_item, paper_item = entities_body["items"]
        assert parent_item["entity_id"] == parent_entity.entity_id
        assert parent_item["entity_type"] == parent_entity.entity_type
        assert parent_item["confidence_score"] == _FAKE_RUNNER_CONFIDENCE_SCORE
        assert parent_item["confidence"] == parent_entity.confidence_json
        assert parent_item["geometry"] == parent_entity.geometry_json
        assert parent_item["properties"] == parent_entity.properties_json
        assert parent_item["provenance"] == parent_entity.provenance_json
        assert parent_item["layout_ref"] == parent_entity.layout_ref
        assert parent_item["layer_ref"] == parent_entity.layer_ref
        assert parent_item["block_ref"] == parent_entity.block_ref
        assert parent_item["parent_entity_ref"] is None
        assert parent_item["source_identity"] == parent_entity.source_identity
        assert parent_item["source_hash"] is None
        assert parent_item["layout_id"] == str(parent_entity.layout_id)
        assert parent_item["layer_id"] == str(parent_entity.layer_id)
        assert parent_item["block_id"] == str(parent_entity.block_id)
        assert parent_item["parent_entity_row_id"] is None
        assert "canonical_entity_json" not in parent_item
        assert "geometry_json" not in parent_item
        assert "properties_json" not in parent_item
        assert "provenance_json" not in parent_item
        assert "confidence_json" not in parent_item

        assert child_item["entity_id"] == child_entity.entity_id
        assert child_item["parent_entity_ref"] == child_entity.parent_entity_ref
        assert child_item["source_hash"] == child_entity.source_hash
        assert child_item["layout_id"] == str(child_entity.layout_id)
        assert child_item["layer_id"] == str(child_entity.layer_id)
        assert child_item["block_id"] is None
        assert child_item["parent_entity_row_id"] == str(child_entity.parent_entity_row_id)

        assert paper_item["entity_id"] == paper_entity.entity_id
        assert paper_item["layout_ref"] == "Paper"
        assert paper_item["source_identity"] == "entity-source-paper"

    async def test_revision_entities_support_filters_and_shared_cursor_validation(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Entity listing should support filters and shared cursor pagination semantics."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_materialized_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                blocks=[{"block_ref": "DOOR-1", "name": "DOOR-1"}],
                entities=[
                    _build_contract_entity(
                        entity_id="entity-parent",
                        entity_type="insert",
                        layer_ref="A-DOOR",
                        block_ref="DOOR-1",
                        source_id="entity-source-parent",
                    ),
                    _build_contract_entity(
                        entity_id="entity-child",
                        entity_type="line",
                        layer_ref="A-WALL",
                        parent_entity_ref="entity-parent",
                        source_id="entity-source-child",
                        source_hash="a" * 64,
                    ),
                    _build_contract_entity(
                        entity_id="entity-paper",
                        entity_type="circle",
                        layout_ref="Paper",
                        layer_ref="A-WALL",
                        source_id="entity-source-paper",
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

        filter_cases = [
            ({"entity_type": "insert"}, ["entity-parent"]),
            ({"layout_ref": "Paper"}, ["entity-paper"]),
            ({"layer_ref": "A-DOOR"}, ["entity-parent"]),
            ({"block_ref": "DOOR-1"}, ["entity-parent"]),
            ({"parent_entity_ref": "entity-parent"}, ["entity-child"]),
            ({"source_identity": "entity-source-paper"}, ["entity-paper"]),
            ({"source_hash": "a" * 64}, ["entity-child"]),
        ]

        for params, expected_entity_ids in filter_cases:
            response = await async_client.get(
                f"/v1/revisions/{drawing_revision.id}/entities",
                params=params,
            )
            assert response.status_code == 200
            assert [item["entity_id"] for item in response.json()["items"]] == expected_entity_ids

        first_page_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={"limit": 2},
        )
        assert first_page_response.status_code == 200
        first_page = first_page_response.json()
        assert [item["entity_id"] for item in first_page["items"]] == [
            "entity-parent",
            "entity-child",
        ]
        assert first_page["next_cursor"] is not None

        second_page_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={"limit": 2, "cursor": first_page["next_cursor"]},
        )
        assert second_page_response.status_code == 200
        second_page = second_page_response.json()
        assert [item["entity_id"] for item in second_page["items"]] == ["entity-paper"]
        assert second_page["next_cursor"] is None

        invalid_cursor_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={"cursor": "not-a-valid-cursor"},
        )
        assert invalid_cursor_response.status_code == 400
        assert invalid_cursor_response.json() == {
            "error": {
                "code": ErrorCode.INVALID_CURSOR.value,
                "message": "Invalid cursor format",
                "details": None,
            }
        }

    async def test_materialization_endpoints_return_404_for_unknown_or_inactive_revision(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Materialization reads should hide missing or soft-deleted revision data."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

        missing_revision_id = uuid.uuid4()
        missing_response = await async_client.get(f"/v1/revisions/{missing_revision_id}/layouts")

        assert missing_response.status_code == 404
        assert missing_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{missing_revision_id}' not found",
                "details": None,
            }
        }

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")
        assert delete_response.status_code == 204

        inactive_response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/layouts")

        assert inactive_response.status_code == 404
        assert inactive_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{drawing_revision.id}' not found",
                "details": None,
            }
        }

    async def test_materialization_endpoints_return_409_when_manifest_missing(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Legacy revisions without materialization manifests should return 409."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        adapter_output_id = uuid.uuid4()
        revision_id = uuid.uuid4()
        assert job.extraction_profile_id is not None

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        async with session_maker() as session:
            session.add(
                AdapterRunOutput(
                    id=adapter_output_id,
                    project_id=job.project_id,
                    source_file_id=job.file_id,
                    extraction_profile_id=job.extraction_profile_id,
                    source_job_id=job.id,
                    adapter_key=_FAKE_RUNNER_ADAPTER_KEY,
                    adapter_version=_FAKE_RUNNER_ADAPTER_VERSION,
                    input_family="pdf_vector",
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    canonical_json={
                        "canonical_entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layouts": [],
                        "layers": [],
                        "blocks": [],
                        "entities": [],
                        "entity_counts": {
                            "layouts": 0,
                            "layers": 0,
                            "blocks": 0,
                            "entities": 0,
                        },
                    },
                    provenance_json={
                        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "adapter": {
                            "key": _FAKE_RUNNER_ADAPTER_KEY,
                            "version": _FAKE_RUNNER_ADAPTER_VERSION,
                        },
                        "source": {
                            "file_id": str(job.file_id),
                            "job_id": str(job.id),
                            "extraction_profile_id": str(job.extraction_profile_id),
                            "input_family": "pdf_vector",
                            "revision_kind": "ingest",
                        },
                        "records": [],
                        "generated_at": "2026-01-02T03:04:05+00:00",
                    },
                    confidence_json={"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
                    confidence_score=_FAKE_RUNNER_CONFIDENCE_SCORE,
                    warnings_json=[],
                    diagnostics_json={"adapter": _FAKE_RUNNER_ADAPTER_KEY, "diagnostics": []},
                    result_checksum_sha256="0" * 64,
                )
            )
            session.add(
                DrawingRevision(
                    id=revision_id,
                    project_id=job.project_id,
                    source_file_id=job.file_id,
                    extraction_profile_id=job.extraction_profile_id,
                    source_job_id=job.id,
                    adapter_run_output_id=adapter_output_id,
                    predecessor_revision_id=None,
                    revision_sequence=1,
                    revision_kind="ingest",
                    review_state=_FAKE_RUNNER_REVIEW_STATE,
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                    confidence_score=_FAKE_RUNNER_CONFIDENCE_SCORE,
                )
            )
            await session.commit()

        response = await async_client.get(f"/v1/revisions/{revision_id}/entities")

        assert response.status_code == 409
        assert response.json() == {
            "error": {
                "code": ErrorCode.NORMALIZED_ENTITIES_NOT_MATERIALIZED.value,
                "message": (
                    f"Normalized entities for drawing revision '{revision_id}' "
                    "have not been materialized"
                ),
                "details": None,
            }
        }

    async def test_materialization_endpoints_return_empty_lists_for_empty_manifest(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Revisions with zero-count manifests should return 200 with empty items."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_empty_ingestion(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layouts=[],
                layers=[],
                blocks=[],
                entities=[],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_empty_ingestion)

        await process_ingest_job(job.id)

        _adapter_outputs, drawing_revisions, _validation_reports, _generated_artifacts = (
            await _load_project_outputs(project["id"])
        )
        drawing_revision = drawing_revisions[0]

        expected_counts = {"layouts": 0, "layers": 0, "blocks": 0, "entities": 0}

        for suffix in ("layouts", "layers", "blocks", "entities"):
            response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/{suffix}")
            assert response.status_code == 200
            assert response.json()["items"] == []
            assert response.json()["next_cursor"] is None
            assert response.json()["counts"] == expected_counts
            assert response.json()["manifest"]["counts"] == expected_counts
