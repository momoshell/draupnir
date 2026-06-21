"""Integration tests for revision-scoped materialization read APIs."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx
import pytest

import app.db.session as session_module
import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.adapters import libredwg as libredwg_adapter
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.interpretation.loaders import load_revision_entities_by_type
from app.jobs.worker import process_ingest_job
from app.models.adapter_run_output import AdapterRunOutput
from app.models.cad_changeset import CadChangeSet
from app.models.drawing_revision import DrawingRevision
from app.models.revision_materialization import RevisionLayer
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
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
    _build_fake_ingest_payload,
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


def _clone_model(instance: Any, **overrides: Any) -> Any:
    """Clone a persisted SQLAlchemy model instance with selected overrides."""

    data = {column.name: getattr(instance, column.name) for column in instance.__table__.columns}
    data.update(overrides)
    return instance.__class__(**data)


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
                        entity_id="entity/paper",
                        entity_type="circle",
                        layout_ref="Paper",
                        layer_ref="A-WALL",
                        source_id="entity-source-paper",
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
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
        # Compact default: cheap spine present; heavy blocks null until requested via fields=.
        assert parent_item["entity_id"] == parent_entity.entity_id
        assert parent_item["entity_type"] == parent_entity.entity_type
        assert parent_item["layout_ref"] == parent_entity.layout_ref
        assert parent_item["layer_ref"] == parent_entity.layer_ref
        assert parent_item["block_ref"] == parent_entity.block_ref
        assert parent_item["parent_entity_ref"] is None
        assert parent_item["source_identity"] == parent_entity.source_identity
        assert parent_item["source_hash"] is None
        assert parent_item["geometry"] is None
        assert parent_item["properties"] is None
        assert parent_item["provenance"] is None
        assert parent_item["confidence"] is None
        assert parent_item["canonical"] is None
        # Resolved row ids / canonical-keyed fields are not part of the compact list.
        assert "layout_id" not in parent_item
        assert "parent_entity_row_id" not in parent_item
        assert "confidence_score" not in parent_item
        assert "geometry_json" not in parent_item

        assert child_item["entity_id"] == child_entity.entity_id
        assert child_item["parent_entity_ref"] == child_entity.parent_entity_ref
        assert child_item["source_hash"] == child_entity.source_hash

        assert paper_item["entity_id"] == paper_entity.entity_id
        assert paper_item["layout_ref"] == "Paper"
        assert paper_item["source_identity"] == "entity-source-paper"

        # fields= populates the requested heavy blocks (and only those).
        projected = (
            await async_client.get(
                f"/v1/revisions/{drawing_revision.id}/entities",
                params={"fields": "geometry,confidence"},
            )
        ).json()["items"][0]
        assert projected["geometry"] == parent_entity.geometry_json
        assert projected["confidence"] == parent_entity.confidence_json
        assert projected["properties"] is None
        assert projected["provenance"] is None
        assert projected["canonical"] is None

        # Unknown projection field is rejected.
        bad = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={"fields": "geometry,bogus"},
        )
        assert bad.status_code == 400
        assert bad.json()["error"]["code"] == "INPUT_INVALID"
        assert "bogus" in bad.json()["error"]["details"]["unknown_fields"]

    async def test_entities_spatial_filter_by_bbox_and_near_point(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spatial filters select entities by persisted bbox intersection / point proximity."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        async def _run_spatial_ingestion(
            request: IngestionRunRequest,
        ) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            left = _build_contract_entity(
                entity_id="left", entity_type="line", layer_ref="A-WALL", source_id="s-left"
            )
            left["geometry_json"] = {"start": {"x": 0.0, "y": 0.0}, "end": {"x": 10.0, "y": 10.0}}
            right = _build_contract_entity(
                entity_id="right", entity_type="point", layer_ref="A-WALL", source_id="s-right"
            )
            right["geometry_json"] = {"position": {"x": 100.0, "y": 100.0}}
            return _replace_fake_canonical_payload(
                payload,
                layouts=[{"layout_ref": "Model", "name": "Model"}],
                layers=[{"layer_ref": "A-WALL", "name": "A-WALL"}],
                blocks=[],
                entities=[left, right],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_spatial_ingestion)
        await process_ingest_job(job.id)
        (_outputs, drawing_revisions, _reports, _artifacts) = await _load_project_outputs(
            project["id"]
        )
        base = f"/v1/revisions/{drawing_revisions[0].id}/entities"

        def _ids(response: httpx.Response) -> list[str]:
            return [item["entity_id"] for item in response.json()["items"]]

        # bbox intersection selects the left line only; its persisted bbox is surfaced.
        in_box = await async_client.get(
            base, params={"min_x": -1, "min_y": -1, "max_x": 20, "max_y": 20}
        )
        assert in_box.status_code == 200
        assert _ids(in_box) == ["left"]
        assert in_box.json()["items"][0]["bbox"] == [0.0, 0.0, 10.0, 10.0]

        # near-point: within 5 of the right point.
        assert _ids(
            await async_client.get(base, params={"near_x": 100, "near_y": 100, "radius": 5})
        ) == ["right"]
        # a point inside the left bbox (distance 0) matches it.
        assert _ids(
            await async_client.get(base, params={"near_x": 5, "near_y": 5, "radius": 1})
        ) == ["left"]
        # far from everything.
        assert (
            _ids(await async_client.get(base, params={"near_x": 1000, "near_y": 1000, "radius": 1}))
            == []
        )

        # validation: partial bbox + non-positive radius.
        assert (await async_client.get(base, params={"min_x": 0})).status_code == 400
        assert (
            await async_client.get(base, params={"near_x": 0, "near_y": 0, "radius": 0})
        ).status_code == 400

    async def test_entities_printed_sheet_filter_by_on_sheet(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`on_sheet` projects the membership tag to a column + filters the printed sheet (#569)."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        def _with_membership(entity: dict[str, Any], on_sheet: bool | None) -> dict[str, Any]:
            membership: dict[str, Any] = {"schema_version": "0.1", "on_sheet": on_sheet}
            props = {**entity["properties_json"], "sheet_membership": membership}
            entity["properties_json"] = props
            return entity

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            on = _with_membership(
                _build_contract_entity(
                    entity_id="on", entity_type="line", layer_ref="A-WALL", source_id="s-on"
                ),
                True,
            )
            off = _with_membership(
                _build_contract_entity(
                    entity_id="off", entity_type="line", layer_ref="Z2020T", source_id="s-off"
                ),
                False,
            )
            undet = _with_membership(
                _build_contract_entity(
                    entity_id="undet", entity_type="line", layer_ref="A-WALL", source_id="s-und"
                ),
                None,
            )
            return _replace_fake_canonical_payload(
                payload,
                layouts=[{"layout_ref": "Model", "name": "Model"}],
                layers=[
                    {"layer_ref": "A-WALL", "name": "A-WALL"},
                    {"layer_ref": "Z2020T", "name": "Z2020T"},
                ],
                blocks=[],
                entities=[on, off, undet],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run)
        await process_ingest_job(job.id)
        (_outputs, drawing_revisions, _reports, _artifacts) = await _load_project_outputs(
            project["id"]
        )
        base = f"/v1/revisions/{drawing_revisions[0].id}/entities"

        def _ids(response: httpx.Response) -> list[str]:
            return sorted(item["entity_id"] for item in response.json()["items"])

        # column projected onto the compact spine (no fields= needed)
        all_rows = await async_client.get(base)
        by_id = {i["entity_id"]: i["on_sheet"] for i in all_rows.json()["items"]}
        assert by_id == {"on": True, "off": False, "undet": None}

        # printed-sheet filter: on / off / (NULL excluded from both)
        assert _ids(await async_client.get(base, params={"on_sheet": "true"})) == ["on"]
        assert _ids(await async_client.get(base, params={"on_sheet": "false"})) == ["off"]
        # omitted → all three (full modelspace)
        assert _ids(all_rows) == ["off", "on", "undet"]

    async def test_room_loader_exclude_off_sheet_keeps_on_and_undetermined(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`load_revision_entities_by_type(exclude_off_sheet=True)` drops only KNOWN off-sheet
        entities (on_sheet IS FALSE) and keeps on-sheet (True) AND undetermined (NULL) — so a
        drawing without viewports degrades gracefully instead of emptying (#583)."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        def _with_membership(entity: dict[str, Any], on_sheet: bool | None) -> dict[str, Any]:
            membership: dict[str, Any] = {"schema_version": "0.1", "on_sheet": on_sheet}
            entity["properties_json"] = {
                **entity["properties_json"],
                "sheet_membership": membership,
            }
            return entity

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            ents = [
                _with_membership(
                    _build_contract_entity(
                        entity_id=eid, entity_type="line", layer_ref="A-WALL", source_id=f"s-{eid}"
                    ),
                    flag,
                )
                for eid, flag in (("on", True), ("off", False), ("undet", None))
            ]
            return _replace_fake_canonical_payload(
                payload,
                layouts=[{"layout_ref": "Model", "name": "Model"}],
                layers=[{"layer_ref": "A-WALL", "name": "A-WALL"}],
                blocks=[],
                entities=ents,
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run)
        await process_ingest_job(job.id)
        (_outputs, drawing_revisions, _reports, _artifacts) = await _load_project_outputs(
            project["id"]
        )
        revision_id = drawing_revisions[0].id

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            scoped = await load_revision_entities_by_type(
                session, revision_id, ("line",), exclude_off_sheet=True
            )
            full = await load_revision_entities_by_type(session, revision_id, ("line",))

        assert sorted(e.entity_id for e in scoped) == [
            "on",
            "undet",
        ]  # off-sheet dropped, NULL kept
        assert sorted(e.entity_id for e in full) == ["off", "on", "undet"]  # full modelspace

    async def test_devices_endpoint_returns_schedule_and_tag_association(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """/devices returns the fixture schedule plus nearest-tag association for INSERTs."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        def _device(entity_id: str, source: str, x: float, y: float) -> dict[str, Any]:
            entity = _build_contract_entity(
                entity_id=entity_id,
                entity_type="insert",
                layout_ref="Model",
                layer_ref="F810A",
                block_ref="Smoke-Detector",
                source_id=source,
            )
            entity["geometry_json"] = {"transform": {"insertion_point": {"x": x, "y": y, "z": 0.0}}}
            return entity

        def _tag(entity_id: str, source: str, x: float, y: float, text: str) -> dict[str, Any]:
            entity = _build_contract_entity(
                entity_id=entity_id,
                entity_type="text",
                layout_ref="Model",
                layer_ref="Fire Alarm Device Tags",
                source_id=source,
            )
            entity["geometry_json"] = {"insertion": {"x": x, "y": y, "z": 0.0}, "text": text}
            return entity

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layers=[
                    {"layer_ref": "F810A", "name": "F810A"},
                    {"layer_ref": "Fire Alarm Device Tags", "name": "Fire Alarm Device Tags"},
                ],
                blocks=[{"block_ref": "Smoke-Detector", "name": "Smoke-Detector"}],
                entities=[
                    _device("dev-1", "src-dev-1", 0.0, 0.0),
                    _device("dev-2", "src-dev-2", 10.0, 0.0),
                    _tag("tag-1", "src-tag-1", 0.5, 0.0, "SD-01"),
                    _tag("tag-2", "src-tag-2", 9.5, 0.0, "SD-02"),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run)
        await process_ingest_job(job.id)

        _outputs, drawing_revisions, _reports, _artifacts = await _load_project_outputs(
            project["id"]
        )
        revision_id = drawing_revisions[0].id

        response = await async_client.get(
            f"/v1/revisions/{revision_id}/devices",
            params={"tag_layer": "Fire Alarm Device Tags", "max_tag_distance": 2.0},
        )
        assert response.status_code == 200
        body = response.json()

        assert body["schedule"] == [{"block_ref": "Smoke-Detector", "count": 2}]
        devices = {item["entity_id"]: item for item in body["items"]}
        assert devices["dev-1"]["depth"] == 0
        assert devices["dev-1"]["tag"]["text"] == "SD-01"
        assert devices["dev-1"]["tag"]["distance"] == pytest.approx(0.5)
        assert devices["dev-2"]["tag"]["text"] == "SD-02"
        assert body["association"]["max_tag_distance"] == 2.0

    async def test_devices_endpoint_enumerates_nested_block_instances(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A device nested inside a placed block is enumerated with a composed world position."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))

        nested_device = {
            "entity_id": "nested-smoke",
            "entity_type": "insert",
            "entity_schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
            "layer_ref": "F810A",
            "block_ref": "Smoke-Detector",
            "geometry": {
                "transform": {
                    "insertion_point": {"x": 10.0, "y": 0.0, "z": 0.0},
                    "scale": {"x": 1.0, "y": 1.0, "z": 1.0},
                    "rotation_radians": 0.0,
                }
            },
        }

        root = _build_contract_entity(
            entity_id="container-root",
            entity_type="insert",
            layout_ref="Model",
            layer_ref="A-CONTAINER",
            block_ref="Container",
            source_id="src-root",
        )
        root["geometry_json"] = {
            "transform": {
                "insertion_point": {"x": 100.0, "y": 0.0, "z": 0.0},
                "scale": {"x": 1.0, "y": 1.0, "z": 1.0},
                "rotation_radians": 0.0,
            }
        }
        tag = _build_contract_entity(
            entity_id="tag-nested",
            entity_type="text",
            layout_ref="Model",
            layer_ref="Fire Alarm Device Tags",
            source_id="src-tag-nested",
        )
        tag["geometry_json"] = {"insertion": {"x": 110.0, "y": 0.0, "z": 0.0}, "text": "SD-NEST"}

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layers=[
                    {"layer_ref": "A-CONTAINER", "name": "A-CONTAINER"},
                    {"layer_ref": "F810A", "name": "F810A"},
                    {"layer_ref": "Fire Alarm Device Tags", "name": "Fire Alarm Device Tags"},
                ],
                blocks=[
                    {
                        "block_ref": "Container",
                        "name": "Container",
                        "base_point": {"x": 0.0, "y": 0.0, "z": 0.0},
                        "entities": [nested_device],
                    },
                    {"block_ref": "Smoke-Detector", "name": "Smoke-Detector"},
                ],
                entities=[root, tag],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run)
        await process_ingest_job(job.id)

        _outputs, drawing_revisions, _reports, _artifacts = await _load_project_outputs(
            project["id"]
        )
        revision_id = drawing_revisions[0].id

        response = await async_client.get(
            f"/v1/revisions/{revision_id}/devices",
            params={"tag_layer": "Fire Alarm Device Tags", "max_tag_distance": 1.0},
        )
        assert response.status_code == 200
        body = response.json()

        schedule = {entry["block_ref"]: entry["count"] for entry in body["schedule"]}
        assert schedule == {"Container": 1, "Smoke-Detector": 1}

        nested = next(item for item in body["items"] if item["block_ref"] == "Smoke-Detector")
        assert nested["depth"] == 1
        assert nested["position"]["x"] == pytest.approx(110.0)
        assert nested["position"]["y"] == pytest.approx(0.0)
        assert nested["tag"]["text"] == "SD-NEST"

    async def test_materialization_manifest_serializes_null_origin_ids(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Materialization reads should serialize nullable manifest and row origin identifiers."""
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
                layers=[],
                blocks=[],
                entities=[],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        manifests, layouts, _layers, _blocks, _entities = await _load_project_materialization(
            project["id"]
        )
        drawing_revision = drawing_revisions[0]
        manifest = manifests[0]

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None

        changeset_id = uuid.uuid4()
        null_origin_changeset = CadChangeSet(
            id=changeset_id,
            project_id=drawing_revision.project_id,
            base_revision_id=drawing_revision.id,
            status="applied",
            created_by="test",
        )
        null_origin_job = _clone_model(
            job,
            id=uuid.uuid4(),
            base_revision_id=drawing_revision.id,
            job_type="reprocess",
            status="succeeded",
            started_at=drawing_revision.created_at + timedelta(seconds=1),
            finished_at=drawing_revision.created_at + timedelta(seconds=1),
            created_at=drawing_revision.created_at + timedelta(seconds=1),
        )
        null_origin_revision = _clone_model(
            drawing_revision,
            id=uuid.uuid4(),
            extraction_profile_id=None,
            source_job_id=null_origin_job.id,
            adapter_run_output_id=None,
            changeset_id=changeset_id,
            predecessor_revision_id=drawing_revision.id,
            revision_sequence=drawing_revision.revision_sequence + 1,
            revision_kind="changeset",
            created_at=drawing_revision.created_at + timedelta(seconds=1),
        )
        null_origin_manifest = _clone_model(
            manifest,
            id=uuid.uuid4(),
            drawing_revision_id=null_origin_revision.id,
            extraction_profile_id=None,
            source_job_id=null_origin_job.id,
            adapter_run_output_id=None,
            counts_json={
                "layouts": len(layouts),
                "layers": 0,
                "blocks": 0,
                "entities": 0,
            },
        )
        null_origin_layouts = [
            _clone_model(
                layout,
                id=uuid.uuid4(),
                drawing_revision_id=null_origin_revision.id,
                extraction_profile_id=None,
                source_job_id=null_origin_job.id,
                adapter_run_output_id=None,
            )
            for layout in layouts
        ]

        async with session_maker() as session:
            session.add(null_origin_changeset)
            await session.commit()

        async with session_maker() as session:
            session.add(null_origin_job)
            await session.commit()

        async with session_maker() as session:
            session.add(null_origin_revision)
            session.add(null_origin_manifest)
            session.add_all(null_origin_layouts)
            await session.commit()

        response = await async_client.get(f"/v1/revisions/{null_origin_revision.id}/layouts")

        assert response.status_code == 200
        body = response.json()
        assert body["manifest"]["extraction_profile_id"] is None
        assert body["manifest"]["adapter_run_output_id"] is None
        assert body["counts"] == {
            "layouts": len(layouts),
            "layers": 0,
            "blocks": 0,
            "entities": 0,
        }
        assert _item_ids(body["items"]) == [str(layout.id) for layout in null_origin_layouts]
        assert all(item["extraction_profile_id"] is None for item in body["items"])
        assert all(item["adapter_run_output_id"] is None for item in body["items"])

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
                        entity_id="entity/paper",
                        entity_type="circle",
                        layout_ref="Paper",
                        layer_ref="A-WALL",
                        source_id="entity-source-paper",
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        drawing_revision = drawing_revisions[0]

        filter_cases = [
            ({"entity_id": "entity-parent"}, ["entity-parent"]),
            ({"entity_type": "insert"}, ["entity-parent"]),
            ({"layout_ref": "Paper"}, ["entity/paper"]),
            ({"layer_ref": "A-DOOR"}, ["entity-parent"]),
            ({"block_ref": "DOOR-1"}, ["entity-parent"]),
            ({"parent_entity_ref": "entity-parent"}, ["entity-child"]),
            ({"source_identity": "entity-source-paper"}, ["entity/paper"]),
            ({"source_hash": "a" * 64}, ["entity-child"]),
        ]

        for params, expected_entity_ids in filter_cases:
            response = await async_client.get(
                f"/v1/revisions/{drawing_revision.id}/entities",
                params=params,
            )
            assert response.status_code == 200
            assert [item["entity_id"] for item in response.json()["items"]] == expected_entity_ids

        combined_filter_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={
                "entity_id": "entity/paper",
                "layout_ref": "Paper",
                "source_identity": "entity-source-paper",
            },
        )
        assert combined_filter_response.status_code == 200
        assert [item["entity_id"] for item in combined_filter_response.json()["items"]] == [
            "entity/paper"
        ]

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
        assert [item["entity_id"] for item in second_page["items"]] == ["entity/paper"]
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

    async def test_revision_materialization_entity_filter_supports_text_entities(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
                entities=[
                    _build_contract_entity(
                        entity_id="text-entity",
                        entity_type="text",
                        layout_ref="Model",
                        layer_ref="A-WALL",
                        source_id="text-source",
                    ),
                    _build_contract_entity(
                        entity_id="line-entity",
                        entity_type="line",
                        layout_ref="Model",
                        layer_ref="A-WALL",
                        source_id="line-source",
                        source_hash="b" * 64,
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        drawing_revision = drawing_revisions[0]

        response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={"entity_type": "text"},
        )
        assert response.status_code == 200
        assert [item["entity_id"] for item in response.json()["items"]] == ["text-entity"]
        assert response.json()["items"][0]["entity_type"] == "text"

        singular_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities/{quote('text-entity', safe='')}"
        )
        assert singular_response.status_code == 200
        assert singular_response.json()["entity_id"] == "text-entity"
        assert singular_response.json()["entity_type"] == "text"

    async def test_revision_materialization_entity_filter_supports_libredwg_text_payload(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
            text_entity = libredwg_adapter._build_text_entity(
                {
                    "type": "MTEXT",
                    "handle": "7Z",
                    "layout": "Model",
                    "layer": "A-WALL",
                    "insertion": {"x": 10, "y": 20, "z": 0},
                    "text": "Layout test text",
                },
                units=libredwg_adapter._resolve_units({}),
            )
            return _replace_fake_canonical_payload(payload, entities=[text_entity])

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        drawing_revision = drawing_revisions[0]

        response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities",
            params={
                "layout_ref": "Model",
                "layer_ref": "A-WALL",
                "entity_type": "text",
                "fields": "geometry,properties",
            },
        )
        assert response.status_code == 200
        items = response.json()["items"]
        assert [item["entity_id"] for item in items] == ["libredwg-text-7z"]
        assert items[0]["geometry"]["text"] == "Layout test text"
        assert items[0]["properties"]["text"] == "Layout test text"

        single = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities/{quote('libredwg-text-7z', safe='')}"
        )
        assert single.status_code == 200
        assert single.json()["entity_id"] == "libredwg-text-7z"
        assert single.json()["layout_ref"] == "Model"
        assert single.json()["layer_ref"] == "A-WALL"
        assert single.json()["geometry"]["text"] == "Layout test text"
        assert single.json()["properties"]["text"] == "Layout test text"

    async def test_revision_entity_lookup_supports_singular_reads_and_encoded_paths(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Singular entity reads should resolve exact entity identifiers.

        Encoded slashes must round-trip through the path converter.
        """
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
                entities=[
                    _build_contract_entity(
                        entity_id="entity-parent",
                        entity_type="insert",
                        layer_ref="A-DOOR",
                        source_id="entity-source-parent",
                    ),
                    _build_contract_entity(
                        entity_id="entity/paper",
                        entity_type="circle",
                        layout_ref="Paper",
                        layer_ref="A-WALL",
                        source_id="entity-source-paper",
                    ),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run_materialized_ingestion)

        await process_ingest_job(job.id)

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        drawing_revision = drawing_revisions[0]

        entity_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities/entity-parent"
        )
        assert entity_response.status_code == 200
        assert entity_response.json()["entity_id"] == "entity-parent"
        assert entity_response.json()["entity_type"] == "insert"

        encoded_entity_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities/{quote('entity/paper', safe='')}"
        )
        assert encoded_entity_response.status_code == 200
        assert encoded_entity_response.json()["entity_id"] == "entity/paper"
        assert encoded_entity_response.json()["layout_ref"] == "Paper"

        missing_entity_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities/missing-entity"
        )
        assert missing_entity_response.status_code == 404
        assert missing_entity_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": "Revision entity with identifier 'missing-entity' not found",
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

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        drawing_revision = drawing_revisions[0]

        missing_revision_id = uuid.uuid4()
        missing_response = await async_client.get(f"/v1/revisions/{missing_revision_id}/layouts")
        missing_entity_response = await async_client.get(
            f"/v1/revisions/{missing_revision_id}/entities/entity-parent"
        )

        assert missing_response.status_code == 404
        assert missing_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{missing_revision_id}' not found",
                "details": None,
            }
        }
        assert missing_entity_response.status_code == 404
        assert missing_entity_response.json() == missing_response.json()

        delete_response = await async_client.delete(f"/v1/projects/{project['id']}")
        assert delete_response.status_code == 204

        inactive_response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/layouts")
        inactive_entity_response = await async_client.get(
            f"/v1/revisions/{drawing_revision.id}/entities/entity-parent"
        )

        assert inactive_response.status_code == 404
        assert inactive_response.json() == {
            "error": {
                "code": ErrorCode.NOT_FOUND.value,
                "message": f"Drawing revision with identifier '{drawing_revision.id}' not found",
                "details": None,
            }
        }
        assert inactive_entity_response.status_code == 404
        assert inactive_entity_response.json() == inactive_response.json()

    async def test_legend_devices_endpoint_returns_schedule_from_legend(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """/legend-devices resolves body tags against the legend into a typed schedule."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        adapter_output_id = uuid.uuid4()
        revision_id = uuid.uuid4()
        assert job.extraction_profile_id is not None

        def _tb(text: str, x: float, y: float) -> dict[str, Any]:
            return {
                "text": text,
                "bbox": {"x_min": x, "y_min": y, "x_max": x + 30.0, "y_max": y + 12.0},
            }

        text_blocks = [
            _tb("LEGEND", 800.0, 10.0),
            _tb("S", 805.0, 40.0),
            _tb("STATIC DOME CAMERA", 840.0, 40.0),
            _tb("S", 100.0, 300.0),  # body instances
            _tb("S", 250.0, 500.0),
        ]

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
                        "schema_version": _FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        "layouts": [],
                        "layers": [],
                        "blocks": [],
                        "entities": [],
                        "metadata": {"text_blocks": text_blocks},
                    },
                    provenance_json={"adapter": {"key": _FAKE_RUNNER_ADAPTER_KEY}},
                    confidence_json={"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
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
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                )
            )
            await session.commit()

        response = await async_client.get(f"/v1/revisions/{revision_id}/legend-devices")
        assert response.status_code == 200
        body = response.json()
        assert body["schedule"] == [
            {"abbreviation": "S", "type_name": "STATIC DOME CAMERA", "count": 2}
        ]
        assert len(body["items"]) == 2
        assert all(item["type_name"] == "STATIC DOME CAMERA" for item in body["items"])
        assert body["summary"]["total_devices"] == 2
        assert body["summary"]["legend_size"] >= 1

    async def test_layer_roles_endpoint_classifies_pen_layers(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """/layer-roles derives semantic roles for pen-signature layers."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        adapter_output_id = uuid.uuid4()
        revision_id = uuid.uuid4()
        assert job.extraction_profile_id is not None

        pens = ["pen-eaeaea-w0.51", "pen-000000-w0.71", "pen-00ff00-w1.42", "default"]

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
                    canonical_json={"layouts": [], "layers": [], "blocks": [], "entities": []},
                    provenance_json={"adapter": {"key": _FAKE_RUNNER_ADAPTER_KEY}},
                    confidence_json={"score": _FAKE_RUNNER_CONFIDENCE_SCORE},
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
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                )
            )
            for index, name in enumerate(pens):
                session.add(
                    RevisionLayer(
                        id=uuid.uuid4(),
                        project_id=job.project_id,
                        source_file_id=job.file_id,
                        extraction_profile_id=job.extraction_profile_id,
                        source_job_id=job.id,
                        drawing_revision_id=revision_id,
                        adapter_run_output_id=adapter_output_id,
                        canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                        sequence_index=index,
                        payload_json={"name": name},
                        layer_ref=name,
                    )
                )
            await session.commit()

        response = await async_client.get(f"/v1/revisions/{revision_id}/layer-roles")
        assert response.status_code == 200
        body = response.json()
        roles = {item["name"]: item["role"] for item in body["items"]}
        assert roles == {
            "pen-eaeaea-w0.51": "background",
            "pen-000000-w0.71": "foreground",
            "pen-00ff00-w1.42": "services",
            "default": "unknown",
        }
        assert body["rule_version"] == "1"
        assert body["summary"]["counts"]["services"] == 1

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
                    canonical_entity_schema_version=_FAKE_RUNNER_CANONICAL_SCHEMA_VERSION,
                )
            )
            await session.commit()

        response = await async_client.get(f"/v1/revisions/{revision_id}/entities")
        singular_response = await async_client.get(
            f"/v1/revisions/{revision_id}/entities/missing-entity"
        )

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
        assert singular_response.status_code == 409
        assert singular_response.json() == response.json()

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

        (
            _adapter_outputs,
            drawing_revisions,
            _validation_reports,
            _generated_artifacts,
        ) = await _load_project_outputs(project["id"])
        drawing_revision = drawing_revisions[0]

        expected_counts = {"layouts": 0, "layers": 0, "blocks": 0, "entities": 0}

        for suffix in ("layouts", "layers", "blocks", "entities"):
            response = await async_client.get(f"/v1/revisions/{drawing_revision.id}/{suffix}")
            assert response.status_code == 200
            assert response.json()["items"] == []
            assert response.json()["next_cursor"] is None
            assert response.json()["counts"] == expected_counts
            assert response.json()["manifest"]["counts"] == expected_counts
