"""Integration tests for the per-revision orientation summary API.

The summary is a pure aggregation; these tests assert each aggregated field
matches the default response of its dedicated underlying resource.
"""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

import httpx
import pytest

import app.jobs.worker as worker_module
from app.core.errors import ErrorCode
from app.ingestion.finalization import IngestFinalizationPayload
from app.ingestion.runner import IngestionRunRequest
from app.jobs.worker import process_ingest_job
from tests.conftest import requires_database
from tests.jobs_test_helpers import _create_project, _get_job_for_file, _upload_file
from tests.test_ingest_output_persistence import (
    _build_contract_entity,
    _load_project_outputs,
    _replace_fake_canonical_payload,
)
from tests.test_jobs import _build_fake_ingest_payload

_DXF_UNITS = {
    "normalized": "millimeter",
    "source": "$INSUNITS",
    "source_value": 4,
    "conversion_target": "meter",
    "conversion_factor": 0.001,
}


def _materialized_runner() -> Callable[[IngestionRunRequest], Awaitable[IngestFinalizationPayload]]:
    """Fake ``run_ingestion`` producing a small multi-layer revision with units."""

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        payload = _replace_fake_canonical_payload(
            payload,
            layouts=[{"layout_ref": "Model", "name": "Model"}],
            layers=[
                {"layer_ref": "A-WALL", "name": "A-WALL"},
                {"layer_ref": "A-DOOR", "name": "A-DOOR"},
                {"layer_ref": "M-DUCT", "name": "M-DUCT"},
            ],
            blocks=[{"block_ref": "DOOR-1", "name": "DOOR-1"}],
            entities=[
                _build_contract_entity(
                    entity_id="device-1",
                    entity_type="insert",
                    layer_ref="A-DOOR",
                    block_ref="DOOR-1",
                    source_id="src-device-1",
                ),
                _build_contract_entity(
                    entity_id="wall-1",
                    entity_type="line",
                    layer_ref="A-WALL",
                    source_id="src-wall-1",
                    source_hash="a" * 64,
                ),
            ],
        )
        canonical = {**payload.canonical_json, "units": _DXF_UNITS}
        return replace(payload, canonical_json=canonical)

    return _run


@requires_database
class TestRevisionSummaryApi:
    """Tests for ``GET /revisions/{revision_id}/summary``."""

    async def _ingest(self, async_client: httpx.AsyncClient) -> Any:
        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(job.id)
        _outputs, drawing_revisions, _reports, _artifacts = await _load_project_outputs(
            project["id"]
        )
        return drawing_revisions[0]

    async def test_summary_aggregates_and_matches_underlying_resources(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One call returns counts/scale/coverage matching the dedicated endpoints."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _materialized_runner())

        revision = await self._ingest(async_client)
        rid = revision.id

        summary = (await async_client.get(f"/v1/revisions/{rid}/summary")).json()
        entities = (await async_client.get(f"/v1/revisions/{rid}/entities")).json()
        layer_roles = (await async_client.get(f"/v1/revisions/{rid}/layer-roles")).json()
        devices = (await async_client.get(f"/v1/revisions/{rid}/devices")).json()
        rooms = (await async_client.get(f"/v1/revisions/{rid}/rooms")).json()
        scale = (await async_client.get(f"/v1/revisions/{rid}/scale")).json()
        report = (await async_client.get(f"/v1/revisions/{rid}/validation-report")).json()

        assert summary["revision_id"] == str(rid)

        # Entity counts mirror the materialization manifest.
        assert summary["entity_counts"] == entities["manifest"]["counts"]
        assert summary["entity_counts"]["layers"] == 3
        assert summary["entity_counts"]["entities"] == 2

        # Layers + roles mirror /layer-roles.
        assert summary["layer_count"] == len(layer_roles["items"])
        assert summary["layer_roles"] == layer_roles["summary"]["counts"]

        # Devices mirror /devices schedule total.
        assert summary["device_count"] == devices["association"]["total_devices"]

        # Rooms mirror /rooms summary.
        assert summary["room_count"] == rooms["summary"]["rooms"]
        assert summary["named_room_count"] == rooms["summary"]["named_rooms"]

        # Scale mirrors /scale.
        assert summary["scale"] == scale
        assert summary["scale"]["units"]["normalized"] == "millimeter"

        # Coverage mirrors /validation-report; by_type echoes coverage entities.
        assert summary["coverage"] == report.get("coverage")
        expected_by_type = (report.get("coverage") or {}).get("entities", {}).get("by_type", {})
        assert summary["entities_by_type"] == expected_by_type

    async def test_summary_room_counts_match_rooms_when_labels_off_tag_layer(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression for #584: /summary and /rooms must agree even when room labels sit on a
        non-device-tag layer. The old /summary path built labels from the device-tag loader and
        missed them, reporting a different room count than /rooms (which uses all text)."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        def _text(entity_id: str, layer: str, text: str, x: float, y: float) -> dict[str, Any]:
            entity = _build_contract_entity(
                entity_id=entity_id,
                entity_type="text",
                layer_ref=layer,
                source_id=f"s-{entity_id}",
            )
            entity["geometry_json"] = {"type": "text", "text": text, "insertion": {"x": x, "y": y}}
            return entity

        async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
            payload = _build_fake_ingest_payload(request)
            return _replace_fake_canonical_payload(
                payload,
                layouts=[{"layout_ref": "Model", "name": "Model"}],
                layers=[
                    {"layer_ref": "A-IDEN", "name": "A-IDEN"},  # room-label layer (no tag token)
                    {"layer_ref": "Device Tags", "name": "Device Tags"},  # matches a tag token
                ],
                blocks=[],
                entities=[
                    _text("rname", "A-IDEN", "PH Plantroom", 5.0, 5.4),
                    _text("rnum", "A-IDEN", "0.9.01", 5.0, 5.0),
                    _text("dtag", "Device Tags", "H", 50.0, 50.0),
                ],
            )

        monkeypatch.setattr(worker_module, "run_ingestion", _run)
        revision = await self._ingest(async_client)
        rid = revision.id

        summary = (await async_client.get(f"/v1/revisions/{rid}/summary")).json()
        rooms = (await async_client.get(f"/v1/revisions/{rid}/rooms")).json()

        # Meaningful (not 0==0): the fixture yields a real named, numbered label room.
        assert rooms["summary"]["named_rooms"] >= 1
        assert summary["room_count"] == rooms["summary"]["rooms"]
        assert summary["named_room_count"] == rooms["summary"]["named_rooms"]

    async def test_missing_revision_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
    ) -> None:
        """An unknown revision id returns the standard not-found error envelope."""
        _ = self

        response = await async_client.get(f"/v1/revisions/{uuid.uuid4()}/summary")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value
