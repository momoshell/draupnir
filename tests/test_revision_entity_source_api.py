"""Integration test for the source-anchored entity drill-down API (#522).

Uploads the real DXF fixture (so the original is stored) and materializes an entity
whose recorded source identity is the fixture's real DXF handle, then drills from the
canonical entity back to its raw source object. A controlled fake runner keeps the
test isolated from other lane tests while the re-derivation still runs against the
genuine stored file.
"""

import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

import app.jobs.worker as worker_module
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

_DXF_FIXTURE = Path(__file__).with_name("fixtures") / "dxf" / "simple-line.dxf"
# The single LINE in simple-line.dxf carries DXF handle "1" on layer "0".
_LINE_HANDLE = "1"


def _fixture_handle_runner() -> Callable[
    [IngestionRunRequest], Awaitable[IngestFinalizationPayload]
]:
    """Materialize one entity whose source identity is the fixture's real DXF handle."""

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        payload = _build_fake_ingest_payload(request)
        return _replace_fake_canonical_payload(
            payload,
            layers=[{"layer_ref": "0", "name": "0"}],
            entities=[
                _build_contract_entity(
                    entity_id="dxf-line-1",
                    entity_type="line",
                    layer_ref="0",
                    source_id=_LINE_HANDLE,
                    source_hash="a" * 64,
                )
            ],
        )

    return _run


@requires_database
class TestRevisionEntitySourceApi:
    """Tests for ``GET /revisions/{revision_id}/entities/{entity_id}/source``."""

    async def _ingest_dxf(self, async_client: httpx.AsyncClient) -> tuple[Any, Any]:
        project = await _create_project(async_client)
        uploaded = await _upload_file(
            async_client,
            project["id"],
            filename="simple-line.dxf",
            content=_DXF_FIXTURE.read_bytes(),
            media_type="application/dxf",
        )
        job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(job.id)
        _outputs, revisions, _reports, _artifacts = await _load_project_outputs(project["id"])
        return revisions[0], uploaded

    async def test_drills_from_entity_to_raw_dxf_fragment(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A DXF entity resolves to its raw native object via the stored source."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _fixture_handle_runner())

        revision, _uploaded = await self._ingest_dxf(async_client)

        response = await async_client.get(f"/v1/revisions/{revision.id}/entities/dxf-line-1/source")

        assert response.status_code == 200
        body = response.json()
        assert body["entity_id"] == "dxf-line-1"
        assert body["source_identity"] == _LINE_HANDLE
        assert body["upload_format"] == "dxf"
        assert body["available"] is True
        assert body["fragment"]["format"] == "dxf"
        assert body["fragment"]["native_type"] == "LINE"
        assert body["fragment"]["handle"] == _LINE_HANDLE
        assert body["reason"] is None

    async def test_unknown_entity_returns_404(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An unknown entity id on a materialized revision returns 404."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _fixture_handle_runner())

        revision, _uploaded = await self._ingest_dxf(async_client)
        response = await async_client.get(
            f"/v1/revisions/{revision.id}/entities/{uuid.uuid4()}/source"
        )
        assert response.status_code == 404
