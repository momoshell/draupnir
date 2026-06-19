"""Integration test for the revision-to-revision diff API (#524).

Ingests a file, then reprocesses it with a deliberately different canonical so the
two revisions differ by one added, one removed, and one changed entity — exercising
the full stack (materialized source_identity/source_hash + the diff endpoint).
"""

import uuid
from collections.abc import Awaitable, Callable
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

_A_WALL = {"layer_ref": "A-WALL", "name": "A-WALL"}
_A_DOOR = {"layer_ref": "A-DOOR", "name": "A-DOOR"}
_M_DUCT = {"layer_ref": "M-DUCT", "name": "M-DUCT"}


def _entity(source_id: str, entity_type: str, layer: str, source_hash: str) -> dict[str, Any]:
    return _build_contract_entity(
        entity_id=source_id,
        entity_type=entity_type,
        layer_ref=layer,
        source_id=source_id,
        source_hash=source_hash,
    )


def _two_revision_runner() -> Callable[[IngestionRunRequest], Awaitable[IngestFinalizationPayload]]:
    """First ingest → {c1,c2} on A-WALL/A-DOOR; reprocess → {c1', c3} on A-WALL/M-DUCT."""

    calls = {"n": 0}

    async def _run(request: IngestionRunRequest) -> IngestFinalizationPayload:
        calls["n"] += 1
        payload = _build_fake_ingest_payload(request)
        if calls["n"] == 1:
            return _replace_fake_canonical_payload(
                payload,
                layers=[_A_WALL, _A_DOOR],
                entities=[
                    _entity("c1", "line", "A-WALL", "a" * 64),
                    _entity("c2", "line", "A-DOOR", "b" * 64),
                ],
            )
        return _replace_fake_canonical_payload(
            payload,
            layers=[_A_WALL, _M_DUCT],
            entities=[
                _entity("c1", "line", "A-WALL", "c" * 64),  # same identity, new hash → changed
                _entity("c3", "circle", "A-WALL", "d" * 64),  # added
            ],
        )

    return _run


@requires_database
class TestRevisionDiffApi:
    """Tests for ``GET /revisions/{revision_id}/diff``."""

    async def _two_revisions(self, async_client: httpx.AsyncClient) -> tuple[Any, Any, str]:
        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        ingest_job = await _get_job_for_file(str(uploaded["id"]))
        await process_ingest_job(ingest_job.id)

        reprocess = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json={"extraction_profile": {"profile_version": "v0.1"}},
        )
        assert reprocess.status_code == 202
        await process_ingest_job(uuid.UUID(reprocess.json()["id"]))

        _outputs, revisions, _reports, _artifacts = await _load_project_outputs(project["id"])
        return revisions[0], revisions[1], project["id"]

    async def test_diff_reports_added_removed_changed(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Diffing a reprocessed revision against its base surfaces the structural delta."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _two_revision_runner())

        base, target, _project_id = await self._two_revisions(async_client)

        response = await async_client.get(
            f"/v1/revisions/{target.id}/diff",
            params={"against": str(base.id), "fields": "added,removed,changed"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["base_revision_id"] == str(base.id)
        assert body["target_revision_id"] == str(target.id)

        entities = body["entities"]
        assert (entities["added"], entities["removed"], entities["changed"]) == (1, 1, 1)
        assert entities["added_ids"] == ["c3"]
        assert entities["removed_ids"] == ["c2"]
        assert entities["changed_ids"] == ["c1"]

        assert body["layers"]["added"] == ["M-DUCT"]
        assert body["layers"]["removed"] == ["A-DOOR"]
        assert body["counts_by_type"]["circle"]["delta"] == 1
        assert body["counts_by_type"]["line"]["delta"] == -1

    async def test_diff_against_missing_revision_returns_404(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An unknown ``against`` revision yields the standard not-found envelope."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _two_revision_runner())

        _base, target, _project_id = await self._two_revisions(async_client)
        response = await async_client.get(
            f"/v1/revisions/{target.id}/diff", params={"against": str(uuid.uuid4())}
        )
        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    async def test_diff_across_files_returns_400(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Diffing revisions of two different files is rejected (same-file required)."""
        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        monkeypatch.setattr(worker_module, "run_ingestion", _two_revision_runner())

        project = await _create_project(async_client)
        file_a = await _upload_file(async_client, project["id"], filename="a.pdf")
        await process_ingest_job((await _get_job_for_file(str(file_a["id"]))).id)
        file_b = await _upload_file(async_client, project["id"], filename="b.pdf")
        await process_ingest_job((await _get_job_for_file(str(file_b["id"]))).id)

        _outputs, revisions, _reports, _artifacts = await _load_project_outputs(project["id"])
        rev_a, rev_b = revisions[0], revisions[1]

        response = await async_client.get(
            f"/v1/revisions/{rev_a.id}/diff", params={"against": str(rev_b.id)}
        )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == ErrorCode.INPUT_INVALID.value
