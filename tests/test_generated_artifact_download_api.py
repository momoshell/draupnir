"""Integration tests for the generated artifact download API."""

import hashlib
import uuid
from datetime import UTC, datetime

import httpx
import pytest

import app.db.session as session_module
from app.core.errors import ErrorCode
from app.models.generated_artifact import GeneratedArtifact
from app.storage import get_storage
from app.storage.keys import build_generated_artifact_storage_key
from tests.conftest import requires_database
from tests.jobs_test_helpers import (
    _create_project,
    _get_job_for_file,
    _mark_source_deleted,
    _upload_file,
)

_WELL_FORMED_BUT_WRONG_CHECKSUM = "a" * 64


async def _persist_generated_artifact(
    *,
    project_id: uuid.UUID,
    source_file_id: uuid.UUID,
    job_id: uuid.UUID,
    body: bytes | None,
    name: str = "report.json",
    media_type: str = "application/json",
    checksum_override: str | None = None,
    deleted: bool = False,
) -> uuid.UUID:
    """Insert a generated artifact row, optionally persisting its bytes."""

    artifact_id = uuid.uuid4()
    storage_key = build_generated_artifact_storage_key(artifact_id, "payload.bin")
    storage = get_storage()

    if body is not None:
        meta = await storage.put(storage_key, body, immutable=True)
        storage_uri = meta.storage_uri
        size_bytes = meta.size_bytes
        checksum = meta.checksum_sha256
    else:
        storage_uri = f"file:///nonexistent/{storage_key}"
        size_bytes = 0
        checksum = "0" * 64

    if checksum_override is not None:
        checksum = checksum_override

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        artifact = GeneratedArtifact(
            id=artifact_id,
            project_id=project_id,
            source_file_id=source_file_id,
            job_id=job_id,
            artifact_kind="canonical_entities",
            name=name,
            format="json",
            media_type=media_type,
            size_bytes=size_bytes,
            checksum_sha256=checksum,
            generator_name="tests.generated_artifact_download",
            generator_version="1.0.0",
            generator_config_json={},
            storage_key=storage_key,
            storage_uri=storage_uri,
            lineage_json={},
            deleted_at=datetime.now(UTC) if deleted else None,
        )
        session.add(artifact)
        await session.commit()

    return artifact_id


@requires_database
class TestGeneratedArtifactDownloadApi:
    """Tests for downloading generated artifact bytes through the API."""

    async def _seed_lineage(
        self,
        async_client: httpx.AsyncClient,
    ) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
        """Create a project/file/job lineage and return their identifiers."""

        project = await _create_project(async_client)
        uploaded = await _upload_file(async_client, project["id"])
        job = await _get_job_for_file(str(uploaded["id"]))
        return uuid.UUID(project["id"]), uuid.UUID(str(uploaded["id"])), job.id

    async def test_download_returns_artifact_bytes_and_headers(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """A visible artifact downloads with its bytes and integrity headers."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        project_id, file_id, job_id = await self._seed_lineage(async_client)
        body = b'{"canonical": true}'
        artifact_id = await _persist_generated_artifact(
            project_id=project_id,
            source_file_id=file_id,
            job_id=job_id,
            body=body,
            name="canonical.json",
            media_type="application/json",
        )

        response = await async_client.get(
            f"/v1/generated-artifacts/{artifact_id}/download",
        )

        assert response.status_code == 200
        assert response.content == body
        assert response.headers["content-type"].startswith("application/json")
        assert response.headers["content-length"] == str(len(body))
        assert 'filename="canonical.json"' in response.headers["content-disposition"]
        expected_checksum = hashlib.sha256(body).hexdigest()
        assert response.headers["x-checksum-sha256"] == expected_checksum
        assert response.headers["etag"] == f'"{expected_checksum}"'

    async def test_download_unknown_artifact_returns_sanitized_404(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """An unknown artifact id returns a sanitized not-found error."""
        _ = (self, cleanup_projects)

        response = await async_client.get(
            f"/v1/generated-artifacts/{uuid.uuid4()}/download",
        )

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    async def test_download_soft_deleted_artifact_returns_404(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """A soft-deleted artifact is invisible to download."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        project_id, file_id, job_id = await self._seed_lineage(async_client)
        artifact_id = await _persist_generated_artifact(
            project_id=project_id,
            source_file_id=file_id,
            job_id=job_id,
            body=b"deleted-bytes",
            deleted=True,
        )

        response = await async_client.get(
            f"/v1/generated-artifacts/{artifact_id}/download",
        )

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    @pytest.mark.parametrize(
        ("delete_project", "delete_file"),
        [(False, True), (True, False)],
    )
    async def test_download_invisible_lineage_returns_404(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        delete_project: bool,
        delete_file: bool,
    ) -> None:
        """An artifact under soft-deleted file/project lineage is not downloadable."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        project_id, file_id, job_id = await self._seed_lineage(async_client)
        artifact_id = await _persist_generated_artifact(
            project_id=project_id,
            source_file_id=file_id,
            job_id=job_id,
            body=b"hidden",
        )
        await _mark_source_deleted(
            project_id,
            file_id,
            delete_project=delete_project,
            delete_file=delete_file,
        )

        response = await async_client.get(
            f"/v1/generated-artifacts/{artifact_id}/download",
        )

        assert response.status_code == 404
        assert response.json()["error"]["code"] == ErrorCode.NOT_FOUND.value

    async def test_download_missing_storage_bytes_returns_500_storage_failed(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Visible lineage with absent stored bytes is a sanitized storage failure."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        project_id, file_id, job_id = await self._seed_lineage(async_client)
        artifact_id = await _persist_generated_artifact(
            project_id=project_id,
            source_file_id=file_id,
            job_id=job_id,
            body=None,
        )

        response = await async_client.get(
            f"/v1/generated-artifacts/{artifact_id}/download",
        )

        assert response.status_code == 500
        assert response.json()["error"]["code"] == ErrorCode.STORAGE_FAILED.value

    async def test_download_checksum_mismatch_returns_500_storage_failed(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Corrupt stored bytes (checksum mismatch) is a sanitized storage failure."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        project_id, file_id, job_id = await self._seed_lineage(async_client)
        artifact_id = await _persist_generated_artifact(
            project_id=project_id,
            source_file_id=file_id,
            job_id=job_id,
            body=b"real-bytes",
            checksum_override=_WELL_FORMED_BUT_WRONG_CHECKSUM,
        )

        response = await async_client.get(
            f"/v1/generated-artifacts/{artifact_id}/download",
        )

        assert response.status_code == 500
        assert response.json()["error"]["code"] == ErrorCode.STORAGE_FAILED.value

    async def test_download_sanitizes_content_disposition_filename(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Artifact names with header-injection characters are sanitized."""
        _ = (self, cleanup_projects, enqueued_job_ids)

        project_id, file_id, job_id = await self._seed_lineage(async_client)
        artifact_id = await _persist_generated_artifact(
            project_id=project_id,
            source_file_id=file_id,
            job_id=job_id,
            body=b"safe",
            name='evil"\r\nSet-Cookie: x=1.json',
        )

        response = await async_client.get(
            f"/v1/generated-artifacts/{artifact_id}/download",
        )

        assert response.status_code == 200
        disposition = response.headers["content-disposition"]
        # CRLF removal is what actually prevents header injection.
        assert "\r" not in disposition
        assert "\n" not in disposition
        # The ASCII fallback must not contain the raw quote that would break out
        # of the quoted filename value.
        ascii_fallback = disposition.split("filename=", 1)[1].split(";", 1)[0]
        assert ascii_fallback == '"evilSet-Cookie: x=1.json"'
        # The RFC 5987 form carries the exact name percent-encoded.
        assert "filename*=UTF-8''" in disposition
