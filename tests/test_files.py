"""Integration tests for project file upload and retrieval endpoints."""

import asyncio
import hashlib
import uuid
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import select

import app.api.v1.files as files_api
import app.db.session as session_module
from app.core.config import settings
from app.models.file import File as FileModel
from app.models.job import Job
from app.storage import LocalFilesystemStorage, StoredObjectMeta, get_storage
from tests.conftest import requires_database


@pytest_asyncio.fixture
async def created_project(
    async_client: httpx.AsyncClient,
    cleanup_projects: None,
) -> dict[str, Any]:
    """Create a project and return its data."""
    response = await async_client.post(
        "/v1/projects",
        json={
            "name": "Files Test Project",
            "description": "A project for file tests",
        },
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


async def _upload_file(
    async_client: httpx.AsyncClient,
    project_id: str,
    filename: str,
    content: bytes,
    media_type: str,
) -> dict[str, Any]:
    """Upload a file for a project and return response payload."""
    response = await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": (filename, content, media_type)},
    )
    assert response.status_code == 201
    return cast(dict[str, Any], response.json())


def _make_get_db_override_with_commit_error(
    commit_error: BaseException,
) -> Callable[[], AsyncGenerator[Any, None]]:
    """Create a request-scoped get_db override with an instance-level commit failure."""

    async def _override_get_db() -> AsyncGenerator[Any, None]:
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        session = session_maker()

        async def _fail_commit() -> None:
            raise commit_error

        cast(Any, session).commit = _fail_commit

        try:
            yield session
        finally:
            await session.close()

    return _override_get_db


class RecordingStorage:
    """Test double that records upload persistence inputs."""

    def __init__(self, *, returned_checksum_sha256: str | None = None) -> None:
        self.put_calls: list[tuple[str, Path, bytes, bool]] = []
        self.delete_calls: list[str] = []
        self.returned_checksum_sha256 = returned_checksum_sha256

    async def put(
        self,
        key: str,
        data: bytes | Path,
        *,
        immutable: bool = False,
    ) -> StoredObjectMeta:
        """Record put calls and return deterministic metadata."""
        assert isinstance(data, Path)
        body = data.read_bytes()
        self.put_calls.append((key, data, body, immutable))
        return StoredObjectMeta(
            key=key,
            storage_uri=f"memory://{key}",
            size_bytes=len(body),
            checksum_sha256=(
                self.returned_checksum_sha256 or hashlib.sha256(body).hexdigest()
            ),
        )

    async def get(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> Any:
        """Unused protocol method for test double completeness."""
        _ = expected_checksum_sha256
        raise NotImplementedError(key)

    async def stat(
        self,
        key: str,
        *,
        expected_checksum_sha256: str | None = None,
    ) -> StoredObjectMeta:
        """Unused protocol method for test double completeness."""
        _ = expected_checksum_sha256
        raise NotImplementedError(key)

    async def exists(self, key: str) -> bool:
        """Unused protocol method for test double completeness."""
        raise NotImplementedError(key)

    async def delete(self, key: str) -> None:
        """Unused protocol method for test double completeness."""
        self.delete_calls.append(key)

    async def delete_failed_put(self, key: str, *, storage_uri: str) -> None:
        """Record failed-put cleanup requests for protocol parity."""
        _ = storage_uri
        self.delete_calls.append(key)

    async def presign(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in_seconds: int = 3600,
    ) -> str | None:
        """Unused protocol method for test double completeness."""
        _ = (key, method, expires_in_seconds)
        return None


class LocalChecksumMismatchStorage(LocalFilesystemStorage):
    """Local storage backend that reports the wrong checksum after writing."""

    async def put(
        self,
        key: str,
        data: bytes | Path,
        *,
        immutable: bool = False,
    ) -> StoredObjectMeta:
        """Persist bytes, then report intentionally mismatched checksum metadata."""
        meta = await super().put(key, data, immutable=immutable)
        return StoredObjectMeta(
            key=meta.key,
            storage_uri=meta.storage_uri,
            size_bytes=meta.size_bytes,
            checksum_sha256="0" * 64,
        )


@requires_database
class TestProjectFiles:
    """Tests for project file upload and retrieval endpoints."""

    @pytest.fixture(autouse=True)
    def _stub_enqueue_ingest_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Stub enqueue publish so file endpoint tests do not require RabbitMQ."""

        def _fake_enqueue(job_id: uuid.UUID) -> None:
            _ = job_id

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _fake_enqueue)

    async def test_upload_file_creates_file_and_pending_job(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should persist file metadata/content and create a pending job."""
        _ = self
        payload = b"%PDF-1.7\nmock-pdf-content\n"

        uploaded = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="plan.pdf",
            content=payload,
            media_type="application/pdf",
        )

        assert uploaded["project_id"] == created_project["id"]
        assert uploaded["original_filename"] == "plan.pdf"
        assert uploaded["media_type"] == "application/pdf"
        assert uploaded["detected_format"] == "pdf"
        assert uploaded["size_bytes"] == len(payload)
        assert uploaded["checksum_sha256"] == hashlib.sha256(payload).hexdigest()
        assert uploaded["immutable"] is True
        assert "created_at" in uploaded
        assert "storage_uri" not in uploaded

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(FileModel.id == uuid.UUID(str(uploaded["id"])))
            )
            file_row = file_result.scalar_one_or_none()

            result = await session.execute(
                select(Job).where(Job.file_id == uuid.UUID(str(uploaded["id"])))
            )
            job = result.scalar_one_or_none()

        assert file_row is not None
        storage_uri = file_row.storage_uri
        assert storage_uri.startswith("file://")
        stored_path = Path(storage_uri.removeprefix("file://"))
        assert stored_path.exists()
        assert stored_path.relative_to(Path(settings.upload_storage_root).resolve()).as_posix() == (
            f"originals/{uploaded['id']}/{uploaded['checksum_sha256']}"
        )
        assert stored_path.read_bytes() == payload
        mode = stored_path.stat().st_mode & 0o777
        assert mode == 0o444
        assert (mode & 0o333) == 0
        assert str(stored_path).startswith(str(Path(settings.upload_storage_root).resolve()))

        assert job is not None
        assert job.project_id == uuid.UUID(str(created_project["id"]))
        assert job.file_id == uuid.UUID(str(uploaded["id"]))
        assert job.job_type == "ingest"
        assert job.status == "pending"
        assert job.attempts == 0
        assert job.cancel_requested is False

    async def test_reupload_same_bytes_creates_new_file_row(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Uploading identical bytes twice should create two distinct file rows."""
        _ = self
        payload = b"%PDF-1.7\nsame-content"

        first = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="same-a.pdf",
            content=payload,
            media_type="application/pdf",
        )
        second = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="same-b.pdf",
            content=payload,
            media_type="application/pdf",
        )

        assert first["id"] != second["id"]
        assert first["checksum_sha256"] == second["checksum_sha256"]

        response = await async_client.get(f"/v1/projects/{created_project['id']}/files")
        assert response.status_code == 200
        listed = response.json()
        assert len(listed["items"]) == 2

    async def test_upload_file_persists_through_storage_dependency_put(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
        app: FastAPI,
    ) -> None:
        """POST should hand final persistence to the injected storage backend."""
        _ = self
        payload = b"%PDF-1.7\nstorage-spy"
        storage = RecordingStorage()
        app.dependency_overrides[get_storage] = lambda: storage
        try:
            uploaded = await _upload_file(
                async_client=async_client,
                project_id=created_project["id"],
                filename="client-name.pdf",
                content=payload,
                media_type="application/pdf",
            )
        finally:
            app.dependency_overrides.pop(get_storage, None)

        expected_key = (
            f"originals/{uploaded['id']}/{hashlib.sha256(payload).hexdigest()}"
        )
        assert len(storage.put_calls) == 1
        put_key, put_path, put_body, put_immutable = storage.put_calls[0]
        assert put_key == expected_key
        assert put_body == payload
        assert put_immutable is True
        assert put_path.name.endswith(".part")
        assert ".staging" in put_path.parts

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(FileModel.id == uuid.UUID(str(uploaded["id"])))
            )
            file_row = file_result.scalar_one_or_none()

        assert file_row is not None
        assert file_row.storage_uri == f"memory://{expected_key}"

    async def test_upload_file_rejects_storage_checksum_mismatch_and_cleans_persisted_upload(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
        app: FastAPI,
    ) -> None:
        """POST should cleanup real local-storage writes on checksum mismatch before DB commit."""
        _ = self
        payload = b"%PDF-1.7\nchecksum-mismatch"
        upload_root = Path(settings.upload_storage_root).resolve()
        storage = LocalChecksumMismatchStorage(upload_root)
        app.dependency_overrides[get_storage] = lambda: storage
        try:
            response = await async_client.post(
                f"/v1/projects/{created_project['id']}/files",
                files={"file": ("mismatch.pdf", payload, "application/pdf")},
            )
        finally:
            app.dependency_overrides.pop(get_storage, None)

        assert response.status_code == 500
        assert response.json() == {
            "error": {
                "code": "STORAGE_FAILED",
                "message": "Stored file checksum mismatch detected.",
                "details": None,
            }
        }
        staging_root = upload_root / ".staging"
        assert not staging_root.exists() or not any(staging_root.iterdir())
        originals_root = upload_root / "originals"
        assert not originals_root.exists() or not any(
            path.is_file() for path in originals_root.rglob("*")
        )

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(
                    FileModel.project_id == uuid.UUID(str(created_project["id"]))
                )
            )
            job_result = await session.execute(
                select(Job).where(Job.project_id == uuid.UUID(str(created_project["id"])))
            )

        assert file_result.scalars().all() == []
        assert job_result.scalars().all() == []

    async def test_list_project_files_success(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """GET list should return files for the target project."""
        _ = self
        first = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="first.dwg",
            content=b"AC1032-dwg-mock",
            media_type="application/acad",
        )
        second = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="second.pdf",
            content=b"%PDF-1.7\npdf-mock",
            media_type="application/pdf",
        )

        response = await async_client.get(f"/v1/projects/{created_project['id']}/files")
        assert response.status_code == 200

        data = response.json()
        assert "items" in data
        assert "next_cursor" in data
        assert data["next_cursor"] is None
        assert len(data["items"]) == 2

        returned_ids = {item["id"] for item in data["items"]}
        assert returned_ids == {first["id"], second["id"]}

    async def test_list_project_files_pagination(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """GET list should paginate files with cursor + limit."""
        _ = self

        created_ids: list[str] = []
        for i in range(5):
            uploaded = await _upload_file(
                async_client=async_client,
                project_id=created_project["id"],
                filename=f"file-{i}.pdf",
                content=f"%PDF-1.7\npayload-{i}".encode(),
                media_type="application/pdf",
            )
            created_ids.append(str(uploaded["id"]))

        first_response = await async_client.get(
            f"/v1/projects/{created_project['id']}/files?limit=2"
        )
        assert first_response.status_code == 200
        first_data = first_response.json()
        assert len(first_data["items"]) == 2
        assert first_data["next_cursor"] is not None
        first_page_ids = [str(item["id"]) for item in first_data["items"]]

        second_response = await async_client.get(
            f"/v1/projects/{created_project['id']}/files?limit=2&cursor={first_data['next_cursor']}"
        )
        assert second_response.status_code == 200
        second_data = second_response.json()
        assert len(second_data["items"]) == 2
        assert second_data["next_cursor"] is not None
        second_page_ids = [str(item["id"]) for item in second_data["items"]]

        third_response = await async_client.get(
            f"/v1/projects/{created_project['id']}/files?limit=2&cursor={second_data['next_cursor']}"
        )
        assert third_response.status_code == 200
        third_data = third_response.json()
        assert len(third_data["items"]) == 1
        assert third_data["next_cursor"] is None
        third_page_ids = [str(item["id"]) for item in third_data["items"]]

        assert not set(first_page_ids) & set(second_page_ids)
        assert not set(first_page_ids) & set(third_page_ids)
        assert not set(second_page_ids) & set(third_page_ids)
        assert set(first_page_ids + second_page_ids + third_page_ids) == set(created_ids)

    async def test_list_project_files_invalid_cursor(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """GET list should return a 400 envelope for malformed cursors."""
        _ = self

        response = await async_client.get(
            f"/v1/projects/{created_project['id']}/files?cursor=invalid-cursor"
        )
        assert response.status_code == 400

        data = response.json()
        assert data == {
            "error": {
                "code": "INVALID_CURSOR",
                "message": "Invalid cursor format",
                "details": None,
            }
        }

    async def test_get_project_file_detail_success(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """GET detail should return file metadata for the matching project/file."""
        _ = self
        uploaded = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="detail.ifc",
            content=b"ISO-10303-21;\nHEADER;\nENDSEC;\n",
            media_type="application/octet-stream",
        )

        response = await async_client.get(
            f"/v1/projects/{created_project['id']}/files/{uploaded['id']}"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == uploaded["id"]
        assert data["project_id"] == created_project["id"]
        assert data["original_filename"] == "detail.ifc"

    async def test_get_project_file_not_found(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """GET detail should return File 404 for unknown file id."""
        _ = self
        response = await async_client.get(
            f"/v1/projects/{created_project['id']}/files/{uuid.uuid4()}"
        )
        assert response.status_code == 404
        data = response.json()

        assert data["error"]["code"] == "NOT_FOUND"
        assert "File" in data["error"]["message"]
        assert data["error"]["details"] is None

    async def test_get_project_file_cross_project_returns_file_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """GET detail with mismatched project/file scope should return File 404."""
        _ = self
        _ = cleanup_projects

        first_project_response = await async_client.post(
            "/v1/projects",
            json={"name": "Project One"},
        )
        second_project_response = await async_client.post(
            "/v1/projects", json={"name": "Project Two"}
        )
        assert first_project_response.status_code == 201
        assert second_project_response.status_code == 201

        first_project_id = first_project_response.json()["id"]
        second_project_id = second_project_response.json()["id"]

        uploaded = await _upload_file(
            async_client=async_client,
            project_id=first_project_id,
            filename="scoped.pdf",
            content=b"%PDF-1.7\nscope-test",
            media_type="application/pdf",
        )

        response = await async_client.get(
            f"/v1/projects/{second_project_id}/files/{uploaded['id']}"
        )
        assert response.status_code == 404
        data = response.json()

        assert data["error"]["code"] == "NOT_FOUND"
        assert "File" in data["error"]["message"]
        assert data["error"]["details"] is None

    async def test_upload_file_project_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """POST should return Project 404 when parent project does not exist."""
        _ = self
        _ = cleanup_projects

        response = await async_client.post(
            f"/v1/projects/{uuid.uuid4()}/files",
            files={"file": ("missing.pdf", b"%PDF-1.7\nx", "application/pdf")},
        )
        assert response.status_code == 404
        data = response.json()
        assert data["error"]["code"] == "NOT_FOUND"
        assert "Project" in data["error"]["message"]

    async def test_list_project_files_project_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """GET list should return Project 404 when project does not exist."""
        _ = self
        _ = cleanup_projects

        response = await async_client.get(f"/v1/projects/{uuid.uuid4()}/files")
        assert response.status_code == 404
        data = response.json()
        assert data["error"]["code"] == "NOT_FOUND"
        assert "Project" in data["error"]["message"]

    async def test_upload_file_rejects_payload_over_size_limit(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """POST should reject payloads one byte over cap with a 413 envelope."""
        _ = self
        monkeypatch.setattr(settings, "max_upload_mb", 1)
        cap_bytes = settings.max_upload_mb * 1024 * 1024
        header = b"%PDF-1.7\n"
        payload = header + (b"x" * ((cap_bytes - len(header)) + 1))

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={"file": ("oversize.pdf", payload, "application/pdf")},
        )
        assert response.status_code == 413
        data = response.json()
        assert data["error"]["code"] == "INPUT_INVALID"
        assert (
            data["error"]["message"]
            == f"Uploaded file exceeds maximum allowed size of {settings.max_upload_mb} MB."
        )
        assert data["error"]["details"] is None

        upload_root = Path(settings.upload_storage_root).resolve()
        assert not upload_root.exists() or not any(upload_root.iterdir())

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(
                    FileModel.project_id == uuid.UUID(str(created_project["id"]))
                )
            )
            job_result = await session.execute(
                select(Job).where(Job.project_id == uuid.UUID(str(created_project["id"])))
            )

        assert file_result.scalars().all() == []
        assert job_result.scalars().all() == []

    async def test_upload_file_accepts_payload_exactly_at_size_limit(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """POST should accept payloads exactly at the configured size limit."""
        _ = self
        monkeypatch.setattr(settings, "max_upload_mb", 1)
        cap_bytes = settings.max_upload_mb * 1024 * 1024
        header = b"%PDF-1.7\n"
        payload = header + (b"x" * (cap_bytes - len(header)))

        uploaded = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="at-limit.pdf",
            content=payload,
            media_type="application/pdf",
        )

        assert uploaded["size_bytes"] == cap_bytes

    async def test_upload_file_rejects_unsupported_format_before_writing(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should reject unsupported/unknown formats before file persistence/job creation."""
        _ = self

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={"file": ("plan.xyz", b"unsupported", "application/octet-stream")},
        )

        assert response.status_code == 415
        data = response.json()
        assert data == {
            "error": {
                "code": "INPUT_UNSUPPORTED_FORMAT",
                "message": "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc.",
                "details": None,
            }
        }

        upload_root = Path(settings.upload_storage_root).resolve()
        assert not upload_root.exists() or not any(upload_root.iterdir())

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(
                    FileModel.project_id == uuid.UUID(str(created_project["id"]))
                )
            )
            job_result = await session.execute(
                select(Job).where(Job.project_id == uuid.UUID(str(created_project["id"])))
            )

        assert file_result.scalars().all() == []
        assert job_result.scalars().all() == []

    async def test_upload_file_accepts_bytes_when_filename_and_media_type_mismatch(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should trust sniffed bytes over client-supplied filename/media type."""
        _ = self

        uploaded = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="upload.bin",
            content=b"%PDF-1.7\nmetadata-mismatch",
            media_type="application/octet-stream",
        )

        assert uploaded["original_filename"] == "upload.bin"
        assert uploaded["media_type"] == "application/octet-stream"
        assert uploaded["detected_format"] == "pdf"

    async def test_upload_file_rejects_overlong_original_filename_before_storage_or_db_write(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should reject overlong original_filename before storage/job persistence."""
        _ = self

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={"file": (("a" * 513) + ".pdf", b"%PDF-1.7\ncontent", "application/pdf")},
        )

        assert response.status_code == 400
        data = response.json()
        assert data == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "original_filename exceeds maximum length of 512 characters.",
                "details": None,
            }
        }

        upload_root = Path(settings.upload_storage_root).resolve()
        assert not upload_root.exists() or not any(upload_root.iterdir())

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(
                    FileModel.project_id == uuid.UUID(str(created_project["id"]))
                )
            )
            job_result = await session.execute(
                select(Job).where(Job.project_id == uuid.UUID(str(created_project["id"])))
            )

        assert file_result.scalars().all() == []
        assert job_result.scalars().all() == []

    async def test_upload_file_rejects_overlong_media_type_before_storage_or_db_write(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should reject overlong media_type before storage/job persistence."""
        _ = self

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={
                "file": (
                    "plan.pdf",
                    b"%PDF-1.7\ncontent",
                    "application/" + ("x" * 244),
                )
            },
        )

        assert response.status_code == 400
        data = response.json()
        assert data == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "media_type exceeds maximum length of 255 characters.",
                "details": None,
            }
        }

        upload_root = Path(settings.upload_storage_root).resolve()
        assert not upload_root.exists() or not any(upload_root.iterdir())

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(
                    FileModel.project_id == uuid.UUID(str(created_project["id"]))
                )
            )
            job_result = await session.execute(
                select(Job).where(Job.project_id == uuid.UUID(str(created_project["id"])))
            )

        assert file_result.scalars().all() == []
        assert job_result.scalars().all() == []

    async def test_upload_file_rejects_text_with_nonleading_section(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should reject text that contains SECTION without leading DXF header."""
        _ = self

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={"file": ("plan.txt", b"notes\nSECTION\nmore-notes\n", "text/plain")},
        )

        assert response.status_code == 415
        data = response.json()
        assert data == {
            "error": {
                "code": "INPUT_UNSUPPORTED_FORMAT",
                "message": "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc.",
                "details": None,
            }
        }

        upload_root = Path(settings.upload_storage_root).resolve()
        assert not upload_root.exists() or not any(upload_root.iterdir())

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            file_result = await session.execute(
                select(FileModel).where(
                    FileModel.project_id == uuid.UUID(str(created_project["id"]))
                )
            )
            job_result = await session.execute(
                select(Job).where(Job.project_id == uuid.UUID(str(created_project["id"])))
            )

        assert file_result.scalars().all() == []
        assert job_result.scalars().all() == []

    async def test_upload_file_accepts_dxf_with_utf8_bom_and_ascii_whitespace(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should accept DXF when only UTF-8 BOM/ASCII whitespace precedes header."""
        _ = self

        payload = b"\xef\xbb\xbf \t\r\n0\r\nSECTION\n2\nHEADER\n0\nENDSEC\n0\nEOF\n"
        uploaded = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="plan.dxf",
            content=payload,
            media_type="application/dxf",
        )

        assert uploaded["detected_format"] == "dxf"

    async def test_upload_file_accepts_binary_dxf_with_octet_stream_media_type(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should accept binary DXF bytes even when clients send generic metadata."""
        _ = self

        payload = b"AutoCAD Binary DXF\r\n\x1a\x00\x00\x00binary-dxf-body"
        uploaded = await _upload_file(
            async_client=async_client,
            project_id=created_project["id"],
            filename="plan.dxf",
            content=payload,
            media_type="application/octet-stream",
        )

        assert uploaded["original_filename"] == "plan.dxf"
        assert uploaded["media_type"] == "application/octet-stream"
        assert uploaded["detected_format"] == "dxf"

    async def test_upload_file_rejects_dxf_with_binary_prefix_before_header(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should reject DXF-like payload when binary bytes precede header."""
        _ = self

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={
                "file": (
                    "plan.dxf",
                    b"\x00\x01\t\n0\nSECTION\n2\nHEADER\n0\nENDSEC\n0\nEOF\n",
                    "application/dxf",
                )
            },
        )

        assert response.status_code == 415
        assert response.json() == {
            "error": {
                "code": "INPUT_UNSUPPORTED_FORMAT",
                "message": "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc.",
                "details": None,
            }
        }

    async def test_upload_file_rejects_png_bytes_renamed_as_dxf(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """POST should reject unsupported bytes despite a .dxf filename."""
        _ = self

        response = await async_client.post(
            f"/v1/projects/{created_project['id']}/files",
            files={
                "file": (
                    "plan.dxf",
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR",
                    "application/octet-stream",
                )
            },
        )

        assert response.status_code == 415
        assert response.json() == {
            "error": {
                "code": "INPUT_UNSUPPORTED_FORMAT",
                "message": "Unsupported file format. Supported formats: pdf, dwg, dxf, ifc.",
                "details": None,
            }
        }

    async def test_upload_file_commit_failure_retains_written_bytes_conservatively(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
        app: FastAPI,
    ) -> None:
        """POST should retain persisted bytes when DB commit raises RuntimeError."""
        _ = self
        payload = b"%PDF-1.7\npayload"

        app.dependency_overrides[session_module.get_db] = (
            _make_get_db_override_with_commit_error(RuntimeError("forced commit failure"))
        )
        try:
            with pytest.raises(RuntimeError, match="forced commit failure"):
                await async_client.post(
                    f"/v1/projects/{created_project['id']}/files",
                    files={"file": ("commit-fail.pdf", payload, "application/pdf")},
                )
        finally:
            app.dependency_overrides.pop(session_module.get_db, None)

        upload_root = Path(settings.upload_storage_root).resolve()
        staging_root = upload_root / ".staging"
        assert not staging_root.exists() or not any(staging_root.iterdir())

        originals_root = upload_root / "originals"
        persisted_paths = [path for path in originals_root.rglob("*") if path.is_file()]
        assert len(persisted_paths) == 1
        assert persisted_paths[0].read_bytes() == payload

    async def test_upload_file_commit_cancelled_error_retains_written_bytes_conservatively(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
        app: FastAPI,
    ) -> None:
        """POST should retain persisted bytes when DB commit raises CancelledError."""
        _ = self
        payload = b"%PDF-1.7\npayload"

        app.dependency_overrides[session_module.get_db] = (
            _make_get_db_override_with_commit_error(asyncio.CancelledError())
        )
        try:
            caught: BaseException | None = None
            try:
                await async_client.post(
                    f"/v1/projects/{created_project['id']}/files",
                    files={
                        "file": (
                            "commit-cancelled.pdf",
                            payload,
                            "application/pdf",
                        )
                    },
                )
            except BaseException as exc:
                caught = exc

            assert caught is not None
            assert isinstance(caught, asyncio.CancelledError) or (
                isinstance(caught, RuntimeError) and str(caught) == "No response returned."
            )
        finally:
            app.dependency_overrides.pop(session_module.get_db, None)

        upload_root = Path(settings.upload_storage_root).resolve()
        staging_root = upload_root / ".staging"
        assert not staging_root.exists() or not any(staging_root.iterdir())

        originals_root = upload_root / "originals"
        persisted_paths = [path for path in originals_root.rglob("*") if path.is_file()]
        assert len(persisted_paths) == 1
        assert persisted_paths[0].read_bytes() == payload
