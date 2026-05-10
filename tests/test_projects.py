"""Integration tests for Project CRUD endpoints."""

import base64
import json
import typing
import uuid
from typing import Any

import httpx
import pytest_asyncio
from sqlalchemy import select

import app.db.session as session_module
from app.models.project import Project
from tests.conftest import requires_database


@pytest_asyncio.fixture
async def created_project(
    async_client: httpx.AsyncClient,
    cleanup_projects: None,
) -> dict[str, Any]:
    """Create a project and return its data."""
    response = await async_client.post(
        "/v1/projects",
        json={"name": "Test Project", "description": "A test project"},
    )
    assert response.status_code == 201
    return typing.cast(dict[str, Any], response.json())


async def _mark_project_deleted(project_id: str) -> None:
    """Mark a project row as soft-deleted for read-path tests."""
    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None

    async with session_maker() as session:
        project = (
            await session.execute(select(Project).where(Project.id == uuid.UUID(project_id)))
        ).scalar_one()
        project.deleted_at = project.updated_at
        await session.commit()


@requires_database
class TestCreateProject:
    """Tests for POST /v1/projects endpoint."""

    async def test_create_project_success(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should create a project and return 201 with correct response shape.

        Success case: POST with valid data returns 201, response contains
        name, id, created_at, updated_at fields.
        """
        _ = self
        _ = cleanup_projects

        # Arrange
        project_data = {
            "name": "My Test Project",
            "description": "A project for testing",
            "default_unit_system": "metric",
            "default_currency": "USD",
        }

        # Act
        response = await async_client.post("/v1/projects", json=project_data)

        # Assert
        assert response.status_code == 201
        data = response.json()

        # Verify response shape
        assert "id" in data
        assert "name" in data
        assert "description" in data
        assert "default_unit_system" in data
        assert "default_currency" in data
        assert "created_at" in data
        assert "updated_at" in data

        # Verify values
        assert data["name"] == project_data["name"]
        assert data["description"] == project_data["description"]
        assert data["default_unit_system"] == project_data["default_unit_system"]
        assert data["default_currency"] == project_data["default_currency"]

        # Verify id is a valid UUID
        assert uuid.UUID(data["id"])

        # Verify timestamps are present and valid ISO format
        assert data["created_at"] is not None
        assert data["updated_at"] is not None

    async def test_create_project_minimal_data(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should create a project with only required fields.

        Success case: POST with only name returns 201, optional fields are null.
        """
        _ = self
        _ = cleanup_projects

        # Arrange
        project_data = {"name": "Minimal Project"}

        # Act
        response = await async_client.post("/v1/projects", json=project_data)

        # Assert
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == project_data["name"]
        assert data["description"] is None
        assert data["default_unit_system"] is None
        assert data["default_currency"] is None

    async def test_create_project_validation_error_empty_name(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should reject project with empty name.

        Failure case: POST with empty name returns 422 validation error.
        """
        _ = self
        _ = cleanup_projects

        # Arrange
        project_data = {"name": ""}

        # Act
        response = await async_client.post("/v1/projects", json=project_data)

        # Assert
        assert response.status_code == 422
        data = response.json()

        assert "error" in data
        assert data["error"]["code"] == "VALIDATION_ERROR"
        assert data["error"]["message"] == "Request validation failed"
        assert isinstance(data["error"]["details"], list)


@requires_database
class TestGetProject:
    """Tests for GET /v1/projects/{id} endpoint."""

    async def test_get_project_success(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Should return existing project with correct data.

        Success case: GET with valid ID returns 200 and project data.
        """
        _ = self

        # Arrange
        project_id = created_project["id"]

        # Act
        response = await async_client.get(f"/v1/projects/{project_id}")

        # Assert
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == project_id
        assert data["name"] == created_project["name"]
        assert data["description"] == created_project["description"]

    async def test_get_project_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return 404 for non-existent project.

        Failure case: GET with random UUID returns 404 with correct error shape.
        """
        _ = self
        _ = cleanup_projects

        # Arrange
        random_uuid = str(uuid.uuid4())

        # Act
        response = await async_client.get(f"/v1/projects/{random_uuid}")

        # Assert
        assert response.status_code == 404
        data = response.json()

        # Verify error response shape
        assert "error" in data
        assert data["error"]["code"] == "NOT_FOUND"
        assert "message" in data["error"]
        assert random_uuid in data["error"]["message"]
        assert data["error"]["details"] is None

    async def test_get_project_invalid_uuid(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return 422 for invalid UUID format.

        Failure case: GET with invalid UUID format returns 422 validation error.
        """
        _ = self
        _ = cleanup_projects

        # Act
        response = await async_client.get("/v1/projects/not-a-uuid")

        # Assert
        assert response.status_code == 422
        data = response.json()

        assert "error" in data
        assert data["error"]["code"] == "VALIDATION_ERROR"
        assert data["error"]["message"] == "Request validation failed"
        assert isinstance(data["error"]["details"], list)

    async def test_get_project_soft_deleted_returns_not_found(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Should hide soft-deleted projects from detail reads."""
        _ = self

        await _mark_project_deleted(created_project["id"])

        response = await async_client.get(f"/v1/projects/{created_project['id']}")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "NOT_FOUND"


@requires_database
class TestListProjects:
    """Tests for GET /v1/projects endpoint."""

    async def test_list_projects_empty(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return empty list when no projects exist.

        Success case: GET returns 200 with empty items array and null next_cursor.
        """
        _ = self
        _ = cleanup_projects

        # Act
        response = await async_client.get("/v1/projects")

        # Assert
        assert response.status_code == 200
        data = response.json()

        # Verify response shape
        assert "items" in data
        assert "next_cursor" in data
        assert data["items"] == []
        assert data["next_cursor"] is None

    async def test_list_projects_with_data(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Should return list with existing projects.

        Success case: GET returns 200 with projects in items array.
        """
        _ = self

        # Act
        response = await async_client.get("/v1/projects")

        # Assert
        assert response.status_code == 200
        data = response.json()

        assert len(data["items"]) >= 1
        assert data["next_cursor"] is None

        # Verify the created project is in the list
        project_ids = [item["id"] for item in data["items"]]
        assert created_project["id"] in project_ids

    async def test_list_projects_excludes_soft_deleted_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should exclude projects with deleted_at set from list results."""
        _ = self
        _ = cleanup_projects

        visible_response = await async_client.post(
            "/v1/projects",
            json={"name": "Visible Project"},
        )
        deleted_response = await async_client.post(
            "/v1/projects",
            json={"name": "Deleted Project"},
        )
        assert visible_response.status_code == 201
        assert deleted_response.status_code == 201

        await _mark_project_deleted(deleted_response.json()["id"])

        response = await async_client.get("/v1/projects")
        assert response.status_code == 200

        project_ids = {item["id"] for item in response.json()["items"]}
        assert visible_response.json()["id"] in project_ids
        assert deleted_response.json()["id"] not in project_ids

    async def test_list_projects_pagination(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should paginate results correctly.

        Success case: Creating > limit projects and requesting with small limit
        returns next_cursor, and fetching next page returns different items.
        """
        _ = self
        _ = cleanup_projects

        # Arrange: Create 5 projects
        created_ids: list[str] = []
        for i in range(5):
            response = await async_client.post(
                "/v1/projects",
                json={"name": f"Project {i}"},
            )
            assert response.status_code == 201
            created_ids.append(response.json()["id"])

        # Act: Request first page with limit=2
        response = await async_client.get("/v1/projects?limit=2")

        # Assert first page
        assert response.status_code == 200
        data = response.json()

        assert len(data["items"]) == 2
        assert data["next_cursor"] is not None

        first_page_ids = [item["id"] for item in data["items"]]

        # Act: Request second page
        response = await async_client.get(f"/v1/projects?limit=2&cursor={data['next_cursor']}")

        # Assert second page
        assert response.status_code == 200
        data = response.json()

        assert len(data["items"]) == 2
        assert data["next_cursor"] is not None

        second_page_ids = [item["id"] for item in data["items"]]

        # Verify pages have different items
        assert not set(first_page_ids) & set(second_page_ids)

        # Act: Request third page
        response = await async_client.get(f"/v1/projects?limit=2&cursor={data['next_cursor']}")

        # Assert third page (should have 1 item, no next cursor)
        assert response.status_code == 200
        data = response.json()

        assert len(data["items"]) == 1
        assert data["next_cursor"] is None

        third_page_ids = [item["id"] for item in data["items"]]

        # Verify all pages have different items
        assert not set(first_page_ids) & set(third_page_ids)
        assert not set(second_page_ids) & set(third_page_ids)

        # Verify all created projects are accounted for
        all_ids = first_page_ids + second_page_ids + third_page_ids
        assert set(all_ids) == set(created_ids)

    async def test_list_projects_invalid_cursor(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return 400 for invalid cursor.

        Failure case: GET with invalid cursor returns 400 with error details.
        """
        _ = self
        _ = cleanup_projects

        # Act
        response = await async_client.get("/v1/projects?cursor=invalid-cursor")

        # Assert
        assert response.status_code == 400
        data = response.json()

        assert "error" in data
        assert data["error"]["code"] == "INVALID_CURSOR"
        assert data["error"]["message"] == "Invalid cursor format"
        assert data["error"]["details"] is None

    async def test_list_projects_malformed_cursors(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return 400 envelope for malformed cursors."""
        _ = self
        _ = cleanup_projects

        invalid_utf8_cursor = base64.urlsafe_b64encode(b"\xff").decode().rstrip("=")
        invalid_json_cursor = base64.urlsafe_b64encode(b"not-json").decode().rstrip("=")
        missing_keys_cursor = base64.urlsafe_b64encode(
            json.dumps({"created_at": "2026-01-01T00:00:00"}).encode()
        ).decode().rstrip("=")
        invalid_datetime_cursor = base64.urlsafe_b64encode(
            json.dumps({"created_at": "not-a-datetime", "id": str(uuid.uuid4())}).encode()
        ).decode().rstrip("=")
        invalid_uuid_cursor = base64.urlsafe_b64encode(
            json.dumps({"created_at": "2026-01-01T00:00:00", "id": "not-a-uuid"}).encode()
        ).decode().rstrip("=")
        non_object_array_cursor = (
            base64.urlsafe_b64encode(json.dumps([]).encode()).decode().rstrip("=")
        )
        non_object_null_cursor = (
            base64.urlsafe_b64encode(json.dumps(None).encode()).decode().rstrip("=")
        )
        non_object_string_cursor = base64.urlsafe_b64encode(
            json.dumps("x").encode()
        ).decode().rstrip("=")

        malformed_cursors = [
            "%%%",  # invalid base64
            invalid_utf8_cursor,
            invalid_json_cursor,
            missing_keys_cursor,
            invalid_datetime_cursor,
            invalid_uuid_cursor,
            non_object_array_cursor,
            non_object_null_cursor,
            non_object_string_cursor,
        ]

        for malformed_cursor in malformed_cursors:
            response = await async_client.get(f"/v1/projects?cursor={malformed_cursor}")
            assert response.status_code == 400
            data = response.json()
            assert "error" in data
            assert data["error"]["code"] == "INVALID_CURSOR"
            assert data["error"]["message"] == "Invalid cursor format"
            assert data["error"]["details"] is None


@requires_database
class TestUpdateProject:
    """Tests for PATCH /v1/projects/{id} endpoint."""

    async def test_update_project_success(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Should update project and return updated data.

        Success case: PATCH with valid data returns 200, then GET verifies changes.
        """
        _ = self

        # Arrange
        project_id = created_project["id"]
        update_data = {
            "name": "Updated Project Name",
            "description": "Updated description",
        }

        # Act: Update the project
        response = await async_client.patch(f"/v1/projects/{project_id}", json=update_data)

        # Assert update response
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == project_id
        assert data["name"] == update_data["name"]
        assert data["description"] == update_data["description"]

        # Act: Verify with GET
        response = await async_client.get(f"/v1/projects/{project_id}")

        # Assert persisted changes
        assert response.status_code == 200
        data = response.json()

        assert data["name"] == update_data["name"]
        assert data["description"] == update_data["description"]

    async def test_update_project_partial(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Should update only provided fields.

        Success case: PATCH with partial data updates only specified fields.
        """
        _ = self

        # Arrange
        project_id = created_project["id"]
        original_description = created_project["description"]
        update_data = {"name": "Only Name Updated"}

        # Act
        response = await async_client.patch(f"/v1/projects/{project_id}", json=update_data)

        # Assert
        assert response.status_code == 200
        data = response.json()

        assert data["name"] == update_data["name"]
        assert data["description"] == original_description  # Unchanged

    async def test_update_project_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return 404 for non-existent project.

        Failure case: PATCH with random UUID returns 404 with correct error shape.
        """
        _ = self
        _ = cleanup_projects

        # Arrange
        random_uuid = str(uuid.uuid4())
        update_data = {"name": "Updated Name"}

        # Act
        response = await async_client.patch(f"/v1/projects/{random_uuid}", json=update_data)

        # Assert
        assert response.status_code == 404
        data = response.json()

        assert "error" in data
        assert data["error"]["code"] == "NOT_FOUND"


@requires_database
class TestDeleteProject:
    """Tests for DELETE /v1/projects/{id} endpoint."""

    async def test_delete_project_success(
        self,
        async_client: httpx.AsyncClient,
        created_project: dict[str, Any],
    ) -> None:
        """Should delete project and return 204.

        Success case: DELETE returns 204, then GET returns 404.
        """
        _ = self

        # Arrange
        project_id = created_project["id"]

        # Act: Delete the project
        response = await async_client.delete(f"/v1/projects/{project_id}")

        # Assert delete response
        assert response.status_code == 204
        assert response.content == b""

        # Act: Verify deletion with GET
        response = await async_client.get(f"/v1/projects/{project_id}")

        # Assert project no longer exists
        assert response.status_code == 404

        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        async with session_maker() as session:
            project = (
                await session.execute(
                    select(Project).where(Project.id == uuid.UUID(created_project["id"]))
                )
            ).scalar_one_or_none()

        assert project is not None
        assert project.deleted_at is not None

    async def test_delete_project_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Should return 404 for non-existent project.

        Failure case: DELETE with random UUID returns 404 with correct error shape.
        """
        _ = self
        _ = cleanup_projects

        # Arrange
        random_uuid = str(uuid.uuid4())

        # Act
        response = await async_client.delete(f"/v1/projects/{random_uuid}")

        # Assert
        assert response.status_code == 404
        data = response.json()

        assert "error" in data
        assert data["error"]["code"] == "NOT_FOUND"
