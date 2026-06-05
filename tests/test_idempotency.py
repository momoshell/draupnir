"""Integration tests for endpoint-level Idempotency-Key behavior."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, Callable, Mapping
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import app.api.idempotency as idempotency_api
from app.api.idempotency import (
    IdempotencyReplay,
    IdempotencyReservation,
    IdempotentMutationKnownError,
    IdempotentMutationOps,
    IdempotentMutationSuccess,
    build_idempotency_fingerprint,
    claim_idempotency_response,
    complete_idempotency_response,
    get_idempotency_key,
    hash_idempotency_key,
    mark_idempotency_completed,
    replay_idempotency,
    replay_idempotency_response,
    run_idempotent_mutation,
)
from app.api.v1 import files as files_api
from app.api.v1 import jobs as jobs_api
from app.core.config import settings
from app.core.errors import ErrorCode
from app.db import session as session_module
from app.jobs import worker as worker_module
from app.jobs.worker import process_ingest_job
from app.models.drawing_revision import DrawingRevision
from app.models.extraction_profile import ExtractionProfile
from app.models.file import File as FileModel
from app.models.idempotency_key import IdempotencyKey, IdempotencyStatus
from app.models.job import Job
from app.models.project import Project
from tests.conftest import requires_database
from tests.test_jobs import _build_fake_ingest_payload


def _headers(idempotency_key: str | None) -> dict[str, str]:
    """Build optional request headers for idempotent requests."""

    if idempotency_key is None:
        return {}
    return {"Idempotency-Key": idempotency_key}


def test_build_idempotency_fingerprint_is_order_independent() -> None:
    """Canonical JSON fingerprinting should not depend on input key order."""

    first = build_idempotency_fingerprint("projects.create", {"name": "A", "description": "B"})
    second = build_idempotency_fingerprint(
        "projects.create",
        {"description": "B", "name": "A"},
    )

    assert first == second


def test_hash_idempotency_key_uses_configured_hmac_secret() -> None:
    """Raw keys should hash to a stable HMAC digest instead of being stored directly."""

    first = hash_idempotency_key("raw-key-1")
    second = hash_idempotency_key("raw-key-1")

    assert first == second
    assert first != "raw-key-1"
    assert len(first) == 64


@pytest.mark.asyncio
async def test_replay_idempotency_returns_retry_after_for_in_progress_conflict() -> None:
    """In-progress idempotency conflicts should short-circuit with Retry-After."""

    key = "unit-in-progress-1"
    payload = {"name": "Blocked Project"}
    fingerprint = build_idempotency_fingerprint("projects.create", payload)
    record = IdempotencyKey(
        key_hash=hash_idempotency_key(key),
        request_fingerprint=fingerprint,
        request_method="POST",
        request_path="/projects",
        status=IdempotencyStatus.IN_PROGRESS.value,
    )

    class _FakeResult:
        def scalar_one_or_none(self) -> IdempotencyKey:
            return record

    class _FakeAsyncSession:
        async def execute(self, _: Any) -> _FakeResult:
            return _FakeResult()

    replay = await replay_idempotency(
        cast(Any, _FakeAsyncSession()),
        key=key,
        fingerprint=fingerprint,
    )

    assert replay is not None
    assert replay.response.status_code == 409
    assert replay.response.headers["Retry-After"] == "1"
    assert json.loads(bytes(replay.response.body)) == {
        "error": {
            "code": "IDEMPOTENCY_CONFLICT",
            "message": "A request with this Idempotency-Key is already in progress.",
            "details": {"reason": "in_progress"},
        }
    }


@pytest.mark.asyncio
async def test_get_idempotency_key_rejects_invalid_rfc9110_token() -> None:
    """Header validation should reject keys outside the RFC 9110 token grammar."""

    with pytest.raises(HTTPException) as exc_info:
        await get_idempotency_key("contains space")

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_replay_idempotency_response_returns_none_without_key() -> None:
    """Optional replay helper should no-op when no key is supplied."""

    result = await replay_idempotency_response(
        cast(Any, object()),
        key=None,
        fingerprint="fingerprint-1",
    )

    assert result is None


@pytest.mark.asyncio
async def test_replay_idempotency_response_unwraps_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Optional replay helper should unwrap short-circuit replay responses."""

    replay_response = Response(status_code=204)

    async def _fake_replay_idempotency(_: Any, *, key: str, fingerprint: str) -> IdempotencyReplay:
        assert key == "replay-key-1"
        assert fingerprint == "fingerprint-2"
        return IdempotencyReplay(response=replay_response)

    monkeypatch.setattr("app.api.idempotency.replay_idempotency", _fake_replay_idempotency)

    result = await replay_idempotency_response(
        cast(Any, object()),
        key="replay-key-1",
        fingerprint="fingerprint-2",
    )

    assert result is replay_response


@pytest.mark.asyncio
async def test_claim_idempotency_response_returns_none_without_key() -> None:
    """Optional claim helper should no-op when no key is supplied."""

    result = await claim_idempotency_response(
        cast(Any, object()),
        key=None,
        fingerprint="fingerprint-3",
        method="POST",
        path="/v1/projects",
    )

    assert result is None


@pytest.mark.asyncio
async def test_claim_idempotency_response_unwraps_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Optional claim helper should unwrap replay conflicts into a response."""

    replay_response = JSONResponse(status_code=409, content={"error": {"code": "conflict"}})

    async def _fake_claim_idempotency(
        _: Any,
        *,
        key: str,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReplay:
        assert (key, fingerprint, method, path) == (
            "claim-key-1",
            "fingerprint-4",
            "PATCH",
            "/v1/projects/project-1",
        )
        return IdempotencyReplay(response=replay_response)

    monkeypatch.setattr("app.api.idempotency.claim_idempotency", _fake_claim_idempotency)

    result = await claim_idempotency_response(
        cast(Any, object()),
        key="claim-key-1",
        fingerprint="fingerprint-4",
        method="PATCH",
        path="/v1/projects/project-1",
    )

    assert result is replay_response


class _FakeScalarResult:
    """Minimal async execute result wrapper for idempotency unit tests."""

    def __init__(self, record: IdempotencyKey) -> None:
        self._record = record

    def scalar_one(self) -> IdempotencyKey:
        return self._record


class _FakeIdempotencySession:
    """Small session double to verify helper commit behavior."""

    def __init__(self, record: IdempotencyKey) -> None:
        self._record = record
        self.commit_calls = 0

    async def execute(self, _: Any) -> _FakeScalarResult:
        return _FakeScalarResult(self._record)

    async def commit(self) -> None:
        self.commit_calls += 1


class _CommitTrackingSession:
    """Minimal session double that records commit ordering for orchestration tests."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.commit_calls = 0
        self.rollback_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1
        self._events.append("commit")

    async def rollback(self) -> None:
        self.rollback_calls += 1
        self._events.append("rollback")


async def _upload_lineage_counts(project_id: str) -> tuple[int, int, int]:
    """Return persisted file/job/extraction-profile counts for a project."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    project_uuid = uuid.UUID(project_id)

    async with session_maker() as session:
        file_count = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(FileModel)
                    .where(FileModel.project_id == project_uuid)
                )
            ).scalar_one()
        )
        job_count = int(
            (
                await session.execute(
                    select(func.count()).select_from(Job).where(Job.project_id == project_uuid)
                )
            ).scalar_one()
        )
        extraction_profile_count = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(ExtractionProfile)
                    .where(ExtractionProfile.project_id == project_uuid)
                )
            ).scalar_one()
        )

    return file_count, job_count, extraction_profile_count


@pytest.mark.asyncio
async def test_run_idempotent_mutation_supports_no_key_execution() -> None:
    """Shared orchestration should no-op idempotency helpers when the key is absent."""

    events: list[str] = []
    session = _CommitTrackingSession(events)

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert key is None
        assert fingerprint == "fingerprint-7"
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert key is None
        assert (fingerprint, method, path) == ("fingerprint-7", "POST", "/v1/projects")
        return None

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = db
        events.append("complete")
        assert reservation is None
        assert status_code == 201
        assert response_body == {"id": "project-1"}
        return JSONResponse(status_code=status_code, content=response_body)

    async def _preclaim() -> Response | None:
        events.append("preclaim")
        return None

    async def _mutate() -> IdempotentMutationSuccess[str]:
        events.append("mutate")
        return IdempotentMutationSuccess(value="project-1", status_code=201)

    def _serialize(value: str) -> dict[str, Any]:
        events.append("serialize")
        return {"id": value}

    response = await run_idempotent_mutation(
        cast(Any, session),
        key=None,
        fingerprint="fingerprint-7",
        method="POST",
        path="/v1/projects",
        preclaim=_preclaim,
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response.status_code == 201
    assert json.loads(bytes(response.body)) == {"id": "project-1"}
    assert session.commit_calls == 1
    assert events == ["replay", "preclaim", "claim", "mutate", "serialize", "complete", "commit"]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_short_circuits_replay_response() -> None:
    """Replay responses should return before preclaim, claim, mutation, or commit."""

    events: list[str] = []
    session = _CommitTrackingSession(events)
    replay_response = Response(status_code=204)

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("replay-key-2", "fingerprint-8")
        return replay_response

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = (db, key, fingerprint, method, path)
        raise AssertionError("claim should not run after replay short-circuit")

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = (db, reservation, status_code, response_body)
        raise AssertionError("complete should not run after replay short-circuit")

    async def _mutate() -> IdempotentMutationSuccess[dict[str, Any]]:
        raise AssertionError("mutate should not run after replay short-circuit")

    def _serialize(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("serialize should not run after replay short-circuit")

    response = await run_idempotent_mutation(
        cast(Any, session),
        key="replay-key-2",
        fingerprint="fingerprint-8",
        method="POST",
        path="/v1/projects",
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response is replay_response
    assert session.commit_calls == 0
    assert events == ["replay"]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_short_circuits_claim_response() -> None:
    """Claim responses should return before reload, mutation, completion, or commit."""

    events: list[str] = []
    session = _CommitTrackingSession(events)
    claim_response = JSONResponse(status_code=409, content={"error": {"code": "conflict"}})

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("claim-key-2", "fingerprint-9")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            "claim-key-2",
            "fingerprint-9",
            "PATCH",
            "/v1/projects/project-1",
        )
        return claim_response

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = (db, reservation, status_code, response_body)
        raise AssertionError("complete should not run after claim short-circuit")

    async def _preclaim() -> Response | None:
        events.append("preclaim")
        return None

    async def _reload_after_claim() -> None:
        raise AssertionError("reload should not run after claim short-circuit")

    async def _mutate() -> IdempotentMutationSuccess[dict[str, Any]]:
        raise AssertionError("mutate should not run after claim short-circuit")

    def _serialize(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("serialize should not run after claim short-circuit")

    response = await run_idempotent_mutation(
        cast(Any, session),
        key="claim-key-2",
        fingerprint="fingerprint-9",
        method="PATCH",
        path="/v1/projects/project-1",
        preclaim=_preclaim,
        reload_after_claim=_reload_after_claim,
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response is claim_response
    assert session.commit_calls == 0
    assert events == ["replay", "preclaim", "claim"]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_reloads_serializes_and_runs_after_commit() -> None:
    """Successful orchestration should reload after claim and run hooks after commit."""

    events: list[str] = []
    session = _CommitTrackingSession(events)
    expected_reservation = IdempotencyReservation(record_id=uuid.uuid4())

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("success-key-1", "fingerprint-10")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            "success-key-1",
            "fingerprint-10",
            "POST",
            "/v1/projects",
        )
        return expected_reservation

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = db
        events.append("complete")
        assert reservation == expected_reservation
        assert status_code == 201
        assert response_body == {"id": "project-2"}
        return JSONResponse(status_code=status_code, content=response_body)

    async def _preclaim() -> Response | None:
        events.append("preclaim")
        return None

    async def _reload_after_claim() -> None:
        events.append("reload")

    async def _after_commit() -> None:
        events.append("after_commit")

    async def _mutate() -> IdempotentMutationSuccess[str]:
        events.append("mutate")
        return IdempotentMutationSuccess(
            value="project-2",
            status_code=201,
            after_commit=_after_commit,
        )

    def _serialize(value: str) -> dict[str, Any]:
        events.append("serialize")
        return {"id": value}

    response = await run_idempotent_mutation(
        cast(Any, session),
        key="success-key-1",
        fingerprint="fingerprint-10",
        method="POST",
        path="/v1/projects",
        preclaim=_preclaim,
        reload_after_claim=_reload_after_claim,
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response.status_code == 201
    assert json.loads(bytes(response.body)) == {"id": "project-2"}
    assert session.commit_calls == 1
    assert events == [
        "replay",
        "preclaim",
        "claim",
        "reload",
        "mutate",
        "serialize",
        "complete",
        "commit",
        "after_commit",
    ]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_snapshots_replay_safe_reload_http_exception() -> None:
    """Replay-safe HTTP errors from reload_after_claim should rollback, snapshot, and commit."""

    events: list[str] = []
    session = _CommitTrackingSession(events)
    expected_reservation = IdempotencyReservation(record_id=uuid.uuid4())
    response_body = {
        "error": {
            "code": "REVISION_CONFLICT",
            "message": "Base revision became stale.",
            "details": None,
        }
    }

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("reload-http-key-1", "fingerprint-10b")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            "reload-http-key-1",
            "fingerprint-10b",
            "POST",
            "/v1/files/file-1/reprocess",
        )
        return expected_reservation

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = db
        events.append("complete")
        assert reservation == expected_reservation
        assert status_code == 409
        assert response_body == {
            "error": {
                "code": "REVISION_CONFLICT",
                "message": "Base revision became stale.",
                "details": None,
            }
        }
        return JSONResponse(status_code=status_code, content=response_body)

    async def _reload_after_claim() -> None:
        events.append("reload")
        raise HTTPException(status_code=409, detail=response_body)

    async def _mutate() -> IdempotentMutationSuccess[str]:
        raise AssertionError("mutate should not run after replay-safe reload error")

    def _serialize(_: str) -> dict[str, Any]:
        raise AssertionError("serialize should not run after replay-safe reload error")

    response = await run_idempotent_mutation(
        cast(Any, session),
        key="reload-http-key-1",
        fingerprint="fingerprint-10b",
        method="POST",
        path="/v1/files/file-1/reprocess",
        reload_after_claim=_reload_after_claim,
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response.status_code == 409
    assert json.loads(bytes(response.body)) == response_body
    assert session.commit_calls == 1
    assert session.rollback_calls == 1
    assert events == ["replay", "claim", "reload", "rollback", "complete", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("details", "case_id"),
    [
        pytest.param(None, "none-details", id="none-details"),
        pytest.param(["field", {"reason": "invalid"}], "list-details", id="list-details"),
        pytest.param("payload_invalid", "scalar-details", id="scalar-details"),
    ],
)
async def test_run_idempotent_mutation_snapshots_replay_safe_mutate_http_exception(
    details: Any,
    case_id: str,
) -> None:
    """Replay-safe HTTP errors from mutate should rollback, snapshot, and commit."""

    events: list[str] = []
    session = _CommitTrackingSession(events)
    expected_reservation = IdempotencyReservation(record_id=uuid.uuid4())
    response_body = {
        "error": {
            "code": "INPUT_INVALID",
            "message": "Payload is invalid.",
            "details": details,
        }
    }

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == (f"mutate-http-key-1-{case_id}", f"fingerprint-10c-{case_id}")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            f"mutate-http-key-1-{case_id}",
            f"fingerprint-10c-{case_id}",
            "PATCH",
            "/v1/projects/project-9",
        )
        return expected_reservation

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = db
        events.append("complete")
        assert reservation == expected_reservation
        assert status_code == 400
        assert response_body == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "Payload is invalid.",
                "details": details,
            }
        }
        return JSONResponse(status_code=status_code, content=response_body)

    async def _mutate() -> IdempotentMutationSuccess[str]:
        events.append("mutate")
        raise HTTPException(status_code=400, detail=response_body)

    def _serialize(_: str) -> dict[str, Any]:
        raise AssertionError("serialize should not run after replay-safe mutate error")

    response = await run_idempotent_mutation(
        cast(Any, session),
        key=f"mutate-http-key-1-{case_id}",
        fingerprint=f"fingerprint-10c-{case_id}",
        method="PATCH",
        path="/v1/projects/project-9",
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response.status_code == 400
    assert json.loads(bytes(response.body)) == response_body
    assert session.commit_calls == 1
    assert session.rollback_calls == 1
    assert events == ["replay", "claim", "mutate", "rollback", "complete", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "detail", "headers"),
    [
        pytest.param(
            400,
            {"detail": "not-envelope"},
            None,
            id="non-envelope-detail",
        ),
        pytest.param(
            400,
            {
                "error": {
                    "code": "INPUT_INVALID",
                    "message": "Payload is invalid.",
                    "details": {"labels"},
                }
            },
            None,
            id="non-json-compatible-details",
        ),
        pytest.param(
            400,
            {
                "error": {
                    "code": "NOT_A_REAL_ERROR_CODE",
                    "message": "Payload is invalid.",
                    "details": None,
                }
            },
            None,
            id="invalid-error-code",
        ),
        pytest.param(
            409,
            {
                "error": {
                    "code": "REVISION_CONFLICT",
                    "message": "Base revision became stale.",
                    "details": None,
                }
            },
            {"Retry-After": "1"},
            id="custom-headers",
        ),
        pytest.param(
            500,
            {
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Unexpected failure.",
                    "details": None,
                }
            },
            None,
            id="server-error",
        ),
    ],
)
async def test_run_idempotent_mutation_does_not_snapshot_unsafe_http_exceptions(
    status_code: int,
    detail: Any,
    headers: dict[str, str] | None,
) -> None:
    """Unsafe HTTP exceptions should propagate without rollback, completion, or commit."""

    events: list[str] = []
    session = _CommitTrackingSession(events)

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = (db, key, fingerprint)
        events.append("replay")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = (db, key, fingerprint, method, path)
        events.append("claim")
        return IdempotencyReservation(record_id=uuid.uuid4())

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = (db, reservation, status_code, response_body)
        raise AssertionError("complete should not run for unsafe HTTP exceptions")

    async def _mutate() -> IdempotentMutationSuccess[str]:
        events.append("mutate")
        raise HTTPException(status_code=status_code, detail=detail, headers=headers)

    def _serialize(_: str) -> dict[str, Any]:
        raise AssertionError("serialize should not run for unsafe HTTP exceptions")

    with pytest.raises(HTTPException) as exc_info:
        await run_idempotent_mutation(
            cast(Any, session),
            key="unsafe-http-key-1",
            fingerprint="fingerprint-10d",
            method="POST",
            path="/v1/projects",
            mutate=_mutate,
            serialize_result=_serialize,
            ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
        )

    assert exc_info.value.status_code == status_code
    assert exc_info.value.detail == detail
    assert exc_info.value.headers == headers
    assert session.commit_calls == 0
    assert session.rollback_calls == 0
    assert events == ["replay", "claim", "mutate"]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_completes_known_error_snapshot() -> None:
    """Known local errors should snapshot and commit without calling the serializer."""

    events: list[str] = []
    session = _CommitTrackingSession(events)
    expected_reservation = IdempotencyReservation(record_id=uuid.uuid4())
    response_body = {
        "error": {
            "code": "STORAGE_FAILED",
            "message": "Failed to persist uploaded file.",
            "details": None,
        }
    }

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("known-error-key-1", "fingerprint-11")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            "known-error-key-1",
            "fingerprint-11",
            "POST",
            "/v1/projects/project-1/files",
        )
        return expected_reservation

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = db
        events.append("complete")
        assert reservation == expected_reservation
        assert status_code == 500
        assert response_body == {
            "error": {
                "code": "STORAGE_FAILED",
                "message": "Failed to persist uploaded file.",
                "details": None,
            }
        }
        return JSONResponse(status_code=status_code, content=response_body)

    async def _mutate() -> IdempotentMutationKnownError:
        events.append("mutate")
        return IdempotentMutationKnownError(status_code=500, response_body=response_body)

    def _serialize(_: str) -> dict[str, Any]:
        raise AssertionError("serialize should not run for known-error snapshots")

    response = await run_idempotent_mutation(
        cast(Any, session),
        key="known-error-key-1",
        fingerprint="fingerprint-11",
        method="POST",
        path="/v1/projects/project-1/files",
        mutate=_mutate,
        serialize_result=_serialize,
        ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
    )

    assert response.status_code == 500
    assert json.loads(bytes(response.body)) == response_body
    assert session.commit_calls == 1
    assert events == ["replay", "claim", "mutate", "complete", "commit"]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_propagates_arbitrary_exceptions() -> None:
    """Unexpected exceptions should escape without completion snapshots or commits."""

    events: list[str] = []
    session = _CommitTrackingSession(events)

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("exception-key-1", "fingerprint-12")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            "exception-key-1",
            "fingerprint-12",
            "DELETE",
            "/v1/projects/project-2",
        )
        return IdempotencyReservation(record_id=uuid.uuid4())

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = (db, reservation, status_code, response_body)
        raise AssertionError("complete should not run for arbitrary exceptions")

    async def _mutate() -> IdempotentMutationSuccess[str]:
        events.append("mutate")
        raise RuntimeError("boom")

    def _serialize(_: str) -> dict[str, Any]:
        raise AssertionError("serialize should not run for arbitrary exceptions")

    with pytest.raises(RuntimeError, match="boom"):
        await run_idempotent_mutation(
            cast(Any, session),
            key="exception-key-1",
            fingerprint="fingerprint-12",
            method="DELETE",
            path="/v1/projects/project-2",
            mutate=_mutate,
            serialize_result=_serialize,
            ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
        )

    assert session.commit_calls == 0
    assert events == ["replay", "claim", "mutate"]


@pytest.mark.asyncio
async def test_run_idempotent_mutation_runs_pre_commit_failure_cleanup_before_commit_starts() -> (
    None
):
    """Pre-commit failures should cleanup before any final commit begins."""

    events: list[str] = []
    session = _CommitTrackingSession(events)

    async def _replay(db: AsyncSession, *, key: str | None, fingerprint: str) -> Response | None:
        _ = db
        events.append("replay")
        assert (key, fingerprint) == ("cleanup-key-1", "fingerprint-13")
        return None

    async def _claim(
        db: AsyncSession,
        *,
        key: str | None,
        fingerprint: str,
        method: str,
        path: str,
    ) -> IdempotencyReservation | Response | None:
        _ = db
        events.append("claim")
        assert (key, fingerprint, method, path) == (
            "cleanup-key-1",
            "fingerprint-13",
            "POST",
            "/v1/projects/project-3/files",
        )
        return IdempotencyReservation(record_id=uuid.uuid4())

    async def _complete(
        db: AsyncSession,
        reservation: IdempotencyReservation | None,
        *,
        status_code: int,
        response_body: dict[str, Any] | None,
    ) -> Response:
        _ = (db, reservation, status_code, response_body)
        events.append("complete")
        raise RuntimeError("complete failed")

    async def _mutate() -> IdempotentMutationSuccess[str]:
        events.append("mutate")
        return IdempotentMutationSuccess(value="project-3", status_code=201)

    def _serialize(value: str) -> dict[str, Any]:
        events.append("serialize")
        return {"id": value}

    async def _cleanup() -> None:
        events.append("cleanup")

    with pytest.raises(RuntimeError, match="complete failed"):
        await run_idempotent_mutation(
            cast(Any, session),
            key="cleanup-key-1",
            fingerprint="fingerprint-13",
            method="POST",
            path="/v1/projects/project-3/files",
            mutate=_mutate,
            serialize_result=_serialize,
            ops=IdempotentMutationOps(replay=_replay, claim=_claim, complete=_complete),
            pre_commit_failure_cleanup=_cleanup,
        )

    assert session.commit_calls == 0
    assert events == ["replay", "claim", "mutate", "serialize", "complete", "cleanup"]


@pytest.mark.asyncio
async def test_complete_idempotency_response_marks_snapshot_and_returns_json() -> None:
    """Completion helper should snapshot success bodies without committing the session."""

    record = IdempotencyKey(
        id=uuid.uuid4(),
        key_hash=hash_idempotency_key("complete-json-1"),
        request_fingerprint="fingerprint-5",
        request_method="POST",
        request_path="/v1/projects",
        status=IdempotencyStatus.IN_PROGRESS.value,
    )
    session = _FakeIdempotencySession(record)
    reservation = IdempotencyReservation(record_id=record.id)
    response_body = {"id": "project-1", "name": "Replay Project"}

    response = await complete_idempotency_response(
        cast(Any, session),
        reservation,
        status_code=201,
        response_body=response_body,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 201
    assert json.loads(bytes(response.body)) == response_body
    assert record.status == IdempotencyStatus.COMPLETED.value
    assert record.response_status_code == 201
    assert record.response_body_json == response_body
    assert record.completed_at is not None
    assert session.commit_calls == 0


@pytest.mark.asyncio
async def test_complete_idempotency_response_returns_204_without_commit() -> None:
    """Completion helper should build empty replay responses without committing."""

    record = IdempotencyKey(
        id=uuid.uuid4(),
        key_hash=hash_idempotency_key("complete-empty-1"),
        request_fingerprint="fingerprint-6",
        request_method="DELETE",
        request_path="/v1/projects/project-1",
        status=IdempotencyStatus.IN_PROGRESS.value,
    )
    session = _FakeIdempotencySession(record)
    reservation = IdempotencyReservation(record_id=record.id)

    response = await complete_idempotency_response(
        cast(Any, session),
        reservation,
        status_code=204,
        response_body=None,
    )

    assert isinstance(response, Response)
    assert not isinstance(response, JSONResponse)
    assert response.status_code == 204
    assert response.body == b""
    assert record.status == IdempotencyStatus.COMPLETED.value
    assert record.response_status_code == 204
    assert record.response_body_json is None
    assert record.completed_at is not None
    assert session.commit_calls == 0


@pytest.fixture(autouse=True)
def _set_idempotency_hash_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use a deterministic test secret for idempotency key hashing."""

    monkeypatch.setattr(settings, "idempotency_key_hash_secret", "test-idempotency-secret")


@pytest.fixture(autouse=True)
def _fake_ingestion_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch worker ingestion with a deterministic fake runner payload."""

    async def _fake_run_ingestion(request: Any) -> Any:
        return _build_fake_ingest_payload(request)

    monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)


async def _create_project(
    async_client: httpx.AsyncClient,
    *,
    payload: Mapping[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> httpx.Response:
    """Create a project with optional idempotency headers."""

    return await async_client.post(
        "/v1/projects",
        json=dict(payload or {"name": "Idempotency Test Project"}),
        headers=_headers(idempotency_key),
    )


async def _upload_pdf(
    async_client: httpx.AsyncClient,
    *,
    project_id: str,
    filename: str = "plan.pdf",
    content: bytes = b"%PDF-1.7\nidempotency\n",
    idempotency_key: str | None = None,
) -> httpx.Response:
    """Upload a supported PDF file with optional idempotency headers."""

    return await async_client.post(
        f"/v1/projects/{project_id}/files",
        files={"file": (filename, content, "application/pdf")},
        headers=_headers(idempotency_key),
    )


def _get_async_client_app(async_client: httpx.AsyncClient) -> Any:
    """Return the FastAPI app behind the shared async test client."""

    app = getattr(async_client, "app", None)
    if app is not None:
        return app
    return cast(Any, async_client)._transport.app


def _make_get_db_override_with_rollback_error(
    rollback_error: BaseException,
) -> Callable[[], AsyncGenerator[Any, None]]:
    """Create a request-scoped get_db override with an instance-level rollback failure."""

    async def _override_get_db() -> AsyncGenerator[Any, None]:
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        session = session_maker()

        async def _fail_rollback() -> None:
            raise rollback_error

        cast(Any, session).rollback = _fail_rollback

        try:
            yield session
        finally:
            await session.close()

    return _override_get_db


async def _project_count() -> int:
    """Return the total persisted project row count."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return int((await session.execute(select(func.count()).select_from(Project))).scalar_one())


async def _job_count_for_file(file_id: str) -> int:
    """Return the number of persisted jobs for a file."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        statement = select(func.count()).select_from(Job).where(Job.file_id == uuid.UUID(file_id))
        return int((await session.execute(statement)).scalar_one())


async def _get_job_for_file(file_id: str) -> Job:
    """Return the persisted job for a file upload."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return (
            await session.execute(select(Job).where(Job.file_id == uuid.UUID(file_id)))
        ).scalar_one()


async def _get_job(job_id: str | uuid.UUID) -> Job:
    """Return the persisted job by identifier."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        job = await session.get(Job, uuid.UUID(str(job_id)))

    assert job is not None
    return job


async def _get_file_revisions(file_id: str) -> list[DrawingRevision]:
    """Return drawing revisions for a file in revision order."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(DrawingRevision)
                    .where(DrawingRevision.source_file_id == uuid.UUID(file_id))
                    .order_by(DrawingRevision.revision_sequence, DrawingRevision.id)
                )
            ).scalars()
        )


async def _finalize_initial_revision(file_id: str) -> Job:
    """Process the initial ingest job so reprocess has a finalized base."""

    initial_job = await _get_job_for_file(file_id)
    await process_ingest_job(initial_job.id)
    return await _get_job(initial_job.id)


async def _update_job_status(job_id: str, *, status_value: str) -> None:
    """Mutate a persisted job status for replay assertions."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        assert job is not None
        job.status = status_value
        await session.commit()


async def _mark_job_failed(
    job_id: str,
    *,
    error_code: str | None = None,
    error_message: str = "failed",
) -> None:
    """Place a persisted job into the failed state for retry tests."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        assert job is not None
        job.status = "failed"
        job.error_code = error_code
        job.error_message = error_message
        await session.commit()


async def _get_idempotency_record(raw_key: str) -> IdempotencyKey:
    """Return a persisted idempotency record by raw key."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return (
            await session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.key_hash == hash_idempotency_key(raw_key)
                )
            )
        ).scalar_one()


async def _get_idempotency_record_or_none(raw_key: str) -> IdempotencyKey | None:
    """Return a persisted idempotency record by raw key when present."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        return (
            await session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.key_hash == hash_idempotency_key(raw_key)
                )
            )
        ).scalar_one_or_none()


async def _insert_in_progress_record(
    *,
    raw_key: str,
    fingerprint: str,
    method: str,
    path: str,
) -> None:
    """Insert an in-progress reservation to drive conflict behavior."""

    session_maker = session_module.AsyncSessionLocal
    assert session_maker is not None
    async with session_maker() as session:
        session.add(
            IdempotencyKey(
                key_hash=hash_idempotency_key(raw_key),
                request_fingerprint=fingerprint,
                request_method=method,
                request_path=path,
                status=IdempotencyStatus.IN_PROGRESS.value,
            )
        )
        await session.commit()


@requires_database
class TestEndpointIdempotency:
    """Mutating endpoint replays and conflicts should match the TRD contract."""

    async def test_create_project_replays_completed_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """POST /projects should replay the original 201 response for the same key."""

        _ = self
        _ = cleanup_projects
        key = "project-create-1"
        payload = {"name": "Replay Project", "description": "first body"}

        first = await _create_project(async_client, payload=payload, idempotency_key=key)
        second = await _create_project(async_client, payload=payload, idempotency_key=key)

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()
        assert await _project_count() == 1

    async def test_update_project_replays_completed_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """PATCH /projects/{id} should replay the original 200 response for the same key."""

        _ = self
        _ = cleanup_projects
        created = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "project-update-1"
        payload = {"description": "updated once"}

        first = await async_client.patch(
            f"/v1/projects/{created['id']}",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.patch(
            f"/v1/projects/{created['id']}",
            json=payload,
            headers=_headers(key),
        )

        assert first.status_code == 200
        assert second.status_code == 200
        assert second.json() == first.json()

    async def test_delete_project_replays_completed_empty_response(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """DELETE /projects/{id} should replay the original 204 response for the same key."""

        _ = self
        _ = cleanup_projects
        created = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "project-delete-1"

        first = await async_client.delete(
            f"/v1/projects/{created['id']}",
            headers=_headers(key),
        )
        second = await async_client.delete(
            f"/v1/projects/{created['id']}",
            headers=_headers(key),
        )

        assert first.status_code == 204
        assert second.status_code == 204
        assert first.content == b""
        assert second.content == b""

    async def test_invalid_idempotency_key_is_rejected(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """Invalid header tokens should fail RFC 9110 token validation."""

        _ = self
        _ = cleanup_projects

        response = await _create_project(
            async_client,
            payload={"name": "Bad Key"},
            idempotency_key="contains space",
        )

        assert response.status_code == 400
        assert response.json() == {
            "error": {
                "code": "INPUT_INVALID",
                "message": "Idempotency-Key must be a valid RFC 9110 token.",
                "details": None,
            }
        }

    async def test_matching_in_progress_reservation_returns_conflict_with_retry_after(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """A matching in-progress reservation should return the shared 409 conflict shape."""

        _ = self
        _ = cleanup_projects
        key = "project-in-progress-1"
        payload = {"name": "Blocked Project"}
        fingerprint = build_idempotency_fingerprint("projects.create", payload)
        await _insert_in_progress_record(
            raw_key=key,
            fingerprint=fingerprint,
            method="POST",
            path="/projects",
        )

        response = await _create_project(async_client, payload=payload, idempotency_key=key)

        assert response.status_code == 409
        assert response.headers["Retry-After"] == "1"
        assert response.json() == {
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": "A request with this Idempotency-Key is already in progress.",
                "details": {"reason": "in_progress"},
            }
        }

    async def test_update_matching_in_progress_reservation_returns_conflict_with_retry_after(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
    ) -> None:
        """PATCH /projects/{id} should include Retry-After on matching in-progress conflicts."""

        _ = self
        _ = cleanup_projects
        created = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "project-update-in-progress-1"
        payload = {"description": "Blocked update"}
        fingerprint = build_idempotency_fingerprint(
            f"projects.update:{created['id']}",
            payload,
        )
        await _insert_in_progress_record(
            raw_key=key,
            fingerprint=fingerprint,
            method="PATCH",
            path=f"/projects/{created['id']}",
        )

        response = await async_client.patch(
            f"/v1/projects/{created['id']}",
            json=payload,
            headers=_headers(key),
        )

        assert response.status_code == 409
        assert response.headers["Retry-After"] == "1"
        assert response.json() == {
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": "A request with this Idempotency-Key is already in progress.",
                "details": {"reason": "in_progress"},
            }
        }

    async def test_upload_replays_completed_response_without_duplicate_rows(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """POST /files should replay the original 201 response and avoid duplicate rows."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-1"

        first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        second = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()
        assert len(enqueued_job_ids) == 1
        assert await _job_count_for_file(first.json()["id"]) == 1

    async def test_upload_same_key_different_request_returns_request_mismatch(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Reusing the same upload key for a different fingerprint should fail with 409."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-mismatch-1"

        first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        second = await _upload_pdf(
            async_client,
            project_id=project["id"],
            filename="different.pdf",
            content=b"%PDF-1.7\ndifferent\n",
            idempotency_key=key,
        )

        assert first.status_code == 201
        assert second.status_code == 409
        assert second.json() == {
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": ("This Idempotency-Key is already associated with a different request."),
                "details": {"reason": "request_mismatch"},
            }
        }

    async def test_upload_publish_failure_replays_success_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Publish failures after commit must replay the original success snapshot."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-enqueue-failure-1"

        def _fail_enqueue(_: uuid.UUID, **__: Any) -> None:
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _fail_enqueue)

        first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        second = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        record = await _get_idempotency_record(key)

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()
        assert await _job_count_for_file(first.json()["id"]) == 1
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 201
        assert record.response_body_json == first.json()

    async def test_upload_storage_failure_replays_sanitized_error_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Storage failures after claim must finalize and replay the sanitized 500."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-storage-failure-1"

        class _FailingStorage:
            async def put(self, *_: Any, **__: Any) -> Any:
                raise OSError("disk full")

        app = _get_async_client_app(async_client)
        get_storage_dependency = cast(Any, files_api).get_storage
        app.dependency_overrides[get_storage_dependency] = lambda: _FailingStorage()
        try:
            first = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
            second = await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        finally:
            app.dependency_overrides.pop(get_storage_dependency, None)

        record = await _get_idempotency_record(key)

        assert first.status_code == 500
        assert first.json() == {
            "error": {
                "code": "STORAGE_FAILED",
                "message": "Failed to persist uploaded file.",
                "details": None,
            }
        }
        assert second.status_code == 500
        assert second.json() == first.json()
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 500
        assert record.response_body_json == first.json()
        assert enqueued_job_ids == []

    @pytest.mark.parametrize(
        "rollback_error",
        [
            pytest.param(None, id="rollback-ok"),
            pytest.param(RuntimeError("forced rollback failure"), id="rollback-fails"),
            pytest.param(asyncio.CancelledError(), id="rollback-cancelled"),
        ],
    )
    async def test_upload_file_cleans_idempotent_persisted_upload_when_db_finalization_fails(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        monkeypatch: pytest.MonkeyPatch,
        rollback_error: BaseException | None,
    ) -> None:
        """Idempotent uploads should cleanup persisted bytes on pre-commit finalization failure."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        key = "file-upload-complete-failure-1"
        upload_root = Path(settings.upload_storage_root).resolve()
        original_ops = idempotency_api.DEFAULT_IDEMPOTENT_MUTATION_OPS
        app = _get_async_client_app(async_client)

        async def _fail_complete(
            db: AsyncSession,
            reservation: IdempotencyReservation | None,
            *,
            status_code: int,
            response_body: dict[str, Any] | None,
        ) -> Response:
            _ = (db, reservation, status_code, response_body)
            raise RuntimeError("forced idempotent finalization failure")

        monkeypatch.setattr(
            idempotency_api,
            "DEFAULT_IDEMPOTENT_MUTATION_OPS",
            IdempotentMutationOps(
                replay=original_ops.replay,
                claim=original_ops.claim,
                complete=_fail_complete,
            ),
        )

        if rollback_error is not None:
            app.dependency_overrides[session_module.get_db] = (
                _make_get_db_override_with_rollback_error(rollback_error)
            )

        try:
            with pytest.raises(RuntimeError, match="forced idempotent finalization failure"):
                await _upload_pdf(async_client, project_id=project["id"], idempotency_key=key)
        finally:
            app.dependency_overrides.pop(session_module.get_db, None)

        staging_root = upload_root / ".staging"
        assert not staging_root.exists() or not any(staging_root.iterdir())

        originals_root = upload_root / "originals"
        assert not originals_root.exists() or not any(
            path.is_file() for path in originals_root.rglob("*")
        )
        assert await _upload_lineage_counts(project["id"]) == (0, 0, 0)

    async def test_mark_idempotency_completed_does_not_overwrite_completed_snapshot(
        self,
        cleanup_projects: None,
    ) -> None:
        """Completed snapshots must remain immutable once a response is finalized."""

        _ = self
        _ = cleanup_projects
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        raw_key = "idempotency-completed-once-1"

        async with session_maker() as session:
            record = IdempotencyKey(
                key_hash=hash_idempotency_key(raw_key),
                request_fingerprint=build_idempotency_fingerprint(
                    "jobs.retry:test",
                    {"job_id": "job-1"},
                ),
                request_method="POST",
                request_path="/jobs/job-1/retry",
                status=IdempotencyStatus.IN_PROGRESS.value,
            )
            session.add(record)
            await session.commit()
            await session.refresh(record)

            reservation = IdempotencyReservation(record_id=record.id)
            first_body = {"job_id": "job-1", "status": "pending"}
            await mark_idempotency_completed(
                session,
                reservation,
                status_code=202,
                response_body=first_body,
            )
            await session.commit()
            await session.refresh(record)
            first_completed_at = record.completed_at

            await mark_idempotency_completed(
                session,
                reservation,
                status_code=500,
                response_body={
                    "error": {
                        "code": "INTERNAL_ERROR",
                        "message": "Failed to enqueue ingest job",
                        "details": None,
                    }
                },
            )
            await session.commit()
            await session.refresh(record)

            assert record.status == IdempotencyStatus.COMPLETED.value
            assert record.response_status_code == 202
            assert record.response_body_json == first_body
            assert record.completed_at == first_completed_at

    async def test_reprocess_replays_original_job_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """POST /reprocess should replay the original 202 job snapshot for the same key."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids

        async def _fake_run_ingestion(request: Any) -> Any:
            return _build_fake_ingest_payload(request)

        monkeypatch.setattr(worker_module, "run_ingestion", _fake_run_ingestion)

        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        initial_job = await _get_job_for_file(uploaded["id"])
        await process_ingest_job(initial_job.id)
        key = "file-reprocess-1"
        payload = {"extraction_profile": {"profile_version": "v0.1"}}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        assert first.status_code == 202
        original_base_revision_id = first.json()["base_revision_id"]
        assert first.json()["job_type"] == "reprocess"
        assert original_base_revision_id is not None

        reprocess_job = await _get_job(first.json()["id"])
        assert reprocess_job.base_revision_id == uuid.UUID(original_base_revision_id)

        await process_ingest_job(reprocess_job.id)

        revisions = await _get_file_revisions(uploaded["id"])
        assert [revision.revision_sequence for revision in revisions] == [1, 2]
        assert str(revisions[-1].id) != original_base_revision_id

        updated_reprocess_job = await _get_job(reprocess_job.id)
        assert updated_reprocess_job.base_revision_id == uuid.UUID(original_base_revision_id)

        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )

        assert second.status_code == 202
        assert second.json() == first.json()
        assert second.json()["base_revision_id"] == original_base_revision_id
        assert await _job_count_for_file(uploaded["id"]) == 2

    async def test_reprocess_missing_base_revision_returns_conflict_without_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Missing base revisions should not persist a completed reprocess snapshot."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        key = "file-reprocess-missing-base-1"
        payload = {"extraction_profile": {"profile_version": "v0.1"}}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )

        assert first.status_code == 409
        assert first.json() == {
            "error": {
                "code": "REVISION_CONFLICT",
                "message": "Reprocess requires a finalized base revision.",
                "details": None,
            }
        }
        assert second.status_code == 409
        assert second.json() == first.json()
        assert await _get_idempotency_record_or_none(key) is None

    async def test_reprocess_invalid_extraction_profile_replays_original_not_found(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """Precondition failures must not leave reprocess keys stuck in progress."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        key = "file-reprocess-missing-profile-1"
        payload = {"extraction_profile_id": str(uuid.uuid4())}

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )

        assert first.status_code == 404
        assert second.status_code == 404
        assert second.json() == first.json()
        assert await _get_idempotency_record_or_none(key) is None

    async def test_db_backed_helper_replays_completed_snapshot_for_post_claim_http_error(
        self,
        cleanup_projects: None,
    ) -> None:
        """A replay-safe post-claim HTTP error should complete once and replay on retry."""

        _ = self
        _ = cleanup_projects
        session_maker = session_module.AsyncSessionLocal
        assert session_maker is not None
        key = "db-backed-http-error-1"
        fingerprint = build_idempotency_fingerprint(
            "idempotency.test.post_claim_http_error",
            {"project_id": "project-1"},
        )
        response_body = {
            "error": {
                "code": "INPUT_INVALID",
                "message": "Payload is invalid.",
                "details": None,
            }
        }
        mutate_calls = 0
        mutate_session: AsyncSession | None = None
        disposable_project_id: uuid.UUID | None = None

        async def _mutate() -> IdempotentMutationSuccess[str]:
            nonlocal mutate_calls, mutate_session, disposable_project_id
            mutate_calls += 1
            assert mutate_session is not None
            disposable_project = Project(name=f"disposable-project-{uuid.uuid4()}")
            mutate_session.add(disposable_project)
            await mutate_session.flush()
            disposable_project_id = disposable_project.id
            raise HTTPException(status_code=400, detail=response_body)

        def _serialize(_: str) -> dict[str, Any]:
            raise AssertionError("serialize should not run for replay-safe HTTP errors")

        async with session_maker() as first_session:
            mutate_session = first_session
            first = await run_idempotent_mutation(
                first_session,
                key=key,
                fingerprint=fingerprint,
                method="POST",
                path="/tests/idempotency/post-claim-http-error",
                mutate=_mutate,
                serialize_result=_serialize,
            )

        assert first.status_code == 400
        assert json.loads(bytes(first.body)) == response_body

        record = await _get_idempotency_record(key)
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 400
        assert record.response_body_json == response_body
        assert disposable_project_id is not None

        async with session_maker() as verify_session:
            assert (await verify_session.get(Project, disposable_project_id)) is None

        async def _mutate_again() -> IdempotentMutationSuccess[str]:
            raise AssertionError("mutate should not run when the completed snapshot replays")

        async with session_maker() as second_session:
            second = await run_idempotent_mutation(
                second_session,
                key=key,
                fingerprint=fingerprint,
                method="POST",
                path="/tests/idempotency/post-claim-http-error",
                mutate=_mutate_again,
                serialize_result=_serialize,
            )

        assert mutate_calls == 1
        assert second.status_code == 400
        assert json.loads(bytes(second.body)) == response_body

        async with session_maker() as verify_session:
            assert (await verify_session.get(Project, disposable_project_id)) is None

    async def test_reprocess_publish_failure_replays_success_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reprocess publish failures after commit must replay success."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        await _finalize_initial_revision(uploaded["id"])
        key = "file-reprocess-enqueue-failure-1"
        payload = {"extraction_profile": {"profile_version": "v0.1"}}

        def _fail_enqueue(_: uuid.UUID, **__: Any) -> None:
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(files_api, "enqueue_ingest_job", _fail_enqueue)

        first = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        second = await async_client.post(
            f"/v1/projects/{project['id']}/files/{uploaded['id']}/reprocess",
            json=payload,
            headers=_headers(key),
        )
        record = await _get_idempotency_record(key)

        assert first.status_code == 202
        assert first.json()["job_type"] == "reprocess"
        assert first.json()["base_revision_id"] is not None
        assert second.status_code == 202
        assert second.json() == first.json()
        assert await _job_count_for_file(uploaded["id"]) == 2
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 202
        assert record.response_body_json == first.json()

    async def test_cancel_replays_original_job_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
    ) -> None:
        """POST /cancel should replay the original 202 job snapshot for the same key."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        job = await _get_job_for_file(uploaded["id"])
        key = "job-cancel-1"

        first = await async_client.post(f"/v1/jobs/{job.id}/cancel", headers=_headers(key))
        assert first.status_code == 202
        await _update_job_status(first.json()["id"], status_value="cancelled")

        second = await async_client.post(f"/v1/jobs/{job.id}/cancel", headers=_headers(key))

        assert second.status_code == 202
        assert second.json() == first.json()

    async def test_retry_replays_original_job_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """POST /retry should replay the original 202 job snapshot for the same key."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )

        async def _record_retry_enqueue(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            enqueued_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(jobs_api, "publish_job_enqueue_intent", _record_retry_enqueue)

        job = await _get_job_for_file(uploaded["id"])
        await _mark_job_failed(str(job.id))
        key = "job-retry-1"
        enqueued_before_retry = len(enqueued_job_ids)

        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        assert first.status_code == 202
        assert len(enqueued_job_ids) == enqueued_before_retry + 1
        await _update_job_status(first.json()["id"], status_value="running")

        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))

        assert second.status_code == 202
        assert second.json() == first.json()
        assert len(enqueued_job_ids) == enqueued_before_retry + 1

    async def test_retry_revision_conflict_replays_original_failed_snapshot_without_enqueue(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """REVISION_CONFLICT retries should replay the unchanged failed job snapshot."""

        _ = self
        _ = cleanup_projects
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )

        async def _record_retry_enqueue(
            job_id: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _ = (publisher, suppress_exceptions, kwargs)
            enqueued_job_ids.append(str(job_id))
            return True

        monkeypatch.setattr(jobs_api, "publish_job_enqueue_intent", _record_retry_enqueue)

        job = await _get_job_for_file(uploaded["id"])
        await _mark_job_failed(
            str(job.id),
            error_code=ErrorCode.REVISION_CONFLICT.value,
            error_message="Reprocess base revision became stale before finalization.",
        )
        key = "job-retry-revision-conflict-1"
        enqueued_before_retry = len(enqueued_job_ids)

        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        record = await _get_idempotency_record(key)

        assert first.status_code == 202
        assert second.status_code == 202
        assert second.json() == first.json()
        assert first.json()["status"] == "failed"
        assert first.json()["error_code"] == ErrorCode.REVISION_CONFLICT.value
        assert len(enqueued_job_ids) == enqueued_before_retry
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 202
        assert record.response_body_json == first.json()

    async def test_retry_publish_failure_replays_success_snapshot(
        self,
        async_client: httpx.AsyncClient,
        cleanup_projects: None,
        enqueued_job_ids: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retry publish failures after commit must replay success."""

        _ = self
        _ = cleanup_projects
        _ = enqueued_job_ids
        project = cast(dict[str, Any], (await _create_project(async_client)).json())
        uploaded = cast(
            dict[str, Any],
            (await _upload_pdf(async_client, project_id=project["id"])).json(),
        )
        job = await _get_job_for_file(uploaded["id"])
        await _mark_job_failed(str(job.id))
        key = "job-retry-enqueue-failure-1"

        async def _fail_enqueue(
            _: uuid.UUID,
            *,
            publisher: Any | None = None,
            suppress_exceptions: bool = False,
            **kwargs: Any,
        ) -> bool:
            _unused = (publisher, suppress_exceptions, kwargs)
            if suppress_exceptions:
                return False
            raise RuntimeError("broker unavailable")

        monkeypatch.setattr(jobs_api, "publish_job_enqueue_intent", _fail_enqueue)

        first = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        second = await async_client.post(f"/v1/jobs/{job.id}/retry", headers=_headers(key))
        record = await _get_idempotency_record(key)

        assert first.status_code == 202
        assert first.json()["status"] == "pending"
        assert second.status_code == 202
        assert second.json() == first.json()
        assert record.status == IdempotencyStatus.COMPLETED.value
        assert record.response_status_code == 202
        assert record.response_body_json == first.json()
