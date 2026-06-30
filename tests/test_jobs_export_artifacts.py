"""Unit tests for export artifact worker helpers."""

from __future__ import annotations

import hashlib
import uuid
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.exports._base import ExportArtifact
from app.jobs import export_artifacts
from app.jobs.execution_inputs import _ExportExecutionInput
from app.jobs.export_execution_input import _ExportJobInputError


def test_build_export_artifact_name_uses_registered_lineage_anchor() -> None:
    takeoff_id = uuid.uuid4()

    artifact_name = export_artifacts.build_export_artifact_name(
        export_kind="quantity_csv",
        export_format="csv",
        drawing_revision_id=uuid.uuid4(),
        quantity_takeoff_id=takeoff_id,
    )

    assert artifact_name == f"quantity-takeoff-{takeoff_id}.csv"


def test_get_export_kind_spec_rejects_unsupported_kind() -> None:
    with pytest.raises(_ExportJobInputError) as exc_info:
        export_artifacts.get_export_kind_spec("unsupported")

    assert exc_info.value.message == "Export job kind is not supported by the worker."
    assert exc_info.value.details == {"export_kind": "unsupported"}


@pytest.mark.asyncio
async def test_render_export_artifact_validates_renderer_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content_bytes = b"artifact"
    execution = _ExportExecutionInput(
        drawing_revision_id=uuid.uuid4(),
        export_kind="fake",
        export_format="json",
        media_type="application/json",
        artifact_name="fake.json",
        options_json={},
    )

    async def render(
        session: AsyncSession,
        execution: _ExportExecutionInput,
    ) -> ExportArtifact:
        return ExportArtifact(
            content_bytes=content_bytes,
            checksum_sha256=hashlib.sha256(content_bytes).hexdigest(),
            size_bytes=len(content_bytes),
            media_type=execution.media_type,
            generator_name="test",
            generator_version="1",
        )

    monkeypatch.setitem(
        export_artifacts.EXPORT_KIND_SPECS,
        "fake",
        export_artifacts.ExportKindSpec(
            format="json",
            media_type="application/json",
            render_fn=render,
            error_type=RuntimeError,
            lineage_anchor=export_artifacts.EXPORT_LINEAGE_ANCHOR_REVISION,
            error_details_fn=lambda _execution: {},
        ),
    )

    rendered = await export_artifacts.render_export_artifact(
        cast(AsyncSession, object()),
        execution,
    )

    assert rendered.content_bytes == content_bytes
    assert rendered.media_type == execution.media_type


@pytest.mark.asyncio
async def test_render_export_artifact_rejects_checksum_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution = _ExportExecutionInput(
        drawing_revision_id=uuid.uuid4(),
        export_kind="bad_checksum",
        export_format="json",
        media_type="application/json",
        artifact_name="bad.json",
        options_json={},
    )

    async def render(
        session: AsyncSession,
        execution: _ExportExecutionInput,
    ) -> ExportArtifact:
        return ExportArtifact(
            content_bytes=b"artifact",
            checksum_sha256="wrong",
            size_bytes=len(b"artifact"),
            media_type=execution.media_type,
            generator_name="test",
            generator_version="1",
        )

    monkeypatch.setitem(
        export_artifacts.EXPORT_KIND_SPECS,
        "bad_checksum",
        export_artifacts.ExportKindSpec(
            format="json",
            media_type="application/json",
            render_fn=render,
            error_type=RuntimeError,
            lineage_anchor=export_artifacts.EXPORT_LINEAGE_ANCHOR_REVISION,
            error_details_fn=lambda _execution: {},
        ),
    )

    with pytest.raises(ValueError, match="checksum"):
        await export_artifacts.render_export_artifact(
            cast(AsyncSession, object()),
            execution,
        )
