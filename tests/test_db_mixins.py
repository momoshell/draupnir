"""Tests for shared SQLAlchemy declarative mixins/helpers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from sqlalchemy import DateTime, String, Table
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.mixins import ProjectScopedMixin, RevisionLineageMixin, TimestampMixin, sha256_column
from app.models.revision_materialization import (
    RevisionBlock,
    RevisionEntity,
    RevisionEntityManifest,
    RevisionLayer,
    RevisionLayout,
)


def _foreign_key_names(column: Any) -> set[str]:
    return {foreign_key.constraint.name for foreign_key in column.foreign_keys}


def _table_for(model_cls: Any) -> Table:
    return cast(Table, model_cls.__table__)


@pytest.mark.parametrize(
    (
        "model_cls",
        "table_name",
        "source_file_comment",
        "extraction_profile_comment",
        "source_job_comment",
        "drawing_comment",
        "adapter_comment",
        "schema_comment",
        "created_at_comment",
    ),
    [
        (
            RevisionEntityManifest,
            "revision_entity_manifests",
            "Immutable source file identifier for the materialized revision",
            "Immutable extraction profile identifier used for materialization",
            "Job identifier that materialized normalized revision entities",
            "Drawing revision identifier whose normalized entities were materialized",
            "Adapter run output identifier materialized into revision-scoped entity tables",
            "Canonical entity schema version for the materialized revision payloads",
            "Manifest creation timestamp",
        ),
        (
            RevisionLayout,
            "revision_layouts",
            "Immutable source file identifier for this materialized layout row",
            "Immutable extraction profile identifier used for this layout row",
            "Job identifier that materialized this layout row",
            "Drawing revision identifier that owns this layout row",
            "Adapter run output identifier that produced this layout row",
            "Canonical entity schema version for this layout row payload",
            "Revision layout materialization timestamp",
        ),
        (
            RevisionLayer,
            "revision_layers",
            "Immutable source file identifier for this materialized layer row",
            "Immutable extraction profile identifier used for this layer row",
            "Job identifier that materialized this layer row",
            "Drawing revision identifier that owns this layer row",
            "Adapter run output identifier that produced this layer row",
            "Canonical entity schema version for this layer row payload",
            "Revision layer materialization timestamp",
        ),
        (
            RevisionBlock,
            "revision_blocks",
            "Immutable source file identifier for this materialized block row",
            "Immutable extraction profile identifier used for this block row",
            "Job identifier that materialized this block row",
            "Drawing revision identifier that owns this block row",
            "Adapter run output identifier that produced this block row",
            "Canonical entity schema version for this block row payload",
            "Revision block materialization timestamp",
        ),
        (
            RevisionEntity,
            "revision_entities",
            "Immutable source file identifier for this materialized entity row",
            "Immutable extraction profile identifier used for this entity row",
            "Job identifier that materialized this entity row",
            "Drawing revision identifier that owns this entity row",
            "Adapter run output identifier that produced this entity row",
            "Canonical entity schema version for this entity row payload",
            "Revision entity materialization timestamp",
        ),
    ],
)
def test_revision_materialization_models_use_shared_mixins(
    model_cls: Any,
    table_name: str,
    source_file_comment: str,
    extraction_profile_comment: str,
    source_job_comment: str,
    drawing_comment: str,
    adapter_comment: str,
    schema_comment: str,
    created_at_comment: str,
) -> None:
    assert issubclass(model_cls, ProjectScopedMixin)
    assert issubclass(model_cls, RevisionLineageMixin)
    assert issubclass(model_cls, TimestampMixin)

    table = _table_for(model_cls)
    assert table.name == table_name

    project_id = table.c.project_id
    assert project_id.nullable is False
    assert project_id.index is True
    assert project_id.comment == "Owning project identifier"
    assert f"fk_{table_name}_project_id_projects" in _foreign_key_names(project_id)

    source_file_id = table.c.source_file_id
    assert source_file_id.nullable is False
    assert source_file_id.index is True
    assert source_file_id.comment == source_file_comment

    extraction_profile_id = table.c.extraction_profile_id
    assert extraction_profile_id.nullable is True
    assert extraction_profile_id.index is True
    assert extraction_profile_id.comment == extraction_profile_comment

    source_job_id = table.c.source_job_id
    assert source_job_id.nullable is False
    assert source_job_id.index is True
    assert source_job_id.comment == source_job_comment
    assert f"fk_{table_name}_source_job_id_jobs" in _foreign_key_names(source_job_id)

    drawing_revision_id = table.c.drawing_revision_id
    assert drawing_revision_id.nullable is False
    assert drawing_revision_id.comment == drawing_comment

    adapter_run_output_id = table.c.adapter_run_output_id
    assert adapter_run_output_id.nullable is True
    assert adapter_run_output_id.index is True
    assert adapter_run_output_id.comment == adapter_comment

    canonical_entity_schema_version = table.c.canonical_entity_schema_version
    assert isinstance(canonical_entity_schema_version.type, String)
    assert canonical_entity_schema_version.type.length == 16
    assert canonical_entity_schema_version.nullable is False
    assert canonical_entity_schema_version.comment == schema_comment

    created_at = table.c.created_at
    assert isinstance(created_at.type, DateTime)
    assert created_at.type.timezone is True
    assert created_at.nullable is False
    assert created_at.default is not None
    assert created_at.comment == created_at_comment


def test_revision_entity_source_hash_preserves_sha256_contract() -> None:
    table = _table_for(RevisionEntity)
    source_hash = table.c.source_hash

    assert isinstance(source_hash.type, String)
    assert source_hash.type.length == 64
    assert source_hash.nullable is True
    assert source_hash.index is True
    assert source_hash.comment == (
        "Stable source hash derived from canonical entity top-level or provenance refs"
    )

    check_constraint_names = {
        constraint.name for constraint in table.constraints if constraint.name
    }
    assert "ck_revision_entities_source_hash" in check_constraint_names


def test_sha256_column_helper_standardizes_string_length() -> None:
    class Sha256Probe(Base):
        __tablename__ = "test_sha256_probe"

        id: Mapped[int] = mapped_column(primary_key=True)
        digest: Mapped[str | None] = sha256_column(
            nullable=True,
            index=True,
            comment="Probe digest",
        )

    try:
        table = _table_for(Sha256Probe)
        digest = table.c.digest
        assert isinstance(digest.type, String)
        assert digest.type.length == 64
        assert digest.nullable is True
        assert digest.index is True
        assert digest.comment == "Probe digest"
    finally:
        Base.metadata.remove(table)
