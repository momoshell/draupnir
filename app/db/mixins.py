"""Shared declarative SQLAlchemy mixins/helpers."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, ClassVar, cast

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, MappedColumn, declarative_mixin, declared_attr, mapped_column


def _require_str_config(model_cls: type[Any], attribute_name: str) -> str:
    value = getattr(model_cls, attribute_name, None)
    if not isinstance(value, str) or not value:
        raise TypeError(f"{model_cls.__name__} must define a non-empty {attribute_name}")
    return value


def _table_name(model_cls: type[Any]) -> str:
    table_name = getattr(model_cls, "__tablename__", None)
    if not isinstance(table_name, str) or not table_name:
        raise TypeError(f"{model_cls.__name__} must define a non-empty __tablename__")
    return table_name


def _fk_name(model_cls: type[Any], local_column: str, remote_table: str) -> str:
    return f"fk_{_table_name(model_cls)}_{local_column}_{remote_table}"


@declarative_mixin
class TimestampMixin:
    """Adds a created_at timestamp column with subclass-owned comment text."""

    __created_at_comment__: ClassVar[str]

    @declared_attr
    def created_at(cls) -> Mapped[datetime]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            DateTime(timezone=True),
            default=func.now(),
            nullable=False,
            comment=_require_str_config(model_cls, "__created_at_comment__"),
        )


@declarative_mixin
class ProjectScopedMixin:
    """Adds the project ownership column used by scoped lineage tables."""

    @declared_attr
    def project_id(cls) -> Mapped[uuid.UUID]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            ForeignKey(
                "projects.id",
                name=_fk_name(model_cls, "project_id", "projects"),
                ondelete="RESTRICT",
            ),
            nullable=False,
            index=True,
            comment="Owning project identifier",
        )


@declarative_mixin
class RevisionLineageMixin:
    """Adds the repeated revision materialization lineage columns only."""

    __source_file_comment__: ClassVar[str]
    __extraction_profile_comment__: ClassVar[str]
    __source_job_comment__: ClassVar[str]
    __drawing_revision_comment__: ClassVar[str]
    __adapter_run_output_comment__: ClassVar[str]
    __canonical_entity_schema_version_comment__: ClassVar[str]

    @declared_attr
    def source_file_id(cls) -> Mapped[uuid.UUID]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            nullable=False,
            index=True,
            comment=_require_str_config(model_cls, "__source_file_comment__"),
        )

    @declared_attr
    def extraction_profile_id(cls) -> Mapped[uuid.UUID | None]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            nullable=True,
            index=True,
            comment=_require_str_config(model_cls, "__extraction_profile_comment__"),
        )

    @declared_attr
    def source_job_id(cls) -> Mapped[uuid.UUID]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            ForeignKey(
                "jobs.id",
                name=_fk_name(model_cls, "source_job_id", "jobs"),
                ondelete="RESTRICT",
            ),
            nullable=False,
            index=True,
            comment=_require_str_config(model_cls, "__source_job_comment__"),
        )

    @declared_attr
    def drawing_revision_id(cls) -> Mapped[uuid.UUID]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            nullable=False,
            comment=_require_str_config(model_cls, "__drawing_revision_comment__"),
        )

    @declared_attr
    def adapter_run_output_id(cls) -> Mapped[uuid.UUID | None]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            nullable=True,
            index=True,
            comment=_require_str_config(model_cls, "__adapter_run_output_comment__"),
        )

    @declared_attr
    def canonical_entity_schema_version(cls) -> Mapped[str]:  # noqa: N805
        model_cls = cast(type[Any], cls)
        return mapped_column(
            String(16),
            nullable=False,
            comment=_require_str_config(model_cls, "__canonical_entity_schema_version_comment__"),
        )


def sha256_column(*, nullable: bool, index: bool, comment: str) -> MappedColumn[Any]:
    """Return the standard nullable/non-nullable SHA-256 text column shape."""

    return mapped_column(String(64), nullable=nullable, index=index, comment=comment)
