"""Pure unit tests for job finalization row-builders (issue D5a).

These exercise the row-construction seam with in-memory fakes only — no database,
no session. The builders read attributes off the engine result / execution input,
so duck-typed fakes (cast to the declared types) are sufficient and keep the tests
fast and isolated.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import cast

from app.estimating.engine.contracts import EstimateEngineOutput
from app.estimating.quantities.contracts import QuantityEngineResult
from app.jobs.execution_inputs import _QuantityTakeoffExecutionInput
from app.jobs.result_builders import (
    build_estimate_rows,
    build_quantity_takeoff_rows,
)


def _execution(drawing_revision_id: uuid.UUID) -> _QuantityTakeoffExecutionInput:
    return cast(
        _QuantityTakeoffExecutionInput,
        SimpleNamespace(
            drawing_revision_id=drawing_revision_id,
            validation_status="valid",
        ),
    )


def _result() -> QuantityEngineResult:
    contributor = SimpleNamespace(
        entity_id="ent-1",
        quantity_type="length",
        value=12.5,
        unit="m",
        context={"phase": "demo"},
        duplicate_entity_ids=("dup-1", "", "dup-2"),
    )
    aggregate = SimpleNamespace(
        quantity_type="length",
        total=12.5,
        unit="",  # normalized to "unknown"
        context=None,
    )
    exclusion = SimpleNamespace(
        entity_id="ent-2",
        quantity_type=None,
        context=None,
    )
    return cast(
        QuantityEngineResult,
        SimpleNamespace(
            contributors=(contributor,),
            aggregates=(aggregate,),
            exclusions=(exclusion,),
        ),
    )


def test_build_quantity_takeoff_rows_builds_header_and_linked_items() -> None:
    project_id = uuid.uuid4()
    file_id = uuid.uuid4()
    job_id = uuid.uuid4()
    revision_id = uuid.uuid4()

    rows = build_quantity_takeoff_rows(
        project_id=project_id,
        source_file_id=file_id,
        source_job_id=job_id,
        execution=_execution(revision_id),
        result=_result(),
    )

    takeoff = rows.takeoff
    assert takeoff.project_id == project_id
    assert takeoff.source_file_id == file_id
    assert takeoff.source_job_id == job_id
    assert takeoff.drawing_revision_id == revision_id

    # Every item is linked to the freshly-allocated takeoff id.
    assert rows.items, "expected at least one item"
    assert all(item.quantity_takeoff_id == takeoff.id for item in rows.items)

    by_kind = {item.item_kind: item for item in rows.items}
    contributor = by_kind["contributor"]
    # Context disambiguation: type is suffixed with the serialized context.
    assert contributor.quantity_type == 'length:{"phase":"demo"}'
    assert contributor.value == 12.5
    assert contributor.source_entity_id == "ent-1"
    # Empty duplicate ids are dropped.
    assert contributor.excluded_source_entity_ids_json == ["dup-1", "dup-2"]

    aggregate = by_kind["aggregate"]
    assert aggregate.quantity_type == "length"  # no context -> no suffix
    assert aggregate.unit == "unknown"  # empty unit normalized
    assert aggregate.source_entity_id is None

    exclusion = by_kind["exclusion"]
    assert exclusion.value is None
    assert exclusion.unit == "unknown"
    assert exclusion.quantity_type == "unknown"  # None type normalized
    assert exclusion.source_entity_id == "ent-2"


def test_build_estimate_rows_materializes_version_entries_and_line_items() -> None:
    revision_id = uuid.uuid4()
    version_kwargs = {"id": uuid.uuid4(), "drawing_revision_id": revision_id}
    entry_kwargs = [{"id": uuid.uuid4()}, {"id": uuid.uuid4()}]
    line_kwargs = [{"id": uuid.uuid4()}]

    output = cast(
        EstimateEngineOutput,
        SimpleNamespace(
            estimate_version_model_kwargs=lambda: dict(version_kwargs),
            snapshot_entry_model_kwargs=lambda: tuple(dict(k) for k in entry_kwargs),
            line_item_model_kwargs=lambda: tuple(dict(k) for k in line_kwargs),
        ),
    )

    rows = build_estimate_rows(output)

    assert rows.version.drawing_revision_id == revision_id
    assert len(rows.snapshot_entries) == 2
    assert len(rows.line_items) == 1
