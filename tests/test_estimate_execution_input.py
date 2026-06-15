"""Pure unit tests for estimate execution-input lineage validation (issue D7c).

The estimate assembly needs a live session for catalog resolution, but its
lineage checks are pure over loaded rows. These tests exercise those validators
with in-memory fakes (cast to the model types) — no database, no session.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.jobs.estimate_execution_input import (
    validate_estimate_input,
    validate_estimate_job,
    validate_quantity_takeoff,
)
from app.jobs.estimate_mapping import _EstimateJobInputError
from app.jobs.lifecycle import _RevisionConflictError, _StaleJobAttemptError
from app.models.estimate_job_input import EstimateJobInput
from app.models.job import JobType
from app.models.quantity_takeoff import QuantityTakeoff

PROJECT_ID = uuid.uuid4()
FILE_ID = uuid.uuid4()
JOB_ID = uuid.uuid4()
REVISION_ID = uuid.uuid4()
TAKEOFF_ID = uuid.uuid4()
TOKEN = uuid.uuid4()


def _job(**overrides: Any) -> Any:
    base = {
        "status": "running",
        "attempt_token": TOKEN,
        "job_type": JobType.ESTIMATE.value,
        "base_revision_id": REVISION_ID,
        "project_id": PROJECT_ID,
        "file_id": FILE_ID,
        "id": JOB_ID,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _estimate_input(**overrides: Any) -> EstimateJobInput:
    base = {
        "project_id": PROJECT_ID,
        "source_file_id": FILE_ID,
        "drawing_revision_id": REVISION_ID,
        "quantity_takeoff_id": TAKEOFF_ID,
        "quantity_gate": "allowed",
        "trusted_totals": True,
        "source_job_type": JobType.ESTIMATE.value,
    }
    base.update(overrides)
    return cast(EstimateJobInput, SimpleNamespace(**base))


def _takeoff(**overrides: Any) -> QuantityTakeoff:
    base = {
        "id": TAKEOFF_ID,
        "project_id": PROJECT_ID,
        "source_file_id": FILE_ID,
        "drawing_revision_id": REVISION_ID,
        "quantity_gate": "allowed",
        "trusted_totals": True,
        "source_job_type": JobType.QUANTITY_TAKEOFF.value,
    }
    base.update(overrides)
    return cast(QuantityTakeoff, SimpleNamespace(**base))


# --- validate_estimate_job ---


def test_validate_estimate_job_returns_job_when_valid() -> None:
    job = _job()
    assert validate_estimate_job(job, attempt_token=TOKEN, job_id=JOB_ID) is job


def test_validate_estimate_job_missing_raises_lookup_error() -> None:
    with pytest.raises(LookupError):
        validate_estimate_job(None, attempt_token=TOKEN, job_id=JOB_ID)


def test_validate_estimate_job_stale_attempt_raises() -> None:
    with pytest.raises(_StaleJobAttemptError):
        validate_estimate_job(_job(attempt_token=uuid.uuid4()), attempt_token=TOKEN, job_id=JOB_ID)


def test_validate_estimate_job_wrong_type_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported estimate job type"):
        validate_estimate_job(
            _job(job_type=JobType.EXPORT.value), attempt_token=TOKEN, job_id=JOB_ID
        )


def test_validate_estimate_job_missing_base_revision_raises_conflict() -> None:
    with pytest.raises(_RevisionConflictError):
        validate_estimate_job(_job(base_revision_id=None), attempt_token=TOKEN, job_id=JOB_ID)


# --- validate_estimate_input ---


def test_validate_estimate_input_returns_when_valid() -> None:
    estimate_input = _estimate_input()
    assert validate_estimate_input(estimate_input, job=_job()) is estimate_input


def test_validate_estimate_input_missing_raises() -> None:
    with pytest.raises(_EstimateJobInputError) as exc:
        validate_estimate_input(None, job=_job())
    assert exc.value.details is not None
    assert exc.value.details["reason"] == "missing_estimate_job_input"


def test_validate_estimate_input_lineage_mismatch_raises() -> None:
    with pytest.raises(_EstimateJobInputError) as exc:
        validate_estimate_input(_estimate_input(drawing_revision_id=uuid.uuid4()), job=_job())
    assert exc.value.details is not None
    assert exc.value.details["reason"] == "estimate_input_lineage_mismatch"


# --- validate_quantity_takeoff ---


def test_validate_quantity_takeoff_returns_when_valid() -> None:
    takeoff = _takeoff()
    assert (
        validate_quantity_takeoff(takeoff, estimate_input=_estimate_input(), job=_job()) is takeoff
    )


def test_validate_quantity_takeoff_missing_raises() -> None:
    with pytest.raises(_EstimateJobInputError) as exc:
        validate_quantity_takeoff(None, estimate_input=_estimate_input(), job=_job())
    assert exc.value.details is not None
    assert exc.value.details["reason"] == "missing_quantity_takeoff"


def test_validate_quantity_takeoff_gate_mismatch_raises() -> None:
    with pytest.raises(_EstimateJobInputError) as exc:
        validate_quantity_takeoff(
            _takeoff(quantity_gate="blocked"),
            estimate_input=_estimate_input(),
            job=_job(),
        )
    assert exc.value.details is not None
    assert exc.value.details["reason"] == "quantity_takeoff_lineage_mismatch"


def test_validate_quantity_takeoff_untrusted_totals_raises() -> None:
    with pytest.raises(_EstimateJobInputError) as exc:
        validate_quantity_takeoff(
            _takeoff(trusted_totals=False),
            estimate_input=_estimate_input(),
            job=_job(),
        )
    assert exc.value.details is not None
    assert exc.value.details["reason"] == "quantity_takeoff_lineage_mismatch"
