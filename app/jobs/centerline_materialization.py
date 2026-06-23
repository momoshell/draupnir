"""Centerline materialization orchestrator (worker-side, impure).

Loads routed entities + geometry for a revision, calls the appropriate producer
selected by ``input_family``, and persists
:class:`~app.models.revision_routed_length.RevisionRoutedLength` rows with an
idempotent INSERT ... ON CONFLICT DO NOTHING.

This module is intentionally impure: it accesses the DB and imports ORM models.
It is NOT imported on the read path.  Any future cv2/skimage import must be lazy
and guarded so an ImportError degrades gracefully (job fails; read stays provisional).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.ingestion.centerline_contract import CURRENT_ALGO_VERSION, Centerline
from app.ingestion.centerline_dwg import dwg_centerlines
from app.ingestion.centerline_passthrough import passthrough_centerlines
from app.interpretation.routed_runs import RunGroup, identify_routed_runs
from app.models.revision_routed_length import RevisionRoutedLength

logger = get_logger(__name__)

# Type alias for centerline producer callables.
_ProducerFn = Callable[[Sequence[RunGroup], Mapping[str, Mapping[str, Any]]], list[Centerline]]


def select_centerline_producer(input_family: str | None) -> _ProducerFn:
    """Return the appropriate centerline producer for the given ``input_family``.

    - ``"dwg"`` / ``"dxf"`` -> :func:`~app.ingestion.centerline_dwg.dwg_centerlines`
    - everything else (``"pdf_vector"``, ``"pdf_raster"``, ``None``, …) ->
      :func:`~app.ingestion.centerline_passthrough.passthrough_centerlines`

    Branching is intentionally minimal so #641 can add a ``pdf_vector`` branch
    with a single line.
    """
    if input_family in ("dwg", "dxf"):
        return dwg_centerlines
    return passthrough_centerlines


async def materialize_centerline_lengths(
    session: AsyncSession,
    *,
    job_id: UUID,
    project_id: UUID,
    source_file_id: UUID,
    drawing_revision_id: UUID,
    adapter_run_output_id: UUID | None,
    canonical_entity_schema_version: str,
) -> list[Centerline]:
    """Load routed entities, run the appropriate producer, and persist results.

    Idempotent: uses ``INSERT ... ON CONFLICT (uq_revision_routed_lengths_group_version)
    DO NOTHING`` so duplicate calls for the same (revision, group, version) are safe.
    Append-only trigger on the table prevents UPDATE/DELETE.

    Parameters
    ----------
    session:
        Active async session (must NOT be inside the ingest transaction).
    job_id:
        The CENTERLINE job that triggered this materialization (lineage).
    project_id, source_file_id, drawing_revision_id:
        Revision lineage columns.
    adapter_run_output_id:
        Nullable lineage column from the revision's manifest.
    canonical_entity_schema_version:
        Schema version from the revision's manifest.

    Returns
    -------
    list[Centerline]
        The produced centerlines (may be empty if the revision has no routed entities).
    """
    # Lazy import avoids circular import via service_takeoff_loaders -> api.v1.revision_routes.
    from app.interpretation.service_takeoff_loaders import load_service_takeoff_inputs

    # Load all takeoff inputs (routed entities + geometry).
    inputs = await load_service_takeoff_inputs(session, drawing_revision_id)

    # Identify run groups from routed entities + legend.
    run_groups = identify_routed_runs(inputs.routed_entities, inputs.legend).groups

    if not run_groups:
        logger.info(
            "centerline_materialization_no_groups",
            job_id=str(job_id),
            drawing_revision_id=str(drawing_revision_id),
        )
        return []

    # Select producer by input_family and produce per-group centerlines.
    producer = select_centerline_producer(inputs.input_family)
    centerlines = producer(run_groups, inputs.geometry_by_entity_id)

    # Persist with idempotent upsert.
    values: list[dict[str, Any]] = []
    for cl in centerlines:
        values.append(
            {
                "id": uuid.uuid4(),
                "project_id": project_id,
                "source_file_id": source_file_id,
                "extraction_profile_id": None,
                "source_job_id": job_id,
                "drawing_revision_id": drawing_revision_id,
                "adapter_run_output_id": adapter_run_output_id,
                "canonical_entity_schema_version": canonical_entity_schema_version,
                "layer_ref": cl.layer_ref,
                "colour_key": cl.colour_key,
                "algo_version": cl.algo_version,
                "raster_params_hash": cl.raster_params_hash,
                "producer_kind": cl.producer_kind,
                "skeleton_length_du": cl.geometry.length_du,
                "entity_count": cl.entity_count,
                "geometry_json": None,
            }
        )

    if values:
        stmt = (
            insert(RevisionRoutedLength)
            .values(values)
            .on_conflict_do_nothing(constraint="uq_revision_routed_lengths_group_version")
        )
        await session.execute(stmt)

    logger.info(
        "centerline_materialization_persisted",
        job_id=str(job_id),
        drawing_revision_id=str(drawing_revision_id),
        group_count=len(centerlines),
        algo_version=CURRENT_ALGO_VERSION,
    )
    return centerlines
