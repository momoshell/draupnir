"""Routed-service takeoff compute-on-read route (issue #606, P3 / be-p3-03).

ADR-005: compute-on-read only. No persistence. Does NOT import or reference
app.estimating.quantities, QuantityTakeoff, QuantityItem, result_builders,
estimate_execution_input, or estimate_assembly.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.revision_lineage import _get_active_revision_manifest_or_409
from app.api.v1.revision_routes.rooms import _resolve_rooms
from app.core.logging import get_logger
from app.db.session import get_db, get_session_maker
from app.ingestion.centerline_contract import _xy
from app.interpretation.rise_drop import KIND_DROP, KIND_RISE, cluster_rise_drop_symbols
from app.interpretation.routed_connectivity import refine_shared_by_connectivity
from app.interpretation.routed_runs import identify_routed_runs
from app.interpretation.run_service_identity import fuse_run_service_identities
from app.interpretation.run_tags import parse_tag
from app.interpretation.segment_label_takeoff import (
    SegmentLabel,
    SegmentLabelResult,
    compute_segment_label_lengths,
)
from app.interpretation.service_takeoff import SERVICE_UNKNOWN, compute_service_takeoff
from app.interpretation.service_takeoff_loaders import (
    _DEFAULT_CENTERLINE_LAYER_TOKENS,
    INPUT_FAMILY_PDF_VECTOR,
    load_bundle_bands_by_colour,
    load_measured_geometry,
    load_measured_lengths,
    load_service_fill_bands,
    load_service_takeoff_inputs,
    load_stack_headers,
    load_tag_stack_texts,
)
from app.interpretation.tag_stack_service import assign_services_by_tag_stack
from app.jobs.worker import enqueue_centerline_job as _enqueue_centerline_job_direct
from app.jobs.worker import prepare_job_enqueue_intent as _prepare_job_enqueue_intent_direct
from app.jobs.worker import publish_job_enqueue_intent as _publish_job_enqueue_intent_direct
from app.models.job import Job, JobType
from app.schemas.revision import RevisionEntityManifestRead
from app.schemas.service_takeoff import (
    ServiceFillAttributionRead,
    ServiceFillColourRead,
    ServiceSegmentLabelAttributionRead,
    ServiceSegmentServiceRead,
    ServiceSegmentSizeRead,
    ServiceTagAttributionRead,
    ServiceTagColourRead,
    ServiceTakeoffLineRead,
    ServiceTakeoffResponse,
    ServiceTakeoffScaleRead,
    ServiceTakeoffSummaryRead,
)

service_takeoff_router = APIRouter()

logger = get_logger(__name__)

ServiceTakeoffScope = Literal["sheet", "modelspace"]
_DEFAULT_SCOPE: ServiceTakeoffScope = "sheet"
_CENTERLINE_INFLIGHT_STATUSES: tuple[str, ...] = ("pending", "running")


# ---------------------------------------------------------------------------
# Route-local seams for monkeypatching in tests
# ---------------------------------------------------------------------------


def _enqueue_centerline_job(job_id: UUID) -> None:
    """Route-local centerline enqueue seam for tests."""
    _enqueue_centerline_job_direct(job_id)


def _prepare_job_enqueue_intent(job: Job) -> None:
    """Route-local enqueue intent staging seam for tests."""
    _prepare_job_enqueue_intent_direct(job)


async def _publish_job_enqueue_intent(*args: object, **kwargs: object) -> None:
    """Route-local enqueue publication seam for tests."""
    await _publish_job_enqueue_intent_direct(*args, **kwargs)  # type: ignore[arg-type]


async def _enqueue_centerline_materialization(
    *, revision_id: UUID, project_id: UUID, source_file_id: UUID
) -> None:
    """Best-effort lazy trigger for centerline materialization of a revision.

    Deduped: skips when a non-terminal CENTERLINE job already exists for the revision, so
    repeated/concurrent reads of an unmaterialized revision do not pile up jobs. Runs on its
    OWN short-lived session so the read handler's snapshot is untouched, and never raises into
    the read path (enqueue failure -> serve provisional, logged).
    """
    session_maker = get_session_maker()
    if session_maker is None:
        return
    try:
        async with session_maker() as session:
            existing = await session.scalar(
                select(Job.id)
                .where(
                    Job.base_revision_id == revision_id,
                    Job.job_type == JobType.CENTERLINE.value,
                    Job.status.in_(_CENTERLINE_INFLIGHT_STATUSES),
                )
                .limit(1)
            )
            if existing is not None:
                return  # already queued/running -- dedup
            centerline_job = Job(
                id=uuid4(),
                project_id=project_id,
                file_id=source_file_id,
                extraction_profile_id=None,
                base_revision_id=revision_id,
                parent_job_id=None,
                job_type=JobType.CENTERLINE.value,
                status="pending",
                attempts=0,
                max_attempts=3,
                enqueue_status="pending",
                enqueue_attempts=0,
                cancel_requested=False,
                error_code=None,
                error_message=None,
                started_at=None,
                finished_at=None,
            )
            session.add(centerline_job)
            _prepare_job_enqueue_intent(centerline_job)
            await session.flush()
            job_id = centerline_job.id
            await session.commit()
        await _publish_job_enqueue_intent(
            job_id,
            publisher=_enqueue_centerline_job,
            suppress_exceptions=True,
        )
    except Exception:
        # Enqueue failure must never degrade the read -- serve provisional, but surface a signal.
        logger.warning("centerline_enqueue_failed", revision_id=str(revision_id), exc_info=True)


# Default tag-association radius in metres (#661 — adapters pre-scale geometry to metres).
# Calibrated across M-540003 (pipe callout tag at 3.85 m) + E-610003 (labels ≤2.2 m);
# plateau-stable 5-7 m; junk >=16 m. Single-building-calibrated -- scale-relative radius
# is the follow-on.
_DEFAULT_TAG_RADIUS: float = 5.0


@service_takeoff_router.get(
    "/revisions/{revision_id}/service-takeoff",
    response_model=ServiceTakeoffResponse,
)
async def get_revision_service_takeoff(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    layer_refs: Annotated[list[str] | None, Query()] = None,
    tag_layers: Annotated[list[str] | None, Query()] = None,
    legend_layers: Annotated[list[str] | None, Query()] = None,
    scope: Annotated[ServiceTakeoffScope, Query()] = _DEFAULT_SCOPE,
    snap_tolerance: Annotated[float, Query(ge=0.0)] = 0.0,
    min_area: Annotated[float, Query(ge=0.0)] = 0.0,
    radius: Annotated[float, Query(gt=0.0)] = _DEFAULT_TAG_RADIUS,
) -> ServiceTakeoffResponse:
    """Compute the routed-service takeoff for a drawing revision (compute-on-read).

    Groups linework by (layer, colour), fuses pipe-tag text annotations, scopes runs to
    room polygons, and aggregates drawn + real-world lengths per (service, size, room)
    bucket. All inputs are loaded from the canonical entity store; nothing is persisted.

    Honest degradation: a revision with no adapter run, no pipe layers, or no confirmed
    scale returns 200 with empty or ``drawing_units_only`` lines and ``unscaled=True``.
    Unknown-service and unassigned-room lines appear in ``items`` and are counted in
    ``summary``. Never 500 on degenerate input.

    ``scope`` mirrors the rooms endpoint: ``sheet`` (default) restricts routed linework and
    room geometry to the printed sheet; ``modelspace`` interprets the full modelspace.
    ``radius`` is the tag-to-run association radius in metres.
    """
    manifest = await _get_active_revision_manifest_or_409(revision_id, db)
    exclude_off_sheet = scope == "sheet"

    # Step 1 -- load all takeoff inputs atomically.
    inputs = await load_service_takeoff_inputs(
        db,
        revision_id,
        layer_refs=layer_refs,
        tag_layers=tag_layers,
        legend_layers=legend_layers,
        exclude_off_sheet=exclude_off_sheet,
    )

    # Step 2 -- identify routed runs (P1).
    runs = identify_routed_runs(inputs.routed_entities, inputs.legend).groups

    # Step 2b -- load materialized centerline lengths + geometry (C0 lazy-materialization).
    measured_mapping, present_keys = await load_measured_lengths(db, revision_id)
    # LP2 (#654): persisted centerline polylines, used to distribute length per room by clipping.
    measured_geometry = await load_measured_geometry(db, revision_id)
    required_keys: set[tuple[str | None, str | None]] = {(g.layer_ref, g.colour_key) for g in runs}
    # Fully materialized only when every required group is present (and there is at
    # least one run -- an empty revision is trivially uninteresting but still provisional).
    fully_materialized: bool = bool(required_keys) and required_keys <= present_keys

    if not fully_materialized:
        # Lazy, deduped, best-effort background materialization on a separate session
        # (keeps the read snapshot intact; never degrades the read).
        await _enqueue_centerline_materialization(
            revision_id=revision_id,
            project_id=manifest.project_id,
            source_file_id=manifest.source_file_id,
        )

    # Step 3 -- fuse service identities from tags (P2).
    identities = fuse_run_service_identities(
        runs,
        inputs.geometry_by_entity_id,
        inputs.tag_placements,
        radius=radius,
    ).identities

    # Step 4 -- resolve rooms (reuses the rooms pipeline).
    room_result = await _resolve_rooms(
        db,
        revision_id,
        snap_tolerance=snap_tolerance,
        min_area=min_area,
        exclude_off_sheet=exclude_off_sheet,
    )

    # Step 5a -- cluster rise/drop symbols from loaded ARC+HATCH entities.
    rise_symbols = cluster_rise_drop_symbols(
        inputs.rise_entities, inputs.legend, kind=KIND_RISE
    ).symbols
    drop_symbols = cluster_rise_drop_symbols(
        inputs.drop_entities, inputs.legend, kind=KIND_DROP
    ).symbols

    # Step 5b -- compute takeoff (P3 coordinator).
    # Pass measured lengths when present; unmeasured groups fall back to naive entity-sum.
    result = compute_service_takeoff(
        runs=runs,
        identities=identities,
        geometry_by_entity_id=inputs.geometry_by_entity_id,
        rooms=room_result.rooms,
        scale=inputs.scale,
        rise_symbols=rise_symbols,
        drop_symbols=drop_symbols,
        measured_length_by_group=measured_mapping if measured_mapping else None,
        measured_geometry_by_group=measured_geometry if measured_geometry else None,
    )

    # Step 5c -- fill-colour attribution (DWG only; compute-on-read, Phase 1 / #663).
    # Only meaningful for DWG revisions that have Center Line entities. For PDF revisions
    # the HATCH fill bands are absent so attribution is honest-absent (None).
    fill_attribution: ServiceFillAttributionRead | None = None
    is_pdf = inputs.input_family == INPUT_FAMILY_PDF_VECTOR
    if not is_pdf:
        # Filter routed_entities to centerline-token layers, line entity type only.
        cl_tokens_lower = tuple(t.lower() for t in _DEFAULT_CENTERLINE_LAYER_TOKENS)
        centerline_segments: list[tuple[tuple[float, float], tuple[float, float]]] = []
        for ent in inputs.routed_entities:
            if ent.entity_type != "line":
                continue
            lr = (ent.layer_ref or "").lower()
            if not any(tok in lr for tok in cl_tokens_lower):
                continue
            geom = ent.geometry
            if not isinstance(geom, dict):
                continue
            s = _xy(geom.get("start"))
            e = _xy(geom.get("end"))
            if s is not None and e is not None:
                centerline_segments.append((s, e))

        if centerline_segments:
            fill_bands = await load_service_fill_bands(db, revision_id)
            # BUG FIX: removed the dead first compute_fill_attributed_lengths call that
            # was immediately overwritten; refine_shared_by_connectivity internally
            # recomputes verdicts via _segment_verdicts and is the authoritative result.
            raw_fill = refine_shared_by_connectivity(
                centerline_segments=centerline_segments,
                fill_bands=fill_bands,
            )
            fill_attribution = ServiceFillAttributionRead(
                per_colour=[
                    ServiceFillColourRead(
                        colour_key=fc.colour_key,
                        colour_index=fc.colour_index,
                        colour_rgb=fc.colour_rgb,
                        length_m=fc.length_m,
                    )
                    for fc in raw_fill.per_colour
                ],
                shared_length_m=raw_fill.shared_length_m,
                total_length_m=raw_fill.total_length_m,
                centerline_segment_count=raw_fill.centerline_segment_count,
            )

    # Step 5d -- tag-stack service attribution (DWG only; Phase 3 / #674).
    # PDF revisions lack HATCH fill bands so the matcher has no bundle geometry; honest-absent.
    # Tag service OVERRIDES legend discipline for routed colours (additive only — lengths
    # are never touched). Discipline from RunServiceIdentity is attached as context.
    tag_service_attribution: ServiceTagAttributionRead | None = None
    if not is_pdf:
        tag_texts = await load_tag_stack_texts(
            db,
            revision_id,
            input_family=inputs.input_family,
        )
        stack_headers = await load_stack_headers(
            db,
            revision_id,
            input_family=inputs.input_family,
        )
        bundle_bands = await load_bundle_bands_by_colour(
            db,
            revision_id,
            exclude_off_sheet=exclude_off_sheet,
            input_family=inputs.input_family,
        )
        tag_stack_result = assign_services_by_tag_stack(
            tags=tag_texts,
            headers=stack_headers,
            bundle_bands_by_colour=bundle_bands,
        )
        # Build a colour_key → discipline lookup from Step 3 identities for context.
        _discipline_by_colour: dict[str, str | None] = {
            ident.colour_key: ident.discipline
            for ident in identities
            if ident.colour_key is not None
        }
        tag_service_attribution = ServiceTagAttributionRead(
            per_colour=[
                ServiceTagColourRead(
                    colour_key=a.colour_key,
                    service=a.service,
                    sizes=list(a.sizes),
                    size_kind=a.size_kind,
                    discipline=_discipline_by_colour.get(a.colour_key),
                )
                for a in tag_stack_result.assignments
            ],
            unmatched_colour_keys=list(tag_stack_result.unmatched_colour_keys),
            matched_stack_count=tag_stack_result.matched_stack_count,
            ambiguous=tag_stack_result.ambiguous,
        )

    # Step 5e -- per-segment nearest-label type attribution (DWG only; #687).
    # Consumes the ALREADY-LOADED measured_geometry (flattened across all groups) and the
    # ALREADY-LOADED tag_placements. parse_tag filters non-tag prose so no pre-filter needed.
    # Honest-absent (None) when measured_geometry is empty — the CENTERLINE job is already
    # lazily enqueued above for unmaterialized revisions.
    segment_label_attribution: ServiceSegmentLabelAttributionRead | None = None
    if not is_pdf and measured_geometry:
        # Flatten all groups' polylines into one list.
        flat_polylines: list[tuple[tuple[float, float], ...]] = []
        for polylines in measured_geometry.values():
            flat_polylines.extend(polylines)

        # Build SegmentLabel list from parsed tag_placements (parse_tag is the content gate).
        seg_labels: list[SegmentLabel] = []
        for placement in inputs.tag_placements:
            obs = parse_tag(placement.text)
            if obs is None:
                continue
            seg_labels.append(
                SegmentLabel(
                    point=placement.point,
                    service=obs.service,
                    size_raw=obs.size.raw,
                    size_kind=obs.size.kind,
                )
            )

        raw_seg: SegmentLabelResult = compute_segment_label_lengths(
            centerline_polylines=flat_polylines,
            labels=seg_labels,
            nearest_max_m=_DEFAULT_TAG_RADIUS,
        )
        segment_label_attribution = ServiceSegmentLabelAttributionRead(
            per_service=[
                ServiceSegmentServiceRead(service=s.service, length_m=s.length_m)
                for s in raw_seg.per_service
            ],
            per_size=[
                ServiceSegmentSizeRead(
                    service=s.service,
                    size_raw=s.size_raw,
                    size_kind=s.size_kind,
                    length_m=s.length_m,
                )
                for s in raw_seg.per_size
            ],
            unknown_length_m=raw_seg.unknown_length_m,
            total_length_m=raw_seg.total_length_m,
            segment_count=raw_seg.segment_count,
        )

    # Step 6 -- adapt result to response (explicit kwargs, no from_attributes across frozen
    # dataclass boundary).
    # length_provisional reflects length TRUSTWORTHINESS by format: PDF is provisional
    # (double-line wall inflation), DWG is not (accurate via the Center Line layer, #616).
    # It is intentionally NOT tied to centerline materialization in C0: the passthrough
    # producer equals the naive entity-sum, so materializing it does not make length more
    # trustworthy. The materialization-aware provisional gate lands with a real producer
    # (#641/LP3), keyed on a bumped CURRENT_ALGO_VERSION.
    length_provisional = is_pdf

    items = []
    for line in result.lines:
        # For PDF revisions, SERVICE_UNKNOWN lengths are not trustworthy (background
        # linework inflates the figure). Suppress the metre/drawing-unit values while
        # keeping run_count and other non-length fields honest. real_length_m -> None
        # (explicit absence); drawing_length -> 0.0 because the schema field is non-nullable
        # (Field(..., ge=0.0)) -- the paired real_length_m=None is the "suppressed" signal.
        suppress_length = is_pdf and line.service == SERVICE_UNKNOWN
        items.append(
            ServiceTakeoffLineRead(
                service=line.service,
                size_raw=line.size_raw,
                size_kind=line.size_kind,
                discipline=line.discipline,
                room_id=line.room_id,
                room_name=line.room_name,
                room_number=line.room_number,
                drawing_length=0.0 if suppress_length else line.drawing_length,
                real_length_m=None if suppress_length else line.real_length_m,
                basis=line.basis,
                units_confidence=line.units_confidence,
                run_count=line.run_count,
                identity_status=line.identity_status,
                confidence=line.confidence,
                riser_count=line.riser_count,
                drop_count=line.drop_count,
                bundle=line.bundle,
            )
        )

    distinct_services = len({item.service for item in items})
    distinct_sizes = len({(item.service, item.size_raw) for item in items})
    distinct_rooms = len({item.room_id for item in items})

    scale_read = ServiceTakeoffScaleRead(
        units_confidence=inputs.scale.units_confidence,
        real_world_available=inputs.scale.real_world_available,
        contradicted=inputs.scale.contradicted,
        conversion_factor=inputs.scale.conversion_factor,
    )

    summary = ServiceTakeoffSummaryRead(
        services=distinct_services,
        sizes=distinct_sizes,
        rooms=distinct_rooms,
        lines=len(items),
        unassigned_runs=result.unassigned_run_count,
        unknown_service_runs=result.unknown_service_run_count,
        total_risers=result.total_risers,
        total_drops=result.total_drops,
    )

    return ServiceTakeoffResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        items=items,
        summary=summary,
        scale=scale_read,
        fill_attribution=fill_attribution,
        tag_service_attribution=tag_service_attribution,
        segment_label_attribution=segment_label_attribution,
        unscaled=result.unscaled,
        length_provisional=length_provisional,
    )
