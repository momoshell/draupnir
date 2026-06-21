"""Device / fixture-schedule interpretation route (tier-3, derived from the canonical model)."""

from collections import Counter
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.pagination import DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE
from app.api.pagination import MAX_PAGE_SIZE as _MAX_PAGE_SIZE
from app.api.pagination import (
    decode_cursor_payload,
    encode_cursor_payload,
    read_cursor_int,
)
from app.api.v1.revision_lineage import (
    _get_active_revision,
    _get_active_revision_manifest_or_409,
)
from app.core.exceptions import raise_not_found
from app.db.session import get_db
from app.interpretation.device_identity import (
    BASIS_NONE,
    KIND_ARCHITECTURE,
    KIND_DEVICE,
    KIND_LEGEND_EXEMPLAR,
    KIND_UNKNOWN,
    STATUS_UNKNOWN,
    DeviceIdentity,
    resolve_device_identities,
)
from app.interpretation.devices import (
    attach_tags,
    enumerate_devices,
    load_tag_candidates,
    schedule_from_devices,
)
from app.interpretation.layer_roles import RULE_VERSION, classify_layer_role
from app.interpretation.legend import resolve_legend_devices, schedule_from_legend_devices
from app.interpretation.legend_dictionary import (
    FamilyInput,
    ProseInput,
    TagInput,
    from_block_families,
    from_prose_schedule,
    from_tag_layers,
    fuse,
)
from app.interpretation.loaders import load_adapter_text_blocks, load_legend_text_candidates
from app.models.revision_materialization import RevisionLayer
from app.schemas.devices import (
    DeviceRead,
    DeviceScheduleEntry,
    DeviceSemanticsRead,
    LegendDeviceRead,
    LegendDeviceScheduleEntry,
    RevisionDeviceListResponse,
    RevisionLegendDeviceListResponse,
)
from app.schemas.layer_roles import LayerRoleRead, RevisionLayerRoleListResponse
from app.schemas.revision import RevisionEntityManifestRead

devices_router = APIRouter()

_MAX_NESTING_DEPTH = 8

_VALID_KIND_VALUES = {"all", "device", "architecture"}


@devices_router.get(
    "/revisions/{revision_id}/devices",
    response_model=RevisionDeviceListResponse,
)
async def list_revision_devices(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
    device_layer: Annotated[list[str] | None, Query()] = None,
    tag_layer: Annotated[list[str] | None, Query()] = None,
    max_tag_distance: Annotated[float | None, Query(gt=0.0)] = None,
    max_depth: Annotated[int, Query(ge=0, le=_MAX_NESTING_DEPTH)] = _MAX_NESTING_DEPTH,
    kind: Annotated[str, Query()] = "all",
) -> RevisionDeviceListResponse:
    """List device instances (the block-instance tree) with nearest-tag association + schedule.

    Devices are enumerated by walking placed blocks down to ``max_depth`` (0 = top-level INSERTs
    only), composing placement transforms so each instance gets a world position; the ``schedule``
    aggregates counts by block reference across all enumerated instances. Each device is matched to
    the nearest tag text (default: layers whose name contains ``tag``/``device``; falls back to any
    text) within an optional ``max_tag_distance``.

    ``kind`` filters returned items: ``device`` (fire/BMS/etc.), ``architecture`` (doors/windows),
    or ``all`` (default, includes every kind). Invalid values return 422. Legend summary and
    schedule_by_type always reflect the full resolved set.
    """
    if kind not in _VALID_KIND_VALUES:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=422,
            detail=f"kind must be one of {sorted(_VALID_KIND_VALUES)!r}; got {kind!r}",
        )

    manifest = await _get_active_revision_manifest_or_409(revision_id, db)

    # Enumerate the full device set (pre-page) and build the raw schedule.
    devices = await enumerate_devices(
        db, revision_id, device_layers=device_layer, max_depth=max_depth
    )
    schedule = schedule_from_devices(devices)

    # Build tag candidates over the full revision.
    candidates = await load_tag_candidates(db, revision_id, tag_layers=tag_layer)

    # Attach tags to ALL devices (needed for legend + identity resolution).
    all_tagged = attach_tags(devices, candidates, max_distance=max_tag_distance)

    # Build the legend over the FULL device set.
    families = [FamilyInput(family_name=d.block_ref) for d in all_tagged if d.block_ref]
    prose_texts = await load_legend_text_candidates(db, revision_id)
    prose = [ProseInput(text=t) for t in prose_texts]
    # Source C: distinct tag tokens from ALL tag candidates (not just associated ones).
    tag_tokens = list({c.text for c in candidates})
    tags = [TagInput(token=t) for t in tag_tokens]
    legend = fuse(
        [*from_block_families(families), *from_prose_schedule(prose), *from_tag_layers(tags)]
    )

    # Resolve identity for every device instance.
    identities = resolve_device_identities(all_tagged, legend)
    identity_by_id: dict[str, DeviceIdentity] = {i.entity_id: i for i in identities}

    # Build the legend summary.
    legend_entries = legend.entries
    sources_seen: set[str] = set()
    for entry in legend_entries:
        sources_seen.update(entry.sources)
    resolved_count = sum(1 for i in identities if i.kind == KIND_DEVICE and i.status == "resolved")
    unresolved_count = sum(
        1 for i in identities if i.kind == KIND_DEVICE and i.status != "resolved"
    )
    legend_summary = {
        "legend_size": len(legend_entries),
        "sources": sorted(sources_seen),
        "resolved_count": resolved_count,
        "unresolved_count": unresolved_count,
    }

    # Build schedule_by_type — EXCLUDES legend_exemplar from device buckets.
    type_counts: Counter[str] = Counter()
    arch_count = 0
    for identity in identities:
        if identity.kind == KIND_LEGEND_EXEMPLAR:
            # Kept in items but excluded from the device count denominator.
            continue
        if identity.kind == KIND_ARCHITECTURE:
            arch_count += 1
        elif identity.kind == KIND_DEVICE:
            bucket = identity.type_name if identity.type_name else "unresolved"
            type_counts[bucket] += 1
        else:
            # annotation/unknown land in unresolved bucket.
            type_counts["unresolved"] += 1

    schedule_by_type: list[dict[str, object]] = []
    for type_name, count in sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        schedule_by_type.append({"type_name": type_name, "count": count})
    if arch_count:
        schedule_by_type.append({"type_name": "architecture", "count": arch_count})

    # Paginate AFTER tag attach (all_tagged is the full tagged list).
    offset = read_cursor_int(decode_cursor_payload(cursor), "offset") if cursor else 0

    # Apply kind filter before pagination.
    if kind == "all":
        filtered = all_tagged
    elif kind == "device":
        filtered = [
            d
            for d in all_tagged
            if identity_by_id.get(d.entity_id) and identity_by_id[d.entity_id].kind == KIND_DEVICE
        ]
    else:  # architecture
        filtered = [
            d
            for d in all_tagged
            if identity_by_id.get(d.entity_id)
            and identity_by_id[d.entity_id].kind == KIND_ARCHITECTURE
        ]

    page = filtered[offset : offset + limit]
    next_offset = offset + limit
    next_cursor = (
        encode_cursor_payload({"offset": next_offset}) if next_offset < len(filtered) else None
    )

    # Build DeviceRead items with semantics attached.
    items: list[DeviceRead] = []
    for device in page:
        resolved: DeviceIdentity = identity_by_id.get(device.entity_id) or DeviceIdentity(
            entity_id=device.entity_id,
            kind=KIND_UNKNOWN,
            status=STATUS_UNKNOWN,
            type_name=None,
            abbreviation=None,
            description=None,
            basis=BASIS_NONE,
            source_layers=(),
            confidence=None,
            competing_type_names=(),
        )
        semantics = DeviceSemanticsRead(
            entity_id=resolved.entity_id,
            kind=resolved.kind,
            status=resolved.status,
            type_name=resolved.type_name,
            abbreviation=resolved.abbreviation,
            description=resolved.description,
            basis=resolved.basis,
            source_layers=list(resolved.source_layers),
            confidence=resolved.confidence,
            competing_type_names=list(resolved.competing_type_names),
        )
        items.append(
            DeviceRead(
                entity_id=device.entity_id,
                sequence_index=device.sequence_index,
                depth=device.depth,
                block_ref=device.block_ref,
                layer_ref=device.layer_ref,
                position=device.position,
                tag=device.tag,  # type: ignore[arg-type]
                semantics=semantics,
            )
        )

    return RevisionDeviceListResponse(
        manifest=RevisionEntityManifestRead.model_validate(manifest),
        schedule=[DeviceScheduleEntry(**entry) for entry in schedule],
        items=items,
        next_cursor=next_cursor,
        association={
            "tag_layers": tag_layer,
            "device_layers": device_layer,
            "max_tag_distance": max_tag_distance,
            "max_depth": max_depth,
            "total_devices": len(devices),
            "default_tag_layer_tokens": ["tag", "device"],
        },
        legend=legend_summary,
        schedule_by_type=schedule_by_type,
        kind=kind,
    )


@devices_router.get(
    "/revisions/{revision_id}/legend-devices",
    response_model=RevisionLegendDeviceListResponse,
)
async def list_revision_legend_devices(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    cursor: str | None = Query(default=None),
) -> RevisionLegendDeviceListResponse:
    """List devices located by resolving drawing-body tags against the legend (vector PDFs).

    The legend is parsed into a symbol dictionary (abbreviation -> device type); body text blocks
    whose text matches an abbreviation are emitted as located, typed devices, and ``schedule``
    aggregates counts per type. Revisions without a parseable legend return an empty schedule.
    """

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None

    text_blocks = await load_adapter_text_blocks(db, revision)
    dictionary, devices = resolve_legend_devices(text_blocks)
    schedule = schedule_from_legend_devices(devices)

    offset = read_cursor_int(decode_cursor_payload(cursor), "offset") if cursor else 0
    page = devices[offset : offset + limit]
    next_offset = offset + limit
    next_cursor = (
        encode_cursor_payload({"offset": next_offset}) if next_offset < len(devices) else None
    )

    return RevisionLegendDeviceListResponse(
        schedule=[LegendDeviceScheduleEntry(**entry) for entry in schedule],
        items=[
            LegendDeviceRead(
                abbreviation=device.abbreviation,
                type_name=device.type_name,
                position={"x": device.x, "y": device.y},
            )
            for device in page
        ],
        next_cursor=next_cursor,
        summary={
            "legend_size": len(dictionary),
            "total_devices": len(devices),
        },
    )


@devices_router.get(
    "/revisions/{revision_id}/layer-roles",
    response_model=RevisionLayerRoleListResponse,
)
async def list_revision_layer_roles(
    revision_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> RevisionLayerRoleListResponse:
    """Derive a coarse semantic role per layer (background / foreground / services / unknown).

    A deterministic, versioned rule table classifies pen-signature layers by colour lightness and
    saturation. The role is attached on top of the stable layer identity (it never renames a
    layer); non-pen layers (DWG / OCG) are reported as ``unknown``.
    """

    revision = await _get_active_revision(revision_id, db)
    if revision is None:
        raise_not_found("Drawing revision", str(revision_id))
    assert revision is not None

    rows = (
        await db.scalars(
            select(RevisionLayer)
            .where(RevisionLayer.drawing_revision_id == revision_id)
            .order_by(RevisionLayer.sequence_index.asc(), RevisionLayer.id.asc())
        )
    ).all()

    items: list[LayerRoleRead] = []
    counts: Counter[str] = Counter()
    for row in rows:
        name = row.payload_json.get("name") if isinstance(row.payload_json, dict) else None
        role = classify_layer_role(name if isinstance(name, str) else None)
        counts[role.role] += 1
        items.append(
            LayerRoleRead(
                layer_ref=row.layer_ref,
                name=name if isinstance(name, str) else None,
                role=role.role,
                basis=role.basis,
            )
        )

    return RevisionLayerRoleListResponse(
        items=items,
        rule_version=RULE_VERSION,
        summary={"counts": dict(counts)},
    )
