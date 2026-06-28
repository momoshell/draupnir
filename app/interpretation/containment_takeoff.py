"""Per-containment-type routed length attributed by fill-hatch band (issue #755, Phase 752b).

Attributes the drawing's persisted centerline segments to containment-band polygons (HATCH
entities on tray/conduit/duct/… layers) by spatial overlap, producing per-containment-type
metres via compose-before-attribute: each ContainmentBand is resolved to a type token via
the ContainmentLegend, mapped to a synthetic FillBand, and fed to the existing
``compute_fill_attributed_lengths`` engine without any changes to that engine.

Sentinel convention
-------------------
- ``"__shared__"``   — engine sentinel (geometrically ambiguous segment, no clear winner).
  This sentinel lives inside ``compute_fill_attributed_lengths`` and flows through here.
- ``"__unmapped__"`` — our sentinel for a band whose legend entry is absent or whose
  containment_type is None (honest-absent: swatch present but no label).  A segment whose
  nearest band is unmapped falls into the ``None`` bucket (containment_type=None), NOT into
  the engine's shared bucket — they have distinct meanings.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.containment_legend import ContainmentLegend
from app.interpretation.segment_label_takeoff import (
    SegmentLabel,
    compute_segment_label_lengths,
)
from app.interpretation.service_fill_takeoff import FillBand, compute_fill_attributed_lengths

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Reserved token strings — MUST NOT collide with real containment_type values.
# ---------------------------------------------------------------------------

_TOKEN_SHARED: str = "__shared__"  # engine internal sentinel; pass-through
_TOKEN_UNMAPPED: str = "__unmapped__"  # honest-absent / legend miss

# Prefix applied when a real containment_type string literally equals a reserved token.
# Vanishingly unlikely; the guard prevents a silent collision.
_DISAMBIG_PREFIX: str = "__ct__"


def _type_to_token(containment_type: str | None) -> str:
    """Map a resolved containment type to a stable token for the fill engine.

    ``None`` (honest-absent) → ``_TOKEN_UNMAPPED``.
    A string that equals a reserved token → prefixed string to avoid collision.
    All other strings → used as-is (the string IS the token).
    """
    if containment_type is None:
        return _TOKEN_UNMAPPED
    if containment_type in (_TOKEN_SHARED, _TOKEN_UNMAPPED):
        return f"{_DISAMBIG_PREFIX}{containment_type}"
    return containment_type


def _token_to_type(token: str) -> str | None:
    """Reverse of ``_type_to_token``.

    ``_TOKEN_UNMAPPED``                  → ``None`` (honest-absent).
    ``_DISAMBIG_PREFIX + <reserved>``    → ``<reserved>`` (real type that collided with a
                                           sentinel; ONLY the two specifically-prefixed
                                           reserved tokens are stripped — exact match, so a
                                           real type merely *starting* with the prefix is
                                           passed through verbatim and the round-trip is lossless).
    Anything else                        → the token IS the containment_type string.
    """
    if token == _TOKEN_UNMAPPED:
        return None
    if token in (f"{_DISAMBIG_PREFIX}{_TOKEN_SHARED}", f"{_DISAMBIG_PREFIX}{_TOKEN_UNMAPPED}"):
        return token[len(_DISAMBIG_PREFIX) :]
    return token


# ---------------------------------------------------------------------------
# Public dataclasses (frozen interface contract)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ContainmentBand:
    """One HATCH polygon extracted from a containment layer (tray/duct/conduit/…)."""

    colour_key: str
    colour_index: int | None
    colour_rgb: str | None
    ring: tuple[tuple[float, float], ...]  # metres
    pattern_name: str  # "" when absent; NEVER None


@dataclass(frozen=True, slots=True)
class ContainmentTypeLength:
    """Attributed length for a single resolved containment type.

    ``containment_type=None`` is the honest-absent sentinel: a band was present in the
    geometry but either had no matching legend entry, or its legend entry had no label.
    It is NOT the same as the engine's ``__shared__`` (geometrically ambiguous) bucket.
    """

    containment_type: str | None  # None = honest-absent
    length_m: float
    member_colour_keys: tuple[str, ...]  # sorted, deduped
    member_pattern_names: tuple[str, ...]  # sorted, deduped


# Provenance marker for lengths recovered via the run-label mechanism.
BASIS_RUN_LABEL: str = "run_label"


@dataclass(frozen=True, slots=True)
class ContainmentLabelLength:
    """Attributed length recovered from an unmapped segment via its nearest run-label.

    Length the legend could not resolve is attributed here instead, with a distinct
    provenance marker so it is never silently merged into legend totals.
    """

    containment_type: str  # the run-label SERVICE token, e.g. "FA DECTN ALM"
    length_m: float
    basis: str  # provenance marker — always BASIS_RUN_LABEL


@dataclass(frozen=True, slots=True)
class ContainmentAttributionResult:
    """Result of containment-type attribution over the full centerline segment set.

    Primary invariant: Σ(per_type lengths) + shared_length_m == total_length_m ±0.1 m.
    Extended invariant (when labels supplied):
      Σ(real-type per_type) + Σ(label_attributed) + label_unknown_length_m
      + shared_length_m == total_length_m ±0.1 m.
    The ``None`` per_type entry, if present, has length_m == label_unknown_length_m.

    ``per_type`` is sorted: by containment_type (None sorts LAST, after all real strings).
    ``shared_length_m`` corresponds to the engine's ``__shared__`` bucket (geometrically
    ambiguous — equidistant or unattributable), NOT to the honest-absent (unmapped) bands.
    ``label_attributed``: sorted by containment_type; basis=BASIS_RUN_LABEL.
    ``label_unknown_length_m``: unmapped AND no in-cap label — honest-UNKNOWN.
    """

    per_type: tuple[ContainmentTypeLength, ...]
    shared_length_m: float
    total_length_m: float
    centerline_segment_count: int
    label_attributed: tuple[ContainmentLabelLength, ...] = ()
    label_unknown_length_m: float = 0.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_containment_attributed_lengths(
    *,
    centerline_segments: Sequence[tuple[tuple[float, float], tuple[float, float]]],
    containment_bands: Sequence[ContainmentBand],
    legend: ContainmentLegend,
    overlap_ratio_min: float = 0.30,
    nearest_max_m: float = 0.100,
    nearest_margin_m: float = 0.020,
    band_buffer_m: float = 0.011,
    labels: Sequence[SegmentLabel] = (),
    label_nearest_max_m: float = 5.0,
) -> ContainmentAttributionResult:
    """Attribute centerline segment lengths to containment-type bands.

    Compose-before-attribute pattern
    ---------------------------------
    1. For each ContainmentBand, resolve its containment type via
       ``legend.lookup(colour_key, pattern_name)``.
    2. Build a synthetic FillBand whose ``colour_key`` is a stable TOKEN encoding the
       resolved type (or ``_TOKEN_UNMAPPED`` for honest-absent / legend miss).
    3. Feed all synthetic FillBands to ``compute_fill_attributed_lengths``.
    4. Translate result tokens back to containment_type values.
    5. Fold bands sharing the SAME resolved type into one ContainmentTypeLength entry
       (lengths sum; member keys/patterns accumulated, sorted, deduped).
    6. (Optional) Run the label pass over segments that resolved to ``_TOKEN_UNMAPPED``:
       those segments are wrapped as 2-vertex polylines and fed to
       ``compute_segment_label_lengths``.  Recovered length moves into ``label_attributed``
       (basis=BASIS_RUN_LABEL); the residue stays in the ``None`` per_type bucket as
       ``label_unknown_length_m`` (honest-UNKNOWN).

    ``shared_length_m`` is the engine's geometric-ambiguity bucket (unchanged).
    The honest-absent bucket (``containment_type=None``) is a per_type entry.
    When ``labels`` is empty (default), ``label_attributed`` and ``label_unknown_length_m``
    are zero/empty and the result is byte-identical to pre-label behaviour.

    Empty inputs return an empty result without raising.

    Extended invariant (when labels supplied):
      Σ(real-type per_type) + Σ(label_attributed) + label_unknown_length_m
      + shared_length_m == total_length_m ±0.1 m.
    """
    if not centerline_segments:
        return ContainmentAttributionResult(
            per_type=(),
            shared_length_m=0.0,
            total_length_m=0.0,
            centerline_segment_count=0,
        )

    # --- Step 1+2: resolve each band → synthetic FillBand with token as colour_key ---
    # We also track which (colour_key, pattern_name) members map to each token
    # so we can populate member_colour_keys/member_pattern_names after attribution.
    synthetic_bands: list[FillBand] = []
    # token → set of (original_colour_key, pattern_name)
    token_members: dict[str, set[tuple[str, str]]] = {}

    for band in containment_bands:
        entry = legend.lookup(band.colour_key, band.pattern_name)
        ct = None if entry is None else entry.containment_type

        token = _type_to_token(ct)

        synthetic_bands.append(
            FillBand(
                colour_key=token,
                colour_index=band.colour_index,
                colour_rgb=band.colour_rgb,
                ring=band.ring,
            )
        )
        token_members.setdefault(token, set()).add((band.colour_key, band.pattern_name))

    # --- Step 3: delegate geometry computation to the existing engine ---
    fill_result = compute_fill_attributed_lengths(
        centerline_segments=centerline_segments,
        fill_bands=synthetic_bands,
        overlap_ratio_min=overlap_ratio_min,
        nearest_max_m=nearest_max_m,
        nearest_margin_m=nearest_margin_m,
        band_buffer_m=band_buffer_m,
    )

    # --- Step 4+5: translate tokens → types; fold same-type entries ---
    # type (or None) → accumulated length
    type_lengths: dict[str | None, float] = {}
    # type (or None) → accumulated members (mutable sets for dedup)
    type_member_ck: dict[str | None, set[str]] = {}
    type_member_pn: dict[str | None, set[str]] = {}

    for fill_entry in fill_result.per_colour:
        token = fill_entry.colour_key
        ct = _token_to_type(token)
        type_lengths[ct] = type_lengths.get(ct, 0.0) + fill_entry.length_m

        # Populate members from the pre-built token_members mapping.
        members = token_members.get(token, set())
        cks = type_member_ck.setdefault(ct, set())
        pns = type_member_pn.setdefault(ct, set())
        for ck_val, pn_val in members:
            cks.add(ck_val)
            pns.add(pn_val)

    # --- Step 6: label pass over unmapped segments (optional) ---
    # segment_attribution is aligned 1:1 with centerline_segments (including zero-length,
    # which carry None and contribute nothing to any length bucket).
    label_attributed: tuple[ContainmentLabelLength, ...] = ()
    label_unknown_length_m: float = 0.0

    unmapped_token = _type_to_token(None)  # == _TOKEN_UNMAPPED; never hardcode the string

    if labels:
        seg_attr = fill_result.segment_attribution
        # Collect exactly those input segments that the legend pass left unmapped.
        unmapped_segs: list[tuple[tuple[float, float], tuple[float, float]]] = [
            centerline_segments[i]
            for i in range(len(centerline_segments))
            if i < len(seg_attr) and seg_attr[i] == unmapped_token
        ]

        if unmapped_segs:
            # Wrap each (start, end) segment as a 2-vertex polyline for the label engine.
            unmapped_polylines: list[tuple[tuple[float, float], ...]] = [
                (start, end) for start, end in unmapped_segs
            ]
            label_result = compute_segment_label_lengths(
                centerline_polylines=unmapped_polylines,
                labels=labels,
                nearest_max_m=label_nearest_max_m,
            )

            label_attributed = tuple(
                sorted(
                    (
                        ContainmentLabelLength(
                            containment_type=svc.service,
                            length_m=svc.length_m,
                            basis=BASIS_RUN_LABEL,
                        )
                        for svc in label_result.per_service
                    ),
                    key=lambda e: e.containment_type,
                )
            )
            label_unknown_length_m = label_result.unknown_length_m

            # Reconcile the None per_type bucket: recovered length moves out of honest-absent
            # into label_attributed.  After the label pass, None bucket == label_unknown_length_m.
            if None in type_lengths:
                total_label_recovered = sum(e.length_m for e in label_attributed)
                original_unmapped = type_lengths[None]
                partition_err = total_label_recovered + label_unknown_length_m - original_unmapped
                if abs(partition_err) >= 0.1:
                    # Conservation violated beyond tolerance — route residual into unknown
                    # so the API response stays consistent; never crash a compute-on-read path.
                    _log.warning(
                        "containment label-pass partition error %.4f m "
                        "(recovered=%.4f unknown=%.4f unmapped=%.4f); "
                        "routing residual into label_unknown_length_m",
                        partition_err,
                        total_label_recovered,
                        label_unknown_length_m,
                        original_unmapped,
                    )
                    label_unknown_length_m = original_unmapped - total_label_recovered
                    # Clamp to zero — never emit negative unknown.
                    if label_unknown_length_m < 0.0:
                        label_unknown_length_m = 0.0
                type_lengths[None] = label_unknown_length_m

    # Sort per_type: real types alphabetically, None (honest-absent) LAST.
    def _sort_key(ct: str | None) -> str:
        return ct if ct is not None else chr(0x10FFFF)

    per_type: tuple[ContainmentTypeLength, ...] = tuple(
        ContainmentTypeLength(
            containment_type=ct,
            length_m=round(type_lengths[ct], 6),
            member_colour_keys=tuple(sorted(type_member_ck.get(ct, set()))),
            member_pattern_names=tuple(sorted(type_member_pn.get(ct, set()))),
        )
        for ct in sorted(type_lengths, key=_sort_key)
    )

    return ContainmentAttributionResult(
        per_type=per_type,
        shared_length_m=fill_result.shared_length_m,
        total_length_m=fill_result.total_length_m,
        centerline_segment_count=fill_result.centerline_segment_count,
        label_attributed=label_attributed,
        label_unknown_length_m=round(label_unknown_length_m, 6),
    )
