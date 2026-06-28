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

Basis values on ContainmentTypeLength
--------------------------------------
- ``"legend"``     — entry resolved via legend (colour, pattern) lookup.
- ``"run_label"``  — entry recovered via the run-label mechanism (label pass over unmapped
                     segments).
- ``"unresolved"`` — honest-absent entry (containment_type=None): band present but no
                     matching legend entry or legend entry has no label.

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

    ``basis`` encodes how the attribution was determined:
      - ``"legend"``     — resolved via legend (colour, pattern) lookup.
      - ``"run_label"``  — recovered via the run-label mechanism (label pass).
      - ``"unresolved"`` — honest-absent (containment_type=None): band present but unmapped.
    """

    containment_type: str | None  # None = honest-absent
    length_m: float
    member_colour_keys: tuple[str, ...]  # sorted, deduped
    member_pattern_names: tuple[str, ...]  # sorted, deduped
    basis: str = "legend"  # provenance marker; default for backwards-compat construction


# Provenance marker constants.
BASIS_LEGEND: str = "legend"
BASIS_RUN_LABEL: str = "run_label"
BASIS_UNRESOLVED: str = "unresolved"


@dataclass(frozen=True, slots=True)
class ContainmentLabelLength:
    """Attributed length recovered from an unmapped segment via its nearest run-label.

    Kept for internal use within compute_containment_attributed_lengths; NOT exposed in
    ContainmentAttributionResult — label entries are folded into per_type (basis=run_label).
    """

    containment_type: str  # the run-label SERVICE token, e.g. "FA DECTN ALM"
    length_m: float
    basis: str  # provenance marker — always BASIS_RUN_LABEL


@dataclass(frozen=True, slots=True)
class ContainmentAttributionResult:
    """Result of containment-type attribution over the full centerline segment set.

    Extended invariant: Σ(per_type lengths, all bases) + label_unknown_length_m
      + shared_length_m == total_length_m ±0.1 m.
    The ``None`` per_type entry (basis="unresolved"), if present, has
    length_m == label_unknown_length_m.

    ``per_type`` contains ALL attributed entries — both legend-resolved (basis="legend") and
    label-recovered (basis="run_label") — sorted deterministically: by basis then
    containment_type (None sorts LAST within its basis group).
    ``shared_length_m`` corresponds to the engine's ``__shared__`` bucket (geometrically
    ambiguous — equidistant or unattributable), NOT to the honest-absent (unmapped) bands.
    ``label_unknown_length_m``: unmapped AND no in-cap label — honest-UNKNOWN.
    """

    per_type: tuple[ContainmentTypeLength, ...]
    shared_length_m: float
    total_length_m: float
    centerline_segment_count: int
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
       (lengths sum; member keys/patterns accumulated, sorted, deduped).  These entries
       carry basis=BASIS_LEGEND.
    6. (Optional) Run the label pass over segments that resolved to ``_TOKEN_UNMAPPED``:
       those segments are wrapped as 2-vertex polylines and fed to
       ``compute_segment_label_lengths``.  Recovered entries are folded INTO ``per_type``
       with basis=BASIS_RUN_LABEL (see step 7); the residue stays in the ``None`` per_type
       bucket as ``label_unknown_length_m`` (honest-UNKNOWN).
    7. F2 — merge fragmented label families: when folding label entries into per_type,
       merge a longer label-service into a shorter one when the shorter is a whole-word
       prefix of the longer AND the shorter also appears as a label family in the same
       label pass (e.g. "LIFE SAFETY FB" → "LIFE SAFETY" because "LIFE SAFETY" is present).
       Cross-basis merging is NEVER done (legend "ESSENTIAL TRUNKING" and label "ESSENTIAL"
       remain distinct entries).
    8. per_type is sorted deterministically: by (basis, containment_type, None last).

    ``shared_length_m`` is the engine's geometric-ambiguity bucket (unchanged).
    The honest-absent bucket (``containment_type=None``, basis=BASIS_UNRESOLVED) is a
    per_type entry.
    When ``labels`` is empty (default), no label entries appear in per_type and
    ``label_unknown_length_m`` is zero; the result is byte-identical to pre-label behaviour
    except that legend entries now carry basis=BASIS_LEGEND.

    Empty inputs return an empty result without raising.

    Extended invariant:
      Σ(per_type lengths, all bases) + label_unknown_length_m
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
    label_entries_raw: list[ContainmentLabelLength] = []
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

            label_entries_raw = [
                ContainmentLabelLength(
                    containment_type=svc.service,
                    length_m=svc.length_m,
                    basis=BASIS_RUN_LABEL,
                )
                for svc in label_result.per_service
            ]
            label_unknown_length_m = label_result.unknown_length_m

            # Reconcile the None per_type bucket: recovered length moves out of honest-absent
            # into label entries.  After the label pass, None bucket == label_unknown_length_m.
            if None in type_lengths:
                total_label_recovered = sum(e.length_m for e in label_entries_raw)
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

    # --- Step 7: F2 — merge fragmented label families (run_label basis only) ---
    # Merge a longer label-service into a shorter one when the shorter is a whole-word
    # prefix of the longer AND the shorter also appears as its own label family.
    # Example: "LIFE SAFETY FB" → "LIFE SAFETY" because "LIFE SAFETY" is present as a
    # separate label entry.  Cross-basis merging is never done — a legend entry named
    # "ESSENTIAL TRUNKING" and a label entry "ESSENTIAL" remain distinct.
    if label_entries_raw:
        label_services: set[str] = {lbl.containment_type for lbl in label_entries_raw}
        # Build a merged accumulator: service → total length.
        merged_label: dict[str, float] = {}
        for lbl in label_entries_raw:
            # Find the longest shorter whole-word prefix that is itself a label family.
            # "Whole-word prefix" means the prefix is followed by a space in the longer name.
            target: str = lbl.containment_type
            for candidate in sorted(label_services, key=len, reverse=True):
                if (
                    candidate != target
                    and len(candidate) < len(target)
                    and target.startswith(candidate + " ")
                    and candidate in label_services
                ):
                    # Merge this longer entry into the shorter prefix family.
                    target = candidate
                    break
            merged_label[target] = merged_label.get(target, 0.0) + lbl.length_m

        label_entries_raw = [
            ContainmentLabelLength(
                containment_type=svc,
                length_m=length_m,
                basis=BASIS_RUN_LABEL,
            )
            for svc, length_m in merged_label.items()
        ]

    # --- Step 8: build final per_type: legend entries + folded label entries ---
    # Legend entries carry basis=BASIS_LEGEND (or BASIS_UNRESOLVED for the None bucket).
    # Label entries carry basis=BASIS_RUN_LABEL.
    # Sort deterministically: (basis, containment_type), None sorts LAST within its group.
    # Sort order: legend first, then run_label (folded label entries), then unresolved (None)
    # last so the honest-absent bucket remains the last entry regardless of type name.
    _basis_sort_order = {BASIS_LEGEND: 0, BASIS_RUN_LABEL: 1, BASIS_UNRESOLVED: 2}

    def _sort_key(entry: ContainmentTypeLength) -> tuple[int, str]:
        basis_order = _basis_sort_order.get(entry.basis, 99)
        ct_sort = entry.containment_type if entry.containment_type is not None else chr(0x10FFFF)
        return (basis_order, ct_sort)

    legend_per_type: list[ContainmentTypeLength] = [
        ContainmentTypeLength(
            containment_type=ct,
            length_m=round(type_lengths[ct], 6),
            member_colour_keys=tuple(sorted(type_member_ck.get(ct, set()))),
            member_pattern_names=tuple(sorted(type_member_pn.get(ct, set()))),
            basis=BASIS_UNRESOLVED if ct is None else BASIS_LEGEND,
        )
        for ct in type_lengths
    ]

    label_per_type: list[ContainmentTypeLength] = [
        ContainmentTypeLength(
            containment_type=e.containment_type,
            length_m=round(e.length_m, 6),
            member_colour_keys=(),
            member_pattern_names=(),
            basis=BASIS_RUN_LABEL,
        )
        for e in label_entries_raw
    ]

    per_type: tuple[ContainmentTypeLength, ...] = tuple(
        sorted(legend_per_type + label_per_type, key=_sort_key)
    )

    return ContainmentAttributionResult(
        per_type=per_type,
        shared_length_m=fill_result.shared_length_m,
        total_length_m=fill_result.total_length_m,
        centerline_segment_count=fill_result.centerline_segment_count,
        label_unknown_length_m=round(label_unknown_length_m, 6),
    )
