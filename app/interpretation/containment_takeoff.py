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

from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.containment_legend import ContainmentLegend
from app.interpretation.service_fill_takeoff import FillBand, compute_fill_attributed_lengths

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


@dataclass(frozen=True, slots=True)
class ContainmentAttributionResult:
    """Result of containment-type attribution over the full centerline segment set.

    INVARIANT: Σ(per_type lengths) + shared_length_m == total_length_m within ±0.1 m.
    ``per_type`` is sorted: by containment_type (None sorts LAST, after all real strings).
    ``shared_length_m`` corresponds to the engine's ``__shared__`` bucket (geometrically
    ambiguous — equidistant or unattributable), NOT to the honest-absent (unmapped) bands.
    """

    per_type: tuple[ContainmentTypeLength, ...]
    shared_length_m: float
    total_length_m: float
    centerline_segment_count: int


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

    ``shared_length_m`` is the engine's geometric-ambiguity bucket (unchanged).
    The honest-absent bucket (``containment_type=None``) is a per_type entry.

    Empty inputs return an empty result without raising.
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
    )
