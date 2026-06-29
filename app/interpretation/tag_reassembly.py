"""Spatial pre-pass that reassembles fragmented drainage pipe-tag MTEXT (issue #795, D3a).

DWG exports often split a single pipe tag like ``∅100 SVP AT HL DROPS TB`` across three
separate MTEXT entities placed close together:

- a glyph fragment  ``∅`` (or garbled variants ``Ø``, U+FFFD ``�``, ``?``)
- a number fragment ``100``
- a service fragment ``SVP AT HL DROPS TB``

``parse_tag`` cannot resolve any of these individually.  This module clusters them by
proximity and concatenates them in the canonical order ``glyph + number + " " + service``
so the resulting string is accepted by ``parse_tag(strict_content=True)``.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass

from app.interpretation.run_service_identity import TagPlacement
from app.interpretation.run_tags import (
    _STANDARD_SERVICE_CODES,
    _is_annotation_prose,
    parse_tag,
)

# ---------------------------------------------------------------------------
# Fragment classification
# ---------------------------------------------------------------------------

# A bare integer representing a pipe size - 2 or 3 digits only.
# Single-digit strings (callout leaders: 1, 2, 3) and 4-digit strings are excluded
# because real pipe sizes are 15-150 mm.
_NUMBER_RE = re.compile(r"^\s*\d{2,3}\s*$")

# Canonical diameter glyph variants (the ones that arrive from non-UTF-8 DWG exports).
# U+FFFD (replacement char) and "?" are garbled forms of Ø.
_GLYPH_RE = re.compile(r"^[∅Ø�?]+$")

# Canonical glyph emitted in reassembled text — parse_tag recognises all variants via
# _DIAMETER_CONTEXT_RE but ∅ is the cleanest sentinel that also satisfies the context gate.
_CANONICAL_GLYPH = "∅"

# Tie-break epsilon: two candidates are treated as "equidistant" when their distances
# differ by less than this (metres).  Calibrated to sub-millimetre drawing noise.
_TIE_EPSILON: float = 1e-6

# ---------------------------------------------------------------------------
# Fragment kind constants
# ---------------------------------------------------------------------------

_KIND_NUMBER = "NUMBER"
_KIND_GLYPH = "GLYPH"
_KIND_SERVICE = "SERVICE"
_KIND_NOISE = "NOISE"


def _classify(text: str, allowed: frozenset[str]) -> str:
    """Classify a single placement text into one of the four fragment kinds.

    Returns ``_KIND_NOISE`` for anything that is already accepted by
    ``parse_tag(strict_content=True)`` — intact tags must pass through untouched.

    ``allowed`` is the vocabulary of recognised service codes — either the per-sheet
    legend abbreviations (ground-truth) or ``_STANDARD_SERVICE_CODES`` (fallback).
    A fragment is SERVICE iff its first whitespace-token, uppercased, is a member of
    ``allowed``.
    """
    # Non-regression invariant: if parse_tag already accepts the text, treat as NOISE
    # so the placement survives as-is without being consumed as a fragment.
    if parse_tag(text, strict_content=True) is not None:
        return _KIND_NOISE

    stripped = text.strip()

    if _NUMBER_RE.match(stripped):
        return _KIND_NUMBER

    if _GLYPH_RE.match(stripped):
        return _KIND_GLYPH

    # SERVICE: the FIRST token, uppercased, must be a member of the allowed vocabulary.
    # Membership subsumes both shape and word checks — short English words like DEEP/PAN
    # are rejected because they are absent from the set.
    if stripped and not stripped[0].isdigit():
        normalized = " ".join(stripped.split()).upper()
        if _is_annotation_prose(normalized):
            return _KIND_NOISE
        tokens = normalized.split()
        first = tokens[0] if tokens else ""
        if first in allowed:
            return _KIND_SERVICE

    return _KIND_NOISE


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReassemblyResult:
    """Output of :func:`reassemble_tag_fragments`."""

    placements: tuple[TagPlacement, ...]
    reassembled_count: int
    rejected_cluster_count: int


# ---------------------------------------------------------------------------
# Distance helper
# ---------------------------------------------------------------------------


def _dist(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def reassemble_tag_fragments(
    placements: Sequence[TagPlacement],
    *,
    cluster_radius_m: float = 0.5,
    legend_abbreviations: frozenset[str] | None = None,
) -> ReassemblyResult:
    """Reassemble spatially fragmented pipe-tag MTEXT into single parseable strings.

    Each NUMBER fragment anchors a cluster.  The algorithm finds the nearest GLYPH and
    nearest SERVICE within ``cluster_radius_m`` (Euclidean on point x/y).  Each fragment
    is used at most once.  Ties break deterministically by ``(distance, input_index, text)``.

    Ambiguous clusters (two equidistant GLYPH or SERVICE candidates within ``_TIE_EPSILON``)
    are rejected and counted in ``rejected_cluster_count``.

    After clustering, each candidate triple is validated with
    ``parse_tag(reassembled, strict_content=True)``; clusters that still don't parse are also
    rejected and counted.

    ``legend_abbreviations``, when non-empty, is the per-sheet legend vocabulary and overrides
    the ``_STANDARD_SERVICE_CODES`` fallback entirely (legend is ground truth).

    Every placement not consumed is passed through unchanged.
    """
    # Resolve the allowed-service vocabulary once for the whole call.
    allowed: frozenset[str] = (
        legend_abbreviations if legend_abbreviations else _STANDARD_SERVICE_CODES
    )

    # Classify all placements once.
    kinds = [_classify(p.text, allowed) for p in placements]

    # Build typed index lists.
    numbers: list[int] = []
    glyphs: list[int] = []
    services: list[int] = []

    for idx, kind in enumerate(kinds):
        if kind == _KIND_NUMBER:
            numbers.append(idx)
        elif kind == _KIND_GLYPH:
            glyphs.append(idx)
        elif kind == _KIND_SERVICE:
            services.append(idx)

    consumed: set[int] = set()
    emitted: list[TagPlacement] = []
    reassembled_count = 0
    rejected_cluster_count = 0

    for num_idx in numbers:
        num_pt = placements[num_idx].point

        # --- find nearest GLYPH within radius ---
        glyph_candidates: list[tuple[float, int, str]] = []
        for g_idx in glyphs:
            if g_idx in consumed:
                continue
            d = _dist(num_pt, placements[g_idx].point)
            if d <= cluster_radius_m:
                glyph_candidates.append((d, g_idx, placements[g_idx].text))

        # --- find nearest SERVICE within radius ---
        svc_candidates: list[tuple[float, int, str]] = []
        for s_idx in services:
            if s_idx in consumed:
                continue
            d = _dist(num_pt, placements[s_idx].point)
            if d <= cluster_radius_m:
                svc_candidates.append((d, s_idx, placements[s_idx].text))

        # Sort deterministically: (distance, input_index, text).
        glyph_candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        svc_candidates.sort(key=lambda x: (x[0], x[1], x[2]))

        # Ambiguity check: reject if two candidates are within epsilon of each other.
        def _is_ambiguous(candidates: list[tuple[float, int, str]]) -> bool:
            if len(candidates) < 2:
                return False
            return abs(candidates[0][0] - candidates[1][0]) < _TIE_EPSILON

        if _is_ambiguous(glyph_candidates) or _is_ambiguous(svc_candidates):
            rejected_cluster_count += 1
            continue

        best_glyph: tuple[float, int, str] | None = (
            glyph_candidates[0] if glyph_candidates else None
        )
        best_svc: tuple[float, int, str] | None = svc_candidates[0] if svc_candidates else None

        # Need at least one companion to form a useful cluster; a lone NUMBER is noise.
        if best_glyph is None and best_svc is None:
            continue

        # Build reassembled text: glyph + number + " " + service (order is critical).
        glyph_str = _CANONICAL_GLYPH if best_glyph is not None else ""
        num_str = placements[num_idx].text.strip()
        svc_str = best_svc[2].strip() if best_svc is not None else ""

        if glyph_str and svc_str:
            reassembled = f"{glyph_str}{num_str} {svc_str}"
        elif glyph_str:
            reassembled = f"{glyph_str}{num_str}"
        else:
            reassembled = f"{num_str} {svc_str}"

        # Validate via parse_tag.
        obs = parse_tag(reassembled, strict_content=True)
        if obs is None:
            rejected_cluster_count += 1
            continue

        # Determine point: prefer SERVICE fragment's point, else NUMBER's point.
        point = placements[best_svc[1]].point if best_svc is not None else num_pt
        layer_ref = placements[num_idx].layer_ref

        emitted.append(TagPlacement(text=reassembled, point=point, layer_ref=layer_ref))
        reassembled_count += 1

        # Mark all three as consumed.
        consumed.add(num_idx)
        if best_glyph is not None:
            consumed.add(best_glyph[1])
        if best_svc is not None:
            consumed.add(best_svc[1])

    # Pass through every placement not consumed.
    passthrough = [p for idx, p in enumerate(placements) if idx not in consumed]

    return ReassemblyResult(
        placements=tuple(emitted + passthrough),
        reassembled_count=reassembled_count,
        rejected_cluster_count=rejected_cluster_count,
    )
