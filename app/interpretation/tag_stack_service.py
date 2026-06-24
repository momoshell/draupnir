"""Tag-stack fingerprint matcher: service+size assignment from Pipe-Tag stacks (#674 Phase 3).

Derives per-routed-colour {service, sizes[]} by:
1. Clustering Pipe-Tag text into ordered stacks oriented by a header annotation.
2. Sweeping a scan line through ALL colour bands (globally) to find cross-sections
   that reproduce the stack's service run-length fingerprint.
3. Requiring STABLE (>=2 adjacent candidate positions) and CONSISTENT (same mapping
   across all matches) results before committing any assignment.

Identity-only — produces NO lengths.

**Honest fallback:** never guess; ambiguity -> colour stays opaque (unassigned).
All abstain cases are enforced:
  (1) stack has no header in range;
  (2) count-sequence length differs or any run count differs;
  (3) non-unique mapping (different scan positions yield conflicting colour->service);
  (4) one bundle matches >=2 stacks (non-unique stack match);
  (5) degenerate/non-discriminating fingerprint (total runs < 2, OR all runs same
      service, OR single-run [N]);
  (6) no scan position matched;
  (7) match is not stable (only one isolated scan position matched).

Partial assignment within a mismatched pairing is NOT allowed (all-or-nothing per
stack/bundle). NEVER assign on a guess.

**Scan-line approach (real-data verified on M-540003):**
The matcher receives ALL bands per colour globally (e.g. orange may have hundreds of
bands across the whole drawing). It sweeps vertical or horizontal scan lines at
candidate positions derived from band bbox midpoints. At each position it collects
bands whose bbox spans the line, orders them by perpendicular coordinate, and
GAP-SEGMENTS them into contiguous bundle clusters (any perpendicular gap >
BUNDLE_GAP_TOL_M starts a new cluster). Each cluster's colour sequence is
RLE-encoded independently and checked against the stack's service count sequence.
This prevents the scan line from reading the whole drawing column as one bundle.

**Composition note:** this module emits service+size ONLY (no legend read, no
discipline, no length field). The route composes tag-service OVER legend-discipline:
tag wins for routed colours that appear in both a pipe-tag stack and the drawing legend.

Pure module -- NO DB, ORM, FastAPI, SQLAlchemy, cv2, skimage, matplotlib, or numpy.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from app.interpretation.run_tags import PipeSize, parse_tag

# ---------------------------------------------------------------------------
# Spatial tolerances (all in METRES; geometry pre-scaled, conversion_factor=1.0)
# ---------------------------------------------------------------------------

# Stack axis clustering: tags sharing this cross-axis distance are in the same stack.
# Spike basis: observed pipe-tag columns cluster within ~0.05 m on A0 sheet scans.
STACK_AXIS_TOL_M: float = 0.10

# Bundle level merge: adjacent same-colour bands within this perpendicular gap are
# merged into one level. Spike basis: colour bands at bundle cross-sections can be
# separated by sub-pixel gaps of ~0.01-0.02 m in PDF fills.
LEVEL_MERGE_TOL_M: float = 0.03

# Header association: maximum distance from the nearest stack tag to a StackHeader
# for that header to be considered the orientation reference.
# Spike basis: on A0/A1 sheets a header sits within ~0.5 m of the nearest tag in
# its stack; 1.0 m is generous but avoids cross-drawing associations.
HEADER_MAX_DIST_M: float = 1.0

# Stability requirement: a match must appear at >= this many candidate scan positions
# to be considered stable. A single isolated position is too fragile.
# Spike basis: M-540003 scan lines at x=44 and x=45 both reproduce [1,4,4].
MIN_STABLE_MATCH_COUNT: int = 2

# Candidate scan position deduplication: nearby midpoints along the scan axis are
# collapsed to one candidate to avoid redundant computation.
SCAN_DEDUP_TOL_M: float = 0.03

# Bundle gap segmentation: a perpendicular gap larger than this between consecutive
# crossing bands splits the scan line into separate bundle segments. Each segment
# is matched independently against the stack fingerprint.
# Spike basis: within M-540003 bundle, adjacent pipe-level spacing is ~0.07-0.11 m;
# the inter-bundle gap to the next service cluster is 7.85 m. 0.5 m cleanly
# separates bundles without splitting adjacent pipe levels.
BUNDLE_GAP_TOL_M: float = 0.5

# ---------------------------------------------------------------------------
# Input / output dataclasses (frozen, slots=True; coords in metres)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TagStackText:
    """One Pipe-Tag text entity with its insertion point (metres)."""

    text: str
    point: tuple[float, float]


@dataclass(frozen=True, slots=True)
class StackHeader:
    """Orientation annotation for a pipe-tag stack, e.g. "TOP TO BOTTOM"."""

    text: str
    point: tuple[float, float]


@dataclass(frozen=True, slots=True)
class BundleColourBand:
    """One fill-hatch polygon for a colour in the drawing (metres).

    The matcher receives all bands per colour globally -- it sweeps scan lines
    to localise the cross-section rather than requiring pre-segmented bundles.
    """

    colour_key: str
    ring: tuple[tuple[float, float], ...]


@dataclass(frozen=True, slots=True)
class ParsedStack:
    """Internal (public for testing): one resolved tag stack.

    ``axis``: "vertical" (shared x, entries vary in y) or "horizontal".
    ``orientation``: canonical header text, e.g. "TOP TO BOTTOM".
    ``entries``: tuple of (service, PipeSize) in stack order (top->bottom, etc.).
    ``fingerprint``: run-length encoding of service sequence.
    """

    axis: str
    orientation: str
    entries: tuple[tuple[str, PipeSize], ...]
    fingerprint: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ColourServiceAssignment:
    """Confident colour->service assignment.

    ``sizes``: PipeSize.raw values of the stack entries within the matched run.
    ``size_kind``: common kind ("round"/"rect") if consistent within run, else None.
    """

    colour_key: str
    service: str
    sizes: tuple[str, ...]
    size_kind: str | None


@dataclass(frozen=True, slots=True)
class TagStackServiceResult:
    """Result of tag-stack service assignment over all bundles.

    ``assignments``: confident matches, sorted by colour_key.
    ``unmatched_colour_keys``: colour keys left opaque (abstained), sorted.
    ``matched_stack_count``: number of stacks successfully matched.
    ``ambiguous``: True when at least one abstain case was triggered.
    """

    assignments: tuple[ColourServiceAssignment, ...]
    unmatched_colour_keys: tuple[str, ...]
    matched_stack_count: int
    ambiguous: bool


# ---------------------------------------------------------------------------
# Orientation constants
# ---------------------------------------------------------------------------

# Orientations where the stack reads in spatially descending coord order.
# For TOP_TO_BOTTOM the first entry is at the highest y; scan-line perpendicular
# ordering uses descending y. RIGHT_TO_LEFT likewise uses descending x.
_DESCENDING_ORIENTATIONS: frozenset[str] = frozenset({"TOP TO BOTTOM", "RIGHT TO LEFT"})

_ORIENTATION_SET: frozenset[str] = frozenset(
    {"TOP TO BOTTOM", "BOTTOM TO TOP", "LEFT TO RIGHT", "RIGHT TO LEFT"}
)


def _normalize_header(text: str) -> str:
    """Return the orientation phrase contained in the header text, else "".

    Headers wrap the orientation phrase in surrounding words, e.g.
    "FROM TOP TO BOTTOM TA :" -> "TOP TO BOTTOM", so we substring-match rather
    than require exact equality. If the text contains more than one distinct
    orientation phrase (ambiguous), abstain by returning "".
    """
    normalized = " ".join(text.upper().split())
    matches = [phrase for phrase in _ORIENTATION_SET if phrase in normalized]
    return matches[0] if len(matches) == 1 else ""


# ---------------------------------------------------------------------------
# Run-length encoding helper
# ---------------------------------------------------------------------------


def _rle(seq: Sequence[str]) -> tuple[tuple[str, int], ...]:
    """Run-length encode a sequence of strings.

    Example: ["MA", "VAC", "VAC", "AGSS"] -> (("MA",1), ("VAC",2), ("AGSS",1))
    """
    if not seq:
        return ()
    runs: list[tuple[str, int]] = []
    current = seq[0]
    count = 1
    for item in seq[1:]:
        if item == current:
            count += 1
        else:
            runs.append((current, count))
            current = item
            count = 1
    runs.append((current, count))
    return tuple(runs)


# ---------------------------------------------------------------------------
# Tag clustering
# ---------------------------------------------------------------------------


def _dist2d(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)


def _cluster_tags(
    tags: Sequence[TagStackText],
    axis_tol: float,
) -> list[list[TagStackText]]:
    """Group tags into candidate stacks by shared cross-axis coordinate.

    Tries vertical clustering (shared x within axis_tol) first; if that yields
    only singletons, tries horizontal (shared y). Returns clusters of >=2 tags.

    Greedy nearest-neighbour: a tag joins the first existing cluster that has any
    member within axis_tol on the chosen axis (transitively groups columns with
    small x-drift).
    """
    if not tags:
        return []

    for coord_idx in (0, 1):
        sorted_tags = sorted(tags, key=lambda t: t.point[coord_idx])
        clusters: list[list[TagStackText]] = []
        for tag in sorted_tags:
            placed = False
            for cluster in clusters:
                if any(abs(tag.point[coord_idx] - m.point[coord_idx]) <= axis_tol for m in cluster):
                    cluster.append(tag)
                    placed = True
                    break
            if not placed:
                clusters.append([tag])
        valid = [c for c in clusters if len(c) >= 2]
        if valid:
            return valid

    return []


# ---------------------------------------------------------------------------
# Stack orientation and parsing
# ---------------------------------------------------------------------------


def _sort_key_for_orientation(tag: TagStackText, orientation: str) -> float:
    """Ascending sort key placing stack entries in reading order."""
    x, y = tag.point
    if orientation == "TOP TO BOTTOM":
        return -y
    if orientation == "BOTTOM TO TOP":
        return y
    if orientation == "LEFT TO RIGHT":
        return x
    if orientation == "RIGHT TO LEFT":
        return -x
    return 0.0


def _determine_axis(cluster: list[TagStackText]) -> str:
    """Vertical if x-spread << y-spread, else horizontal.

    "<<" defined as x_spread < 0.5 * y_spread; equal spreads -> vertical default.
    """
    xs = [t.point[0] for t in cluster]
    ys = [t.point[1] for t in cluster]
    x_spread = max(xs) - min(xs)
    y_spread = max(ys) - min(ys)
    return "vertical" if x_spread < 0.5 * y_spread else "horizontal"


def _build_parsed_stack(
    cluster: list[TagStackText],
    headers: Sequence[StackHeader],
    header_max_dist: float,
) -> ParsedStack | None:
    """Build a ParsedStack for the cluster, or None to abstain.

    Abstain when:
    - No header within header_max_dist of the nearest tag (case 1).
    - Header text not a recognised orientation.
    - No tags survive parse_tag (degenerate).
    """
    pts = [t.point for t in cluster]

    # Use nearest-tag distance (not centroid) because headers typically sit
    # just outside one end of the stack.
    best_header: StackHeader | None = None
    best_dist = math.inf
    for h in headers:
        min_d = min(_dist2d(pt, h.point) for pt in pts)
        if min_d < best_dist:
            best_dist = min_d
            best_header = h

    if best_header is None or best_dist > header_max_dist:
        return None

    orientation = _normalize_header(best_header.text)
    if not orientation:
        return None

    axis = _determine_axis(cluster)
    ordered = sorted(cluster, key=lambda t: _sort_key_for_orientation(t, orientation))

    entries: list[tuple[str, PipeSize]] = []
    for tag in ordered:
        obs = parse_tag(tag.text)
        if obs is not None:
            entries.append((obs.service, obs.size))

    if not entries:
        return None

    service_seq = [e[0] for e in entries]
    fingerprint = _rle(service_seq)

    return ParsedStack(
        axis=axis,
        orientation=orientation,
        entries=tuple(entries),
        fingerprint=fingerprint,
    )


# ---------------------------------------------------------------------------
# Scan-line band cross-section
# ---------------------------------------------------------------------------


def _band_bbox(ring: tuple[tuple[float, float], ...]) -> tuple[float, float, float, float] | None:
    """Return (x0, y0, x1, y1) axis-aligned bounding box of a ring, or None."""
    if len(ring) < 3:
        return None
    xs = [p[0] for p in ring]
    ys = [p[1] for p in ring]
    return (min(xs), min(ys), max(xs), max(ys))


def _scan_line_colour_rle(
    bands: Sequence[BundleColourBand],
    scan_pos: float,
    scan_axis: int,  # 0 = vertical scan line (fixed x), 1 = horizontal (fixed y)
    perp_descending: bool,
    level_merge_tol: float,
    bundle_gap_tol: float,
) -> list[tuple[tuple[str, int], ...]]:
    """Colour RLE per contiguous bundle segment at a single scan-line position.

    scan_axis=0 (vertical line at x=scan_pos): selects bands whose x-bbox spans
    scan_pos (x0 <= scan_pos <= x1); orders them by y-centroid.
    scan_axis=1 (horizontal line at y=scan_pos): selects bands whose y-bbox spans
    scan_pos; orders by x-centroid.

    ``perp_descending``: if True, sort descending on the perpendicular coordinate
    (TOP_TO_BOTTOM uses descending y; RIGHT_TO_LEFT uses descending x).

    Gap segmentation: wherever the perpendicular gap between consecutive crossing
    bands exceeds ``bundle_gap_tol``, the scan line is split into a new bundle
    segment. This prevents a whole-column read from being treated as one bundle
    when the drawing has multiple service bundles at different positions.

    Within each segment, adjacent same-colour entries whose perpendicular centroids
    are within ``level_merge_tol`` are merged into one level (PDF gap handling).

    Returns a list of per-segment RLE tuples (one per contiguous bundle cluster).
    Returns an empty list when no bands are crossed.
    """
    # (colour_key, perp_centroid, perp_lo, perp_hi) — lo/hi are band edges on perp axis.
    crossed: list[tuple[str, float, float, float]] = []

    for band in bands:
        bbox = _band_bbox(band.ring)
        if bbox is None:
            continue
        x0, y0, x1, y1 = bbox

        if scan_axis == 0:
            # Vertical scan line at x=scan_pos: band must span x.
            if x0 > scan_pos or x1 < scan_pos:
                continue
            perp = (y0 + y1) / 2.0
            perp_lo, perp_hi = y0, y1
        else:
            # Horizontal scan line at y=scan_pos: band must span y.
            if y0 > scan_pos or y1 < scan_pos:
                continue
            perp = (x0 + x1) / 2.0
            perp_lo, perp_hi = x0, x1

        crossed.append((band.colour_key, perp, perp_lo, perp_hi))

    if not crossed:
        return []

    # Sort by perpendicular centroid ascending (lo/hi follow along); descending
    # reversal applied per-segment after gap detection so gaps use consistent order.
    crossed.sort(key=lambda t: (t[1], t[0]))

    # Gap-segment: split wherever the edge-to-edge gap between consecutive bands
    # exceeds bundle_gap_tol. Edge-to-edge gap = lo_next - hi_prev (ascending order).
    # Using edge gap (not centroid gap) means adjacent touching bands (gap=0) stay
    # in the same segment even when their centroids are ~1 m apart.
    segments: list[list[tuple[str, float, float, float]]] = [[crossed[0]]]
    for entry in crossed[1:]:
        ck, perp, lo, _hi = entry
        prev_hi = segments[-1][-1][3]  # hi of previous band
        edge_gap = lo - prev_hi  # positive when there's space between bands
        if edge_gap > bundle_gap_tol:
            segments.append([])
        segments[-1].append(entry)

    result: list[tuple[tuple[str, int], ...]] = []
    for seg in segments:
        # Apply descending sort within segment if needed.
        if perp_descending:
            seg = list(reversed(seg))

        # Merge adjacent same-colour entries within level_merge_tol (centroid distance).
        merged: list[str] = [seg[0][0]]
        prev_perp = seg[0][1]
        for ck, perp, _lo, _hi in seg[1:]:
            if ck == merged[-1] and abs(perp - prev_perp) <= level_merge_tol:
                pass  # absorbed: same colour within PDF sub-pixel gap tolerance
            else:
                merged.append(ck)
            prev_perp = perp

        result.append(_rle(merged))

    return result


def _candidate_scan_positions(
    bands: Sequence[BundleColourBand],
    scan_axis: int,
    dedup_tol: float,
) -> list[float]:
    """Sorted, deduped candidate scan positions from band bbox midpoints.

    For scan_axis=0 (vertical lines): uses (x0+x1)/2 of each band's bbox.
    For scan_axis=1 (horizontal lines): uses (y0+y1)/2.

    Deduplication: candidate positions within dedup_tol of the previous accepted
    position are dropped (greedy, ascending order).
    """
    mids: list[float] = []
    for band in bands:
        bbox = _band_bbox(band.ring)
        if bbox is None:
            continue
        x0, y0, x1, y1 = bbox
        mid = (x0 + x1) / 2.0 if scan_axis == 0 else (y0 + y1) / 2.0
        mids.append(mid)

    mids.sort()
    deduped: list[float] = []
    for m in mids:
        if not deduped or abs(m - deduped[-1]) > dedup_tol:
            deduped.append(m)
    return deduped


# ---------------------------------------------------------------------------
# Degenerate fingerprint check (abstain case 5)
# ---------------------------------------------------------------------------


def _is_degenerate_fingerprint(fp: tuple[tuple[str, int], ...]) -> bool:
    """True when the fingerprint cannot discriminate:
    - total runs < 2
    - all runs have the same service (single-service multi-run)
    """
    if len(fp) < 2:
        return True
    services = {run[0] for run in fp}
    return len(services) == 1


# ---------------------------------------------------------------------------
# Per-stack scan-line matching
# ---------------------------------------------------------------------------

# Type alias: a colour->service mapping as a sorted tuple (for hashability).
_Mapping = tuple[tuple[str, str], ...]  # ((colour_key, service), ...)


def _colour_service_mapping(
    colour_rle: tuple[tuple[str, int], ...],
    service_rle: tuple[tuple[str, int], ...],
) -> _Mapping | None:
    """Build a colour->service mapping from aligned RLEs, or None if lengths differ."""
    if len(colour_rle) != len(service_rle):
        return None
    if tuple(c for _, c in colour_rle) != tuple(c for _, c in service_rle):
        return None  # count sequences differ
    return tuple(
        sorted((ck, svc) for (ck, _), (svc, _) in zip(colour_rle, service_rle, strict=True))
    )


def _match_stack_to_bands(
    ps: ParsedStack,
    all_bands: list[BundleColourBand],
    level_merge_tol: float,
    bundle_gap_tol: float,
    dedup_tol: float,
    min_stable: int,
) -> tuple[_Mapping, tuple[tuple[str, int], ...]] | None:
    """Find a stable, consistent colour->service mapping for one ParsedStack.

    Sweeps scan lines through all bands. At each candidate position the scan line
    is gap-segmented into contiguous bundle clusters. Each cluster is checked
    independently against the stack fingerprint count sequence.

    A (scan_position, bundle_segment) pair is a "hit" when the segment's count
    sequence matches the stack's count sequence. Stability requires the SAME bundle
    segment (same mapping) to match across >=min_stable adjacent scan positions.

    Returns ``(mapping, colour_rle)`` on success — the colour_rle is the position-
    aligned colour run-length sequence whose run i corresponds to fingerprint run i.
    Callers use this to attach sizes by run index, NOT by re-deriving via service
    name (which breaks when the same service appears in non-adjacent runs).

    Returns None to abstain when:
    - No (position, segment) hit found (case 6).
    - No mapping is stable across >=min_stable adjacent scan positions (case 7).
    - Multiple distinct stable mappings found (non-unique geometry, case 3).
    """
    if ps.orientation in ("TOP TO BOTTOM", "BOTTOM TO TOP"):
        scan_axis = 0  # vertical lines (vary x)
        perp_descending = ps.orientation == "TOP TO BOTTOM"
    else:
        scan_axis = 1  # horizontal lines (vary y)
        perp_descending = ps.orientation == "RIGHT TO LEFT"

    fp = ps.fingerprint
    target_counts = tuple(count for _, count in fp)

    candidates = _candidate_scan_positions(all_bands, scan_axis, dedup_tol)
    if not candidates:
        return None

    # Accumulate per-mapping hit positions AND one representative colour_rle.
    # mapping -> (list of scan positions, first colour_rle that produced that mapping)
    mapping_positions: dict[_Mapping, list[float]] = {}
    mapping_colour_rle: dict[_Mapping, tuple[tuple[str, int], ...]] = {}

    for pos in candidates:
        segments = _scan_line_colour_rle(
            all_bands, pos, scan_axis, perp_descending, level_merge_tol, bundle_gap_tol
        )
        for colour_rle in segments:
            colour_counts = tuple(count for _, count in colour_rle)
            if colour_counts != target_counts:
                continue
            mapping = _colour_service_mapping(colour_rle, fp)
            if mapping is None:
                continue
            mapping_positions.setdefault(mapping, []).append(pos)
            if mapping not in mapping_colour_rle:
                mapping_colour_rle[mapping] = colour_rle

    if not mapping_positions:
        return None  # abstain: no (position, segment) hit (case 6)

    # Stability per mapping: require >=min_stable consecutive-candidate hits.
    stable_mappings: list[_Mapping] = [
        m
        for m, positions in mapping_positions.items()
        if _is_stable(positions, candidates, min_stable)
    ]

    if not stable_mappings:
        return None  # abstain: no mapping is stable (case 7)

    if len(stable_mappings) > 1:
        return None  # abstain: non-unique stable mapping (case 3)

    winner = stable_mappings[0]
    return winner, mapping_colour_rle[winner]


def _is_stable(
    match_positions: list[float],
    all_candidates: list[float],
    min_stable: int,
) -> bool:
    """True iff at least min_stable consecutive-candidate matches exist.

    "Consecutive" means adjacent in the all_candidates list (no non-matching
    candidate sits between them). A single isolated match fails stability.
    """
    if len(match_positions) >= min_stable:
        # Fast path: check for any run of min_stable consecutive matches.
        match_set = set(match_positions)
        count = 0
        for pos in all_candidates:
            if pos in match_set:
                count += 1
                if count >= min_stable:
                    return True
            else:
                count = 0
    return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def assign_services_by_tag_stack(
    *,
    tags: Sequence[TagStackText],
    headers: Sequence[StackHeader],
    bundle_bands_by_colour: Mapping[str, Sequence[BundleColourBand]],
    stack_axis_tol_m: float = STACK_AXIS_TOL_M,
    level_merge_tol_m: float = LEVEL_MERGE_TOL_M,
) -> TagStackServiceResult:
    """Assign services to routed colours via scan-line fingerprint matching.

    Parameters
    ----------
    tags:
        All Pipe-Tag text entities for the drawing (any ordering).
    headers:
        Stack orientation annotations (e.g. "TOP TO BOTTOM").
    bundle_bands_by_colour:
        Mapping from colour_key -> ALL BundleColourBand polygons globally.
        The scan-line sweep finds the cross-section; pre-segmentation is not required.
    stack_axis_tol_m:
        Cross-axis clustering tolerance (metres) for grouping tags into stacks.
    level_merge_tol_m:
        Adjacent same-colour bands within this perpendicular distance (metres)
        are merged into one level (PDF gap tolerance).

    Returns
    -------
    TagStackServiceResult with confident assignments sorted by colour_key.
    Empty inputs return an empty result without raising.
    """
    all_colours_sorted = tuple(sorted(bundle_bands_by_colour))
    empty = TagStackServiceResult(
        assignments=(),
        unmatched_colour_keys=all_colours_sorted,
        matched_stack_count=0,
        ambiguous=False,
    )

    if not tags or not bundle_bands_by_colour:
        return empty

    # Flatten all bands into a single list for scan-line sweeping.
    all_bands: list[BundleColourBand] = []
    for bands in bundle_bands_by_colour.values():
        all_bands.extend(bands)

    # ------------------------------------------------------------------ #
    # Step 1: cluster tags into stacks                                     #
    # ------------------------------------------------------------------ #
    clusters = _cluster_tags(tags, stack_axis_tol_m)
    if not clusters:
        return empty

    # ------------------------------------------------------------------ #
    # Step 2: build ParsedStack for each cluster                           #
    # ------------------------------------------------------------------ #
    parsed_stacks: list[ParsedStack] = []
    abstain_triggered = False

    for cluster in clusters:
        ps = _build_parsed_stack(cluster, headers, HEADER_MAX_DIST_M)
        if ps is None:
            abstain_triggered = True  # case 1: no header in range
            continue
        parsed_stacks.append(ps)

    if not parsed_stacks:
        return TagStackServiceResult(
            assignments=(),
            unmatched_colour_keys=all_colours_sorted,
            matched_stack_count=0,
            ambiguous=True,
        )

    # ------------------------------------------------------------------ #
    # Steps 3-6: match each stack (ONCE per stack), check consistency,     #
    # attach sizes by position-aligned run index.                          #
    # ------------------------------------------------------------------ #
    # Per stack: call _match_stack_to_bands ONCE and keep (mapping, colour_rle).
    # Two-pass approach avoids re-invoking the scan-line sweep for size attachment
    # and correctly handles repeated services in non-adjacent runs (fix 1+2).

    # stack_results: (ParsedStack, mapping, aligned_colour_rle) for each match.
    stack_results: list[tuple[ParsedStack, _Mapping, tuple[tuple[str, int], ...]]] = []

    for ps in parsed_stacks:
        fp = ps.fingerprint
        # Abstain case 5: degenerate fingerprint.
        if _is_degenerate_fingerprint(fp):
            abstain_triggered = True
            continue

        result_pair = _match_stack_to_bands(
            ps,
            all_bands,
            level_merge_tol=level_merge_tol_m,
            bundle_gap_tol=BUNDLE_GAP_TOL_M,
            dedup_tol=SCAN_DEDUP_TOL_M,
            min_stable=MIN_STABLE_MATCH_COUNT,
        )
        if result_pair is None:
            abstain_triggered = True  # cases 2, 6, 7
            continue
        mapping, colour_rle = result_pair
        stack_results.append((ps, mapping, colour_rle))

    if not stack_results:
        return TagStackServiceResult(
            assignments=(),
            unmatched_colour_keys=all_colours_sorted,
            matched_stack_count=0,
            ambiguous=True,
        )

    # ------------------------------------------------------------------ #
    # Consistency check: all matched stacks must agree on colour->service. #
    # Conflict (same colour_key -> different services) -> abstain (case 4)  #
    # ------------------------------------------------------------------ #
    merged_mapping: dict[str, str] = {}
    conflict = False
    for _ps, mapping, _rle in stack_results:
        for ck, svc in mapping:
            if ck in merged_mapping and merged_mapping[ck] != svc:
                conflict = True
                break
            merged_mapping[ck] = svc
        if conflict:
            break

    if conflict:
        abstain_triggered = True
        return TagStackServiceResult(
            assignments=(),
            unmatched_colour_keys=all_colours_sorted,
            matched_stack_count=0,
            ambiguous=True,
        )

    # ------------------------------------------------------------------ #
    # Build assignments: sizes attached by position-aligned run index.     #
    # For run i in the fingerprint, colour_rle[i].colour_key is the match; #
    # stack.entries[offset:offset+count] are the corresponding sizes.      #
    # This correctly handles repeated services in non-adjacent runs.       #
    # ------------------------------------------------------------------ #
    colour_sizes: dict[str, tuple[str, ...]] = {}
    colour_kind: dict[str, str | None] = {}
    matched_count = len(stack_results)

    for ps, _mapping, colour_rle in stack_results:
        fp = ps.fingerprint
        entry_idx = 0
        for run_idx, (_svc, run_count) in enumerate(fp):
            colour_key = colour_rle[run_idx][0]  # position-aligned: run i -> colour i
            run_entries = ps.entries[entry_idx : entry_idx + run_count]
            entry_idx += run_count
            if colour_key not in colour_sizes:
                sizes = tuple(size.raw for _, size in run_entries)
                kinds = {size.kind for _, size in run_entries}
                colour_sizes[colour_key] = sizes
                colour_kind[colour_key] = next(iter(kinds)) if len(kinds) == 1 else None

    assignments: list[ColourServiceAssignment] = []
    for ck in sorted(merged_mapping):
        svc = merged_mapping[ck]
        sizes = colour_sizes.get(ck, ())
        assignments.append(
            ColourServiceAssignment(
                colour_key=ck,
                service=svc,
                sizes=sizes,
                size_kind=colour_kind.get(ck),
            )
        )

    assigned_keys = {a.colour_key for a in assignments}
    unmatched = tuple(ck for ck in all_colours_sorted if ck not in assigned_keys)

    return TagStackServiceResult(
        assignments=tuple(assignments),
        unmatched_colour_keys=unmatched,
        matched_stack_count=matched_count,
        ambiguous=abstain_triggered,
    )
