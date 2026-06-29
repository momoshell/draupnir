"""Pipe-tag text parser (issue #610, Phase 2 / be-p2-01).

Turns a raw pipe-tag text string (as it arrives from a DXF/DWG entity) into a structured
:class:`TagObservation`, or ``None`` when the text cannot be reliably interpreted.

The Ø diameter glyph is **not** relied upon for parsing: it arrives garbled from non-UTF-8
sources (0xD8 → U+FFFD, ``b"\\xef\\xbf\\xbd"``).  Patterns key solely on the digit run and
the ``mm`` / service tokens that follow it.

Recognised forms (first hit wins):
1. Rectangular duct:   ``WxH SERVICE``       e.g. ``700x300 DA``
2. Round + mm:         ``NN mm SERVICE``      e.g. ``Ø76 mm VAC``   (Ø ignored)
3. Round + glyph:      ``NN∅SERVICE``         e.g. ``100∅SVP``       (glyph is anchor)
4. Round, no mm:       ``NN SERVICE``         e.g. ``Ø150 SA``       (Ø ignored)

Size-less tags (e.g. a lone service abbreviation ``EA``) are skipped and return ``None``.
These could be captured in a future follow-up to #610, but without a size the observation
carries insufficient information to be useful for takeoff.

Pure module — NO DB, ORM, FastAPI, or SQLAlchemy imports.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source constant
# ---------------------------------------------------------------------------

BASIS_TAG_TEXT: str = "tag_text"

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

# Strip leading \L underline directive (mirrors service_legend._strip_underline).
_UNDERLINE_DIRECTIVE_RE = re.compile(r"^\\L", re.IGNORECASE)

# Pipe-tag content patterns — use re.search so any garbled Ø prefix is skipped.
# Order matters: rect first (contains 'x'), then round+mm, then bare round.
#
# _RECT_RE accepts optional "mm" after each dimension so that M-560103 labels like
# "100 mmx50 mm REFRIGERANT TRAY" or "100mmx50mm SERVICE" parse correctly.
# The trailing optional mm (group 3 consumed, discarded) precedes the service tail.
_RECT_RE = re.compile(r"(\d+)\s*(?:mm)?\s*[xX]\s*(\d+)\s*(?:mm)?\s+(.+)$", re.IGNORECASE)
_ROUND_MM_RE = re.compile(r"(\d+)\s*mm\s+([A-Za-z/]+)", re.IGNORECASE)
# Glyph-adjacency: <digits><diameter-glyph><service> with no required space between them.
# Matches the single-entity form "15∅CDP", "100∅SVP", "20∅CDP", "15∅CDP AT HL".
# The glyph class mirrors _DIAMETER_CONTEXT_RE; \d{2,3} matches real pipe sizes 15-150
# and rejects single-digit leaders consistent with D3a reassembly.
_ROUND_GLYPH_RE = re.compile(r"(\d{2,3})\s*[∅Ø�?]\s*([A-Za-z/]+)")
_ROUND_RE = re.compile(r"(\d+)\s+([A-Za-z/]+)")

# Unit / dimension-separator tokens that the round-no-mm fallback can otherwise capture as a
# fabricated "service" (e.g. "42 mm" -> MM, "100 X 50" -> X, a note "...200 MM"). A real service
# abbreviation is never one of these, so reject them (legend-is-ground-truth: never guess a
# service from a size-only tag or a stray note).
# "MMX" arises from labels like "100 mmx50 mm" when the old rect regex fails and the
# round-no-mm fallback matches "100" followed by "MMX" — it is never a real service.
_NON_SERVICE_TOKENS: frozenset[str] = frozenset({"MM", "MMX", "CM", "M", "X"})

# Common English function words (prepositions / conjunctions / articles) that can lead a
# note fragment containing a size (e.g. "100MM ABOVE THE DESK", "10MM AND NOT EXCEED"); a real
# service code is never a connective, so these are safe to reject and they stop note prose from
# fabricating a service. NOT service names -- do not add technical abbreviations here.
_NON_SERVICE_WORDS: frozenset[str] = frozenset(
    {
        "ABOVE",
        "BELOW",
        "AND",
        "OR",
        "IN",
        "ON",
        "AT",
        "TO",
        "FROM",
        "WITH",
        "THE",
        "A",
        "OF",
        "AS",
        "PER",
        "NOT",
        "WHERE",
        "EACH",
        "SHALL",
        "BE",
        "FOR",
        "BY",
        "THIS",
        "THAT",
        "ARE",
        "IS",
        "ALL",
        "AN",
        "INTO",
        "ONTO",
        "OVER",
        "UNDER",
    }
)

# Standard-services vocabulary — used as a fallback when no per-sheet legend is supplied.
# tag_reassembly imports this set (run_tags is lower-level; no circular import).
# The per-sheet legend (when present) is ground truth and overrides this set entirely.
# CPD is a tolerated typo-variant of CDP seen in source drawings.
_STANDARD_SERVICE_CODES: frozenset[str] = frozenset(
    {
        # PH / drainage — LENGTH-BEARING pipe runs only. Point fittings that are
        # counted, not measured (RE rodding-eye, FG floor-gully, SG shower-gully,
        # AAV air-admittance-valve), are intentionally EXCLUDED: reassembly produces
        # size+service RUN tags, and a fitting must never capture centerline length.
        "SVP",
        "SWP",
        "VP",
        "RWP",
        "CDP",
        "CPD",
        "WP",
        "FW",
        "SS",
        "VE",
        # Med-gas
        "VAC",
        "MA",
        "OXY",
        "AGSS",
        "N2O",
        # HVAC / wet services
        "SA",
        "RA",
        "EA",
        "OA",
        "DA",
        "CHW",
        "LTHW",
        "MWS",
        "CWS",
        "HWS",
        "CW",
        "HW",
    }
)


# Equipment-vessel keywords that appear in non-pipe labels (e.g. "LOW LOSS HEADER",
# "LOW LESS HEADER"). These labels carry a Ø diameter but refer to a vessel fitting, not a
# measurable pipe run; accepting them steals centerline length via nearest-label matching.
# The set is intentionally tight — add only confirmed equipment-label keywords here.
_EQUIPMENT_KEYWORDS_RE = re.compile(r"\bHEADER\b", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Content gate — title-block / contact prose rejection
# ---------------------------------------------------------------------------

# A run of 6+ consecutive digits is a phone/zip number, never a pipe size.
# (Normal sizes are ≤4 digits; 5-digit postcodes are excluded by the 6-char threshold.)
_LONG_DIGIT_RUN_RE = re.compile(r"\d{6,}")

# Domain-suffix fragments that only appear in URLs / email addresses.
_DOMAIN_SUFFIX_RE = re.compile(r"\.co\.|\.uk|\.com", re.IGNORECASE)

# Contact keywords that only appear in title-block / footer prose.
_CONTACT_KEYWORD_RE = re.compile(r"\b(?:tel|fax|www)\b", re.IGNORECASE)

# Diameter/unit context tokens that justify a bare-number match in _ROUND_RE.
# When none of these are present the bare path requires a legend hit.
# U+FFFD (replacement glyph) is the garbled form of Ø from non-UTF-8 DWGs and is treated
# as a real diameter marker — mirror the [Ø�] convention in service_takeoff_loaders.py.
_DIAMETER_CONTEXT_RE = re.compile(r"[∅Ø�]|mm", re.IGNORECASE)


def _is_annotation_prose(normalized: str) -> bool:
    """Return True when *normalized* text is contact/title-block prose, not a pipe tag.

    Applied BEFORE any size/service pattern so parse_tag returns None early.
    Conservative — only patterns that are unambiguously non-tag content.
    """
    if "@" in normalized:
        return True
    if _CONTACT_KEYWORD_RE.search(normalized):
        return True
    if _DOMAIN_SUFFIX_RE.search(normalized):
        return True
    return bool(_LONG_DIGIT_RUN_RE.search(normalized))


def _valid_service(service: str) -> bool:
    """A service token must be non-empty, not a bare unit/dimension separator, and not a
    common English function word (which only a note fragment would surface)."""
    return (
        bool(service) and service not in _NON_SERVICE_TOKENS and service not in _NON_SERVICE_WORDS
    )


def _is_equipment_label(text: str) -> bool:
    """Return True when the raw label text contains an equipment-vessel keyword.

    Equipment labels (e.g. 'Ø250 LOW LOSS HEADER') share the Ø-diameter format with pipe
    run tags but refer to vessels/fittings, not routed lengths. Rejecting them prevents
    spurious service observations that steal centerline length via nearest-label assignment.
    Extend _EQUIPMENT_KEYWORDS_RE conservatively — only confirmed equipment terms.
    """
    return bool(_EQUIPMENT_KEYWORDS_RE.search(text))


# ---------------------------------------------------------------------------
# Output dataclasses — frozen P2 interface contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PipeSize:
    """Parsed pipe/duct size.

    For round sections ``diameter`` is set; ``width`` and ``height`` are ``None``.
    For rectangular sections ``width`` and ``height`` are set; ``diameter`` is ``None``.
    ``raw`` holds the size substring exactly as matched (e.g. ``"54"``, ``"700x300"``).
    """

    kind: str  # "round" | "rect"
    diameter: int | None  # round only
    width: int | None  # rect only
    height: int | None  # rect only
    raw: str  # raw size substring as parsed ("54", "700x300")


@dataclass(frozen=True, slots=True)
class TagObservation:
    """One structured (service, size) observation extracted from a pipe-tag text string."""

    service: str  # normalized upper-case abbreviation ("VAC")
    size: PipeSize
    raw_text: str  # full original tag text (provenance)
    basis: str  # BASIS_TAG_TEXT


# ---------------------------------------------------------------------------
# Normalisation helpers (mirror service_legend pattern; not imported)
# ---------------------------------------------------------------------------


def _strip_underline(text: str) -> str:
    """Strip a leading \\L underline directive from DXF/DWG text."""
    return _UNDERLINE_DIRECTIVE_RE.sub("", text)


def _normalize_text(raw: str) -> str:
    """Strip underline directive and collapse internal whitespace."""
    return " ".join(_strip_underline(raw).split())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_tag(
    text: str,
    *,
    legend_abbreviations: frozenset[str] | None = None,
    strict_content: bool = False,
) -> TagObservation | None:
    """Parse a pipe-tag text string into a :class:`TagObservation`, or ``None``.

    Returns ``None`` when:
    - The text is empty or whitespace-only.
    - No digit run is found.
    - No service token follows a digit run.
    - The tag appears to be a size-less service-only label (e.g. ``EA``).
    - The text matches the degenerate ``Ø mm`` pattern (digit absent).

    *strict_content* — when ``True``, two additional gates are applied:

    1. **Prose content gate**: contact/title-block prose (email, phone, URL keywords) is
       rejected before pattern matching.
    2. **Bare ``_ROUND_RE`` tightening**: a bare digit+word match (no ``mm``, no rect form)
       is only accepted when a diameter-context token (∅/Ø/U+FFFD/mm) is present in the
       text, or the service token is confirmed by *legend_abbreviations*.

    (Because the prose gate runs first, text containing the contact keywords
    ``tel``/``fax``/``www`` is rejected up front, even with otherwise-valid size context.)

    Default ``False`` preserves byte-identical behaviour for all existing callers
    (routed/colour-keyed paths: ``run_service_identity.py``, ``tag_stack_service.py``,
    ``service_takeoff_loaders.py`` containment label pass).  Set ``True`` only at the
    segment-label call site where tags are sourced from broadened all-text placements.

    *legend_abbreviations* — optional frozenset of known service abbreviations (upper-case).
    When *strict_content* is ``True``, used as a rescue for the bare ``_ROUND_RE`` path:
    a match without diameter-context is accepted if the service appears in the set.
    Has no effect when *strict_content* is ``False``.

    Never raises on any input string.
    """
    try:
        return _parse_tag_inner(
            text,
            legend_abbreviations=legend_abbreviations or frozenset(),
            strict_content=strict_content,
        )
    except Exception:  # tolerant by contract
        return None


def _parse_tag_inner(
    text: str,
    *,
    legend_abbreviations: frozenset[str],
    strict_content: bool,
) -> TagObservation | None:
    """Internal implementation — may assume a str argument."""
    normalized = _normalize_text(text)
    if not normalized:
        return None

    # Prose content gate — only active under strict_content; legend-less callers
    # (run_service_identity.py, tag_stack_service.py, containment label pass) are unaffected.
    if strict_content and _is_annotation_prose(normalized):
        return None

    # Reject equipment-vessel labels before any pattern matching (e.g. "Ø250 LOW LOSS HEADER").
    if _is_equipment_label(normalized):
        return None

    # 1. Rectangular: WxH SERVICE [SERVICE...]
    m = _RECT_RE.search(normalized)
    if m:
        width = int(m.group(1))
        height = int(m.group(2))
        tail = m.group(3).strip()
        # Build multi-word TYPE as the maximal leading run of service-like tokens,
        # stopping at the first function word/unit so note prose doesn't over-capture.
        type_tokens: list[str] = []
        prev_was_token = False
        for tok in tail.split():
            t = tok.upper()
            if t == "&":
                if prev_was_token:
                    type_tokens.append("&")
                    prev_was_token = False  # a following & (or end) won't be kept
                    continue
                break  # leading or doubled & -> stop the run
            if t in _NON_SERVICE_WORDS or t in _NON_SERVICE_TOKENS:
                break
            type_tokens.append(t)
            prev_was_token = True
        while type_tokens and type_tokens[-1] == "&":  # strip dangling trailing &
            type_tokens.pop()
        if not type_tokens:
            return None  # first token was prose / unit — not a real tag
        service = " ".join(type_tokens)
        raw_size = f"{m.group(1)}x{m.group(2)}"
        return TagObservation(
            service=service,
            size=PipeSize(kind="rect", diameter=None, width=width, height=height, raw=raw_size),
            raw_text=text,
            basis=BASIS_TAG_TEXT,
        )

    # 2. Round + mm: NN mm SERVICE
    m = _ROUND_MM_RE.search(normalized)
    if m:
        diameter = int(m.group(1))
        service = m.group(2).strip().upper()
        if not _valid_service(service):
            return None
        return TagObservation(
            service=service,
            size=PipeSize(kind="round", diameter=diameter, width=None, height=None, raw=m.group(1)),
            raw_text=text,
            basis=BASIS_TAG_TEXT,
        )

    # 3. Round + glyph (single-entity): NN∅SERVICE — the glyph directly anchors the size.
    # The glyph itself is the diameter-context marker, so the strict_content tightening
    # differs from the bare path: we don't require an additional context token.  However
    # the service token is still subject to legend-OR-standard-vocabulary validation when
    # strict_content is active (same pattern as tag_reassembly's _classify gate):
    #   - non-empty legend supplied → service must be in legend (legend is ground truth)
    #   - empty/absent legend → service must be in _STANDARD_SERVICE_CODES (drainage etc.)
    # This prevents noise tokens like DEEP/SEAL from being accepted on drainage drawings
    # where the legend is intentionally empty.
    m = _ROUND_GLYPH_RE.search(normalized)
    if m:
        diameter = int(m.group(1))
        service = m.group(2).strip().upper()
        if not _valid_service(service):
            return None
        if strict_content:
            if legend_abbreviations:
                # Legend supplied: must be a member (legend is ground truth).
                if service not in legend_abbreviations:
                    return None
            else:
                # No legend: gate on standard vocabulary so noise tokens are rejected.
                if service not in _STANDARD_SERVICE_CODES:
                    return None
        return TagObservation(
            service=service,
            size=PipeSize(kind="round", diameter=diameter, width=None, height=None, raw=m.group(1)),
            raw_text=text,
            basis=BASIS_TAG_TEXT,
        )

    # 4. Round, no mm: NN SERVICE
    m = _ROUND_RE.search(normalized)
    if m:
        diameter = int(m.group(1))
        service = m.group(2).strip().upper()
        if not _valid_service(service):
            return None
        # Bare-path tightening — only active under strict_content.
        # Requires either a diameter-context token (∅/Ø/U+FFFD/mm) or a legend hit.
        # Without strict_content the behaviour is byte-identical to pre-D1 (no gate).
        if strict_content:
            has_context = bool(_DIAMETER_CONTEXT_RE.search(normalized))
            in_legend = bool(legend_abbreviations) and service in legend_abbreviations
            if not has_context and not in_legend:
                return None
        return TagObservation(
            service=service,
            size=PipeSize(kind="round", diameter=diameter, width=None, height=None, raw=m.group(1)),
            raw_text=text,
            basis=BASIS_TAG_TEXT,
        )

    return None
