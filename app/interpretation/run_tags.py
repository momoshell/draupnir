"""Pipe-tag text parser (issue #610, Phase 2 / be-p2-01).

Turns a raw pipe-tag text string (as it arrives from a DXF/DWG entity) into a structured
:class:`TagObservation`, or ``None`` when the text cannot be reliably interpreted.

The Ø diameter glyph is **not** relied upon for parsing: it arrives garbled from non-UTF-8
sources (0xD8 → U+FFFD, ``b"\\xef\\xbf\\xbd"``).  Patterns key solely on the digit run and
the ``mm`` / service tokens that follow it.

Recognised forms (first hit wins):
1. Rectangular duct:   ``WxH SERVICE``       e.g. ``700x300 DA``
2. Round + mm:         ``NN mm SERVICE``      e.g. ``Ø76 mm VAC``   (Ø ignored)
3. Round, no mm:       ``NN SERVICE``         e.g. ``Ø150 SA``       (Ø ignored)

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
_RECT_RE = re.compile(r"(\d+)\s*[xX]\s*(\d+)\s+(.+)$")
_ROUND_MM_RE = re.compile(r"(\d+)\s*mm\s+([A-Za-z/]+)", re.IGNORECASE)
_ROUND_RE = re.compile(r"(\d+)\s+([A-Za-z/]+)")

# Unit / dimension-separator tokens that the round-no-mm fallback can otherwise capture as a
# fabricated "service" (e.g. "42 mm" -> MM, "100 X 50" -> X, a note "...200 MM"). A real service
# abbreviation is never one of these, so reject them (legend-is-ground-truth: never guess a
# service from a size-only tag or a stray note).
_NON_SERVICE_TOKENS: frozenset[str] = frozenset({"MM", "CM", "M", "X"})

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


def _valid_service(service: str) -> bool:
    """A service token must be non-empty, not a bare unit/dimension separator, and not a
    common English function word (which only a note fragment would surface)."""
    return (
        bool(service) and service not in _NON_SERVICE_TOKENS and service not in _NON_SERVICE_WORDS
    )


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


def parse_tag(text: str) -> TagObservation | None:
    """Parse a pipe-tag text string into a :class:`TagObservation`, or ``None``.

    Returns ``None`` when:
    - The text is empty or whitespace-only.
    - No digit run is found.
    - No service token follows a digit run.
    - The tag appears to be a size-less service-only label (e.g. ``EA``).
    - The text matches the degenerate ``Ø mm`` pattern (digit absent).

    Never raises on any input string.
    """
    try:
        return _parse_tag_inner(text)
    except Exception:  # tolerant by contract
        return None


def _parse_tag_inner(text: str) -> TagObservation | None:
    """Internal implementation — may assume a str argument."""
    normalized = _normalize_text(text)
    if not normalized:
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
        for tok in tail.split():
            t = tok.upper()
            if t == "&":
                # Allow "&" as an in-phrase connector only, never leading
                if type_tokens:
                    type_tokens.append("&")
                continue
            if t in _NON_SERVICE_WORDS or t in _NON_SERVICE_TOKENS:
                break  # stop at prose / units
            type_tokens.append(t)
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

    # 3. Round, no mm: NN SERVICE
    m = _ROUND_RE.search(normalized)
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

    return None
