"""Deterministic estimate PDF export service."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from uuid import UUID

from reportlab.lib import colors  # type: ignore[import-untyped]
from reportlab.lib.enums import TA_RIGHT  # type: ignore[import-untyped]
from reportlab.lib.pagesizes import A4  # type: ignore[import-untyped]
from reportlab.lib.styles import (  # type: ignore[import-untyped]
    ParagraphStyle,
    getSampleStyleSheet,
)
from reportlab.lib.units import mm  # type: ignore[import-untyped]
from reportlab.pdfgen.canvas import Canvas  # type: ignore[import-untyped]
from reportlab.platypus import (  # type: ignore[import-untyped]
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.estimating.money import format_catalog_decimal, format_money
from app.exports._base import (
    ExportArtifactWithOptions,
    JSONValue,
    build_artifact,
    normalize_datetime,
    normalize_json_value_tree,
    normalize_options,
)
from app.models.estimate_version import EstimateItem, EstimateVersion

ESTIMATE_PDF_EXPORT_MEDIA_TYPE = "application/pdf"
ESTIMATE_PDF_EXPORT_GENERATOR_NAME = "estimate_pdf_export"
ESTIMATE_PDF_EXPORT_GENERATOR_VERSION = "1"

_PAGE_SIZE = A4
_PAGE_MARGIN = 15 * mm
_PDF_INFO_TIMESTAMP = b"D:20000101000000+00'00'"
_PDF_ID_PLACEHOLDER = b"0" * 32
_LINE_TABLE_COLUMN_WIDTHS = (
    10 * mm,
    18 * mm,
    58 * mm,
    16 * mm,
    14 * mm,
    18 * mm,
    16 * mm,
    12 * mm,
    18 * mm,
)


class EstimatePdfExportError(Exception):
    """Raised when an estimate PDF export cannot be rendered."""


@dataclass(frozen=True, slots=True)
class EstimatePdfExportResult(ExportArtifactWithOptions):
    """Pure rendered estimate PDF export artifact metadata."""


async def render_estimate_pdf_export(
    db: AsyncSession,
    estimate_version_id: UUID,
    *,
    options: Mapping[str, object] | None = None,
) -> EstimatePdfExportResult:
    """Render an immutable estimate version into deterministic PDF bytes."""

    options_snapshot = _normalize_options(options)

    estimate_version = await db.get(EstimateVersion, estimate_version_id)
    if estimate_version is None:
        raise EstimatePdfExportError(f"Estimate version {estimate_version_id} was not found.")

    items = await _load_estimate_items(db, estimate_version_id)
    content_bytes = _render_estimate_pdf_bytes(estimate_version, items)
    return build_artifact(
        EstimatePdfExportResult,
        content_bytes=content_bytes,
        media_type=ESTIMATE_PDF_EXPORT_MEDIA_TYPE,
        generator_name=ESTIMATE_PDF_EXPORT_GENERATOR_NAME,
        generator_version=ESTIMATE_PDF_EXPORT_GENERATOR_VERSION,
        options=options_snapshot,
    )


async def _load_estimate_items(
    db: AsyncSession,
    estimate_version_id: UUID,
) -> list[EstimateItem]:
    result = await db.execute(
        select(EstimateItem)
        .where(EstimateItem.estimate_version_id == estimate_version_id)
        .order_by(EstimateItem.line_number.asc(), EstimateItem.id.asc())
    )
    return list(result.scalars().all())


def _render_estimate_pdf_bytes(
    estimate_version: EstimateVersion,
    items: Sequence[EstimateItem],
) -> bytes:
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=_PAGE_SIZE,
        leftMargin=_PAGE_MARGIN,
        rightMargin=_PAGE_MARGIN,
        topMargin=_PAGE_MARGIN,
        bottomMargin=_PAGE_MARGIN,
    )
    styles = _build_styles()
    story = [
        Paragraph(f"Estimate {estimate_version.id}", styles["title"]),
        Spacer(1, 4 * mm),
        _build_metadata_table(document.width, estimate_version, len(items), styles["body"]),
        Spacer(1, 5 * mm),
        Paragraph("Totals", styles["section"]),
        Spacer(1, 2 * mm),
        _build_totals_table(document.width, estimate_version, styles["body"], styles["body_right"]),
        Spacer(1, 5 * mm),
        Paragraph("Line items", styles["section"]),
        Spacer(1, 2 * mm),
        _build_items_table(items, styles["table_body"], styles["table_body_right"]),
    ]

    title = f"Draupnir Estimate {estimate_version.id}"

    def canvas_maker(*args: object, **kwargs: object) -> object:
        kwargs.setdefault("invariant", 1)
        kwargs.setdefault("pageCompression", 0)
        pdf_canvas = Canvas(*args, **kwargs)
        pdf_canvas.setAuthor("Draupnir")
        pdf_canvas.setCreator("Draupnir")
        pdf_canvas.setSubject("Estimate PDF export")
        pdf_canvas.setTitle(title)
        return pdf_canvas

    document.build(story, canvasmaker=canvas_maker)
    return _normalize_pdf_bytes(buffer.getvalue())


def _build_styles() -> dict[str, object]:
    stylesheet = getSampleStyleSheet()
    body = ParagraphStyle(
        "EstimatePdfBody",
        parent=stylesheet["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=11,
        spaceAfter=0,
    )
    return {
        "title": ParagraphStyle(
            "EstimatePdfTitle",
            parent=stylesheet["Title"],
            fontName="Helvetica-Bold",
            fontSize=16,
            leading=20,
            textColor=colors.HexColor("#111827"),
            spaceAfter=0,
        ),
        "section": ParagraphStyle(
            "EstimatePdfSection",
            parent=body,
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=14,
            textColor=colors.HexColor("#111827"),
        ),
        "body": body,
        "body_right": ParagraphStyle(
            "EstimatePdfBodyRight",
            parent=body,
            alignment=TA_RIGHT,
        ),
        "table_body": ParagraphStyle(
            "EstimatePdfTableBody",
            parent=body,
            leading=10,
        ),
        "table_body_right": ParagraphStyle(
            "EstimatePdfTableBodyRight",
            parent=body,
            alignment=TA_RIGHT,
            leading=10,
        ),
    }


def _build_metadata_table(
    document_width: float,
    estimate_version: EstimateVersion,
    item_count: int,
    body_style: object,
) -> object:
    rows = [
        _metadata_row("Estimate version", str(estimate_version.id), body_style),
        _metadata_row("Project", str(estimate_version.project_id), body_style),
        _metadata_row("Source file", str(estimate_version.source_file_id), body_style),
        _metadata_row("Revision", str(estimate_version.drawing_revision_id), body_style),
        _metadata_row("Quantity takeoff", str(estimate_version.quantity_takeoff_id), body_style),
        _metadata_row("Source job", str(estimate_version.source_job_id), body_style),
        _metadata_row("Currency", estimate_version.currency, body_style),
        _metadata_row("Created at", _normalize_datetime(estimate_version.created_at), body_style),
        _metadata_row("Line count", str(item_count), body_style),
    ]

    table = Table(rows, colWidths=(38 * mm, document_width - (38 * mm)))
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3f4f6")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def _build_totals_table(
    document_width: float,
    estimate_version: EstimateVersion,
    body_style: object,
    body_right_style: object,
) -> object:
    spacer_width = document_width - (32 * mm) - (28 * mm)
    rows = [
        _totals_row("Subtotal", estimate_version.subtotal_amount, body_style, body_right_style),
        _totals_row("Tax", estimate_version.tax_amount, body_style, body_right_style),
        _totals_row("Total", estimate_version.total_amount, body_style, body_right_style),
    ]
    table = Table(rows, colWidths=(32 * mm, spacer_width, 28 * mm))
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (2, 2), (2, 2), "Helvetica-Bold"),
                ("LINEBELOW", (0, 0), (-1, 1), 0.25, colors.HexColor("#d1d5db")),
                ("LINEABOVE", (0, 2), (-1, 2), 0.75, colors.HexColor("#111827")),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return table


def _build_items_table(
    items: Sequence[EstimateItem],
    body_style: object,
    body_right_style: object,
) -> object:
    rows: list[list[object]] = [
        [
            "Line",
            "Type",
            "Description",
            "Qty",
            "Unit",
            "Rate",
            "Subtotal",
            "Tax",
            "Total",
        ]
    ]
    for item in items:
        rows.append(
            [
                _paragraph(str(item.line_number), body_right_style),
                _paragraph(item.line_type, body_style),
                _paragraph(item.description, body_style),
                _paragraph(
                    _format_optional_decimal(
                        item.quantity_value,
                        formatter=format_catalog_decimal,
                    ),
                    body_right_style,
                ),
                _paragraph(item.quantity_unit or "", body_style),
                _paragraph(
                    _format_optional_decimal(
                        item.unit_rate_amount, formatter=format_catalog_decimal
                    ),
                    body_right_style,
                ),
                _paragraph(
                    format_money(item.subtotal_amount),
                    body_right_style,
                ),
                _paragraph(
                    format_money(item.tax_amount),
                    body_right_style,
                ),
                _paragraph(
                    format_money(item.total_amount),
                    body_right_style,
                ),
            ]
        )

    table = Table(rows, colWidths=_LINE_TABLE_COLUMN_WIDTHS, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("LEADING", (0, 0), (-1, 0), 10),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f9fafb")]),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ALIGN", (0, 0), (0, -1), "RIGHT"),
                ("ALIGN", (3, 0), (3, -1), "RIGHT"),
                ("ALIGN", (5, 0), (8, -1), "RIGHT"),
            ]
        )
    )
    return table


def _metadata_row(label: str, value: str, body_style: object) -> list[object]:
    return [_paragraph(label, body_style), _paragraph(value, body_style)]


def _totals_row(
    label: str,
    amount: Decimal,
    body_style: object,
    body_right_style: object,
) -> list[object]:
    return [
        _paragraph(label, body_style),
        "",
        _paragraph(format_money(amount), body_right_style),
    ]


def _paragraph(text: str, style: object) -> object:
    return Paragraph(_escape_paragraph_text(text), style)


def _escape_paragraph_text(value: str) -> str:
    return (
        value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _format_optional_decimal(
    value: Decimal | None,
    *,
    formatter: Callable[[Decimal], str],
) -> str:
    if value is None:
        return ""
    return formatter(value)


def _normalize_pdf_bytes(content_bytes: bytes) -> bytes:
    normalized = re.sub(
        rb"/CreationDate \([^)]*\)",
        b"/CreationDate (" + _PDF_INFO_TIMESTAMP + b")",
        content_bytes,
    )
    normalized = re.sub(
        rb"/ModDate \([^)]*\)",
        b"/ModDate (" + _PDF_INFO_TIMESTAMP + b")",
        normalized,
    )
    normalized = re.sub(
        rb"/ID\s*\[<[^>]+><[^>]+>\]",
        b"/ID [<" + _PDF_ID_PLACEHOLDER + b"><" + _PDF_ID_PLACEHOLDER + b">]",
        normalized,
    )

    pdf_id = hashlib.sha256(normalized).hexdigest()[:32].encode("ascii")
    return normalized.replace(
        b"/ID [<" + _PDF_ID_PLACEHOLDER + b"><" + _PDF_ID_PLACEHOLDER + b">]",
        b"/ID [<" + pdf_id + b"><" + pdf_id + b">]",
        1,
    )


def _normalize_options(options: Mapping[str, object] | None) -> dict[str, JSONValue]:
    return normalize_options(
        options,
        normalize_value=_normalize_json_value,
        normalize_mapping_key=_normalize_mapping_key,
    )


def _normalize_json_value(value: object) -> JSONValue:
    return normalize_json_value_tree(
        value,
        normalize_scalar=_normalize_json_scalar,
        normalize_mapping_key=_normalize_mapping_key,
        unsupported_value=_unsupported_json_value,
    )


def _normalize_json_scalar(value: object) -> tuple[bool, JSONValue]:
    if isinstance(value, UUID):
        return True, str(value)
    if isinstance(value, datetime):
        return True, _normalize_datetime(value)
    if value is None or isinstance(value, (str, int, bool)):
        return True, value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError("Estimate PDF export options must be finite JSON values.")
        return True, value
    return False, None


def _unsupported_json_value(value: object) -> Exception:
    return TypeError(f"Unsupported estimate PDF export value type: {type(value)!r}")


def _normalize_mapping_key(key: object) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, UUID):
        return str(key)
    if isinstance(key, datetime):
        return _normalize_datetime(key)
    if isinstance(key, (int, bool)):
        return str(key)
    if isinstance(key, float):
        if not math.isfinite(key):
            raise TypeError("Estimate PDF export option keys must be finite.")
        return json.dumps(key, ensure_ascii=False, allow_nan=False)

    raise TypeError(f"Unsupported estimate PDF export mapping key type: {type(key)!r}")


def _normalize_datetime(value: datetime) -> str:
    return normalize_datetime(value)
