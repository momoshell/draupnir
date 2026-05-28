"""Deterministic CSV export services."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from io import StringIO
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.estimate_version import EstimateItem, EstimateVersion
from app.models.quantity_takeoff import QuantityItem, QuantityTakeoff

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

CSV_EXPORT_MEDIA_TYPE = "text/csv"

QUANTITY_CSV_EXPORT_GENERATOR_NAME = "quantity_csv_export"
QUANTITY_CSV_EXPORT_GENERATOR_VERSION = "1"
ESTIMATE_CSV_EXPORT_GENERATOR_NAME = "estimate_csv_export"
ESTIMATE_CSV_EXPORT_GENERATOR_VERSION = "1"

QUANTITY_CSV_EXPORT_HEADERS: tuple[str, ...] = (
    "quantity_takeoff_id",
    "quantity_item_id",
    "project_id",
    "source_file_id",
    "drawing_revision_id",
    "source_job_id",
    "item_kind",
    "quantity_type",
    "value",
    "unit",
    "review_state",
    "validation_status",
    "quantity_gate",
    "source_entity_id",
    "excluded_source_entity_ids",
    "created_at",
)

ESTIMATE_CSV_EXPORT_HEADERS: tuple[str, ...] = (
    "estimate_version_id",
    "estimate_item_id",
    "project_id",
    "source_file_id",
    "drawing_revision_id",
    "quantity_takeoff_id",
    "source_job_id",
    "line_number",
    "line_key",
    "line_type",
    "description",
    "currency",
    "quantity_value",
    "quantity_unit",
    "unit_rate_amount",
    "effective_date",
    "subtotal_amount",
    "tax_amount",
    "total_amount",
    "rounding",
    "quantity_snapshot_entry_id",
    "rate_snapshot_entry_id",
    "material_snapshot_entry_id",
    "formula_snapshot_entry_id",
    "assumption_snapshot_entry_id",
    "created_at",
)

_MONEY_QUANTUM = Decimal("0.01")
_RATE_QUANTITY_QUANTUM = Decimal("0.000001")
_SPREADSHEET_FORMULA_PREFIXES = ("=", "+", "-", "@")


class QuantityCsvExportError(Exception):
    """Raised when a quantity CSV export cannot be rendered."""


class EstimateCsvExportError(Exception):
    """Raised when an estimate CSV export cannot be rendered."""


@dataclass(frozen=True, slots=True)
class CsvExportResult:
    """Pure rendered CSV export artifact metadata."""

    content_bytes: bytes
    checksum_sha256: str
    size_bytes: int
    media_type: str
    generator_name: str
    generator_version: str


async def render_quantity_csv_export(
    db: AsyncSession,
    quantity_takeoff_id: UUID,
) -> CsvExportResult:
    """Render quantity takeoff items into deterministic CSV bytes."""

    quantity_takeoff = await db.get(QuantityTakeoff, quantity_takeoff_id)
    if quantity_takeoff is None:
        raise QuantityCsvExportError(f"Quantity takeoff {quantity_takeoff_id} was not found.")

    result = await db.execute(
        select(QuantityItem)
        .where(QuantityItem.quantity_takeoff_id == quantity_takeoff_id)
        .order_by(QuantityItem.created_at.asc(), QuantityItem.id.asc())
    )
    items = list(result.scalars().all())

    content_bytes = _render_csv_bytes(
        QUANTITY_CSV_EXPORT_HEADERS,
        _iter_quantity_rows(quantity_takeoff, items),
    )
    return _build_result(
        content_bytes,
        generator_name=QUANTITY_CSV_EXPORT_GENERATOR_NAME,
        generator_version=QUANTITY_CSV_EXPORT_GENERATOR_VERSION,
    )


async def render_estimate_csv_export(
    db: AsyncSession,
    estimate_version_id: UUID,
) -> CsvExportResult:
    """Render estimate line items into deterministic CSV bytes."""

    estimate_version = await db.get(EstimateVersion, estimate_version_id)
    if estimate_version is None:
        raise EstimateCsvExportError(f"Estimate version {estimate_version_id} was not found.")

    result = await db.execute(
        select(EstimateItem)
        .where(EstimateItem.estimate_version_id == estimate_version_id)
        .order_by(EstimateItem.line_number.asc(), EstimateItem.id.asc())
    )
    items = list(result.scalars().all())

    content_bytes = _render_csv_bytes(
        ESTIMATE_CSV_EXPORT_HEADERS,
        _iter_estimate_rows(estimate_version, items),
    )
    return _build_result(
        content_bytes,
        generator_name=ESTIMATE_CSV_EXPORT_GENERATOR_NAME,
        generator_version=ESTIMATE_CSV_EXPORT_GENERATOR_VERSION,
    )


def _iter_quantity_rows(
    quantity_takeoff: QuantityTakeoff,
    items: Sequence[QuantityItem],
) -> Iterable[Sequence[str]]:
    for item in items:
        yield (
            str(quantity_takeoff.id),
            str(item.id),
            str(quantity_takeoff.project_id),
            str(quantity_takeoff.source_file_id),
            str(quantity_takeoff.drawing_revision_id),
            str(quantity_takeoff.source_job_id),
            _escape_spreadsheet_text_cell(_stringify_scalar(item.item_kind)),
            _escape_spreadsheet_text_cell(item.quantity_type),
            _format_quantity_item_value(item.value),
            _escape_spreadsheet_text_cell(item.unit),
            _escape_spreadsheet_text_cell(_stringify_scalar(item.review_state)),
            _escape_spreadsheet_text_cell(_stringify_scalar(item.validation_status)),
            _escape_spreadsheet_text_cell(_stringify_scalar(item.quantity_gate)),
            _escape_spreadsheet_text_cell(item.source_entity_id or ""),
            _format_optional_json(item.excluded_source_entity_ids_json),
            _normalize_datetime(item.created_at),
        )


def _iter_estimate_rows(
    estimate_version: EstimateVersion,
    items: Sequence[EstimateItem],
) -> Iterable[Sequence[str]]:
    for item in items:
        yield (
            str(estimate_version.id),
            str(item.id),
            str(estimate_version.project_id),
            str(estimate_version.source_file_id),
            str(estimate_version.drawing_revision_id),
            str(estimate_version.quantity_takeoff_id),
            str(estimate_version.source_job_id),
            str(item.line_number),
            _escape_spreadsheet_text_cell(item.line_key),
            _escape_spreadsheet_text_cell(item.line_type),
            _escape_spreadsheet_text_cell(item.description),
            _escape_spreadsheet_text_cell(item.currency),
            _format_optional_decimal(item.quantity_value, quantum=_RATE_QUANTITY_QUANTUM),
            _escape_spreadsheet_text_cell(item.quantity_unit or ""),
            _format_optional_decimal(item.unit_rate_amount, quantum=_RATE_QUANTITY_QUANTUM),
            item.effective_date.isoformat() if item.effective_date is not None else "",
            _format_decimal(item.subtotal_amount, quantum=_MONEY_QUANTUM),
            _format_decimal(item.tax_amount, quantum=_MONEY_QUANTUM),
            _format_decimal(item.total_amount, quantum=_MONEY_QUANTUM),
            _format_optional_json(item.rounding_json),
            _format_optional_uuid(item.quantity_snapshot_entry_id),
            _format_optional_uuid(item.rate_snapshot_entry_id),
            _format_optional_uuid(item.material_snapshot_entry_id),
            _format_optional_uuid(item.formula_snapshot_entry_id),
            _format_optional_uuid(item.assumption_snapshot_entry_id),
            _normalize_datetime(item.created_at),
        )


def _build_result(
    content_bytes: bytes,
    *,
    generator_name: str,
    generator_version: str,
) -> CsvExportResult:
    return CsvExportResult(
        content_bytes=content_bytes,
        checksum_sha256=hashlib.sha256(content_bytes).hexdigest(),
        size_bytes=len(content_bytes),
        media_type=CSV_EXPORT_MEDIA_TYPE,
        generator_name=generator_name,
        generator_version=generator_version,
    )


def _render_csv_bytes(
    headers: Sequence[str],
    rows: Iterable[Sequence[str]],
) -> bytes:
    buffer = StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(list(headers))
    for row in rows:
        writer.writerow(list(row))
    return buffer.getvalue().encode("utf-8")


def _format_quantity_item_value(value: float | None) -> str:
    if value is None:
        return ""
    if not math.isfinite(value):
        raise QuantityCsvExportError("Quantity item value must be finite for CSV export.")
    if value == 0:
        return "0"
    return format(value, ".15g")


def _format_decimal(value: Decimal, *, quantum: Decimal) -> str:
    normalized = value.quantize(quantum, rounding=ROUND_HALF_UP)
    if normalized.is_zero():
        normalized = abs(normalized)
    return format(normalized, "f")


def _format_optional_decimal(value: Decimal | None, *, quantum: Decimal) -> str:
    if value is None:
        return ""
    return _format_decimal(value, quantum=quantum)


def _format_optional_uuid(value: UUID | None) -> str:
    return "" if value is None else str(value)


def _escape_spreadsheet_text_cell(value: str) -> str:
    """Prefix formula-like text with an apostrophe for spreadsheet safety."""

    if value.startswith(_SPREADSHEET_FORMULA_PREFIXES):
        return f"'{value}"
    return value


def _format_optional_json(value: object) -> str:
    if value is None:
        return ""
    normalized = _normalize_json_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _normalize_json_value(value: object) -> JSONValue:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list | tuple):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {
            _normalize_mapping_key(key): _normalize_json_value(item) for key, item in value.items()
        }
    raise TypeError(f"Unsupported CSV export JSON value type: {type(value)!r}")


def _normalize_mapping_key(key: object) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, UUID):
        return str(key)
    if isinstance(key, datetime):
        return _normalize_datetime(key)
    if isinstance(key, date):
        return key.isoformat()
    if isinstance(key, Decimal):
        return format(key, "f")
    if isinstance(key, (int, float, bool)):
        return str(key)
    raise TypeError(f"Unsupported CSV export mapping key type: {type(key)!r}")


def _normalize_datetime(value: datetime) -> str:
    normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _stringify_scalar(value: object) -> str:
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, str):
        return enum_value
    if isinstance(value, str):
        return value
    raise TypeError(f"Unsupported CSV export scalar value: {type(value)!r}")
