"""Candidate selection helpers for ingestion runners."""

from __future__ import annotations

from dataclasses import dataclass

from app.ingestion.contracts import AdapterDescriptor, InputFamily, UploadFormat
from app.ingestion.registry import (
    descriptors_for_upload_format,
    get_descriptor,
    get_export_descriptor,
    get_export_descriptors,
)


@dataclass(frozen=True, slots=True)
class AdapterCandidate:
    """A concrete adapter candidate for a source upload."""

    upload_format: UploadFormat
    input_family: InputFamily
    descriptor: AdapterDescriptor


def select_adapter_candidates(
    detected_format: str | UploadFormat | InputFamily | None,
    *,
    media_type: str | None,
) -> tuple[AdapterCandidate, ...]:
    """Resolve ordered adapter candidates for a detected upload."""
    input_family = resolve_input_family(detected_format)
    if input_family is not None:
        descriptor = get_descriptor(input_family)
        descriptor_upload_format = descriptor.upload_formats[0]
        return (AdapterCandidate(descriptor_upload_format, input_family, descriptor),)

    upload_format = resolve_upload_format(detected_format, media_type=media_type)
    if upload_format is None:
        raise ValueError("Unsupported upload format for ingestion.")

    return tuple(
        AdapterCandidate(upload_format, descriptor.family, descriptor)
        for descriptor in descriptors_for_upload_format(upload_format)
    )


def _normalize_export_format(output_format: str) -> str:
    normalized = output_format.strip().lower()
    if not normalized:
        raise ValueError("Unsupported export format for ingestion.")
    return normalized


def select_export_candidates(output_format: str) -> tuple[AdapterDescriptor, ...]:
    """Resolve ordered export-capable adapter descriptors for a requested format."""

    normalized_output_format = _normalize_export_format(output_format)
    try:
        return get_export_descriptors(normalized_output_format)
    except KeyError as exc:
        raise ValueError("Unsupported export format for ingestion.") from exc


def select_export_descriptor(output_format: str) -> AdapterDescriptor:
    """Resolve a single export-capable adapter descriptor for a requested format."""

    normalized_output_format = _normalize_export_format(output_format)
    try:
        return get_export_descriptor(normalized_output_format)
    except KeyError as exc:
        raise ValueError("Unsupported export format for ingestion.") from exc


def resolve_input_family(
    detected_format: str | UploadFormat | InputFamily | None,
) -> InputFamily | None:
    """Resolve a concrete input family when already known."""
    if isinstance(detected_format, InputFamily):
        return detected_format

    if isinstance(detected_format, UploadFormat) or detected_format is None:
        return None

    normalized = detected_format.strip().lower()
    try:
        return InputFamily(normalized)
    except ValueError:
        return None


def resolve_upload_format(
    detected_format: str | UploadFormat | InputFamily | None,
    *,
    media_type: str | None,
) -> UploadFormat | None:
    """Resolve an upload format from immutable file metadata."""
    if isinstance(detected_format, UploadFormat):
        return detected_format

    if isinstance(detected_format, InputFamily):
        descriptor = get_descriptor(detected_format)
        return descriptor.upload_formats[0]

    if detected_format is not None:
        normalized = detected_format.strip().lower()
        try:
            return UploadFormat(normalized)
        except ValueError:
            pass

    if media_type is not None and media_type.lower() == "application/pdf":
        return UploadFormat.PDF

    return None
