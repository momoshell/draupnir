"""Process resource-limit helpers for untrusted parser child processes."""

from __future__ import annotations

import math
import resource
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ParserProcessLimits:
    """Best-effort POSIX resource caps for a parser child process."""

    max_file_size_bytes: int | None = None
    max_address_space_bytes: int | None = None
    max_cpu_seconds: int | float | None = None


def apply_parser_process_limits(
    limits: ParserProcessLimits,
    *,
    resource_module: Any = resource,
) -> None:
    """Apply best-effort parser process limits in the current process.

    This is intentionally no-throw: unsupported limits, platform errors, and
    preexisting lower hard limits should not prevent the child process from
    reporting its normal adapter error path.
    """

    _set_limit(
        resource_module,
        "RLIMIT_FSIZE",
        _positive_int_or_none(limits.max_file_size_bytes),
    )
    _set_limit(
        resource_module,
        "RLIMIT_AS",
        _positive_int_or_none(limits.max_address_space_bytes),
    )
    _set_limit(
        resource_module,
        "RLIMIT_CPU",
        _positive_int_or_none(limits.max_cpu_seconds),
    )


def _positive_int_or_none(value: int | float | None) -> int | None:
    if value is None:
        return None
    if value <= 0:
        return None
    return max(1, math.ceil(value))


def _set_limit(resource_module: Any, limit_name: str, requested_limit: int | None) -> None:
    if requested_limit is None:
        return

    limit_kind = getattr(resource_module, limit_name, None)
    if limit_kind is None:
        return

    try:
        _soft_limit, hard_limit = resource_module.getrlimit(limit_kind)
    except (OSError, ValueError):
        return

    infinity = getattr(resource_module, "RLIM_INFINITY", -1)
    target_limit = requested_limit if hard_limit == infinity else min(requested_limit, hard_limit)
    if target_limit <= 0:
        return

    try:
        resource_module.setrlimit(limit_kind, (target_limit, target_limit))
    except (OSError, ValueError):
        return
