"""Shared check constructors for validation report checks."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .._utils import _json_compatible  # noqa: TID252


def _pass_check(check_key: str, summary_message: str) -> dict[str, Any]:
    return _check(
        check_key=check_key,
        status="pass",
        summary_message=summary_message,
        details={"applicable": True},
    )


def _check(
    *,
    check_key: str,
    status: str,
    summary_message: str,
    finding_refs: list[str] | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "check_key": check_key,
        "status": status,
        "summary_message": summary_message,
        "finding_refs": [] if finding_refs is None else finding_refs,
        "details": {"applicable": True} if details is None else _json_compatible(dict(details)),
    }


def _not_applicable_check(check_key: str, summary_message: str) -> dict[str, Any]:
    return _check(
        check_key=check_key,
        status="pass",
        summary_message=summary_message,
        details={"applicable": False},
    )
