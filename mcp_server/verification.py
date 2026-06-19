"""Agent-facing verification helpers — reshape the API validation report.

Pure functions over the ``/validation-report`` payload (no new computation): a
compact ``verify`` verdict and a ``explain`` view of a single finding. Kept
separate from the server wiring so they are unit-testable without a transport.
"""

from typing import Any

# Check statuses that mean "not a clean pass".
_FAIL_STATUSES = frozenset({"fail"})
_WARN_STATUSES = frozenset({"warning", "review_required"})
_TOP_UNMAPPED_REASONS = 3


def summarize_verification(revision_id: str, report: dict[str, Any]) -> dict[str, Any]:
    """Compact, actionable verdict for a revision from its validation report."""

    status = report.get("validation_status")
    checks = report.get("checks") or []
    failed = [c.get("check_key") for c in checks if c.get("status") in _FAIL_STATUSES]
    warned = [c.get("check_key") for c in checks if c.get("status") in _WARN_STATUSES]

    coverage = report.get("coverage") or {}
    entities = coverage.get("entities") or {}
    mapped_ratio = entities.get("mapped_ratio")
    unmapped_by_reason = coverage.get("unmapped_by_reason") or {}
    top_unmapped = dict(
        sorted(unmapped_by_reason.items(), key=lambda kv: kv[1], reverse=True)[
            :_TOP_UNMAPPED_REASONS
        ]
    )

    findings = report.get("findings") or []

    return {
        "revision_id": revision_id,
        "validation_status": status,
        "usable": status != "invalid",
        "checks": {"failed": failed, "warnings": warned},
        "coverage": {
            "mapped_ratio": mapped_ratio,
            "mapped": entities.get("mapped"),
            "total": entities.get("total"),
            "review_flagged_entities": coverage.get("review_flagged_entities"),
            "top_unmapped_reasons": top_unmapped,
        },
        "findings_total": len(findings),
        "rationale": _rationale(status, failed, warned, mapped_ratio),
    }


def _rationale(status: Any, failed: list[Any], warned: list[Any], mapped_ratio: Any) -> str:
    """One-line human/agent-readable summary of the verdict."""

    parts = [str(status)]
    if failed:
        parts.append(f"{len(failed)} failed check(s)")
    if warned:
        parts.append(f"{len(warned)} warning check(s)")
    if not failed and not warned:
        parts.append("all checks passed")
    if isinstance(mapped_ratio, (int, float)):
        parts.append(f"{round(mapped_ratio * 100)}% of entities mapped")
    return "; ".join(parts)


def explain_finding_from_report(report: dict[str, Any], finding_id: str) -> dict[str, Any]:
    """Resolve a single finding to its detail + the check that raised it."""

    findings = report.get("findings") or []
    finding = next((f for f in findings if f.get("finding_id") == finding_id), None)
    if finding is None:
        return {
            "error": f"Finding {finding_id!r} not found.",
            "available_finding_ids": [f.get("finding_id") for f in findings],
        }

    checks = report.get("checks") or []
    check_key = finding.get("check_key")
    check = next(
        (
            c
            for c in checks
            if c.get("check_key") == check_key or finding_id in (c.get("finding_refs") or [])
        ),
        None,
    )

    return {
        "finding": finding,
        "check": check,
        "target": {"type": finding.get("target_type"), "ref": finding.get("target_ref")},
        "severity": finding.get("severity"),
        "quantity_effect": finding.get("quantity_effect"),
    }
