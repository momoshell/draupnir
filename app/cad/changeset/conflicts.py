"""Pure helpers for CAD changeset apply conflict payloads."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from app.cad.changeset.contracts import (
    ChangeSetApplyConflict,
    ChangeSetApplyConflictDetails,
    ChangeSetApplyConflictTarget,
    RevisionRef,
)


def build_stale_base_conflict_details(
    *,
    change_set_id: uuid.UUID,
    base_revision: RevisionRef,
    current_revision: RevisionRef,
    conflicting_targets: Sequence[ChangeSetApplyConflictTarget] = (),
) -> ChangeSetApplyConflictDetails | None:
    if _is_matching_revision(base_revision=base_revision, current_revision=current_revision):
        return None

    return ChangeSetApplyConflictDetails(
        base_revision_id=base_revision.revision_id,
        base_revision_sequence=base_revision.revision_sequence,
        current_revision_id=current_revision.revision_id,
        current_revision_sequence=current_revision.revision_sequence,
        change_set_id=change_set_id,
        conflicting_targets=tuple(conflicting_targets),
    )


def build_stale_base_conflict(
    *,
    change_set_id: uuid.UUID,
    base_revision: RevisionRef,
    current_revision: RevisionRef,
    conflicting_targets: Sequence[ChangeSetApplyConflictTarget] = (),
) -> ChangeSetApplyConflict | None:
    details = build_stale_base_conflict_details(
        change_set_id=change_set_id,
        base_revision=base_revision,
        current_revision=current_revision,
        conflicting_targets=conflicting_targets,
    )
    if details is None:
        return None
    return ChangeSetApplyConflict(details=details)


def _is_matching_revision(*, base_revision: RevisionRef, current_revision: RevisionRef) -> bool:
    return (
        base_revision.revision_id == current_revision.revision_id
        and base_revision.revision_sequence == current_revision.revision_sequence
    )
