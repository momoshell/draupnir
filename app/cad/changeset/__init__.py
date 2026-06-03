"""Pure CAD changeset apply contracts and conflict helpers."""

from app.cad.changeset.apply import (
    ALLOWED_PROPERTY_PATHS,
    SUPPORTED_OPERATION_TYPES,
    apply_change_set,
)
from app.cad.changeset.conflicts import (
    build_stale_base_conflict,
    build_stale_base_conflict_details,
)
from app.cad.changeset.contracts import (
    AppliedEntity,
    ChangeSetApplyConflict,
    ChangeSetApplyConflictDetails,
    ChangeSetApplyConflictTarget,
    ChangeSetApplyEffects,
    ChangeSetApplyError,
    ChangeSetApplyResult,
    ChangeSetApplySuccess,
    ChangeSetOperation,
    ChangeSetOperationTarget,
    RevisionEntitySnapshot,
    RevisionRef,
)
from app.cad.changeset.loading import (
    ChangeSetApplyLoadError,
    LoadedChangeSetApplyInput,
    load_and_apply_change_set,
    load_change_set_apply_input,
)

__all__ = [
    "ALLOWED_PROPERTY_PATHS",
    "SUPPORTED_OPERATION_TYPES",
    "AppliedEntity",
    "ChangeSetApplyConflict",
    "ChangeSetApplyConflictDetails",
    "ChangeSetApplyConflictTarget",
    "ChangeSetApplyEffects",
    "ChangeSetApplyError",
    "ChangeSetApplyLoadError",
    "ChangeSetApplyResult",
    "ChangeSetApplySuccess",
    "ChangeSetOperation",
    "ChangeSetOperationTarget",
    "LoadedChangeSetApplyInput",
    "RevisionEntitySnapshot",
    "RevisionRef",
    "apply_change_set",
    "build_stale_base_conflict",
    "build_stale_base_conflict_details",
    "load_and_apply_change_set",
    "load_change_set_apply_input",
]
