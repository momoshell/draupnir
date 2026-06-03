# Changeset Contract

This document is the source-of-truth contract for Phase 9 changesets and changeset operations. It defines the durable operation payloads, lifecycle, validation, concurrency, and first revised-DXF support expectations used by follow-up implementation issues.

It complements, but does not replace, the higher-level revision model in `docs/TRD.md` and the CAD revision engine overview in `docs/ARCHITECTURE.md`.

## Scope

- proposed changesets against an existing `base_revision_id`
- operation payload schemas and sequencing rules
- validation rules before apply and before export
- status lifecycle and ownership boundaries
- first revised-DXF support matrix

## Non-Goals

- direct mutation of original uploads or previously committed revisions
- implicit merge/rebase of stale changesets
- DWG or IFC write contracts in Phase 9
- AI as a source of truth; agents remain advisory-only and may propose, not directly apply, CAD mutations

## Core Model

- A changeset is prepared against exactly one `base_revision_id`.
- Applying a changeset is append-only: success creates a new applied revision linked to the base revision and the changeset.
- Existing revisions, uploads, and generated artifacts remain immutable.
- Exports are produced from an applied revision, never directly from a proposed changeset.
- Per #64, apply uses an optimistic current-revision check. If the current revision no longer matches the requested base, apply fails with `REVISION_CONFLICT`.

## Operation Envelope

Every operation uses the same discriminated envelope.

| Field | Requirement |
| --- | --- |
| `operation_id` | Stable operation UUID/id within the changeset |
| `sequence_index` | Integer apply order within the changeset |
| `operation_type` | One of the eight operation types below |
| `payload_version` | Operation payload schema version |
| `target` | Target selector for existing entities/blocks/material candidates when applicable |
| `payload` | Type-specific operation body |
| `reason` | Human-readable rationale |
| `provenance` | Proposal provenance including user/agent origin context |
| `expected_source_identity` | Required when present on the target source |
| `expected_source_hash` | Required when present on the target source |
|

Target guards are mandatory for operations that address existing content. When available, the target must include `revision_entity_id` plus expected source identity/hash values so validation and apply can detect drift.

## Target Rules

- Existing-entity operations should target `revision_entity_id` when available.
- Guards must match the same project, file, and base revision lineage as the parent changeset.
- A target mismatch is a validation/apply failure, not a best-effort retarget.
- `remove_entity` is a derived-revision tombstone in the resulting applied revision, not destructive deletion of history.

## Operation Payloads

### `annotate_entity`

Metadata/review operation only by default.

- Target: existing entity
- Payload: annotation metadata, note text, author/source, optional review tags
- Rule: CAD-visible notes, leaders, or text are not implied here; they must use `add_entity`
- DXF note: valid internally as metadata/review operations, but not CAD geometry by default

### `change_layer`

- Target: existing entity
- Payload: destination layer ref/name
- Rule: `change_layer` requires an existing-layer target in the base revision; implicit layer creation is out of scope
- DXF note: part of the narrow first revised-DXF subset

### `add_entity`

- Target: none required for standalone addition; may optionally reference placement context
- Payload: canonical entity payload, provenance, confidence/review metadata, layout/layer refs, and any block context required by the entity type
- Rule: use this for CAD-visible annotations or notes
- DXF note: initially limited to simple DXF-native entities

### `remove_entity`

- Target: existing entity
- Payload: removal reason and optional replacement context
- Rule: produces a derived-revision tombstone/equivalent omission in the applied revision; does not erase prior lineage
- DXF note: part of the narrow first revised-DXF subset

### `replace_block`

- Target: existing block definition or insert scope, as defined by implementation
- Payload: replacement block content plus mapping context for affected inserts/entities
- Rule: block replacement is valid contract surface but outside the first revised-DXF write subset

### `replace_profile_material_candidate`

- Target: existing profile/material candidate association
- Payload: replacement candidate selection plus reason/review context
- Rule: non-geometric contract surface; must preserve auditability of the prior candidate
- DXF note: valid internally but blocked from revised_dxf in Phase 9

### `update_property`

- Target: existing entity or block-scoped object when explicitly supported
- Payload: property path/key and replacement value
- Rule: `update_property whitelist` applies. Only documented non-geometric properties are allowed.
- Initial whitelist: review metadata, classification tags, text content for text-like entities, and simple presentation attributes
- Disallowed: geometry mutation, lineage fields, quantity fields, pricing fields, and any implicit semantic rewrite outside the whitelist
- DXF note: only whitelisted non-geometric updates are in the first revised-DXF subset

### `flag_for_review`

Metadata/review operation only by default.

- Target: existing entity, block, or changeset-scoped review subject
- Payload: review flag code, rationale, severity/priority, optional assignee/context
- Rule: does not itself create CAD-visible geometry
- DXF note: valid internally as metadata/review operations, but blocked from revised_dxf

## Status Lifecycle

Status Lifecycle separates API-owned workflow transitions from worker-owned execution transitions.

### API-owned

- `proposed` — created, editable per workflow policy
- `validation_requested` — validation job/intention requested
- `approved` — accepted for apply
- `rejected` — explicitly rejected

### worker-owned

- `validating` — validation execution in progress
- `validated` — validation completed with no blocking issues for apply/export eligibility
- `validation_failed` — validation execution completed with blocking schema/target/gate failures
- `applying` — apply execution in progress
- `applied` — new drawing revision committed successfully
- `apply_failed` — execution failed after approval
- `revision_conflict` — apply blocked by stale base/current revision mismatch

User/API owns proposal, validation request, approval, and rejection workflow. Workers own execution-derived validation/apply states and results. Approval and application are separate. A changeset may be `approved` and still fail during apply.

## Concurrency And Ownership

- `base_revision_id` is required on every changeset.
- Apply must re-check the current finalized revision immediately before commit.
- The #64 contract is authoritative: stale apply returns `REVISION_CONFLICT` with the requested `base_revision_id`, the current revision id, and changeset/apply target context where available.
- API-owned records cover request workflow such as changesets and approval intent.
- Worker-owned records cover execution-derived outputs such as the applied revision, validation reports, export artifacts, and job events.
- Retry of a stale changeset does not auto-merge or silently retarget.

## Validation Gates

Changesets must be validated before export and before apply.

Minimum gates:

- envelope schema and payload-version validity
- allowed operation type and ordering semantics
- target existence within the declared base revision
- lineage match to the same project/file/base revision
- target guard match for `revision_entity_id`, expected identity, and expected hash when present
- `update_property` whitelist enforcement
- finite/valid geometry for any geometric payloads
- layer/layout/block references valid for the resulting revision
- provenance/review metadata shape remains auditable
- cumulative resulting revision remains technically valid after all operations are applied in sequence

Validation success means the changeset is eligible for the documented next step, not that every export format supports every valid operation.

## DXF Support Matrix

The first revised-DXF contract is intentionally narrow.

| Operation | Contract validity | revised_dxf status | Notes |
| --- | --- | --- | --- |
| `annotate_entity` | valid | blocked from revised_dxf | metadata/review operations by default |
| `change_layer` | valid | supported for revised_dxf | existing layer only |
| `add_entity` | valid | supported for revised_dxf | only simple DXF-native entities |
| `remove_entity` | valid | supported for revised_dxf | represented through derived revision output |
| `replace_block` | valid | blocked from revised_dxf | deferred beyond first subset |
| `replace_profile_material_candidate` | valid | blocked from revised_dxf | non-geometric/internal first |
| `update_property` | valid | not directly mapped by revised_dxf writer v1 | metadata-only updates stay internal; later export validation decides revised_dxf eligibility |
| `flag_for_review` | valid | blocked from revised_dxf | metadata/review operations |

### simple DXF-native entities

For Phase 9, `add_entity` support is limited to modelspace `LINE` and simple 2D polyline payloads already representable by the initial write path. Broader geometry, block rewrites, paper-space content, and arbitrary CAD-visible annotation authoring are deferred.

## Export And Apply Relationship

- revised-DXF export operates on an applied revision with changeset lineage
- proposed-only changesets do not produce revised CAD artifacts
- revised_dxf writer v1 fails closed: unsupported entity, geometry, layout, layer, block, unit, or coordinate cases return structured errors and produce no revised-DXF artifact
- a changeset may be valid for internal workflow but still be blocked from revised_dxf if one or more operations are outside the supported subset or only affect internal metadata

## Cross-Reference Summary

- High-level revision semantics: `docs/TRD.md`
- CAD revision engine overview: `docs/ARCHITECTURE.md`
