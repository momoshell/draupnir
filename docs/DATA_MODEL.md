# Data Model

This document describes the planned Postgres model. Exact column names may
change during implementation, but these concepts should remain stable.

## Core Tables

### projects

- id
- name
- description
- default_unit_system
- default_currency
- created_at
- updated_at

### files

- id
- project_id
- original_filename
- media_type
- detected_format
- storage_uri
- size_bytes
- checksum_sha256
- immutable
- created_at

### jobs

- id
- project_id
- file_id
- job_type
- status
- attempts
- max_attempts
- cancel_requested
- error_code
- error_message
- started_at
- finished_at
- created_at

### job_events

- id
- job_id
- level
- message
- data_json
- created_at

## Drawing Tables

### drawing_revisions

- id
- project_id
- source_file_id
- parent_revision_id
- changeset_id
- revision_number
- source_format
- normalized_unit
- original_unit
- conversion_factor
- confidence
- created_at

### layers

- id
- drawing_revision_id
- name
- color
- line_type
- attributes_json

### blocks

- id
- drawing_revision_id
- name
- source_ref
- insertion_count
- attributes_json

### entities

- id
- drawing_revision_id
- source_file_id
- source_ref
- entity_type
- layer_id
- block_id
- bbox_json
- geometry_summary_json
- attributes_json
- provenance_json
- confidence
- created_at

The `entities` table stores full normalized entity records. Large raw payloads
may also be stored in artifact storage when needed, but queryable normalized
fields should remain in Postgres.

## Quantity Tables

### quantity_takeoffs

- id
- project_id
- drawing_revision_id
- status
- unit_system
- created_by_job_id
- created_at

### quantity_items

- id
- quantity_takeoff_id
- quantity_type
- value
- unit
- normalized_value
- normalized_unit
- entity_refs_json
- formula
- provenance_json
- confidence

Quantity types include:

- count
- length
- area
- volume
- mass
- perimeter
- bounding_box

## Estimation Tables

### rate_catalogs

- id
- name
- currency
- region
- effective_from
- effective_to
- created_at

### rates

- id
- rate_catalog_id
- code
- description
- unit
- unit_price
- source
- manual
- confidence
- attributes_json

### estimates

- id
- project_id
- quantity_takeoff_id
- rate_catalog_id
- currency
- status
- assumptions_json
- created_by_job_id
- created_at

### estimate_items

- id
- estimate_id
- description
- quantity_item_id
- rate_id
- quantity
- unit
- unit_price
- subtotal
- assumptions_json
- provenance_json

## Revision Tables

### cad_change_sets

- id
- project_id
- base_drawing_revision_id
- status
- proposed_by
- source
- summary
- created_at
- approved_at

Sources include:

- user
- agent
- system

### cad_change_operations

- id
- changeset_id
- operation_type
- target_entity_id
- parameters_json
- reason
- status
- validation_result_id
- created_at

### validation_results

- id
- project_id
- changeset_id
- status
- errors_json
- warnings_json
- created_at

## Artifact Tables

### export_artifacts

- id
- project_id
- drawing_revision_id
- estimate_id
- changeset_id
- artifact_type
- storage_uri
- media_type
- size_bytes
- checksum_sha256
- created_by_job_id
- created_at

