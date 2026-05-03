# Test Fixtures

This directory contains test fixtures for the Draupnir CAD/BIM ingestion system.

## Fixture Strategy v0.1

### Hard Rules

- **Do not commit proprietary or client drawing samples** unless they have been explicitly cleared for use as test fixtures.
- All committed fixtures must be legally safe to distribute.
- Keep generated artifacts out of git unless they are explicitly designated as fixtures.
- Every fixture must be listed in `manifest.yaml`.

### Fixture Tiers

Use the smallest fixture that proves the behavior under test.

| Tier | Purpose | Git policy |
| --- | --- | --- |
| `tiny_synthetic` | Minimal generated sample for deterministic parser and quantity assertions | Usually committed |
| `realistic_public` | Publicly licensed sample that better reflects real drawing structure | Committed only with clear license and source |
| `proprietary_local_only` | Client/proprietary sample used only in local or private validation | **Never committed** |
| `pathological` | Deliberately awkward, malformed, or edge-case sample for robustness checks | Commit only if legally safe |

### Format Acceptance Criteria

Record expectations that are stable today. Do not encode final tolerance math here; capture the fields needed for later policy.

#### DXF

- Prefer `tiny_synthetic` fixtures first.
- Capture expected entity counts, units, layers, and deterministic lengths/areas when known.
- Use for canonical geometry and quantity smoke tests.

#### IFC

- Capture the expected discipline/object scope at a coarse level first (for example walls, slabs, spaces, or openings).
- Record any required unit assumptions and whether quantities are expected to come from model geometry, not hand-entered metadata.
- Keep acceptance focused on stable extraction behavior, not full BIM semantic completeness.

#### Vector PDF

- Record whether geometry is expected to remain vector-native after ingestion.
- Capture expected page count, scale assumptions, layer/group hints if available, and whether text should be extracted separately from linework.
- Use `pathological` fixtures for clipped paths, merged strokes, or unusual page transforms.

#### Raster PDF

- Treat as review-first input.
- Capture expected OCR/tracing limitations in notes.
- Record stable checks such as page count, known scale calibration requirement, and whether the output should remain provisional or review-required.

### Quantity Acceptance Checks

`acceptance_checks.quantities` is the single fixture-level source for quantity acceptance expectations. Use `expected_quantities` only as a compact summary of deterministic expected values that tests may also assert directly.

Each quantity check should be keyed by the quantity name (`line_count`, `total_length`, `total_area`, `wall_count`, and similar) and document the comparison shape needed now, even if global policy values are finalized later in #61.

Recommended check shape:

```yaml
acceptance_checks:
  quantities:
    total_length:
      expected: 10.0
      comparison: exact # or tolerance / review_gated
      tolerance:
        type: absolute # or relative
        value: 0.0
      rounding:
        stage: pre_compare # or post_aggregate / none
        mode: none # optional when stage is none
        places: 3 # optional when rounding applies
      provenance_required: true
      source_entity_refs:
        - entities.LINE:0
      expected_review_state: approved
```

Field guidance:

- `expected`: expected numeric value or count.
- `comparison`: comparison mode for the check. Use `exact` for deterministic equality, `tolerance` when a tolerance window is required, and `review_gated` when the fixture should not pass quantity acceptance without human review.
- `tolerance`: declare the manifest shape now when non-exact comparison may be needed. `type` should be `absolute` or `relative`; `value` may defer to a later policy value from #61 if not yet fixed.
- `rounding`: record whether rounding happens before comparison, after aggregation, or not at all. Include `mode` and `places` only when rounding is expected to matter.
- `provenance_required`: whether the quantity must resolve back to extracted source entities or other stable provenance evidence.
- `source_entity_refs`: optional fixture-level references to the source entities, groups, or canonical extraction identifiers that should explain the quantity.
- `expected_review_state`: use when quantity acceptance is intentionally gated by review posture rather than pure numeric comparison.

Use `review_gated` for raster/review-first inputs and any fixture where quantities must remain provisional until review. Do not invent global tolerance defaults in the fixture manifest; record the comparison shape and any fixture-specific exception only.

### Safe Manifest Fields

Each fixture entry should use safe, reviewable metadata only:

- `filename`: Path relative to `tests/fixtures`
- `format`: File format such as `DXF`, `IFC`, or `PDF`
- `tier`: Fixture tier from the table above
- `source`: Origin of the fixture (`generated`, public URL/reference, or local-only description)
- `license`: SPDX identifier or `Proprietary-Local-Only` for non-committed local samples
- `allowed_in_git`: Whether the file may be committed
- `units`: Measurement units used for extraction expectations
- `expected_extraction_notes`: Stable extraction expectations and caveats
- `expected_review_state`: Expected review posture when relevant
- `expected_validation_status`: Expected validation posture when relevant
- `expected_quantities`: Stable quantity expectations
- `acceptance_checks`: Structured checks for format-specific and quantity-specific expectations

Do not store secrets, client names, private URLs, or sensitive project details in the manifest.

### Adding or Updating Fixtures

1. Create or obtain the fixture file.
2. Confirm the fixture belongs in the right tier.
3. Ensure the license/source is safe for the intended git policy.
4. Add or update the `manifest.yaml` entry with stable expectations only.
5. Update this README if a new fixture tier or acceptance pattern is introduced.
