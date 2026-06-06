# Local DWG/PDF Extraction Comparison Checklist

Use this checklist after a local-only DWG, vector PDF, or raster PDF review run when you need to compare the source drawing against extracted outputs without committing proprietary artifacts or sensitive notes.

## Hard rules

- Keep the source sample, extracted outputs, overlays, and notes local-only.
- Do not commit screenshots, raw extraction snippets, converted JSON, OCR dumps, private paths, IDs, sample names, or sample-derived sensitive metadata.
- Use neutral placeholders when you need to capture an example for discussion.

## Review record

- Review date: `<YYYY-MM-DD>`
- Input type: `<DWG | vector PDF | raster PDF>`
- Reviewer: `<initials-or-role>`
- Local reference label: `<neutral-placeholder>`

## 1. Title-block identity

- Expected title-block fields present: `<yes | partial | no>`
- Drawing identity appears consistent with the source: `<yes | partial | no>`
- Revision/date/sheet markers recovered well enough for human comparison: `<yes | partial | no>`
- Notes:
  - `<neutral observation>`

## 2. Text and label recovery

- Key labels are present and readable: `<yes | partial | no>`
- Text placement is close enough to support review: `<yes | partial | no>`
- Important text is missing, duplicated, or garbled: `<none | minor | material>`
- Notes:
  - `<neutral observation>`

## 3. Geometry and overlay comparison

- Major linework/features appear in the extracted output: `<yes | partial | no>`
- Overlay alignment looks acceptable: `<yes | partial | no>`
- Missing or misplaced geometry is: `<none | minor | material>`
- Extents/clustering issues are: `<none | minor | material>`
- Notes:
  - `<neutral observation>`

## 4. Layer and entity-type mapping

- Important layers/layouts are attached where expected: `<yes | partial | no>`
- Entity types look plausible for the source content: `<yes | partial | no>`
- Unknown or fallback mappings are: `<expected | elevated | blocking>`
- Notes:
  - `<neutral observation>`

## 5. Units and scale

- Extracted units match expectation: `<yes | no | unknown>`
- PDF scale/calibration status: `<not needed | complete | still required | unknown>`
- Units/scale issue would make quantities untrusted: `<yes | no>`
- Notes:
  - `<neutral observation>`

## 6. Validation status

- Validation result: `<valid | valid_with_warnings | needs_review | invalid>`
- Findings needing follow-up before trust: `<none | minor | material>`
- Notes:
  - `<neutral observation>`

Interpretation:

- `valid`: acceptable for downstream trust checks.
- `valid_with_warnings`: acceptable only if warnings do not undermine the comparison.
- `needs_review`: human review still required; do not treat as trusted yet.
- `invalid`: blocked for trusted downstream use.

## 7. Quantity gate interpretation

- Quantity run attempted: `<yes | no>`
- Quantity gate: `<allowed | allowed_provisional | review_gated | blocked | not_run>`
- Output trust level: `<trusted | provisional | untrusted | not_run>`
- Notes:
  - `<neutral observation>`

Interpretation:

- `allowed`: acceptable for normal downstream use if the comparison checks also pass.
- `allowed_provisional`: usable only as provisional output.
- `review_gated`: review-first; do not treat totals as trusted.
- `blocked`: do not use the quantity output.

## Trust-before-takeoff decision

Mark the extraction as trusted for takeoff only if all of the following are true:

- Title-block identity and key labels are recovered well enough to confirm you reviewed the right content.
- Geometry and overlay comparison show no material missing or misaligned content.
- Layer/entity-type mapping is plausible for the source.
- Units are known, and PDF scale/calibration is complete when required.
- Validation is `valid` or `valid_with_warnings` without material unresolved findings.
- Quantity gate is `allowed`; if it is `allowed_provisional`, keep the result explicitly provisional and do not treat it as fully trusted.

If any of those checks fail, keep the result untrusted, review-gated, blocked, or provisional as appropriate.
