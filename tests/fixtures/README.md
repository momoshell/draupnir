# Test Fixtures

This directory contains test fixtures for the Draupnir CAD/BIM ingestion system.

## Fixture Policy

### No Proprietary or Client Samples

- **Do not commit proprietary or client drawing samples** unless they have been explicitly cleared for use as test fixtures.
- All fixtures must be legally safe to commit and distribute.

### Generated Fixtures Preferred

- Prefer fixtures that are **generated specifically for this repository**.
- Generated fixtures should use permissive licenses (CC0-1.0, MIT, or equivalent).
- Keep generated artifacts out of git unless they are explicitly designated as fixtures.

### Manifest Required

- **Every fixture must be listed in `manifest.yaml`** with complete metadata.
- The manifest tracks: filename, format, source, license, git status, units, and expected extraction behavior.

### Metadata Requirements

Each fixture entry in the manifest must include:

- `filename`: Path relative to fixtures directory
- `format`: File format (DXF, DWG, PDF, IFC, etc.)
- `source`: Origin of the fixture ("generated", "external", or specific URL)
- `license`: SPDX license identifier (CC0-1.0, MIT, Apache-2.0, etc.)
- `allowed_in_git`: Boolean indicating if the file can be committed
- `units`: Measurement units (meters, millimeters, inches, etc.)
- `expected_extraction_notes`: What the ingestion system should extract
- `expected_quantities`: Known quantities for validation (if applicable)

### Adding New Fixtures

1. Create or obtain the fixture file
2. Ensure it has a clear license compatible with the repository
3. Add an entry to `manifest.yaml` with complete metadata
4. Update this README if the fixture represents a new format or category
