# Weather Source Provenance Audit

Date: 2026-05-12

## Rules Enforced

Full-checkpoint promotion remains strict. Rows are excluded from primary clean
promotion when:

- fair value is null or invalid
- settlement is missing or non-binary
- a clean provenance row has `source_asof_ts > checkpoint_ts`
- provenance is degraded, repaired, late-only, or unknown

The promotion path now groups provenance diagnostics and emits operator next
actions instead of silently reporting a single aggregate rejection count.

## Decision

Keep source-provenance promotion strict. Do not dilute full-checkpoint evidence
with late-only or repaired rows. Use degraded rows for diagnostics and data
pipeline repair prioritization only.

## Next Actions

- Treat `clean_provenance_asof_violation` as a pipeline timestamp bug until
  proven otherwise.
- Keep external forecast repair rows out of primary promotion.
- Watch the grouped diagnostics in decision-corpus reports; target less than
  10% rejected rows before relying on promotion metrics as production truth.

