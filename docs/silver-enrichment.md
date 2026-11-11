# Silver enrichment and orchestration (A7 proposal)

Status: **implemented and integration-tested, but production publication remains gated**
until the shared A5 model and Person B's quality components are approved.

## Enrichment

`src/silver/enrich.py` adds deterministic fields without changing the document-line
grain:

- a SHA-256 `record_id` derived from the five-field business key;
- landing year, month and `yyyy-MM`;
- the documented “delivered in the following year” flag for month code `13`;
- days from last catch date to landing date;
- domestic-landing and FAO-mapping flags.

Optional reference tables use validated many-to-one left joins. Reference keys must be
non-null and unique. Attributes must use `snake_case`, are prefixed with the reference
name and cannot overwrite existing columns. A match flag has three meanings:

- `true`: configured reference and matching code;
- `false`: configured reference but unmatched code;
- `null`: reference table was not configured, so no claim is made.

The proposed reference names are FDIR species, FAO species, landing municipality,
landing country and gear. No unapproved external dataset is embedded in the project.

## Transform flow and publication safety

`src/silver/transform.py` wires the intended flow:

```text
approved Bronze snapshot
        → clean
        → quality rules
        → enrich
        → row reconciliation
        → versioned Silver Parquet snapshot
```

It selects exactly one completed, schema-approved Bronze snapshot per catch year. If
several source revisions exist, `silver.approved_source_versions` must identify one.
Output is staged on the destination filesystem, counted after writing and renamed into
place only after reconciliation. A repeated identical build is skipped.

Two gates prevent an unfinished shared layer from being presented as trusted Silver:

1. The A5 model must have `APPROVAL_STATUS = "approved"`.
2. A quality rules engine must be active.

Tests may explicitly enable `silver.allow_provisional_model` and
`quality.allow_unvalidated_silver`; both default to `false`. Rejected rows require
Person B's quarantine writer, and a rejected ratio above the configured threshold
blocks publication.

## Current boundary

The A7 code, derived fields, reference-join contract, CLI wiring, staging, reconciliation
and idempotency are tested. Since the model approval (#50), the quality rules (#51, #52)
and the quarantine (#53), Silver runs as part of the normal pipeline on dev (2023) and
prod (2023–2025).
