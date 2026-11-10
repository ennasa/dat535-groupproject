# Proposed Silver model (A5)

Status: **awaiting joint approval from Person A and Person B**.

## Record and key

One Silver record represents the current version of one document product line. The
proposed business key is:

1. `catch_year`
2. `sales_org_code`
3. `document_type_code`
4. `document_number`
5. `line_number`

A4 found every component populated and no duplicate key groups across all 1,079,929
records. `document_number` remains a string identifier; leading zeroes must not be lost.

## Schema

`src/silver/model.py` is the authoritative proposed contract. It maps selected Bronze
fields to stable English `snake_case` names and assigns target Spark types. It retains:

- document identity and version fields;
- landing time and geography;
- vessel, catch area and gear attributes;
- FDIR, consolidated and FAO species classifications;
- product classifications, quantities, weights and catch value;
- Bronze source and ingestion provenance.

Codes remain strings. Dates use `date`, known date-time fields use UTC `timestamp`, flags
use `boolean`, counts use `long`, and measures use `decimal(24,6)`. Weights are named
with the `_kg` unit. Nullable fields follow measured A4 coverage. Bronze remains the
complete source archive for fields not selected for this reusable analytical model.

`document_version_at_utc` converts the Directorate's Norwegian local system time with
the `Europe/Oslo` zone, including daylight-saving rules. `landing_time_local`
deliberately remains a time string: the source does not provide a reliable time zone for
every domestic and foreign landing, so the model does not invent a UTC instant.
`source_updated_date` remains a date because the 2023 source contains no clock time.
Ingestion time is already a genuine UTC timestamp.

## Duplicate and history policy

1. Remove exact duplicate rows defensively.
2. For a repeated key, keep the greatest tuple of document version number, document
   version timestamp and source update date.
3. If tied rows disagree outside the key, quarantine them instead of choosing by
   arrival order.

Silver uses one approved, completed Bronze snapshot per catch year. An approved revised
archive rebuilds and atomically replaces only that year's Silver partition. Every row
keeps the source archive SHA-256, so the selected history is reproducible. Bronze keeps
all earlier source versions.

## Reference joins proposed for A7

Species (FDIR and FAO), landing municipality/country and gear are joined by their stable
codes. Each join must be validated as many-to-one, use a left join and retain unmatched
records with a quality flag. A5 does not invent or embed reference datasets; Person B
must confirm the configured sources and uniqueness checks before A7.

## Approval checklist

Person B should confirm:

- the grain and five-field key;
- the selected schema and nullable fields;
- the deterministic duplicate and annual revision policies;
- which governed reference datasets are available on the platform;
- that left joins with retain-and-flag are compatible with the rules engine.

After both people agree, change `APPROVAL_STATUS` in `model.py`, record the review in
the issue/PR and mark A5 completed in `docs/design.md`.
