# Silver cleaning (A6)

`src/silver/clean.py` converts a Bronze DataFrame to the proposed A5 Silver contract.
It is deliberately separate from Person B's reject/correct/flag policy and from A7
enrichment and writing.

## Transformations

- Select and rename the 74 modelled fields to stable English `snake_case` names.
- Trim all text and represent empty or whitespace-only values as null.
- Keep identifiers and codes as strings, including leading zeroes.
- Parse source dates from `dd.MM.yyyy`.
- Parse document version timestamps with nanoseconds and convert Norwegian local
  `Europe/Oslo` time to UTC, including daylight-saving rules.
- Keep landing clock time as local text because the source lacks a reliable time zone
  for all domestic and foreign landings.
- Convert decimal commas to decimal points and cast measures to `decimal(24,6)`.
- Express weights in kilograms through `_kg` target names and catch value as `_nok`.
- Convert the partial-landing signal `0`/`1` to boolean.
- Remove rows that become identical across all non-provenance Silver fields.
- Preserve Bronze source name, file, period, archive SHA-256, ingestion time and run ID.

Malformed dates, numbers and flags become null. They are not silently corrected or
removed. The quality rules and agreed A/B policy must decide whether each resulting row
is retained, flagged or quarantined. Conflicting rows on the business key are also not
resolved by `clean.py`; the version policy from A5 requires a separate explicit step.

## Validation

Tests cover Norwegian dates, nanosecond timestamps and UTC conversion, decimal commas,
leading zeroes, trimming, blank values, invalid casts, exact duplicates, incomplete
inputs and the checked-in 133-field source fixture. The compact run record is
[`evidence/a6-validation.json`](evidence/a6-validation.json).

Run in the Linux Spark environment:

```bash
PYTHONPATH=src pytest tests/silver -q
```
