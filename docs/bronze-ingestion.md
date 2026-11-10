# Bronze ingestion

The Bronze job downloads each configured annual catch archive and preserves its
original ZIP and CSV. A separate Parquet dataset contains the source fields as
strings and ingestion metadata. No records are filtered, deduplicated or cast to
analytical types.

## Processing

1. Download the annual archive and calculate its SHA-256.
2. Skip an already completed snapshot with the same checksum.
3. Extract the expected CSV, checking the archive CRC, UTF-8, header and field counts.
4. Compare the header with the versioned 2023 A1 schema baseline.
5. Read the CSV with an explicit string schema and add provenance columns.
6. Write Parquet in a staging directory and reconcile its row count with the CSV.
7. Publish the snapshot by renaming the staging directory; write the run manifest.

Empty CSV fields remain empty strings in Parquet. Whitespace, leading zeros,
decimal commas and source column names are preserved. The original files provide
the byte-for-byte source record.

## Configuration and execution

`config/base.yaml` defines the source name and download URL template.
`config/dev.yaml` selects catch year `2023`; production periods remain explicitly
unconfigured. Run from the project root with the required Python, Java and Spark
environment active:

```bash
PIPELINE_ENV=dev python src/main.py --layers bronze
```

Use a new run ID for each invocation, or omit `--run-id` to generate a UUID.
On Windows, use a configured Linux environment through WSL or the project VM;
native Windows Parquet writes require additional Hadoop tooling.

## Stored output

```text
data/dev/
  bronze/fiskeridir_catch/period=2023/source_version=<archive-sha256>/
    fangstdata_2023.csv.zip
    fangstdata_2023.csv
    records/                   # Parquet
    _snapshot.json             # Completed snapshot metadata
  manifests/<run-id>.json
```

The Parquet metadata columns are `_source_name`, `_source_file`, `_source_period`,
`_source_sha256`, `_ingested_at` (UTC) and `_run_id`. The manifest records the URL,
download time, archive and CSV hashes, file sizes, row count, output and run status.

The expected raw schema is stored in `src/bronze/expected_schema.json`. It contains
the 133 ordered source fields observed during A1, all represented as strings. Each
snapshot and run manifest records whether the schema was unchanged and lists added,
missing, reordered or retyped fields. CSV has no declared types, so normal ingestion
reports every raw field as `string`; the comparator also supports explicit type maps.

An unchanged archive is downloaded for comparison but does not rewrite Bronze.
A revised archive creates another snapshot; earlier versions remain available.
Downstream jobs must select one recorded snapshot per catch year. Skipped files
contribute zero newly ingested rows to the layer result.

A download, parsing, write or reconciliation failure returns a failed layer result
and a nonzero CLI exit. Failed staging data are removed, existing snapshots remain
available, and a later invocation can retry. Jobs targeting the same storage should
run serially. Administrative overwrite remains separate work.

A schema-changed source is different: its original ZIP, CSV and Parquet snapshot are
preserved for inspection, and the schema report is recorded. Bronze then returns a
failed result, preventing downstream layers from using the snapshot until the change
has been reviewed and the expected schema has deliberately been updated.

## Validation

```bash
ruff check .
PIPELINE_ENV=test pytest
```

Tests use local archives, including a two-record fixture with the actual 133-column
schema. They cover byte and field preservation, provenance, unchanged reruns,
revisions, invalid inputs, schema changes, retry after failure and CLI failure propagation.

Local validation used Ubuntu WSL, Python 3.12.3, Java 17 and PySpark 3.5.3.
CI remains configured for the project's Python 3.11 target.

| Check | Result |
|---|---|
| Lint | Passed |
| Automated tests | 34 passed |
| Real 2023 dev run | 1,079,929 CSV rows reconciled with 1,079,929 Parquet rows |
| Repeated dev run | Skipped; no new snapshot or rewritten records |
| A3 schema check | Stored 2023 snapshot matched all 133 expected fields |
| A4 full-data profile | 1,079,929 rows and 133 fields profiled; no duplicate rows or candidate keys |
| Stored ZIP and CSV hashes | Verified against the manifest |

Run IDs: `a2-2023-first` and `a2-2023-rerun`. The compact
[validation record](evidence/a2-validation.json) identifies the input hashes and
manifests. [A3 validation](evidence/a3-validation.json) records the schema test and
real-snapshot check. [A4 profiling evidence](evidence/a4-profile.json) contains the
complete per-field metrics and counted findings. This verifies A2-A4 locally;
deployment to the school VM is separate work.

Source: [Fiskeridirektoratet, catch data linked with vessel data](https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata).
The source exploration and field definitions are recorded in
[`01_explore_source.ipynb`](../notebooks/01_explore_source.ipynb).
