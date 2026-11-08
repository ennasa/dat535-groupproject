# Person B handoff: remaining work

Person A's local chain A1–A10 is implemented through an explicitly unvalidated
Gold/consumer preview. The following work is still required before the pipeline can be
called trusted, deployed or complete.

## 1. Joint approval required now

Review [`silver-model.md`](silver-model.md) and `src/silver/model.py`, then agree on:

- one document product line as the Silver grain;
- the five-field business key;
- the selected 74 fields, types and nullable fields;
- duplicate and annual source-revision handling;
- reference-data sources and retain-and-flag left-join policy.

Only after agreement, change `APPROVAL_STATUS` to `"approved"` and record approval in
the issue/PR. The current gate intentionally blocks normal Silver publication.

## 2. Person B Silver components

### B4 — Rules engine

Implement `silver.rules.apply_rules(df, rules)` with the frozen contract in
`docs/design.md`. It must return accepted rows, rejected rows and `RuleResult` counts.
At minimum, agree and configure rules for:

- non-null and uniqueness of the five-field key;
- invalid required casts;
- negative weights;
- the one missing round-weight row;
- zero round weight (currently 169,439 source rows; do not reject without agreement);
- missing landing county and FAO mapping;
- unknown configured reference codes.

### B5 — Reject / correct / flag policy

Agree each action with Person A and add the rules to `config/base.yaml`. The existing
A4 counts are the baseline. Missing values must not be silently converted to zero.

### B6 — Quarantine

Implement the quarantine writer under
`quarantine/<table>/run_id=<run-id>/`, including failed rule names. Connect it to
`silver.transform.run`; the current transform refuses to publish when rejected rows
exist but no writer is supplied.

### B7 — Quality report

Write per-run before/after, invalid, flagged, accepted and rejected counts to
`reports/quality/<run-id>.json`. Reconcile its rejected count with quarantine.

### B8 — Silver tests

Add tests for every configured rule, correction, rejection, flag and quarantine path.
Person A's schema, cleaning, enrichment and orchestration tests already pass.

## 3. Remaining Bronze/platform work

- Metadata, checksum manifests and ingestion tests exist locally.
- Finish and test the documented B2 `--force` behavior; it is not implemented.
- Register dev/prod self-hosted runners on the school VMs.
- Confirm Python 3.11, Java 17, Spark 3.5.3 and storage paths.
- Complete deploy/run workflows, service setup and environment-specific secrets.
- Add structured run logging, scheduled execution, alerting and failure recovery.

## 4. Gold validation and publication (B10)

Implement `gold.validate` and `gold.publish`:

- reconcile fact line counts and round-weight totals with trusted Silver;
- verify dimension foreign keys and unique Gold grain;
- block promotion after any failed Silver/Gold check;
- retain the previous published Gold on failure;
- atomically promote a candidate and record the latest successful run.

`gold.build` currently creates immutable candidates only. The local A9/A10 candidate is
explicitly unvalidated and must never be promoted.

## 5. Joint final runs and evidence

After the components above are connected:

1. Run trusted Bronze → Silver → Gold on dev.
2. Repeat A8 baseline and optimized runs on the same school VM and capture Spark UI
   Jobs/Stages screenshots.
3. Regenerate `03_gold_consumer.ipynb` from published Gold.
4. Run and document failure/recovery scenarios.
5. Deploy a tagged production release and complete the scheduled end-to-end run.
6. Finish PR review, issues, report, presentation, demo and evidence collection.
