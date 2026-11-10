# DAT535 Medallion Pipeline: Design Document

_TBD_ marks decisions that depend on the dataset (#1) or on profiling (#15).

## 1. Goals and scope

| Part | What we deliver | Weight | Owner |
|---|---|---|---|
| Bronze | Raw source data ingested reproducibly and unchanged, with metadata for traceability and safe reruns | Medium | **A** ingestion · **B** metadata, idempotency, tests |
| Silver | Trusted, validated, reusable dataset: grain and model, cleaning, quality rules, quarantine, before/after quality evidence, performance evidence | **Highest** | **Shared** |
| Gold | Curated dataset for one concrete consumer, validated against Silver, published only when checks pass | Medium | **A** design, build, consumer · **B** validation, publish gate |
| Operations | Feature branches and PRs, CI, dev and prod environments, deployment, scheduled runs, logging, failure handling | Medium | **B** |
| Delivery | 8–10 page report, 10-minute presentation, live demo, run evidence | — | **Shared** |

A owns the data-facing work: understanding, ingesting, cleaning and serving the data. B owns the platform it runs on and the checks around it. Silver is shared because it carries the most weight and both must be able to explain it.

Out of scope: streaming ingestion, a production-grade dashboard, managed cloud services.

## 2. Key concepts

- **Bronze / Silver / Gold:** raw as delivered → cleaned and validated at a fixed grain, reusable → purpose-built for one consumer. Each layer reads only from the one before it.
- **Grain:** what one Silver record represents. Keys, deduplication and rules all follow from it.
- **Quality rule:** a named check with an action: **reject** (to quarantine), **correct** (fix the value) or **flag** (keep, with a marker column).
- **Quarantine:** rejected records, stored per run with the rule that rejected them.
- **Run ID:** identifies one run (GitHub run ID, or a UUID locally). Written to logs, metadata and the manifest.
- **Manifest:** JSON per run listing ingested files (checksum, period) and each layer's result. Makes reruns idempotent.
- **`PIPELINE_ENV`:** `dev`, `prod` or `test`; selects the config file merged on top of `config/base.yaml`.

## 3. Architecture

```
 Source (TBD)  ── batch, per period
        ▼
 BRONZE   bronze/<source>/period=YYYY-MM/          raw + _source_*, _ingested_at, _run_id
          manifests/<run_id>.json
        ▼
 SILVER   silver/<table>/                          typed, deduplicated, validated, enriched
          quarantine/<table>/run_id=<id>/          rejected records
          reports/quality/<run_id>.json            before/after metrics
        ▼
 GOLD     gold/<table>/                            published only if checks pass
        ▼
 Consumer (TBD)

 src/main.py runs the layers in order with one run ID and stops downstream on failure.

 ci.yml      GitHub-hosted runner          lint + pytest on every PR
 deploy.yml  VM runner dev / prod           main → dev, tag v* → prod
 run.yml     VM runner prod                scheduled Bronze → Silver → Gold

 One VM (4 cores, 7.8 GB): runners dev and prod, storage ~/dat535/dev and ~/dat535/prod.
```

All paths are relative to `storage.root` in the environment's config.


## 4. Components

Shared files are agreed in Phase 0; afterwards, changes to them need both people's approval.

```
src/
├── main.py                 # runs layers in order, stop-on-failure                  [shared]
├── common/
│   ├── contracts.py        # RunContext, LayerResult, RuleResult (§5)               [shared]
│   ├── config.py           # base + env YAML merge                                  [B] done
│   ├── spark.py            # SparkSession from config                               [B] done
│   ├── logging.py          # log lines with run_id, layer, counts, duration         [B]
│   └── io.py               # layer paths and read/write helpers                     [B]
├── bronze/
│   ├── source.py           # fetch/read one source period                           [A]
│   ├── ingest.py           # run(): ingest new periods unchanged                    [A]
│   ├── schema.py           # expected schema, detect and log changes                [A]
│   ├── metadata.py         # add metadata columns                                   [B]
│   └── manifest.py         # checksums, skip already-ingested files                 [B]
├── silver/
│   ├── model.py            # Silver schema, grain, keys                             [shared]
│   ├── clean.py            # types, standardisation, deduplication                  [A]
│   ├── enrich.py           # reference joins, derived fields                        [A]
│   ├── transform.py        # run(): clean → rules → enrich → write                  [A]
│   ├── rules.py            # config-driven quality rules engine                     [B]
│   ├── quarantine.py       # write rejected records                                 [B]
│   └── quality_report.py   # before/after metrics                                   [B]
└── gold/
    ├── build.py            # run(): Gold tables (DataFrames / Spark SQL)            [A]
    ├── validate.py         # reconcile Gold with Silver                             [B]
    └── publish.py          # publish only if checks pass                            [B]
config/                     # base, dev, prod, test                                  [B] done
tests/data/                 # samples incl. deliberately bad records                 [A]
tests/test_*.py             # one per module                                         [owner of the module]
notebooks/                  # profiling, performance, Gold consumer                  [A]
.github/workflows/          # ci, deploy, run                                        [B]
docs/                       # project-definition.md, design.md                       [shared]
```

## 5. Shared contracts (Phase 0, then frozen)

Each layer is written against these, starting from a stub that returns a valid result, so neither person waits for the other.

```python
@dataclass(frozen=True)
class RunContext:
    run_id: str
    env: Literal["dev", "prod", "test"]
    started_at: datetime                      # UTC
    config: dict[str, Any]

@dataclass
class RuleResult:
    rule: str
    action: Literal["reject", "correct", "flag"]
    failed: int

@dataclass
class LayerResult:
    layer: Literal["bronze", "silver", "gold"]
    status: Literal["success", "failed", "skipped"]
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    outputs: list[str] = field(default_factory=list)
    rules: list[RuleResult] = field(default_factory=list)
    message: str = ""
```

```python
bronze.ingest.run(spark, ctx) -> LayerResult
silver.transform.run(spark, ctx) -> LayerResult
gold.build.run(spark, ctx) -> LayerResult

bronze.metadata.add_metadata(df, source_name, source_file, period, ctx) -> DataFrame
bronze.manifest.is_ingested(sha256, ctx) -> bool
silver.rules.apply_rules(df, rules) -> (accepted_df, rejected_df, list[RuleResult])
gold.validate.reconcile(silver_df, gold_df) -> list[str]      # empty = valid
```

```yaml
quality:
  max_reject_ratio: 0.05
  rules:
    - name: timestamp_not_null
      column: <column>          # TBD after #15
      check: not_null           # not_null | range | in_set | regex | unique
      params: {}                # e.g. {min: 0, max: 500} for range
      action: reject            # reject | correct | flag
```

```json
{"run_id": "...", "env": "prod", "started_at": "...",
 "files":  [{"name": "...", "period": "2025-01", "bytes": 0, "sha256": "...", "status": "ingested"}],
 "layers": [{"layer": "bronze", "status": "success", "rows_in": 0, "rows_out": 0, "rows_rejected": 0}]}
```

**Working without waiting:**
- B's run control is built and tested against stubs that return `skipped`, or `failed` when a test forces it.
- B's metadata, manifest and rules engine are tested on small hand-made DataFrames.
- A's Gold is developed against a small Silver sample before Silver is finished.

## 6. Run control

`src/main.py`:
1. Load config for `PIPELINE_ENV`; create `RunContext` (run ID from `--run-id`, else a UUID).
2. Run each layer in `run.layers` and log its `LayerResult`.
3. If a layer fails, skip the rest, write the manifest and exit non-zero, so the workflow fails.
4. After Silver, if `rows_rejected / rows_in > quality.max_reject_ratio`, Silver counts as failed.
5. Gold is published only if `reconcile` returns no problems; otherwise the previous Gold stays.

## 7. Layer designs

### 7.1 Bronze
- **Method:** batch per period, scheduled to match the source's refresh. _TBD (#9): API or file download._
- **Preserve:** content written as delivered; only metadata columns added; no rows dropped.
- **Layout:** partitioned by period, so one period can be reprocessed alone.
- **Idempotency:** checksums in the manifest; known files are skipped; `--force` overwrites one period.
- **Late data:** a re-published file has a new checksum, is ingested into its period again, and the change is logged.
- **Schema changes:** new columns kept and logged; missing or retyped columns logged as warnings and counted in the run summary.

### 7.2 Silver (primary focus; filled in after #15)
Each decision is recorded with its reason.
- **Data understanding:** what one source record is; essential, optional, derived and unreliable fields.
- **Grain, keys, model:** _TBD (#16)._ Relationships to reference data and how history is represented.
- **Cleaning and standardisation:** types, timestamps in UTC, units, names, trimming, exact duplicates.
- **Quality rules and policy:** _TBD (#17, #18):_ rule → check → action → reason.
- **Enrichment:** _TBD (#21):_ joins and derived attributes.
- **Quality evidence:** missing, duplicate, invalid, accepted and rejected counts before and after (#22).
- **Implementation:** DataFrame API and Spark SQL; lower-level APIs only where justified.
- **Performance:** Spark UI baseline, then only evidence-backed changes, measured before and after (#24, #25).
- **Scalability:** behaviour with larger volumes, late data and schema evolution.

### 7.3 Gold
- **Consumer and question:** _TBD (#26)._
- **Tables, dimensions, measures:** _TBD (#27)._ Smaller and shaped for one use, unlike reusable Silver.
- **Correctness:** totals reconcile with Silver.
- **Readiness:** refreshed after each successful run; the manifest records the latest published run.
- **Upstream failure:** previous Gold stays published; the run is marked failed.

## 8. Testing strategy
- **Unit tests** (`PIPELINE_ENV=test`, run in CI): config and no-secrets check (done); metadata present and raw content unchanged; reruns skip known files; schema changes detected; each quality rule's check, action and counts on bad records; Gold reconciliation.
- **Pipeline test:** `main.py` on `tests/data`, plus a forced failure that must skip downstream layers.
- **Before each release tag:** a manual `run.yml` on dev.
- **Performance:** measured on the VMs; CI checks correctness only.

## 9. Work split

### 9.1 Dependency graph

```
Phase 0 (both): definition ─ repo/config/CI ─ contracts + stubs
   │
   ├─ A: explore #9 ─► ingest #10 ─► schema #13
   │                     └─ M1 ─► profile #15 ─► model #16 (both) ─► clean #19 ─► enrich #21 ─► perf #24/#25
   │                                                                                 └─► Gold #26–#28 ─► consumer #31
   │
   └─ B: metadata #11 ─► manifest #12 ─► ingest tests #14
         rules engine #17 ─ M2 ─► policy #18 (both) ─► quarantine #20 ─► quality report #22 ─► tests #23
         VMs + runners #6 ─ M3 ─► deploy #32 ─► schedule #33 ─► logging #34 ─► failure handling #35
         Gold validate #29 ─► publish gate #30
```

A's chain is the critical path, so it starts first. B builds against the contracts with test data and never waits for A.

### 9.2 Phase 0: setup and definition

| Issue | Task | Owner | What to do · Done when |
|---|---|---|---|
| #1 | Choose dataset | A | Shortlist two sources; download a sample of each; compare source, format, volume, refresh pattern, licence, quality problems and reference data. · One dataset chosen and justified in the definition. |
| #2 | Confirm platform | — | Done: self-managed VMs. |
| #3 | Project definition | A: §1–3, §5 · B: §6 · both: §4 | Write `docs/project-definition.md` (1–2 pages) using the six guide sections; Silver gets the most detail. · Merged and submitted at check-in. |
| #4, #5, #7 | Repo structure, config, CI | B | Folders, pinned requirements, base/dev/prod/test config with loader, `ci.yml` with lint + pytest, manual-only deploy/run stubs. · Done; merged when the PR is approved. |
| #8 | README | B | Setup, configuration, running tests and the pipeline, workflow, environments. · Another person can clone and run the tests from it. |
| — | Contracts and stubs | both | Agree §5; add `contracts.py` and stub `run()` functions returning `LayerResult`. · `PIPELINE_ENV=dev python src/main.py` runs all three stubs. |
| #6 | VM and runners | B, with A | Runners `dev` and `prod` installed as services on the VM; versions matched (Spark 3.5.0, Java 8, Python 3.11); storage in `~/dat535/dev` and `~/dat535/prod`. · A manual `deploy.yml` succeeds on dev and prod. |

**Exit criterion:** CI green on `main`, stubs run end-to-end, definition submitted.

### 9.3 Phases 1–3: Person A (data)

| # | Issue | Task | What to do · Done when |
|---|---|---|---|
| A1 | #9 | Explore the source | Document format, volume per period, refresh pattern, access method, schema and field meanings in a notebook; note obvious quality problems. · Expected schema and access method written down. |
| A2 | #10 | Batch ingestion | Implement `source.py` and `ingest.run()`: fetch each configured period and write it unchanged to `bronze/<source>/period=…/`, calling B's `add_metadata` and `is_ingested`. · Running dev ingests the configured periods. **M1.** |
| A3 | #13 | Schema changes | Store the expected schema; compare each batch; log added, missing and retyped columns and include them in `LayerResult.message`. · A changed sample file is detected in a test. |
| A4 | #15 | Profile the data | In a notebook: null rates, ranges, distinct counts, duplicates and distributions per field; list the problems found with counts. · Profiling results are ready for #16–#18 and quoted in §7.2. |
| A5 | #16 | Grain, keys, model (with B) | Decide what one Silver record is, its key, how duplicates on the key are resolved and how reference data joins; write `silver/model.py` and §7.2. · Both agree; schema committed. |
| A6 | #19 | Cleaning | Implement `clean.py`: casts, UTC timestamps, units, names, trimming, exact-duplicate removal. · Tests on bad samples pass. |
| A7 | #21 | Enrichment | Implement `enrich.py`: reference-data joins and derived fields; wire `transform.run()` (clean → B's rules → enrich → write). · Silver is written for the dev periods. |
| A8 | #24, #25 | Performance | Record a Spark UI baseline of the Silver job (stages, shuffles, skew, durations); apply only changes the baseline justifies (early filters, partitioning, broadcast joins); measure again. · Before/after numbers and screenshots in the evidence folder. |
| A9 | #26–#28 | Gold | Define the consumer and the question; design dimensions, measures and aggregations; implement `gold/build.py` with DataFrames / Spark SQL. · Gold tables built from dev Silver. |
| A10 | #31 | Consumer | Build the notebook, dashboard or model that uses Gold and answers the question. · Ready to show in the demo. |

### 9.4 Phases 1–3: Person B (platform)

| # | Issue | Task | What to do · Done when |
|---|---|---|---|
| B1 | #11 | Ingestion metadata | Implement `add_metadata()`: `_source_name`, `_source_file`, `_source_period`, `_ingested_at` (UTC), `_run_id`. · Unit test shows all columns present and non-null, other columns unchanged. |
| B2 | #12 | Idempotent ingestion | Implement `manifest.py`: SHA-256 per file, `is_ingested()`, manifest written per run, `--force` overwrites one period. · Running the same period twice leaves Bronze unchanged and logs "skipped". |
| B3 | #14 | Ingestion tests | Tests in `tests/bronze/` for raw content preserved, metadata, reruns and schema-change detection (#13). · All pass in CI after M1. |
| B4 | #17 | Rules engine | Implement `apply_rules()` for `not_null`, `range`, `in_set`, `regex`, `unique`, reading rules from config. · Each check works on hand-made bad records. **M2.** |
| B5 | #18 | Reject / correct / flag policy (with A) | From A's profiling, decide the action and reason per rule; add the rules to `base.yaml`; record the table in §7.2. · Rules agreed and in config. |
| B6 | #20 | Quarantine | Write `rejected_df` to `quarantine/<table>/run_id=…/` with the rule name. · Rejected counts in the run match the quarantine. |
| B7 | #22 | Quality report | Compute missing, duplicate, invalid, accepted and rejected counts before and after Silver; write `reports/quality/<run_id>.json`. · Report produced on every run; usable in the report as evidence. |
| B8 | #23 | Silver tests | Tests for every rule and the Silver schema. · All pass in CI. |
| B9 | #6, #32 | Runners and deploy | Finish runner setup; make `deploy.yml` deploy `main` to dev and `v*` tags to prod. · Merge to `main` deploys dev; a tag deploys prod. **M3.** |
| B10 | #29, #30 | Gold validation and publishing | Implement `reconcile()` (row counts, sums of key measures) and `publish.py` (publish only if Silver quality and reconciliation pass, else keep the previous version). · A test with a broken Gold blocks publishing. |

**Sync points:**
- **M1** (A2): real Bronze data. B checks metadata and manifest on it and finishes B3.
- **M2** (B4): rules engine merged. A and B put the real rules in config (B5).
- **M3** (B9): runners online. Deploy and scheduled runs become automatic.

### 9.5 Phase 4: operations

| Issue | Task | Owner | What to do · Done when |
|---|---|---|---|
| #33 | Scheduled run | B | Enable the schedule in `run.yml`; pass the GitHub run ID. · A scheduled run completes on prod. |
| #34 | Logging and run status | B | Implement `common/logging.py`; one summary line per layer (status, rows, rejected, duration) in the Actions log. · Status of every layer visible without opening files. |
| #35 | Failure handling | B | Implement §6 steps 3–5 in `main.py`. · A forced failure skips downstream layers and fails the workflow. |
| #36 | End-to-end prod run | both | Tag a release, deploy to prod, run the full pipeline, check Gold and the quality report. · Successful run with logs saved as evidence. |
| #37 | Branch protection, environments | B | After GitHub Pro: ruleset on `main` (PR, 1 approval, CI required, no force push); `dev`/`prod` environments with their secrets. · Rules active. |

### 9.6 Phase 5: delivery

| Issue | Task | Owner | What to do · Done when |
|---|---|---|---|
| #38 | Report outline | both | Structure by decisions (from §10), not chronology. · Agreed outline. |
| #39 | Report draft | A: data, Silver logic, performance, Gold · B: ingestion reliability, quality engine, operations · both: Silver, reflection | 8–10 pages with code excerpts, model, quality metrics, tests and run evidence. · Full draft reviewed by both. |
| #40 | Evidence | B: runs, logs, deploys · A: profiling, quality, performance | Screenshots and numbers collected in one folder. · Every claim in the report has evidence. |
| #41 | Presentation | both | 10 minutes: key decisions, evidence, lessons learnt; each presents their own parts. · Slides ready. |
| #42 | Demo rehearsal | both | Rehearse deploy, run, schedule and Gold output; each can explain the other's parts. · Full run-through under time. |

### 9.7 Git workflow
- One branch per issue (`bronze/metadata`, `silver/rules-engine`); PR into `main` with `Closes #n`; the other person reviews; squash merge.
- Shared files (`main.py`, `contracts.py`, `silver/model.py`, `docs/`) need both people's approval.
- Update this document in the same PR as the change it describes.

## 10. Key decisions and trade-offs

| Decision | Rationale |
|---|---|
| Separate modules and storage per layer | Each layer is tested and rerun on its own |
| One self-managed VM with separate dev and prod runners, configs and storage folders | Only one VM is available; separate runners (`dev`, `prod`), configs and folders (`~/dat535/dev`, `~/dat535/prod`) keep prod data and runs apart. Trade-off: both share 4 cores and 7.8 GB RAM, so runs are serialised with a workflow concurrency group |
| CI on GitHub-hosted runners; deploy and run on self-hosted runners | PRs are checked even when the VMs are unreachable; runners only connect outward |
| Layered YAML config selected by `PIPELINE_ENV` | No environment values in code; differences reviewed in PRs |
| Secrets only in GitHub Actions secrets | The repository may be shared for grading |
| Squash-merged PRs with review | One reviewed, issue-linked commit per change |
| Prod deploys from release tags | Environment reviewers are unavailable on a private repo; a tag is a deliberate, traceable release |
| Quality rules in config with reject / correct / flag | Rules are visible, reviewable and testable without code changes |
| Quarantine instead of dropping | Rejected records can be counted and inspected; provides before/after evidence |
| Stop downstream on failure; gated Gold publishing | Consumers never see Gold built from bad data |
| Python 3.11, PySpark 3.5.0, Java 8, pinned | Matches the VM (Spark 3.5.0, Java 8, Python 3.11 in `~/spark-env`); CI uses the same versions |
| Tuning only from Spark UI evidence | The course grades evidence-based optimisation |
| Dataset: _TBD (#1)_ | _TBD_ |

## 11. Risks
- **Dataset not chosen:** blocks the definition and Silver. Shortlist and sample this week.
- **VM only reachable on campus:** runners are installed; changes to the VM itself need campus access. CI and development don't need the VM.
- **Shared VM resources:** dev and prod compete for 4 cores and 7.8 GB RAM. Runs are serialised; keep dev data small.
- **A's chain is long:** B takes over performance (#24, #25) or the consumer (#31) if A falls behind.

## 12. Worth knowing

Notes about something one person built that affects the other's work. Add at the end: heading, what it affects, suggested follow-up.