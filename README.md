# DAT535 Group Project – Medallion Data Pipeline

A batch data pipeline built with Apache Spark (PySpark) following the Medallion Architecture:

- **Bronze** – raw source data ingested unchanged, with metadata for traceability
- **Silver** – cleaned, validated and standardised data with documented quality rules
- **Gold** – curated datasets built for a specific consumer (dashboard, report or model)

The pipeline runs in separate **dev** and **prod** environments and is tested, deployed and scheduled with GitHub Actions.

> **Dataset:** Fiskeridirektoratet catch and vessel data(NLOD), starting with catch year 2023.
> See the [source exploration](notebooks/01_explore_source.ipynb).

## Repository structure

```
src/
  bronze/        ingestion (Phase 1)
  silver/        cleaning, validation, enrichment (Phase 2)
  gold/          serving tables (Phase 3)
  common/        shared code: config loader, Spark session
  main.py        pipeline entry point
tests/           pytest tests; tests/data/ holds small sample files
config/          base.yaml + dev/prod/test overrides
notebooks/       exploration and profiling
docs/            project definition and design notes
.github/workflows/
  ci.yml         lint + tests on every pull request
  deploy.yml     deploy to dev/prod (self-hosted runners)
  run.yml        run the pipeline (manual now, scheduled later)
```

## Getting started

### Requirements

- Python **3.11** (PySpark 3.5 does not support Python 3.13)
- Java **17**
- Git

### Setup

```bash
git clone https://github.com/ennasa/dat535-groupproject.git
cd dat535-groupproject
python3.11 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run the tests

```bash
ruff check .
PIPELINE_ENV=test pytest
```

### Run the pipeline locally

```bash
PIPELINE_ENV=dev python src/main.py                    # all layers
PIPELINE_ENV=dev python src/main.py --layers bronze    # one layer
```

Each run gets a run ID (a random UUID locally, the GitHub Actions run ID in workflows). Pass your own with `--run-id`.

**Status:** the full pipeline is implemented and has run on both environments.

| Layer | What it does | Docs |
|---|---|---|
| Bronze | Downloads each catch year, keeps every version unchanged (by SHA-256), adds metadata, checks the schema; `--force` re-ingests | [Bronze ingestion](docs/bronze-ingestion.md) |
| Silver | Cleans, applies the quality rules in `config/base.yaml` (reject / flag), writes rejected rows to quarantine and a before/after quality report per run | [Silver model](docs/silver-model.md), [cleaning](docs/silver-cleaning.md), [enrichment](docs/silver-enrichment.md) |
| Gold | Builds a star schema (month × region × species), validates it against Silver, and publishes it only if every check passes | [Gold model](docs/gold-model.md) |
| Consumer | Reads the published Gold and answers the question in Spark SQL | [`03_gold_consumer.ipynb`](notebooks/03_gold_consumer.ipynb) |

After a full, successful run, old versions are deleted (retention), and Silver always uses
the newest Bronze version of each year. Each run writes a manifest
(`<storage>/manifests/<run_id>.json`) and a quality report (`<storage>/reports/quality/<run_id>.json`).
Performance evidence is in [performance.md](docs/performance.md).

## Configuration

Settings live in `config/`:

| File | Purpose |
|------|---------|
| `base.yaml` | Shared defaults: storage layout, source, Spark settings, quality thresholds, run behaviour, logging |
| `dev.yaml`  | Development: small data slice, local paths, debug logging |
| `prod.yaml` | Production: full data, VM paths, cluster settings |
| `test.yaml` | Tests and CI: minimal local Spark, sample data only |

The `PIPELINE_ENV` variable (`dev`, `prod` or `test`, default `dev`) selects which file is merged on top of `base.yaml`.

**Secrets never go in YAML.** They come from environment variables, set as GitHub Actions secrets with `DEV_` / `PROD_` prefixes. A test fails if a config key looks like a credential.

## Development workflow

1. Pick an issue from the project board and assign yourself.
2. Create a feature branch from `main`, e.g. `bronze/ingestion-metadata`.
3. Commit, push and open a pull request with `Closes #<issue>` in the description.
4. CI (`ci.yml`) must pass, and the other team member reviews.
5. Squash merge, so `main` gets one commit per change. The branch is deleted automatically.

Labels (`setup`, `docs`, `bronze`, `silver`, `gold`, `ops`) and milestones (Phase 0–5) organise the issues.

## Environments and deployment

| | dev | prod |
|---|---|---|
| Runs on | self-hosted runner labelled `dev` | self-hosted runner labelled `prod` |
| Config | `config/dev.yaml` | `config/prod.yaml` |
| Deployed from | `main` | release tags `v*` |

**Current status:** both runners run on one VM, with separate storage (`~/dat535/dev`, `~/dat535/prod`). Dev processes catch year 2023, prod 2023–2025. `deploy.yml` is started manually (Actions → Deploy → *Run workflow*); automatic deploys on merge/tag are not enabled yet. `run.yml` runs on prod every night at 03:17 UTC and can also be started manually.

## Team

| Member | Main responsibility |
|---|---|
| Ennasa | Repository, configuration, CI/CD, environments |
| Robot flame | Dataset, ingestion, project definition |

Both members work on the Silver layer together.
