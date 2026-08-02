# bls-productivity-pipeline
End-to-end Databricks data pipeline ingesting BLS productivity time-series &amp; population data into Bronze, Silver, and Gold Delta layers using a Lakeflow (Spark) Declarative Pipeline, packaged as a Databricks Asset Bundle.

Design rationale, data-format notes, and documented assumptions for the Bronze/Silver/Gold pipeline live in [docs/PIPELINE_PLAN.md](docs/PIPELINE_PLAN.md) - read that for the "why," this README covers the "how to run it."

New to this project? [docs/guide/README.md](docs/guide/README.md) is a narrated, step-by-step walkthrough (with screenshots) covering setup, ingestion, deployment, the pipeline, results, and the Genie Agent, in order.

Curious about the reasoning, trade-offs, and what was hardest to get right? See [PROCESS.md](PROCESS.md).

## Sources

- BLS productivity time series: https://download.bls.gov/pub/time.series/pr/ (requires a `User-Agent` header with contact info per BLS's [access policy](https://www.bls.gov/bls/pss.htm), or requests get a 403)
- Data USA population API: https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population

## Project structure

```
databricks.yml                          Bundle root: variables (catalog_name, bronze_volume_name,
                                         contact_email) and targets.
.env.example                            Template for supplying contact_email via a gitignored
                                         .env + BUNDLE_VAR_contact_email (see "Deploying" below).
variable-overrides.example.json         Template for supplying contact_email via a gitignored
                                         .databricks/bundle/<target>/variable-overrides.json.
resources/
  ingestion_job.yml                     Job: setup -> [ingest_bls_pr, ingest_population] -> refresh_bls_pipeline.
  bls_pipeline.yml                      Lakeflow Declarative Pipeline resource (serverless, multi-schema publish).

setup/
  01_create_catalog_schemas_volume.py   Databricks notebook (PySpark): creates the catalog,
                                         bronze/silver/gold schemas, and the bronze raw-landing
                                         Volume (with its folder layout). Idempotent (IF NOT
                                         EXISTS everywhere) - designed to run as a Workflow job
                                         task on every run without failing or duplicating state.

ingestion/
  _manifest_common.py     Shared helpers (not a runnable task, included via `%run`): creates/
                           reads/updates the `raw_ingestion_manifest` Delta table used to decide
                           what's new, changed, unchanged, or removed on every run.
  02_ingest_bls_pr.py      Crawls the live BLS pr/ directory listing (no hardcoded filenames),
                           sends a compliant User-Agent header, and downloads only files that are
                           new or changed since the last run into `/bls/pr/...` in the volume.
  03_ingest_population.py Pulls the Data USA population API JSON into `/population/...` in the
                           same volume, skipping the rewrite if the content hasn't changed.

transformations/
  bronze/                 Materialized views reading the raw volume files 1:1, with explicit
                           schemas and non-null-key expectations.
  silver/                 Cleaned/typed/deduped observations, decoded human-readable series
                           dimensions, cleaned population records.
  gold/                   The three required analytical answers (SQL primary implementation).
  gold_alternates/        Equivalent PySpark implementations of the same three answers - a
                           sibling of gold/, not nested inside it, so the pipeline's gold glob
                           never picks them up. Documented but not wired in - see its README.

docs/
  PIPELINE_PLAN.md         Design doc: data formats discovered, assumptions made, DAG, and the
                           exact Gold query logic for all three questions.
  guide/                   Step-by-step walkthrough (setup -> ingestion -> deploy -> pipeline ->
                           results -> Genie), with screenshot placeholders - start at guide/README.md.
```

## Unity Catalog layout

```
<catalog>
├── bronze
│   ├── Volume: raw_landing
│   │   ├── /bls/pr/...        raw mirror of the BLS pr/ time-series directory
│   │   └── /population/...    raw JSON from the Data USA population API
│   ├── Table: raw_ingestion_manifest   tracks (source, relative_path) -> size/last-modified/
│   │                                    content hash/status, so re-runs skip unchanged files
│   │                                    and flag ones the source removed
│   └── bronze_pr_series, bronze_pr_sector/class/measure/duration/seasonal/period/footnote,
│       bronze_pr_data_current, bronze_pr_data_all, bronze_population
├── silver
│   └── silver_pr_series_dim, silver_pr_observations, silver_population
└── gold
    └── gold_population_stats, gold_series_best_year, gold_prs30006032_by_year
```

## Deploying and running (Databricks Asset Bundle)

`contact_email` has no default, so you must supply it every time you `validate`/`deploy`/`run`.
Passing it inline (`--var contact_email=...`) works but is easy to forget and clutters your shell
history with your email. Pick one of these instead - both are gitignored, so your email never gets
committed:

**Option A - `.env` file, loaded into your shell:**

```bash
cp .env.example .env        # then edit .env and fill in your email
set -a && source .env && set +a
databricks bundle validate
```

**Option B - `variable-overrides.json`, read automatically by the CLI (no sourcing needed):**

```bash
mkdir -p .databricks/bundle/dev
cp variable-overrides.example.json .databricks/bundle/dev/variable-overrides.json
# then edit that file and fill in your email
databricks bundle validate
```

Either way, once the variable is set:

```bash
databricks bundle deploy
databricks bundle run bls_ingestion_job
```

`bls_ingestion_job` runs `setup -> [ingest_bls_pr, ingest_population] -> refresh_bls_pipeline`
end to end - the last task refreshes the Lakeflow pipeline that rebuilds Bronze, Silver, and
Gold. No schedule is defined by default (Databricks Free Edition has a daily fair-usage quota),
so re-run the command above whenever you want fresh data; see `resources/ingestion_job.yml` for
how to add a `trigger.periodic` block once you're ready to automate it.

### Targets (`dev` / `prod`)

`databricks.yml` defines two targets:

- **`dev`** (default) - `mode: development` (resource names get a `[dev <user>]` prefix,
  schedules stay paused). `presets.source_linked_deployment: false` is set explicitly so dev
  always deploys a real synced copy of the files to `workspace.root_path`, instead of the
  workspace-UI default of referencing your live working tree directly - this keeps dev's file
  resolution behavior identical to prod's, so a working dev deploy is representative of what
  prod will do.
- **`prod`** - `mode: production` (stricter validation, no name prefix) and a dedicated
  `workspace.root_path` (`/Workspace/Production/.bundle/bls-productivity-pipeline`, not tied to
  any one user's home folder or `/Shared`, which is writable by every workspace user).

Deploy to prod the same way, just with `-t prod`, and provide `contact_email` via
`.databricks/bundle/prod/variable-overrides.json` (Option B above, copied to the `prod` folder
instead of `dev`):

```bash
databricks bundle deploy -t prod
databricks bundle run bls_ingestion_job -t prod
```

## Transformations (Bronze / Silver / Gold)

`resources/bls_pipeline.yml` defines a single Lakeflow Declarative Pipeline (serverless) that
publishes fully-qualified tables across all three schemas. Every table is a materialized view
(full batch recompute) rather than a streaming table - the raw files are small and get
*overwritten in place* by the ingestion job rather than appended, which doesn't fit Auto
Loader's new-file-arrival model. Full details, including exactly why each design choice was
made and the SQL for all three Gold questions, are in
[docs/PIPELINE_PLAN.md](docs/PIPELINE_PLAN.md).

Data quality is enforced with Lakeflow expectations at every layer:

- **Expected schema**: every Bronze table reads its raw file against an explicit `StructType`
  rather than inferring one.
- **Non-null keys**: `@dp.expect_or_drop` on `series_id`/`year`/`period` (BLS) and
  `nation_id`/`year`/`population` (population) in Bronze and Silver.

**Gold answers the three required questions:**

1. `gold_population_stats` - mean and (sample) standard deviation of US population, 2013-2018.
2. `gold_series_best_year` - for every `series_id`, the year with the largest summed value
   across `Q01`-`Q04` (BLS's own docs define `Q05` as a separate "Annual Average," not a 5th
   quarter), joined to a human-readable `series_label` (e.g. *"Manufacturing: All workers,
   Hours worked, % Change from previous quarter (Seasonally Adjusted)"*).
3. `gold_prs30006032_by_year` - `PRS30006032`/`Q01` value per year, left-joined to that year's
   US population (left, not inner - BLS coverage starts in 1987, population coverage is
   2013-2024 minus a real 2020 ACS gap, so most years legitimately have no population).

Each question is implemented once in SQL (`transformations/gold/*.sql`, the primary
implementation actually wired into the pipeline) and once in PySpark
(`transformations/gold_alternates/*.py`, functionally equivalent but kept in a sibling folder so
the pipeline's `gold/**` library glob never picks it up) - see
[transformations/gold_alternates/README.md](transformations/gold_alternates/README.md).

## Setup

`setup/01_create_catalog_schemas_volume.py` is a PySpark Databricks notebook meant to run as the
first task in a Databricks Workflow job (Notebook task type). It takes two parameters, exposed as
both notebook widgets and Job task parameters:

- `catalog_name` (default `bls_productivity`) - set this to an existing catalog if your workspace
  doesn't allow creating new catalogs (common on trial / single-catalog metastores).
- `bronze_volume_name` (default `raw_landing`)

Every operation is `IF NOT EXISTS` / naturally idempotent, so scheduling this task to run before
every job run is safe - it never fails or duplicates state on repeat runs. You can also import it
into the workspace and run it interactively for a one-off setup.

## Ingestion

`ingestion/02_ingest_bls_pr.py` and `ingestion/03_ingest_population.py` are the two ingestion
tasks. Both share `ingestion/_manifest_common.py` (included via `%run`, not run directly) which
maintains a `<catalog>.bronze.raw_ingestion_manifest` Delta table recording, per source file,
its size/last-modified (or content hash for the population API), and whether it's currently
`active` or was `removed` from the source. On every run:

- Files that are new or whose size/last-modified changed are (re)downloaded and overwrite their
  path in the volume; the manifest is updated.
- Files that are unchanged are skipped entirely - no network call beyond listing the directory,
  no rewrite, no reprocessing.
- Files present in the manifest but missing from the current source listing are marked `removed`
  in the manifest (the previously-landed raw file itself is left in place for audit history).

Parameters:

- `catalog_name` / `bronze_volume_name` - same as the setup task.
- `contact_email` (**required** for `02_ingest_bls_pr.py`) - sent in the `User-Agent` header
  (`bls-productivity-pipeline/1.0 (contact: <email>)`) per BLS's
  [access policy](https://www.bls.gov/bls/pss.htm), which blocks robots that don't identify a way
  to contact the requester. Without it BLS returns 403 Forbidden.
- `base_url` / `api_url` - override the source URL if needed; defaults point at the BLS `pr/`
  directory and the Data USA population endpoint respectively.

This is wired up as `resources/ingestion_job.yml`'s `setup_catalog_schemas_volume` ->
(`ingest_bls_pr` and `ingest_population` in parallel) -> `refresh_bls_pipeline` - see
"Deploying and running" above.
