# bls-productivity-pipeline
End-to-end Databricks data pipeline ingesting BLS productivity time-series &amp; population data into Bronze, Silver, and Gold Delta layers using Spark Declarative Pipelines.

## Sources

- BLS productivity time series: https://download.bls.gov/pub/time.series/pr/ (requires a `User-Agent` header with contact info per BLS's [access policy](https://www.bls.gov/bls/pss.htm), or requests get a 403)
- Data USA population API: https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population

## Project structure

```
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
│   └── (bronze Delta tables built from the raw files land here too)
├── silver   (cleaned, typed, conformed tables)
└── gold     (business-level aggregates for analytics/reporting)
```

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

Suggested Workflow job layout: `01_create_catalog_schemas_volume` -> (`02_ingest_bls_pr` and
`03_ingest_population` in parallel, both depending only on the setup task) -> silver/gold tasks
(not yet built).
