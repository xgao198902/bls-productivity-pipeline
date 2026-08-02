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
```

## Unity Catalog layout

```
<catalog>
├── bronze
│   ├── Volume: raw_landing
│   │   ├── /bls/pr/...        raw mirror of the BLS pr/ time-series directory
│   │   ├── /population/...    raw JSON from the Data USA population API
│   │   └── /_manifest/...     ingestion manifest (tracks what's already landed, so re-runs
│   │                          don't reprocess unchanged files)
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
