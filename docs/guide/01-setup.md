← [Back to guide index](README.md) · Next: [Ingestion →](02-ingestion.md)

# Step 1: Setup — Catalog, Schemas, and Volume

**Goal:** create the Unity Catalog objects everything else depends on, before
any data can land or any table can be created.

## What gets created

`setup/01_create_catalog_schemas_volume.py` is a PySpark notebook that creates:

- A Unity Catalog **catalog** (default name `bls_productivity`).
- Three **schemas** inside it: `bronze`, `silver`, `gold`.
- A **Volume** named `raw_landing` inside the `bronze` schema, with a
  `bls/pr/` and `population/` folder layout for the two raw data sources.

## Why this is its own step

Every statement in this notebook is `CREATE ... IF NOT EXISTS`, which makes it
fully idempotent — safe to run before every single job run without failing or
duplicating anything. That's deliberate: it's wired up as the *first* task in
the ingestion job (see [Deploying the bundle](03-deploy-bundle.md)), so the
catalog/schema/volume are guaranteed to exist before ingestion tries to write
to them, on every run, not just the first one.

## Parameters

Exposed as both notebook widgets and Job task parameters, so they can be set
once in `databricks.yml` and flow through everywhere:

| Parameter | Default | Notes |
| --- | --- | --- |
| `catalog_name` | `bls_productivity` | Set to an existing catalog name if your workspace doesn't allow creating new catalogs (common on trial / single-catalog metastores, including Databricks Free Edition). |
| `bronze_volume_name` | `raw_landing` | |

## Running it

This runs automatically as the first task (`setup_catalog_schemas_volume`) of
`bls_ingestion_job` — see [Deploying the bundle](03-deploy-bundle.md). You can
also open the notebook directly in the Databricks workspace and run it
interactively for a one-off, manual setup.

## Verifying it worked

In **Catalog Explorer**, you should see the new catalog with its three empty
schemas, and a `raw_landing` Volume under `bronze` with empty `bls/pr/` and
`population/` folders — nothing has been ingested yet at this point.

<!-- SCREENSHOT (optional): Catalog Explorer showing the created catalog,
     schemas, and the empty raw_landing Volume folder structure. -->

---
← [Back to guide index](README.md) · Next: [Ingestion →](02-ingestion.md)
