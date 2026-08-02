← [Back to guide index](README.md) · Previous: [Setup](01-setup.md) · Next: [Deploying the bundle →](03-deploy-bundle.md)

# Step 2: Ingestion — Landing Raw Data in the Volume

**Goal:** pull the full BLS `pr/` directory and the Data USA population API
response into the `raw_landing` Volume created in [Step 1](01-setup.md),
without hardcoding filenames, and without re-downloading anything that hasn't
changed since the last run.

## The two ingestion tasks

| Task | File | Source | Downloads |
| --- | --- | --- | --- |
| `ingest_bls_pr` | `ingestion/02_ingest_bls_pr.py` | https://download.bls.gov/pub/time.series/pr/ | 9 files: `pr.series`, `pr.data.0.Current`, `pr.data.1.AllData`, and 6 lookup files (`pr.sector`, `pr.class`, `pr.measure`, `pr.duration`, `pr.seasonal`, `pr.period`, `pr.footnote`) |
| `ingest_population` | `ingestion/03_ingest_population.py` | Data USA population API | 1 JSON file |

Both write into `/Volumes/<catalog>/bronze/<volume>/...` and share
`ingestion/_manifest_common.py`, which maintains a
`<catalog>.bronze.raw_ingestion_manifest` Delta table.

## Getting past BLS's 403 Forbidden

BLS's [access policy](https://www.bls.gov/bls/pss.htm) blocks robots that
don't identify a way to contact the requester. `ingest_bls_pr` sends a
`User-Agent` header with your email (`bls-productivity-pipeline/1.0 (contact:
<email>)`), which is why `contact_email` is a required parameter — without
it, every request to BLS gets a 403.

## Why re-running this is safe

This is the core "don't reprocess what's already ingested" requirement. On
every run:

- **New or changed files** (different size or last-modified date on BLS's
  side, or a different content hash for the population API) get downloaded
  and overwrite their path in the Volume; the manifest is updated.
- **Unchanged files are skipped entirely** — no download, no rewrite. The
  only network call made for an unchanged file is the directory listing
  itself (to check whether it changed), not a re-download.
- **Files removed from the source** are marked `removed` in the manifest
  (the previously-landed file itself stays in the Volume for audit history —
  nothing is silently deleted).

## Running it

Both tasks run as part of `bls_ingestion_job` (`ingest_bls_pr` and
`ingest_population` run in parallel, after setup) — see
[Deploying the bundle](03-deploy-bundle.md). You can also run either notebook
standalone in the workspace for testing.

## Verifying it worked

Browse `/Volumes/<catalog>/bronze/<volume>/bls/pr/` and
`/Volumes/<catalog>/bronze/<volume>/population/` in Catalog Explorer — you
should see all 9 BLS files plus `population.json`. Querying
`<catalog>.bronze.raw_ingestion_manifest` shows one row per file with its
size/last-modified/content-hash and `active`/`removed` status.

<!-- SCREENSHOT (optional): Volume file browser showing the landed files in
     bls/pr/ and population/, or a query result from raw_ingestion_manifest. -->

---
← [Back to guide index](README.md) · Previous: [Setup](01-setup.md) · Next: [Deploying the bundle →](03-deploy-bundle.md)
