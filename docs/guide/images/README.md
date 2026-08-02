This folder holds the screenshots referenced by the guide pages in
[`docs/guide/`](../README.md). It's otherwise empty in version control — add
your own screenshots here with these exact filenames and the guide pages
will pick them up automatically:

| Filename | Used in | What to capture |
| --- | --- | --- |
| `catalog-explorer.png` | [guide index](../README.md) | Catalog Explorer's tree view of the `bls_productivity` catalog, expanded to show the `bronze`/`silver`/`gold` schemas, the `raw_landing` Volume, and their tables. |
| `ingestion-job-tasks.png` | [03-deploy-bundle.md](../03-deploy-bundle.md) | The `bls_ingestion_job` task graph (Jobs & Pipelines > `bls_ingestion_job` > Tasks), showing `setup_catalog_schemas_volume -> [ingest_bls_pr, ingest_population] -> refresh_bls_pipeline`. |
| `jobs-and-pipelines.png` | [03-deploy-bundle.md](../03-deploy-bundle.md) | The **Jobs & Pipelines** list showing the deployed `bls_ingestion_job` and `bls_medallion_pipeline` after a successful `databricks bundle deploy`. |
| `pipeline-dag.png` | [guide index](../README.md), [04-pipeline.md](../04-pipeline.md) | The Lakeflow pipeline's graph view (Workflows > Pipelines > `bls_medallion_pipeline`), ideally after a successful run so every node is green. |
| `gold-population-stats.png` | [05-results.md](../05-results.md) | Query result / table preview for `gold_population_stats`. |
| `gold-series-best-year.png` | [05-results.md](../05-results.md) | Query result / table preview for `gold_series_best_year`. |
| `gold-prs30006032-by-year.png` | [05-results.md](../05-results.md) | Query result / table preview for `gold_prs30006032_by_year`. |
| `genie-chat.png` | [06-genie.md](../06-genie.md) | The Genie chat UI with a question and its answer. |

A few optional ones are called out inline in the individual step pages
(Catalog Explorer, Volume file browser, deploy confirmation, Genie Agent
creation screen) if you want to add those too.
