# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Catalog, Medallion Schemas, and Bronze Landing Volume
# MAGIC
# MAGIC Idempotent setup task meant to run as a step in a Databricks Workflow job on
# MAGIC every run, before the ingestion task. Creates, only if missing:
# MAGIC - a Unity Catalog catalog
# MAGIC - `bronze` / `silver` / `gold` schemas
# MAGIC - a Volume inside `bronze` that raw files land in before anything is parsed:
# MAGIC   - `/bls/pr/...`      mirror of https://download.bls.gov/pub/time.series/pr/
# MAGIC   - `/population/...`  raw JSON from the Data USA population API
# MAGIC   - `/_manifest/...`   ingestion manifest (lets the ingestion job skip files it
# MAGIC                        has already landed instead of reprocessing them)
# MAGIC
# MAGIC Parameters (exposed as both notebook widgets and Databricks Job task parameters):
# MAGIC - `catalog_name` (default `bls_productivity`) - point this at an existing catalog if
# MAGIC   your workspace doesn't allow creating new ones (common on trial / single-catalog
# MAGIC   metastores).
# MAGIC - `bronze_volume_name` (default `raw_landing`)
# MAGIC
# MAGIC Every operation below is written to be safe to run any number of times: catalog/
# MAGIC schema/volume creation use `IF NOT EXISTS`, and directory creation is naturally a
# MAGIC no-op if the directory already exists.

# COMMAND ----------

import re

from pyspark.errors import AnalysisException

dbutils.widgets.text("catalog_name", "bls_productivity")
dbutils.widgets.text("bronze_volume_name", "raw_landing")

catalog_name = dbutils.widgets.get("catalog_name").strip()
bronze_volume_name = dbutils.widgets.get("bronze_volume_name").strip()

# COMMAND ----------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def validate_identifier(name: str, label: str) -> None:
    """Guard against malformed input before it's interpolated into DDL strings."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {label} '{name}': must start with a letter and contain only "
            "letters, digits, and underscores."
        )


validate_identifier(catalog_name, "catalog_name")
validate_identifier(bronze_volume_name, "bronze_volume_name")

# COMMAND ----------

# MAGIC %md ## 1. Catalog
# MAGIC
# MAGIC On workspaces where catalog creation is restricted to metastore admins (common on
# MAGIC trial workspaces), this call can fail with a permission error even when the catalog
# MAGIC already exists. We log a warning and continue rather than failing the job outright;
# MAGIC if the catalog genuinely doesn't exist and can't be created, schema creation in the
# MAGIC next step will fail with a clear, unambiguous error instead.

# COMMAND ----------

try:
    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {catalog_name} "
        "COMMENT 'BLS productivity + Data USA population medallion pipeline'"
    )
    print(f"Catalog '{catalog_name}' ready.")
except AnalysisException as e:
    print(
        f"WARNING: could not create catalog '{catalog_name}' automatically "
        f"(likely missing CREATE CATALOG privilege): {e}\n"
        "Continuing on the assumption it already exists."
    )

# COMMAND ----------

# MAGIC %md ## 2. Schemas (medallion layers)

# COMMAND ----------

SCHEMA_COMMENTS = {
    "bronze": "Raw landing volume plus 1:1 Delta representations of the raw BLS and population files",
    "silver": "Cleaned, typed, deduplicated, conformed tables built from bronze",
    "gold": "Business-level aggregated tables for analytics and reporting",
}

for schema, comment in SCHEMA_COMMENTS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema} COMMENT '{comment}'")
    print(f"Schema '{catalog_name}.{schema}' ready.")

# COMMAND ----------

# MAGIC %md ## 3. Bronze landing volume + folder layout

# COMMAND ----------

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {catalog_name}.bronze.{bronze_volume_name} "
    "COMMENT 'Raw landing zone for BLS productivity time-series files (pr/) and "
    "Data USA population API JSON'"
)
print(f"Volume '{catalog_name}.bronze.{bronze_volume_name}' ready.")

volume_path = f"/Volumes/{catalog_name}/bronze/{bronze_volume_name}"

for sub_dir in ("bls/pr", "population", "_manifest"):
    dbutils.fs.mkdirs(f"{volume_path}/{sub_dir}")
    print(f"Directory '{volume_path}/{sub_dir}' ready.")

# COMMAND ----------

# MAGIC %md ## 4. Verify

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

display(dbutils.fs.ls(volume_path))

# COMMAND ----------

result = f"OK: catalog={catalog_name}, volume={volume_path}"
print(result)
dbutils.notebook.exit(result)
