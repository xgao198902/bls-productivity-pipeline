# Databricks notebook source
# MAGIC %md
# MAGIC # Shared manifest helpers
# MAGIC
# MAGIC Included via `%run ./_manifest_common` by the BLS and population ingestion
# MAGIC notebooks (not meant to be run directly). Implements the manifest-driven sync
# MAGIC pattern that lets ingestion re-run any number of times without reprocessing
# MAGIC files that haven't changed:
# MAGIC
# MAGIC 1. `ensure_manifest_table` - creates a Delta table tracking every raw file ever
# MAGIC    landed in the bronze volume (one row per source file, keyed by
# MAGIC    `(source, relative_path)`).
# MAGIC 2. `load_manifest_state` - reads the current state for one source into a plain
# MAGIC    dict keyed by relative path, so the driver can diff it against a fresh
# MAGIC    directory listing / API response without needing Spark for that comparison.
# MAGIC 3. `needs_fetch` - decides whether a remote file is new, changed, or can be
# MAGIC    skipped, based on size/last-modified (or content hash for single-file
# MAGIC    sources like the population API).
# MAGIC 4. `upsert_manifest` - MERGEs this run's results back into the manifest table.
# MAGIC 5. `mark_removed` - flags manifest rows whose file is no longer present at the
# MAGIC    source, instead of silently deleting anything.

# COMMAND ----------

from datetime import datetime, timezone
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MANIFEST_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("relative_path", StringType(), False),
        StructField("remote_url", StringType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("last_modified", StringType(), True),
        StructField("content_hash", StringType(), True),
        StructField("status", StringType(), False),
        StructField("first_seen_at", TimestampType(), False),
        StructField("last_seen_at", TimestampType(), False),
        StructField("last_ingested_at", TimestampType(), True),
    ]
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def manifest_table_name(catalog: str) -> str:
    return f"{catalog}.bronze.raw_ingestion_manifest"


def ensure_manifest_table(spark, catalog: str) -> str:
    table = manifest_table_name(catalog)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source STRING,
            relative_path STRING,
            remote_url STRING,
            size_bytes BIGINT,
            last_modified STRING,
            content_hash STRING,
            status STRING,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            last_ingested_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Tracks every raw file landed in the bronze volume, so re-ingestion only
                 (re)downloads files that are new or changed, and can detect removals.'
        """
    )
    return table


def load_manifest_state(spark, catalog: str, source: str) -> dict:
    """Returns {relative_path: {size_bytes, last_modified, content_hash, status,
    first_seen_at, last_ingested_at}} for every row currently tracked for `source`."""
    table = ensure_manifest_table(spark, catalog)
    rows = spark.table(table).where(col("source") == source).collect()
    return {
        r["relative_path"]: {
            "size_bytes": r["size_bytes"],
            "last_modified": r["last_modified"],
            "content_hash": r["content_hash"],
            "status": r["status"],
            "first_seen_at": r["first_seen_at"],
            "last_ingested_at": r["last_ingested_at"],
        }
        for r in rows
    }


def needs_fetch(remote_size, remote_last_modified, existing: Optional[dict]) -> bool:
    """True if `existing` is missing, was previously marked removed, or its
    size/last-modified no longer match the source - i.e. it must be (re)downloaded."""
    if existing is None or existing.get("status") != "active":
        return True
    if remote_size is not None and existing.get("size_bytes") != remote_size:
        return True
    if remote_last_modified is not None and existing.get("last_modified") != remote_last_modified:
        return True
    return False


def upsert_manifest(spark, catalog: str, rows: list) -> None:
    """MERGE this run's manifest rows (one per remote file/object seen) into the
    manifest table. Safe to call repeatedly - matches on (source, relative_path)."""
    if not rows:
        return
    table = ensure_manifest_table(spark, catalog)
    updates_df = spark.createDataFrame(rows, schema=MANIFEST_SCHEMA)
    target = DeltaTable.forName(spark, table)
    (
        target.alias("t")
        .merge(
            updates_df.alias("s"),
            "t.source = s.source AND t.relative_path = s.relative_path",
        )
        .whenMatchedUpdate(
            set={
                "remote_url": "s.remote_url",
                "size_bytes": "s.size_bytes",
                "last_modified": "s.last_modified",
                "content_hash": "s.content_hash",
                "status": "s.status",
                "last_seen_at": "s.last_seen_at",
                "last_ingested_at": "s.last_ingested_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def mark_removed(spark, catalog: str, source: str, seen_relative_paths: list) -> int:
    """Flip status to 'removed' for manifest rows of `source` that are still
    'active' but weren't seen in this run's listing - i.e. the source deleted
    them. The raw file already landed is left in place for audit/history; only
    the manifest status changes, so downstream layers can decide whether to
    exclude it. Returns the number of rows flipped."""
    table = ensure_manifest_table(spark, catalog)
    seen_df = spark.createDataFrame([(p,) for p in seen_relative_paths], "relative_path STRING")
    seen_df.createOrReplaceTempView("_seen_this_run")

    before = spark.table(table).where((col("source") == source) & (col("status") == "active")).count()
    spark.sql(
        f"""
        UPDATE {table} AS t
        SET status = 'removed'
        WHERE t.source = '{source}'
          AND t.status = 'active'
          AND NOT EXISTS (SELECT 1 FROM _seen_this_run s WHERE s.relative_path = t.relative_path)
        """
    )
    after = spark.table(table).where((col("source") == source) & (col("status") == "active")).count()
    return before - after
