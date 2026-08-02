"""Bronze: raw copy of pr.series - one row per BLS series_id with its dimension
codes (sector/class/measure/duration/seasonal) and coverage dates.

Read as a materialized view (full batch read on every pipeline update) rather
than a streaming table: the source file is small (~15 KB) and gets overwritten
in place by the ingestion job rather than incrementally appended, which is a
poor fit for Auto Loader's new-file-arrival model. A full re-read is simpler,
always correct, and cheap at this size.
"""

from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = spark.conf.get("catalog_name")
VOLUME_ROOT = spark.conf.get("bronze_volume_path")

PR_SERIES_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), True),
        StructField("sector_code", StringType(), True),
        StructField("class_code", StringType(), True),
        StructField("measure_code", StringType(), True),
        StructField("duration_code", StringType(), True),
        StructField("seasonal", StringType(), True),
        StructField("base_year", StringType(), True),
        StructField("footnote_codes", StringType(), True),
        StructField("begin_year", StringType(), True),
        StructField("begin_period", StringType(), True),
        StructField("end_year", StringType(), True),
        StructField("end_period", StringType(), True),
    ]
)


def _read_bls_tsv(path: str, schema: StructType):
    """BLS time-series files are tab-separated with space-padded fixed-width
    values and a header row. An explicit schema (rather than inference) both
    enforces the expected shape and sidesteps header-casing inconsistencies
    across files (e.g. pr.seasonal capitalizes its header, others don't)."""
    return (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(schema)
        .load(path)
    )


@dp.table(
    name=f"{CATALOG}.bronze.bronze_pr_series",
    comment="Raw pr.series: one row per BLS series_id with its dimension codes and coverage dates.",
)
@dp.expect_or_drop("non_null_series_id", "series_id IS NOT NULL AND trim(series_id) != ''")
def bronze_pr_series():
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.series", PR_SERIES_SCHEMA)
