"""Bronze: raw copies of the two pr.data files.

pr.data.0.Current holds all current year-to-date observations (refreshed more
often, reflects the latest revisions); pr.data.1.AllData holds the full
history. They overlap - Silver unions and dedups them, preferring Current.
"""

from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = spark.conf.get("catalog_name")
VOLUME_ROOT = spark.conf.get("bronze_volume_path")

PR_DATA_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("period", StringType(), True),
        StructField("value", StringType(), True),
        StructField("footnote_codes", StringType(), True),
    ]
)


def _read_pr_data(path: str):
    return (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(PR_DATA_SCHEMA)
        .load(path)
    )


@dp.table(
    name=f"{CATALOG}.bronze.bronze_pr_data_current",
    comment="Raw pr.data.0.Current: all current year-to-date observations.",
)
@dp.expect_or_drop(
    "non_null_keys", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL"
)
def bronze_pr_data_current():
    return _read_pr_data(f"{VOLUME_ROOT}/bls/pr/pr.data.0.Current")


@dp.table(
    name=f"{CATALOG}.bronze.bronze_pr_data_all",
    comment="Raw pr.data.1.AllData: the full observation history.",
)
@dp.expect_or_drop(
    "non_null_keys", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL"
)
def bronze_pr_data_all():
    return _read_pr_data(f"{VOLUME_ROOT}/bls/pr/pr.data.1.AllData")
