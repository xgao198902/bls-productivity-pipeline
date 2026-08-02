"""Silver: unions pr.data.0.Current and pr.data.1.AllData into one clean,
typed, deduplicated observation table - one row per (series_id, year, period).

When both files report the same (series_id, year, period), the Current file's
value wins, since it reflects the latest revision for the in-progress year.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog_name")


@dp.table(
    name=f"{CATALOG}.silver.silver_pr_observations",
    comment="One row per (series_id, year, period), deduped/typed from the two raw pr.data files.",
)
@dp.expect_or_drop(
    "non_null_keys", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL"
)
@dp.expect_or_drop("valid_period", "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')")
@dp.expect("has_value", "value IS NOT NULL")
def silver_pr_observations():
    current = spark.read.table(f"{CATALOG}.bronze.bronze_pr_data_current").withColumn(
        "_source_rank", F.lit(1)
    )
    all_data = spark.read.table(f"{CATALOG}.bronze.bronze_pr_data_all").withColumn(
        "_source_rank", F.lit(2)
    )

    typed = current.unionByName(all_data).select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.trim(F.col("year")).cast("int").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.trim(F.col("value")).cast("double").alias("value"),
        F.trim(F.col("footnote_codes")).alias("footnote_codes"),
        F.col("_source_rank"),
    )

    dedup_window = Window.partitionBy("series_id", "year", "period").orderBy("_source_rank")
    return (
        typed.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_source_rank")
    )
