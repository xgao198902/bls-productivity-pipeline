"""ALTERNATE implementation of Q2 (best year per series_id, with a
human-readable label) in PySpark, using pyspark.sql.Window instead of a SQL
window function.

The SQL version in transformations/gold/gold_series_best_year.sql is the
PRIMARY implementation actually wired into the pipeline. This file lives in
transformations/gold_alternates/ - a sibling of transformations/gold/, not a
subfolder of it - specifically so resources/bls_pipeline.yml's
`transformations/gold/**` library glob never picks it up, so it is present
for review but not executed.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog_name")


@dp.table(
    name=f"{CATALOG}.gold.gold_series_best_year",
    comment="Best year (largest summed quarterly value) per BLS series_id, with a human-readable label.",
)
@dp.expect_or_drop("non_null_series_id", "series_id IS NOT NULL")
@dp.expect_or_drop("non_null_best_year", "best_year IS NOT NULL")
def gold_series_best_year():
    observations = spark.read.table(f"{CATALOG}.silver.silver_pr_observations")
    dimensions = spark.read.table(f"{CATALOG}.silver.silver_pr_series_dim")

    # "Quarters" = Q01-Q04 only; Q05 is BLS's own "Annual Average" aggregate
    # (see pr.txt sections 3 & 7), not a 5th quarter to sum in.
    yearly_totals = (
        observations.filter(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("total_value"))
    )

    # Ties broken by earliest year, deterministically.
    best_year_window = Window.partitionBy("series_id").orderBy(
        F.col("total_value").desc(), F.col("year").asc()
    )
    best_years = (
        yearly_totals.withColumn("rn", F.row_number().over(best_year_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    return best_years.join(dimensions, "series_id", "inner").select(
        best_years["series_id"],
        dimensions["series_label"],
        best_years["year"].alias("best_year"),
        best_years["total_value"],
    )
