"""ALTERNATE implementation of Q1 (mean/stddev of US population, 2013-2018) in
PySpark.

The SQL version in transformations/gold/gold_population_stats.sql is the
PRIMARY implementation actually wired into the pipeline. This file lives in
transformations/gold_alternates/ - a sibling of transformations/gold/, not a
subfolder of it - specifically so resources/bls_pipeline.yml's
`transformations/gold/**` library glob never picks it up, so it is present
for review but not executed. To swap which implementation feeds the Gold
table, move this file into transformations/gold/ and update the pipeline's
library glob to point at .py instead of .sql (or include both, dropping the
duplicate table name from one of them).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")


@dp.table(
    name=f"{CATALOG}.gold.gold_population_stats",
    comment="Mean and (sample) standard deviation of US population, 2013-2018 inclusive.",
)
@dp.expect_or_fail("has_mean", "mean_population IS NOT NULL")
def gold_population_stats():
    population = spark.read.table(f"{CATALOG}.silver.silver_population")
    return population.filter(
        (F.col("nation") == "United States") & F.col("year").between(2013, 2018)
    ).agg(
        F.lit(2013).alias("year_start"),
        F.lit(2018).alias("year_end"),
        F.count("*").alias("n_years"),
        F.avg("population").alias("mean_population"),
        # Sample stddev (Spark's default) - see the SQL primary's header comment
        # for the documented assumption; use F.stddev_pop for population stddev.
        F.stddev("population").alias("stddev_population"),
    )
