"""ALTERNATE implementation of Q3 (PRS30006032, period Q01, value per year
left-joined to population) in PySpark.

The SQL version in transformations/gold/gold_prs30006032_by_year.sql is the
PRIMARY implementation actually wired into the pipeline. This file lives in
transformations/gold_alternates/ - a sibling of transformations/gold/, not a
subfolder of it - specifically so resources/bls_pipeline.yml's
`transformations/gold/**` library glob never picks it up, so it is present
for review but not executed.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")


@dp.table(
    name=f"{CATALOG}.gold.gold_prs30006032_by_year",
    comment="PRS30006032, period Q01: value per year, left-joined to that year's US population.",
)
@dp.expect_or_drop("non_null_year", "year IS NOT NULL")
def gold_prs30006032_by_year():
    observations = spark.read.table(f"{CATALOG}.silver.silver_pr_observations").filter(
        (F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01")
    )
    # LEFT JOIN: BLS coverage starts in 1987, population coverage is 2013-2024
    # (minus a genuine 2020 ACS gap) - most years legitimately have no population.
    population = spark.read.table(f"{CATALOG}.silver.silver_population").filter(
        F.col("nation") == "United States"
    )

    return (
        observations.alias("o")
        .join(population.alias("p"), on=F.col("o.year") == F.col("p.year"), how="left")
        .select(F.col("o.year"), F.col("o.value"), F.col("p.population"))
        .orderBy("o.year")
    )
