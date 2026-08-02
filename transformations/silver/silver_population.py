"""Silver: cleaned population records - one row per (nation, year), with
snake_case column names in place of the API's "Nation ID"/"Year"/"Population".
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")


@dp.table(
    name=f"{CATALOG}.silver.silver_population",
    comment="Cleaned Data USA population records: nation_id, nation, year, population.",
)
@dp.expect_or_drop(
    "non_null_keys", "nation_id IS NOT NULL AND year IS NOT NULL AND population IS NOT NULL"
)
def silver_population():
    bronze = spark.read.table(f"{CATALOG}.bronze.bronze_population")
    return bronze.select(
        F.col("`Nation ID`").alias("nation_id"),
        F.col("Nation").alias("nation"),
        F.col("Year").cast("int").alias("year"),
        F.col("Population").cast("double").alias("population"),
    )
