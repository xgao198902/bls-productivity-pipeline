"""Silver: one row per series_id with its codes decoded into human-readable
text via the pr.* lookup tables, plus a synthesized `series_label` so someone
unfamiliar with BLS series codes can understand at a glance what's measured.

Example: PRS30006032 (sector 3000, class 6, measure 03, duration 2, seasonal S)
becomes "Manufacturing: All workers, Hours worked, % Change from previous
quarter (Seasonally Adjusted)".
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, concat, lit

CATALOG = spark.conf.get("catalog_name")


@dp.table(
    name=f"{CATALOG}.silver.silver_pr_series_dim",
    comment="Decoded, human-readable dimensions for every BLS pr series_id.",
)
@dp.expect_or_drop("non_null_series_id", "series_id IS NOT NULL")
def silver_pr_series_dim():
    series = spark.read.table(f"{CATALOG}.bronze.bronze_pr_series")
    sector = spark.read.table(f"{CATALOG}.bronze.bronze_pr_sector")
    class_ = spark.read.table(f"{CATALOG}.bronze.bronze_pr_class")
    measure = spark.read.table(f"{CATALOG}.bronze.bronze_pr_measure")
    duration = spark.read.table(f"{CATALOG}.bronze.bronze_pr_duration")
    seasonal = spark.read.table(f"{CATALOG}.bronze.bronze_pr_seasonal")

    joined = (
        series.join(sector, "sector_code", "left")
        .join(class_, "class_code", "left")
        .join(measure, "measure_code", "left")
        .join(duration, "duration_code", "left")
        .join(seasonal, series["seasonal"] == seasonal["seasonal_code"], "left")
    )

    series_label = concat(
        coalesce(col("sector_name"), lit("Unknown sector")),
        lit(": "),
        coalesce(col("class_text"), lit("unknown class")),
        lit(", "),
        coalesce(col("measure_text"), lit("unknown measure")),
        lit(", "),
        coalesce(col("duration_text"), lit("unknown duration")),
        lit(" ("),
        coalesce(col("seasonal_text"), lit("unknown seasonality")),
        lit(")"),
    )

    return joined.select(
        series["series_id"].alias("series_id"),
        series_label.alias("series_label"),
        series["sector_code"].alias("sector_code"),
        col("sector_name"),
        series["class_code"].alias("class_code"),
        col("class_text"),
        series["measure_code"].alias("measure_code"),
        col("measure_text"),
        series["duration_code"].alias("duration_code"),
        col("duration_text"),
        series["seasonal"].alias("seasonal_code"),
        col("seasonal_text"),
        series["begin_year"].cast("int").alias("begin_year"),
        series["begin_period"].alias("begin_period"),
        series["end_year"].cast("int").alias("end_year"),
        series["end_period"].alias("end_period"),
    )
