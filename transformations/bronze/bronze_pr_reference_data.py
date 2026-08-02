"""Bronze: raw copies of the small BLS mapping/lookup files that decode the
codes used in pr.series - sector, class, measure, duration, seasonal, period,
and footnote. Each is a tiny dimension table keyed by its code column.
"""

from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = spark.conf.get("catalog_name")
VOLUME_ROOT = spark.conf.get("bronze_volume_path")


def _read_bls_tsv(path: str, schema: StructType):
    return (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(schema)
        .load(path)
    )


def _standard_lookup_schema(code_field: str, text_field: str) -> StructType:
    """Most BLS mapping files share this shape: code, text, display_level,
    selectable, sort_sequence (pr.seasonal and pr.period are the exceptions,
    handled with their own schemas below)."""
    return StructType(
        [
            StructField(code_field, StringType(), True),
            StructField(text_field, StringType(), True),
            StructField("display_level", StringType(), True),
            StructField("selectable", StringType(), True),
            StructField("sort_sequence", StringType(), True),
        ]
    )


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_sector", comment="Raw pr.sector lookup: sector_code -> sector_name.")
@dp.expect_or_drop("non_null_sector_code", "sector_code IS NOT NULL")
def bronze_pr_sector():
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.sector", _standard_lookup_schema("sector_code", "sector_name"))


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_class", comment="Raw pr.class lookup: class_code -> class_text.")
@dp.expect_or_drop("non_null_class_code", "class_code IS NOT NULL")
def bronze_pr_class():
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.class", _standard_lookup_schema("class_code", "class_text"))


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_measure", comment="Raw pr.measure lookup: measure_code -> measure_text.")
@dp.expect_or_drop("non_null_measure_code", "measure_code IS NOT NULL")
def bronze_pr_measure():
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.measure", _standard_lookup_schema("measure_code", "measure_text"))


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_duration", comment="Raw pr.duration lookup: duration_code -> duration_text.")
@dp.expect_or_drop("non_null_duration_code", "duration_code IS NOT NULL")
def bronze_pr_duration():
    return _read_bls_tsv(
        f"{VOLUME_ROOT}/bls/pr/pr.duration", _standard_lookup_schema("duration_code", "duration_text")
    )


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_seasonal", comment="Raw pr.seasonal lookup: seasonal_code -> seasonal_text.")
@dp.expect_or_drop("non_null_seasonal_code", "seasonal_code IS NOT NULL")
def bronze_pr_seasonal():
    # pr.seasonal's header is "Seasonal_code"/"Seasonal_text" (capitalized),
    # unlike every other mapping file - an explicit schema sidesteps this.
    schema = StructType(
        [
            StructField("seasonal_code", StringType(), True),
            StructField("seasonal_text", StringType(), True),
        ]
    )
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.seasonal", schema)


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_period", comment="Raw pr.period lookup: period -> period_name.")
@dp.expect_or_drop("non_null_period", "period IS NOT NULL")
def bronze_pr_period():
    schema = StructType(
        [
            StructField("period", StringType(), True),
            StructField("period_abbr", StringType(), True),
            StructField("period_name", StringType(), True),
        ]
    )
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.period", schema)


@dp.table(name=f"{CATALOG}.bronze.bronze_pr_footnote", comment="Raw pr.footnote lookup: footnote_code -> footnote_text.")
@dp.expect_or_drop("non_null_footnote_code", "footnote_code IS NOT NULL")
def bronze_pr_footnote():
    schema = StructType(
        [
            StructField("footnote_code", StringType(), True),
            StructField("footnote_text", StringType(), True),
        ]
    )
    return _read_bls_tsv(f"{VOLUME_ROOT}/bls/pr/pr.footnote", schema)
