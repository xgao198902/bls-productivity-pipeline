"""Bronze: raw copy of the Data USA population API response, flattened from
its nested {annotations, page, columns, data: [...]} shape into one row per
(nation, year). Column names keep the source's original casing/spacing
("Nation ID", "Nation", "Year", "Population") - Silver renames them.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode

CATALOG = spark.conf.get("catalog_name")
VOLUME_ROOT = spark.conf.get("bronze_volume_path")


@dp.table(
    name=f"{CATALOG}.bronze.bronze_population",
    comment="Raw population.json, flattened to one row per (nation, year).",
    # Delta rejects spaces (and a few other characters) in column names by
    # default - "Nation ID" trips DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES
    # unless column mapping is enabled. This is the standard fix rather than
    # renaming the column here, so Bronze keeps the source's exact field
    # names as documented above (Silver does the renaming).
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
@dp.expect_or_drop(
    "non_null_keys",
    "`Nation ID` IS NOT NULL AND `Year` IS NOT NULL AND `Population` IS NOT NULL",
)
def bronze_population():
    raw = spark.read.option("multiLine", "true").json(f"{VOLUME_ROOT}/population/population.json")
    return raw.select(explode(col("data")).alias("rec")).select("rec.*")
