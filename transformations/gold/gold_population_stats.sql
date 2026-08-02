-- Q1: mean and standard deviation of the annual US population, 2013-2018 inclusive.
--
-- Primary implementation (SQL). PySpark alternate:
--   transformations/gold_alternates/gold_population_stats_pyspark.py (not wired into this pipeline).
--
-- Standard deviation uses the sample formula (STDDEV / STDDEV_SAMP, divide by
-- N-1) - Spark's default for STDDEV. Documented assumption: swap to
-- STDDEV_POP below if population (divide by N) standard deviation is wanted
-- instead.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.gold_population_stats (
  CONSTRAINT has_mean EXPECT (mean_population IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Mean and (sample) standard deviation of US population, 2013-2018 inclusive."
AS
SELECT
  2013 AS year_start,
  2018 AS year_end,
  COUNT(*) AS n_years,
  AVG(population) AS mean_population,
  STDDEV(population) AS stddev_population
FROM ${catalog_name}.silver.silver_population
WHERE nation = 'United States'
  AND year BETWEEN 2013 AND 2018;
