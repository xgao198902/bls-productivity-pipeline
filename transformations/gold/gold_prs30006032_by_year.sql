-- Q3: for series_id = PRS30006032, period = Q01, the value each year, joined
-- with that year's US population where available.
--
-- LEFT JOIN is required (not INNER): BLS coverage for this series starts in
-- 1987 while population coverage is 2013-2024 (minus 2020, a genuine ACS
-- 1-year-estimate gap - not a data quality issue), so most years legitimately
-- have a NULL population. An INNER JOIN would silently drop most of the
-- requested series history instead of answering "joined... where available".
--
-- Primary implementation (SQL). PySpark alternate:
--   transformations/gold_alternates/gold_prs30006032_by_year_pyspark.py (not wired into this pipeline).
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.gold_prs30006032_by_year (
  CONSTRAINT non_null_year EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "PRS30006032, period Q01: value per year, left-joined to that year's US population."
AS
SELECT
  o.year,
  o.value,
  p.population
FROM ${catalog_name}.silver.silver_pr_observations o
LEFT JOIN ${catalog_name}.silver.silver_population p
  ON o.year = p.year AND p.nation = 'United States'
WHERE o.series_id = 'PRS30006032'
  AND o.period = 'Q01'
ORDER BY o.year;
