-- Q2: for every series_id, the year with the largest sum of value across its
-- quarters, with a human-readable label so someone unfamiliar with BLS series
-- codes can understand at a glance what's being measured.
--
-- "Quarters" = period IN ('Q01','Q02','Q03','Q04') only. pr.txt sections 3 and
-- 7 both explicitly document Q05 as "Annual Average" - a separately computed
-- aggregate, not a 5th quarter - so including it would double-count each
-- year's total rather than summing "across quarters" as asked.
--
-- Ties (equal total_value) are broken by earliest year, deterministically.
--
-- Primary implementation (SQL). PySpark alternate (pyspark.sql.Window):
--   transformations/gold_alternates/gold_series_best_year_pyspark.py (not wired into this pipeline).
CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.gold.gold_series_best_year (
  CONSTRAINT non_null_series_id EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_null_best_year EXPECT (best_year IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Best year (largest summed quarterly value) per BLS series_id, with a human-readable label."
AS
WITH yearly_totals AS (
  SELECT series_id, year, SUM(value) AS total_value
  FROM ${catalog_name}.silver.silver_pr_observations
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    series_id,
    year,
    total_value,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC, year ASC) AS rn
  FROM yearly_totals
)
SELECT
  r.series_id,
  d.series_label,
  r.year AS best_year,
  r.total_value
FROM ranked r
JOIN ${catalog_name}.silver.silver_pr_series_dim d
  ON r.series_id = d.series_id
WHERE r.rn = 1;
