# Gold alternates

Each file here is a fully-working PySpark implementation of one of the three
Gold questions, functionally equivalent to the SQL implementation of the same
table name in `transformations/gold/*.sql`. They exist to demonstrate the same
logic in both Spark SQL and PySpark, per the assignment's requirement -
SQL was picked as the "primary" version that actually feeds each Gold table,
and these are the documented alternates.

**These files are intentionally not executed.** `resources/bls_pipeline.yml`'s
library glob for gold only includes `transformations/gold/**` - this folder,
`transformations/gold_alternates/`, is a sibling of `transformations/gold/`
(not nested inside it) specifically so it's never swept in by that glob, and
the pipeline never registers these as datasets alongside the `.sql` versions
(which would otherwise be a duplicate-table-name conflict).

To make one of these the active implementation instead: move it into
`transformations/gold/`, then either remove the corresponding `.sql` file or
update `resources/bls_pipeline.yml`'s gold glob so only one implementation per
table is picked up.

| File | Gold table | SQL primary |
| --- | --- | --- |
| `gold_population_stats_pyspark.py` | `gold.gold_population_stats` | `../gold/gold_population_stats.sql` |
| `gold_series_best_year_pyspark.py` | `gold.gold_series_best_year` | `../gold/gold_series_best_year.sql` |
| `gold_prs30006032_by_year_pyspark.py` | `gold.gold_prs30006032_by_year` | `../gold/gold_prs30006032_by_year.sql` |
