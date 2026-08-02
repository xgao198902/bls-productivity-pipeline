# Process Notes

This is the "why," written as a retrospective rather than a reference doc.
For the exact mechanics referenced below, see
[`docs/PIPELINE_PLAN.md`](docs/PIPELINE_PLAN.md) (design doc with the full
SQL, schemas, and DAG) and [`README.md`](README.md) (how to run it).

## Architecture

**Why Bronze/Silver/Gold at all, for a dataset this small.** The medallion
split isn't about scale here — the raw files are a few MB — it's about
separating three concerns that would otherwise get tangled into one query:
*fidelity* (Bronze keeps every raw column, verbatim, so nothing is lost if a
later assumption turns out wrong), *cleanup* (Silver is the one place codes
get decoded, types get cast, and duplicates get resolved — once, not
re-derived in every downstream query), and *the actual questions being asked*
(Gold only exists to answer the three required questions, and nothing else
touches it). Concretely, this paid off once already: when the "Nation ID"
column-name issue came up, the fix (enabling Delta column mapping) only
touched one Bronze table, because Silver already had the job of renaming
that column, and Gold never sees BLS-column-naming quirks at all.

**Why materialized views instead of streaming tables.** The ingestion job
*overwrites* files in place in the Volume rather than appending new ones —
there's no natural "new file arrived" event to hook Auto Loader onto, and
faking one (via `cloudFiles.allowOverwrites` plus manual checkpoint
resets) would add real complexity to work around a problem this data size
doesn't have. A `@dp.table` that fully reads the Volume on every pipeline
update is simpler, always correct by construction, and, per the Lakeflow
docs, expectations force a full recompute on materialized views regardless —
so there's no incremental-processing benefit actually being given up by this
choice at this scale.

**Why SQL is primary and PySpark is the alternate for Gold.** This is
genuinely a style choice — the two are functionally identical, and swapping
which one is wired into the pipeline is a one-line change to
`resources/bls_pipeline.yml`. SQL was picked as primary because these three
queries are aggregations and window functions with no non-trivial control
flow, and it reads more directly as "the answer to the question" without
DataFrame-API scaffolding around it. The PySpark versions exist as an
equivalence proof and live in `transformations/gold_alternates/`, a sibling
of `transformations/gold/` (not nested inside it), specifically so the
pipeline's `gold/**` glob never picks them up as duplicate datasets.

**How re-running ingestion safely was handled.** The requirement was
explicit: no hardcoded filenames, and no reprocessing of files that haven't
changed. Both ingestion notebooks crawl their source's live directory
listing (BLS's `pr/` index page, the Data USA API response) instead of
assuming a fixed file list, and both read/write a shared
`raw_ingestion_manifest` Delta table keyed on `(source, relative_path)` that
tracks size + last-modified (or a content hash, for the single population
JSON file) from the last successful ingest. A file only gets re-downloaded
if one of those has changed; everything else is skipped with no network call
beyond the directory listing itself. Files that disappear from the source
get marked `removed` in the manifest rather than silently forgotten — the
previously-landed file stays in the Volume for audit history. This also
answered a question I had partway through: chaining ingestion and the
pipeline refresh into one job (`setup -> ingest -> refresh`) means that even
on a "nothing changed" run, Bronze/Silver/Gold still recompute — which is
fine at this data size, but it's worth calling out explicitly as a
consequence of the "materialized views over streaming tables" choice above,
not something free.

**Why a Databricks Asset Bundle.** Once there's a setup notebook, two
ingestion notebooks, a multi-schema pipeline, and (eventually) a `dev`/`prod`
distinction, clicking these together by hand in the workspace UI stops being
reproducible. The bundle makes the whole environment — catalog, schemas,
volume, job, pipeline — buildable from a clean checkout, with `dev` and
`prod` as explicit, diff-able targets in `databricks.yml` rather than tribal
knowledge about which workspace has which resources.

**What resources the bundle actually declares, and what compute they run
on.** Two resources, both under `resources/`:

- `resources/ingestion_job.yml` — a **Job** (`bls_ingestion_job`) with four
  tasks: `setup_catalog_schemas_volume` -> `[ingest_bls_pr,
  ingest_population]` (parallel) -> `refresh_bls_pipeline` (a `pipeline_task`
  that triggers the resource below). Every notebook task runs on
  **serverless compute** — there's no job cluster definition anywhere in
  this bundle — since the data volume doesn't come close to justifying the
  cost or startup-time tuning of a dedicated cluster. No `trigger` is
  configured, so it's run-on-demand only (see Trade-offs, "Cost," for why).
- `resources/bls_pipeline.yml` — a **Lakeflow Declarative Pipeline**
  (`bls_medallion_pipeline`), also `serverless: true`, publishing across the
  `bronze`/`silver`/`gold` schemas of one catalog rather than needing
  per-schema pipelines. Its `libraries` are three `glob` includes (one per
  layer) rather than a per-file list, and its `configuration` block is how
  `catalog_name` and `bronze_volume_path` reach the transformation source
  files without hardcoding — read via `spark.conf.get(...)` in Python and
  `${...}` substitution in SQL.

Both resources are parameterized by the same bundle `variables`
(`catalog_name`, `bronze_volume_name`, `contact_email`), so `dev` and `prod`
point at different catalogs/paths without duplicating any resource
definitions — see [`README.md`](README.md#targets-dev--prod) for the
target-level differences (`root_path`, naming).

## Trade-offs

Decisions here were made for a take-home assignment against ~15 small files,
not a production system. Things I'd handle differently for a real client:

- **Schema drift.** Bronze tables read against a hardcoded `StructType`,
  which is a data-quality asset (catches unexpected shapes as *failures*
  rather than silently-wrong data) but also a liability if BLS or Data USA
  add a column — the read would need an explicit schema update, not
  auto-adapt. For a real client I'd add a lightweight schema-drift check
  (compare the raw file's header to the expected schema and alert rather
  than fail silently) so a source change is a Slack message, not a mystery
  pipeline failure discovered days later.
- **Data volume.** The "materialized view, full recompute every run" choice
  (see Architecture) is a direct consequence of the source files being a few
  MB. If this were, say, all of BLS's time-series data (many GB, genuinely
  append-only in practice), I'd switch to streaming tables with Auto Loader
  and incremental Silver/Gold logic — full recompute would get slow and
  wasteful well before it got infeasible.
- **Cost.** There's deliberately no schedule on `bls_ingestion_job` (to avoid
  eating Databricks Free Edition's daily fair-usage quota), and the pipeline
  runs on serverless compute with no cluster-sizing tuning. A real client
  would want a defined refresh cadence, budget alerts, and probably
  classic/job clusters with autoscaling and auto-termination tuned against
  actual data volume rather than serverless-by-default.
- **Access control.** Everything here lives in one catalog with default
  grants, and `prod` has no `run_as` configured, so ownership of a deploy
  depends on whoever happens to run `databricks bundle deploy -t prod` (an
  earlier version pinned a personal email there instead, which is worse —
  hardcoding an individual's identity into version control). A real
  deployment would set `run_as` to a dedicated service principal (not a
  person's account either way), plus Unity Catalog groups instead of
  individual grants, and — if any of the source data were sensitive rather
  than public BLS/Census data — row filters or column masks at the
  Silver/Gold boundary.
- **Monitoring.** Right now, "did it work" means opening the pipeline's run
  page and reading it. A real client needs job-failure notifications
  (email/Slack/PagerDuty), dashboards on the Lakeflow expectation metrics
  (rows dropped per expectation, over time — not just point-in-time), and a
  freshness SLO ("Gold should never be more than N hours stale") with
  alerting if ingestion silently stops finding new data.
- **Testing.** There's no automated test suite — correctness was verified by
  reading `pr.txt`'s documentation carefully and spot-checking row counts
  (see [`docs/guide/05-results.md`](docs/guide/05-results.md)) after each
  run. A real client codebase would have unit tests for the trickier
  transformation logic (the Q05-exclusion rule in particular — see
  Retrospective) that fail loudly if someone "simplifies" it later.
- **Secrets.** `contact_email` isn't secret, so a gitignored `.env` /
  `variable-overrides.json` is proportionate. A real client's actual
  credentials (API keys, DB passwords) would go in a Databricks secret
  scope, referenced via `{{secrets/scope/key}}` in the bundle, never in a
  local file at all — even a gitignored one.

## Retrospective

**Hardest to get right: a "successful" run that was actually silently
wrong.** `silver_pr_series_dim` and `silver_pr_observations` came back
completely empty after a pipeline run, but the run showed green, and every
expectation reported "100% written, 0% dropped." That combination is
deceptive — it looks like a clean pass, but it's actually the signature of a
materialized view computed over an *empty* upstream table: there's nothing
to violate an expectation against, so nothing gets flagged. It turned out to
be stale state from earlier ad hoc/partial testing (running the pipeline
before ingestion had actually populated Bronze). A Full Refresh fixed it
immediately once Bronze had real data — but the real lesson was that
"expectations all passed" is not the same claim as "this table has the data
it should," and the fix that actually matters is sequencing: the job chains
ingestion before the pipeline refresh precisely so this can't happen on a
real run, only on manual/partial testing.

**Second hardest: tooling friction in the Asset Bundle, not the data logic.**
The Lakeflow pipelines API rejects single-asterisk glob patterns
(`*.py`) and, separately, kept rejecting `libraries.file` entries built from
`${workspace.file_path}` substitution with a fairly opaque "Either glob or
notebook/file field ... should be set" error even once the referenced files
were confirmed to exist at the resolved path. Neither of these had anything
to do with the actual transformation logic; they cost more iteration than
anything data-related, and the eventual fix (restructure
`transformations/gold_alternates/` as a sibling folder and use a plain
`**` glob for all three layers) was simpler than continuing to chase the
`file:` + substitution path.

**Worth calling out separately: a documentation detail that would've been an
easy silent bug.** BLS's `pr.txt` explicitly documents `period = 'Q05'` as a
separately-computed "Annual Average," not a fifth quarter. Gold question 2
asks for the year with the largest total "summed across quarters" — naively
summing every `period` value per `series_id`/`year` would silently
double-count each year (once from Q01-Q04, again from Q05) and still produce
a plausible-looking, wrong answer with no error or expectation failure
anywhere. Catching this required actually reading the source's documentation
rather than inferring meaning from the code values alone — a good reminder
that "the schema looks fine" and "the aggregation is correct" are
independent claims.

## AI Assistance Disclosure

I used Cursor's agent (built on Claude) throughout this project as a
reference/pair-programming tool, not as a black box — I can walk through the
logic of every file in this repo. Concretely:

- The AI drafted the initial scaffolding at each stage: the medallion
  catalog/schema/volume setup, the manifest-driven incremental ingestion
  pattern, the Bronze/Silver/Gold transformation files, the Databricks Asset
  Bundle structure, and this documentation set.
- I ran everything for real against an actual Databricks workspace at every
  step, rather than trusting that drafted code would work, and fed the
  *actual* results back in — real 403 errors from BLS, real
  `INVALID_PARAMETER_VALUE` deployment failures, real
  `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` errors, and the real
  (surprising) symptom of Silver tables coming back empty with expectations
  reporting 100% written. Each of those drove a specific, verifiable fix
  (the `User-Agent` header, the `**` glob pattern, Delta column mapping, a
  Full Refresh plus reordering the job's tasks) rather than a guessed one.
- I independently verified the final output rather than taking generated
  code on faith — spot-checking row counts on `gold_series_best_year` (237)
  and `gold_prs30006032_by_year` (39) against what the pipeline's join logic
  should produce, and cross-checking `gold_population_stats`'s mean/stddev
  through the Genie Agent's own independently-generated SQL (see
  [`docs/guide/05-results.md`](docs/guide/05-results.md) and
  [`docs/guide/06-genie.md`](docs/guide/06-genie.md)).
- Design decisions that required reading and interpreting the source data
  directly — the `Q05`/"Annual Average" exclusion in particular — were
  confirmed against BLS's own `pr.txt` documentation, not accepted on the
  AI's say-so.

I'm able to explain the reasoning behind every choice documented above
(medallion layering, materialized views vs. streaming tables, the manifest
table's schema, why expectations live where they do, the Q05 exclusion) in
detail.
