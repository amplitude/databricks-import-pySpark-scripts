# Databricks Query Validation Tool & Version/Schema Resilience — Design

Date: 2026-06-05
Status: Approved (pending spec review)

## Background

This repo holds PySpark scripts that Falcon runs as `SparkPythonTask` jobs on a
customer's Databricks workspace to unload Delta table data to S3.

**Deployment model (verified).** Falcon creates the Databricks job with
`SparkPythonTask.setSource(Source.GIT)` and a `GitSource` pointing at
`github.com/amplitude/databricks-import-pySpark-scripts` @ branch `main`
(`DatabricksToS3WorkerJobExecutor.java:846` and `:879`). Databricks therefore
checks out the **entire repo** and runs the entrypoint from within that checkout,
so the repo root is on `sys.path` at runtime. Two consequences:

- A sibling top-level module **can be imported** by the entrypoint scripts on the
  cluster — no zip/wheel bundling needed.
- Production jobs run off **`main`**, so the shared module and every entrypoint
  that imports it must land on `main` together (single PR).

Historically each script carried its own copy of helpers such as
`replace_table_name_in_sql`, guarded centrally by the substitution tests
(`test/test_table_name_substitution.py`, `_MODULES_UNDER_TEST`). Since the GIT
deployment model makes sibling imports safe, this design replaces that
duplication with a single shared module (see "Shared module" below).

Two customer-facing problems motivate this work:

1. **No visibility into the real query.** The unload script rewrites every source
   table reference in the customer's SQL into a temp-view name before running it
   (`replace_table_name_in_sql` → `build_views_for_tables` → `spark.sql`).
   Customers cannot see the transformed SQL that actually executes, so failures
   like the DWH-620 substring-collision corruption, or a `/* ... */` block
   comment / `event_type` projection issue, are hard for them to diagnose. They
   want a way to test a query and see exactly what runs.

2. **CDF reads break on table version/schema drift.** A continuous import reads
   the Delta Change Data Feed between a stored start version and the current
   (end) version. When the table schema changed in that range, Databricks raises
   `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA` (or
   `..._INCOMPATIBLE_SCHEMA_CHANGE` for column-mapping renames), failing the run.
   The script should be more resilient to these version changes.

## Goals

- Give customers a tool to validate a query and see the fully transformed SQL
  that the unload job will execute, both offline and on their cluster.
- Make the unload scripts resilient to CDF schema changes across versions —
  preventively where possible, with graceful recovery otherwise.
- Document both in the README.

## Non-Goals

- No changes to the Falcon (Java) side in this repo.
- No change to the unload write path / output format.
- No extraction of the pyspark-dependent view-building/fetch/filter logic into the
  shared module — only the pure (pyspark-free) SQL helpers are shared for now, to
  keep scope tight and the local preview tool dependency-free.

---

## Shared module: `databricks_sql_utils.py`

A single, **pyspark-free** module at the repo root holding the pure helpers that
every script and tool needs:

- `replace_table_name_in_sql` (+ `_IDENTIFIER_CHAR`) — the whole-identifier
  table-name → temp-view substitution (the DWH-620 fix lives here).
- `build_temp_view_name`
- `parse_table_versions_map_arg`

All entrypoints import from it and their local copies are deleted:
`unload_databricks_data_to_s3.py`, `unload_databricks_data_to_s3_partition.py`,
`TEST_unload_databricks_data_to_s3.py`, plus the two new tools below.

Keeping it pyspark-free means the local preview tool and the existing
pyspark-stubbed unit tests import it with no Spark install. The
pyspark-dependent logic (`build_views_for_tables`, `fetch_data`, `filter_data`)
stays in the on-cluster scripts for now.

Because production jobs run off `main` via GIT source, the shared module and all
importing entrypoints must be merged to `main` in the same PR.

---

## Feature 1: Query inspection — two distinct tools

The two use cases differ in everything — dependencies (none vs. full Spark + S3
secrets), where they run (laptop vs. Databricks job), audience, and what
"success" means ("here's the SQL" vs. "it parses and has the right columns").
They are therefore **two separate scripts**, each with one clear purpose. Both
import the shared `databricks_sql_utils.py`.

Shared inputs (same shape as the real job, so a customer can copy their job
config):

- `--table_versions_map` — e.g. `cat.sch.t1=0-12,cat.sch.t2=10-100`.
- `--data_type` — `EVENT` | `USER_PROPERTY` | `GROUP_PROPERTY` | `WAREHOUSE_PROPERTY`.
- `--sql` or `--sql-file` — the transformation SQL (file option avoids shell
  quoting pain for large multi-line queries).

### Tool A: `preview_databricks_query_sql.py` (local, offline)

Pure string work; no pyspark, no creds — runs on a laptop with zero setup.

- Applies the table → temp-view-name substitution and **prints the fully
  transformed SQL** — the exact text the real run hands to `spark.sql()`. This is
  the "see the real query being executed" ask.
- Structural lint that needs no cluster:
  - balanced parentheses and quotes,
  - every table named in the SQL is present in `--table_versions_map` and vice
    versa (flags typo'd / missing table mappings),
  - the collapsed-line `--` comment hazard (a `--` line comment that, if newlines
    were ever stripped, would comment out the rest of the query).
- Exit `0` = clean, non-zero = lint finding.

### Tool B: `validate_databricks_query.py` (on-cluster, runs as a `SparkPythonTask`)

Runs like the real job but read-only — never writes to S3.

- Creates the temp views for each table at the requested versions.
- Calls `spark.sql(transformed_sql)` and reads `.schema` — triggers Spark's
  parse + analysis **without materializing/writing** anything. This is what would
  have surfaced the customer's "no event_type" / comment problem for real.
- Prints the resolved output schema (column names + types).
- Runs a **required-column check** per `data_type`, mirroring Falcon's
  `RequiredColumnsRule` (`SQLTestSuite*Import.java`, `SQLUtils`):
  - `EVENT` → `event_type`, `event_properties`, `time` + identity
  - `USER_PROPERTY` → `user_properties` + identity
  - `GROUP_PROPERTY` → `groups` (no identity columns)
  - `WAREHOUSE_PROPERTY` → identity only
  - **identity** depends on the pipeline's record-identity setting
    (`SQLTestSuite.addRequiredIdentityColumns`): `USER_ID` → `user_id` (default),
    `DEVICE_ID` → `device_id`, `USER_ID_AND_DEVICE_ID` → both. The tool exposes an
    optional `--record-identity {USER_ID,DEVICE_ID,USER_ID_AND_DEVICE_ID}`
    (default `USER_ID`) so it matches the customer's pipeline config.
  - Note: the time-strategy `timestampColumn` requirement
    (`TIMESTAMP`/`MAX_TIMESTAMP` strategies) is pipeline config the tool does not
    receive, so it is intentionally out of scope for this check.
- A missing required column produces a clear FAIL naming the column.
- `--sample N`: `.limit(N).show()` so the customer can eyeball real rows. No write
  path exists in this tool under any flag.
- Exit `0` = pass, non-zero = validation failure (CI-friendly).

---

## Feature 2: Version/schema resilience in the unload scripts

**Scope clarified during planning.** The two parts land in different scripts
because the scripts have different structure:

- **(a) Preventive conf** → both `unload_databricks_data_to_s3.py` **and**
  `unload_databricks_data_to_s3_partition.py`. It is a single, safe line and
  applies regardless of the rest of the script.
- **(b) Recovery retry** → `unload_databricks_data_to_s3.py` **only**. The
  recovery machinery (`build_views_for_tables`, `extract_missing_cdf_error_signature`,
  the top-level latest-only retry) exists **only** in this script. The partition
  script (`...partition.py:169-203`) uses a simpler inline loop with no fallback
  scaffolding, and is **not referenced by the Falcon executor** (which wires only
  `unload_databricks_data_to_s3.py` and `TEST_unload_databricks_data_to_s3.py`,
  per `DatabricksToS3WorkerJobExecutor.java:122-123`). Porting the full retry
  machinery into the partition script is out of scope (YAGNI); it still gets the
  preventive conf.

The `TEST_unload_databricks_data_to_s3.py` canary script gets only the shared-module
import refactor — no resilience changes.

### (a) Preventive — Spark conf at session setup

Immediately after `SparkSession.builder.getOrCreate()`:

```python
spark.conf.set(
    "spark.databricks.delta.changeDataFeed.defaultSchemaModeForColumnMappingTable",
    "endVersion",
)
```

Tells Delta CDF to use the end-version schema when reading changes across a
column-mapping schema change, transparently fixing the common
`DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA` case for compatible/additive
changes — no customer cluster configuration required.

### (b) Recovery — extend the existing latest-only fallback (`unload_databricks_data_to_s3.py` only)

The script already recovers from missing CDF files via
`extract_missing_cdf_error_signature` driving a latest-only (`start=end=end_version`)
retry. Add a sibling detector:

```python
def extract_incompatible_schema_error_signature(error) -> Optional[str]:
    # matches DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA
    #     and DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE
```

Wire it into the same two catch points the missing-file path already uses:

- the per-table `except` in `build_views_for_tables`, and
- the top-level `except` in `__main__`.

On an incompatible-schema failure, retry the offending table at end-version-only
instead of failing the whole run. The trade-off — the changed-version range is
skipped, so slightly less data is read for that one table — is logged via the
existing `log_info` / `table_results` mechanism, so it is visible in the run's
`table_results.json`.

This mirrors the Falcon-side DWHO-3987 error classification, but here it is
actual recovery on the Spark side rather than error labeling.

---

## README

`README.md` (currently two lines) gains a usage section documenting:

- **Preview your SQL locally** (`preview_databricks_query_sql.py`): flags and a
  copy-paste example.
- **Validate against your cluster** (`validate_databricks_query.py`): flags,
  example, and the read-only / no-write guarantee.
- **Resilience behavior**: what the `endVersion` conf does and when a table is
  auto-retried latest-only (and the data-skipping implication).

---

## Testing

- `test/test_table_name_substitution.py`: simplified to test the single shared
  `databricks_sql_utils.replace_table_name_in_sql` directly (the `_MODULES_UNDER_TEST`
  fan-out across per-script copies is removed, since there is now one copy).
- New unit tests (pyspark-stubbed, like the existing pure-helper tests):
  - `extract_incompatible_schema_error_signature` matches both error codes and
    returns `None` otherwise,
  - `preview_databricks_query_sql.py`: transformed-SQL output and each lint check
    (balanced parens/quotes, table-map mismatch, `--`-comment hazard),
  - `validate_databricks_query.py`: the required-column check (pass and fail cases
    per `data_type`) exercised against an in-memory schema, with pyspark stubbed.

## Affected files

- `databricks_sql_utils.py` (new — shared pyspark-free helpers)
- `preview_databricks_query_sql.py` (new — local preview tool)
- `validate_databricks_query.py` (new — on-cluster validation tool)
- `unload_databricks_data_to_s3.py` (import shared module; resilience conf + retry)
- `unload_databricks_data_to_s3_partition.py` (import shared module; resilience conf only)
- `TEST_unload_databricks_data_to_s3.py` (import shared module — delete local copy)
- `test/test_table_name_substitution.py` (point at the shared module)
- `test/` new resilience + preview + validation tests
- `README.md` (usage docs)

All of the above ship in **one PR to `main`** (GIT-source deployment requires the
shared module and its importers to be present together).
