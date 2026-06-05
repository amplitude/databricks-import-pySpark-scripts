# Databricks Query Validation Tool & Version/Schema Resilience — Design

Date: 2026-06-05
Status: Approved (pending spec review)

## Background

This repo holds standalone PySpark scripts that Falcon uploads to a customer's
Databricks workspace and runs as `SparkPythonTask` jobs to unload Delta table
data to S3. Each script is self-contained (uploaded and executed independently),
so shared helpers such as `replace_table_name_in_sql` are intentionally
duplicated across scripts and guarded centrally by the substitution tests
(`test/test_table_name_substitution.py`, `_MODULES_UNDER_TEST`).

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
- No refactor of the deliberate "each script is self-contained" deployment model.
- No change to the unload write path / output format.

---

## Feature 1: Query-validation tool

**New standalone script:** `validate_databricks_query.py`, alongside the unload
scripts. Self-contained, carries its own copy of the substitution/view helpers,
and is added to `_MODULES_UNDER_TEST` in `test/test_table_name_substitution.py`
so the substring-collision regression tests cover it too.

### Inputs

Same shape as the real job, so a customer can copy their job config:

- `--table_versions_map` — e.g. `cat.sch.t1=0-12,cat.sch.t2=10-100` (same parser
  as the unload script).
- `--data_type` — `EVENT` | `USER_PROPERTY` | `GROUP_PROPERTY` | `WAREHOUSE_PROPERTY`.
- `--sql` or `--sql-file` — the transformation SQL (file option avoids shell
  quoting pain for large multi-line queries).
- `--local` — offline mode flag (default is on-cluster).
- `--sample N` — (on-cluster only) show N sample rows.

### Mode: `--local` (offline, no Spark, no creds)

- Applies the table → temp-view-name substitution and **prints the fully
  transformed SQL** — the exact text the real run hands to `spark.sql()`.
- Structural lint that needs no cluster:
  - balanced parentheses and quotes,
  - every table named in the SQL is present in `--table_versions_map` and vice
    versa (flags typo'd / missing table mappings),
  - the collapsed-line `--` comment hazard (a `--` line comment that, if newlines
    were ever stripped, would comment out the rest of the query).
- pyspark is imported lazily so this mode runs on a laptop with no Spark install
  (the substitution logic is pure string manipulation, consistent with how the
  existing tests stub pyspark).

### Mode: on-cluster (default — run as a `SparkPythonTask`, like the real job)

- Everything `--local` prints, **plus**:
  - creates the temp views for each table at the requested versions,
  - calls `spark.sql(transformed_sql)` and reads `.schema` — this triggers
    Spark's parse + analysis **without writing anything to S3**,
  - prints the resolved output schema (column names + types),
  - runs a **required-column check** per `data_type`:
    - `EVENT` → `event_type`, `time`, and (`user_id` or `device_id`)
    - `USER_PROPERTY` / `GROUP_PROPERTY` / `WAREHOUSE_PROPERTY` → their respective
      required keys
    - **NOTE:** exact required-column sets to be confirmed against Falcon's
      ingestion contract before finalizing.
  - A missing required column produces a clear FAIL naming the column (this is
    what would have surfaced the customer's "no event_type" report).
- `--sample N`: `.limit(N).show()` so the customer can eyeball real rows. There
  is no write path in this tool under any flag.

### Output / exit codes

- Human-readable sections: transformed SQL, lint findings, schema, required-column
  result, optional sample.
- Exit code `0` = pass, non-zero = validation failure (CI-friendly).

---

## Feature 2: Version/schema resilience in the unload scripts

Applies to both production unload scripts: `unload_databricks_data_to_s3.py` and
`unload_databricks_data_to_s3_partition.py`. The `TEST_unload_databricks_data_to_s3.py`
canary script is intentionally left unchanged.

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

### (b) Recovery — extend the existing latest-only fallback

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

- the validation tool: both modes, all flags, and copy-paste examples,
- the resilience behavior: what the `endVersion` conf does and when a table is
  auto-retried latest-only (and the data-skipping implication).

---

## Testing

- `test/test_table_name_substitution.py`: add `validate_databricks_query` to
  `_MODULES_UNDER_TEST` so the substring-collision regression covers it.
- New unit tests (pyspark-stubbed, like the existing pure-helper tests):
  - `extract_incompatible_schema_error_signature` matches both error codes and
    returns `None` otherwise,
  - the validation tool's local-mode substitution output and required-column
    check (pass and fail cases per `data_type`).

## Affected files

- `validate_databricks_query.py` (new)
- `unload_databricks_data_to_s3.py` (resilience)
- `unload_databricks_data_to_s3_partition.py` (resilience)
- `test/test_table_name_substitution.py` (add new module to coverage)
- `test/` new resilience + validation tests
- `README.md` (usage docs)
