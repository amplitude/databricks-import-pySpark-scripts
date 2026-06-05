# databricks-import-pySpark-scripts

PySpark scripts that import data from Databricks into Amplitude (run by Falcon as
SparkPythonTask jobs via Git source).

## Scripts

- `unload_databricks_data_to_s3.py` — the production unload job.
- `unload_databricks_data_to_s3_partition.py` — unload variant with repartitioning.
- `databricks_sql_utils.py` — shared, pyspark-free SQL helpers used by all scripts
  and tools (table-name → temp-view substitution, etc.).

## Inspecting / testing a query

The unload job rewrites every source table reference in your SQL into a temp-view
name before running it. These tools let you see and validate exactly what runs.

### Preview the transformed SQL locally (no Spark, no credentials)

Preview the exact query the job will run, and lint it, on your laptop:

    python3 preview_databricks_query_sql.py \
      --table_versions_map cat.sch.a=0-5,cat.sch.b=0-5 \
      --data_type EVENT \
      --sql-file my_query.sql

It prints the transformed SQL and structural warnings (unbalanced parens/quotes,
tables in the version map that aren't referenced, tables referenced that are
missing from the map, and `--` line-comment fragility). These checks are
heuristic, not a SQL parser. Exit code is non-zero if there are findings.

### Validate against your cluster (read-only, never writes to S3)

Run as a SparkPythonTask on your Databricks cluster to actually parse/analyze the
query and check the required output columns for the data type:

    python3 validate_databricks_query.py \
      --table_versions_map cat.sch.a=0-5 \
      --data_type EVENT \
      --record-identity USER_ID \
      --sql-file my_query.sql \
      --sample 5

It creates the temp views, runs `spark.sql()` for parse + analysis (no write),
prints the resolved schema, and verifies the required columns for the data type:

| data_type | required columns |
|---|---|
| EVENT | `event_type`, `event_properties`, `time` + identity |
| USER_PROPERTY | `user_properties` + identity |
| GROUP_PROPERTY | `groups` |
| WAREHOUSE_PROPERTY | identity only |

`--record-identity` (default `USER_ID`) controls the identity columns:
`USER_ID` → `user_id`, `DEVICE_ID` → `device_id`, `USER_ID_AND_DEVICE_ID` → both.
Exit code is non-zero if a required column is missing.

## Tests

    python3 -m unittest discover -s test -v
