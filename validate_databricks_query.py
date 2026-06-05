"""
On-cluster, read-only validation of a Databricks import query.

Run as a SparkPythonTask on the customer's cluster (same environment as the real
unload job). It creates the temp views, runs spark.sql() for parse + analysis
(WITHOUT writing anything to S3), prints the resolved output schema, and checks
the required output columns for the data type. It never writes data.

The required-column contract mirrors Falcon's RequiredColumnsRule
(SQLTestSuite*Import.java + SQLUtils):
    EVENT             -> event_type, event_properties, time + identity
    USER_PROPERTY     -> user_properties + identity
    GROUP_PROPERTY    -> groups (no identity)
    WAREHOUSE_PROPERTY-> identity only
identity depends on the pipeline's record-identity setting:
    USER_ID (default) -> user_id ; DEVICE_ID -> device_id ;
    USER_ID_AND_DEVICE_ID -> user_id + device_id
"""
import argparse
import sys

from databricks_sql_utils import (
    parse_table_versions_map_arg,
    build_temp_view_name,
    replace_table_name_in_sql,
)

_BASE_REQUIRED = {
    "EVENT": {"event_type", "event_properties", "time"},
    "USER_PROPERTY": {"user_properties"},
    "GROUP_PROPERTY": {"groups"},
    "WAREHOUSE_PROPERTY": set(),
}
# GROUP_PROPERTY does not require identity columns (it keys on `groups`).
_DATA_TYPES_WITH_IDENTITY = {"EVENT", "USER_PROPERTY", "WAREHOUSE_PROPERTY"}


def _identity_columns(record_identity: str) -> set:
    if record_identity == "USER_ID_AND_DEVICE_ID":
        return {"user_id", "device_id"}
    if record_identity == "DEVICE_ID":
        return {"device_id"}
    return {"user_id"}  # USER_ID / default


def required_columns_for(data_type: str, record_identity: str) -> set:
    """Required output columns for a data type, mirroring Falcon's RequiredColumnsRule."""
    required = set(_BASE_REQUIRED[data_type])
    if data_type in _DATA_TYPES_WITH_IDENTITY:
        required |= _identity_columns(record_identity)
    return required


def check_required_columns(actual_columns, data_type: str, record_identity: str) -> list[str]:
    """Return the sorted list of required columns missing from actual_columns."""
    actual = set(actual_columns)
    return sorted(required_columns_for(data_type, record_identity) - actual)


def _transform_sql(sql, versions, spark, ingestion_in_mutability_mode):
    """Create a temp view per table at its end version and rewrite the SQL."""
    transformed = sql
    for table, version_range in versions.items():
        ending_version = version_range[1]
        df = spark.sql(f"select * from {table} version as of {ending_version}")
        if not ingestion_in_mutability_mode and "_change_type" in df.columns:
            df = df.drop("_commit_version", "_commit_timestamp", "_change_type")
        view_name = build_temp_view_name(table)
        df.createOrReplaceTempView(view_name)
        transformed = replace_table_name_in_sql(transformed, table, view_name)
    return transformed


def main() -> int:
    from pyspark.sql import SparkSession  # lazy: only needed on-cluster

    parser = argparse.ArgumentParser(
        description="Validate a Databricks import query against the cluster (read-only, no S3 write)."
    )
    parser.add_argument("--table_versions_map", required=True,
                        help="e.g. catalog.schema.table=0-12,catalog2.schema2.table2=10-100")
    parser.add_argument("--data_type", required=True,
                        choices=["EVENT", "USER_PROPERTY", "GROUP_PROPERTY", "WAREHOUSE_PROPERTY"])
    parser.add_argument("--record-identity", default="USER_ID",
                        choices=["USER_ID", "DEVICE_ID", "USER_ID_AND_DEVICE_ID"],
                        help="record-identity setting of the pipeline (default USER_ID)")
    parser.add_argument("--ingestion_in_mutability_mode", action="store_true", default=False)
    parser.add_argument("--sample", type=int, default=0,
                        help="if > 0, show this many sample rows (read-only)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="the transformation SQL")
    group.add_argument("--sql-file", help="path to a file containing the transformation SQL")
    args, _ = parser.parse_known_args()

    if args.sql is not None:
        sql = args.sql
    else:
        try:
            with open(args.sql_file, "r") as sql_file:
                sql = sql_file.read()
        except OSError as error:
            print(f"Could not read --sql-file '{args.sql_file}': {error}", file=sys.stderr)
            return 2
    versions = parse_table_versions_map_arg(args.table_versions_map)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set(
        "spark.databricks.delta.changeDataFeed.defaultSchemaModeForColumnMappingTable",
        "endVersion",
    )

    transformed = _transform_sql(sql, versions, spark, args.ingestion_in_mutability_mode)
    print("=== Transformed SQL (this is what the job runs) ===")
    print(transformed)
    print()

    df = spark.sql(transformed)  # parse + analysis, no materialization
    actual_columns = df.columns
    print("=== Resolved output schema ===")
    df.printSchema()
    print()

    missing = check_required_columns(actual_columns, args.data_type, args.record_identity)
    if missing:
        print("=== Required-column check: FAIL ===")
        for column in missing:
            print(f"  - missing required column: {column}")
    else:
        print("=== Required-column check: PASS ===")

    if args.sample > 0:
        print(f"=== Sample ({args.sample} rows) ===")
        df.limit(args.sample).show(truncate=False)

    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
