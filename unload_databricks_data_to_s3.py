import argparse
import collections
import json
import math
import uuid
import time
from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, ArrayType, MapType, NullType, DataType
)

# cargo ingestion is impacted when the file size is greater than 2GB, because
# the ingested files need to be broken down into smaller files by chopper
# this value was adjusted from 1M down to 100K for the Zillow POC (2025-08-14)
# to try to get Zillow files under 2GB each
MAX_RECORDS_PER_OUTPUT_FILE: int = 100_000

MISSING_CDF_FILE_ERROR_SIGNATURE = "DELTA_CHANGE_DATA_FILE_NOT_FOUND"
SPARK_DBR_FILE_NOT_EXIST_SIGNATURE = "FAILED_READ_FILE.DBR_FILE_NOT_EXIST"

LOG_MESSAGES: list[str] = []


def log_info(message: str) -> None:
    """
    Print a timestamped log message with INFO level.
    Uses print instead of logging module to avoid conflicts with Spark's Log4j configuration.
    
    :param message: Message to log
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"{timestamp} [INFO] {message}"
    print(log_message)
    LOG_MESSAGES.append(log_message)


def get_databricks_run_id() -> str:
    """
    Retrieve the Databricks job run ID for SparkPythonTask jobs.
    This script is always executed as a SparkPythonTask, never as a notebook.
    Falls back to UUID if retrieval fails.
    
    Uses safeToJson() which is whitelisted in shared access mode clusters.
    """
    
    try:
        context_json = dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()
        context = json.loads(context_json)
        
        # Get multitaskParentRunId (the job run ID)
        # Note: We do NOT use currentRunId as it's the task run ID, not job run ID
        attributes = context.get('attributes', {})
        run_id = attributes.get('multitaskParentRunId')
        
        if run_id:
            log_info(f"Retrieved job run ID: {run_id}")
            return str(run_id)
        
        # If multitaskParentRunId not found, log available attributes and fall back to UUID
        log_info(f"WARNING: multitaskParentRunId not found in safeToJson(). Available attributes: {list(attributes.keys())}")
    except Exception as exc:
        log_info(f"Failed to retrieve run ID from safeToJson(): {exc}")
    
    # Fallback: Generate UUID
    fallback_id = str(uuid.uuid1())
    log_info(f"Generated fallback run ID: {fallback_id}")
    return fallback_id

def extract_missing_cdf_error_signature(error: Exception) -> Optional[str]:
    """
    Return a signature string if the exception message indicates a CDF-related missing file.
    We match on the explicit CDF message, and also allow the Spark DBR file-not-exist signature
    as a fallback
    """
    message = str(error) if error else ""
    if not message:
        return None
    if MISSING_CDF_FILE_ERROR_SIGNATURE in message:
        return MISSING_CDF_FILE_ERROR_SIGNATURE
    if SPARK_DBR_FILE_NOT_EXIST_SIGNATURE in message:
        return SPARK_DBR_FILE_NOT_EXIST_SIGNATURE
    return None

def _drop_nulltype_fields(col: Column, dtype: DataType) -> Column:
    """
    Recursively remove NullType (VOID) fields from a column while preserving structure.
    """
    if isinstance(dtype, StructType):
        # Keep only fields that are not NullType
        valid_fields = [
            f for f in dtype.fields if not isinstance(f.dataType, NullType)
        ]
        if not valid_fields:
            # Struct becomes completely empty; return null (Spark has issues with empty structs)
            return F.lit(None)
        return F.struct(*[
            _drop_nulltype_fields(col.getField(f.name), f.dataType).alias(f.name)
            for f in valid_fields
        ])

    elif isinstance(dtype, ArrayType):
        # Clean each element, then DROP null elements; empty arrays remain []
        cleaned = F.transform(col, lambda x: _drop_nulltype_fields(x, dtype.elementType))
        cleaned = F.filter(cleaned, lambda x: x.isNotNull())
        return cleaned

    elif isinstance(dtype, MapType):
        # Maps can't have NullType keys, but we can clean NullType values
        if isinstance(dtype.valueType, NullType):
            # {} : build from empty entries to preserve "empty map"
            return F.map_from_arrays(F.array(), F.array())
        return F.map_from_entries(
            F.transform(
                F.map_entries(col),
                lambda kv: F.struct(
                    kv["key"].alias("key"),
                    _drop_nulltype_fields(kv["value"], dtype.valueType).alias("value")
                )
            )
        )

    elif isinstance(dtype, NullType):
        # Drop this field (return None)
        return F.lit(None)

    else:
        # Non-nulltype primitive or supported data type
        return col


def drop_void_fields(df: DataFrame) -> DataFrame:
    """
    Returns a new DataFrame with all NullType (VOID) fields removed recursively.
    """
    new_cols = []
    for f in df.schema.fields:
        if isinstance(f.dataType, NullType):
            # Skip VOID columns entirely
            continue
        elif isinstance(f.dataType, ArrayType) and isinstance(f.dataType.elementType, NullType):
            # Skip arrays of void entirely
            continue
        elif isinstance(f.dataType, MapType) and isinstance(f.dataType.valueType, NullType):
            # Skip maps with void values entirely
            continue
        new_cols.append(_drop_nulltype_fields(F.col(f.name), f.dataType).alias(f.name))
    return df.select(*new_cols)

def parse_table_versions_map_arg(table_versions_map: str) -> dict[str, list[int]]:
    """
    Extract table && version range numbers from input str.
    :param table_versions_map: table versions map. Sample input 'catalog.schema.table=1-2,catalog.schema2.table2=11-12'
    which means table 'catalog.schema.table' with version range [1,2] and table 'catalog.schema2.table2'
    with version range [11,12].
    :return: table to version ranges map. Sample output: {'catalog.schema.table': [1,2]}
    """
    dictionary = collections.defaultdict(list)
    table_and_versions_list = table_versions_map.split(",")
    for table_and_versions in table_and_versions_list:
        table_name = table_and_versions.split("=")[0]
        versions = table_and_versions.split("=")[1].split("-")
        dictionary[table_name].append(int(versions[0]))
        dictionary[table_name].append(int(versions[1]))
    return dictionary


def build_temp_view_name(table_full_name: str) -> str:
    """
    Build temp view name for the table. Wrap table name with '`' to escape '.'. Append `epoch` so view name is very
    unlikely collapse with another table.
    :param table_full_name: table name
    :return: temp view name for the table
    """
    return '`{table}.{epoch}`'.format(table=table_full_name, epoch=int(time.time()))


def build_sql_to_query_table_of_version(table_full_name: str, ending_version: int) -> str:
    sql_statement = "select * from {table} version as of {version}".format(table=table_full_name, version=ending_version)
    print("SQL statement to fetch data: {sql}.".format(sql=sql_statement))
    return sql_statement


def build_sql_to_query_table_between_versions(table_full_name: str, starting_version: int, ending_version: int) -> str:
    sql_statement = "select * from table_changes(\"{table}\", {starting_version}, {ending_version})".format(
        table=table_full_name, starting_version=starting_version, ending_version=ending_version)
    print("SQL statement to fetch data: {sql}.".format(sql=sql_statement))
    return sql_statement


def fetch_data(table_full_name: str, starting_version: int, ending_version: int) -> DataFrame:
    if starting_version == 0:
        return spark.sql(build_sql_to_query_table_of_version(table_full_name, ending_version))
    else:
        return spark.sql(build_sql_to_query_table_between_versions(table_full_name, starting_version, ending_version))


def filter_data(data_frame: DataFrame, data_type: str) -> DataFrame:
    if "_change_type" in data_frame.columns:
        if data_type == "EVENT":
            # for EVENT, only keep new inserted rows.
            data_frame = data_frame.filter(col("_change_type").isNull() | col("_change_type").eqNullSafe("insert"))
        else:
            # For USER_PROPERTY and GROUP_PROPERTY, keep both insert && updated rows.
            data_frame = data_frame.filter(col("_change_type").isNull() | col("_change_type").eqNullSafe("insert")
                                           | col("_change_type").eqNullSafe("update_postimage"))
        data_frame = data_frame.drop("_commit_version", "_commit_timestamp", "_change_type")
    return data_frame


def get_partition_count(event_count: int, max_event_count_per_output_file: int) -> int:
    return max(1, math.ceil(event_count / max_event_count_per_output_file))


def calculate_num_partitions(df: DataFrame, max_records_per_file: int, target_partitions: int = None) -> int:
    """
    Calculate the number of partitions needed based on DataFrame record count and max records per file.
    Optionally uses target_partitions (derived from cluster size) to optimize parallelism.
    Logs the count time and partition calculation.
    
    :param df: DataFrame to count
    :param max_records_per_file: Maximum records per output file
    :param target_partitions: Target partition count from cluster config (optional, pre-calculated as maxNodes * multiplier)
    :return: Number of partitions needed (minimum 1)
    """
    # TODO: Add unit tests for partition calculation logic with various target_partitions values
    if target_partitions is not None:
        # Use target_partitions directly for full control during testing/rollout
        # Once we understand performance impact, we may revert to max(calculated, target)
        num_partitions = max(1, target_partitions)
        log_info(f"Partition sizing: using target from cluster={num_partitions}")
    else:
        count_start = time.time()
        record_count = df.count()
        count_time = time.time() - count_start
        log_info(f"DataFrame count: {record_count:,} records (took {count_time:.2f}s)")
        
        calculated_partitions = math.ceil(record_count / max_records_per_file)
        num_partitions = max(1, calculated_partitions)
        log_info(f"Partition sizing: using {num_partitions} partitions (from record count)")
    
    return num_partitions


def export_meta_data(event_count: int, partition_count: int):
    meta_data: list = [{'event_count': event_count, 'partition_count': partition_count}]
    spark.createDataFrame(meta_data).write.mode("overwrite").json(args.s3_path + "/meta")



def build_views_for_tables(
        original_sql: str,
        table_to_import_version_range_map: dict[str, list[int]],
        data_type: str,
        ingestion_in_mutability_mode: bool,
        table_results: dict[str, dict[str, object]],
        force_latest_only: bool = False) -> str:
    """
    Build temp views for all tables and replace table names in the provided SQL with view names.

    Rationale on error handling with Spark's lazy evaluation:
    - We first try to catch errors at the per-table level to identify the offending table and
      immediately flip ONLY that table to [end,end]. This helps attribution and minimizes skipping.
      However, because Spark defers execution, some file-missing problems won't surface here.
    - For those deferred failures, we also have a top-level catch (see caller) that retries
      ALL tables in latest-only mode after a CDF signature is detected during the write.

    Modes:
    - Normal (force_latest_only=False):
      Attempt each view with [start,end]; on a table-specific CDF error, fallback that table to [end,end]
      and record the versions in table_results. Other tables keep their original range.

    - Latest-only (force_latest_only=True):
      Force every table to [end,end] and update table_results for each table.

    Returns the transformed SQL with table identifiers replaced by temp view names.
    """
    sql_local = original_sql
    for table, import_version_range in table_to_import_version_range_map.items():
        starting_version = import_version_range[0]
        ending_version = import_version_range[1]

        if table not in table_results:
            table_results[table] = {
                "initialStartVersion": starting_version,
                "initialEndVersion": ending_version,
                "initialFetchError": None,
                "finalStartVersion": starting_version,
                "finalEndVersion": ending_version,
            }

        log_info(f"Processing table: {table}, version range: {starting_version}-{ending_version}")

        def _fetch_and_create_view(start_v: int, end_v: int):
            df = fetch_data(table, start_v, end_v)
            if not ingestion_in_mutability_mode:
                df = filter_data(df, data_type)
            view_name: str = build_temp_view_name(table)
            df.createOrReplaceTempView(view_name)
            return view_name

        if force_latest_only:
            # Force start=end=end_version
            table_results[table]["finalStartVersion"] = ending_version
            table_results[table]["finalEndVersion"] = ending_version
            view_name = _fetch_and_create_view(ending_version, ending_version)
            sql_local = sql_local.replace(table, view_name)
            log_info(f"Forced latest-only read for {table} at version {ending_version}.")
            continue

        # Default path with per-table fallback retained:
        # Only the failing table is switched to [end,end]; others keep their original [start,end].
        # Note: Missing-file errors may not trigger here due to lazy evaluation; those will be
        # handled by the higher-level catch during the write phase.
        try:
            view_name = _fetch_and_create_view(starting_version, ending_version)
            sql_local = sql_local.replace(table, view_name)
        except Exception as fetch_error:
            fallback_signature = extract_missing_cdf_error_signature(fetch_error)
            if fallback_signature is None:
                # propagate non-CDF errors
                raise
            log_info(
                f"Encountered missing CDF files for {table} (signature={fallback_signature}). "
                f"Skipping versions {table_results[table]['initialStartVersion']}-{table_results[table]['initialEndVersion'] - 1} and re-reading at last known good version {ending_version}."
            )
            table_results[table]["initialFetchError"] = str(fetch_error)
            table_results[table]["finalStartVersion"] = ending_version
            table_results[table]["finalEndVersion"] = ending_version

            view_name = _fetch_and_create_view(ending_version, ending_version)
            sql_local = sql_local.replace(table, view_name)
            log_info(f"Successfully read {table} at version {ending_version}.")

    return sql_local


def write_export_data_for_versions(
        original_sql: str,
        table_to_import_version_range_map: dict[str, list[int]],
        data_type: str,
        ingestion_in_mutability_mode: bool,
        table_results: dict[str, dict[str, object]],
        args,
        force_latest_only: bool) -> None:
    """
    Build views for all tables (optionally forcing latest-only), create export DataFrame, apply
    partitioning strategy, and perform the write which TRIGGERS execution (lazy → materialized).

    Error surfacing strategy with lazy execution:
    - Exceptions often occur only at write time. We therefore propagate any exception to the caller,
      which inspects the message for the CDF signature and, if present, retries the retrieval, transformation, 
      and write in latest-only mode to recover. If the error is non-CDF, the caller re-raises.
    """
    sql_to_run = build_views_for_tables(
        original_sql=original_sql,
        table_to_import_version_range_map=table_to_import_version_range_map,
        data_type=data_type,
        ingestion_in_mutability_mode=ingestion_in_mutability_mode,
        table_results=table_results,
        force_latest_only=force_latest_only,
    )

    # Create export DataFrame (deferred execution)
    log_info("Creating DataFrame with SQL transformation (execution deferred)")
    export_data: DataFrame = spark.sql(sql_to_run)

    # Validate max_records_per_file for any partitioning strategy
    if args.partitioning_strategy != 'none' and args.max_records_per_file <= 0:
        raise ValueError(
            f"max_records_per_file must be greater than 0 when using partitioning strategy '{args.partitioning_strategy}', "
            f"got {args.max_records_per_file}")

    # Apply partitioning strategy
    if args.partitioning_strategy == 'repartition':
        log_info(f"Applying repartition strategy with max_records_per_file={args.max_records_per_file}")
        num_partitions = calculate_num_partitions(export_data, args.max_records_per_file, args.target_partitions)
        log_info(f"Planning repartition to {num_partitions} partitions (will execute during write)")
        export_data = export_data.repartition(num_partitions)
    elif args.partitioning_strategy == 'coalesce':
        log_info(f"Applying coalesce strategy with max_records_per_file={args.max_records_per_file}")
        spark.conf.set("spark.sql.files.maxRecordsPerFile", args.max_records_per_file)
        num_partitions = calculate_num_partitions(export_data, args.max_records_per_file, args.target_partitions)
        log_info(f"Planning coalesce to {num_partitions} partitions (will execute during write)")
        export_data = export_data.coalesce(num_partitions)
    else:
        log_info("No partitioning strategy specified - writing with existing partition structure")

    # Write in requested format (triggers full execution)
    log_info(f"Starting write operation to {args.s3_path} in {args.format} format")
    log_info("This action will execute all deferred operations: read → filter → transform → repartition/coalesce → write")
    write_start = time.time()

    if args.format == 'json':
        export_data.write.mode("overwrite").json(args.s3_path)
    elif args.format == 'parquet':
        export_data = drop_void_fields(export_data)
        export_data.write.mode("overwrite").option("compression", "zstd").option("compressionLevel", 3).parquet(args.s3_path)
    else:
        raise ValueError(f"Unsupported format: {args.format}")

    write_time = time.time() - write_start
    log_info(f"Write complete in {write_time:.2f} seconds")


# Example: python3 ./unload_databricks_data_to_s3.py --table_versions_map test_category_do_not_delete_or_modify.canary_tests.employee=16-16 --data_type EVENT --sql "select unix_millis(current_timestamp()) as time, id as user_id, \"databricks_import_canary_test_event\" as event_type, named_struct('name', name, 'home', home, 'age', age, 'income', income) as user_properties, named_struct('group_type1', ARRAY(\"group_A\", \"group_B\")) as groups, named_struct('group_property', \"group_property_value\") as group_properties from test_category_do_not_delete_or_modify.canary_tests.employee" --secret_scope amplitude_databricks_import --secret_key_name_for_aws_access_key source_destination_55_batch_1350266533_aws_access_key --secret_key_name_for_aws_secret_key source_destination_55_batch_1350266533_aws_secret_key --secret_key_name_for_aws_session_token source_destination_55_batch_1350266533_aws_session_token --s3_region us-west-2 --s3_endpoint s3.us-west-2.amazonaws.com --s3_path s3a://com-amplitude-falcon-stag2/databricks_import/unloaded_data/source_destination_55/batch_1350266533/
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='unload data from databricks using SparkPython')
    # replace 'required=True' with 'nargs='?', default=None' to make it optional.
    parser.add_argument("--table_versions_map", required=True,
                        help="""tables and version ranges where data imported from.
        Format syntax is '[{tableVersion},...,{tableVersion*N}]'. '{tableVersion}' will be
        '{catalogName}.{schemaName}.{tableName}={startingVersion}-{endingVersion}'.
        Example: catalog1.schema1.table1=0-12,catalog2.schema2.table2=10-100 """)
    parser.add_argument("--data_type", required=True,
                        choices=['EVENT', 'USER_PROPERTY', 'GROUP_PROPERTY', 'WAREHOUSE_PROPERTY'],
                        help="""type of data to be imported.""")
    parser.add_argument("--secret_scope", required=True, help="databricks secret scope name")
    parser.add_argument("--secret_key_name_for_aws_access_key", required=True,
                        help="databricks secret key name of aws_access_key")
    parser.add_argument("--secret_key_name_for_aws_secret_key", required=True,
                        help="databricks secret key name of aws_secret_key")
    parser.add_argument("--secret_key_name_for_aws_session_token", required=True,
                        help="databricks secret key name of aws_session_token")
    parser.add_argument("--secret_key_name_for_sql", required=True,
                        help="databricks secret key name of transformation sql")
    parser.add_argument("--s3_endpoint", required=True, help="s3 endpoint")
    parser.add_argument("--s3_path", required=True, help="s3 path where data will be written into")
    parser.add_argument("--ingestion_in_mutability_mode",
                        help="""if provided, will not apply filter to exclude change data for some mutation actions.
                        Otherwise, will include append-only (i.e. insert) for event data and upsert-only (i.e. insert
                        and update_postimage) for user/group properties. The filter is enabled by default.""",
                        action='store_true', default=False)
    parser.add_argument("--partitioning-strategy",
                        choices=['none', 'repartition', 'coalesce'],
                        default='none',
                        help="Partitioning strategy: none (default), repartition (split based on max_records_per_file), coalesce (future use)")
    parser.add_argument("--max_records_per_file",
                        help="max records per output file",
                        nargs='?',
                        type=int,
                        default=MAX_RECORDS_PER_OUTPUT_FILE,
                        const=MAX_RECORDS_PER_OUTPUT_FILE)
    parser.add_argument("--format",
                        choices=['json', 'parquet'],
                        default='json',
                        help="Output format: json (uncompressed) or parquet (zstd level 3)")
    parser.add_argument("--target_partitions",
                        help="Target number of partitions based on cluster size (optional, calculated as maxNodes * multiplier)",
                        nargs='?',
                        type=int,
                        default=None)

    args, unknown = parser.parse_known_args()

    start_time = time.time()
    log_info("Starting Databricks unload job")

    spark = SparkSession.builder.getOrCreate()
    # setup s3 credentials for data export
    aws_access_key = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_aws_access_key)
    aws_secret_key = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_aws_secret_key)
    aws_session_token = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_aws_session_token)
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    spark.conf.set("fs.s3a.access.key", aws_access_key)
    spark.conf.set("fs.s3a.secret.key", aws_secret_key)
    spark.conf.set("fs.s3a.session.token", aws_session_token)
    spark.conf.set("fs.s3a.endpoint", args.s3_endpoint)


    sql: str = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_sql)

    # Build temp views (with extra outer try/except to fallback latest-only for all tables)
    table_to_import_version_range_map: dict[str, list[int]] = parse_table_versions_map_arg(args.table_versions_map)
    run_id = get_databricks_run_id()
    log_info(f"Databricks run ID: {run_id}")
    table_results: dict[str, dict[str, object]] = {}

    try:
        write_export_data_for_versions(
            original_sql=sql,
            table_to_import_version_range_map=table_to_import_version_range_map,
            data_type=args.data_type,
            ingestion_in_mutability_mode=args.ingestion_in_mutability_mode,
            table_results=table_results,
            args=args,
            force_latest_only=False,
        )
    except Exception as e:
        sig = extract_missing_cdf_error_signature(e)
        if sig is None:
            # non-CDF error: re-raise immediately
            raise
        log_info(
            f"Failed with CDF missing-file signature ({sig}). "
            f"Retrying with latest-only (start=end=end_version) for all tables."
        )
        # If we get a CDF error, retry with only the latest version for all tables any exception should propagate
        write_export_data_for_versions(
            original_sql=sql,
            table_to_import_version_range_map=table_to_import_version_range_map,
            data_type=args.data_type,
            ingestion_in_mutability_mode=args.ingestion_in_mutability_mode,
            table_results=table_results,
            args=args,
            force_latest_only=True,
        )

    total_time = time.time() - start_time
    log_info(f"Total job time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    log_info("Databricks unload job completed successfully")

    logs_base = args.s3_path.rstrip("/") + f"/logs/run_{run_id}"
    log_info(f"Writing logs to {logs_base}")
    
    table_results_path = logs_base + "/table_results.json"
    logs_path = logs_base + "/logs.txt"
    dbutils.fs.put(table_results_path, json.dumps({"tables": table_results}, indent=2), overwrite=True)
    dbutils.fs.put(logs_path, "\n".join(LOG_MESSAGES), overwrite=True)