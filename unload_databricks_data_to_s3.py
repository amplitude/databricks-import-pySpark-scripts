import argparse
import collections
import math
import time
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


def log_info(message: str) -> None:
    """
    Print a timestamped log message with INFO level.
    Uses print instead of logging module to avoid conflicts with Spark's Log4j configuration.
    
    :param message: Message to log
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} [INFO] {message}")

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

    # Build temp views
    table_to_import_version_range_map: dict[str, list[int]] = parse_table_versions_map_arg(args.table_versions_map)
    for table, import_version_range in table_to_import_version_range_map.items():
        data: DataFrame = fetch_data(table, import_version_range[0], import_version_range[1])

        if not args.ingestion_in_mutability_mode:
            data = filter_data(data, args.data_type)

        view_name: str = build_temp_view_name(table)
        data.createOrReplaceTempView(view_name)
        # replace table name in sql to get prepared for sql transformation
        sql = sql.replace(table, view_name)

    # Enable Adaptive Query Execution (AQE) for coalesce strategy to optimize partitioning while reading data. This should speed
    # up the final coalesce operation during the write step.
    if args.partitioning_strategy == 'coalesce':
        log_info("Configuring Adaptive Query Execution (AQE) for coalesce strategy")
        # Group small source files together during the initial read to prevent creating thousands of tiny partitions.
        # Instead of creating one partition for a small number of source files, Spark combines files until each
        # partition reaches ~128MB, reducing partition count.
        spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
        
        # Enable Adaptive Query Execution to allow Spark to dynamically optimize the query plan based on
        # runtime statistics rather than static estimates. This enables the coalescePartitions optimization below.
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        
        # Automatically merge small partitions after shuffle operations to maintain efficient partition sizes.
        # This ensures that even if the query creates uneven partitions, they'll be consolidated before writing,
        # preventing the creation of many small output files.
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        log_info("AQE configuration complete")

    # run SQL to transform data
    log_info("Creating DataFrame with SQL transformation (execution deferred)")
    export_data: DataFrame = spark.sql(sql)

    # Validate max_records_per_file for any partitioning strategy
    if args.partitioning_strategy != 'none' and args.max_records_per_file <= 0:
        raise ValueError(f"max_records_per_file must be greater than 0 when using partitioning strategy '{args.partitioning_strategy}', got {args.max_records_per_file}")
    
    # Apply partitioning strategy
    # export data with conditional partitioning and format selection
    if args.partitioning_strategy == 'repartition':
        log_info(f"Applying repartition strategy with max_records_per_file={args.max_records_per_file}")
        num_partitions = calculate_num_partitions(export_data, args.max_records_per_file, args.target_partitions)
        
        # Add repartition to execution plan (will be applied during write with full shuffle)
        log_info(f"Planning repartition to {num_partitions} partitions (will execute during write)")
        export_data = export_data.repartition(num_partitions)
    elif args.partitioning_strategy == 'coalesce':
        log_info(f"Applying coalesce strategy with max_records_per_file={args.max_records_per_file}")
        
        # TODO - enable this for all partition strategy in future. Not doing that now just be safe.
        spark.conf.set("spark.sql.files.maxRecordsPerFile", args.max_records_per_file)
        
        # Calculate desired number of partitions
        num_partitions = calculate_num_partitions(export_data, args.max_records_per_file, args.target_partitions)
        
        current_partitions = export_data.rdd.getNumPartitions()
        
        # Add coalesce to execution plan (will be applied during write)
        log_info(f"Planning coalesce: {current_partitions} → {num_partitions} partitions (will execute during write)")
        export_data = export_data.coalesce(num_partitions)
    else:  # default to 'none'
        log_info("No partitioning strategy specified - writing with existing partition structure")

    # Write in requested format
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
    
    total_time = time.time() - start_time
    log_info(f"Write complete in {write_time:.2f} seconds")
    log_info(f"Total job time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    log_info("Databricks unload job completed successfully")
