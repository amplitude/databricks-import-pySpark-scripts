import argparse
import collections
import math
import re
import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

MAX_RECORDS_PER_OUTPUT_FILE: int = 1_000_000

INITIAL_SYNC_SQL_TEMPLATE = """
  SELECT
    STRUCT(STRUCT(*) AS current_version) AS data,
    'insert' as mutation_type,
    '{mutation_row_type}' AS data_type,
    UUID() as mutation_insert_id,
    unix_timestamp() * 1000 as mutation_time_ms
  FROM ({selectSql})
"""

CONTINUOUS_SYNC_SQL_TEMPLATE = """
  WITH CdfData AS (
    {cdfSelectSql}
  ),
  FormattedChanges AS (
    SELECT STRUCT(* except(_change_type, _commit_version, _commit_timestamp)) AS data, _change_type, _commit_version, _commit_timestamp FROM CdfData
  ),
  Combined AS (
    SELECT
      STRUCT(
        CASE WHEN c1._change_type = 'update_postimage' THEN c1.data ELSE c2.data END AS current_version,
        CASE WHEN c1._change_type = 'update_preimage' THEN c1.data ELSE c2.data END AS previous_version
      ) AS data,
      'update' as _change_type,
      c1._commit_timestamp,
      c1._commit_version
    FROM FormattedChanges c1
    LEFT JOIN FormattedChanges c2 ON c1._commit_version = c2._commit_version AND c1._commit_timestamp = c2._commit_timestamp AND c1._change_type <> c2._change_type AND c1.data.{id_column} = c2.data.{id_column}
    WHERE c1._change_type IN ('update_postimage', 'update_preimage') AND c2._change_type IN ('update_postimage', 'update_preimage')

    UNION ALL

    SELECT
      STRUCT(
        c1.data AS current_version,
        null AS previous_version
      ) as data,
      c1._change_type,
      c1._commit_timestamp,
      c1._commit_version
    FROM FormattedChanges c1
    WHERE c1._change_type NOT IN ('update_postimage', 'update_preimage')
  ),
  DistinctData AS (
    SELECT DISTINCT
      data as data,
      _change_type,
      _commit_timestamp,
      _commit_version
    FROM Combined
  )
  SELECT
    data,
    _change_type as mutation_type,
    '{mutation_row_type}' AS data_type,
    UUID() as mutation_insert_id,
    unix_timestamp(_commit_timestamp) * 1000 as mutation_time_ms
  FROM DistinctData
"""


def normalize_sql_query(sql: str) -> str:
    """
    Normalize sql query to remove new lines, leading/trailing spaces, and replace multiple spaces with single space.
    :param sql: sql query
    :return: normalized sql query
    """
    return ' '.join(sql.split())


def determine_first_table_name_in_sql(normalized_sql: str, table_names: set[str]) -> str:
    """
    Determine the first table name in the SQL query.
    :param normalized_sql: SELECT SQL query without extra spaces and new lines
    :param table_names: table names
    :return: first table name in the SQL query
    """
    min_table_index: int = len(normalized_sql)
    first_table_name: str = ""

    for table_name in table_names:
        table_index: int = normalized_sql.find(table_name)
        if table_index != -1 and table_index < min_table_index:
            min_table_index = table_index
            first_table_name = table_name

    return first_table_name


def copy_and_inject_cdf_metadata_column_names(normalized_sql: str) -> str:
    """
    Creates a copy of the input SQL query with the CDF metadata column names injected into the SELECT clause.
    :param normalized_sql: SELECT SQL query without extra spaces and new lines
    :return: copy of the SQL query with CDF metadata column names injected into the SELECT clause
    """
    # Inject CDF metadata column names into the first SELECT clause
    # Uses regex to replace the first occurrence of "select". The re.IGNORECASE flag is used to make the pattern
    # case-insensitive
    pattern = re.compile("select", re.IGNORECASE)
    return pattern.sub("SELECT _commit_version, _commit_timestamp, _change_type,", normalized_sql, count=1)


def determine_id_column_name_for_mutation_row_type(mutation_row_type: str) -> str:
    """
    Determine the ID column name for the mutation row type. The ID column name should be required to be included in the
    select clause of the SQL query and validated during source connection creation.
    :param mutation_row_type: type of mutation row (e.g. http_api_event_json, http_api_user_json, http_api_group_json)
    :return: ID column name for the mutation row type
    """
    if mutation_row_type == "http_api_event_json":
        return "insert_id"
    elif mutation_row_type == "http_api_user_json":
        # TODO: Implement this when http_api_user_json requirements are finalized: AMP-96981
        raise NotImplementedError("http_api_user_json is not supported yet.")
    elif mutation_row_type == "http_api_group_json":
        # TODO: Implement this when http_api_group_json requirements are finalized: AMP-96981
        raise NotImplementedError("http_api_group_json is not supported yet.")
    else:
        raise ValueError("Invalid mutation row type: {mutation_row_type}".format(mutation_row_type=mutation_row_type))


def generate_sql_to_unload_mutation_data(normalized_sql: str, mutation_row_type: str, is_initial_sync: bool) -> str:
    """
    Generates SQL query to unload mutation data.
    :param normalized_sql: SELECT SQL query without extra spaces and new lines
    :param mutation_row_type: type of mutation row (e.g. http_api_event_json, http_api_user_json, http_api_group_json)
    :param is_initial_sync: boolean flag indicating if this is the initial unload for the source connection
    :return: SQL query to unload mutation data
    """

    if is_initial_sync:
        return INITIAL_SYNC_SQL_TEMPLATE.format(
            selectSql=normalized_sql,
            mutation_row_type=mutation_row_type)
    else:
        return CONTINUOUS_SYNC_SQL_TEMPLATE.format(
            cdfSelectSql=copy_and_inject_cdf_metadata_column_names(normalized_sql),
            mutation_row_type=mutation_row_type,
            id_column=determine_id_column_name_for_mutation_row_type(mutation_row_type))


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
    return "select * from {table} version as of {version}".format(table=table_full_name, version=ending_version)


def build_sql_to_query_table_between_versions(table_full_name: str, starting_version: int, ending_version: int) -> str:
    return "select * from table_changes(\"{table}\", {starting_version}, {ending_version})".format(
        table=table_full_name, starting_version=starting_version, ending_version=ending_version)


def fetch_data(table_full_name: str, starting_version: int, ending_version: int) -> DataFrame:
    if starting_version == 0:
        return spark.sql(build_sql_to_query_table_of_version(table_full_name, ending_version))
    else:
        # TODO: filter data
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
                        choices=['EVENT', 'USER_PROPERTY', 'GROUP_PROPERTY'],
                        help="""type of data to be imported.
                            Valid values are ['EVENT', 'USER_PROPERTY', 'GROUP_PROPERTY'].""")
    parser.add_argument("--sql", required=True, help="transformation sql")
    parser.add_argument("--secret_scope", required=True, help="databricks secret scope name")
    parser.add_argument("--secret_key_name_for_aws_access_key", required=True,
                        help="databricks secret key name of aws_access_key")
    parser.add_argument("--secret_key_name_for_aws_secret_key", required=True,
                        help="databricks secret key name of aws_secret_key")
    parser.add_argument("--secret_key_name_for_aws_session_token", required=True,
                        help="databricks secret key name of aws_session_token")
    parser.add_argument("--s3_region", nargs='?', default=None, help="s3 region")
    parser.add_argument("--s3_endpoint", nargs='?', default=None, help="s3 endpoint")
    parser.add_argument("--s3_path", required=True, help="s3 path where data will be written into")
    parser.add_argument("--max_records_per_file", help="max records per output file", nargs='?', type=int,
                        default=MAX_RECORDS_PER_OUTPUT_FILE, const=MAX_RECORDS_PER_OUTPUT_FILE)
    parser.add_argument("--unload_mutation_data",
                        help="if provided, will include change data for all mutation types (insert, update, delete) "
                             "in the resulting dataset along with DataBricks native metadata (i.e. _change_type, "
                             "_commit_version, _commit_timestamp)",
                        action='store_true', default=False)
    parser.add_argument("--mutation_row_type",
                        help="""type of mutation row to be included in the resulting dataset. Required if 
                        --unload_mutation_data is provided. Valid values are ['http_api_event_json',
                        'http_api_user_json', 'http_api_group_json']""",
                        required=False,
                        choices=['http_api_event_json', 'http_api_user_json', 'http_api_group_json'])

    args, unknown = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    # setup s3 credentials for data export
    aws_access_key = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_aws_access_key)
    aws_secret_key = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_aws_secret_key)
    aws_session_token = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key_name_for_aws_session_token)
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    spark.conf.set("fs.s3a.access.key", aws_access_key)
    spark.conf.set("fs.s3a.secret.key", aws_secret_key)
    spark.conf.set("fs.s3a.session.token", aws_session_token)
    if args.s3_region is not None:
        spark.conf.set("fs.s3a.endpoint.region", args.s3_region)
    if args.s3_endpoint is not None:
        spark.conf.set("fs.s3a.endpoint", args.s3_endpoint)

    print(f'SQL query before normalization:\n{args.sql}')
    sql: str = normalize_sql_query(args.sql)
    print(f'SQL query after normalization:\n{sql}')

    # Build temp views
    table_to_import_version_range_map: dict[str, list[int]] = parse_table_versions_map_arg(args.table_versions_map)
    first_table_name_in_sql: str = determine_first_table_name_in_sql(sql, set(table_to_import_version_range_map.keys()))
    is_initial_sync: bool = True
    for table, import_version_range in table_to_import_version_range_map.items():
        if table == first_table_name_in_sql:
            is_initial_sync = import_version_range[0] == 0

        data: DataFrame = fetch_data(table, import_version_range[0], import_version_range[1])
        # filter data if not unloading mutation data or table is not the first table in sql
        if not args.unload_mutation_data or table != first_table_name_in_sql:
            data = filter_data(data, args.data_type)
        view_name: str = build_temp_view_name(table)
        data.createOrReplaceTempView(view_name)
        # replace table name in sql to get prepared for sql transformation
        sql = sql.replace(table, view_name)

    if args.unload_mutation_data:
        sql = generate_sql_to_unload_mutation_data(sql, args.mutation_row_type, is_initial_sync)
        print(f'SQL for unloading mutation data:\n{sql}')

    # run SQL to transform data
    export_data: DataFrame = spark.sql(sql)

    if not export_data.isEmpty():
        event_count = export_data.count()

        partition_count = get_partition_count(event_count, args.max_records_per_file)

        # export meta data
        export_meta_data(event_count, partition_count)

        export_data = export_data.repartition(partition_count)
        # export data
        export_data.write.mode("overwrite").json(args.s3_path)
        print("Unloaded {event_count} events.".format(event_count=event_count))
    else:
        print("No events were exported.")
    # stop spark session
    spark.stop()
