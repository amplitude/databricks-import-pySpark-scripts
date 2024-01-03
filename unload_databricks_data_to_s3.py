import argparse
import collections
import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def parse_table_versions_map_arg(table_versions_map: str) -> dict[str, list[int]]:
    """
    Extract table && version range numbers from input str.
    :param table_versions_map: table versions map. Sample input 'catalog.schema.table=1-2,catalog.schema2.table2=11-12'
    which means table 'catalog.schema.table' with version range [1,2] and table 'catalog.schema2.table2'
    with version range [11,12].
    :return: table to version ranges map. Sample output: {'catalog.schema.table': [1,2]}
    """
    d = collections.defaultdict(list)
    table_and_versions_list = table_versions_map.split(",")
    for table_and_versions in table_and_versions_list:
        table = table_and_versions.split("=")[0]
        versions = table_and_versions.split("=")[1].split("-")
        d[table].append(int(versions[0]))
        d[table].append(int(versions[1]))
    return d


def build_temp_view_name(table: str) -> str:
    """
    Build temp view name for the table. Wrap table name with '`' to escape '.'. Append `epoch` to table name
    to make view name more unique.
    :param table: table name
    :return: temp view name for the table
    """
    return '`{table}.{epoch}`'.format(table=table, epoch=int(time.time()))


def build_sql_to_query_table_of_version(table_full_name: str, ending_version: int) -> str:
    return "select * from {table} version as of {version}".format(table=table_full_name, version=ending_version)


def build_sql_to_query_table_between_versions(table_full_name: str, starting_version: int, ending_version: int) -> str:
    return "select * from table_changes(\"{table}\", {starting_version}, {ending_version})".format(
        table=table_full_name, starting_version=starting_version, ending_version=ending_version)


def pull_data(table_full_name: str, starting_version: int, ending_version: int) -> DataFrame:
    if starting_version == 0:
        return spark.sql(build_sql_to_query_table_of_version(table_full_name, ending_version))
    else:
        # TODO: filter data
        return spark.sql(build_sql_to_query_table_between_versions(table_full_name, starting_version, ending_version))

    # print(args.table_versions_map)
    # print(args.data_type)
    # print(args.sql)
    # print(args.secret_key_name_for_aws_access_key)
    # print(args.secret_key_name_for_aws_secret_key)
    # print(args.secret_key_name_for_aws_session_token)
    # print(args.s3_path)


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    parser = argparse.ArgumentParser(description='unload data from databricks using SparkPython')
    parser.add_argument("table_versions_map", help="""tables and version ranges where data imported from. 
    Format syntax is '[{tableVersion},...,{tableVersion*N}]'. '{tableVersion}' will be 
    '{catalogName}.{schemaName}.{tableName}={startingVersion}-{endingVersion}'. 
    Example: catalog1.schema1.table1=0-12,catalog2.schema2.table2=10-100 """)
    parser.add_argument("data_type",
                        help="""type of data to be imported. 
                        Valid values are ['EVENT', 'USER_PROPERTY', 'GROUP_PROPERTY'].""")
    parser.add_argument("sql", help="transformation sql")
    parser.add_argument("secret_key_name_for_aws_access_key", help="databricks secret key name of aws_access_key")
    parser.add_argument("secret_key_name_for_aws_secret_key", help="databricks secret key name of aws_secret_key")
    parser.add_argument("secret_key_name_for_aws_session_token", help="databricks secret key name of aws_session_token")
    parser.add_argument("s3_path", help="s3 path where data will be written into")

    args = parser.parse_args()

    # print(args.table_versions_map)
    # print(args.data_type)
    sql: str = args.sql
    # print(args.secret_key_name_for_aws_access_key)
    # print(args.secret_key_name_for_aws_secret_key)
    # print(args.secret_key_name_for_aws_session_token)
    # print(args.s3_path)

    table_to_import_version_range_map: dict[str, list[int]] = parse_table_versions_map_arg(args.table_versions_map)
    for table, import_version_range in table_to_import_version_range_map.items():
        df: DataFrame = pull_data(table, import_version_range[0], import_version_range[1])
        print(df.show())
        view_name: str = build_temp_view_name(table)
        df.createOrReplaceTempView(view_name)
        sql.replace(table, view_name)
        df2: DataFrame = spark.sql(sql)
        print(df2.show())
