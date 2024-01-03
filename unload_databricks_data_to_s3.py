import argparse
import collections

from pyspark.sql import DataFrame


def parse_table_versions_map_arg(table_versions_map: str) -> dict[str, list[int]]:
    d = collections.defaultdict(list)
    table_and_versions_list = table_versions_map.split(",")
    for table_and_versions in table_and_versions_list:
        table = table_and_versions.split("=")[0]
        versions = table_and_versions.split("=")[1].split("-")
        d[table].append(int(versions[0]))
        d[table].append(int(versions[1]))
    return d


def pull_data(table_full_name: str, starting_version: int, ending_version: int) -> DataFrame:
    if starting_version == 0:
        return spark.sql("select * from {table} VERSION AS OF {version}"
                         .format(table=table_full_name, version=ending_version))
    else:
        # TODO: filter data
        return spark.sql("select * from table_changes({table}, {starting_version}, {ending_version})"
                         .format(table=table_full_name,starting_version=starting_version,ending_version=ending_version))


def import_data():
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
    table_to_import_version_range_map = parse_table_versions_map_arg(args.table_versions_map)
    for table, import_version_range in table_to_import_version_range_map.items():
        df: DataFrame = pull_data(table, import_version_range[0], import_version_range[1])
        print(df.show())
    # print(args.table_versions_map)
    # print(args.data_type)
    # print(args.sql)
    # print(args.secret_key_name_for_aws_access_key)
    # print(args.secret_key_name_for_aws_secret_key)
    # print(args.secret_key_name_for_aws_session_token)
    # print(args.s3_path)


if __name__ == '__main__':
    import_data()
