import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description='unload data from databricks using SparkPython')
    parser.add_argument("--table_versions_map", help="tables and version ranges where data imported from. "
                                                 "Shall be in format [{tableVersion},...,{tableVersion*N}]. "
                                                 "{tableVersion} will be '{catalogName}.{schemaName}.{tableName}={startingVersion}-{endingVersion}'")
    parser.add_argument("--data_type", help="type of data to be imported. Shall be 'EVENT', 'USER_PROPERTY' or 'GROUP_PROPERTY'")
    parser.add_argument("--sql", help="transformation sql")
    parser.add_argument("--aws_access_key_secret_key", help="databricks secret key name of aws_access_key")
    parser.add_argument("--aws_secret_key_secret_key", help="databricks secret key name of aws_secret_key")
    parser.add_argument("--aws_session_token_secret_key", help="databricks secret key name of aws_session_token")
    parser.add_argument("--s3_path", help="s3 path where data will be written into")

    args = parser.parse_args()
    print(parser.table_versions_map)
    print(parser.data_type)
    print(parser.sql)
    print(parser.aws_access_key_secret_key)
    print(parser.aws_secret_key_secret_key)
    print(parser.aws_session_token_secret_key)
    print(parser.s3_path)


if __name__ == '__main__':
    main();
