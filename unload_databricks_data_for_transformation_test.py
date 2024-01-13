import argparse

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from utils import build_temp_view_name, parse_tables_arg

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='unload data from databricks using SparkPython')
    # replace 'required=True' with 'nargs='?', default=None' to make it optional.
    parser.add_argument("--tables", required=True,
                        help="""tables where data imported from.
        Format syntax is 'table1,...,tableN'. Example: catalog1.schema1.table1,catalog2.schema2.table2""")
    parser.add_argument("--sql", required=True, help="transformation sql")
    parser.add_argument("--secret_scope", required=True, help="databricks secret scope name")
    parser.add_argument("--secret_key_name_for_aws_access_key", required=True,
                        help="databricks secret key name of aws_access_key")
    parser.add_argument("--secret_key_name_for_aws_secret_key", required=True,
                        help="databricks secret key name of aws_secret_key")
    parser.add_argument("--secret_key_name_for_aws_session_token", required=True,
                        help="databricks secret key name of aws_session_token")
    parser.add_argument("--s3_region", required=True, help="s3 region")
    parser.add_argument("--s3_path", required=True, help="s3 path where data will be written into")

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
    spark.conf.set("fs.s3a.endpoint.region", args.s3_region)

    sql: str = args.sql

    # Build temp views
    tables: list[str] = parse_tables_arg(args.tables)
    for table in tables:
        # Use `take` but not `limit` operation since `take` is an action that performs data collection while
        # `limit` is a transformation which will take SUPER long time when table is big.
        data = spark.createDataFrame(spark.sql("select * from {table}".format(table=table)).take(10))

        view_name: str = build_temp_view_name(table)
        data.createOrReplaceTempView(view_name)
        # replace table name in sql to get prepared for sql transformation
        sql = sql.replace(table, view_name)

    # run SQL to transform data
    export_data: DataFrame = spark.sql(sql)
    # export data
    export_data.coalesce(1).write.mode("overwrite").json(args.s3_path)
    # stop spark session
    spark.stop()