import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


def _install_pyspark_stub():
    if "pyspark" in sys.modules:
        return
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    sqltypes = types.ModuleType("pyspark.sql.types")
    sql.SparkSession = object
    sql.DataFrame = object
    sql.Column = object
    functions.col = lambda *a, **k: None
    sql.functions = functions
    for name in ("StructType", "ArrayType", "MapType", "NullType", "DataType"):
        setattr(sqltypes, name, type(name, (), {}))
    pyspark.sql = sql
    sys.modules.update({
        "pyspark": pyspark,
        "pyspark.sql": sql,
        "pyspark.sql.functions": functions,
        "pyspark.sql.types": sqltypes,
    })


_install_pyspark_stub()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
MODULES = [importlib.import_module(name) for name in (
    "unload_databricks_data_to_s3", "TEST_unload_databricks_data_to_s3"
)]


class TestCountFreeExport(unittest.TestCase):
    def write_export(self, module, strategy, target, format="json", limit=10):
        dataframe = MagicMock()
        dataframe.count.return_value = 25
        dataframe.repartition.return_value = dataframe
        dataframe.coalesce.return_value = dataframe
        writer = dataframe.write.mode.return_value
        writer.option.return_value = writer
        spark = MagicMock()
        args = types.SimpleNamespace(
            partitioning_strategy=strategy,
            target_partitions=target,
            max_records_per_file=limit,
            format=format,
            s3_path="s3a://test/export",
        )
        with patch.object(module, "spark", spark, create=True), \
                patch.object(module, "build_views_for_tables", return_value="SELECT * FROM source"), \
                patch.object(module, "build_export_dataframe", return_value=dataframe), \
                patch.object(module, "drop_void_fields", return_value=dataframe) as drop_void:
            module.write_export_data_for_versions(
                "SELECT * FROM source", {}, "EVENT", True, {}, args, False
            )
        return dataframe, writer, spark, drop_void

    def test_configured_repartition_skips_count_and_caps_both_writers(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    dataframe, writer, spark, drop_void = self.write_export(
                        module, "repartition", 2, format
                    )
                    dataframe.count.assert_not_called()
                    dataframe.repartition.assert_called_once_with(2)
                    writer.option.assert_any_call("maxRecordsPerFile", 10)
                    spark.conf.set.assert_not_called()
                    getattr(writer, format).assert_called_once_with("s3a://test/export")
                    if format == "parquet":
                        writer.option.assert_any_call("compression", "zstd")
                        writer.option.assert_any_call("compressionLevel", 3)
                        drop_void.assert_called_once_with(dataframe)
                    else:
                        drop_void.assert_not_called()

    def test_unconfigured_repartition_keeps_count_sizing_and_writer_defaults(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    dataframe, writer, spark, _ = self.write_export(
                        module, "repartition", None, format
                    )
                    dataframe.count.assert_called_once_with()
                    dataframe.repartition.assert_called_once_with(3)
                    self.assertNotIn(
                        unittest.mock.call("maxRecordsPerFile", 10), writer.option.call_args_list
                    )
                    spark.conf.set.assert_not_called()

    def test_none_keeps_existing_partitions_and_writer_defaults(self):
        for module in MODULES:
            for target in (None, 2):
                with self.subTest(module=module.__name__, target=target):
                    dataframe, writer, spark, _ = self.write_export(module, "none", target)
                    dataframe.count.assert_not_called()
                    dataframe.repartition.assert_not_called()
                    dataframe.coalesce.assert_not_called()
                    writer.option.assert_not_called()
                    spark.conf.set.assert_not_called()

    def test_coalesce_keeps_existing_session_cap(self):
        for module in MODULES:
            with self.subTest(module=module.__name__):
                dataframe, writer, spark, _ = self.write_export(module, "coalesce", 2)
                dataframe.count.assert_not_called()
                dataframe.coalesce.assert_called_once_with(2)
                spark.conf.set.assert_called_once_with("spark.sql.files.maxRecordsPerFile", 10)
                writer.option.assert_not_called()

    def test_configured_repartition_rejects_nonpositive_file_limit(self):
        for module in MODULES:
            for limit in (0, -1):
                with self.subTest(module=module.__name__, limit=limit):
                    with self.assertRaisesRegex(ValueError, "max_records_per_file must be greater than 0"):
                        self.write_export(module, "repartition", 2, limit=limit)

    def test_target_partition_sizing_retains_minimum_one(self):
        for module in MODULES:
            with self.subTest(module=module.__name__):
                dataframe, writer, _, _ = self.write_export(module, "repartition", 0)
                dataframe.count.assert_not_called()
                dataframe.repartition.assert_called_once_with(1)
                writer.option.assert_called_once_with("maxRecordsPerFile", 10)


if __name__ == "__main__":
    unittest.main()
