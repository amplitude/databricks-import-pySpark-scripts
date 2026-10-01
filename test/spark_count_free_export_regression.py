"""Run separately with PySpark installed; does not require Databricks or S3 credentials.

    python -m unittest discover -s test -p 'spark_count_free_export_regression.py'
"""
import importlib
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
MODULES = [importlib.import_module(name) for name in (
    "unload_databricks_data_to_s3", "TEST_unload_databricks_data_to_s3"
)]


class TestSparkCountFreeExport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("count-free-export-regression")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.session_limit = cls.spark.conf.get("spark.sql.files.maxRecordsPerFile")
        cls.spark.conf.set("spark.sql.files.maxRecordsPerFile", 999)

    @classmethod
    def tearDownClass(cls):
        cls.spark.conf.set("spark.sql.files.maxRecordsPerFile", cls.session_limit)
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def export(self, module, sql, format, destination, strategy="repartition"):
        args = types.SimpleNamespace(
            partitioning_strategy=strategy,
            target_partitions=2,
            max_records_per_file=5,
            format=format,
            s3_path=str(destination),
        )
        with patch.object(module, "spark", self.spark, create=True), \
                patch.object(module, "build_views_for_tables", return_value=sql), \
                patch.object(DataFrame, "count", side_effect=AssertionError("export must not count")):
            module.write_export_data_for_versions(sql, {}, "EVENT", True, {}, args, False)
        self.assertEqual("999", self.spark.conf.get("spark.sql.files.maxRecordsPerFile"))

    def read_export(self, destination, format, schema):
        return self.spark.read.schema(schema).format(format).load(str(destination))

    def test_json_and_parquet_preserve_rows_and_bound_files(self):
        sql = "SELECT id, concat('payload-', id) AS payload FROM range(37) ORDER BY id DESC"
        expected = self.spark.sql(sql)
        expected_rows = sorted(expected.collect())
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    destination = Path(self.directory.name) / module.__name__ / format
                    self.export(module, sql, format, destination)
                    actual = self.read_export(destination, format, expected.schema)
                    self.assertEqual(expected_rows, sorted(actual.collect()))
                    counts = actual.groupBy(F.input_file_name()).count().collect()
                    self.assertGreater(len(counts), 2)
                    self.assertTrue(all(0 < row[1] <= 5 for row in counts))

    def test_empty_export_succeeds_without_count(self):
        sql = "SELECT id, concat('payload-', id) AS payload FROM range(0) ORDER BY id DESC"
        schema = self.spark.sql(sql).schema
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    destination = Path(self.directory.name) / module.__name__ / format
                    self.export(module, sql, format, destination)
                    self.assertEqual([], self.read_export(destination, format, schema).collect())

    def test_writer_limit_does_not_leak_into_next_export(self):
        sql = "SELECT id FROM range(0, 37, 1, 1)"
        schema = self.spark.sql(sql).schema
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    base = Path(self.directory.name) / module.__name__ / format
                    self.export(module, sql, format, base / "bounded")
                    self.export(module, sql, format, base / "none", strategy="none")
                    actual = self.read_export(base / "none", format, schema)
                    counts = actual.groupBy(F.input_file_name()).count().collect()
                    self.assertEqual([37], [row[1] for row in counts])


if __name__ == "__main__":
    unittest.main()
