"""Run with PySpark installed; writes temporary local JSON/Parquet files only."""
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


class TestSparkAdaptiveExport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("adaptive-export-regression")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.adaptive.enabled", "true")
                     .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                     .config("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true")
                     .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
                     .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1024")
                     .config("spark.sql.files.maxRecordsPerFile", "999")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def export_and_validate(self, module, format, rows, initial_partitions, advisory_bytes, limit, configure=False):
        self.spark.conf.set("spark.sql.shuffle.partitions", initial_partitions)
        self.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", advisory_bytes)
        sql = (f"SELECT id, concat(sha2(cast(id AS STRING), 256), "
               f"sha2(cast(id + {rows} AS STRING), 256)) AS payload, "
               "CASE WHEN id % 11 = 0 THEN NULL ELSE 'café 東京' END AS optional_value "
               f"FROM range(0, {rows}, 1, 16)")
        expected = self.spark.sql(sql)
        destination = Path(self.directory.name) / module.__name__ / format
        args = types.SimpleNamespace(partitioning_strategy="rebalance", target_partitions=None,
                                     max_records_per_file=limit, format=format, s3_path=str(destination),
                                     configure_rebalance_aqe=configure)
        settings = {key: self.spark.conf.get(key, "true") for key in (
            "spark.sql.shuffle.partitions", "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            "spark.sql.files.maxRecordsPerFile", *MODULES[0].REBALANCE_SETTINGS)}
        with patch.object(module, "spark", self.spark, create=True), \
                patch.object(module, "build_views_for_tables", return_value=sql), \
                patch.object(DataFrame, "count", side_effect=AssertionError("export must not count")):
            module.write_export_data_for_versions(sql, {}, "EVENT", True, {}, args, False)
        tracker = self.spark.sparkContext.statusTracker()
        job = tracker.getJobInfo(max(tracker.getJobIdsForGroup(None)))
        writer_tasks = tracker.getStageInfo(max(job.stageIds)).numTasks
        self.assertEqual(settings, {key: self.spark.conf.get(key, "true") for key in settings})
        actual = self.spark.read.schema(expected.schema).format(format).load(str(destination))
        self.assertEqual([], actual.exceptAll(expected).limit(1).collect())
        self.assertEqual([], expected.exceptAll(actual).limit(1).collect())
        files = actual.groupBy(F.input_file_name()).count().collect()
        self.assertTrue(all(0 < row[1] <= limit for row in files))
        self.assertEqual(rows, sum(row[1] for row in files))
        print(f"{module.__name__} {format}: rows={rows}, initial_partitions={initial_partitions}, "
              f"writer_tasks={writer_tasks}, files={len(files)}")
        return writer_tasks, len(files)

    def test_small_export_coalesces_writer_tasks(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    tasks, files = self.export_and_validate(module, format, 10000, 64, "64MB", 100000)
                    self.assertLess(tasks, 64)
                    self.assertEqual(1, files)

    def test_large_export_splits_oversized_shuffle_partitions(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    tasks, _ = self.export_and_validate(module, format, 120000, 2, "128KB", 10000)
                    self.assertGreater(tasks, 2)

    def test_adaptive_writer_rotates_files_after_coalescing(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    tasks, files = self.export_and_validate(module, format, 37, 64, "64MB", 5)
                    self.assertEqual(1, tasks)
                    self.assertGreater(files, tasks)

    def test_session_setup_reads_registered_defaults_without_session_overrides(self):
        settings = {key: self.spark.conf.get(key) for key in MODULES[0].REBALANCE_SETTINGS
                    if key != "spark.databricks.optimizer.adaptive.enabled"}
        try:
            for key in settings:
                self.spark.conf.unset(key)
            for module in MODULES:
                args = types.SimpleNamespace(partitioning_strategy="rebalance", target_partitions=None,
                                             configure_rebalance_aqe=True)
                with patch.object(module, "spark", self.spark, create=True):
                    with module.configured_rebalance_session(args) as strategy:
                        self.assertEqual("rebalance", strategy)
                        for key in settings:
                            self.assertEqual("true", self.spark.conf.get(key))
        finally:
            for key, value in settings.items():
                self.spark.conf.set(key, value)

    def test_session_setup_enables_aqe_and_restores_disabled_settings(self):
        settings = {key: self.spark.conf.get(key, None) for key in MODULES[0].REBALANCE_SETTINGS}
        try:
            for key, value in settings.items():
                if value is not None:
                    self.spark.conf.set(key, "false")
            for module in MODULES:
                for format in ("json", "parquet"):
                    with self.subTest(module=module.__name__, format=format):
                        tasks, files = self.export_and_validate(
                            module, format, 37, 64, "64MB", 5, configure=True
                        )
                        self.assertEqual(1, tasks)
                        self.assertGreater(files, tasks)
        finally:
            for key, value in settings.items():
                if value is not None:
                    self.spark.conf.set(key, value)

    def test_empty_export_does_not_count(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    _, files = self.export_and_validate(module, format, 0, 64, "64MB", 100000)
                    self.assertEqual(0, files)


if __name__ == "__main__":
    unittest.main()
