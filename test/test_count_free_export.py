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
    def write_export(self, module, strategy, target, format="json", limit=10, settings=None, version="3.5.3",
                     locked=None, setter_failure=None, ignore_setting=None, write_error=None, restore_failure=None, plan_error=None):
        dataframe = MagicMock()
        dataframe.count.return_value = 25
        dataframe.repartition.return_value = dataframe
        dataframe.coalesce.return_value = dataframe
        dataframe.hint.return_value = dataframe
        writer = dataframe.write.mode.return_value
        writer.option.return_value = writer
        spark = MagicMock()
        spark.version = version
        state = dict(settings or {})
        spark.test_settings = state
        spark.conf.get.side_effect = lambda key, default=None: state.get(key, default)
        # Registered Spark settings have defaults even when no session override exists.
        for key in module.REBALANCE_SETTINGS:
            if key != "spark.databricks.optimizer.adaptive.enabled":
                state.setdefault(key, "true")
        spark.conf.isModifiable.side_effect = lambda key: key not in (locked or ())

        def set_config(key, value):
            if key == setter_failure and value == "true":
                raise RuntimeError("setting rejected")
            if key == restore_failure and value == "false":
                raise RuntimeError("restore rejected")
            if key != ignore_setting:
                state[key] = str(value)

        spark.conf.set.side_effect = set_config
        if write_error is not None:
            getattr(writer, format).side_effect = write_error
        self.last_spark = spark
        self.last_dataframe = dataframe
        args = types.SimpleNamespace(
            partitioning_strategy=strategy,
            target_partitions=target,
            max_records_per_file=limit,
            format=format,
            s3_path="s3a://test/export",
        )
        with patch.object(module, "spark", spark, create=True), \
                patch.object(module, "build_views_for_tables", return_value="SELECT * FROM source"), \
                patch.object(module, "build_export_dataframe", return_value=dataframe, side_effect=plan_error), \
                patch.object(module, "drop_void_fields", return_value=dataframe) as drop_void:
            module.write_export_data_for_versions(
                "SELECT * FROM source", {}, "EVENT", True, {}, args, False
            )
        return dataframe, writer, spark, drop_void

    def test_rebalance_skips_count_and_caps_both_writers_without_changing_session(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    dataframe, writer, spark, _ = self.write_export(module, "rebalance", None, format)
                    dataframe.count.assert_not_called()
                    dataframe.repartition.assert_not_called()
                    dataframe.coalesce.assert_not_called()
                    dataframe.hint.assert_called_once_with("REBALANCE")
                    writer.option.assert_any_call("maxRecordsPerFile", 10)
                    spark.conf.set.assert_not_called()
                    getattr(writer, format).assert_called_once_with("s3a://test/export")

    def test_rebalance_enables_and_restores_disabled_settings(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                for setting in module.REBALANCE_SETTINGS:
                    with self.subTest(module=module.__name__, format=format, setting=setting):
                        dataframe, writer, spark, _ = self.write_export(
                            module, "rebalance", None, format, settings={setting: "false"}
                        )
                        dataframe.count.assert_not_called()
                        dataframe.hint.assert_called_once_with("REBALANCE")
                        writer.option.assert_any_call("maxRecordsPerFile", 10)
                        self.assertEqual("false", spark.test_settings[setting])
                        self.assertEqual([
                            unittest.mock.call(setting, "true"), unittest.mock.call(setting, "false")
                        ], spark.conf.set.call_args_list)

    def test_rebalance_leaves_enabled_settings_and_absent_databricks_key_alone(self):
        for module in MODULES:
            dataframe, _, spark, _ = self.write_export(module, "rebalance", None)
            dataframe.count.assert_not_called()
            spark.conf.set.assert_not_called()
            self.assertNotIn("spark.databricks.optimizer.adaptive.enabled", spark.test_settings)

    def test_rebalance_falls_back_on_unsupported_version(self):
        for module in MODULES:
            for version in ("3.1.3", "unknown"):
                for format in ("json", "parquet"):
                    with self.subTest(module=module.__name__, version=version, format=format):
                        dataframe, writer, spark, _ = self.write_export(
                            module, "rebalance", None, format, version=version
                        )
                        dataframe.count.assert_called_once_with()
                        dataframe.repartition.assert_called_once_with(3)
                        dataframe.hint.assert_not_called()
                        writer.option.assert_any_call("maxRecordsPerFile", 10)
                        spark.conf.set.assert_not_called()

    def test_rebalance_restores_partial_setup_and_falls_back(self):
        for module in MODULES:
            settings = {key: "false" for key in module.REBALANCE_SETTINGS}
            rejected = module.REBALANCE_SETTINGS[1]
            for failure in ({"locked": [rejected]}, {"setter_failure": rejected},
                            {"ignore_setting": rejected}):
                for format in ("json", "parquet"):
                    with self.subTest(module=module.__name__, format=format, failure=failure):
                        dataframe, writer, spark, _ = self.write_export(
                            module, "rebalance", None, format, settings=settings, **failure
                        )
                        dataframe.count.assert_called_once_with()
                        dataframe.repartition.assert_called_once_with(3)
                        dataframe.hint.assert_not_called()
                        writer.option.assert_any_call("maxRecordsPerFile", 10)
                        self.assertEqual(settings, spark.test_settings)

    def test_rebalance_restores_session_after_write_failure_without_fallback(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    settings = {key: "false" for key in module.REBALANCE_SETTINGS}
                    error = RuntimeError("export write failed")
                    with self.assertRaises(RuntimeError) as raised:
                        self.write_export(module, "rebalance", None, format, settings=settings,
                                          write_error=error)
                    self.assertIs(error, raised.exception)
                    self.assertEqual(settings, self.last_spark.test_settings)
                    self.last_dataframe.count.assert_not_called()
                    self.last_dataframe.repartition.assert_not_called()

    def test_rebalance_restores_session_after_planning_failure(self):
        for module in MODULES:
            with self.subTest(module=module.__name__):
                settings = {key: "false" for key in module.REBALANCE_SETTINGS}
                error = RuntimeError("query planning failed")
                with self.assertRaises(RuntimeError) as raised:
                    self.write_export(module, "rebalance", None,
                                      settings=settings, plan_error=error)
                self.assertIs(error, raised.exception)
                self.assertEqual(settings, self.last_spark.test_settings)
                self.last_dataframe.count.assert_not_called()
                self.last_dataframe.write.mode.assert_not_called()

    def test_restore_failure_attempts_other_settings_and_fails_the_export(self):
        for module in MODULES:
            settings = {key: "false" for key in module.REBALANCE_SETTINGS}
            rejected = module.REBALANCE_SETTINGS[-1]
            with self.assertRaisesRegex(RuntimeError, "Failed to restore adaptive export"):
                self.write_export(module, "rebalance", None, settings=settings,
                                  restore_failure=rejected)
            for setting in settings:
                self.assertEqual("true" if setting == rejected else "false",
                                 self.last_spark.test_settings[setting])

    def test_restore_failure_preserves_the_original_write_error(self):
        for module in MODULES:
            settings = {key: "false" for key in module.REBALANCE_SETTINGS}
            error = RuntimeError("original write failure")
            with self.assertRaises(RuntimeError) as raised:
                self.write_export(module, "rebalance", None, settings=settings,
                                  restore_failure=module.REBALANCE_SETTINGS[-1], write_error=error)
            self.assertIs(error, raised.exception)
            self.last_dataframe.count.assert_not_called()

    def test_successful_retry_does_not_suppress_restore_failure(self):
        for module in MODULES:
            for format in ("json", "parquet"):
                with self.subTest(module=module.__name__, format=format):
                    settings = {key: "false" for key in module.REBALANCE_SETTINGS}
                    error = RuntimeError("DELTA_CHANGE_DATA_FILE_NOT_FOUND")
                    try:
                        self.write_export(module, "rebalance", None, format,
                                          settings=settings, write_error=error)
                    except RuntimeError as initial_error:
                        self.assertIs(error, initial_error)
                        with self.assertRaisesRegex(RuntimeError, "Failed to restore adaptive export"):
                            self.write_export(module, "rebalance", None, format,
                                              settings=settings, restore_failure=module.REBALANCE_SETTINGS[-1])
                        writer = self.last_dataframe.write.mode.return_value
                        getattr(writer, format).assert_called_once_with("s3a://test/export")
                    else:
                        self.fail("Expected the initial CDF failure")

    def test_rebalance_rejects_a_fixed_target_even_on_old_spark(self):
        for module in MODULES:
            with self.assertRaisesRegex(ValueError, "target_partitions must be omitted"):
                self.write_export(module, "rebalance", 8, version="3.1.3")

    def test_other_strategies_do_not_configure_aqe(self):
        for module in MODULES:
            for strategy in ("repartition", "coalesce", "none"):
                dataframe, _, spark, _ = self.write_export(module, strategy, 2)
                dataframe.hint.assert_not_called()
                self.assertFalse(any(call.args[0] in module.REBALANCE_SETTINGS
                                     for call in spark.conf.set.call_args_list))

    def test_rebalance_rejects_a_fixed_target(self):
        for module in MODULES:
            with self.subTest(module=module.__name__):
                with self.assertRaisesRegex(ValueError, "target_partitions must be omitted"):
                    self.write_export(module, "rebalance", 8)

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
