import os
import sys
import types
import unittest


def _install_pyspark_stub() -> None:
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
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions
    sys.modules["pyspark.sql.types"] = sqltypes


_install_pyspark_stub()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unload_databricks_data_to_s3 as mod


class TestIncompatibleSchemaSignature(unittest.TestCase):

    def test_matches_incompatible_data_schema(self):
        err = Exception(
            "[DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA] Retrieving table "
            "changes between version 31 and 53 failed because of an incompatible data schema."
        )
        self.assertEqual(
            "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
            mod.extract_incompatible_schema_error_signature(err),
        )

    def test_matches_incompatible_schema_change(self):
        err = Exception("oops [DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE] column renamed")
        self.assertEqual(
            "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            mod.extract_incompatible_schema_error_signature(err),
        )

    def test_returns_none_for_unrelated_error(self):
        self.assertIsNone(mod.extract_incompatible_schema_error_signature(Exception("network timeout")))

    def test_returns_none_for_empty(self):
        self.assertIsNone(mod.extract_incompatible_schema_error_signature(None))
