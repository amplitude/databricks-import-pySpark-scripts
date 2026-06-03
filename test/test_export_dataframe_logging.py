import os
import sys
import types
import unittest

# unload_databricks_data_to_s3 imports pyspark at module load time, but the
# behaviour under test (logging the transformed SQL on failure) does not need a
# real Spark session — we inject a fake one. Stub pyspark so the module imports
# without a local Spark install.
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


class _FakeSparkRaises:
    """Spark double whose sql() rejects the query, like a PARSE_SYNTAX_ERROR."""

    def __init__(self, error: Exception):
        self.error = error

    def sql(self, query):
        raise self.error


class _FakeSparkReturns:
    """Spark double whose sql() returns a sentinel DataFrame."""

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def sql(self, query):
        return self.dataframe


class TestBuildExportDataframeLogging(unittest.TestCase):

    def setUp(self):
        self._orig_spark = getattr(mod, "spark", None)
        mod.LOG_MESSAGES.clear()

    def tearDown(self):
        mod.spark = self._orig_spark

    def test_logs_transformed_sql_when_spark_rejects_it(self):
        # The transformed SQL (source tables swapped for temp views) is what
        # Spark parses and differs from the customer's original query; on a
        # parse/analysis failure it must be logged so the failure can be
        # root-caused without re-deriving the rewrite.
        transformed_sql = "SELECT * FROM `cat.sch.t.123` AS f JOIN `cat.sch.u.456` AS u"
        error = ValueError("[PARSE_SYNTAX_ERROR] Syntax error at or near 'AS'.")
        mod.spark = _FakeSparkRaises(error)

        with self.assertRaises(ValueError):
            mod.build_export_dataframe(transformed_sql)

        logged = "\n".join(mod.LOG_MESSAGES)
        self.assertIn(transformed_sql, logged)

    def test_returns_dataframe_and_does_not_log_failure_on_success(self):
        sentinel = object()
        mod.spark = _FakeSparkReturns(sentinel)

        result = mod.build_export_dataframe("SELECT 1")

        self.assertIs(result, sentinel)
        self.assertFalse(
            any("Failed to build DataFrame" in message for message in mod.LOG_MESSAGES)
        )


if __name__ == "__main__":
    unittest.main()
