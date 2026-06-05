import os
import sys
import types
import unittest


# The unload scripts import pyspark at module load. The wiring under test (that
# each script re-exports the shared substitution helper) needs no real Spark, so
# stub pyspark before importing them.
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

import importlib

import databricks_sql_utils

_ENTRYPOINTS = (
    "unload_databricks_data_to_s3",
    "unload_databricks_data_to_s3_partition",
    "TEST_unload_databricks_data_to_s3",
)


class TestSharedModuleWiring(unittest.TestCase):

    def test_each_entrypoint_reexports_shared_substitution(self):
        for name in _ENTRYPOINTS:
            with self.subTest(module=name):
                module = importlib.import_module(name)
                self.assertIs(
                    module.replace_table_name_in_sql,
                    databricks_sql_utils.replace_table_name_in_sql,
                )


if __name__ == "__main__":
    unittest.main()
