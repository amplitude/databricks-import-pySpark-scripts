import os
import sys
import types
import unittest

# unload_databricks_data_to_s3 imports pyspark at module load time, but the
# table-name substitution logic under test is pure string manipulation with no
# Spark dependency. Stub the pyspark modules so the module can be imported and
# the helper exercised without a local Spark install.
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

# Every standalone unload script carries its own copy of the substitution helper
# (each is uploaded to and executed on Databricks independently), so the fix must
# hold in all of them.
_MODULES_UNDER_TEST = (
    "unload_databricks_data_to_s3",
    "unload_databricks_data_to_s3_partition",
    "TEST_unload_databricks_data_to_s3",
)


def _helpers():
    for module_name in _MODULES_UNDER_TEST:
        module = importlib.import_module(module_name)
        yield module_name, module.replace_table_name_in_sql


class TestReplaceTableNameInSql(unittest.TestCase):

    def test_replaces_a_standalone_table_reference(self):
        for module_name, replace_table_name_in_sql in _helpers():
            with self.subTest(module=module_name):
                sql = "FROM cat.sch.dim_product AS dp"
                result = replace_table_name_in_sql(
                    sql, "cat.sch.dim_product", "`cat.sch.dim_product.1700000000`"
                )
                self.assertEqual("FROM `cat.sch.dim_product.1700000000` AS dp", result)

    def test_does_not_corrupt_table_name_that_contains_the_target_as_a_prefix(self):
        # 'cat.sch.dim_product' is a substring of 'cat.sch.dim_product_version_map'.
        # Replacing the shorter name must leave the longer reference untouched,
        # otherwise the generated SQL becomes
        #   `cat.sch.dim_product.1700000000`_version_map AS dpvm
        # which Databricks rejects with PARSE_SYNTAX_ERROR at or near 'AS'.
        for module_name, replace_table_name_in_sql in _helpers():
            with self.subTest(module=module_name):
                sql = (
                    "FROM cat.sch.dim_product AS dp\n"
                    "LEFT JOIN cat.sch.dim_product_version_map AS dpvm ON dp.id = dpvm.id"
                )
                result = replace_table_name_in_sql(
                    sql, "cat.sch.dim_product", "`cat.sch.dim_product.1700000000`"
                )
                self.assertEqual(
                    "FROM `cat.sch.dim_product.1700000000` AS dp\n"
                    "LEFT JOIN cat.sch.dim_product_version_map AS dpvm ON dp.id = dpvm.id",
                    result,
                )

    def test_replaces_every_standalone_occurrence(self):
        for module_name, replace_table_name_in_sql in _helpers():
            with self.subTest(module=module_name):
                sql = "SELECT * FROM cat.sch.t WHERE EXISTS (SELECT 1 FROM cat.sch.t)"
                result = replace_table_name_in_sql(sql, "cat.sch.t", "`cat.sch.t.42`")
                self.assertEqual(
                    "SELECT * FROM `cat.sch.t.42` WHERE EXISTS (SELECT 1 FROM `cat.sch.t.42`)",
                    result,
                )

    def test_rewrites_table_name_used_as_a_qualified_column_prefix(self):
        # A fully-qualified table name can also appear as a column qualifier
        # (e.g. `cat.sch.t.col`). That prefix refers to the same table and must
        # be rewritten to the temp view too, otherwise the SELECT list points at
        # the original table while the FROM points at the view. The '.' before
        # `col` must NOT block the match (it is a separator, not part of the
        # table identifier).
        for module_name, replace_table_name_in_sql in _helpers():
            with self.subTest(module=module_name):
                sql = "SELECT cat.sch.t.col FROM cat.sch.t"
                result = replace_table_name_in_sql(sql, "cat.sch.t", "`cat.sch.t.42`")
                self.assertEqual(
                    "SELECT `cat.sch.t.42`.col FROM `cat.sch.t.42`",
                    result,
                )


if __name__ == "__main__":
    unittest.main()
