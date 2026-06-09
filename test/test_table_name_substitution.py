import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from databricks_sql_utils import replace_table_name_in_sql


class TestReplaceTableNameInSql(unittest.TestCase):

    def test_replaces_a_standalone_table_reference(self):
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
        sql = "SELECT * FROM cat.sch.t WHERE EXISTS (SELECT 1 FROM cat.sch.t)"
        result = replace_table_name_in_sql(sql, "cat.sch.t", "`cat.sch.t.42`")
        self.assertEqual(
            "SELECT * FROM `cat.sch.t.42` WHERE EXISTS (SELECT 1 FROM `cat.sch.t.42`)",
            result,
        )

    def test_rewrites_table_name_used_as_a_qualified_column_prefix(self):
        sql = "SELECT cat.sch.t.col FROM cat.sch.t"
        result = replace_table_name_in_sql(sql, "cat.sch.t", "`cat.sch.t.42`")
        self.assertEqual(
            "SELECT `cat.sch.t.42`.col FROM `cat.sch.t.42`",
            result,
        )


if __name__ == "__main__":
    unittest.main()
