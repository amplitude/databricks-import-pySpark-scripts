import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from preview_databricks_query_sql import transform_sql, lint_sql


class TestTransformSql(unittest.TestCase):

    def test_substitutes_every_mapped_table_with_a_view_name(self):
        sql = "SELECT * FROM cat.sch.a JOIN cat.sch.b ON a.id = b.id"
        versions = {"cat.sch.a": [0, 5], "cat.sch.b": [0, 5]}
        out = transform_sql(sql, versions)
        # original fully-qualified names no longer appear as bare references
        self.assertNotIn("FROM cat.sch.a ", out)
        self.assertNotIn("JOIN cat.sch.b ", out)
        # each table becomes a backtick-wrapped temp view name
        self.assertIn("`cat.sch.a.", out)
        self.assertIn("`cat.sch.b.", out)

    def test_substring_table_not_corrupted(self):
        sql = "FROM cat.sch.dim_product AS dp LEFT JOIN cat.sch.dim_product_version_map AS m ON dp.id = m.id"
        versions = {"cat.sch.dim_product": [0, 1], "cat.sch.dim_product_version_map": [0, 1]}
        out = transform_sql(sql, versions)
        # If the shorter name corrupted the longer one, the suffix "_version_map AS m"
        # would appear literally (unsubstituted). Verify it does NOT appear bare.
        self.assertNotIn("_version_map AS m", out)
        # The longer table must have been fully replaced with its own view name
        self.assertIn("`cat.sch.dim_product_version_map.", out)


class TestLintSql(unittest.TestCase):

    def test_clean_sql_has_no_findings(self):
        sql = "SELECT a.x AS event_type FROM cat.sch.a AS a"
        versions = {"cat.sch.a": [0, 1]}
        self.assertEqual([], lint_sql(sql, versions))

    def test_flags_unbalanced_parentheses(self):
        sql = "SELECT MAP('k', a.v FROM cat.sch.a AS a"
        versions = {"cat.sch.a": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("paren" in f.lower() for f in findings))

    def test_flags_table_in_map_not_referenced_in_sql(self):
        sql = "SELECT * FROM cat.sch.a"
        versions = {"cat.sch.a": [0, 1], "cat.sch.unused": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("cat.sch.unused" in f for f in findings))

    def test_flags_table_referenced_but_missing_from_map(self):
        sql = "SELECT * FROM cat.sch.a JOIN cat.sch.missing ON a.id = missing.id"
        versions = {"cat.sch.a": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("cat.sch.missing" in f for f in findings))

    def test_flags_line_comment_hazard(self):
        sql = "SELECT a.x AS event_type, -- a.y AS device_id\n a.z FROM cat.sch.a AS a"
        versions = {"cat.sch.a": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("comment" in f.lower() for f in findings))
