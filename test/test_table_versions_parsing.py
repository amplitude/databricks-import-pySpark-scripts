import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from databricks_sql_utils import parse_table_versions_map_arg


class TestParseTableVersionsMap(unittest.TestCase):

    def test_parses_single_entry(self):
        self.assertEqual({"cat.sch.t": [0, 12]}, parse_table_versions_map_arg("cat.sch.t=0-12"))

    def test_parses_multiple_entries(self):
        self.assertEqual(
            {"cat.sch.t": [1, 12], "cat2.sch2.t2": [11, 12]},
            parse_table_versions_map_arg("cat.sch.t=1-12,cat2.sch2.t2=11-12"),
        )

    def test_rejects_missing_equals(self):
        with self.assertRaises(ValueError):
            parse_table_versions_map_arg("cat.sch.t0-12")

    def test_rejects_missing_range(self):
        with self.assertRaises(ValueError):
            parse_table_versions_map_arg("cat.sch.t=12")

    def test_rejects_non_integer_version(self):
        with self.assertRaises(ValueError):
            parse_table_versions_map_arg("cat.sch.t=a-b")

    def test_rejects_duplicate_table(self):
        with self.assertRaises(ValueError):
            parse_table_versions_map_arg("cat.sch.t=0-1,cat.sch.t=2-3")

    def test_rejects_empty_string(self):
        with self.assertRaises(ValueError):
            parse_table_versions_map_arg("")

    def test_rejects_empty_entry_between_commas(self):
        with self.assertRaises(ValueError):
            parse_table_versions_map_arg("cat.sch.t=0-1,,cat2.sch2.t2=2-3")


if __name__ == "__main__":
    unittest.main()
