import unittest
from unload_databricks_data_to_s3 import parse_table_versions_map_arg, build_sql_to_query_table_of_version, \
    build_sql_to_query_table_between_versions


class TestStringMethods(unittest.TestCase):

    def test_parse_table_version(self):
        expected = {'catalog.schema.table': [100, 200]}
        self.assertEqual(expected,
                         parse_table_versions_map_arg("catalog.schema.table=100-200"))
        expected = {'catalog.schema.table': [1, 12], 'catalog2.schema2.table2': [11, 12]}
        self.assertEqual(expected,
                         parse_table_versions_map_arg("catalog.schema.table=1-12,catalog2.schema2.table2=11-12"))

    def test_build_sql_to_query_table_of_version(self):
        expected = "select * from table1 version as of 10"
        self.assertEqual(expected, build_sql_to_query_table_of_version("table1", 10))

    def test_build_sql_to_query_table_between_versions(self):
        expected = "select * from table_changes(\"table1\", 10, 11)"
        self.assertEqual(expected, build_sql_to_query_table_between_versions("table1", 10, 11))

