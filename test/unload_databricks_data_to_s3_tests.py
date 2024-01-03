import unittest
from unload_databricks_data_to_s3 import parse_table_versions_map_arg


class TestStringMethods(unittest.TestCase):

    def test_parse_table_version(self):
        expected = {'catalog.schema.table': [100, 200]}
        self.assertEqual(expected,
                         parse_table_versions_map_arg("catalog.schema.table=100-200"))
        expected = {'catalog.schema.table': [1, 12], 'catalog2.schema2.table2': [11, 12]}
        self.assertEqual(expected,
                         parse_table_versions_map_arg("catalog.schema.table=1-12,catalog2.schema2.table2=11-12"))
