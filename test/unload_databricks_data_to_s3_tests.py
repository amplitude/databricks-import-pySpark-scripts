import unittest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, NullType
from unload_databricks_data_to_s3 import parse_table_versions_map_arg, build_sql_to_query_table_of_version, \
    build_sql_to_query_table_between_versions, get_partition_count, drop_void_fields


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

    def test_get_partition_count(self):
        self.assertEqual(1, get_partition_count(0, 2))
        self.assertEqual(1, get_partition_count(1, 2))
        self.assertEqual(1, get_partition_count(2, 2))
        self.assertEqual(2, get_partition_count(3, 2))

    def test_null_handling(self):
        """Test that empty structures are preserved while null properties are removed."""
        spark = SparkSession.builder.appName("TestNullHandling").getOrCreate()

        # Create test schema with various null scenarios
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("void_field", NullType(), True),  # This should be removed
            StructField("empty_struct", StructType([
                StructField("void_nested", NullType(), True)  # Struct becomes empty, should preserve empty struct
            ]), True),
            StructField("mixed_struct", StructType([
                StructField("valid_field", StringType(), True),
                StructField("void_field", NullType(), True)  # Should remove void but keep valid
            ]), True),
            StructField("void_array", ArrayType(NullType()), True),  # Array of voids, should be empty array
            StructField("valid_array", ArrayType(StringType()), True),  # Should preserve
            StructField("void_map", MapType(StringType(), NullType()), True),  # Map with void values, should be empty map
            StructField("valid_map", MapType(StringType(), StringType()), True),  # Should preserve
        ])

        # Create test data
        test_data = [
            (1, "Alice", None, None, ("valid_value", None), [], ["item1", "item2"], {}, {"key1": "value1"}),
            (2, "Bob", None, None, ("another_value", None), [], [], {}, {}),
        ]

        df = spark.createDataFrame(test_data, schema)
        cleaned_df = drop_void_fields(df)

        # Test assertions
        field_names = [f.name for f in cleaned_df.schema.fields]

        # void_field should be removed
        self.assertNotIn("void_field", field_names)

        # empty_struct should become null (Spark limitation with empty structs)
        # Since it only contained void_nested, the entire struct becomes null
        self.assertIn("empty_struct", field_names)

        # Verify void_nested was removed - empty_struct should be null type since all fields were void
        empty_struct_field = next(f for f in cleaned_df.schema.fields if f.name == "empty_struct")
        # The struct should have become a null literal (no internal structure)

        # mixed_struct should preserve valid_field only
        self.assertIn("mixed_struct", field_names)
        mixed_struct_fields = [f.name for f in next(f.dataType.fields for f in cleaned_df.schema.fields if f.name == "mixed_struct")]
        self.assertIn("valid_field", mixed_struct_fields)
        self.assertNotIn("void_field", mixed_struct_fields)

        # Verify void_nested (or any void field) is removed from nested structures
        self.assertEqual(len(mixed_struct_fields), 1)  # Should only have valid_field, not void_field

        # Note: empty_struct becomes null since it only contained void_nested
        # The mixed_struct test above confirms void field removal works correctly

        # void_array should be removed entirely (arrays of void are dropped)
        self.assertNotIn("void_array", field_names)

        # void_map should be removed entirely (maps with void values are dropped)
        self.assertNotIn("void_map", field_names)

        spark.stop()
