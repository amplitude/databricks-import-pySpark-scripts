import unittest
from unittest.mock import MagicMock

from unload_databricks_data_to_s3 import parse_table_versions_map_arg, build_sql_to_query_table_of_version, \
    build_sql_to_query_table_between_versions, get_partition_count, normalize_sql_query, \
    determine_first_table_name_in_sql, copy_and_inject_cdf_metadata_column_names, \
    determine_id_column_name_for_mutation_row_type, generate_sql_to_unload_mutation_data, \
    replace_double_slashes_with_single_slash, move_spark_metadata_to_separate_s3_folder


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

    def test_normalize_sql_query(self):
        input_query = """
        SELECT *
            FROM   
            table1  
        WHERE  
  
            id = 10
   
        """
        expected_output = "SELECT * FROM table1 WHERE id = 10"
        self.assertEqual(expected_output, normalize_sql_query(input_query))

    def test_determine_first_table_name_in_sql(self):
        sql_query = normalize_sql_query(
            "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.id = 10")
        table_names = {"table2", "table1"}
        expected_output = "table1"
        self.assertEqual(expected_output, determine_first_table_name_in_sql(sql_query, table_names))

        sql_query = normalize_sql_query(
            "SELECT * FROM table2 JOIN table1 ON table2.id = table1.id WHERE table2.id = 10")
        expected_output = "table2"
        self.assertEqual(expected_output, determine_first_table_name_in_sql(sql_query, table_names))

        sql_query = normalize_sql_query("SELECT * FROM table1")
        expected_output = "table1"
        self.assertEqual(expected_output, determine_first_table_name_in_sql(sql_query, table_names))

    def test_copy_and_inject_cdf_metadata_column_names(self):
        sql_query = normalize_sql_query("SELECT * FROM table1")
        expected_output = "SELECT _commit_version, _commit_timestamp, _change_type, * FROM table1"
        self.assertEqual(expected_output, copy_and_inject_cdf_metadata_column_names(sql_query))

        sql_query = normalize_sql_query("select * FROM table1")
        expected_output = "SELECT _commit_version, _commit_timestamp, _change_type, * FROM table1"
        self.assertEqual(expected_output, copy_and_inject_cdf_metadata_column_names(sql_query))

        sql_query = normalize_sql_query("sElEct * FROM table1")
        expected_output = "SELECT _commit_version, _commit_timestamp, _change_type, * FROM table1"
        self.assertEqual(expected_output, copy_and_inject_cdf_metadata_column_names(sql_query))

        sql_query = normalize_sql_query("SELECT * FROM table1 JOIN (SELECT * FROM table2) ON table1.id = table2.id")
        expected_output = ("SELECT _commit_version, _commit_timestamp, _change_type, * FROM table1 "
                           "JOIN (SELECT * FROM table2) ON table1.id = table2.id")
        self.assertEqual(expected_output, copy_and_inject_cdf_metadata_column_names(sql_query))

        sql_query = normalize_sql_query("SELECT id, name FROM table1")
        expected_output = "SELECT _commit_version, _commit_timestamp, _change_type, id, name FROM table1"
        self.assertEqual(expected_output, copy_and_inject_cdf_metadata_column_names(sql_query))

    def test_determine_id_column_name_for_mutation_row_type(self):
        self.assertEqual('insert_id', determine_id_column_name_for_mutation_row_type('http_api_event_json'))
        with self.assertRaises(NotImplementedError):
            determine_id_column_name_for_mutation_row_type('http_api_user_json')
        with self.assertRaises(NotImplementedError):
            determine_id_column_name_for_mutation_row_type('http_api_group_json')
        with self.assertRaises(ValueError):
            determine_id_column_name_for_mutation_row_type('unknown')

    def test_generate_sql_to_unload_mutation_data(self):
        sql_query = normalize_sql_query("SELECT id as insert_id, name FROM table1")
        mutation_row_type = "http_api_event_json"
        is_initial_sync = True
        expected_output = """
          SELECT
            STRUCT(STRUCT(*) AS current_version) AS data,
            'insert' as mutation_type,
            'http_api_event_json' AS data_type,
            UUID() as mutation_insert_id,
            unix_timestamp() * 1000 as mutation_time_ms
          FROM (SELECT id as insert_id, name FROM table1)
        """
        self.assertEqual(normalize_sql_query(expected_output),
                         normalize_sql_query(generate_sql_to_unload_mutation_data(
                             sql_query, mutation_row_type, is_initial_sync)))

        sql_query = normalize_sql_query("SELECT id as insert_id, name FROM table1")
        mutation_row_type = "http_api_event_json"
        is_initial_sync = False
        expected_output = """
          WITH CdfData AS (
            SELECT _commit_version, _commit_timestamp, _change_type, id as insert_id, name FROM table1
          ),
          FormattedChanges AS (
            SELECT STRUCT(* except(_change_type, _commit_version, _commit_timestamp)) AS data, _change_type, _commit_version, _commit_timestamp FROM CdfData
          ),
          Combined AS (
            SELECT
              STRUCT(
                CASE WHEN c1._change_type = 'update_postimage' THEN c1.data ELSE c2.data END AS current_version,
                CASE WHEN c1._change_type = 'update_preimage' THEN c1.data ELSE c2.data END AS previous_version
              ) AS data,
              'update' as _change_type,
              c1._commit_timestamp,
              c1._commit_version
            FROM FormattedChanges c1
            LEFT JOIN FormattedChanges c2 ON c1._commit_version = c2._commit_version AND c1._commit_timestamp = c2._commit_timestamp AND c1._change_type <> c2._change_type AND c1.data.insert_id = c2.data.insert_id
            WHERE c1._change_type IN ('update_postimage', 'update_preimage') AND c2._change_type IN ('update_postimage', 'update_preimage')

            UNION ALL

            SELECT
              STRUCT(
                c1.data AS current_version,
                null AS previous_version
              ) as data,
              c1._change_type,
              c1._commit_timestamp,
              c1._commit_version
            FROM FormattedChanges c1
            WHERE c1._change_type NOT IN ('update_postimage', 'update_preimage')
          ),
          DistinctData AS (
            SELECT DISTINCT
              data as data,
              _change_type,
              _commit_timestamp,
              _commit_version
            FROM Combined
          )
          SELECT
            data,
            _change_type as mutation_type,
            'http_api_event_json' AS data_type,
            UUID() as mutation_insert_id,
            unix_timestamp(_commit_timestamp) * 1000 as mutation_time_ms
          FROM DistinctData
        """
        self.assertEqual(normalize_sql_query(expected_output),
                         normalize_sql_query(generate_sql_to_unload_mutation_data(
                             sql_query, mutation_row_type, is_initial_sync)))

    def test_replace_double_slashes_with_single_slash(self):
        input_string = '///path/to//file////with//double//////slashes/end/'
        expected_output = '/path/to/file/with/double/slashes/end/'
        self.assertEqual(expected_output, replace_double_slashes_with_single_slash(input_string))

    def test_move_spark_metadata_to_separate_s3_folder(self):
        # given
        bucket = 'mybucket'
        prefix = '/myprefix/with/subdir'
        s3_uri = f's3://{bucket}/{prefix}'

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': f'{prefix}/file1'},
                {'Key': f'{prefix}/part-file2'},
                {'Key': f'{prefix}/file3'},
                {'Key': f'{prefix}/part-file4'}
            ]
        }

        # when
        move_spark_metadata_to_separate_s3_folder(mock_s3, s3_uri)

        # then
        # Check that the list_objects_v2 method was called with the correct bucket and prefix
        mock_s3.list_objects_v2.assert_called_with(Bucket='mybucket', Prefix='/myprefix/with/subdir', Delimiter='/')

        # Check that the copy_object and delete_object methods were called for the correct files
        mock_s3.copy_object.assert_any_call(Bucket='mybucket', CopySource='mybucket/myprefix/with/subdir/file1', Key='/myprefix/with/subdir/spark_metadata/file1')
        mock_s3.delete_object.assert_any_call(Bucket='mybucket', Key='/myprefix/with/subdir/file1')
        mock_s3.copy_object.assert_any_call(Bucket='mybucket', CopySource='mybucket/myprefix/with/subdir/file3', Key='/myprefix/with/subdir/spark_metadata/file3')
        mock_s3.delete_object.assert_any_call(Bucket='mybucket', Key='/myprefix/with/subdir/file3')

        # Check copy_object and delete_object were called the correct number of times
        self.assertEqual(2, mock_s3.copy_object.call_count)
        self.assertEqual(2, mock_s3.delete_object.call_count)
