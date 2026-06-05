import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from validate_databricks_query import _transform_sql, check_required_columns, required_columns_for


class FakeColumn:

    def __init__(self, expression):
        self.expression = expression

    def isNull(self):
        return FakeColumn(f"{self.expression} IS NULL")

    def eqNullSafe(self, value):
        return FakeColumn(f"{self.expression} <=> {value}")

    def __or__(self, other):
        return FakeColumn(f"({self.expression} OR {other.expression})")


class FakeDataFrame:

    def __init__(self, columns):
        self.columns = columns
        self.filters = []
        self.drops = []
        self.view_name = None

    def __getitem__(self, column):
        return FakeColumn(column)

    def filter(self, condition):
        self.filters.append(condition.expression)
        return self

    def drop(self, *columns):
        self.drops.append(columns)
        self.columns = [column for column in self.columns if column not in columns]
        return self

    def createOrReplaceTempView(self, view_name):
        self.view_name = view_name


class FakeSpark:

    def __init__(self):
        self.queries = []
        self.data_frames = []

    def sql(self, query):
        self.queries.append(query)
        data_frame = FakeDataFrame(["id", "_commit_version", "_commit_timestamp", "_change_type"])
        self.data_frames.append(data_frame)
        return data_frame


class TestTransformSql(unittest.TestCase):

    def test_uses_snapshot_for_initial_version(self):
        spark = FakeSpark()

        with patch("validate_databricks_query.build_temp_view_name", return_value="`view`"):
            transformed = _transform_sql(
                "select * from catalog.schema.table",
                {"catalog.schema.table": [0, 12]},
                spark,
                "EVENT",
                False,
            )

        self.assertEqual(
            ["select * from catalog.schema.table version as of 12"],
            spark.queries,
        )
        self.assertEqual("select * from `view`", transformed)

    def test_uses_table_changes_and_filters_cdf_rows_for_incremental_versions(self):
        spark = FakeSpark()

        with patch("validate_databricks_query.build_temp_view_name", return_value="`view`"):
            _transform_sql(
                "select * from catalog.schema.table",
                {"catalog.schema.table": [10, 12]},
                spark,
                "USER_PROPERTY",
                False,
            )

        self.assertEqual(
            ['select * from table_changes("catalog.schema.table", 10, 12)'],
            spark.queries,
        )
        self.assertEqual(
            ["((_change_type IS NULL OR _change_type <=> insert) OR _change_type <=> update_postimage)"],
            spark.data_frames[0].filters,
        )
        self.assertEqual(
            [("_commit_version", "_commit_timestamp", "_change_type")],
            spark.data_frames[0].drops,
        )

    def test_skips_cdf_filtering_in_mutability_mode(self):
        spark = FakeSpark()

        with patch("validate_databricks_query.build_temp_view_name", return_value="`view`"):
            _transform_sql(
                "select * from catalog.schema.table",
                {"catalog.schema.table": [10, 12]},
                spark,
                "EVENT",
                True,
            )

        self.assertEqual([], spark.data_frames[0].filters)
        self.assertEqual([], spark.data_frames[0].drops)


class TestRequiredColumnsFor(unittest.TestCase):

    def test_event_default_identity_is_user_id(self):
        self.assertEqual(
            {"event_type", "event_properties", "time", "user_id"},
            required_columns_for("EVENT", "USER_ID"),
        )

    def test_event_device_id_identity(self):
        self.assertEqual(
            {"event_type", "event_properties", "time", "device_id"},
            required_columns_for("EVENT", "DEVICE_ID"),
        )

    def test_event_both_identity(self):
        self.assertEqual(
            {"event_type", "event_properties", "time", "user_id", "device_id"},
            required_columns_for("EVENT", "USER_ID_AND_DEVICE_ID"),
        )

    def test_user_property_default(self):
        self.assertEqual(
            {"user_properties", "user_id"},
            required_columns_for("USER_PROPERTY", "USER_ID"),
        )

    def test_group_property_requires_groups_only_no_identity(self):
        self.assertEqual({"groups"}, required_columns_for("GROUP_PROPERTY", "USER_ID"))

    def test_warehouse_property_identity_only(self):
        self.assertEqual({"user_id"}, required_columns_for("WAREHOUSE_PROPERTY", "USER_ID"))


class TestCheckRequiredColumns(unittest.TestCase):

    def test_passes_when_all_present(self):
        missing = check_required_columns(
            ["event_type", "event_properties", "time", "user_id", "extra"],
            "EVENT", "USER_ID",
        )
        self.assertEqual([], missing)

    def test_reports_missing_event_type(self):
        missing = check_required_columns(
            ["event_properties", "time", "user_id"], "EVENT", "USER_ID",
        )
        self.assertEqual(["event_type"], missing)

    def test_group_property_has_no_identity_requirement(self):
        # GROUP_PROPERTY keys on `groups` and requires no identity column.
        missing = check_required_columns(["groups"], "GROUP_PROPERTY", "USER_ID")
        self.assertEqual([], missing)
