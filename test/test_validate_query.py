import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from validate_databricks_query import required_columns_for, check_required_columns


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

    def test_rejects_unknown_data_type(self):
        with self.assertRaises(ValueError):
            required_columns_for("BOGUS", "USER_ID")

    def test_rejects_unknown_record_identity(self):
        with self.assertRaises(ValueError):
            required_columns_for("EVENT", "BOGUS")


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
