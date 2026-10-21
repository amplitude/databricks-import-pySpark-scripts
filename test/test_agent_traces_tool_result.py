"""AA-152586: [Agent] Result ID / [Agent] Result Type through the agent traces converter.

``fixtures/tool_result_cases.json`` is a byte-for-byte copy of the Amplitude AI
SDK's ``langley/amplitude_ai/tests/golden/tool_result_cases.json``.
"""

import datetime as dt
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent_traces_convert import (
    ContentMode,
    ConversionConfig,
    SourceFormat,
    convert_record,
    sanitize_tool_result,
)

_GOLDEN = os.path.join(os.path.dirname(__file__), "fixtures", "tool_result_cases.json")

MAPPING = {
    "event_type": "$.type",
    "user_id": "$.identity.user",
    "time": "$.timestamp",
    "event_properties": {
        "[Agent] Agent ID": "$.agent_id",
        "[Agent] Trace ID": "$.trace_id",
        "[Agent] Span ID": "$.span_id",
        "[Agent] Session ID": "$.session_id",
        "[Agent] Is Error": "$.is_error",
        "[Agent] Tool Success": "$.tool_success",
        "[Agent] Result ID": "$.result_id",
        "[Agent] Result Type": "$.result_type",
    },
}


def _repeat(spec):
    return spec["char"] * spec["count"]


def _golden_cases():
    with open(_GOLDEN, encoding="utf-8") as handle:
        cases = json.load(handle)["cases"]
    for case in cases:
        raw = case["input"]
        if isinstance(raw, dict) and "id_repeat" in raw:
            raw = dict({k: v for k, v in raw.items() if k != "id_repeat"}, id=_repeat(raw["id_repeat"]))
        if "expected_repeat" in case:
            expected = {"[Agent] Result ID": _repeat(case["expected_repeat"])}
        else:
            expected = case["expected"]
        yield case["name"], raw, expected


def mapped_config(**overrides):
    values = {"source_format": SourceFormat.MAPPED_COLUMNS, "mapping": MAPPING}
    values.update(overrides)
    return ConversionConfig(**values)


def result_attributes(record):
    span = record.payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    return {
        item["key"]: item["value"]
        for item in span["attributes"]
        if item["key"].startswith("amplitude.tool.result")
    }


class GoldenCaseTests(unittest.TestCase):
    def test_every_golden_case_matches_the_sdk_rules(self):
        names = []
        for name, raw, expected in _golden_cases():
            names.append(name)
            with self.subTest(name=name):
                self.assertEqual(expected, sanitize_tool_result(raw))
        self.assertGreaterEqual(len(names), 25)

    def test_integral_float_id_becomes_integer_string(self):
        self.assertEqual({"[Agent] Result ID": "12345"}, sanitize_tool_result({"id": 12345.0}))


class MappedToolResultTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "type": "[Agent] Tool Call",
            "identity": {"user": "user-1"},
            "timestamp": 1_735_689_600_000,
            "agent_id": "agent-1",
            "trace_id": "a" * 32,
            "span_id": "b" * 16,
            "session_id": "session-1",
            "result_id": " ord_1 ",
            "result_type": "order",
        }

    def test_successful_tool_call_carries_both_in_every_content_mode(self):
        for mode in ContentMode:
            with self.subTest(mode=mode):
                attrs = result_attributes(convert_record(self.row, mapped_config(content_mode=mode))[0])
                self.assertEqual(
                    {
                        "amplitude.tool.result_id": {"stringValue": "ord_1"},
                        "amplitude.tool.result_type": {"stringValue": "order"},
                    },
                    attrs,
                )

    def test_explicit_success_flags_carry_the_result(self):
        for success in (True, "true", 1, "", None):
            with self.subTest(success=success):
                row = dict(self.row, tool_success=success, is_error=False)
                attrs = result_attributes(convert_record(row, mapped_config())[0])
                self.assertEqual({"stringValue": "ord_1"}, attrs["amplitude.tool.result_id"])

    def test_failed_tool_call_carries_neither(self):
        for failure in ({"is_error": True}, {"is_error": "true"}, {"tool_success": False}, {"tool_success": "false"}):
            for mode in ContentMode:
                with self.subTest(failure=failure, mode=mode):
                    row = dict(self.row, **failure)
                    self.assertEqual({}, result_attributes(convert_record(row, mapped_config(content_mode=mode))[0]))

    def test_invalid_id_drops_both(self):
        for bad_id in ("   ", "x" * 513, True, 2**53, 1.5, None):
            with self.subTest(bad_id=bad_id):
                row = dict(self.row, result_id=bad_id)
                self.assertEqual({}, result_attributes(convert_record(row, mapped_config())[0]))

    def test_invalid_type_drops_only_the_type(self):
        row = dict(self.row, result_type="Order")
        self.assertEqual(
            {"amplitude.tool.result_id": {"stringValue": "ord_1"}},
            result_attributes(convert_record(row, mapped_config())[0]),
        )

    def test_integer_ids_are_emitted_as_strings(self):
        for raw in (98765, 98765.0, "98765"):
            with self.subTest(raw=raw):
                row = dict(self.row, result_id=raw)
                attrs = result_attributes(convert_record(row, mapped_config())[0])
                self.assertEqual({"stringValue": "98765"}, attrs["amplitude.tool.result_id"])

    def test_full_mode_redaction_does_not_rewrite_the_id(self):
        config = mapped_config(
            content_mode=ContentMode.FULL,
            redact_pii=True,
            custom_redaction_patterns=("ord_",),
        )
        for raw in ("buyer@example.com", "555-123-4567", "ord_1", "10.0.0.1"):
            with self.subTest(raw=raw):
                row = dict(self.row, result_id=raw)
                attrs = result_attributes(convert_record(row, config)[0])
                self.assertEqual({"stringValue": raw}, attrs["amplitude.tool.result_id"])

    def test_non_tool_events_carry_neither(self):
        row = dict(self.row, type="[Agent] AI Response")
        self.assertEqual({}, result_attributes(convert_record(row, mapped_config())[0]))

    def test_metadata_only_stable_key_ignores_the_result(self):
        config = mapped_config(content_mode=ContentMode.METADATA_ONLY)
        with_result = convert_record(self.row, config)[0]
        without = convert_record(dict(self.row, result_id=None, result_type=None), config)[0]
        self.assertEqual(without.stable_key, with_result.stable_key)


class RawSpanToolResultTests(unittest.TestCase):
    def row(self, attributes):
        return {
            "trace_id": "01" * 16,
            "trace_metadata": {"session_id": "session-1", "enduser.id": "user-1"},
            "spans": [
                {
                    "span_id": "02" * 8,
                    "name": "book",
                    "span_type": "TOOL",
                    "start_time": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                    "end_time": dt.datetime(2026, 1, 1, 0, 0, 1, tzinfo=dt.timezone.utc),
                    "attributes": attributes,
                }
            ],
        }

    def test_raw_attributes_pass_through_unredacted_in_every_content_mode(self):
        encoded = [
            {"key": "amplitude.tool.result_id", "value": {"stringValue": "buyer@example.com"}},
            {"key": "amplitude.tool.result_type", "value": {"stringValue": "order"}},
        ]
        for attributes in (
            {"amplitude.tool.result_id": "buyer@example.com", "amplitude.tool.result_type": "order"},
            encoded,
        ):
            for mode in ContentMode:
                with self.subTest(mode=mode, encoded=isinstance(attributes, list)):
                    config = ConversionConfig(source_format=SourceFormat.MLFLOW_UC, content_mode=mode)
                    attrs = result_attributes(convert_record(self.row(attributes), config)[0])
                    self.assertEqual(
                        {
                            "amplitude.tool.result_id": {"stringValue": "buyer@example.com"},
                            "amplitude.tool.result_type": {"stringValue": "order"},
                        },
                        attrs,
                    )


if __name__ == "__main__":
    unittest.main()
