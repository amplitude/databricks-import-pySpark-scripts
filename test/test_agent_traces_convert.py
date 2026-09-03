import datetime as dt
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent_traces_convert import (
    ContentMode,
    ConversionConfig,
    ConversionError,
    Protocol,
    SourceFormat,
    combine_payloads,
    convert_record,
    json_path_get,
    stable_insert_id,
)


MAPPING = {
    "event_type": "$.type",
    "user_id": "$.identity.user",
    "time": "$.timestamp",
    "event_properties": {
        "[Agent] Agent ID": "$.agent_id",
        "[Agent] Trace ID": "$.trace_id",
        "[Agent] Span ID": "$.span_id",
        "[Agent] Tool Input": "$.tool_input",
        "constant": {"$literal": "$not-a-path"},
    },
}


def mapped_config(**overrides):
    values = {
        "source_format": SourceFormat.MAPPED_COLUMNS,
        "mapping": MAPPING,
        "content_mode": ContentMode.METADATA_ONLY,
        "strict_essentials": True,
    }
    values.update(overrides)
    return ConversionConfig(**values)


class JsonPathTests(unittest.TestCase):
    def test_nested_bracket_index_and_wildcard(self):
        document = {"items": [{"v": 1}, {"v": 2}], "odd-key": {"x": "ok"}}
        self.assertEqual(2, json_path_get(document, "$.items[1].v"))
        self.assertEqual([1, 2], json_path_get(document, "$.items[*].v"))
        self.assertEqual("ok", json_path_get(document, "$['odd-key'].x"))

    def test_unsupported_syntax_fails_closed(self):
        with self.assertRaises(ConversionError):
            json_path_get({"items": []}, "$..items")


class MappedColumnsTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "type": "[Agent] Tool Call",
            "identity": {"user": "user-1"},
            "timestamp": 1_735_689_600_000,
            "agent_id": "agent-1",
            "trace_id": "a" * 32,
            "span_id": "b" * 16,
            "tool_input": {"password": "do-not-export-by-default"},
        }

    def test_metadata_only_is_default_and_insert_id_is_stable(self):
        first = convert_record(self.row, mapped_config())[0]
        second = convert_record(dict(self.row), mapped_config())[0]
        self.assertEqual(Protocol.HTTP_V2, first.protocol)
        self.assertEqual(first.stable_key, second.stable_key)
        self.assertTrue(first.payload["insert_id"].startswith("dbx-agent-"))
        self.assertNotIn(
            "[Agent] Tool Input", first.payload["event_properties"]
        )
        self.assertEqual(
            "$not-a-path", first.payload["event_properties"]["constant"]
        )

    def test_full_content_mode_retains_content(self):
        record = convert_record(
            self.row, mapped_config(content_mode=ContentMode.FULL)
        )[0]
        self.assertEqual(
            self.row["tool_input"],
            record.payload["event_properties"]["[Agent] Tool Input"],
        )

    def test_session_end_is_filtered(self):
        row = dict(self.row, type="[Agent] Session End")
        self.assertEqual([], convert_record(row, mapped_config()))

    def test_strict_essentials_rejects_missing_identity(self):
        row = dict(self.row)
        row["identity"] = {}
        with self.assertRaisesRegex(ConversionError, "user_id or device_id"):
            convert_record(row, mapped_config())

    def test_named_format_is_plumbed(self):
        record = convert_record(
            self.row, mapped_config(named_format="customer-v1")
        )[0]
        self.assertEqual(
            "customer-v1",
            record.payload["event_properties"]["[Agent] Source Format"],
        )

    def test_insert_id_changes_with_payload(self):
        event = {"event_type": "[Agent] Span", "user_id": "u"}
        other = dict(event, user_id="v")
        self.assertEqual(stable_insert_id(event), stable_insert_id(dict(event)))
        self.assertNotEqual(stable_insert_id(event), stable_insert_id(other))

    def test_datetime_timestamp_maps_to_http_v2_millis(self):
        when = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        row = dict(self.row, timestamp=when)
        record = convert_record(row, mapped_config())[0]
        self.assertEqual(1_767_268_800_000, record.payload["time"])


class MlflowUcTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "trace_id": "01" * 16,
            "request": {"messages": [{"content": "private"}]},
            "response": {"content": "also private"},
            "trace_metadata": json.dumps({"experiment": "exp-1"}),
            "tags": {"team": "ai"},
            "spans": [
                {
                    "span_id": "02" * 8,
                    "name": "predict",
                    "start_time": dt.datetime(
                        2026, 1, 1, tzinfo=dt.timezone.utc
                    ),
                    "end_time": dt.datetime(
                        2026, 1, 1, 0, 0, 1, tzinfo=dt.timezone.utc
                    ),
                    "attributes": {
                        "gen_ai.request.model": "gpt-test",
                        "gen_ai.prompt": "private",
                        "gen_ai.usage.prompt_tokens": 42,
                        "tokens": 12,
                    },
                    "status": {"code": "STATUS_CODE_OK"},
                }
            ],
        }

    def test_builds_otlp_json_and_strips_content(self):
        config = ConversionConfig(source_format=SourceFormat.MLFLOW_UC)
        converted = convert_record(self.row, config)[0]
        self.assertEqual(Protocol.OTLP_JSON, converted.protocol)
        self.assertEqual(self.row["trace_id"], converted.stable_key)
        resource = converted.payload["resourceSpans"][0]
        attributes = {
            item["key"]: item["value"]
            for item in resource["scopeSpans"][0]["spans"][0]["attributes"]
        }
        self.assertIn("gen_ai.request.model", attributes)
        self.assertIn("gen_ai.usage.prompt_tokens", attributes)
        self.assertIn("tokens", attributes)
        self.assertNotIn("gen_ai.prompt", attributes)
        resource_keys = {
            item["key"] for item in resource["resource"]["attributes"]
        }
        self.assertNotIn("mlflow.trace.request", resource_keys)
        self.assertNotIn("mlflow.trace.response", resource_keys)

    def test_full_mode_includes_trace_request_response(self):
        config = ConversionConfig(
            source_format=SourceFormat.MLFLOW_UC,
            content_mode=ContentMode.FULL,
        )
        resource = convert_record(self.row, config)[0].payload["resourceSpans"][0]
        keys = {item["key"] for item in resource["resource"]["attributes"]}
        self.assertIn("mlflow.trace.request", keys)
        self.assertIn("mlflow.trace.response", keys)

    def test_rejects_invalid_trace_id(self):
        row = dict(self.row, trace_id="not-otel")
        with self.assertRaisesRegex(ConversionError, "32-character hexadecimal"):
            convert_record(
                row, ConversionConfig(source_format=SourceFormat.MLFLOW_UC)
            )

    def test_combines_otlp_chunks(self):
        config = ConversionConfig(source_format=SourceFormat.MLFLOW_UC)
        first = convert_record(self.row, config)[0]
        other = dict(self.row, trace_id="03" * 16)
        second = convert_record(other, config)[0]
        combined = combine_payloads(Protocol.OTLP_JSON, [first, second])
        self.assertEqual(2, len(combined["resourceSpans"]))


if __name__ == "__main__":
    unittest.main()
