import datetime as dt
import json
import os
import sys
import types
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent_traces_convert import (
    ContentMode,
    ConversionConfig,
    ConversionError,
    Protocol,
    SourceFormat,
    canonical_session_id,
    combine_payloads,
    convert_record,
    json_path_get,
    stable_insert_id,
)
import agent_traces_job
from agent_traces_job import (
    _resolve_session_id_column,
    derive_session_id,
    parse_args,
    process_partition,
    run,
    select_sessions,
    session_skip_counts,
    session_is_sampled,
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

    def test_wildcard_keeps_partial_index_matches(self):
        document = {"items": [{"vals": [1]}, {"vals": []}, {"vals": [2, 3]}]}
        self.assertEqual([1, 2], json_path_get(document, "$.items[*].vals[0]"))
        self.assertIsNone(
            json_path_get({"items": [{"vals": []}]}, "$.items[*].vals[0]", None)
        )

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

    def test_full_is_default_and_insert_id_is_stable(self):
        first = convert_record(self.row, mapped_config())[0]
        second = convert_record(dict(self.row), mapped_config())[0]
        self.assertEqual(Protocol.HTTP_V2, first.protocol)
        self.assertEqual(first.stable_key, second.stable_key)
        self.assertTrue(first.payload["insert_id"].startswith("dbx-agent-"))
        self.assertLessEqual(len(first.payload["insert_id"]), 64)
        self.assertIn(
            "[Agent] Tool Input", first.payload["event_properties"]
        )
        self.assertEqual(
            "$not-a-path", first.payload["event_properties"]["constant"]
        )

    def test_metadata_only_strips_content(self):
        record = convert_record(
            self.row, mapped_config(content_mode=ContentMode.METADATA_ONLY)
        )[0]
        self.assertNotIn(
            "[Agent] Tool Input", record.payload["event_properties"]
        )

    def test_default_full_mode_redacts_builtin_pii_and_base64(self):
        row = dict(
            self.row,
            tool_input={
                "email": "user@example.com",
                "us_phone": "(555) 123-4567",
                "intl_phone": "+441234567890",
                "card": "4111 1111 1111 1111",
                "ssn": "123-45-6789",
                "ipv4": "10.0.0.1",
                "ipv6": "2001:db8::1",
                "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
            },
        )
        tool_input = convert_record(row, mapped_config())[0].payload[
            "event_properties"
        ]["[Agent] Tool Input"]
        self.assertEqual("[email]", tool_input["email"])
        self.assertEqual("([phone]", tool_input["us_phone"])
        self.assertEqual("[phone]", tool_input["intl_phone"])
        self.assertEqual("[credit_card]", tool_input["card"])
        self.assertEqual("[ssn]", tool_input["ssn"])
        self.assertEqual("[ip_address]", tool_input["ipv4"])
        self.assertEqual("[ip_address]", tool_input["ipv6"])
        self.assertEqual("[base64 image redacted]", tool_input["image"])

    def test_redaction_can_be_disabled_and_custom_patterns_apply(self):
        row = dict(self.row, tool_input="user@example.com account ACME-123")
        record = convert_record(
            row,
            mapped_config(
                redact_pii=False,
                custom_redaction_patterns=(r"ACME-\d+",),
            ),
        )[0]
        self.assertEqual(
            "user@example.com account [REDACTED]",
            record.payload["event_properties"]["[Agent] Tool Input"],
        )

    def test_redacts_llm_message_content(self):
        mapping = dict(MAPPING)
        mapping["event_properties"] = dict(
            MAPPING["event_properties"],
            **{"$llm_message": "$.llm_message"},
        )
        row = dict(self.row, llm_message={"text": "Contact user@example.com"})
        record = convert_record(row, mapped_config(mapping=mapping))[0]
        self.assertEqual(
            "Contact [email]",
            record.payload["event_properties"]["$llm_message"]["text"],
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
        self.assertLessEqual(len(stable_insert_id(event)), 64)
        self.assertTrue(stable_insert_id(event).startswith("dbx-agent-"))

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
        config = ConversionConfig(
            source_format=SourceFormat.MLFLOW_UC,
            content_mode=ContentMode.METADATA_ONLY,
        )
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
        config = ConversionConfig(source_format=SourceFormat.MLFLOW_UC)
        resource = convert_record(self.row, config)[0].payload["resourceSpans"][0]
        keys = {item["key"] for item in resource["resource"]["attributes"]}
        self.assertIn("mlflow.trace.request", keys)
        self.assertIn("mlflow.trace.response", keys)

    def test_full_mode_redacts_mlflow_content_recursively(self):
        row = dict(
            self.row,
            request={"message": "email user@example.com from 192.168.1.1"},
        )
        resource = convert_record(
            row, ConversionConfig(source_format=SourceFormat.MLFLOW_UC)
        )[0].payload["resourceSpans"][0]["resource"]["attributes"]
        request = next(
            item["value"]
            for item in resource
            if item["key"] == "mlflow.trace.request"
        )
        values = request["kvlistValue"]["values"]
        self.assertEqual(
            "email [email] from [ip_address]",
            values[0]["value"]["stringValue"],
        )

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


class _FakeSchema:
    def __init__(self, column_names):
        self.fields = [
            type("Field", (), {"name": name, "dataType": "string"})()
            for name in column_names
        ]


class _FakeRDD:
    def __init__(self, rows):
        self.rows = rows
        self.results = None

    def mapPartitionsWithIndex(self, function):
        self.results = list(function(0, iter(self.rows)))
        return self

    def collect(self):
        return self.results


class _FakeDataFrame:
    def __init__(self, rows, column_names=()):
        self.schema = _FakeSchema(column_names)
        self.rdd = _FakeRDD(rows)
        self.filters = []

    def filter(self, condition):
        self.filters.append(condition)
        return self


class _FakeColumn:
    def __gt__(self, other):
        return ("gt", other)

    def __le__(self, other):
        return ("le", other)


class _FakeLiteral:
    def __init__(self, value):
        self.value = value

    def cast(self, data_type):
        return self.value


def fake_pyspark_modules():
    """Minimal pyspark.sql.functions stand-in for driver-only code paths."""
    functions = types.ModuleType("pyspark.sql.functions")
    functions.col = lambda name: _FakeColumn()
    functions.lit = _FakeLiteral
    sql = types.ModuleType("pyspark.sql")
    sql.functions = functions
    root = types.ModuleType("pyspark")
    root.sql = sql
    return {
        "pyspark": root,
        "pyspark.sql": sql,
        "pyspark.sql.functions": functions,
    }


class _FakeSparkContext:
    def __init__(self):
        self.py_files = []

    def addPyFile(self, path):
        self.py_files.append(path)


class _FakeSpark:
    def __init__(self, data_frame):
        self.data_frame = data_frame
        self.sparkContext = _FakeSparkContext()

    def table(self, name):
        return self.data_frame


class JobOptionsTests(unittest.TestCase):
    def test_defaults_full_redacted_and_unsampled(self):
        args, unknown = parse_args(
            ["--table", "catalog.schema.table", "--format", "mlflow-uc", "--dry-run"]
        )
        self.assertEqual([], unknown)
        self.assertEqual("full", args.content_mode)
        self.assertTrue(args.redact_pii)
        self.assertEqual(1.0, args.sample_rate)
        self.assertIsNone(args.max_sessions)
        self.assertIsNone(args.protocol)

    def test_validates_sampling_and_custom_patterns(self):
        with self.assertRaisesRegex(ValueError, "sample-rate"):
            parse_args(
                [
                    "--table",
                    "t",
                    "--format",
                    "mlflow-uc",
                    "--dry-run",
                    "--sample-rate",
                    "0",
                ]
            )
        with self.assertRaisesRegex(ValueError, "custom redaction"):
            parse_args(
                [
                    "--table",
                    "t",
                    "--format",
                    "mlflow-uc",
                    "--dry-run",
                    "--custom-redaction-patterns-json",
                    '["[invalid"]',
                ]
            )

    def test_sample_rate_bounds_are_enforced(self):
        for value in ("0", "0.0", "-0.5", "1.5"):
            with self.assertRaisesRegex(ValueError, "sample-rate"):
                parse_args(
                    [
                        "--table",
                        "t",
                        "--format",
                        "mlflow-uc",
                        "--dry-run",
                        "--sample-rate",
                        value,
                    ]
                )
        args, _ = parse_args(
            [
                "--table",
                "t",
                "--format",
                "mlflow-uc",
                "--dry-run",
                "--sample-rate",
                "0.25",
            ]
        )
        self.assertEqual(0.25, args.sample_rate)

    def test_session_sampling_is_stable(self):
        decisions = [session_is_sampled("conversation-1", 0.5) for _ in range(10)]
        self.assertEqual([decisions[0]] * 10, decisions)
        self.assertTrue(session_is_sampled("conversation-1", 1.0))
        self.assertFalse(session_is_sampled(None, 1.0))

    def test_sampling_and_cap_keep_whole_conversations(self):
        rows = [
            {"conversation_id": "conv-{}".format(index % 40), "seq": index}
            for index in range(400)
        ]
        session_ids = [row["conversation_id"] for row in rows]
        for sample_rate, max_sessions in ((0.5, None), (1.0, 7), (0.5, 3)):
            kept = set(select_sessions(session_ids, sample_rate, max_sessions))
            selected = [row for row in rows if row["conversation_id"] in kept]
            for conversation in set(session_ids):
                rows_in_conversation = [
                    row for row in rows if row["conversation_id"] == conversation
                ]
                selected_in_conversation = [
                    row for row in selected if row["conversation_id"] == conversation
                ]
                self.assertIn(
                    len(selected_in_conversation),
                    (0, len(rows_in_conversation)),
                    "conversation {} was split by sample_rate={} max_sessions={}".format(
                        conversation, sample_rate, max_sessions
                    ),
                )
            if max_sessions is not None:
                self.assertLessEqual(len(kept), max_sessions)
            self.assertEqual(
                kept, set(select_sessions(reversed(session_ids), sample_rate, max_sessions))
            )

    def test_missing_derived_session_ids_are_bucketed_and_skipped(self):
        session_ids = ["conversation-a", None, "", "conversation-a", "conversation-b"]
        self.assertEqual(
            {"conversation-a", "conversation-b"},
            set(select_sessions(session_ids, 1.0, 200)),
        )
        self.assertEqual(
            {"missing_session_id": 2},
            session_skip_counts(
                rows_before=5,
                rows_missing_session=2,
                rows_after_sampling=3,
                rows_after_cap=3,
            ),
        )

    def test_default_cap_uses_converter_when_no_session_column(self):
        args, _ = parse_args(
            [
                "--table",
                "t",
                "--format",
                "mlflow-uc",
                "--dry-run",
                "--max-sessions",
                "200",
            ]
        )
        self.assertIsNone(
            _resolve_session_id_column(
                _FakeDataFrame([], ("request_id", "trace_id", "trace_metadata")),
                args,
            )
        )
        self.assertIsNone(
            _resolve_session_id_column(
                _FakeDataFrame([], ("trace_id", "conversation_id")), args
            )
        )
        override_args, _ = parse_args(
            [
                "--table",
                "t",
                "--format",
                "mlflow-uc",
                "--dry-run",
                "--max-sessions",
                "200",
                "--session-id-column",
                "custom_conversation_key",
            ]
        )
        self.assertEqual(
            "custom_conversation_key",
            _resolve_session_id_column(
                _FakeDataFrame([], ("custom_conversation_key",)), override_args
            ),
        )

    def test_mlflow_nested_session_with_default_cap_and_no_override(self):
        args, _ = parse_args(
            [
                "--table",
                "t",
                "--format",
                "mlflow-uc",
                "--dry-run",
                "--max-sessions",
                "200",
            ]
        )
        self.assertEqual(200, args.max_sessions)
        self.assertIsNone(args.session_id_column)
        values = self._conversion_values("mlflow-uc")
        metadata_row = {
            "trace_id": "request-trace-must-not-be-session",
            "request_id": "request-must-not-be-session",
            "trace_metadata": json.dumps({"conversation_id": "conversation-meta"}),
            "tags": json.dumps({"session_id": "session-tag"}),
        }
        self.assertEqual("conversation-meta", derive_session_id(metadata_row, values))
        tag_row = {
            "trace_id": "other-trace",
            "request_id": "other-request",
            "trace_metadata": "{}",
            "tags": {"session_id": "session-tag"},
        }
        self.assertEqual("session-tag", derive_session_id(tag_row, values))
        self.assertIsNone(
            derive_session_id(
                {"trace_id": "trace-only", "request_id": "request-only"}, values
            )
        )

    def test_mapped_nested_jsonpath_session_with_default_cap_no_override(self):
        args, _ = parse_args(
            [
                "--table",
                "t",
                "--format",
                "mapped-columns",
                "--mapping-json",
                json.dumps(MAPPING),
                "--dry-run",
                "--max-sessions",
                "200",
            ]
        )
        self.assertEqual(200, args.max_sessions)
        self.assertIsNone(args.session_id_column)
        mapping = dict(MAPPING)
        mapping["event_properties"] = dict(
            MAPPING["event_properties"],
            **{"[Agent] Session ID": "$.context.conversation.session_id"},
        )
        values = self._conversion_values("mapped-columns", mapping)
        row = {
            "context": {"conversation": {"session_id": "mapped-session"}},
            "trace_id": "trace-must-not-be-session",
            "request_id": "request-must-not-be-session",
        }
        self.assertEqual("mapped-session", derive_session_id(row, values))
        self.assertIsNone(
            derive_session_id(
                {
                    "context": {"conversation": {}},
                    "trace_id": "trace-only",
                    "request_id": "request-only",
                },
                values,
            )
        )

    def test_canonical_session_helper_matches_converter_contract(self):
        mapping = {
            "event_properties": {
                "[Agent] Session ID": {
                    "$path": "$.nested.session",
                    "required": False,
                }
            }
        }
        self.assertEqual(
            "session-123",
            canonical_session_id(
                {"nested": {"session": "session-123"}},
                ConversionConfig(
                    source_format=SourceFormat.MAPPED_COLUMNS, mapping=mapping
                ),
            ),
        )

    @staticmethod
    def _conversion_values(source_format, mapping=None):
        return {
            "source_format": source_format,
            "mapping": mapping,
            "named_format": None,
            "content_mode": "full",
            "strict_essentials": True,
            "redact_pii": True,
            "custom_redaction_patterns": (),
        }

    def test_dry_run_counts_bytes_without_posting(self):
        case = MappedColumnsTests()
        case.setUp()
        conversion = {
            "source_format": "mapped-columns",
            "mapping": MAPPING,
            "named_format": None,
            "content_mode": "full",
            "strict_essentials": True,
            "redact_pii": True,
            "custom_redaction_patterns": (),
        }
        delivery = {
            "api_key": None,
            "server_zone": "US",
            "chunk_size": 100,
            "max_request_bytes": 1_000_000,
            "max_retries": 0,
            "initial_backoff_seconds": 0,
            "request_timeout_seconds": 1,
            "dry_run": True,
        }
        stats = list(process_partition(0, [case.row], conversion, delivery))
        self.assertEqual(1, stats[0]["records_converted"])
        self.assertEqual(1, stats[0]["requests_would_send"])
        self.assertGreater(stats[0]["bytes_would_send"], 0)
        self.assertEqual(0, stats[0]["requests_sent"])

    def test_protocol_override_rejects_mismatched_payload_protocol(self):
        case = MappedColumnsTests()
        case.setUp()
        conversion = {
            "source_format": "mapped-columns",
            "mapping": MAPPING,
            "named_format": None,
            "content_mode": "full",
            "strict_essentials": True,
            "redact_pii": True,
            "custom_redaction_patterns": (),
        }
        delivery = {
            "api_key": None,
            "server_zone": "US",
            "chunk_size": 100,
            "max_request_bytes": 1_000_000,
            "max_retries": 0,
            "initial_backoff_seconds": 0,
            "request_timeout_seconds": 1,
            "dry_run": True,
            "protocol": "otlp-json",
        }
        with self.assertRaisesRegex(ValueError, "protocol override"):
            list(process_partition(0, [case.row], conversion, delivery))

        delivery["protocol"] = "http-v2"
        stats = list(process_partition(0, [case.row], conversion, delivery))
        self.assertEqual(1, stats[0]["records_converted"])
        self.assertEqual(1, stats[0]["requests_would_send"])

    def test_skips_are_bucketed_by_conversion_reason(self):
        case = MappedColumnsTests()
        case.setUp()
        rows = [
            dict(case.row, identity={}),
            dict(case.row, type="not-an-agent-event"),
            dict(case.row, timestamp="not-a-timestamp"),
            dict(case.row, type="[Agent] Session End"),
        ]
        conversion = {
            "source_format": "mapped-columns",
            "mapping": MAPPING,
            "named_format": None,
            "content_mode": "full",
            "strict_essentials": True,
            "redact_pii": True,
            "custom_redaction_patterns": (),
        }
        delivery = {
            "api_key": None,
            "server_zone": "US",
            "chunk_size": 100,
            "max_request_bytes": 1_000_000,
            "max_retries": 0,
            "initial_backoff_seconds": 0,
            "request_timeout_seconds": 1,
            "dry_run": True,
        }
        stats = list(process_partition(0, rows, conversion, delivery))[0]
        self.assertEqual(
            {
                "missing_identity": 1,
                "invalid_event_type": 1,
                "invalid_timestamp": 1,
                "filtered_event": 1,
            },
            stats["skipped_by_reason"],
        )
        self.assertEqual(0, stats["records_converted"])

    def test_accepts_structured_conversion_outcome(self):
        class _Outcome:
            records = ()
            skipped_by_reason = {"missing_session_id": 2}

        original = agent_traces_job.convert_record
        agent_traces_job.convert_record = lambda record, config: _Outcome()
        try:
            records, skipped = agent_traces_job._conversion_outcome({}, mapped_config())
        finally:
            agent_traces_job.convert_record = original
        self.assertEqual([], records)
        self.assertEqual({"missing_session_id": 2}, skipped)


class DryRunTests(unittest.TestCase):
    def _dry_run(self, rows, extra_args=()):
        args, unknown = parse_args(
            [
                "--table",
                "catalog.schema.table",
                "--format",
                "mapped-columns",
                "--mapping-json",
                json.dumps(MAPPING),
                "--watermark-column",
                "updated_at",
                "--watermark-start",
                "2026-01-01",
                "--watermark-end",
                "2026-02-01",
                "--dry-run",
                *extra_args,
            ]
        )
        self.data_frame = _FakeDataFrame(rows, ("updated_at",))
        spark = _FakeSpark(self.data_frame)
        self.spark = spark
        with unittest.mock.patch.dict(sys.modules, fake_pyspark_modules()):
            return run(args, unknown_args=unknown, spark=spark)

    def test_dry_run_reports_stats_and_never_advances_watermark(self):
        case = MappedColumnsTests()
        case.setUp()
        posted = []

        def fail_on_post(*call_args, **call_kwargs):
            posted.append(call_args)
            raise AssertionError("dry run must not POST")

        original = agent_traces_job.urllib.request.urlopen
        agent_traces_job.urllib.request.urlopen = fail_on_post
        try:
            result = self._dry_run([case.row, dict(case.row, identity={})])
        finally:
            agent_traces_job.urllib.request.urlopen = original

        self.assertEqual([], posted)
        self.assertEqual("dry_run", result["status"])
        self.assertTrue(result["dry_run"])
        self.assertFalse(result["watermark"]["advanced"])
        self.assertEqual(
            {
                "column": "updated_at",
                "start_exclusive": "2026-01-01",
                "end_inclusive": "2026-02-01",
                "advanced": False,
            },
            result["watermark"],
        )
        stats = result["stats"]
        self.assertEqual(2, stats["rows_read"])
        self.assertEqual(1, stats["converted"])
        self.assertEqual({"missing_identity": 1}, stats["skipped_by_reason"])
        self.assertEqual(0, stats["requests_sent"])
        self.assertEqual(0, stats["bytes_sent"])
        self.assertEqual(1, stats["requests_would_send"])
        self.assertGreater(stats["bytes_would_send"], 0)
        # Watermark bounds only filtered the read; nothing was written back.
        self.assertEqual(2, len(self.data_frame.filters))

    def test_converter_is_distributed_before_session_selection_udf(self):
        case = MappedColumnsTests()
        case.setUp()
        original = agent_traces_job._apply_session_selection

        def assert_converter_ready(data_frame, args, conversion_values):
            self.assertTrue(self.spark.sparkContext.py_files)
            self.assertTrue(
                self.spark.sparkContext.py_files[0].endswith(
                    "agent_traces_convert.py"
                )
            )
            return data_frame, agent_traces_job._unselected_session_stats(None)

        agent_traces_job._apply_session_selection = assert_converter_ready
        try:
            result = self._dry_run(
                [case.row], extra_args=("--max-sessions", "200")
            )
        finally:
            agent_traces_job._apply_session_selection = original
        self.assertEqual("dry_run", result["status"])


if __name__ == "__main__":
    unittest.main()
