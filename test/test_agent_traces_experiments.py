"""AA-152586: experiment assignment through the agent traces converter.

``fixtures/experiment_cases.json`` is a byte-for-byte copy of the Amplitude AI
SDK's ``langley/amplitude_ai/tests/golden/experiment_cases.json``; the cases
whose levels hold only ``call`` apply to one warehouse row.
"""

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
    experiment_properties,
    sanitize_experiments,
)

_GOLDEN = os.path.join(os.path.dirname(__file__), "fixtures", "experiment_cases.json")

MAPPING = {
    "event_type": "$.type",
    "user_id": "$.identity.user",
    "time": "$.timestamp",
    "event_properties": {
        "[Agent] Agent ID": "$.agent_id",
        "[Agent] Trace ID": "$.trace_id",
        "[Agent] Span ID": "$.span_id",
        "[Agent] Session ID": "$.session_id",
        "experiments": "$.experiments",
        "experiment_key": "$.experiment_key",
        "experiment_variant": "$.experiment_variant",
    },
}


def _expand(value):
    if isinstance(value, dict):
        if "$repeat" in value:
            return value["$repeat"] * value["count"]
        if "$concat" in value:
            return "".join(_expand(part) for part in value["$concat"])
        return {key: _expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    return value


def _call_only_cases():
    with open(_GOLDEN, encoding="utf-8") as handle:
        cases = json.load(handle)["cases"]
    for case in cases:
        if set(case["levels"]) == {"call"}:
            yield case["name"], _expand(case["levels"]["call"]), _expand(case["expected"])


def _span(record):
    return record.payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]


def _evaluations(record):
    return [
        {item["key"]: item["value"]["stringValue"] for item in event["attributes"]}
        for event in _span(record).get("events", [])
        if event["name"] == "feature_flag.evaluation"
    ]


class GoldenCaseTests(unittest.TestCase):
    def test_call_level_cases_match_the_sdk_rules(self):
        names = []
        for name, call, expected in _call_only_cases():
            names.append(name)
            with self.subTest(name=name):
                self.assertEqual(expected, experiment_properties(sanitize_experiments(call)))
        self.assertGreaterEqual(len(names), 25)


class MappedExperimentTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "type": "[Agent] AI Response",
            "identity": {"user": "user-1"},
            "timestamp": 1_735_689_600_000,
            "agent_id": "agent-1",
            "trace_id": "a" * 32,
            "span_id": "b" * 16,
            "session_id": "session-1",
        }

    def convert(self, content_mode=ContentMode.FULL, **fields):
        config = ConversionConfig(
            source_format=SourceFormat.MAPPED_COLUMNS,
            mapping=MAPPING,
            content_mode=content_mode,
        )
        row = dict(self.row, **fields)
        return convert_record(row, config)[0]

    def test_experiments_list_becomes_one_evaluation_each(self):
        record = self.convert(
            experiments=[
                {"key": "prompt", "variant": "v2", "provider": "statsig"},
                {"key": "model", "variant": "small", "unit": "turn"},
            ]
        )
        self.assertEqual(
            [
                {
                    "feature_flag.key": "prompt",
                    "feature_flag.result.variant": "v2",
                    "amplitude.experiment.unit": "agent_session",
                    "feature_flag.provider.name": "statsig",
                },
                {
                    "feature_flag.key": "model",
                    "feature_flag.result.variant": "small",
                    "amplitude.experiment.unit": "turn",
                },
            ],
            _evaluations(record),
        )

    def test_json_text_experiments_column(self):
        record = self.convert(experiments=json.dumps([{"key": "prompt", "variant": "v2"}]))
        self.assertEqual(["prompt"], [item["feature_flag.key"] for item in _evaluations(record)])

    def test_key_and_variant_columns_override_the_list(self):
        record = self.convert(
            experiments=[{"key": "prompt", "variant": "v1"}, {"key": "model", "variant": "small"}],
            experiment_key="prompt",
            experiment_variant="v2",
        )
        self.assertEqual(
            [("prompt", "v2"), ("model", "small")],
            [(item["feature_flag.key"], item["feature_flag.result.variant"]) for item in _evaluations(record)],
        )

    def test_kept_unredacted_in_every_content_mode(self):
        for mode in ContentMode:
            with self.subTest(mode=mode.value):
                record = self.convert(mode, experiment_key="signup_flow", experiment_variant="buyer@example.com")
                self.assertEqual(
                    [("signup_flow", "buyer@example.com")],
                    [(item["feature_flag.key"], item["feature_flag.result.variant"]) for item in _evaluations(record)],
                )

    def test_invalid_rows_add_no_events_and_no_attributes(self):
        record = self.convert(experiment_key="a:b", experiment_variant="v", experiments="not json")
        self.assertNotIn("events", _span(record))
        keys = {item["key"] for item in _span(record)["attributes"]}
        self.assertFalse({"experiments", "experiment_key", "experiment_variant"} & keys)

    def test_eleventh_experiment_dropped(self):
        record = self.convert(experiments=[{"key": "e{}".format(index), "variant": "v"} for index in range(11)])
        self.assertEqual(10, len(_evaluations(record)))


if __name__ == "__main__":
    unittest.main()
