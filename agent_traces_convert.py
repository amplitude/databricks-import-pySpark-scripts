"""Pure conversion boundary for the Databricks agent traces import job.

This module intentionally depends only on the Python standard library.  Falcon's
shared conversion package can replace it later as long as it preserves the
``convert_record`` contract documented at the bottom of this file.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


class ConversionError(ValueError):
    """A source record cannot be converted without losing required identity.

    ``reason`` is a stable machine-readable bucket so the job can report
    skip counts per cause.  Falcon's shared converter may later return a
    structured outcome object instead of raising; both shapes carry the same
    reason vocabulary documented in ``SKIP_REASONS``.
    """

    def __init__(self, message: str, reason: str = "invalid_record") -> None:
        super().__init__(message)
        self.reason = reason


SKIP_REASONS = (
    "missing_session_id",
    "missing_identity",
    "invalid_event_type",
    "invalid_timestamp",
    "invalid_record",
    "filtered_event",
)


class SourceFormat(str, enum.Enum):
    MAPPED_COLUMNS = "mapped-columns"
    MLFLOW_UC = "mlflow-uc"


class ContentMode(str, enum.Enum):
    METADATA_ONLY = "metadata_only"
    FULL = "full"


class Protocol(str, enum.Enum):
    HTTP_V2 = "http-v2"
    OTLP_JSON = "otlp-json"


@dataclasses.dataclass(frozen=True)
class ConversionConfig:
    source_format: SourceFormat
    mapping: Optional[Mapping[str, Any]] = None
    named_format: Optional[str] = None
    content_mode: ContentMode = ContentMode.FULL
    strict_essentials: bool = True
    redact_pii: bool = True
    custom_redaction_patterns: Tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class ConvertedRecord:
    protocol: Protocol
    payload: Mapping[str, Any]
    stable_key: str


_MISSING = object()
_AGENT_ID = "[Agent] Agent ID"
_TRACE_ID = "[Agent] Trace ID"
_SPAN_ID = "[Agent] Span ID"
_SESSION_ID = "[Agent] Session ID"
_SESSION_END = "[Agent] Session End"
_IDENTITY_KEYS = frozenset(
    {
        "user_id",
        "userid",
        "device_id",
        "deviceid",
        "enduser.id",
        "amplitude.session.id",
        "gen_ai.conversation.id",
        "session_id",
        "conversation_id",
        "[agent] session id",
        "[agent] agent id",
        "[agent] trace id",
        "[agent] span id",
        "trace_id",
        "span_id",
        "parent_span_id",
        "parentspanid",
        "insert_id",
    }
)
_IDENTITY_KEY_SUFFIXES = frozenset(
    {
        "session_id",
        "conversation_id",
        "user_id",
        "userid",
        "device_id",
        "deviceid",
    }
)

_SENSITIVE_AGENT_PROPERTIES = {
    "$llm_message",
    "[Agent] Attachments",
    "[Agent] Input State",
    "[Agent] Output State",
    "[Agent] Reasoning Content",
    "[Agent] System Prompt",
    "[Agent] Tool Definitions",
    "[Agent] Tool Input",
    "[Agent] Tool Output",
}
_SENSITIVE_KEY_PARTS = (
    "attachment",
    "content",
    "completion",
    "input.value",
    "input_state",
    "inputs",
    "message",
    "output.value",
    "output_state",
    "outputs",
    "prompt",
    "reasoning",
    "request",
    "response",
    "tool.arguments",
    "tool.result",
)
_REDACTED_BASE64 = "[base64 image redacted]"
_BUILTIN_REDACTIONS = (
    (r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[email]"),
    (r"(?<!\d)\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})(?!\d)", "[phone]"),
    (r"\b(?:\d{4}[-\s]?){3}\d{4}\b", "[credit_card]"),
    (r"\b\d{3}(?:-| )\d{2}(?:-| )\d{4}\b", "[ssn]"),
    (r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "[ip_address]"),
    (
        r"(?<=//)\[::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}\]"
        r"|(?<=//)\[::1\]"
        r"|\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b"
        r"|\b(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}\b"
        r"|(?<![^\s])::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}\b"
        r"|(?<![^\s])::1\b",
        "[ip_address]",
    ),
    (r"(?<!\w)\+[1-9]\d{6,14}\b", "[phone]"),
)


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    return str(value)


def normalize(value: Any) -> Any:
    """Convert Spark Row/date/bytes values into JSON-compatible values."""
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    if isinstance(value, Mapping):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        normalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )


def validate_redaction_patterns(patterns: Sequence[str]) -> Tuple[str, ...]:
    """Validate customer regexes once on the driver before Spark execution."""
    output = []
    for pattern in patterns:
        if not isinstance(pattern, str):
            raise ConversionError("custom redaction patterns must be strings")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConversionError(
                "invalid custom redaction regex {!r}: {}".format(pattern, exc)
            )
        output.append(pattern)
    return tuple(output)


def _is_raw_base64(text: str) -> bool:
    if len(text) <= 20 or len(text) % 4 != 0:
        return False
    if not re.search(r"[+/=]", text):
        return False
    return bool(re.match(r"^[A-Za-z0-9+/]+=*$", text))


def _redact_text(
    text: str, redact_pii: bool, custom_patterns: Sequence[str]
) -> str:
    if redact_pii:
        if re.match(r"^data:[^;]+;base64,", text) or _is_raw_base64(text):
            return _REDACTED_BASE64
        for pattern, replacement in _BUILTIN_REDACTIONS:
            text = re.sub(pattern, replacement, text)
    for pattern in custom_patterns:
        text = re.sub(pattern, "[REDACTED]", text)
    return text


def _is_identity_key(name: str) -> bool:
    lowered = name.lower()
    if lowered in _IDENTITY_KEYS or lowered.endswith("enduser.id"):
        return True
    return lowered.rsplit(".", 1)[-1] in _IDENTITY_KEY_SUFFIXES


def _redact_content(
    value: Any, redact_pii: bool, custom_patterns: Sequence[str]
) -> Any:
    if isinstance(value, str):
        return _redact_text(value, redact_pii, custom_patterns)
    if isinstance(value, Mapping):
        return {
            key: item
            if _is_identity_key(str(key))
            else _redact_content(item, redact_pii, custom_patterns)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _redact_content(item, redact_pii, custom_patterns) for item in value
        ]
    return value


def stable_insert_id(event: Mapping[str, Any]) -> str:
    """Return a deterministic insert_id for replay-safe HTTP V2 imports."""
    material = dict(event)
    material.pop("insert_id", None)
    digest = hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()
    # Amplitude HTTP V2 caps insert_id at 64 characters.
    return "dbx-agent-" + digest[:54]


_PATH_TOKEN = re.compile(
    r"""
    (?:
      ^\$
      | \.([A-Za-z_][A-Za-z0-9_\-]*)
      | \[['"]([^'"]+)['"]\]
      | \[(\d+|\*)\]
    )
    """,
    re.VERBOSE,
)


def _parse_json_path(path: str) -> List[Any]:
    if not isinstance(path, str) or not path.startswith("$"):
        raise ConversionError("JSONPath must start with '$': {!r}".format(path))
    tokens: List[Any] = []
    position = 0
    for match in _PATH_TOKEN.finditer(path):
        if match.start() != position:
            raise ConversionError("unsupported JSONPath syntax: {!r}".format(path))
        position = match.end()
        if match.group(1) is not None:
            tokens.append(match.group(1))
        elif match.group(2) is not None:
            tokens.append(match.group(2))
        elif match.group(3) is not None:
            token = match.group(3)
            tokens.append("*" if token == "*" else int(token))
    if position != len(path):
        raise ConversionError("unsupported JSONPath syntax: {!r}".format(path))
    return tokens


def json_path_get(document: Any, path: str, default: Any = _MISSING) -> Any:
    """Evaluate the deterministic JSONPath subset used by mapping definitions.

    Supported syntax: ``$``, ``.name``, ``['name']``, ``[index]`` and ``[*]``.
    A wildcard returns a flattened list of all matching descendants.
    """
    current = [normalize(document)]
    wildcard_seen = False
    try:
        for token in _parse_json_path(path):
            next_values: List[Any] = []
            for value in current:
                if token == "*":
                    wildcard_seen = True
                    if isinstance(value, Mapping):
                        next_values.extend(value.values())
                    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                        next_values.extend(value)
                    continue
                if isinstance(token, int):
                    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                        if 0 <= token < len(value):
                            next_values.append(value[token])
                elif isinstance(value, Mapping) and token in value:
                    next_values.append(value[token])
            current = next_values
            if not current:
                raise KeyError(path)
    except (IndexError, KeyError):
        if default is _MISSING:
            raise ConversionError("JSONPath did not match: {}".format(path))
        return default
    return current if wildcard_seen else current[0]


def _resolve(spec: Any, row: Mapping[str, Any]) -> Any:
    """Resolve JSONPath/literal/object mapping nodes recursively."""
    if isinstance(spec, str) and spec.startswith("$"):
        return json_path_get(row, spec, None)
    if isinstance(spec, Mapping):
        if set(spec) == {"$literal"}:
            return normalize(spec["$literal"])
        if "$path" in spec:
            value = json_path_get(row, str(spec["$path"]), spec.get("default"))
            if value is None and "required" in spec and bool(spec["required"]):
                raise ConversionError("required mapping path is empty: {}".format(spec["$path"]))
            return value
        return {str(key): _resolve(value, row) for key, value in spec.items()}
    if isinstance(spec, list):
        return [_resolve(value, row) for value in spec]
    return normalize(spec)


def _drop_none(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _drop_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_drop_none(item) for item in value if item is not None]
    return value


def _metadata_only_event(event: Dict[str, Any]) -> None:
    properties = event.get("event_properties")
    if isinstance(properties, Mapping):
        event["event_properties"] = {
            key: value
            for key, value in properties.items()
            if key not in _SENSITIVE_AGENT_PROPERTIES
        }


def _validate_http_event(event: Mapping[str, Any], strict: bool) -> None:
    event_type = event.get("event_type")
    if not isinstance(event_type, str) or not event_type.startswith("[Agent] "):
        raise ConversionError(
            "event_type must be a canonical '[Agent] ...' event",
            reason="invalid_event_type",
        )
    if event_type == _SESSION_END:
        raise ConversionError(
            "[Agent] Session End must not be imported", reason="invalid_event_type"
        )
    if not event.get("user_id") and not event.get("device_id"):
        raise ConversionError(
            "HTTP V2 event requires user_id or device_id", reason="missing_identity"
        )
    if not strict:
        return
    properties = event.get("event_properties")
    if not isinstance(properties, Mapping):
        raise ConversionError("strict essentials require event_properties")
    for key in (_AGENT_ID, _TRACE_ID):
        if not properties.get(key):
            raise ConversionError("strict essentials require {!r}".format(key))
    if event_type in ("[Agent] Span", "[Agent] Tool Call") and not properties.get(_SPAN_ID):
        raise ConversionError("{} requires {!r}".format(event_type, _SPAN_ID))


def _convert_mapped(row: Mapping[str, Any], config: ConversionConfig) -> List[ConvertedRecord]:
    if not config.mapping:
        raise ConversionError("mapped-columns format requires a mapping")
    event = _drop_none(_resolve(config.mapping, row))
    if not isinstance(event, dict):
        raise ConversionError("mapping must resolve to an HTTP V2 event object")
    # Session End is SDK lifecycle output, not a source trace/span.  Imports must
    # neither synthesize nor replay it.
    if event.get("event_type") == _SESSION_END:
        return []
    if "time" in event:
        event["time"] = _http_v2_time(event["time"])
    if config.named_format:
        properties = event.setdefault("event_properties", {})
        if isinstance(properties, dict):
            properties.setdefault("[Agent] Source Format", config.named_format)
    if config.content_mode == ContentMode.METADATA_ONLY:
        _metadata_only_event(event)
    elif config.content_mode == ContentMode.FULL:
        properties = event.get("event_properties")
        if isinstance(properties, dict):
            for key in _SENSITIVE_AGENT_PROPERTIES:
                if key in properties:
                    properties[key] = _redact_content(
                        properties[key],
                        config.redact_pii,
                        config.custom_redaction_patterns,
                    )
    _validate_http_event(event, config.strict_essentials)
    event.setdefault("insert_id", stable_insert_id(event))
    return [
        ConvertedRecord(
            protocol=Protocol.HTTP_V2,
            payload=event,
            stable_key=str(event["insert_id"]),
        )
    ]


def _parse_json_container(value: Any, field_name: str, default: Any) -> Any:
    if value is None:
        return default
    value = normalize(value)
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError as exc:
            raise ConversionError("{} contains invalid JSON: {}".format(field_name, exc))
    return value


def _to_hex_id(value: Any, byte_length: int, field_name: str) -> str:
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).hex()
    text = str(value or "").replace("-", "").lower()
    required_length = byte_length * 2
    if len(text) != required_length or not re.match(r"^[0-9a-f]+$", text):
        raise ConversionError(
            "{} must be a {}-character hexadecimal value".format(field_name, required_length)
        )
    return text


def _http_v2_time(value: Any, field_name: str = "time") -> int:
    if value is None:
        raise ConversionError(
            "{} is required".format(field_name), reason="invalid_timestamp"
        )
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if re.match(r"^-?\d+$", text):
        return int(text)
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ConversionError(
            "{} is not a timestamp: {!r}".format(field_name, value),
            reason="invalid_timestamp",
        )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp() * 1000)


def _unix_nanos(value: Any, field_name: str) -> str:
    if value is None:
        raise ConversionError(
            "{} is required".format(field_name), reason="invalid_timestamp"
        )
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return str(int(value.timestamp() * 1_000_000_000))
    if isinstance(value, (int, float)):
        numeric = int(value)
        # MLflow schemas have used seconds, milliseconds, microseconds and nanos.
        magnitude = abs(numeric)
        if magnitude < 100_000_000_000:
            numeric *= 1_000_000_000
        elif magnitude < 100_000_000_000_000:
            numeric *= 1_000_000
        elif magnitude < 100_000_000_000_000_000:
            numeric *= 1_000
        return str(numeric)
    text = str(value).strip()
    if re.match(r"^-?\d+$", text):
        numeric = int(text)
        magnitude = abs(numeric)
        if magnitude < 100_000_000_000:
            numeric *= 1_000_000_000
        elif magnitude < 100_000_000_000_000:
            numeric *= 1_000_000
        elif magnitude < 100_000_000_000_000_000:
            numeric *= 1_000
        return str(numeric)
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ConversionError(
            "{} is not a timestamp: {!r}".format(field_name, value),
            reason="invalid_timestamp",
        )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return str(int(parsed.timestamp() * 1_000_000_000))


def _otlp_any_value(value: Any) -> Mapping[str, Any]:
    value = normalize(value)
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, (bytes, bytearray)):
        return {"bytesValue": bytes(value).hex()}
    if isinstance(value, Mapping):
        return {
            "kvlistValue": {
                "values": [
                    {"key": str(key), "value": _otlp_any_value(item)}
                    for key, item in value.items()
                    if item is not None
                ]
            }
        }
    if isinstance(value, Sequence):
        return {"arrayValue": {"values": [_otlp_any_value(item) for item in value]}}
    return {"stringValue": str(value)}


def _is_sensitive_attribute(name: str) -> bool:
    lowered = name.lower()
    segments = re.split(r"[.\[\]]+", lowered)
    for part in _SENSITIVE_KEY_PARTS:
        if part in ("request", "response"):
            if segments and segments[-1] == part:
                return True
            continue
        if "." in part:
            part_segments = part.split(".")
            for index in range(len(segments) - len(part_segments) + 1):
                if segments[index : index + len(part_segments)] == part_segments:
                    return True
            continue
        if any(part == segment for segment in segments):
            return True
    return False


def _otlp_attributes(
    attributes: Any,
    content_mode: ContentMode,
    redact_pii: bool = True,
    custom_patterns: Sequence[str] = (),
) -> List[Mapping[str, Any]]:
    attributes = _parse_json_container(attributes, "attributes", {})
    if isinstance(attributes, list):
        # Already-encoded OTLP attributes are accepted, but metadata filtering
        # still applies based on their key.
        output = []
        for attribute in attributes:
            if not isinstance(attribute, Mapping) or "key" not in attribute:
                raise ConversionError("OTLP attribute entries require key/value")
            if content_mode == ContentMode.METADATA_ONLY and _is_sensitive_attribute(
                str(attribute["key"])
            ):
                continue
            normalized = normalize(attribute)
            if (
                content_mode == ContentMode.FULL
                and "value" in normalized
                and not _is_identity_key(str(attribute["key"]))
            ):
                normalized["value"] = _redact_content(
                    normalized["value"], redact_pii, custom_patterns
                )
            output.append(normalized)
        return output
    if not isinstance(attributes, Mapping):
        raise ConversionError("attributes must be an object or OTLP attribute list")
    return [
        {
            "key": str(key),
            "value": _otlp_any_value(
                value
                if content_mode != ContentMode.FULL or _is_identity_key(str(key))
                else _redact_content(value, redact_pii, custom_patterns)
            ),
        }
        for key, value in attributes.items()
        if value is not None
        and not (
            content_mode == ContentMode.METADATA_ONLY
            and _is_sensitive_attribute(str(key))
        )
    ]


def _status(status: Any) -> Mapping[str, Any]:
    status = _parse_json_container(status, "status", {})
    if isinstance(status, str):
        status = {"code": status}
    if not isinstance(status, Mapping):
        return {}
    code = status.get("code", status.get("status_code", "STATUS_CODE_UNSET"))
    if isinstance(code, int):
        code = {
            0: "STATUS_CODE_UNSET",
            1: "STATUS_CODE_OK",
            2: "STATUS_CODE_ERROR",
        }.get(code, "STATUS_CODE_UNSET")
    code = str(code).upper()
    if not code.startswith("STATUS_CODE_"):
        code = {
            "OK": "STATUS_CODE_OK",
            "ERROR": "STATUS_CODE_ERROR",
            "UNSET": "STATUS_CODE_UNSET",
        }.get(code, "STATUS_CODE_UNSET")
    result: Dict[str, Any] = {"code": code}
    message = status.get("message", status.get("description"))
    if message:
        result["message"] = str(message)
    return result


def _convert_span(
    raw_span: Mapping[str, Any],
    fallback_trace_id: Any,
    config: ConversionConfig,
) -> Mapping[str, Any]:
    span = normalize(raw_span)
    trace_id = span.get("trace_id", span.get("traceId", fallback_trace_id))
    span_id = span.get("span_id", span.get("spanId"))
    parent_id = span.get("parent_span_id", span.get("parentSpanId", span.get("parent_id")))
    start = span.get(
        "start_time_unix_nano",
        span.get("startTimeUnixNano", span.get("start_time")),
    )
    end = span.get(
        "end_time_unix_nano",
        span.get("endTimeUnixNano", span.get("end_time")),
    )
    converted: Dict[str, Any] = {
        "traceId": _to_hex_id(trace_id, 16, "trace_id"),
        "spanId": _to_hex_id(span_id, 8, "span_id"),
        "name": str(span.get("name") or span.get("span_name") or "mlflow.span"),
        "kind": str(span.get("kind", "SPAN_KIND_INTERNAL")).upper(),
        "startTimeUnixNano": _unix_nanos(start, "span start time"),
        "endTimeUnixNano": _unix_nanos(end, "span end time"),
        "attributes": _otlp_attributes(
            span.get("attributes", {}),
            config.content_mode,
            config.redact_pii,
            config.custom_redaction_patterns,
        ),
        "status": _status(span.get("status", {})),
    }
    if parent_id:
        converted["parentSpanId"] = _to_hex_id(parent_id, 8, "parent_span_id")
    events = span.get("events")
    if events:
        parsed_events = _parse_json_container(events, "events", [])
        if isinstance(parsed_events, list):
            converted["events"] = [
                normalize(
                    {
                        "name": str(event.get("name", "")),
                        "timeUnixNano": _unix_nanos(
                            event.get(
                                "time_unix_nano",
                                event.get("timeUnixNano", event.get("time")),
                            ),
                            "event time",
                        ),
                        "attributes": _otlp_attributes(
                            event.get("attributes", {}),
                            config.content_mode,
                            config.redact_pii,
                            config.custom_redaction_patterns,
                        ),
                    }
                )
                for event in parsed_events
                if isinstance(event, Mapping)
            ]
    return converted


def _resource_attributes(
    row: Mapping[str, Any], config: ConversionConfig
) -> List[Mapping[str, Any]]:
    metadata = _parse_json_container(
        row.get("trace_metadata", row.get("traceMetadata")), "trace_metadata", {}
    )
    tags = _parse_json_container(row.get("tags"), "tags", {})
    attributes: Dict[str, Any] = {}
    if isinstance(metadata, Mapping):
        attributes.update({"mlflow.trace.{}".format(k): v for k, v in metadata.items()})
    if isinstance(tags, Mapping):
        attributes.update({"mlflow.tag.{}".format(k): v for k, v in tags.items()})
    attributes.setdefault("service.name", row.get("service_name", "mlflow-unity-catalog"))
    if config.content_mode == ContentMode.FULL:
        if row.get("request") is not None:
            attributes["mlflow.trace.request"] = row.get("request")
        if row.get("response") is not None:
            attributes["mlflow.trace.response"] = row.get("response")
    return _otlp_attributes(
        attributes,
        config.content_mode,
        config.redact_pii,
        config.custom_redaction_patterns,
    )


def _convert_mlflow(row: Mapping[str, Any], config: ConversionConfig) -> List[ConvertedRecord]:
    trace_id = row.get("trace_id", row.get("traceId"))
    trace_hex = _to_hex_id(trace_id, 16, "trace_id")
    spans = _parse_json_container(row.get("spans"), "spans", [])
    if not isinstance(spans, list) or not spans:
        raise ConversionError("mlflow-uc record requires a non-empty spans array")
    converted_spans = [
        _convert_span(span, trace_hex, config)
        for span in spans
        if isinstance(span, Mapping)
    ]
    if len(converted_spans) != len(spans):
        raise ConversionError("every spans entry must be an object")
    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": _resource_attributes(row, config)
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": config.named_format or "mlflow-unity-catalog"
                        },
                        "spans": converted_spans,
                    }
                ],
            }
        ]
    }
    return [
        ConvertedRecord(
            protocol=Protocol.OTLP_JSON,
            payload=payload,
            stable_key=trace_hex,
        )
    ]


def convert_record(
    record: Mapping[str, Any], config: ConversionConfig
) -> List[ConvertedRecord]:
    """Convert one source row into zero or more transport envelopes.

    Contract:
      * input and output are JSON-compatible mappings;
      * conversion is deterministic and has no I/O;
      * ``stable_key`` is replay-stable;
      * output protocol is either HTTP V2 or OTLP/HTTP JSON;
      * ``[Agent] Session End`` is never produced.
    """
    row = normalize(record)
    if not isinstance(row, Mapping):
        raise ConversionError("record must be an object")
    if config.source_format == SourceFormat.MAPPED_COLUMNS:
        return _convert_mapped(row, config)
    if config.source_format == SourceFormat.MLFLOW_UC:
        return _convert_mlflow(row, config)
    raise ConversionError("unsupported source format: {}".format(config.source_format))


def _canonical_session_value(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (Mapping, list, tuple)):
        return None
    text = str(value).strip()
    return text or None


def canonical_session_id(
    record: Mapping[str, Any], config: ConversionConfig
) -> Optional[str]:
    """Resolve the canonical conversation/session key without converting content.

    This uses the same mapping and MLflow container parsing as ``convert_record``.
    Request and trace IDs are intentionally never considered because they can
    vary across rows belonging to one conversation.
    """
    row = normalize(record)
    if not isinstance(row, Mapping):
        return None
    if config.source_format == SourceFormat.MAPPED_COLUMNS:
        if not config.mapping:
            return None
        event = _drop_none(_resolve(config.mapping, row))
        if not isinstance(event, Mapping):
            return None
        properties = event.get("event_properties")
        if not isinstance(properties, Mapping):
            return None
        return _canonical_session_value(properties.get(_SESSION_ID))
    if config.source_format == SourceFormat.MLFLOW_UC:
        metadata = _parse_json_container(
            row.get("trace_metadata", row.get("traceMetadata")),
            "trace_metadata",
            {},
        )
        tags = _parse_json_container(row.get("tags"), "tags", {})
        for container in (row, metadata, tags):
            if not isinstance(container, Mapping):
                continue
            for key in ("conversation_id", "session_id"):
                session_id = _canonical_session_value(container.get(key))
                if session_id is not None:
                    return session_id
        return None
    return None


def combine_payloads(
    protocol: Protocol, records: Iterable[ConvertedRecord]
) -> Mapping[str, Any]:
    """Combine same-protocol envelopes into one executor HTTP request."""
    records = list(records)
    if protocol == Protocol.HTTP_V2:
        return {"events": [record.payload for record in records]}
    resource_spans: List[Any] = []
    for record in records:
        resource_spans.extend(record.payload.get("resourceSpans", []))
    return {"resourceSpans": resource_spans}
