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
import math
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
    session_id_column: Optional[str] = None
    user_id_column: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class ConvertedRecord:
    protocol: Protocol
    payload: Mapping[str, Any]
    stable_key: str


_MISSING = object()


def _first_present(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    """Return the first non-None alias.

    Spark ``asDict()`` and JSON spans often include null keys.  ``dict.get``
    treats those as hits, which would skip later aliases and fallbacks.
    """
    if not isinstance(mapping, Mapping):
        return default
    for key in keys:
        if key not in mapping:
            continue
        value = mapping[key]
        if value is not None:
            return value
    return default


_AGENT_ID = "[Agent] Agent ID"
_TRACE_ID = "[Agent] Trace ID"
_SPAN_ID = "[Agent] Span ID"
_SESSION_ID = "[Agent] Session ID"
_SESSION_END = "[Agent] Session End"
_USER_MESSAGE = "[Agent] User Message"
_AI_RESPONSE = "[Agent] AI Response"
_TOOL_CALL = "[Agent] Tool Call"
_SPAN = "[Agent] Span"
# Must stay in sync with Langley's _mlflow_session_id so sampling groups rows by
# the same key the converter ultimately assigns as the session.
_MLFLOW_SESSION_KEYS = (
    "session_id",
    "sessionId",
    "conversation_id",
    "conversationId",
    "amplitude.session.id",
    "amplitude.session_id",
    "gen_ai.conversation.id",
    "gen_ai.session.id",
    "session.id",
    "mlflow.trace.session",
)
# OtlpTraceTranslator reads these in order; emit all three so mapped and
# MLflow rows resolve the same Amplitude session.
_RECEIVER_SESSION_KEYS = (
    "gen_ai.conversation.id",
    "session.id",
    "amplitude.session_id",
)


def _apply_session_attributes(attributes: Dict[str, Any], session_id: Any) -> None:
    if session_id is None:
        return
    for key in _RECEIVER_SESSION_KEYS:
        attributes[key] = session_id


_IDENTITY_KEYS = frozenset(
    {
        "user_id",
        "userid",
        "device_id",
        "deviceid",
        "enduser.id",
        "amplitude.session.id",
        "amplitude.session_id",
        "gen_ai.conversation.id",
        "session.id",
        "session_id",
        "sessionid",
        "conversation_id",
        "conversationid",
        "mlflow.trace.session",
        "mlflow.trace.user",
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
        "sessionid",
        "conversation_id",
        "conversationid",
        "user_id",
        "userid",
        "device_id",
        "deviceid",
    }
)
# _resource_attributes re-prefixes metadata and tags, so MLflow's own identity
# tags arrive as mlflow.trace.* / mlflow.tag.* and are matched on the full name
# rather than a bare "session"/"user" suffix, which would also exempt content.
_IDENTITY_KEY_ENDINGS = ("enduser.id", "mlflow.trace.session", "mlflow.trace.user")

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
_SECRET_KEY_PARTS = frozenset(
    ("api_key", "apikey", "authorization", "credential", "password", "secret", "token")
)
_METADATA_ATTRIBUTE_ALLOWLIST = frozenset(
    {
        "gen_ai.operation.name",
        "gen_ai.provider.name",
        "gen_ai.system",
        "gen_ai.request.model",
        "gen_ai.response.model",
        "gen_ai.response.id",
        "gen_ai.response.finish_reason",
        "gen_ai.response.finish_reasons",
        "gen_ai.usage.input_tokens",
        "gen_ai.usage.output_tokens",
        "gen_ai.usage.reasoning.output_tokens",
        "gen_ai.usage.cache_read.input_tokens",
        "gen_ai.usage.cache_creation.input_tokens",
        "gen_ai.usage.cost",
        "gen_ai.conversation.id",
        "session.id",
        "amplitude.session_id",
        "enduser.id",
        "gen_ai.agent.id",
        "deployment.environment",
        "service.name",
        "openinference.span.kind",
        "llm.model_name",
        "llm.token_count.prompt",
        "llm.token_count.completion",
        "llm.system",
        "mlflow.spanType",
    }
)
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_REDACTED_BASE64 = "[base64 image redacted]"
_BUILTIN_REDACTIONS = (
    (r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}", "Bearer [secret]"),
    (r"\b(?:sk|rk|pk)-[A-Za-z0-9_-]{12,}\b", "[secret]"),
    (r"\bAKIA[0-9A-Z]{16}\b", "[secret]"),
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


def _binary_hex(value: Any) -> Optional[str]:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return None


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if _binary_hex(value) is not None:
        return _binary_hex(value)
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    return str(value)


def normalize(value: Any) -> Any:
    """Convert Spark Row/date/bytes values into JSON-compatible values.

    Naive datetimes are treated as UTC. The job pins
    ``spark.sql.session.timeZone`` to UTC so ``Row.asDict()`` renders them that
    way regardless of cluster locale.
    """
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
    # NaN/Infinity from DoubleType columns are not valid JSON and would make
    # Amplitude reject the whole chunk, so drop them rather than emit them.
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if _binary_hex(value) is not None:
        return _binary_hex(value)
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
    if lowered in _IDENTITY_KEYS or lowered.endswith(_IDENTITY_KEY_ENDINGS):
        return True
    return lowered.rsplit(".", 1)[-1] in _IDENTITY_KEY_SUFFIXES


def _is_secret_key(name: str) -> bool:
    lowered = _CAMEL_BOUNDARY.sub("_", str(name)).lower()
    if lowered in _SECRET_KEY_PARTS:
        return True
    if any(lowered.endswith("_{}".format(part)) for part in _SECRET_KEY_PARTS):
        return True
    parts = set(re.split(r"[^a-z0-9]+", lowered))
    return bool(parts & _SECRET_KEY_PARTS)


def _redact_content(
    value: Any, redact_pii: bool, custom_patterns: Sequence[str]
) -> Any:
    if isinstance(value, str):
        return _redact_text(value, redact_pii, custom_patterns)
    if isinstance(value, Mapping):
        output = {}
        for key, item in value.items():
            key_text = str(key)
            if _is_identity_key(key_text):
                output[key] = item
            elif _is_secret_key(key_text):
                output[key] = "[secret]"
            else:
                output[key] = _redact_content(item, redact_pii, custom_patterns)
        return output
    if isinstance(value, list):
        return [
            _redact_content(item, redact_pii, custom_patterns) for item in value
        ]
    return value


def stable_insert_id(event: Mapping[str, Any]) -> str:
    """Return a deterministic replay key for mapped imports."""
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
            if _is_identity_key(str(key))
            or key
            in {
                _AGENT_ID,
                _TRACE_ID,
                _SPAN_ID,
                _SESSION_ID,
                "[Agent] Parent Span ID",
                "[Agent] Model Name",
                "[Agent] Model Provider",
                "[Agent] Input Tokens",
                "[Agent] Output Tokens",
                "[Agent] Latency Ms",
                "[Agent] Is Error",
                "[Agent] Tool Name",
                "[Agent] Invocation ID",
                "[Agent] Span Name",
                "[Agent] Environment",
                "[Agent] Source Format",
            }
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
    if not properties.get(_AGENT_ID):
        raise ConversionError("strict essentials require {!r}".format(_AGENT_ID))


def _derived_hex(value: Any, length: int, material: Any) -> str:
    if value is not None and str(value).strip():
        return _to_hex_id(value, length // 2, "identifier")
    return hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()[:length]


def _agent_message_text(value: Any) -> Any:
    if isinstance(value, Mapping) and "text" in value:
        return value["text"]
    return value


def _mapped_otlp_span(event: Mapping[str, Any], config: ConversionConfig) -> Mapping[str, Any]:
    properties = dict(event.get("event_properties") or {})
    event_type = event["event_type"]
    time_millis = _http_v2_time(event.get("time"))
    trace_material = {
        "session": properties.get(_SESSION_ID),
        "user": event.get("user_id") or event.get("device_id"),
        "event": event,
    }
    trace_id = _derived_hex(properties.get(_TRACE_ID), 32, trace_material)
    span_id = _derived_hex(properties.get(_SPAN_ID), 16, event)
    operation = {
        _USER_MESSAGE: "chat",
        _AI_RESPONSE: "chat",
        _TOOL_CALL: "execute_tool",
    }.get(event_type, "span")
    attrs: Dict[str, Any] = {"gen_ai.operation.name": operation}

    aliases = {
        _AGENT_ID: "gen_ai.agent.id",
        "[Agent] Model Name": "gen_ai.response.model",
        "[Agent] Model Provider": "gen_ai.provider.name",
        "[Agent] Input Tokens": "gen_ai.usage.input_tokens",
        "[Agent] Output Tokens": "gen_ai.usage.output_tokens",
        "[Agent] Reasoning Tokens": "gen_ai.usage.reasoning.output_tokens",
        "[Agent] Cache Read Tokens": "gen_ai.usage.cache_read.input_tokens",
        "[Agent] Cache Creation Tokens": "gen_ai.usage.cache_creation.input_tokens",
        "[Agent] Cost USD": "gen_ai.usage.cost",
        "[Agent] Provider Request ID": "gen_ai.response.id",
        "[Agent] Finish Reason": "gen_ai.response.finish_reason",
        "[Agent] System Prompt": "gen_ai.system_instructions",
        "[Agent] Tool Name": "gen_ai.tool.name",
        "[Agent] Invocation ID": "gen_ai.tool.call.id",
        "[Agent] Tool Input": "gen_ai.tool.call.arguments",
        "[Agent] Tool Output": "gen_ai.tool.call.result",
        "[Agent] Environment": "deployment.environment",
    }
    for source, target in aliases.items():
        if properties.get(source) is not None:
            attrs[target] = properties[source]
    _apply_session_attributes(attrs, properties.get(_SESSION_ID))
    identity = event.get("user_id") or event.get("device_id")
    if identity is not None:
        attrs["enduser.id"] = identity
    message = _agent_message_text(properties.get("$llm_message"))
    if message is not None:
        if event_type == _USER_MESSAGE:
            attrs["gen_ai.input.messages"] = [{"role": "user", "content": message}]
        elif event_type == _AI_RESPONSE:
            attrs["gen_ai.output.messages"] = [{"role": "assistant", "content": message}]
    if "gen_ai.input.messages" not in attrs and properties.get("[Agent] Input State") is not None:
        attrs["gen_ai.input.messages"] = properties["[Agent] Input State"]
    if "gen_ai.output.messages" not in attrs and properties.get("[Agent] Output State") is not None:
        attrs["gen_ai.output.messages"] = properties["[Agent] Output State"]
    if event_type == _SPAN and properties.get("[Agent] Span Name"):
        name = str(properties["[Agent] Span Name"])
    else:
        name = event_type

    status: Dict[str, Any] = {
        "code": "STATUS_CODE_ERROR"
        if properties.get("[Agent] Is Error")
        else "STATUS_CODE_OK"
    }
    if properties.get("[Agent] Error Message"):
        status["message"] = properties["[Agent] Error Message"]
    span: Dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": "SPAN_KIND_INTERNAL",
        "startTimeUnixNano": str(time_millis * 1_000_000),
        "endTimeUnixNano": str(
            time_millis * 1_000_000
            + max(1, int(float(properties.get("[Agent] Latency Ms", 0)) * 1_000_000))
        ),
        "attributes": _otlp_attributes(
            attrs,
            config.content_mode,
            config.redact_pii,
            config.custom_redaction_patterns,
        ),
        "status": status,
    }
    if properties.get("[Agent] Parent Span ID"):
        span["parentSpanId"] = _to_hex_id(
            properties["[Agent] Parent Span ID"], 8, "parent_span_id"
        )
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": []},
                "scopeSpans": [
                    {
                        "scope": {"name": config.named_format or "databricks-mapped-columns"},
                        "spans": [span],
                    }
                ],
            }
        ]
    }


def _convert_mapped(row: Mapping[str, Any], config: ConversionConfig) -> List[ConvertedRecord]:
    if not config.mapping:
        raise ConversionError("mapped-columns format requires a mapping")
    event = _drop_none(_resolve(config.mapping, row))
    if not isinstance(event, dict):
        raise ConversionError("mapping must resolve to an HTTP V2 event object")
    properties = event.setdefault("event_properties", {})
    if isinstance(properties, dict) and config.session_id_column:
        properties[_SESSION_ID] = row.get(config.session_id_column)
    if config.user_id_column:
        event["user_id"] = row.get(config.user_id_column)
    # Session End is SDK lifecycle output, not a source trace/span.  Imports must
    # neither synthesize nor replay it.
    if event.get("event_type") == _SESSION_END:
        return []
    if "time" in event:
        event["time"] = _http_v2_time(event["time"])
    if config.named_format:
        if isinstance(properties, dict):
            properties.setdefault("[Agent] Source Format", config.named_format)
    if config.content_mode == ContentMode.METADATA_ONLY:
        _metadata_only_event(event)
    else:
        properties = event.get("event_properties")
        if isinstance(properties, dict):
            event["event_properties"] = _redact_content(
                properties, config.redact_pii, config.custom_redaction_patterns
            )
    _validate_http_event(event, config.strict_essentials)
    stable_key = stable_insert_id(event)
    payload = _mapped_otlp_span(event, config)
    return [
        ConvertedRecord(
            protocol=Protocol.OTLP_JSON,
            payload=payload,
            stable_key=stable_key,
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
    binary = _binary_hex(value)
    if binary is not None:
        value = binary
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
        if isinstance(value, float) and not math.isfinite(value):
            raise ConversionError(
                "{} is not a timestamp: {!r}".format(field_name, value),
                reason="invalid_timestamp",
            )
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
        if isinstance(value, float) and not math.isfinite(value):
            raise ConversionError(
                "{} is not a timestamp: {!r}".format(field_name, value),
                reason="invalid_timestamp",
            )
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
    binary = _binary_hex(value)
    if binary is not None:
        return {"bytesValue": binary}
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
        return {"arrayValue": {"values": [_otlp_any_value(item) for item in value if item is not None]}}
    if value is None:
        return {}
    return {"stringValue": str(value)}


def _is_sensitive_attribute(name: str) -> bool:
    # MLflow writes camelCase content keys (mlflow.spanInputs / mlflow.spanOutputs),
    # so break case boundaries before matching whole segments. Segments are matched
    # exactly so metadata like gen_ai.usage.prompt_tokens survives metadata-only mode.
    lowered = _CAMEL_BOUNDARY.sub(".", name).lower()
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


def _metadata_attribute_allowed(name: str) -> bool:
    return (
        name in _METADATA_ATTRIBUTE_ALLOWLIST
        or _is_identity_key(name)
        or name.startswith("gen_ai.usage.")
    )


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
            if content_mode == ContentMode.METADATA_ONLY and not _metadata_attribute_allowed(
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
            "value": (
                {"stringValue": "[secret]"}
                if content_mode == ContentMode.FULL
                and _is_secret_key(str(key))
                and not _is_identity_key(str(key))
                else _otlp_any_value(
                    value
                    if content_mode != ContentMode.FULL or _is_identity_key(str(key))
                    else _redact_content(value, redact_pii, custom_patterns)
                )
            ),
        }
        for key, value in attributes.items()
        if value is not None
        and not (
            content_mode == ContentMode.METADATA_ONLY
            and not _metadata_attribute_allowed(str(key))
        )
    ]


_SPAN_KIND_BY_ORDINAL = {
    0: "SPAN_KIND_UNSPECIFIED",
    1: "SPAN_KIND_INTERNAL",
    2: "SPAN_KIND_SERVER",
    3: "SPAN_KIND_CLIENT",
    4: "SPAN_KIND_PRODUCER",
    5: "SPAN_KIND_CONSUMER",
}
_SPAN_KIND_NAMES = frozenset(_SPAN_KIND_BY_ORDINAL.values())


def _span_kind(kind: Any) -> str:
    if kind is None or kind == "":
        return "SPAN_KIND_INTERNAL"
    if isinstance(kind, bool):
        return "SPAN_KIND_INTERNAL"
    if isinstance(kind, int):
        return _SPAN_KIND_BY_ORDINAL.get(kind, "SPAN_KIND_INTERNAL")
    name = str(kind).strip().upper()
    if name.isdigit():
        return _SPAN_KIND_BY_ORDINAL.get(int(name), "SPAN_KIND_INTERNAL")
    if name in _SPAN_KIND_NAMES:
        return name
    prefixed = f"SPAN_KIND_{name}"
    return prefixed if prefixed in _SPAN_KIND_NAMES else "SPAN_KIND_INTERNAL"


def _status(status: Any, config: Optional[ConversionConfig] = None) -> Mapping[str, Any]:
    status = _parse_json_container(status, "status", {})
    if isinstance(status, str):
        status = {"code": status}
    if not isinstance(status, Mapping):
        return {}
    code = _first_present(status, "code", "status_code", default="STATUS_CODE_UNSET")
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
    message = _first_present(status, "message", "description")
    if message:
        message = str(message)
        # Error text routinely embeds user content, so it gets the same
        # treatment as span attributes rather than passing through raw.
        if config is not None:
            message = _redact_content(
                message, config.redact_pii, config.custom_redaction_patterns
            )
        result["message"] = message
    return result


def _convert_span(
    raw_span: Mapping[str, Any],
    fallback_trace_id: Any,
    config: ConversionConfig,
) -> Mapping[str, Any]:
    span = normalize(raw_span)
    trace_id = _first_present(span, "trace_id", "traceId", default=fallback_trace_id)
    span_id = _first_present(span, "span_id", "spanId")
    parent_id = _first_present(span, "parent_span_id", "parentSpanId", "parent_id")
    start = _first_present(
        span, "start_time_unix_nano", "startTimeUnixNano", "start_time"
    )
    end = _first_present(span, "end_time_unix_nano", "endTimeUnixNano", "end_time")
    raw_attributes = _parse_json_container(span.get("attributes", {}), "attributes", {})
    if not isinstance(raw_attributes, Mapping):
        raise ConversionError("span attributes must be an object")
    attributes = dict(raw_attributes)
    span_type = str(
        span.get("span_type")
        or span.get("spanType")
        or span.get("type")
        or attributes.get("mlflow.spanType")
        or attributes.get("openinference.span.kind")
        or ""
    ).upper()
    operation = {
        "LLM": "chat",
        "CHAT": "chat",
        "CHAT_MODEL": "chat",
        "TOOL": "execute_tool",
        "FUNCTION": "execute_tool",
    }.get(span_type, "span")
    attributes.setdefault("gen_ai.operation.name", operation)
    if span_type:
        attributes.setdefault("openinference.span.kind", span_type)

    raw_input = _first_present(span, "inputs", "input")
    if raw_input is None:
        raw_input = _first_present(attributes, "mlflow.spanInputs", "input.value")
    raw_output = _first_present(span, "outputs", "output")
    if raw_output is None:
        raw_output = _first_present(attributes, "mlflow.spanOutputs", "output.value")
    def message_value(value: Any, default_role: str) -> Any:
        if isinstance(value, Mapping):
            if "messages" in value:
                return value["messages"]
            if "content" in value:
                return [{"role": value.get("role", default_role), "content": value["content"]}]
        return value

    if raw_input is not None:
        parsed_input = _parse_json_container(raw_input, "span input", raw_input)
        if operation == "execute_tool":
            attributes.setdefault("gen_ai.tool.call.arguments", parsed_input)
        else:
            attributes.setdefault(
                "gen_ai.input.messages", message_value(parsed_input, "user")
            )
    if raw_output is not None:
        parsed_output = _parse_json_container(raw_output, "span output", raw_output)
        if operation == "execute_tool":
            attributes.setdefault("gen_ai.tool.call.result", parsed_output)
        else:
            attributes.setdefault(
                "gen_ai.output.messages", message_value(parsed_output, "assistant")
            )

    converted: Dict[str, Any] = {
        "traceId": _to_hex_id(trace_id, 16, "trace_id"),
        "spanId": _to_hex_id(span_id, 8, "span_id"),
        "name": str(_first_present(span, "name", "span_name") or "mlflow.span"),
        "kind": _span_kind(span.get("kind")),
        "startTimeUnixNano": _unix_nanos(start, "span start time"),
        "endTimeUnixNano": _unix_nanos(end, "span end time"),
        "attributes": _otlp_attributes(
            attributes,
            config.content_mode,
            config.redact_pii,
            config.custom_redaction_patterns,
        ),
        "status": _status(span.get("status", {}), config),
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
                            _first_present(
                                event, "time_unix_nano", "timeUnixNano", "time"
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


def _mlflow_session_value(
    row: Mapping[str, Any], metadata: Any, tags: Any
) -> Optional[str]:
    """Resolve the MLflow conversation key from the row, metadata, then tags.

    Shared by ``canonical_session_id`` and ``_resource_attributes`` so sampling
    groups by exactly the session Amplitude receives.
    """
    for container in (row, metadata, tags):
        if not isinstance(container, Mapping):
            continue
        for key in _MLFLOW_SESSION_KEYS:
            session_id = _canonical_session_value(container.get(key))
            if session_id is not None:
                return session_id
    return None


def _resource_attributes(
    row: Mapping[str, Any], config: ConversionConfig
) -> List[Mapping[str, Any]]:
    metadata = _parse_json_container(
        _first_present(row, "trace_metadata", "traceMetadata"), "trace_metadata", {}
    )
    tags = _parse_json_container(row.get("tags"), "tags", {})
    attributes: Dict[str, Any] = {}
    if isinstance(metadata, Mapping):
        attributes.update({"mlflow.trace.{}".format(k): v for k, v in metadata.items()})
    if isinstance(tags, Mapping):
        attributes.update({"mlflow.tag.{}".format(k): v for k, v in tags.items()})
    session_id = _canonical_session_value(row.get(config.session_id_column)) if config.session_id_column else None
    session_id = session_id or _mlflow_session_value(row, metadata, tags)
    _apply_session_attributes(attributes, session_id)
    user_id = _canonical_session_value(row.get(config.user_id_column)) if config.user_id_column else None
    if user_id is None:
        for container in (row, metadata, tags):
            if isinstance(container, Mapping):
                for key in ("user_id", "userId", "enduser.id", "mlflow.trace.user"):
                    user_id = _canonical_session_value(container.get(key))
                    if user_id is not None:
                        break
            if user_id is not None:
                break
    if user_id is not None:
        attributes["enduser.id"] = user_id
    attributes.setdefault(
        "service.name",
        _first_present(row, "service_name", default="mlflow-unity-catalog"),
    )
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
    trace_id = _first_present(row, "trace_id", "traceId")
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
    resource_attributes = _resource_attributes(row, config)
    records = []
    for index, converted_span in enumerate(converted_spans):
        payload = {
            "resourceSpans": [
                {
                    "resource": {"attributes": resource_attributes},
                    "scopeSpans": [
                        {
                            "scope": {
                                "name": config.named_format or "mlflow-unity-catalog"
                            },
                            "spans": [converted_span],
                        }
                    ],
                }
            ]
        }
        records.append(
            ConvertedRecord(
                protocol=Protocol.OTLP_JSON,
                payload=payload,
                stable_key="{}:{}".format(trace_hex, index),
            )
        )
    return records


def convert_record(
    record: Mapping[str, Any], config: ConversionConfig
) -> List[ConvertedRecord]:
    """Convert one source row into zero or more transport envelopes.

    Contract:
      * input and output are JSON-compatible mappings;
      * conversion is deterministic and has no I/O;
      * ``stable_key`` is replay-stable;
      * output protocol is OTLP/HTTP JSON;
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
        if config.session_id_column:
            return _canonical_session_value(row.get(config.session_id_column))
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
        if config.session_id_column:
            return _canonical_session_value(row.get(config.session_id_column))
        metadata = _parse_json_container(
            row.get("trace_metadata", row.get("traceMetadata")),
            "trace_metadata",
            {},
        )
        tags = _parse_json_container(row.get("tags"), "tags", {})
        return _mlflow_session_value(row, metadata, tags)
    return None


def combine_payloads(
    protocol: Protocol, records: Iterable[ConvertedRecord]
) -> Mapping[str, Any]:
    """Combine same-protocol envelopes into one executor HTTP request."""
    records = list(records)
    if protocol != Protocol.OTLP_JSON:
        raise ValueError("only OTLP JSON delivery is supported")
    resource_spans: List[Any] = []
    for record in records:
        resource_spans.extend(record.payload.get("resourceSpans", []))
    return {"resourceSpans": resource_spans}
