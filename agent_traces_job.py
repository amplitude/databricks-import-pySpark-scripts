"""Databricks SparkPythonTask for direct agent-trace import to Amplitude.

Rows stay in the customer's Databricks account.  Spark executors convert and
POST them directly to Amplitude; this job never stages data in S3 or Cargo.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import email.utils
import hashlib
import json
import os
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from agent_traces_convert import (
    ContentMode,
    ConversionConfig,
    ConversionError,
    ConvertedRecord,
    Protocol,
    SourceFormat,
    canonical_session_id,
    combine_payloads,
    convert_record,
    validate_redaction_patterns,
)


HTTP_V2_ENDPOINTS = {
    "US": "https://api2.amplitude.com/2/httpapi",
    "EU": "https://api.eu.amplitude.com/2/httpapi",
}
OTLP_ENDPOINTS = {
    "US": "https://api2.amplitude.com/v1/traces",
    "EU": "https://api.eu.amplitude.com/v1/traces",
}
RESULT_PREFIX = "AGENT_TRACES_JOB_RESULT="
MAX_MAPPING_BYTES = 1_048_576


@dataclasses.dataclass(frozen=True)
class DeliveryConfig:
    api_key: Optional[str]
    server_zone: str
    chunk_size: int
    max_request_bytes: int
    max_retries: int
    initial_backoff_seconds: float
    request_timeout_seconds: float
    dry_run: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import Databricks agent traces directly into Amplitude"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--table", help="Unity Catalog or Hive table name")
    source.add_argument("--view", help="Unity Catalog or Hive view name")
    source.add_argument("--path", help="DBFS/cloud data path readable by Spark")
    parser.add_argument(
        "--path-format",
        choices=("delta", "parquet", "json"),
        default="delta",
        help="Spark reader format when --path is used",
    )
    parser.add_argument(
        "--format",
        dest="source_format",
        choices=tuple(item.value for item in SourceFormat),
        required=True,
        help="Source conversion format",
    )
    parser.add_argument(
        "--named-format",
        help=(
            "Stable producer format name. Passed through to the conversion "
            "boundary for future Falcon-managed named converters."
        ),
    )
    mapping = parser.add_mutually_exclusive_group()
    mapping.add_argument("--mapping-json", help="Inline mapped-columns JSON mapping")
    mapping.add_argument(
        "--mapping-json-path",
        help="Local or dbutils-readable path containing mapped-columns JSON",
    )
    parser.add_argument("--watermark-column")
    parser.add_argument("--watermark-start", help="Exclusive lower watermark bound")
    parser.add_argument("--watermark-end", help="Inclusive upper watermark bound")
    parser.add_argument(
        "--content-mode",
        choices=tuple(item.value for item in ContentMode),
        default=ContentMode.FULL.value,
        help="Content export mode (default: full with PII redaction)",
    )
    pii = parser.add_mutually_exclusive_group()
    pii.add_argument(
        "--redact-pii",
        dest="redact_pii",
        action="store_true",
        default=True,
        help="Redact built-in PII patterns in full content mode (default)",
    )
    pii.add_argument(
        "--no-redact-pii",
        dest="redact_pii",
        action="store_false",
        help="Disable built-in PII redaction",
    )
    parser.add_argument(
        "--custom-redaction-patterns-json",
        help="JSON list of additional regex patterns replaced with [REDACTED]",
    )
    parser.add_argument(
        "--sample-rate",
        type=float,
        default=1.0,
        help="Session-stable sampling rate in (0, 1] (default: 1)",
    )
    parser.add_argument(
        "--max-sessions",
        type=int,
        help="Optional deterministic cap on selected sessions",
    )
    parser.add_argument(
        "--session-id-column",
        help=(
            "Optional top-level conversation/session ID column override. When "
            "omitted, the converter derives the canonical session key."
        ),
    )
    parser.add_argument(
        "--no-strict-essentials",
        dest="strict_essentials",
        action="store_false",
        default=True,
        help="Disable canonical event identity validation (migration escape hatch)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--protocol",
        choices=tuple(item.value for item in Protocol),
        help="Delivery protocol override (default inferred from source format)",
    )
    parser.add_argument(
        "--result-path",
        help="Optional dbutils/local path for the machine-readable run result",
    )
    parser.add_argument("--secret-scope", help="Databricks secret scope")
    parser.add_argument(
        "--api-key-secret-key",
        help="Name of the API-key secret in --secret-scope (never the API key)",
    )
    parser.add_argument("--server-zone", choices=("US", "EU"), default="US")
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument("--max-request-bytes", type=int, default=1_000_000)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--initial-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> Tuple[argparse.Namespace, List[str]]:
    """Parse job arguments while tolerating Databricks-injected arguments."""
    args, unknown = build_parser().parse_known_args(argv)
    if bool(args.watermark_start or args.watermark_end) and not args.watermark_column:
        raise ValueError("watermark bounds require --watermark-column")
    if args.watermark_column and not (args.watermark_start or args.watermark_end):
        raise ValueError("--watermark-column requires at least one watermark bound")
    if args.source_format == SourceFormat.MAPPED_COLUMNS.value and not (
        args.mapping_json or args.mapping_json_path
    ):
        raise ValueError("mapped-columns requires --mapping-json or --mapping-json-path")
    if not args.dry_run and not (args.secret_scope and args.api_key_secret_key):
        raise ValueError(
            "delivery requires --secret-scope and --api-key-secret-key; "
            "the API key itself must never be passed on the command line"
        )
    for field in ("chunk_size", "max_request_bytes"):
        if getattr(args, field) <= 0:
            raise ValueError("--{} must be positive".format(field.replace("_", "-")))
    if args.max_retries < 0:
        raise ValueError("--max-retries cannot be negative")
    if args.initial_backoff_seconds < 0 or args.request_timeout_seconds <= 0:
        raise ValueError("backoff must be non-negative and timeout must be positive")
    if not 0 < args.sample_rate <= 1:
        raise ValueError("--sample-rate must be in (0, 1]")
    if args.max_sessions is not None and args.max_sessions <= 0:
        raise ValueError("--max-sessions must be positive")
    _parse_custom_redaction_patterns(args.custom_redaction_patterns_json)
    return args, unknown


def _parse_custom_redaction_patterns(raw: Optional[str]) -> Tuple[str, ...]:
    if not raw:
        return ()
    try:
        patterns = json.loads(raw)
    except ValueError as exc:
        raise ValueError("invalid custom redaction patterns JSON: {}".format(exc))
    if not isinstance(patterns, list):
        raise ValueError("custom redaction patterns JSON must be a list")
    try:
        return validate_redaction_patterns(patterns)
    except ConversionError as exc:
        raise ValueError(str(exc))


def _load_mapping(args: argparse.Namespace, dbutils: Any = None) -> Optional[Mapping[str, Any]]:
    if args.mapping_json:
        raw = args.mapping_json
    elif args.mapping_json_path:
        path = args.mapping_json_path
        if path.startswith(("dbfs:", "s3:", "s3a:", "abfss:")):
            if dbutils is None:
                raise ValueError("dbutils is required to read mapping path {}".format(path))
            raw = dbutils.fs.head(path, MAX_MAPPING_BYTES)
        else:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read(MAX_MAPPING_BYTES + 1)
        if len(raw.encode("utf-8")) > MAX_MAPPING_BYTES:
            raise ValueError("mapping JSON exceeds {} bytes".format(MAX_MAPPING_BYTES))
    else:
        return None
    try:
        mapping = json.loads(raw)
    except ValueError as exc:
        raise ValueError("invalid mapping JSON: {}".format(exc))
    if not isinstance(mapping, Mapping):
        raise ValueError("mapping JSON must be an object")
    return mapping


def _read_source(spark: Any, args: argparse.Namespace) -> Any:
    if args.table:
        return spark.table(args.table)
    if args.view:
        return spark.table(args.view)
    return spark.read.format(args.path_format).load(args.path)


def _apply_watermarks(data_frame: Any, args: argparse.Namespace) -> Any:
    if not args.watermark_column:
        return data_frame
    try:
        field = next(
            item for item in data_frame.schema.fields if item.name == args.watermark_column
        )
    except StopIteration:
        raise ValueError(
            "watermark column {!r} is not in the source schema".format(
                args.watermark_column
            )
        )
    from pyspark.sql import functions as functions

    column = functions.col(args.watermark_column)
    if args.watermark_start is not None:
        lower = functions.lit(args.watermark_start).cast(field.dataType)
        data_frame = data_frame.filter(column > lower)
    if args.watermark_end is not None:
        upper = functions.lit(args.watermark_end).cast(field.dataType)
        data_frame = data_frame.filter(column <= upper)
    return data_frame


# 13 hex digits of SHA-256 fit in a signed 64-bit integer, which is the widest
# value Spark's conv() can return, and give the driver and executors identical
# buckets.
_SESSION_HASH_HEX_DIGITS = 13
_SESSION_HASH_SPACE = 16 ** _SESSION_HASH_HEX_DIGITS
_SESSION_KEY = "__agent_session_id"


def _resolve_session_id_column(data_frame: Any, args: argparse.Namespace) -> Optional[str]:
    fields = {field.name for field in data_frame.schema.fields}
    if args.session_id_column:
        if args.session_id_column not in fields:
            raise ValueError(
                "session ID column {!r} is not in the source schema".format(
                    args.session_id_column
                )
            )
        return args.session_id_column
    return None


def _conversion_config(values: Mapping[str, Any]) -> ConversionConfig:
    return ConversionConfig(
        source_format=SourceFormat(values["source_format"]),
        mapping=values.get("mapping"),
        named_format=values.get("named_format"),
        content_mode=ContentMode(values["content_mode"]),
        strict_essentials=bool(values["strict_essentials"]),
        redact_pii=bool(values["redact_pii"]),
        custom_redaction_patterns=tuple(values.get("custom_redaction_patterns", ())),
    )


def derive_session_id(
    record: Mapping[str, Any], conversion_values: Mapping[str, Any]
) -> Optional[str]:
    """Resolve one row's canonical session key through the converter contract."""
    try:
        return canonical_session_id(record, _conversion_config(conversion_values))
    except ConversionError:
        return None


def session_hash(session_id: Any) -> str:
    """Return the conversation hash used for both sampling and cap ordering."""
    return hashlib.sha256(str(session_id).strip().encode("utf-8")).hexdigest()


def sampling_threshold(sample_rate: float) -> int:
    if not 0 < sample_rate <= 1:
        raise ValueError("sample_rate must be in (0, 1]")
    return int(sample_rate * _SESSION_HASH_SPACE) - 1


def session_is_sampled(session_id: Any, sample_rate: float) -> bool:
    """Mirror the Spark SHA-256 threshold used for stable session sampling."""
    threshold = sampling_threshold(sample_rate)
    if session_id is None or str(session_id).strip() == "":
        return False
    if sample_rate == 1:
        return True
    bucket = int(session_hash(session_id)[:_SESSION_HASH_HEX_DIGITS], 16)
    return bucket <= threshold


def select_sessions(
    session_ids: Iterable[Any],
    sample_rate: float,
    max_sessions: Optional[int] = None,
) -> List[str]:
    """Return the conversations to import, deterministically and whole.

    Mirrors the Spark expressions in :func:`_apply_session_selection` so the
    same conversation set is chosen regardless of row order, partitioning, or
    run count.  Selection is per conversation, never per row, so a chosen
    conversation keeps all of its rows.
    """
    unique = sorted(
        {
            str(session_id).strip()
            for session_id in session_ids
            if session_id is not None and str(session_id).strip()
        }
    )
    sampled = [
        session_id
        for session_id in unique
        if session_is_sampled(session_id, sample_rate)
    ]
    if max_sessions is None:
        return sampled
    return sorted(sampled, key=session_hash)[:max_sessions]


def session_skip_counts(
    rows_before: int,
    rows_missing_session: int,
    rows_after_sampling: int,
    rows_after_cap: int,
) -> Dict[str, int]:
    """Build row-level skip buckets for distributed session selection."""
    skipped: Dict[str, int] = {}
    if rows_missing_session:
        skipped["missing_session_id"] = rows_missing_session
    sampled_out = rows_before - rows_missing_session - rows_after_sampling
    if sampled_out:
        skipped["sampled_out"] = sampled_out
    capped_out = rows_after_sampling - rows_after_cap
    if capped_out:
        skipped["max_sessions"] = capped_out
    return skipped


def _unselected_session_stats(session_column: Optional[str]) -> Dict[str, Any]:
    return {
        "session_id_column": session_column,
        "sessions_seen": None,
        "sessions_sampled": None,
        "sessions_kept": None,
        "rows_before_selection": None,
        "rows_after_selection": None,
        "skipped_by_reason": {},
    }


def _apply_session_selection(
    data_frame: Any,
    args: argparse.Namespace,
    conversion_values: Mapping[str, Any],
) -> Tuple[Any, Dict[str, Any]]:
    """Apply deterministic whole-conversation sampling/capping in Spark.

    Both sampling and the session cap decide per conversation and then join
    those conversations back to the rows, so a selected conversation keeps
    every row and a dropped one keeps none.
    """
    session_column = _resolve_session_id_column(data_frame, args)
    controls_active = args.sample_rate < 1 or args.max_sessions is not None
    if not controls_active:
        return data_frame, _unselected_session_stats(session_column)

    from pyspark.sql import functions

    if session_column is not None:
        data_frame = data_frame.withColumn(
            _SESSION_KEY,
            functions.trim(functions.col(session_column).cast("string")),
        )
    else:
        source_columns = [
            functions.col(field.name) for field in data_frame.schema.fields
        ]
        session_conversion = _conversion_config(conversion_values)

        def resolve_session(row: Any) -> Optional[str]:
            source = row.asDict(recursive=True) if hasattr(row, "asDict") else row
            try:
                return canonical_session_id(source, session_conversion)
            except ConversionError:
                return None

        session_udf = functions.udf(resolve_session, "string")
        data_frame = data_frame.withColumn(
            _SESSION_KEY,
            functions.trim(session_udf(functions.struct(*source_columns))),
        )

    session_key = functions.col(_SESSION_KEY)
    has_session = session_key.isNotNull() & (functions.length(session_key) > 0)
    session_digest = functions.sha2(session_key, 256)

    data_frame = data_frame.cache()
    rows_before = data_frame.count()
    rows_missing_session = data_frame.where(~has_session).count()
    with_session = data_frame.where(has_session)
    sessions = with_session.select(_SESSION_KEY).distinct()
    sessions_seen = sessions.count()

    if args.sample_rate < 1:
        bucket = functions.conv(
            functions.substring(session_digest, 1, _SESSION_HASH_HEX_DIGITS), 16, 10
        ).cast("long")
        sampled_sessions = sessions.where(bucket <= sampling_threshold(args.sample_rate))
    else:
        sampled_sessions = sessions
    sampled_sessions = sampled_sessions.cache()
    sessions_sampled = sampled_sessions.count()

    if args.max_sessions is not None:
        # Ordering by the full digest is a total order over distinct
        # conversation IDs, so the same conversations survive the cap on every
        # run regardless of partitioning or row order.
        kept_sessions = sampled_sessions.orderBy(session_digest).limit(
            args.max_sessions
        )
    else:
        kept_sessions = sampled_sessions
    kept_sessions = kept_sessions.cache()
    sessions_kept = kept_sessions.count()

    join_sessions = (
        functions.broadcast(kept_sessions)
        if args.max_sessions is not None
        else kept_sessions
    )
    selected = with_session.join(join_sessions, _SESSION_KEY, "inner").drop(
        _SESSION_KEY
    )
    rows_after = selected.count()

    if args.sample_rate < 1 and args.max_sessions is not None:
        rows_after_sampling = (
            with_session.join(
                sampled_sessions, _SESSION_KEY, "inner"
            )
            .drop(_SESSION_KEY)
            .count()
        )
    elif args.sample_rate < 1:
        rows_after_sampling = rows_after
    else:
        rows_after_sampling = rows_before - rows_missing_session

    skipped = session_skip_counts(
        rows_before, rows_missing_session, rows_after_sampling, rows_after
    )
    return selected, {
        "session_id_column": session_column,
        "sessions_seen": sessions_seen,
        "sessions_sampled": sessions_sampled,
        "sessions_kept": sessions_kept,
        "rows_before_selection": rows_before,
        "rows_after_selection": rows_after,
        "skipped_by_reason": skipped,
    }


def _retry_after_seconds(headers: Any) -> Optional[float]:
    value = headers.get("Retry-After") if headers else None
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=dt.timezone.utc)
            return max(0.0, (when - dt.datetime.now(dt.timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            return None


def _post_json(
    url: str,
    body: Mapping[str, Any],
    headers: Mapping[str, str],
    config: DeliveryConfig,
    sleep: Any = time.sleep,
) -> Tuple[int, int]:
    encoded = json.dumps(
        body, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    request_headers = {"Content-Type": "application/json"}
    request_headers.update(headers)
    attempts = 0
    while True:
        attempts += 1
        request = urllib.request.Request(
            url, data=encoded, headers=request_headers, method="POST"
        )
        try:
            with urllib.request.urlopen(  # nosemgrep: ssrf-outbound-http-requires-safe-wrapper
                request, timeout=config.request_timeout_seconds
            ) as response:
                status = int(response.status)
                response.read()
            if 200 <= status < 300:
                return len(encoded), attempts
            error_status = status
            retry_after = None
        except urllib.error.HTTPError as exc:
            error_status = int(exc.code)
            retry_after = _retry_after_seconds(exc.headers)
            # Consume and discard the bounded service error response.  Never log
            # the request body because HTTP V2 contains the API key.
            exc.read(64 * 1024)
        except (urllib.error.URLError, TimeoutError, OSError):
            error_status = 0
            retry_after = None

        retryable = error_status in (0, 408, 425, 429) or 500 <= error_status < 600
        if not retryable or attempts > config.max_retries:
            raise RuntimeError(
                "Amplitude request failed after {} attempt(s), status={}".format(
                    attempts, error_status or "network-error"
                )
            )
        delay = (
            retry_after
            if retry_after is not None
            else config.initial_backoff_seconds
            * (2 ** (attempts - 1))
            * random.uniform(0.8, 1.2)
        )
        sleep(delay)


def _request_parts(
    protocol: Protocol,
    records: List[ConvertedRecord],
    delivery: DeliveryConfig,
) -> Tuple[str, Mapping[str, Any], Mapping[str, str]]:
    body = dict(combine_payloads(protocol, records))
    if protocol == Protocol.HTTP_V2:
        # HTTP V2 authenticates in the body.  The body is never logged.
        body["api_key"] = delivery.api_key
        return HTTP_V2_ENDPOINTS[delivery.server_zone], body, {}
    return (
        OTLP_ENDPOINTS[delivery.server_zone],
        body,
        {
            "Authorization": "Api-Key {}".format(delivery.api_key),
            "x-api-key": str(delivery.api_key),
        },
    )


def _deliver_chunk(
    records: List[ConvertedRecord], delivery: DeliveryConfig
) -> Tuple[int, int]:
    if not records:
        return 0, 0
    protocol = records[0].protocol
    if any(record.protocol != protocol for record in records):
        raise ValueError("a delivery chunk cannot mix protocols")
    url, body, headers = _request_parts(protocol, records, delivery)
    if delivery.dry_run:
        encoded = json.dumps(
            body, separators=(",", ":"), ensure_ascii=False, default=str
        ).encode("utf-8")
        return len(encoded), 0
    return _post_json(url, body, headers, delivery)


def _would_exceed_bytes(
    records: List[ConvertedRecord],
    candidate: ConvertedRecord,
    delivery: DeliveryConfig,
) -> bool:
    if not records:
        return False
    _, body, _ = _request_parts(records[0].protocol, records + [candidate], delivery)
    size = len(
        json.dumps(body, separators=(",", ":"), default=str).encode("utf-8")
    )
    return size > delivery.max_request_bytes


def _conversion_outcome(
    record: Mapping[str, Any], conversion: ConversionConfig
) -> Tuple[List[ConvertedRecord], Dict[str, int]]:
    """Return converted records plus per-reason skip counts for one source row.

    Falcon's shared converter is moving to a structured ``ConversionOutcome``
    that reports skips instead of raising.  Accept either shape here so the job
    keeps working through that switch: an object exposing ``records`` and
    ``skipped_by_reason``, or today's list-plus-``ConversionError`` contract.
    """
    try:
        outcome = convert_record(record, conversion)
    except ConversionError as exc:
        return [], {getattr(exc, "reason", None) or "invalid_record": 1}
    records = getattr(outcome, "records", outcome)
    skipped = getattr(outcome, "skipped_by_reason", None) or {}
    counts = {str(reason): int(count) for reason, count in dict(skipped).items()}
    if not records and not counts:
        counts["filtered_event"] = 1
    return list(records), counts


def process_partition(
    partition_id: int,
    rows: Iterable[Any],
    conversion_values: Mapping[str, Any],
    delivery_values: Mapping[str, Any],
) -> Iterator[Mapping[str, int]]:
    """Convert and deliver one Spark partition, yielding one small stats row."""
    conversion = _conversion_config(conversion_values)
    protocol_override = delivery_values.get("protocol")
    if protocol_override is not None:
        protocol_override = Protocol(protocol_override)
    delivery = DeliveryConfig(
        **{
            key: value
            for key, value in delivery_values.items()
            if key != "protocol"
        }
    )
    stats = {
        "partition_id": partition_id,
        "rows_read": 0,
        "records_converted": 0,
        "records_filtered": 0,
        "requests_sent": 0,
        "bytes_sent": 0,
        "requests_would_send": 0,
        "bytes_would_send": 0,
        "attempts": 0,
        "skipped_by_reason": {},
    }
    pending: List[ConvertedRecord] = []

    def flush() -> None:
        if not pending:
            return
        sent_bytes, attempts = _deliver_chunk(pending, delivery)
        if delivery.dry_run:
            stats["requests_would_send"] += 1
            stats["bytes_would_send"] += sent_bytes
        else:
            stats["requests_sent"] += 1
            stats["bytes_sent"] += sent_bytes
            stats["attempts"] += attempts
        pending[:] = []

    for row in rows:
        stats["rows_read"] += 1
        source = row.asDict(recursive=True) if hasattr(row, "asDict") else row
        converted, skipped = _conversion_outcome(source, conversion)
        for reason, count in skipped.items():
            stats["skipped_by_reason"][reason] = (
                stats["skipped_by_reason"].get(reason, 0) + count
            )
            stats["records_filtered"] += count
        if not converted:
            continue
        stats["records_converted"] += len(converted)
        for record in converted:
            if protocol_override is not None:
                record = dataclasses.replace(record, protocol=protocol_override)
            if pending and (
                pending[0].protocol != record.protocol
                or len(pending) >= delivery.chunk_size
                or _would_exceed_bytes(pending, record, delivery)
            ):
                flush()
            pending.append(record)
            # Reject a single over-sized record explicitly instead of relying on
            # the intake service's less actionable 413.
            _, single_body, _ = _request_parts(record.protocol, [record], delivery)
            single_size = len(
                json.dumps(
                    single_body,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    default=str,
                ).encode("utf-8")
            )
            if single_size > delivery.max_request_bytes:
                raise ValueError(
                    "converted record {} is {} bytes, exceeding max-request-bytes {}".format(
                        record.stable_key, single_size, delivery.max_request_bytes
                    )
                )
    flush()
    yield stats


def _sum_stats(partition_stats: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    counters = (
        "rows_read",
        "records_converted",
        "records_filtered",
        "requests_sent",
        "bytes_sent",
        "requests_would_send",
        "bytes_would_send",
        "attempts",
    )
    totals: Dict[str, Any] = {"partitions": 0}
    totals.update({key: 0 for key in counters})
    skipped: Dict[str, int] = {}
    for item in partition_stats:
        totals["partitions"] += 1
        for key in counters:
            totals[key] += int(item.get(key, 0))
        for reason, count in dict(item.get("skipped_by_reason") or {}).items():
            skipped[str(reason)] = skipped.get(str(reason), 0) + int(count)
    totals["skipped_by_reason"] = skipped
    return totals


def _source_descriptor(args: argparse.Namespace) -> Mapping[str, str]:
    if args.table:
        return {"kind": "table", "value": args.table}
    if args.view:
        return {"kind": "view", "value": args.view}
    return {"kind": "path", "value": args.path, "path_format": args.path_format}


def _write_result(result: Mapping[str, Any], path: Optional[str], dbutils: Any) -> None:
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"))
    print(RESULT_PREFIX + encoded)
    if not path:
        return
    if path.startswith(("dbfs:", "s3:", "s3a:", "abfss:")):
        dbutils.fs.put(path, encoded, overwrite=True)
    else:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(encoded)


def run(
    args: argparse.Namespace,
    unknown_args: Optional[Sequence[str]] = None,
    spark: Any = None,
    dbutils: Any = None,
) -> Mapping[str, Any]:
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    if dbutils is None:
        dbutils = globals().get("dbutils")
    if dbutils is None and (
        not args.dry_run
        or (
            args.mapping_json_path
            and args.mapping_json_path.startswith(("dbfs:", "s3:", "s3a:", "abfss:"))
        )
        or (
            args.result_path
            and args.result_path.startswith(("dbfs:", "s3:", "s3a:", "abfss:"))
        )
    ):
        raise RuntimeError("dbutils is unavailable")

    mapping = _load_mapping(args, dbutils)
    # Session-key derivation and conversion both run on executors, so distribute
    # the converter before constructing or evaluating the session UDF.
    converter_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "agent_traces_convert.py"
    )
    spark.sparkContext.addPyFile(converter_path)

    conversion_values = {
        "source_format": args.source_format,
        "mapping": mapping,
        "named_format": args.named_format,
        "content_mode": args.content_mode,
        "strict_essentials": args.strict_essentials,
        "redact_pii": args.redact_pii,
        "custom_redaction_patterns": _parse_custom_redaction_patterns(
            args.custom_redaction_patterns_json
        ),
    }
    data_frame = _apply_watermarks(_read_source(spark, args), args)
    data_frame, session_stats = _apply_session_selection(
        data_frame, args, conversion_values
    )

    api_key = None
    if not args.dry_run:
        api_key = dbutils.secrets.get(
            scope=args.secret_scope, key=args.api_key_secret_key
        )
        if not api_key:
            raise RuntimeError("Databricks secret returned an empty API key")

    delivery_values = dataclasses.asdict(
        DeliveryConfig(
            api_key=api_key,
            server_zone=args.server_zone,
            chunk_size=args.chunk_size,
            max_request_bytes=args.max_request_bytes,
            max_retries=args.max_retries,
            initial_backoff_seconds=args.initial_backoff_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
            dry_run=args.dry_run,
        )
    )
    if args.protocol is not None:
        delivery_values["protocol"] = args.protocol
    started = dt.datetime.now(dt.timezone.utc)
    partition_stats = data_frame.rdd.mapPartitionsWithIndex(
        lambda partition_id, rows: process_partition(
            partition_id, rows, conversion_values, delivery_values
        )
    ).collect()
    completed = dt.datetime.now(dt.timezone.utc)
    totals = _sum_stats(partition_stats)
    if session_stats["rows_before_selection"] is not None:
        totals["rows_read"] = int(session_stats["rows_before_selection"])
    totals["sessions_seen"] = session_stats["sessions_seen"]
    totals["sessions_sampled"] = session_stats["sessions_sampled"]
    totals["sessions_kept"] = session_stats["sessions_kept"]
    totals["converted"] = totals["records_converted"]
    skipped_by_reason = dict(totals["skipped_by_reason"])
    for reason, count in session_stats["skipped_by_reason"].items():
        skipped_by_reason[reason] = skipped_by_reason.get(reason, 0) + count
    totals["skipped_by_reason"] = skipped_by_reason
    result: Dict[str, Any] = {
        "status": "dry_run" if args.dry_run else "succeeded",
        "dry_run": bool(args.dry_run),
        "source": _source_descriptor(args),
        "format": args.source_format,
        "named_format": args.named_format,
        "content_mode": args.content_mode,
        "redact_pii": bool(args.redact_pii),
        "custom_redaction_pattern_count": len(
            _parse_custom_redaction_patterns(args.custom_redaction_patterns_json)
        ),
        "sample_rate": args.sample_rate,
        "max_sessions": args.max_sessions,
        "session_id_column": session_stats["session_id_column"],
        "strict_essentials": bool(args.strict_essentials),
        "server_zone": args.server_zone,
        # Watermark bounds only filter the read. This job never stores or
        # advances a watermark, in dry-run or in delivery mode; the caller owns
        # persistence and must not persist anything from a dry run.
        "watermark": {
            "column": args.watermark_column,
            "start_exclusive": args.watermark_start,
            "end_inclusive": args.watermark_end,
            "advanced": False,
        },
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "duration_ms": int((completed - started).total_seconds() * 1000),
        "stats": totals,
        "ignored_argument_count": len(unknown_args or ()),
    }
    _write_result(result, args.result_path, dbutils)
    return result


def main(argv: Optional[Sequence[str]] = None) -> Mapping[str, Any]:
    args, unknown = parse_args(argv)
    return run(args, unknown_args=unknown)


if __name__ == "__main__":
    main()
