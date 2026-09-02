"""Databricks SparkPythonTask for direct agent-trace import to Amplitude.

Rows stay in the customer's Databricks account.  Spark executors convert and
POST them directly to Amplitude; this job never stages data in S3 or Cargo.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import email.utils
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
    ConvertedRecord,
    Protocol,
    SourceFormat,
    combine_payloads,
    convert_record,
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
        default=ContentMode.METADATA_ONLY.value,
        help="metadata_only is the privacy-safe default",
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
    return args, unknown


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
            with urllib.request.urlopen(
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
    if delivery.dry_run:
        return 0, 0
    url, body, headers = _request_parts(protocol, records, delivery)
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


def process_partition(
    partition_id: int,
    rows: Iterable[Any],
    conversion_values: Mapping[str, Any],
    delivery_values: Mapping[str, Any],
) -> Iterator[Mapping[str, int]]:
    """Convert and deliver one Spark partition, yielding one small stats row."""
    conversion = ConversionConfig(
        source_format=SourceFormat(conversion_values["source_format"]),
        mapping=conversion_values.get("mapping"),
        named_format=conversion_values.get("named_format"),
        content_mode=ContentMode(conversion_values["content_mode"]),
        strict_essentials=bool(conversion_values["strict_essentials"]),
    )
    delivery = DeliveryConfig(**delivery_values)
    stats = {
        "partition_id": partition_id,
        "rows_read": 0,
        "records_converted": 0,
        "records_filtered": 0,
        "requests_sent": 0,
        "bytes_sent": 0,
        "attempts": 0,
    }
    pending: List[ConvertedRecord] = []

    def flush() -> None:
        if not pending:
            return
        sent_bytes, attempts = _deliver_chunk(pending, delivery)
        if not delivery.dry_run:
            stats["requests_sent"] += 1
            stats["bytes_sent"] += sent_bytes
            stats["attempts"] += attempts
        pending[:] = []

    for row in rows:
        stats["rows_read"] += 1
        source = row.asDict(recursive=True) if hasattr(row, "asDict") else row
        converted = convert_record(source, conversion)
        if not converted:
            stats["records_filtered"] += 1
            continue
        stats["records_converted"] += len(converted)
        for record in converted:
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
                json.dumps(single_body, separators=(",", ":"), default=str).encode(
                    "utf-8"
                )
            )
            if single_size > delivery.max_request_bytes:
                raise ValueError(
                    "converted record {} is {} bytes, exceeding max-request-bytes {}".format(
                        record.stable_key, single_size, delivery.max_request_bytes
                    )
                )
    flush()
    yield stats


def _sum_stats(partition_stats: Iterable[Mapping[str, int]]) -> Dict[str, int]:
    totals = {
        "partitions": 0,
        "rows_read": 0,
        "records_converted": 0,
        "records_filtered": 0,
        "requests_sent": 0,
        "bytes_sent": 0,
        "attempts": 0,
    }
    for item in partition_stats:
        totals["partitions"] += 1
        for key in totals:
            if key != "partitions":
                totals[key] += int(item.get(key, 0))
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
        or args.mapping_json_path
        or (
            args.result_path
            and args.result_path.startswith(("dbfs:", "s3:", "s3a:", "abfss:"))
        )
    ):
        raise RuntimeError("dbutils is unavailable")

    mapping = _load_mapping(args, dbutils)
    data_frame = _apply_watermarks(_read_source(spark, args), args)

    # Make the pure converter importable by Python workers for GitSource jobs.
    converter_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "agent_traces_convert.py"
    )
    spark.sparkContext.addPyFile(converter_path)

    api_key = None
    if not args.dry_run:
        api_key = dbutils.secrets.get(
            scope=args.secret_scope, key=args.api_key_secret_key
        )
        if not api_key:
            raise RuntimeError("Databricks secret returned an empty API key")

    conversion_values = {
        "source_format": args.source_format,
        "mapping": mapping,
        "named_format": args.named_format,
        "content_mode": args.content_mode,
        "strict_essentials": args.strict_essentials,
    }
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
    started = dt.datetime.now(dt.timezone.utc)
    partition_stats = data_frame.rdd.mapPartitionsWithIndex(
        lambda partition_id, rows: process_partition(
            partition_id, rows, conversion_values, delivery_values
        )
    ).collect()
    completed = dt.datetime.now(dt.timezone.utc)
    result: Dict[str, Any] = {
        "status": "dry_run" if args.dry_run else "succeeded",
        "dry_run": bool(args.dry_run),
        "source": _source_descriptor(args),
        "format": args.source_format,
        "named_format": args.named_format,
        "content_mode": args.content_mode,
        "strict_essentials": bool(args.strict_essentials),
        "server_zone": args.server_zone,
        "watermark": {
            "column": args.watermark_column,
            "start_exclusive": args.watermark_start,
            "end_inclusive": args.watermark_end,
        },
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "duration_ms": int((completed - started).total_seconds() * 1000),
        "stats": _sum_stats(partition_stats),
        "ignored_argument_count": len(unknown_args or ()),
    }
    _write_result(result, args.result_path, dbutils)
    return result


def main(argv: Optional[Sequence[str]] = None) -> Mapping[str, Any]:
    args, unknown = parse_args(argv)
    return run(args, unknown_args=unknown)


if __name__ == "__main__":
    main()
