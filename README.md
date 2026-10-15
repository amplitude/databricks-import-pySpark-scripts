# databricks-import-pySpark-scripts
Python scripts that import data from Databricks.

## Agent traces import

`agent_traces_job.py` reads warehouse rows with Spark and sends converted agent
events directly to Amplitude as OTLP/JSON `ExportTraceServiceRequest` payloads
at `https://api.amplitude.com/otlp/v1/traces` (US) or
`https://api.eu.amplitude.com/otlp/v1/traces` (EU). Authentication uses
`Authorization: Bearer <key>`; HTTP V2 is not a supported delivery mode. It
supports exactly two source shapes:

- `mapped-columns`: the existing HTTP V2-shaped `[Agent] ...` mapping remains
  accepted, but each mapped row is translated to one receiver-compatible OTLP
  span.
- `mlflow-uc`: MLflow traces from Unity Catalog are converted to OTLP JSON.

The default content mode is `full`. In full mode, built-in PII redaction is on
by default and covers email addresses, US and international phone numbers,
credit cards, SSNs, IPv4/IPv6 addresses, and likely base64 payloads. Use
`--no-redact-pii` only when raw content is explicitly intended. Additional
regexes can be supplied as a JSON list with
`--custom-redaction-patterns-json`; matches are replaced with `[REDACTED]`.

Session controls:

- `--sample-rate FLOAT`: whole-conversation sampling in `(0, 1]`; defaults to
  `1.0`. Values of `0` or below and above `1` are rejected.
- `--max-sessions INTEGER`: optional cap on the number of conversations.
- `--session-id-column NAME`: optional fast-path/override for a top-level source
  column containing the conversation ID. It is used for both selection and the
  exported `gen_ai.conversation.id`, `session.id`, and `amplitude.session_id`.
- `--user-id-column NAME`: optional top-level user override exported as
  `enduser.id`.

Without `--session-id-column`, the job derives the canonical key through the
same converter contract used for event conversion:

- `mapped-columns` resolves `[Agent] Session ID` from `mappingJson`, including
  nested JSONPath mappings.
- `mlflow-uc` checks explicit `conversation_id` and `session_id` values in the
  row, `trace_metadata`, and `tags` (including JSON-encoded metadata/tags).

`request_id` and `trace_id` are never used as a conversation key because they
can change within one conversation. Rows where the converter cannot derive a
key are excluded and counted under `missing_session_id`; they do not fail the
job.

Sampling and the cap both decide per conversation using a SHA-256 hash of the
conversation ID, then join the chosen conversations back to the rows. A
conversation therefore keeps every row or none, and the same conversations are
chosen on every run regardless of partitioning or row order.

`--dry-run` never sends requests, reads the API-key secret, or acknowledges a
watermark. It reports
rows read, conversations seen/sampled/kept, converted records, skip counts by
reason (`missing_session_id`, `missing_identity`, `invalid_event_type`,
`invalid_timestamp`, `filtered_event`, `sampled_out`, `max_sessions`), and the
request bytes that would have been sent, plus at most three secret-safe,
built-in-redacted input/output previews. The job computes
`watermark.snapshot_upper` from the filtered snapshot; after successful live
delivery it returns that value as `watermark.acknowledged_upper`. The caller
owns persistence and must not persist progress from a dry run.

Each OTLP request is capped at 900 KiB decompressed JSON and 2,000 spans.
Retries are bounded by `--max-retries`.

Example:

```shell
python agent_traces_job.py \
  --table catalog.schema.mlflow_traces \
  --format mlflow-uc \
  --content-mode full \
  --sample-rate 0.25 \
  --max-sessions 20 \
  --custom-redaction-patterns-json '["ACME-[0-9]+"]' \
  --dry-run
```

For `mapped-columns`, also pass exactly one of `--mapping-json` or
`--mapping-json-path`. Normal delivery requires `--secret-scope` and
`--api-key-secret-key`; API keys must not be passed directly.


## Count-free Databricks exports

For `unload_databricks_data_to_s3.py` and its `TEST_` counterpart,
`--partitioning-strategy repartition --target_partitions N` skips the preliminary
DataFrame count and repartitions to the supplied target (minimum one). Each JSON
or Parquet writer enforces `--max_records_per_file` independently of that target,
so a writer task can produce several bounded files. This limits rows per file,
not bytes per file or the number of writer tasks. The writer option does not
change the Spark session's file limit.

Without a target, repartition retains count-based sizing. The `none` and
`coalesce` strategies retain their existing behavior.

### Adaptive export partitioning

`--partitioning-strategy rebalance` skips the preliminary count and applies a
Spark `REBALANCE` hint with no fixed target. AQE uses runtime shuffle sizes to
combine small partitions and split oversized ones during the write. Initial
shuffle parallelism and advisory byte sizes come from the existing Spark session
configuration; the exporter does not change those settings or the worker count.
Queued tasks can use additional executors if the cluster scales up, but this mode
does not guarantee full utilization of every potential worker.

The mode requires Spark 3.2 or later and the following settings enabled:

- `spark.sql.adaptive.enabled`
- `spark.databricks.optimizer.adaptive.enabled` on Databricks
- `spark.sql.adaptive.coalescePartitions.enabled`
- `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled`

The exporter enables the required AQE settings in the export's Spark session
before planning the query. Settings already enabled are left alone; settings
changed by the exporter are restored after the write, including on failure.
This does not change the cluster configuration, worker count, initial shuffle
parallelism, or advisory partition size. The Databricks-specific setting is
configured only when the runtime exposes it.

Spark versions older than 3.2, missing prerequisites, or settings that cannot be
enabled fall back to the existing count-based repartition path. The fallback
retains the writer-scoped row cap for JSON and Parquet and logs its reason.
Errors reading the source or writing the export still propagate normally.

The mode rejects `--target_partitions`. JSON and Parquet retain the writer-scoped
`--max_records_per_file` limit: adaptive byte sizing is not a row cap or a
guarantee of output file bytes.

Deploy exporter support before opting connections into `rebalance`. Existing
installed exporter jobs must be updated before those connections are opted in.

After the exporter change is deployed, Falcon can opt a connection into this
mode through `falcon.databricksImport.partitioningStrategyOverrides` using
`"CONNECTION_ID": "rebalance"` (requires the paired Falcon strategy change).
Existing connection defaults are unchanged; remove the entry to roll back.
Verify on the intended Databricks runtime before a production canary.

The default unit suite uses PySpark stubs:

```shell
python -m unittest discover -s test -p 'test*.py'
```

With PySpark 3.5 and a compatible Java runtime installed, run the separate local
Spark regression suite to verify JSON/Parquet row preservation, file row limits,
empty exports, skipping count, and isolation from subsequent exports:

```shell
python -m unittest discover -s test -p 'spark_*_regression.py'
```

These suites write only temporary local files and require no Databricks or S3
credentials. The adaptive suite also verifies that 10,000 rows coalesce to one
writer task, a larger export splits beyond its initial shuffle partition count,
and a coalesced task still rotates capped files. They are excluded from the
default stubbed unit discovery and run in the separate Spark CI job.
