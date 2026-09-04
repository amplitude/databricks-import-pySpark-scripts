# databricks-import-pySpark-scripts
Python scripts that import data from Databricks.

## Agent traces import

`agent_traces_job.py` reads warehouse rows with Spark and sends converted agent
events directly to Amplitude as OTLP/JSON `ExportTraceServiceRequest` payloads
at `/v1/traces`. Authentication uses `Authorization: Bearer <key>`; HTTP V2 is
not a supported delivery mode. It supports exactly two source shapes:

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
