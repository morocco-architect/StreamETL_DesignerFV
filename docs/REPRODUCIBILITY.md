# Reproducibility and evidence protocol

StreamETL uses four explicit evidence levels so that software facts are not confused with
runtime or research claims.

## Evidence levels

- **L1 — code inspection:** implementation is visible in source.
- **L2 — automated test:** a deterministic automated test exercises the stated behavior.
- **L3 — integration/runtime:** the behavior is reproduced on the actual external stack (Docker, Kafka, Spark, AI provider where applicable).
- **L4 — experimental evaluation:** comparative scientific claims are supported by a frozen protocol, data and analysis.

## L2 validation

From repository root:

```bash
./scripts/validate_release.sh
```

The script writes environment metadata, raw pytest output, JUnit XML and coverage XML to
`artifacts/local-validation/`.

CI performs the same class of backend checks on Python 3.10 and 3.12 and also builds the
frontend using Node 20. Public CI evidence from the exact release commit should supersede
locally generated evidence.

## L3 checklist

Before a SoftwareX release is frozen, run the release candidate against the real Docker stack
and retain raw commands/logs for at least these scenarios:

1. Filtering: `examples/filter_orders.json`.
2. Windowed + watermarked aggregation to Parquet: `examples/windowed_sales_to_parquet.json`.
3. Bounded stream-stream join: `examples/bounded_stream_join.json`.
4. Invalid unbounded stream-stream join: confirm deterministic preflight rejection.
5. Invalid non-windowed streaming Aggregate → Parquet: confirm deterministic preflight rejection.
6. Spark runtime failure with a known diagnostic rule: retain raw Spark log and diagnosis response.
7. Ollama provider, if claimed: record model tag/digest, request and validated DAG response.
8. OpenAI provider, if claimed: record API model identifier, request metadata and validated DAG response without storing secrets.

For each successful pipeline retain the native DAG, generated `pipeline_job.py`, service versions,
input messages, output messages/files, timestamps, and exit/status logs.

## L4 Pipeline Intelligence benchmark

Freeze a prompt benchmark before evaluating the AI-assisted builder. Suggested task strata:
Filter, ParseJSON, aggregation, windowed aggregation, stream-static join, bounded stream-stream
join, and invalid/ambiguous requests.

Report at least:

- Exact DAG accuracy;
- node precision/recall/F1;
- edge precision/recall/F1;
- semantic-validity rate;
- generated pipeline execution-success rate;
- repair-success rate;
- invalid/unsafe pipeline acceptance rate.

Only report user-time/productivity outcomes if a controlled human-subject protocol supports them.
The earlier manuscript values such as “~35 min vs ~3 min” and “×11 faster” must not be reused
without such evidence.
